//! /integrations/mount routes — the Linux mount daemon's read surface
//! (RFC-018 S2).
//!
//! UUID-native, no sentinel identifier strings: absent ids mean the root.
//! Cursor pagination is last-seen-id. Download is blob-addressed
//! (snapshot-at-open) and honors HTTP Range. Mutations and /watch arrive
//! in later slices (S6, S4); the host layers device-token auth around the
//! whole router.

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::Response,
    routing::get,
    Extension, Json, Router,
};
use serde::Deserialize;
use std::str::FromStr;
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;

use crate::db;
use crate::host::{DriveState, TxSigner, TxSpec, TxSubmitError};
use crate::model::{Inode, InodeOwner};
use crate::paths::{build_encrypted_path, encrypt_part};
use crate::upload::session_or_status;
use hopnet_common::db::InodeType;
use hopnet_common::mount::{
    MountChangesResponse, MountDeleteRequest, MountEnumerateResponse, MountItem,
    MountModifyRequest, MountMutationResponse,
};
use hopnet_common::CustomUUID;
use hopnet_projection::DatabaseError;

#[cfg(test)]
mod tests;

/// Children per enumerate page. The daemon walks all pages at opendir, so
/// this trades round-trips against response size only.
const PAGE_SIZE: u32 = 100;

pub fn router<S: Clone + Send + Sync + 'static>(state: DriveState) -> Router<S> {
    let reads = Router::new()
        .route("/enumerate", get(get_enumerate))
        .route("/lookup", get(get_lookup))
        .route("/item", get(get_item))
        .route("/changes", get(get_changes))
        .route("/download", get(get_download))
        .route("/watch", get(get_watch));

    // Mutations (RFC-018 S6): strict — respond only after decided AND
    // applied locally. Write-gated like every projection write surface.
    let writes = Router::new()
        .route("/create", axum::routing::post(post_create))
        .route("/modify", axum::routing::patch(patch_modify))
        .route("/content", axum::routing::put(put_content))
        .route("/delete", axum::routing::delete(delete_item))
        .layer(DefaultBodyLimit::max(5 * 1024 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            super::write_gate,
        ));

    reads.merge(writes).with_state(state)
}

fn tx_error_status(e: &TxSubmitError) -> StatusCode {
    match e {
        // Outcome UNKNOWN — the tx may still commit; never claim success.
        TxSubmitError::Timeout => StatusCode::GATEWAY_TIMEOUT,
        TxSubmitError::Rejected(_) => StatusCode::CONFLICT,
        TxSubmitError::Signing => StatusCode::INTERNAL_SERVER_ERROR,
        TxSubmitError::Submit => StatusCode::SERVICE_UNAVAILABLE,
        // Admission closed at a regenesis boundary (RFC-019 S5) — the
        // freeze is temporary, so this is a back-off, not a failure.
        TxSubmitError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// Post-apply read-back: fresh item state at the decided height.
fn read_back_item(
    state: &DriveState,
    user_id: i32,
    inode_id: &CustomUUID,
    siv_key: &aes_siv::Key<aes_siv::siv::Aes256Siv>,
    siv_nonce: &aes_siv::Nonce,
) -> Result<MountItem, StatusCode> {
    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    db::mount::item_by_id(&db_lock, user_id, inode_id, siv_key, siv_nonce).map_err(status_of)
}

/// SSE heartbeat interval; the daemon treats ~3 missed heartbeats as a
/// dead connection.
const WATCH_KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(15);

/// GET /integrations/mount/watch — SSE change push (RFC-018 S4).
///
/// Content-free pokes: "something changed, run /changes from your anchor".
/// Node-scoped, not user-scoped — /changes does the per-user filtering.
/// Lagged receivers emit a poke too (pokes are idempotent, missed ones
/// coalesce). Note: a /watch connection holds one slot of the host's
/// API concurrency limit for its lifetime — fine at desktop scale.
pub async fn get_watch(
    State(state): State<DriveState>,
    Extension(_user_id): Extension<i32>,
) -> axum::response::sse::Sse<
    impl tokio_stream::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event, KeepAlive, Sse};

    let mut rx = state.notify.subscribe();
    let stream = async_stream::stream! {
        // Closed ends the stream; Lagged still means "something changed".
        while let Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) =
            rx.recv().await
        {
            yield Ok(Event::default().data(""));
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(WATCH_KEEPALIVE))
}

fn status_of(e: DatabaseError) -> StatusCode {
    match e {
        DatabaseError::NotFound => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Encrypted path of a parent folder; None parent = root ("" — root
/// children are single-segment paths and the LIKE patterns build from "").
async fn resolve_parent_path(
    state: &DriveState,
    user_id: i32,
    parent_id: &Option<String>,
) -> Result<(String, Option<CustomUUID>), StatusCode> {
    match parent_id {
        None => Ok((String::new(), None)),
        Some(id) => {
            let inode_id = CustomUUID::from_str(id).map_err(|_| StatusCode::BAD_REQUEST)?;
            let db_lock = state
                .db_pool
                .get()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let path = db::documentprovider::get_path_by_inode_id(&db_lock, &inode_id, user_id)
                .map_err(status_of)?;
            Ok((path, Some(inode_id)))
        }
    }
}

fn current_height_or_500(state: &DriveState) -> Result<u64, StatusCode> {
    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    db::current_height(&db_lock).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[derive(Debug, Deserialize)]
pub struct EnumerateQuery {
    /// Parent folder UUID; absent = root.
    pub parent_id: Option<String>,
    /// Opaque resume cursor from the previous page.
    pub cursor: Option<String>,
}

/// GET /integrations/mount/enumerate?parent_id=&cursor=
pub async fn get_enumerate(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<EnumerateQuery>,
) -> Result<Json<MountEnumerateResponse>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;
    let (parent_path, _) = resolve_parent_path(&state, user_id, &query.parent_id).await?;

    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut items = db::mount::children_page(
        &db_lock,
        user_id,
        &parent_path,
        query.cursor.as_deref(),
        PAGE_SIZE + 1,
        &session.siv_key,
        &session.siv_nonce,
    )
    .map_err(status_of)?;

    let next_cursor = if items.len() as u32 > PAGE_SIZE {
        items.pop();
        items
            .last()
            .and_then(|item| item.id.as_ref())
            .map(|id| id.to_string())
    } else {
        None
    };

    let height = db::current_height(&db_lock).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(MountEnumerateResponse {
        items,
        next_cursor,
        height,
    }))
}

#[derive(Debug, Deserialize)]
pub struct LookupQuery {
    /// Parent folder UUID; absent = root.
    pub parent_id: Option<String>,
    /// Plaintext child name (one path segment).
    pub name: String,
}

/// GET /integrations/mount/lookup?parent_id=&name=
pub async fn get_lookup(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<LookupQuery>,
) -> Result<Json<MountItem>, StatusCode> {
    if query.name.is_empty() || query.name.contains('/') {
        return Err(StatusCode::BAD_REQUEST);
    }
    let session = session_or_status(&state, user_id).await?;
    let (parent_path, _) = resolve_parent_path(&state, user_id, &query.parent_id).await?;

    let segment = encrypt_part(&query.name, &session.siv_key, &session.siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let encrypted_path = build_encrypted_path(&parent_path, &segment);

    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let item = db::mount::item_by_exact_path(
        &db_lock,
        user_id,
        &encrypted_path,
        &session.siv_key,
        &session.siv_nonce,
    )
    .map_err(status_of)?;

    Ok(Json(item))
}

#[derive(Debug, Deserialize)]
pub struct ItemQuery {
    /// Inode UUID; absent = the root itself.
    pub id: Option<String>,
}

/// GET /integrations/mount/item?id=
pub async fn get_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
) -> Result<Json<MountItem>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;

    let Some(id) = &query.id else {
        // Synthesized root: no id, no parent, folder, dates unknowable.
        let height = current_height_or_500(&state)?;
        return Ok(Json(MountItem {
            id: None,
            parent_id: None,
            name: String::new(),
            item_type: InodeType::Folder,
            size: None,
            blob_id: None,
            created_ms: 0,
            modified_ms: None,
            height: Some(height),
        }));
    };

    let inode_id = CustomUUID::from_str(id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let item = db::mount::item_by_id(
        &db_lock,
        user_id,
        &inode_id,
        &session.siv_key,
        &session.siv_nonce,
    )
    .map_err(status_of)?;

    Ok(Json(item))
}

#[derive(Debug, Deserialize)]
pub struct MountChangesQuery {
    /// Height anchor; only strictly-newer modifications are returned.
    pub since_height: Option<u64>,
}

/// GET /integrations/mount/changes?since_height= — whole tree.
pub async fn get_changes(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<MountChangesQuery>,
) -> Result<Json<MountChangesResponse>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;
    let since = query.since_height.unwrap_or(0);

    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (items, deleted_ids) = db::mount::changes_since(
        &db_lock,
        user_id,
        since,
        &session.siv_key,
        &session.siv_nonce,
    )
    .map_err(status_of)?;

    let height = db::current_height(&db_lock).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(MountChangesResponse {
        items,
        deleted_ids,
        height,
    }))
}

/// POST /integrations/mount/create — multipart, strict.
///
/// Fields in order: optional `parent_id` (text; absent = root), then
/// EITHER `folder_name` (text) OR exactly one `file_{size}` part whose
/// file_name is the new file's name. Responds 201 with the fresh item
/// and decided height only after local apply.
pub async fn post_create(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<MountMutationResponse>), StatusCode> {
    let session = session_or_status(&state, user_id).await?;

    let mut parent_id: Option<String> = None;
    let mut parent_path: Option<String> = None;
    let mut created: Option<(Inode, Vec<hopnet_storage::store::BlobInsertOp>)> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "parent_id" => {
                if created.is_some() {
                    return Err(StatusCode::BAD_REQUEST);
                }
                parent_id = Some(field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?);
            }
            "folder_name" => {
                let folder_name = field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?;
                if folder_name.is_empty() || folder_name.contains('/') {
                    return Err(StatusCode::BAD_REQUEST);
                }
                let parent =
                    ensure_parent_path(&state, user_id, &parent_id, &mut parent_path).await?;
                let segment = encrypt_part(&folder_name, &session.siv_key, &session.siv_nonce)
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                let path = build_encrypted_path(&parent, &segment);
                ensure_vacant(&state, user_id, &path, &session)?;
                created = Some((
                    Inode {
                        id: CustomUUID::new(None),
                        owner: InodeOwner::Id(user_id),
                        path,
                        inode_type: InodeType::Folder,
                        data_id: None,
                    },
                    Vec::new(),
                ));
            }
            file_field if file_field.starts_with("file_") => {
                let file_size: usize = file_field
                    .strip_prefix("file_")
                    .and_then(|s| s.parse().ok())
                    .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
                let filename = field
                    .file_name()
                    .ok_or(StatusCode::BAD_REQUEST)?
                    .to_string();
                if filename.is_empty() || filename.contains('/') {
                    return Err(StatusCode::BAD_REQUEST);
                }
                let parent =
                    ensure_parent_path(&state, user_id, &parent_id, &mut parent_path).await?;
                let segment = encrypt_part(&filename, &session.siv_key, &session.siv_nonce)
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                ensure_vacant(
                    &state,
                    user_id,
                    &build_encrypted_path(&parent, &segment),
                    &session,
                )?;
                let reader = StreamReader::new(field.map(|r| r.map_err(std::io::Error::other)));
                let (inode, _data_id, blob_op) = crate::upload::assemble_file_inode(
                    &state, &session, user_id, &parent, &filename, reader, file_size,
                )
                .await?;
                created = Some((inode, blob_op.into_iter().collect()));
            }
            _ => return Err(StatusCode::UNPROCESSABLE_ENTITY),
        }
    }

    let Some((inode, blob_ops)) = created else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let inode_id = inode.id.clone();

    let attestation = if let Some(node_id) = state.node_id() {
        let conn = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        crate::upload::build_upload_attestation(&tx, node_id, &blob_ops).unwrap_or_default()
    } else {
        None
    };

    let height =
        crate::upload::submit_inodes(&state, user_id, blob_ops, vec![inode], attestation).await?;

    let item = read_back_item(
        &state,
        user_id,
        &inode_id,
        &session.siv_key,
        &session.siv_nonce,
    )?;
    Ok((
        StatusCode::CREATED,
        Json(MountMutationResponse {
            item: Some(item),
            height,
        }),
    ))
}

async fn ensure_parent_path(
    state: &DriveState,
    user_id: i32,
    parent_id: &Option<String>,
    cache: &mut Option<String>,
) -> Result<String, StatusCode> {
    if let Some(path) = cache {
        return Ok(path.clone());
    }
    let (path, _) = resolve_parent_path(state, user_id, parent_id).await?;
    *cache = Some(path.clone());
    Ok(path)
}

/// Fast-path collision check (the consensus validation re-checks
/// authoritatively; this converts the common case to a clean 409).
fn ensure_vacant(
    state: &DriveState,
    user_id: i32,
    encrypted_path: &str,
    session: &hopnet_projection::host::UserSession,
) -> Result<(), StatusCode> {
    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match db::mount::item_by_exact_path(
        &db_lock,
        user_id,
        encrypted_path,
        &session.siv_key,
        &session.siv_nonce,
    ) {
        Ok(_) => Err(StatusCode::CONFLICT),
        Err(DatabaseError::NotFound) => Ok(()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// PATCH /integrations/mount/modify — JSON rename/move, strict.
pub async fn patch_modify(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Json(request): Json<MountModifyRequest>,
) -> Result<Json<MountMutationResponse>, StatusCode> {
    if request.new_parent_id.is_some() && request.new_parent_root {
        return Err(StatusCode::BAD_REQUEST);
    }
    if request.new_parent_id.is_none() && !request.new_parent_root && request.new_name.is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if let Some(name) = &request.new_name {
        if name.is_empty() || name.contains('/') {
            return Err(StatusCode::BAD_REQUEST);
        }
    }
    let session = session_or_status(&state, user_id).await?;

    let current_path = {
        let db_lock = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::documentprovider::get_path_by_inode_id(&db_lock, &request.id, user_id)
            .map_err(status_of)?
    };

    // Target parent: explicit new parent, root, or unchanged.
    let parent_path = if let Some(new_parent) = &request.new_parent_id {
        let db_lock = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::documentprovider::get_path_by_inode_id(&db_lock, new_parent, user_id)
            .map_err(status_of)?
    } else if request.new_parent_root {
        String::new()
    } else {
        match current_path.rfind('/') {
            Some(0) | None => String::new(),
            Some(idx) => current_path[..idx].to_string(),
        }
    };

    // Target name segment: re-encrypt a new name, or reuse the existing
    // encrypted segment (deterministic SIV — identical either way).
    let segment = match &request.new_name {
        Some(name) => encrypt_part(name, &session.siv_key, &session.siv_nonce)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        None => current_path
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string(),
    };
    let new_encrypted_path = build_encrypted_path(&parent_path, &segment);
    if new_encrypted_path == current_path {
        return Err(StatusCode::BAD_REQUEST);
    }
    ensure_vacant(&state, user_id, &new_encrypted_path, &session)?;

    // Validate-then-rollback (the consensus preflight repeats this
    // authoritatively).
    {
        let mut conn = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let db_tx = conn
            .transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::files::modify_item(
            &db_tx,
            user_id,
            request.id.clone(),
            Some(new_encrypted_path.clone()),
            None,
            None,
            &state.fragments_dir,
            0, // validation only — rolled back, height never persisted
        )
        .map_err(|e| match e {
            DatabaseError::NotFound => StatusCode::NOT_FOUND,
            DatabaseError::ConflictError => StatusCode::CONFLICT,
            DatabaseError::InvalidPayload => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
        db_tx
            .rollback()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let payload = crate::envelopes::ModifyItemPayload {
        user_id,
        inode_id: request.id.clone(),
        new_encrypted_path: Some(new_encrypted_path),
        content_update: None,
        incoming_share_updates: None,
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let height = state
        .txs
        .submit_decided(TxSpec {
            function: "modify_item",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
        .map_err(|e| tx_error_status(&e))?;

    let item = read_back_item(
        &state,
        user_id,
        &request.id,
        &session.siv_key,
        &session.siv_nonce,
    )?;
    Ok(Json(MountMutationResponse {
        item: Some(item),
        height,
    }))
}

/// PUT /integrations/mount/content — multipart content replacement,
/// strict (RFC-018 S7). Fields: `inode_id` (text) then one `file_{size}`
/// part. Whole-file rewrite (mints a new blob — issue #25 tracks deltas).
pub async fn put_content(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    mut multipart: Multipart,
) -> Result<Json<MountMutationResponse>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;

    let inode_id = match multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        Some(field) if field.name() == Some("inode_id") => {
            let text = field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?;
            CustomUUID::from_str(&text).map_err(|_| StatusCode::BAD_REQUEST)?
        }
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let field = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .ok_or(StatusCode::BAD_REQUEST)?;
    let field_name = field.name().ok_or(StatusCode::BAD_REQUEST)?.to_string();
    let file_size: usize = field_name
        .strip_prefix("file_")
        .and_then(|s| s.parse().ok())
        .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;

    let (_data_id, blob_op, incoming_share_updates) =
        crate::http::files::prepare_content_update(&state, user_id, &inode_id, field, file_size)
            .await?;

    let payload = crate::envelopes::ModifyItemPayload {
        user_id,
        inode_id: inode_id.clone(),
        new_encrypted_path: None,
        content_update: Some(crate::envelopes::DriveContentUpdate { blob_op }),
        incoming_share_updates,
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let height = state
        .txs
        .submit_decided(TxSpec {
            function: "modify_item",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
        .map_err(|e| tx_error_status(&e))?;

    let item = read_back_item(
        &state,
        user_id,
        &inode_id,
        &session.siv_key,
        &session.siv_nonce,
    )?;
    Ok(Json(MountMutationResponse {
        item: Some(item),
        height,
    }))
}

/// DELETE /integrations/mount/delete — JSON, strict.
pub async fn delete_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Json(request): Json<MountDeleteRequest>,
) -> Result<Json<MountMutationResponse>, StatusCode> {
    // Deletes need no key material; the call is the liveness gate only.
    let _session = session_or_status(&state, user_id).await?;

    let encrypted_path = {
        let db_lock = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::documentprovider::get_path_by_inode_id(&db_lock, &request.id, user_id)
            .map_err(status_of)?
    };

    // Validate-then-rollback; 409 = non-empty folder without recursive.
    {
        let mut conn = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let db_tx = conn
            .transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if !request.recursive {
            let is_empty =
                db::fileprovider::is_folder_empty(state.db_pool.get(), &encrypted_path, user_id)
                    .unwrap_or(true);
            if !is_empty {
                return Err(StatusCode::CONFLICT);
            }
        }
        db::files::delete_files(&db_tx, encrypted_path.clone(), user_id, 0).map_err(
            |e| match e {
                DatabaseError::NotFound => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        )?;
        db_tx
            .rollback()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let payload = crate::envelopes::DeleteFilesPayload {
        encrypted_path,
        user_id,
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let height = state
        .txs
        .submit_decided(TxSpec {
            function: "delete_files",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
        .map_err(|e| tx_error_status(&e))?;

    Ok(Json(MountMutationResponse { item: None, height }))
}

#[derive(Debug, Deserialize)]
pub struct MountDownloadQuery {
    /// The blob to stream — NOT an inode id. Blob addressing gives open
    /// FUSE handles snapshot-at-open semantics under concurrent modifies.
    pub blob_id: String,
}

/// GET /integrations/mount/download?blob_id= — honors HTTP Range.
pub async fn get_download(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<MountDownloadQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    let blob_id = CustomUUID::from_str(&query.blob_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let requested_range = super::parse_range(&headers);

    let download_info =
        match crate::download::reconstruct_blob_range(&state, &blob_id, user_id, requested_range)
            .await
        {
            Ok(info) => info,
            Err(crate::download::FileReconstructionError::RangeNotSatisfiable(file_size)) => {
                let response = Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(header::CONTENT_RANGE, format!("bytes */{}", file_size))
                    .body(Body::empty())
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                return Ok(response);
            }
            Err(e) => {
                tracing::error!("Error reconstructing blob {}: {:?}", blob_id, e);
                return Err(StatusCode::from(e));
            }
        };

    // Blob-addressed: no filename is known or needed — content only.
    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ACCEPT_RANGES, "bytes");

    if download_info.is_partial {
        let range = download_info.range.as_ref().unwrap();
        let content_length = range.end - range.start + 1;
        builder = builder
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_LENGTH, content_length)
            .header(
                header::CONTENT_RANGE,
                format!(
                    "bytes {}-{}/{}",
                    range.start, range.end, download_info.file_size
                ),
            );
    } else {
        builder = builder
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, download_info.file_size);
    }

    builder
        .body(Body::from_stream(download_info.stream))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}
