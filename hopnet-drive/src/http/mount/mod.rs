//! /integrations/mount routes — the Linux mount daemon's read surface
//! (RFC-018 S2).
//!
//! UUID-native, no sentinel identifier strings: absent ids mean the root.
//! Cursor pagination is last-seen-id. Download is blob-addressed
//! (snapshot-at-open) and honors HTTP Range. Mutations and /watch arrive
//! in later slices (S6, S4); the host layers device-token auth around the
//! whole router.

use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::get,
};
use serde::Deserialize;
use std::str::FromStr;

use crate::db;
use crate::host::DriveState;
use crate::paths::{build_encrypted_path, encrypt_part};
use crate::upload::session_or_status;
use hopnet_common::CustomUUID;
use hopnet_common::db::InodeType;
use hopnet_common::mount::{MountChangesResponse, MountEnumerateResponse, MountItem};
use hopnet_projection::DatabaseError;

#[cfg(test)]
mod tests;

/// Children per enumerate page. The daemon walks all pages at opendir, so
/// this trades round-trips against response size only.
const PAGE_SIZE: u32 = 100;

pub fn router<S: Clone + Send + Sync + 'static>(state: DriveState) -> Router<S> {
    Router::new()
        .route("/enumerate", get(get_enumerate))
        .route("/lookup", get(get_lookup))
        .route("/item", get(get_item))
        .route("/changes", get(get_changes))
        .route("/download", get(get_download))
        .route("/watch", get(get_watch))
        .with_state(state)
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
        loop {
            match rx.recv().await {
                Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    yield Ok(Event::default().data(""));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
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

fn current_height_or_500(state: &DriveState) -> Result<i32, StatusCode> {
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
    pub since_height: Option<i32>,
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
