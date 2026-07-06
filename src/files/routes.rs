use axum::http::StatusCode;
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Extension, Multipart, Path, Query, State},
    http::header,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use rand::Rng;
use reed_solomon_simd::ReedSolomonEncoder;
use std::str::FromStr;

use crate::{
    db::{self, Blake3Hash, DatabaseError, Inode},
    files::functions::{
        calculate_chunk_padding, encrypt_chunk, encrypt_part, encrypt_path, store_fragment,
    },
};
use hopnet_common::FileItem;
use serde::{Deserialize, Serialize};

use super::*;
use crate::db::CustomUUID;
use either::Either::{Left, Right};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;

#[derive(Deserialize)]
pub struct GetQueryParams {
    path: String,
}

#[derive(Deserialize)]
pub struct CleanupQueryParams {
    batch_size: i32,
    retention_days: i64,
}

pub fn router(state: crate::AppState) -> Router<crate::AppState> {
    // Reads bypass the import gate; writes go through it. Splitting into two
    // sub-routers + `merge` lets us attach `.layer(import_gate)` to the write
    // router only — gate attachment is explicit, no in-middleware method peek.
    let reads = Router::new()
        .route("/recent", get(get_recent_files))
        .route("/", get(get_files))
        .route("/{*path}", get(get_file_fragments));

    let writes = Router::new()
        .route(
            "/",
            post(post_files).patch(patch_files).delete(delete_files),
        )
        .layer(axum::middleware::from_fn_with_state(
            state,
            crate::takeout::import_gate::import_gate,
        ));

    reads
        .merge(writes)
        .layer(DefaultBodyLimit::max(5000 * 1_000_000))
}

/// Shared file ingest for both create and modify operations. The pipeline
/// (encrypt-then-RS, fragment store, keyed integrity hash) lives in the
/// substrate crate; this wrapper returns the substrate's BlobInsertOp —
/// the wire sub-payload that rides the drive envelope. The caller attaches
/// the recipient wraps (`access`).
pub async fn process_uploaded_file<R: AsyncRead + Unpin>(
    source: R,
    file_size: usize,
    dataid: CustomUUID,
    per_file_key: &chacha20poly1305::Key,
    fragments_dir: &str,
) -> Result<hopnet_storage::store::BlobInsertOp, StatusCode> {
    let outcome = hopnet_storage::api::put(source, file_size, dataid.clone(), per_file_key, fragments_dir)
        .await
        .map_err(|e| match e {
            hopnet_storage::StorageError::Read(_) => StatusCode::UNPROCESSABLE_ENTITY,
            hopnet_storage::StorageError::Io(_) => StatusCode::INSUFFICIENT_STORAGE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    let fragments = outcome
        .fragments
        .into_iter()
        .map(|f| hopnet_storage::store::FragmentMeta {
            blob_id: dataid.clone(),
            chunk_number: f.chunk_number,
            local_index: f.local_index,
            fragment_id: f.fragment_id,
            fragment_hash: f.fragment_hash,
            recovery: f.recovery,
        })
        .collect();

    Ok(hopnet_storage::store::BlobInsertOp {
        blob_id: dataid,
        integrity_hash: outcome.integrity_hash,
        added_bytes: outcome.added_bytes,
        file_size: file_size as u64,
        fragments,
        access: Vec::new(), // caller attaches recipient wraps
    })
}

#[derive(Deserialize)]
pub struct RebalanceQueryParams {
    max_data_blocks: i32,
    min_age_heights: i32,
}

#[derive(Serialize)]
pub struct FileFragmentsResponse {
    pub file_hash: Blake3Hash,
    pub fragments: Vec<(Blake3Hash, crate::db::ChunkType)>,
}

/// Build share propagation data for a content update on a shared file.
/// Returns (extra_file_access_entries, incoming_share_updates).
/// Called by both PATCH /files and fileprovider modify routes.
pub fn build_share_propagation(
    conn: &rusqlite::Connection,
    inode_id: &CustomUUID,
    user_id: i32,
    new_data_block_id: &CustomUUID,
    per_file_key: &chacha20poly1305::Key,
) -> Result<
    (
        Vec<crate::db::types::BlobAccess>,
        Option<Vec<crate::shares::types::IncomingShareUpdate>>,
    ),
    StatusCode,
> {
    // Look up the inode's current data_id (old data_block)
    let inode_info = crate::db::files::get_inode_by_id(conn, inode_id, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let old_data_block_id = match inode_info.0 {
        Some(id) => id,
        None => return Ok((vec![], None)), // No data block → no shares to propagate
    };

    let sharers = crate::db::shares::get_sharers_for_data_block_conn(conn, &old_data_block_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if sharers.is_empty() {
        return Ok((vec![], None));
    }

    // Create FileAccess entries for other accepted sharers
    let mut extra_file_access_entries = Vec::new();
    for &sharer_id in &sharers {
        if sharer_id == user_id {
            continue;
        }
        let fa = crate::db::types::blob_access_for_user_with_conn(
            conn,
            new_data_block_id.clone(),
            sharer_id,
            per_file_key,
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        extra_file_access_entries.push(fa);
    }

    // Build IncomingShareUpdate entries for pending shares
    let pending =
        crate::db::shares::get_incoming_shares_for_data_block_conn(conn, &old_data_block_id)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let incoming_share_updates = if pending.is_empty() {
        None
    } else {
        let mut updates = Vec::new();
        for incoming in &pending {
            let fa = crate::db::types::blob_access_for_user_with_conn(
                conn,
                new_data_block_id.clone(),
                incoming.recipient_id,
                per_file_key,
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let blob = bincode::serde::encode_to_vec(&fa, bincode::config::standard())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            updates.push(crate::shares::types::IncomingShareUpdate {
                incoming_share_id: incoming.id.clone(),
                new_file_access_blob: blob,
            });
        }
        Some(updates)
    };

    Ok((extra_file_access_entries, incoming_share_updates))
}

pub async fn get_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(params): Query<GetQueryParams>,
) -> Result<Json<Vec<FileItem>>, StatusCode> {
    let session = app_state.get_session(user_id).await?;
    // let's encrypt the path so we can search for it
    let enc_path = encrypt_path(params.path, &session.siv_key, &session.siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match db::files::get_files(
        app_state.db_pool.get(),
        enc_path,
        user_id,
        &session.siv_key,
        &session.siv_nonce,
    ) {
        Ok(files) => Ok(Json(files)),
        Err(e) => {
            tracing::error!("Error getting files: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
pub struct RecentQueryParams {
    limit: Option<i32>,
}

pub async fn get_recent_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(params): Query<RecentQueryParams>,
) -> Result<Json<Vec<FileItem>>, StatusCode> {
    let session = app_state.get_session(user_id).await?;
    let limit = params.limit.unwrap_or(50);
    match db::files::get_recent_files(
        app_state.db_pool.get(),
        user_id,
        limit,
        &session.siv_key,
        &session.siv_nonce,
    ) {
        Ok(files) => Ok(Json(files)),
        Err(e) => {
            tracing::error!("Error getting recent files: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_file_fragments(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Path(path): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    let session = app_state.get_session(user_id).await?;
    let file_path = if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path)
    };

    let filename = path.split('/').next_back().unwrap_or("download");

    // Parse Range header: supports "bytes=START-END" and "bytes=START-" (single range only)
    let requested_range = headers.get(header::RANGE).and_then(|val| {
        let s = val.to_str().ok()?;
        let s = s.strip_prefix("bytes=")?;
        // Ignore multi-range (contains comma)
        if s.contains(',') {
            return None;
        }
        let mut parts = s.splitn(2, '-');
        let start: u64 = parts.next()?.parse().ok()?;
        let end: Option<u64> = parts
            .next()
            .and_then(|e| if e.is_empty() { None } else { e.parse().ok() });
        Some((start, end))
    });

    let enc_path = encrypt_path(file_path, &session.siv_key, &session.siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let download_info = match crate::files::download::reconstruct_file_range(
        &app_state,
        enc_path,
        user_id,
        &app_state.fragments_dir,
        requested_range,
    )
    .await
    {
        Ok(info) => info,
        Err(crate::files::download::FileReconstructionError::RangeNotSatisfiable(file_size)) => {
            let response = Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{}", file_size))
                .body(Body::empty())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            return Ok(response);
        }
        Err(e) => {
            tracing::error!("Error reconstructing file: {:?}", e);
            return Err(StatusCode::from(e));
        }
    };

    // Detect MIME type from filename
    let content_type = mime_guess::from_path(filename)
        .first_raw()
        .unwrap_or("application/octet-stream");

    let mut builder = Response::builder()
        .header(
            header::CONTENT_DISPOSITION,
            format!("inline; filename=\"{}\"", filename),
        )
        .header(header::CONTENT_TYPE, content_type)
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

pub async fn post_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>, // Extract user_id from JWT via auth middleware
    mut multipart: Multipart,
) -> Result<(), StatusCode> {
    let session = app_state.get_session(user_id).await?;
    // Get user from database to access their X25519 public key
    let user = match crate::db::users::get_user_by_userid(app_state.db_pool.get(), user_id) {
        Ok(Some(user)) => user,
        Ok(None) => return Err(StatusCode::UNAUTHORIZED),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // read path part first (either "path" or "parent_item_identifier" + "filename")
    // need path in later file processing
    let mut inodes: Vec<Inode> = Vec::new();
    let mut has_files = false;
    let mut blob_ops: Vec<hopnet_storage::store::BlobInsertOp> = Vec::new();
    let mut folder_name: Option<String> = None;

    // Handle both regular path and FileProvider parent_item_identifier approaches
    let path = match multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(part) => {
            match part.name() {
                Some("path") => {
                    // Regular path approach
                    let unencrypted_path = part
                        .text()
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                    encrypt_path(unencrypted_path, &session.siv_key, &session.siv_nonce)
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                }
                Some("parent_item_identifier") => {
                    // FileProvider approach - need to look up parent path and construct full path
                    let parent_item_identifier = part
                        .text()
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    tracing::debug!(
                        "Received parent_item_identifier: '{}'",
                        parent_item_identifier
                    );

                    // Get parent path and return it encrypted
                    if parent_item_identifier == "NSFileProviderRootContainerItemIdentifier" {
                        tracing::debug!("Handling root container case");
                        encrypt_path("/".to_string(), &session.siv_key, &session.siv_nonce)
                            .await
                            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                    } else if let Some(inode_id_str) = parent_item_identifier.strip_prefix("item:")
                    {
                        // Extract inode_id and look up encrypted path
                        tracing::debug!(
                            "Trying to parse inode_id: '{}' from parent_item_identifier: '{}'",
                            inode_id_str,
                            parent_item_identifier
                        );
                        let inode_id = crate::db::CustomUUID::from_str(inode_id_str)
                            .map_err(|_| StatusCode::BAD_REQUEST)?;

                        tracing::debug!(
                            "Looking up inode_id: {} for user_id: {}",
                            inode_id,
                            user_id
                        );
                        match crate::db::fileprovider::get_item_metadata_by_inode_id(
                            app_state.db_pool.get(),
                            inode_id,
                            user_id,
                        ) {
                            Ok((encrypted_path, _, _, _, _, _)) => {
                                tracing::debug!("Found encrypted_path: {}", &encrypted_path);
                                encrypted_path
                            }
                            Err(e) => {
                                tracing::error!("Failed to find item metadata: {:?}", e);
                                return Err(StatusCode::NOT_FOUND);
                            }
                        }
                    } else {
                        return Err(StatusCode::BAD_REQUEST);
                    }
                }
                _ => return Err(StatusCode::BAD_REQUEST),
            }
        }
        None => return Err(StatusCode::BAD_REQUEST),
    };

    tracing::debug!("Final path after ALL path processing: '{}'", path);

    while let Some(part) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        match part.name() {
            Some(field_name) if field_name.starts_with("file_") => {
                has_files = true;
                let filename = part
                    .file_name()
                    .map(|s| s.to_string())
                    .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                let file_size_str = field_name
                    .strip_prefix("file_")
                    .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
                let file_size = file_size_str
                    .parse::<usize>()
                    .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;

                let reader = StreamReader::new(part.map(|r| r.map_err(std::io::Error::other)));
                let (inode, dataid, blob_op) = crate::files::helpers::assemble_file_inode(
                    &app_state, &session, user_id, &path, &filename, reader, file_size,
                )
                .await?;

                if let Some(op) = blob_op {
                    let _ = dataid; // distribution kicks from on_decided now
                    blob_ops.push(op);
                }
                inodes.push(inode);
            }
            Some("folder_name") => {
                folder_name = Some(
                    part.text()
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                );
            }
            Some(_) => return Err(StatusCode::UNPROCESSABLE_ENTITY),
            None => {}
        }
    }

    // If no files were found, create a folder
    if !has_files {
        tracing::debug!("NO FILES FOUND, FOLDER CREATION");
        let folder_path = if let Some(folder_name) = folder_name {
            // FileProvider approach - concatenate parent path + folder name (same as file creation)
            tracing::debug!("FOLDER CREATION FILEPROVIDER: '{}'", &folder_name);
            path + &encrypt_part(&folder_name, &session.siv_key, &session.siv_nonce)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        } else {
            // Old API approach - path already contains the full folder path
            path
        };
        tracing::debug!("CREATING: '{}'", &folder_path);
        let folder_inode = Inode {
            id: CustomUUID::new(None),
            owner: crate::db::InodeOwner::Id(user_id),
            path: folder_path,
            inode_type: hopnet_common::InodeType::Folder,
            data_id: None,
        };
        inodes.push(folder_inode);
    }

    // Pre-generate parent folder inodes so every node gets identical folder UUIDs
    {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        crate::files::helpers::prepend_missing_parents(&tx, &mut inodes, user_id)?;
    }

    // Build upload attestation for fragments being inserted (best-effort —
    // periodic self-check reconciles if this fails)
    let attestation = if let Ok(node_id) = app_state.get_node_id() {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        match crate::files::helpers::build_upload_attestation(&app_state, &tx, node_id, &blob_ops) {
            Ok(opt) => opt,
            Err(e) => {
                tracing::warn!(
                    "Failed to build upload attestation: {}. Continuing with file insert only - periodic self-check will handle attestation",
                    e
                );
                None
            }
        }
    } else {
        None
    };

    crate::files::helpers::submit_inodes(&app_state, user_id, blob_ops, inodes, attestation).await
}

pub async fn delete_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>, // Extract user_id from JWT via auth middleware
    Query(params): Query<GetQueryParams>,
) -> Result<(), StatusCode> {
    let session = app_state.get_session(user_id).await?;
    let enc_path = encrypt_path(params.path, &session.siv_key, &session.siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Validate that files exist before submitting to consensus
    // IMPORTANT: Use a fresh transaction to avoid snapshot isolation issues
    // Transactions capture a snapshot at creation time, which may not see recently checkpointed data
    {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Quick existence check without full transaction to avoid stale snapshot
        let exists: Result<i32, _> = conn.query_row(
            "SELECT COUNT(*) FROM inodes WHERE path = ? AND owner_id = ?",
            rusqlite::params![enc_path.clone(), user_id],
            |row| row.get(0),
        );

        match exists {
            Ok(count) if count > 0 => {
                // File exists, proceed to consensus
            }
            Ok(_) => {
                return Err(StatusCode::NOT_FOUND);
            }
            Err(e) => {
                tracing::error!("Error validating file existence: {:?}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    // Create payload for consensus
    let payload = crate::files::handlers::DeleteFilesPayload {
        encrypted_path: enc_path,
        user_id,
    };

    // Serialize payload for consensus submission
    match bincode::serde::encode_to_vec(&payload, bincode::config::standard()) {
        Ok(encoded_payload) => {
            let transaction = match crate::consensus::dispatch::create_signed_user_transaction(
                &app_state,
                "delete_files".to_string(),
                encoded_payload,
                user_id,
            )
            .await
            {
                Ok(tx) => tx,
                Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            };
            // Use consensus queue to ensure distributed agreement
            match app_state.consensus_queue.submit(transaction).await {
                Ok(()) => {
                    tracing::info!(
                        "Successfully submitted file deletion to consensus for user {}",
                        user_id
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Failed to submit file deletion to consensus: {:?}", e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to serialize delete files payload: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// PATCH /files — JWT-authenticated content update for an existing file
pub async fn patch_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    mut multipart: Multipart,
) -> Result<(), StatusCode> {
    // First field must be inode_id
    let inode_id = match multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        Some(field) if field.name() == Some("inode_id") => {
            let id_str = field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?;
            CustomUUID::from_str(&id_str).map_err(|_| StatusCode::BAD_REQUEST)?
        }
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    // Validate inode exists, belongs to user, and is a file
    let conn = app_state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let inode_info = crate::db::files::get_inode_by_id(&conn, &inode_id, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if inode_info.2 != hopnet_common::InodeType::File {
        return Err(StatusCode::BAD_REQUEST);
    }
    drop(conn);

    // Second field must be file_<size>
    let field = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .ok_or(StatusCode::BAD_REQUEST)?;
    let field_name = field.name().ok_or(StatusCode::BAD_REQUEST)?.to_string();
    let file_size_str = field_name
        .strip_prefix("file_")
        .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
    let file_size = file_size_str
        .parse::<usize>()
        .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;

    let (_dataid, blob_op, incoming_share_updates) =
        crate::files::functions::prepare_content_update(
            &app_state, user_id, &inode_id, field, file_size,
        )
        .await?;

    // Build, validate, and submit ModifyItemPayload
    let payload = crate::files::handlers::ModifyItemPayload {
        user_id,
        inode_id,
        new_encrypted_path: None,
        content_update: Some(crate::files::handlers::DriveContentUpdate { blob_op }),
        incoming_share_updates,
    };

    {
        let mut conn = app_state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let db_tx = conn
            .transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        crate::db::files::modify_item(
            &db_tx,
            payload.user_id,
            payload.inode_id.clone(),
            payload.new_encrypted_path.clone(),
            payload.content_update.clone().map(|u| u.blob_op),
            None,
            &app_state.fragments_dir,
        )
        .map_err(|e| match e {
            crate::db::DatabaseError::NotFound => StatusCode::NOT_FOUND,
            crate::db::DatabaseError::ConflictError => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
        db_tx
            .rollback()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let transaction = crate::consensus::dispatch::create_signed_user_transaction(
        &app_state,
        "modify_item".to_string(),
        encoded,
        user_id,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    app_state
        .consensus_queue
        .submit(transaction)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(())
}

/// GET /fragments
/// Get count of fragments stored locally on this node
pub async fn get_fragments_count(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>, // Extract user_id from JWT via auth middleware
) -> impl IntoResponse {
    match crate::db::files::get_local_fragment_count(app_state.db_pool.get()) {
        Ok(count) => {
            #[derive(Serialize)]
            struct FragmentCountResponse {
                locally_stored_fragments: i64,
            }

            (
                StatusCode::OK,
                Json(FragmentCountResponse {
                    locally_stored_fragments: count,
                }),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to get local fragment count: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Manual trigger for orphaned data block cleanup
pub async fn post_cleanup_orphaned_data_blocks(
    State(app_state): State<AppState>,
    Query(params): Query<CleanupQueryParams>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!(
        "Manual cleanup trigger requested by user {} (batch_size: {}, retention_days: {})",
        uid,
        params.batch_size,
        params.retention_days
    );

    // Run the cleanup job directly with parameters
    match super::jobs::run_orphaned_data_block_cleanup(
        &app_state,
        params.batch_size,
        params.retention_days,
    )
    .await
    {
        Ok(data_blocks_cleaned) => {
            #[derive(Serialize)]
            struct CleanupResponse {
                status: String,
                data_blocks_cleaned: usize,
            }

            let response = CleanupResponse {
                status: "success".to_string(),
                data_blocks_cleaned,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!("Manual cleanup failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Cleanup failed: {:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// POST /maintenance/rebalance
/// Manually trigger network rebalancing to redistribute fragments to optimal nodes
pub async fn post_rebalance_network(
    State(app_state): State<AppState>,
    Query(params): Query<RebalanceQueryParams>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!(
        "Manual rebalancing trigger requested by user {} (max_data_blocks: {}, min_age_heights: {})",
        uid,
        params.max_data_blocks,
        params.min_age_heights
    );

    // Validate parameters
    if params.max_data_blocks <= 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "error",
                "error": "max_data_blocks must be positive"
            })),
        )
            .into_response();
    }

    if params.min_age_heights < 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "error",
                "error": "min_age_heights cannot be negative"
            })),
        )
            .into_response();
    }

    // Run the rebalancing job directly with parameters
    match super::jobs::run_network_rebalancing(
        &app_state,
        params.max_data_blocks,
        params.min_age_heights,
    )
    .await
    {
        Ok(result) => {
            tracing::info!("Manual rebalancing completed: {:?}", result);
            (StatusCode::OK, Json(result)).into_response()
        }
        Err(e) => {
            tracing::error!("Manual rebalancing failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Rebalancing failed: {:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// GET /diagnostics/fragment-inventory-differential
/// Returns the differential between consensus inventory and local fragments
/// Used for testing and monitoring the self-attestation system
pub async fn get_fragment_inventory_differential(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    let node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match crate::db::inventory::compute_inventory_differential(app_state.db_pool.get(), node_id) {
        Ok(differential) => {
            tracing::debug!(
                "Fragment inventory differential computed for node {}: {} added, {} removed",
                node_id,
                differential.fragments_added.len(),
                differential.fragments_removed.len()
            );
            (StatusCode::OK, Json(differential)).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to compute fragment inventory differential: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /maintenance/fragment-inventory-self-check
/// Manually trigger fragment inventory self-check and consensus submission
pub async fn post_fragment_inventory_self_check(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!(
        "Manual fragment inventory self-check triggered by user {}",
        uid
    );

    match super::jobs::run_fragment_inventory_self_check(&app_state).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::error!("Manual fragment inventory self-check failed: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct OrphanedFragmentsScanParams {
    #[serde(default = "default_grace_period_hours")]
    grace_period_hours: i64,
}

fn default_grace_period_hours() -> i64 {
    1
}

/// GET /maintenance/orphaned-fragments
/// Scan filesystem for fragments not in database (older than grace_period_hours)
/// Returns scan results and stores them for subsequent DELETE operation
pub async fn get_orphaned_fragments_scan(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
    Query(params): Query<OrphanedFragmentsScanParams>,
) -> impl IntoResponse {
    tracing::info!(
        "Orphaned fragments scan triggered by user {} (grace_period_hours: {})",
        uid,
        params.grace_period_hours
    );

    if params.grace_period_hours < 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "error",
                "error": "grace_period_hours cannot be negative"
            })),
        )
            .into_response();
    }

    match super::jobs::run_orphaned_fragments_scan(&app_state, params.grace_period_hours).await {
        Ok(scan_result) => {
            tracing::info!(
                "Scan complete: {} orphaned fragments found ({} bytes)",
                scan_result.orphaned_fragments.len(),
                scan_result.total_bytes
            );
            (StatusCode::OK, Json(scan_result)).into_response()
        }
        Err(e) => {
            tracing::error!("Orphaned fragments scan failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Scan failed: {:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// DELETE /maintenance/orphaned-fragments
/// Delete orphaned fragments based on previous scan results
/// Validates scan exists and isn't stale (> 1 hour old)
pub async fn delete_orphaned_fragments(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!("Orphaned fragments cleanup triggered by user {}", uid);

    match super::jobs::run_orphaned_fragments_cleanup(&app_state).await {
        Ok(result) => {
            tracing::info!(
                "Cleanup complete: {} deleted, {} failed, {} bytes freed",
                result.deleted_count,
                result.failed_count,
                result.bytes_freed
            );
            (StatusCode::OK, Json(result)).into_response()
        }
        Err(e) => {
            tracing::error!("Orphaned fragments cleanup failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("{:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// GET /diagnostics/file-fragments
/// Returns complete fragment distribution data for a specific file
/// Shows which nodes have which fragments according to fragment_inventory
pub async fn get_file_fragment_distribution(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(params): Query<GetQueryParams>,
) -> impl IntoResponse {
    // Encrypt path server-side (following existing pattern)
    let session = match app_state.get_session(user_id).await {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };

    let encrypted_path = match encrypt_path(params.path, &session.siv_key, &session.siv_nonce).await
    {
        Ok(path) => path,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Query database for fragment distribution
    match crate::db::debug::get_file_fragment_distribution(
        app_state.db_pool.get(),
        encrypted_path,
        user_id,
    ) {
        Ok(distribution) => {
            tracing::debug!(
                "Fragment distribution query for file {}: {} fragments ({} original, {} recovery)",
                distribution.inode_id,
                distribution.fragment_count,
                distribution.original_count,
                distribution.recovery_count
            );
            (StatusCode::OK, Json(distribution)).into_response()
        }
        Err(DatabaseError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("Failed to get file fragment distribution: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /diagnostics/network-resilience
/// Returns network-wide file resilience statistics for system overview dashboard
/// Shows distribution of files across fault tolerance levels (cliff chart data)
pub async fn get_network_resilience_stats(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<Json<hopnet_common::db::NetworkResilienceStats>, StatusCode> {
    tracing::info!("Network resilience statistics requested by user {}", uid);

    match crate::db::resilience::compute_network_resilience_stats(app_state.db_pool.get()) {
        Ok(stats) => {
            tracing::info!(
                "Network resilience computed: {} total files ({} unknown, {} unrecoverable, {} critical, {} good, {} excellent, {} exceptional) in {}ms",
                stats.total_files,
                stats.unknown.file_count,
                stats.unrecoverable.file_count,
                stats.critical.file_count,
                stats.good.file_count,
                stats.excellent.file_count,
                stats.exceptional.file_count,
                stats.computation_time_ms
            );
            Ok(Json(stats))
        }
        Err(e) => {
            tracing::error!("Failed to compute network resilience statistics: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
