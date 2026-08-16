//! /integrations/documentprovider routes — Android DocumentProvider surface.
//! Moved from the host's `documentprovider::routes` (RFC-015 Stage D4); the
//! host layers the device-token auth middleware around this router.

use axum::{
    body::Body,
    extract::{Multipart, Query, State},
    http::{header, StatusCode},
    response::Response,
    routing::{delete, get, post},
    Extension, Json, Router,
};
use serde::Deserialize;
use std::str::FromStr;

use crate::db;
use crate::host::{DriveState, TxSigner, TxSpec};
use crate::paths::{build_encrypted_path, decrypt_part, encrypt_part, encrypt_path};
use crate::upload::session_or_status;
use hopnet_common::db::InodeType;
use hopnet_common::documentprovider::{
    DocumentProviderEnumerateResponse, DocumentProviderItem, ModifyDocumentProviderRequest,
    ModifyDocumentProviderResponse,
};
use hopnet_common::CustomUUID;
use hopnet_projection::DatabaseError;

/// Build the DocumentProvider router. Reads bypass the import gate; writes
/// have the gate applied via a sub-router so attachment is explicit. The
/// host wraps the whole router in its device-token auth middleware.
pub fn router<S: Clone + Send + Sync + 'static>(state: DriveState) -> Router<S> {
    let reads = Router::new()
        .route("/enumerate", get(get_enumerate))
        .route("/item", get(get_item))
        .route("/download", get(get_download));

    let writes = Router::new()
        .route("/item", delete(delete_item).patch(patch_item))
        .route("/upload", post(post_upload))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            super::write_gate,
        ));

    reads.merge(writes).with_state(state)
}

/// Query parameters for enumerate endpoint
#[derive(Debug, Deserialize)]
pub struct EnumerateQuery {
    /// Parent folder UUID. If omitted, returns root children.
    pub parent_id: Option<String>,
}

/// Directory enumeration endpoint for Android DocumentProvider
/// GET /integrations/documentprovider/enumerate
/// GET /integrations/documentprovider/enumerate?parent_id={uuid}
pub async fn get_enumerate(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<EnumerateQuery>,
) -> Result<Json<DocumentProviderEnumerateResponse>, StatusCode> {
    // SIV keys from per-user session store
    let session = session_or_status(&state, user_id).await?;

    // Get db lock once for both operations
    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Resolve parent_id to encrypted path
    let (encrypted_parent_path, parent_uuid) = match &query.parent_id {
        Some(id) => {
            let inode_id = CustomUUID::from_str(id).map_err(|_| StatusCode::BAD_REQUEST)?;

            let path = db::documentprovider::get_path_by_inode_id(&db_lock, &inode_id, user_id)
                .map_err(|_| StatusCode::NOT_FOUND)?;

            (path, Some(inode_id))
        }
        None => {
            // Root
            let path = encrypt_path("/".to_string(), &session.siv_key, &session.siv_nonce)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            (path, None)
        }
    };

    tracing::debug!(
        "get_enumerate: user_id={} encrypted_parent_path='{}' parent_id={:?}",
        user_id,
        encrypted_parent_path,
        query.parent_id
    );

    // Get children
    let items = db::documentprovider::get_children(
        &db_lock,
        user_id,
        &encrypted_parent_path,
        &session.siv_key,
        &session.siv_nonce,
        parent_uuid,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tracing::debug!("get_enumerate: found {} items", items.len());
    Ok(Json(DocumentProviderEnumerateResponse { items }))
}

/// Query parameters for item endpoint
#[derive(Debug, Deserialize)]
pub struct ItemQuery {
    /// Document UUID
    pub id: String,
}

/// Single item metadata endpoint for Android DocumentProvider
/// GET /integrations/documentprovider/item?id={uuid}
pub async fn get_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
) -> Result<Json<DocumentProviderItem>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;

    let inode_id = CustomUUID::from_str(&query.id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let item = db::documentprovider::get_item(
        &db_lock,
        &inode_id,
        user_id,
        &session.siv_key,
        &session.siv_nonce,
    )
    .map_err(|e| match e {
        DatabaseError::NotFound => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    Ok(Json(item))
}

/// File download endpoint for Android DocumentProvider
/// GET /integrations/documentprovider/download?id={uuid}
/// Returns streaming file content; honors single `bytes=start-[end]`
/// Range headers (206/416) so the Hop Drive proxy file descriptor can
/// seek without re-downloading the whole file.
pub async fn get_download(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
    headers: axum::http::HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;

    let inode_id = CustomUUID::from_str(&query.id).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Lightweight query - just path and type, no joins
    let (encrypted_path, item_type) = {
        let db_lock = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::documentprovider::get_download_metadata(&db_lock, &inode_id, user_id).map_err(|e| {
            match e {
                DatabaseError::NotFound => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            }
        })?
    };

    // Only allow downloads for files, not folders
    if item_type == InodeType::Folder {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Extract and decrypt only the filename segment (more efficient than decrypt_path)
    let encrypted_filename = encrypted_path
        .rsplit('/')
        .next()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let filename = decrypt_part(encrypted_filename, &session.siv_key, &session.siv_nonce)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Derive MIME type from filename
    let mime_type = mime_guess::from_path(&filename)
        .first()
        .map(|m| m.to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let requested_range = super::parse_range(&headers);

    // Range-aware reconstruction (handles empty files internally)
    let download_info = match crate::download::reconstruct_file_range(
        &state,
        encrypted_path,
        user_id,
        requested_range,
    )
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
            tracing::error!("Error reconstructing file: {:?}", e);
            return Err(StatusCode::from(e));
        }
    };

    // Build streaming response
    let mut builder = Response::builder()
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .header(header::CONTENT_TYPE, mime_type)
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

/// Delete file or folder endpoint for Android DocumentProvider
/// DELETE /integrations/documentprovider/item?id={uuid}
pub async fn delete_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
) -> Result<StatusCode, StatusCode> {
    // Parse inode_id from query
    let inode_id = CustomUUID::from_str(&query.id).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Get db connection
    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Look up encrypted_path by inode_id
    let encrypted_path = db::documentprovider::get_path_by_inode_id(&db_lock, &inode_id, user_id)
        .map_err(|e| match e {
        DatabaseError::NotFound => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    // Drop db_lock before consensus (which may need the connection)
    drop(db_lock);

    // Build DeleteFilesPayload
    let payload = crate::envelopes::DeleteFilesPayload {
        encrypted_path,
        user_id,
    };

    // Serialize payload
    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Sign (host-side) and submit to consensus
    state
        .txs
        .submit(TxSpec {
            function: "delete_files",
            payload: encoded_payload,
            signer: TxSigner::User(user_id),
        })
        .await
        .map_err(|e| {
            if matches!(e, crate::host::TxSubmitError::Signing) {
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            tracing::error!("Failed to delete item via consensus: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::NO_CONTENT)
}

/// Rename or move file/folder endpoint for Android DocumentProvider
/// PATCH /integrations/documentprovider/item
pub async fn patch_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Json(request): Json<ModifyDocumentProviderRequest>,
) -> Result<Json<ModifyDocumentProviderResponse>, StatusCode> {
    tracing::debug!("patch_item: request={:?} user_id={}", request, user_id);

    let session = session_or_status(&state, user_id).await?;

    // Parse inode_id from request
    let inode_id = CustomUUID::from_str(&request.id).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Get db connection
    let db_lock = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Get current encrypted_path
    let current_path = db::documentprovider::get_path_by_inode_id(&db_lock, &inode_id, user_id)
        .map_err(|e| {
            tracing::debug!(
                "patch_item: failed to get current path for inode_id={}: {:?}",
                inode_id,
                e
            );
            match e {
                DatabaseError::NotFound => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            }
        })?;

    // Construct new_encrypted_path based on operation
    let new_encrypted_path = if let Some(ref new_name) = request.name {
        // RENAME: Keep parent, change filename
        let parent_path = if let Some(last_slash) = current_path.rfind('/') {
            &current_path[..last_slash]
        } else {
            ""
        };
        let encrypted_name = encrypt_part(new_name, &session.siv_key, &session.siv_nonce)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        build_encrypted_path(parent_path, &encrypted_name)
    } else if let Some(ref new_parent_id) = request.parent_id {
        // MOVE: Change parent, keep filename
        tracing::debug!(
            "patch_item: MOVE operation, new_parent_id={}",
            new_parent_id
        );
        let new_parent_path = if new_parent_id == "root" {
            // Moving to root
            "".to_string()
        } else {
            let parent_inode_id =
                CustomUUID::from_str(new_parent_id).map_err(|_| StatusCode::BAD_REQUEST)?;
            tracing::debug!(
                "patch_item: looking up parent path for inode_id={}",
                parent_inode_id
            );
            db::documentprovider::get_path_by_inode_id(&db_lock, &parent_inode_id, user_id)
                .map_err(|e| {
                    tracing::debug!("patch_item: failed to get parent path: {:?}", e);
                    match e {
                        DatabaseError::NotFound => StatusCode::NOT_FOUND,
                        _ => StatusCode::INTERNAL_SERVER_ERROR,
                    }
                })?
        };
        // Extract filename from current path (without leading slash)
        let filename = if let Some(last_slash) = current_path.rfind('/') {
            &current_path[last_slash + 1..]
        } else {
            &current_path
        };
        build_encrypted_path(&new_parent_path, filename)
    } else {
        // No operation specified
        return Err(StatusCode::BAD_REQUEST);
    };

    // Drop db_lock before consensus
    drop(db_lock);

    // Build ModifyItemPayload
    let payload = crate::envelopes::ModifyItemPayload {
        user_id,
        inode_id: inode_id.clone(),
        new_encrypted_path: Some(new_encrypted_path),
        content_update: None,
        incoming_share_updates: None,
    };

    // Serialize payload
    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Sign (host-side) and submit to consensus
    state
        .txs
        .submit(TxSpec {
            function: "modify_item",
            payload: encoded_payload,
            signer: TxSigner::User(user_id),
        })
        .await
        .map_err(|e| {
            if matches!(e, crate::host::TxSubmitError::Signing) {
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            tracing::error!("Failed to modify item via consensus: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(ModifyDocumentProviderResponse {
        new_identifier: inode_id.to_string(),
    }))
}

/// Upload file endpoint for Android DocumentProvider
/// POST /integrations/documentprovider/upload
/// Accepts multipart form with parent_item_identifier and file
/// Returns CREATED on success - client should call enumerate to get item metadata
pub async fn post_upload(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    multipart: Multipart,
) -> Result<StatusCode, StatusCode> {
    // Forward to post_files which handles the multipart processing
    // The DocumentProvider uses parent_item_identifier format which post_files already supports
    super::files::post_files(State(state), Extension(user_id), multipart).await?;

    Ok(StatusCode::CREATED)
}
