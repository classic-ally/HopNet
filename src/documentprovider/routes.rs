use axum::{
    body::Body,
    extract::{Multipart, Query, State},
    http::{header, StatusCode},
    middleware,
    response::Response,
    routing::{delete, get, patch, post},
    Extension,
    Json,
    Router,
};
use serde::Deserialize;

use crate::devices::auth::device_token_auth_middleware;
use crate::AppState;
use crate::db::{self, CustomUUID};
use crate::files::functions::{build_encrypted_path, encrypt_part, encrypt_path};
use hopnet_common::documentprovider::{
    DocumentProviderEnumerateResponse, DocumentProviderItem, ModifyDocumentProviderRequest,
    ModifyDocumentProviderResponse,
};
use hopnet_common::db::InodeType;

/// Build the DocumentProvider router. Reads bypass the import gate; writes
/// have the gate applied via a sub-router so attachment is explicit.
pub fn router(app_state: AppState) -> Router<AppState> {
    let reads = Router::new()
        .route("/enumerate", get(get_enumerate))
        .route("/item", get(get_item))
        .route("/download", get(get_download));

    let writes = Router::new()
        .route("/item", delete(delete_item).patch(patch_item))
        .route("/upload", post(post_upload))
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            crate::takeout::import_gate::import_gate,
        ));

    reads
        .merge(writes)
        .layer(middleware::from_fn_with_state(app_state, device_token_auth_middleware))
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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<EnumerateQuery>,
) -> Result<Json<DocumentProviderEnumerateResponse>, StatusCode> {
    // SIV keys from per-user session store
    let session = app_state.get_session(user_id).await?;

    // Get db lock once for both operations
    let db_lock = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Resolve parent_id to encrypted path
    let (encrypted_parent_path, parent_uuid) = match &query.parent_id {
        Some(id) => {
            let inode_id = CustomUUID::from_str(id)
                .map_err(|_| StatusCode::BAD_REQUEST)?;

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

    tracing::debug!("get_enumerate: user_id={} encrypted_parent_path='{}' parent_id={:?}", user_id, encrypted_parent_path, query.parent_id);

    // Get children
    let items = db::documentprovider::get_children(
        &db_lock,
        user_id,
        &encrypted_parent_path,
        &session.siv_key,
        &session.siv_nonce,
        parent_uuid,
    ).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
) -> Result<Json<DocumentProviderItem>, StatusCode> {
    let session = app_state.get_session(user_id).await?;

    let inode_id = CustomUUID::from_str(&query.id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let db_lock = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let item = db::documentprovider::get_item(
        &db_lock,
        &inode_id,
        user_id,
        &session.siv_key,
        &session.siv_nonce,
    ).map_err(|e| match e {
        db::DatabaseError::NotFound => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    Ok(Json(item))
}

/// File download endpoint for Android DocumentProvider
/// GET /integrations/documentprovider/download?id={uuid}
/// Returns streaming file content
pub async fn get_download(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
) -> Result<Response<Body>, StatusCode> {
    let session = app_state.get_session(user_id).await?;

    let inode_id = CustomUUID::from_str(&query.id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // Lightweight query - just path and type, no joins
    let (encrypted_path, item_type) = {
        let db_lock = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::documentprovider::get_download_metadata(&db_lock, &inode_id, user_id)
            .map_err(|e| match e {
                db::DatabaseError::NotFound => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
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
    let filename = crate::files::functions::decrypt_part(encrypted_filename, &session.siv_key, &session.siv_nonce)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Derive MIME type from filename
    let mime_type = mime_guess::from_path(&filename)
        .first()
        .map(|m| m.to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    // Use shared file reconstruction logic (handles empty files internally)
    let stream = crate::files::download::reconstruct_file_stream(
        &app_state,
        encrypted_path,
        user_id,
        &app_state.fragments_dir,
    )
    .await
    .map_err(|e| {
        tracing::error!("Error reconstructing file: {:?}", e);
        StatusCode::from(e)
    })?;

    // Build streaming response
    Response::builder()
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
        .header(header::CONTENT_TYPE, mime_type)
        .body(Body::from_stream(stream))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Delete file or folder endpoint for Android DocumentProvider
/// DELETE /integrations/documentprovider/item?id={uuid}
pub async fn delete_item(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
) -> Result<StatusCode, StatusCode> {
    // Parse inode_id from query
    let inode_id = CustomUUID::from_str(&query.id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // Get db connection
    let db_lock = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Look up encrypted_path by inode_id
    let encrypted_path = db::documentprovider::get_path_by_inode_id(&db_lock, &inode_id, user_id)
        .map_err(|e| match e {
            db::DatabaseError::NotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    // Drop db_lock before consensus (which may need the connection)
    drop(db_lock);

    // Build DeleteFilesPayload
    let payload = crate::files::handlers::DeleteFilesPayload {
        encrypted_path,
        user_id,
    };

    // Serialize payload
    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Create signed transaction
    let transaction = crate::consensus::functions::create_signed_user_transaction(
        &app_state,
        "delete_files".to_string(),
        encoded_payload,
        user_id,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Submit to consensus
    app_state.consensus_queue.submit(transaction).await
        .map_err(|e| {
            tracing::error!("Failed to delete item via consensus: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::NO_CONTENT)
}

/// Rename or move file/folder endpoint for Android DocumentProvider
/// PATCH /integrations/documentprovider/item
pub async fn patch_item(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(request): Json<ModifyDocumentProviderRequest>,
) -> Result<Json<ModifyDocumentProviderResponse>, StatusCode> {
    tracing::debug!("patch_item: request={:?} user_id={}", request, user_id);

    let session = app_state.get_session(user_id).await?;

    // Parse inode_id from request
    let inode_id = CustomUUID::from_str(&request.id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // Get db connection
    let db_lock = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Get current encrypted_path
    let current_path = db::documentprovider::get_path_by_inode_id(&db_lock, &inode_id, user_id)
        .map_err(|e| {
            tracing::debug!("patch_item: failed to get current path for inode_id={}: {:?}", inode_id, e);
            match e {
                db::DatabaseError::NotFound => StatusCode::NOT_FOUND,
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
        tracing::debug!("patch_item: MOVE operation, new_parent_id={}", new_parent_id);
        let new_parent_path = if new_parent_id == "root" {
            // Moving to root
            "".to_string()
        } else {
            let parent_inode_id = CustomUUID::from_str(new_parent_id)
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            tracing::debug!("patch_item: looking up parent path for inode_id={}", parent_inode_id);
            db::documentprovider::get_path_by_inode_id(&db_lock, &parent_inode_id, user_id)
                .map_err(|e| {
                    tracing::debug!("patch_item: failed to get parent path: {:?}", e);
                    match e {
                        db::DatabaseError::NotFound => StatusCode::NOT_FOUND,
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
    let payload = crate::files::handlers::ModifyItemPayload {
        user_id,
        inode_id: inode_id.clone(),
        new_encrypted_path: Some(new_encrypted_path),
        new_data_block_id: None,
        new_data_record: None,
        incoming_share_updates: None,
    };

    // Serialize payload
    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Create signed transaction
    let transaction = crate::consensus::functions::create_signed_user_transaction(
        &app_state,
        "modify_item".to_string(),
        encoded_payload,
        user_id,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Submit to consensus
    app_state.consensus_queue.submit(transaction).await
        .map_err(|e| {
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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    multipart: Multipart,
) -> Result<StatusCode, StatusCode> {
    // Forward to post_files which handles the multipart processing
    // The DocumentProvider uses parent_item_identifier format which post_files already supports
    crate::files::routes::post_files(
        State(app_state),
        Extension(user_id),
        multipart,
    ).await?;

    Ok(StatusCode::CREATED)
}
