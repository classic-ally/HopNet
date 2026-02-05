use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    middleware,
    response::Response,
    routing::get,
    Extension,
    Json,
    Router,
};
use serde::Deserialize;

use crate::devices::auth::device_token_auth_middleware;
use crate::AppState;
use crate::db::{self, CustomUUID};
use crate::files::functions::encrypt_path;
use hopnet_common::documentprovider::{DocumentProviderEnumerateResponse, DocumentProviderItem};
use hopnet_common::db::InodeType;

/// Build the DocumentProvider router
pub fn router(app_state: AppState) -> Router<AppState> {
    Router::new()
        .route("/enumerate", get(get_enumerate))
        .route("/item", get(get_item))
        .route("/download", get(get_download))
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
    // SIV keys derived from user's private key (currently requires server to have keys loaded)
    // TODO: For true multi-user thin client, need per-user key derivation
    let siv_key = app_state.get_siv_key()?;
    let siv_nonce = app_state.get_siv_nonce()?;

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
            let path = encrypt_path("/".to_string(), siv_key, siv_nonce)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            (path, None)
        }
    };

    // Get children
    let items = db::documentprovider::get_children(
        &db_lock,
        user_id,
        &encrypted_parent_path,
        siv_key,
        siv_nonce,
        parent_uuid,
    ).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
    let siv_key = app_state.get_siv_key()?;
    let siv_nonce = app_state.get_siv_nonce()?;

    let inode_id = CustomUUID::from_str(&query.id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let db_lock = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let item = db::documentprovider::get_item(
        &db_lock,
        &inode_id,
        user_id,
        siv_key,
        siv_nonce,
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
    let siv_key = app_state.get_siv_key()?;
    let siv_nonce = app_state.get_siv_nonce()?;

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
    let filename = crate::files::functions::decrypt_part(encrypted_filename, siv_key, siv_nonce)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Derive MIME type from filename
    let mime_type = mime_guess::from_path(&filename)
        .first()
        .map(|m| m.to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    // Use shared file reconstruction logic (handles empty files internally)
    let stream = crate::files::download::reconstruct_file_for_user(
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
