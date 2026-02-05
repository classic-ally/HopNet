use axum::{
    extract::{Query, State},
    http::StatusCode,
    middleware,
    routing::get,
    Extension,
    Json,
    Router,
};
use serde::Deserialize;

use crate::auth::auth_middleware;
use crate::AppState;
use crate::db::{self, CustomUUID};
use crate::files::functions::encrypt_path;
use hopnet_common::documentprovider::DocumentProviderEnumerateResponse;

/// Build the DocumentProvider router
pub fn router(app_state: AppState) -> Router<AppState> {
    Router::new()
        .route("/enumerate", get(get_enumerate))
        .layer(middleware::from_fn_with_state(app_state, auth_middleware))
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
