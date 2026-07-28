use axum::{
    Router,
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::derive_x25519_privkey_from_user;
use hopnet_storage::crypto::StaticRecipient;

use super::dispatch_local::Submitter;
use hopnet_photos_core::dispatch::PhotoDispatch;

pub fn router<S: Clone + Send + Sync + 'static>(app_state: AppState) -> Router<S> {
    Router::new()
        .route("/photos/sidecar/status", get(get_sidecar_status))
        .route("/photos/sidecar/enable", post(post_sidecar_enable))
        .route("/photos/sidecar/disable", post(post_sidecar_disable))
        .route("/photos/sidecar/reinit", post(post_sidecar_reinit))
        .route("/photos/gallery", get(get_gallery))
        .route("/photos/{id}", get(get_photo))
        .route("/photos/recently-deleted", get(get_recently_deleted))
        .route("/photos/transaction", post(post_transaction))
        .route("/photos/sync", get(get_sync_feed))
        .with_state(app_state)
}

#[derive(Serialize)]
struct SidecarStatus {
    enabled: bool,
    cursor: Option<u64>,
    file_on_disk: bool,
}

async fn get_sidecar_status(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<Json<SidecarStatus>, StatusCode> {
    let file_on_disk = super::sidecar_db_path(uid).exists();

    let ph = &state.photos_host;
    let enabled = ph.is_enabled(uid).await;
    let cursor = if enabled {
        if let Some(db) = ph.get_db(uid).await {
            tokio::task::spawn_blocking(move || db.blocking_lock().cursor().ok())
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        } else {
            None
        }
    } else {
        None
    };
    Ok(Json(SidecarStatus {
        enabled,
        cursor,
        file_on_disk,
    }))
}

async fn post_sidecar_enable(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<StatusCode, (StatusCode, String)> {
    if state.photos_host.is_enabled(uid).await {
        return Ok(StatusCode::OK);
    }
    let session = state
        .get_session(uid)
        .await
        .map_err(|s| (s, "no session".into()))?;
    let x25519 = derive_x25519_privkey_from_user(&session.user_keys.private_key);
    let recipient = StaticRecipient(x25519);
    state
        .photos_host
        .enable(uid, recipient, state.db_pool.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(StatusCode::OK)
}

async fn post_sidecar_disable(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> StatusCode {
    state.photos_host.disable(uid).await;
    StatusCode::OK
}

async fn post_sidecar_reinit(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<StatusCode, (StatusCode, String)> {
    let session = state
        .get_session(uid)
        .await
        .map_err(|s| (s, "no session".into()))?;
    let x25519 = derive_x25519_privkey_from_user(&session.user_keys.private_key);
    let recipient = StaticRecipient(x25519);
    state
        .photos_host
        .reinit(uid, recipient, state.db_pool.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct Pagination {
    limit: Option<i64>,
    offset: Option<i64>,
}

fn clamp_page(q: &Pagination) -> (i64, i64) {
    (
        q.limit.unwrap_or(100).clamp(1, 200),
        q.offset.unwrap_or(0).max(0),
    )
}

async fn get_gallery(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<Pagination>,
) -> Result<impl IntoResponse, StatusCode> {
    let (limit, offset) = clamp_page(&q);
    let db = state
        .photos_host
        .get_db(uid)
        .await
        .ok_or(StatusCode::PRECONDITION_REQUIRED)?;
    let rows = tokio::task::spawn_blocking(move || db.blocking_lock().list_active(limit, offset))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows))
}

async fn get_photo(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Path(id): axum::extract::Path<hopnet_common::CustomUUID>,
) -> Result<Json<hopnet_photos_core::sidecar::PhotoRow>, StatusCode> {
    let db = state
        .photos_host
        .get_db(uid)
        .await
        .ok_or(StatusCode::PRECONDITION_REQUIRED)?;
    tokio::task::spawn_blocking(move || db.blocking_lock().get_photo(&id))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}

async fn get_recently_deleted(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<Pagination>,
) -> Result<impl IntoResponse, StatusCode> {
    let (limit, offset) = clamp_page(&q);
    let db = state
        .photos_host
        .get_db(uid)
        .await
        .ok_or(StatusCode::PRECONDITION_REQUIRED)?;
    let rows = tokio::task::spawn_blocking(move || {
        db.blocking_lock().list_recently_deleted(limit, offset)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows))
}

#[derive(Deserialize)]
struct TransactionBody {
    tx_type: String,
    payload: Vec<u8>,
}

async fn post_transaction(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    Json(body): Json<TransactionBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    if !hopnet_photos::handlers::USER_TX_FUNCTIONS.contains(&body.tx_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("unsupported photos tx_type: {}", body.tx_type),
        ));
    }
    let sub = Submitter::new(std::sync::Arc::new(state), uid);
    sub.submit_transaction(&body.tx_type, body.payload)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct SyncQuery {
    since: Option<u64>,
}

async fn get_sync_feed(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<SyncQuery>,
) -> Result<Json<hopnet_photos_core::dispatch::SyncBatch>, (StatusCode, String)> {
    let since = q.since.unwrap_or(0);
    let pool = state.db_pool.clone();
    tokio::task::spawn_blocking(move || super::query::read_photo_changes(&pool, uid, since))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))
}
