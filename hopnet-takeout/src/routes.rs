//! Takeout HTTP surface (RFC-015 Stage D5b). Moved from the host's
//! `takeout::routes` behind [`TakeoutState`]; route paths, status codes, and
//! response shapes are preserved EXACTLY. The host mounts these routers and
//! layers its JWT auth middleware around them.

use crate::TakeoutState;
use crate::db::takeout::{TakeoutPayload, TakeoutStatusPayload};
use crate::export::execute_takeout_materialization;
use axum::{
    Router,
    body::Body,
    extract::{Extension, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{delete, get, post},
};
use chrono::{Duration, Utc};
use hopnet_common::{CustomUUID, TakeoutRecord, TakeoutStatus};
use hopnet_projection::CustomDateTime;
use hopnet_projection::host::{TxSigner, TxSpec};
use std::str::FromStr;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

/// The `/takeout` router (host nests it under its auth middleware).
pub fn router<S: Clone + Send + Sync + 'static>(state: TakeoutState) -> Router<S> {
    Router::new()
        .route("/", get(get_takeouts))
        .route("/can-create", get(get_can_create_takeout))
        .route("/initiate", post(post_initiate_takeout))
        .route("/{id}", delete(delete_takeout))
        .route("/{id}/process", post(post_process_takeout))
        .route("/{id}/download", get(get_download_takeout))
        .nest("/import", crate::import::import_routes())
        .with_state(state)
}

/// The `/maintenance/takeout` router (host merges it into its protected
/// routes — path preserved exactly).
pub fn maintenance_router<S: Clone + Send + Sync + 'static>(state: TakeoutState) -> Router<S> {
    Router::new()
        .route("/maintenance/takeout", post(post_takeout_maintenance))
        .with_state(state)
}

/// GET /takeout - Get all takeouts for the authenticated user
async fn get_takeouts(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<Vec<TakeoutRecord>>, StatusCode> {
    match crate::db::takeout::get_takeouts_by_user(state.db_pool.get(), user_id) {
        Ok(takeouts) => Ok(Json(takeouts)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// GET /takeout/can-create - Check if user can create a new takeout
async fn get_can_create_takeout(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Check if user already has an active takeout (same logic as creation endpoint)
    let can_create =
        match crate::db::takeout::has_active_takeout(state.db_pool.get(), Some(user_id)) {
            Ok(true) => false, // Has active takeout, cannot create
            Ok(false) => true, // No active takeout, can create
            Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        };

    Ok(Json(serde_json::json!({
        "can_create": can_create
    })))
}

/// POST /takeout/initiate - Initiate a new takeout for the authenticated user
async fn post_initiate_takeout(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
) -> StatusCode {
    // Check if user already has an active takeout (rate limiting)
    match crate::db::takeout::has_active_takeout(state.db_pool.get(), Some(user_id)) {
        Ok(true) => return StatusCode::TOO_MANY_REQUESTS,
        Ok(false) => {} // Good to proceed
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    }

    // Get node ID for storage validation
    let node_id = match state.node_id() {
        Some(id) => id,
        None => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Calculate user's total data size — host hook (projection sizing SQL
    // stays host-side; a future pass may fold it into the exporter contract).
    let user_data_size = match state.hooks.user_data_size_bytes(user_id).await {
        Ok(size) => size,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Check if node has enough storage (3x user data size safety factor)
    // Accounts for: fragments + reconstructed files + compressed archive
    let required_storage = user_data_size * 3;
    match state.hooks.node_available_storage_bytes().await {
        Ok(Some(available)) => {
            if available < required_storage {
                tracing::warn!(
                    "Insufficient storage for takeout: required {} bytes, available {} bytes",
                    required_storage,
                    available
                );
                return StatusCode::INSUFFICIENT_STORAGE;
            }
        }
        Ok(None) => {
            tracing::error!(
                "Failed to determine storage availability for node {}",
                node_id
            );
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    }

    // Build takeout payload for consensus submission
    let consensus_height = match state.db_pool.get() {
        Ok(conn) => match crate::db::current_height(&conn) {
            Ok(height) => height,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        },
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    let takeout_id = CustomUUID::new(None);
    let created_at = Utc::now();
    let expires_at = created_at + Duration::hours(24);

    let takeout_payload = TakeoutPayload {
        takeout_id: takeout_id.clone(),
        user_id,
        owner_node_id: node_id,
        status: TakeoutStatus::Pending,
        expires_at: CustomDateTime::new(expires_at),
        consensus_height,
    };

    // Serialize payload for consensus
    let encoded_payload =
        match bincode::serde::encode_to_vec(&takeout_payload, bincode::config::standard()) {
            Ok(data) => data,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        };

    // Sign (user) and submit to consensus via the gateway.
    match state
        .txs
        .submit(TxSpec {
            function: "create_takeout",
            payload: encoded_payload,
            signer: TxSigner::User(user_id),
        })
        .await
    {
        Ok(()) => {
            tracing::info!(
                "Initiated takeout {} for user {} via consensus ({} bytes of data)",
                takeout_id,
                user_id,
                user_data_size
            );
            StatusCode::CREATED
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// DELETE /takeout/{id} - Delete/cancel a takeout
async fn delete_takeout(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
    Path(takeout_id_str): Path<String>,
) -> StatusCode {
    // Parse takeout ID
    let takeout_id = match CustomUUID::from_str(&takeout_id_str) {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("Invalid takeout ID format: {}", takeout_id_str);
            return StatusCode::BAD_REQUEST;
        }
    };

    // Get the specific takeout to verify ownership and current status
    let takeout = match crate::db::takeout::get_takeout_by_id(state.db_pool.get(), &takeout_id) {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::error!("Takeout {} not found", takeout_id);
            return StatusCode::NOT_FOUND;
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Verify the takeout belongs to the authenticated user
    if takeout.user_id != user_id {
        tracing::error!("Takeout {} does not belong to user {}", takeout_id, user_id);
        return StatusCode::FORBIDDEN;
    }

    // Check if takeout can be cancelled (not already in terminal state)
    if matches!(
        takeout.status,
        TakeoutStatus::Expired | TakeoutStatus::Cancelled
    ) {
        tracing::info!(
            "Takeout {} is already in terminal status {:?}",
            takeout_id,
            takeout.status
        );
        return StatusCode::OK; // Already deleted/expired
    }

    // Update status to Cancelled via consensus (this will trigger automatic cleanup)
    let status_payload = TakeoutStatusPayload {
        takeout_id: takeout_id.clone(),
        new_status: TakeoutStatus::Cancelled,
    };

    let encoded_payload =
        match bincode::serde::encode_to_vec(&status_payload, bincode::config::standard()) {
            Ok(data) => data,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        };

    // Node-signed, exactly what the pre-split cancel path signed with.
    match state
        .txs
        .submit(TxSpec {
            function: "update_takeout_status",
            payload: encoded_payload,
            signer: TxSigner::Node,
        })
        .await
    {
        Ok(_) => {
            tracing::info!("Takeout {} cancelled by user {}", takeout_id, user_id);
            StatusCode::OK
        }
        Err(_) => {
            tracing::error!("Failed to cancel takeout {} via consensus", takeout_id);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// POST /takeout/{id}/process - Manually trigger takeout processing on this node
async fn post_process_takeout(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
    Path(takeout_id_str): Path<String>,
) -> StatusCode {
    // Parse takeout ID
    let takeout_id = match CustomUUID::from_str(&takeout_id_str) {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("Invalid takeout ID format: {}", takeout_id_str);
            return StatusCode::BAD_REQUEST;
        }
    };

    // Get current node ID to verify ownership
    let current_node_id = match state.node_id() {
        Some(id) => id,
        None => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Get the specific takeout
    let takeout = match crate::db::takeout::get_takeout_by_id(state.db_pool.get(), &takeout_id) {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::error!("Takeout {} not found", takeout_id);
            return StatusCode::NOT_FOUND;
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Verify this node owns the takeout
    if takeout.owner_node_id != current_node_id {
        tracing::error!(
            "Takeout {} is not owned by this node (owner: {})",
            takeout_id,
            takeout.owner_node_id
        );
        return StatusCode::FORBIDDEN;
    }

    // Verify the takeout belongs to the authenticated user
    if takeout.user_id != user_id {
        tracing::error!("Takeout {} does not belong to user {}", takeout_id, user_id);
        return StatusCode::FORBIDDEN;
    }

    // Check takeout is in pending status
    if takeout.status != TakeoutStatus::Pending {
        tracing::error!(
            "Takeout {} is not in pending status (current: {:?})",
            takeout_id,
            takeout.status
        );
        return StatusCode::CONFLICT;
    }

    // Use the shared materialization logic
    match execute_takeout_materialization(&state, &takeout_id, user_id).await {
        Ok(_) => {
            tracing::info!(
                "Manual takeout processing completed successfully for {}",
                takeout_id
            );
            StatusCode::OK
        }
        Err(e) => {
            tracing::error!("Failed to process takeout {}: {}", takeout_id, e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// GET /takeout/{id}/download - Download the completed takeout archive
async fn get_download_takeout(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
    Path(takeout_id_str): Path<String>,
) -> Result<Response, StatusCode> {
    // Parse takeout ID
    let takeout_id = match CustomUUID::from_str(&takeout_id_str) {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("Invalid takeout ID format: {}", takeout_id_str);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    // Get the specific takeout
    let takeout = match crate::db::takeout::get_takeout_by_id(state.db_pool.get(), &takeout_id) {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::error!("Takeout {} not found", takeout_id);
            return Err(StatusCode::NOT_FOUND);
        }
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Verify the takeout belongs to the authenticated user
    if takeout.user_id != user_id {
        tracing::error!("Takeout {} does not belong to user {}", takeout_id, user_id);
        return Err(StatusCode::FORBIDDEN);
    }

    // Check takeout is in ready status
    if takeout.status != TakeoutStatus::Ready {
        tracing::error!(
            "Takeout {} is not ready for download (current: {:?})",
            takeout_id,
            takeout.status
        );
        return Err(StatusCode::CONFLICT);
    }

    // Construct archive file path
    let archive_path = format!(
        "{}/takeouts/{}.tar.gz",
        state.fragments_dir,
        takeout_id.simple()
    );

    // Check if archive file exists
    if tokio::fs::metadata(&archive_path).await.is_err() {
        tracing::error!("Archive file not found: {}", archive_path);
        return Err(StatusCode::NOT_FOUND);
    }

    // Open the file for streaming
    let file = match File::open(&archive_path).await {
        Ok(file) => file,
        Err(e) => {
            tracing::error!("Failed to open archive file {}: {:?}", archive_path, e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Get file size for Content-Length header
    let file_size = match file.metadata().await {
        Ok(metadata) => metadata.len(),
        Err(e) => {
            tracing::error!("Failed to get file metadata for {}: {:?}", archive_path, e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Create headers
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/gzip"));
    headers.insert(
        "content-length",
        HeaderValue::from_str(&file_size.to_string()).unwrap(),
    );
    headers.insert(
        "content-disposition",
        HeaderValue::from_str(&format!(
            "attachment; filename=\"takeout-{}.tar.gz\"",
            takeout_id.simple()
        ))
        .unwrap(),
    );

    // Create streaming response
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    tracing::info!(
        "Serving takeout download: {} ({} bytes) to user {}",
        takeout_id,
        file_size,
        user_id
    );

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .unwrap();

    // Add headers to response
    *response.headers_mut() = headers;

    Ok(response)
}

/// POST /maintenance/takeout
/// Manual trigger for takeout maintenance (expiration checking and cleanup)
async fn post_takeout_maintenance(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
) -> impl axum::response::IntoResponse {
    tracing::info!(
        "Manual takeout maintenance trigger requested by user {}",
        user_id
    );

    // Call the takeout maintenance job directly
    match crate::jobs::run_takeout_maintenance(&state).await {
        Ok(_) => {
            tracing::info!("Manual takeout maintenance completed successfully");
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "success",
                    "message": "Takeout maintenance completed successfully"
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Manual takeout maintenance failed: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "status": "error",
                    "message": format!("Takeout maintenance failed: {}", e)
                })),
            )
                .into_response()
        }
    }
}
