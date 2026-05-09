use axum::{
    extract::{Extension, State, Path},
    response::{Json, Response, IntoResponse},
    http::{StatusCode, HeaderMap, HeaderValue},
    routing::{get, post, delete},
    Router,
    body::Body,
};
use crate::{AppState, auth};
use hopnet_common::{TakeoutRecord, TakeoutStatus};
use crate::consensus::types::Transaction;
use crate::db::{takeout::{TakeoutPayload, TakeoutStatusPayload}, CustomDateTime, CustomUUID, consensus};
use chrono::{Duration, Utc};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

pub fn takeout_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(get_takeouts))
        .route("/can-create", get(get_can_create_takeout))
        .route("/initiate", post(post_initiate_takeout))
        .route("/{id}", delete(delete_takeout))
        .route("/{id}/process", post(post_process_takeout))
        .route("/{id}/download", get(get_download_takeout))
        .nest("/import", crate::takeout::import::import_routes())
}

/// GET /takeout - Get all takeouts for the authenticated user
async fn get_takeouts(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<Vec<TakeoutRecord>>, StatusCode> {
    match crate::db::takeout::get_takeouts_by_user(app_state.db_pool.get(), user_id) {
        Ok(takeouts) => Ok(Json(takeouts)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// GET /takeout/can-create - Check if user can create a new takeout
async fn get_can_create_takeout(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Check if user already has an active takeout (same logic as creation endpoint)
    let can_create = match crate::db::takeout::has_active_takeout(app_state.db_pool.get(), Some(user_id)) {
        Ok(true) => false,  // Has active takeout, cannot create
        Ok(false) => true,  // No active takeout, can create
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    Ok(Json(serde_json::json!({
        "can_create": can_create
    })))
}

/// POST /takeout/initiate - Initiate a new takeout for the authenticated user
async fn post_initiate_takeout(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> StatusCode {
    // Check if user already has an active takeout (rate limiting)
    match crate::db::takeout::has_active_takeout(app_state.db_pool.get(), Some(user_id)) {
        Ok(true) => return StatusCode::TOO_MANY_REQUESTS,
        Ok(false) => {}, // Good to proceed
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    }
    
    // Get node ID for storage validation
    let node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    
    // Calculate user's total data size
    let user_data_size = match crate::db::takeout::calculate_user_data_size(app_state.db_pool.get(), user_id) {
        Ok(size) => size,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    
    // Check if node has enough storage (3x user data size safety factor)
    // Accounts for: fragments + reconstructed files + compressed archive
    let required_storage = user_data_size * 3;
    match crate::db::takeout::get_node_available_storage(app_state.db_pool.get(), &app_state, node_id).await {
        Ok(Some(available)) => {
            if available < required_storage {
                tracing::warn!(
                    "Insufficient storage for takeout: required {} bytes, available {} bytes",
                    required_storage, available
                );
                return StatusCode::INSUFFICIENT_STORAGE;
            }
        }
        Ok(None) => {
            tracing::error!("Failed to determine storage availability for node {}", node_id);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    }
    
    // Build takeout payload for consensus submission
    let consensus_height = match app_state.db_pool.get() {
        Ok(mut conn) => {
            let tx = match conn.transaction() {
                Ok(tx) => tx,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
            };
            match consensus::get_current_consensus_height(&tx) {
                Ok(height) => height,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
            }
        }
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
    let encoded_payload = match bincode::serde::encode_to_vec(&takeout_payload, bincode::config::standard()) {
        Ok(data) => data,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    
    // Create consensus transaction
    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state,
        "create_takeout".to_string(),
        encoded_payload,
        user_id,
    ).await {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    
    // Submit to consensus
    match app_state.consensus_queue.submit(transaction).await {
        Ok(()) => {
            tracing::info!(
                "Initiated takeout {} for user {} via consensus ({} bytes of data)",
                takeout_id, user_id, user_data_size
            );
            StatusCode::CREATED
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// DELETE /takeout/{id} - Delete/cancel a takeout
async fn delete_takeout(
    State(app_state): State<AppState>,
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
    let takeout = match crate::db::takeout::get_takeout_by_id(app_state.db_pool.get(), &takeout_id) {
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
    if matches!(takeout.status, TakeoutStatus::Expired | TakeoutStatus::Cancelled) {
        tracing::info!("Takeout {} is already in terminal status {:?}", takeout_id, takeout.status);
        return StatusCode::OK; // Already deleted/expired
    }

    // Update status to Cancelled via consensus (this will trigger automatic cleanup)
    let status_payload = TakeoutStatusPayload {
        takeout_id: takeout_id.clone(),
        new_status: TakeoutStatus::Cancelled,
    };

    let encoded_payload = match bincode::serde::encode_to_vec(&status_payload, bincode::config::standard()) {
        Ok(data) => data,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    let transaction = match crate::consensus::functions::create_signed_transaction(
        &app_state,
        "update_takeout_status".to_string(),
        encoded_payload,
    ) {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Submit cancellation to consensus
    match app_state.consensus_queue.submit(transaction).await {
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
    State(app_state): State<AppState>,
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
    let current_node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    
    // Get the specific takeout
    let takeout = match crate::db::takeout::get_takeout_by_id(app_state.db_pool.get(), &takeout_id) {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::error!("Takeout {} not found", takeout_id);
            return StatusCode::NOT_FOUND;
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    
    // Verify this node owns the takeout
    if takeout.owner_node_id != current_node_id {
        tracing::error!("Takeout {} is not owned by this node (owner: {})", takeout_id, takeout.owner_node_id);
        return StatusCode::FORBIDDEN;
    }
    
    // Verify the takeout belongs to the authenticated user
    if takeout.user_id != user_id {
        tracing::error!("Takeout {} does not belong to user {}", takeout_id, user_id);
        return StatusCode::FORBIDDEN;
    }
    
    // Check takeout is in pending status
    if takeout.status != TakeoutStatus::Pending {
        tracing::error!("Takeout {} is not in pending status (current: {:?})", takeout_id, takeout.status);
        return StatusCode::CONFLICT;
    }
    
    // Use the shared materialization logic
    match execute_takeout_materialization(&app_state, &takeout_id, user_id).await {
        Ok(_) => {
            tracing::info!("Manual takeout processing completed successfully for {}", takeout_id);
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
    State(app_state): State<AppState>,
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
    let takeout = match crate::db::takeout::get_takeout_by_id(app_state.db_pool.get(), &takeout_id) {
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
        tracing::error!("Takeout {} is not ready for download (current: {:?})", takeout_id, takeout.status);
        return Err(StatusCode::CONFLICT);
    }

    // Construct archive file path
    let archive_path = format!("{}/takeouts/{}.tar.gz",
        app_state.fragments_dir, takeout_id.simple());

    // Check if archive file exists
    if !tokio::fs::metadata(&archive_path).await.is_ok() {
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
    headers.insert("content-length", HeaderValue::from_str(&file_size.to_string()).unwrap());
    headers.insert(
        "content-disposition",
        HeaderValue::from_str(&format!("attachment; filename=\"takeout-{}.tar.gz\"", takeout_id.simple())).unwrap()
    );

    // Create streaming response
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    tracing::info!("Serving takeout download: {} ({} bytes) to user {}", takeout_id, file_size, user_id);

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .unwrap();

    // Add headers to response
    *response.headers_mut() = headers;

    Ok(response)
}

/// Execute the complete takeout materialization process
/// This is the core logic shared between manual processing and automatic processing
pub async fn execute_takeout_materialization(
    app_state: &AppState,
    takeout_id: &CustomUUID,
    user_id: i32,
) -> Result<(), TakeoutMaterializationError> {
    // Update status to materializing via consensus
    let status_payload = TakeoutStatusPayload {
        takeout_id: takeout_id.clone(),
        new_status: TakeoutStatus::Materializing,
    };

    let encoded_payload = bincode::serde::encode_to_vec(&status_payload, bincode::config::standard())
        .map_err(|e| TakeoutMaterializationError::Serialization(format!("Failed to encode status: {:?}", e)))?;

    let transaction = match crate::consensus::functions::create_signed_transaction(
        &app_state,
        "update_takeout_status".to_string(),
        encoded_payload,
    ) {
        Ok(tx) => tx,
        Err(_) => return Err(TakeoutMaterializationError::Consensus("Failed to sign transaction".to_string())),
    };

    // Submit status update to consensus
    app_state.consensus_queue.submit(transaction).await
        .map_err(|e| TakeoutMaterializationError::Consensus(format!("Failed to update status: {:?}", e)))?;

    // Resolve session for SIV keys (needed by sync folder/archive functions)
    let session = app_state.get_session(user_id).await
        .map_err(|_| TakeoutMaterializationError::Consensus("Session not found for user".to_string()))?;

    // Reserve a coordinator connection for the rest of the takeout pipeline.
    // Mirrors the consensus_queue::batch_processor pattern. Sequential phases
    // (folder materialize, archive entries query, manifest build, per-file
    // status updates) all use this conn, guaranteeing the orchestrator never
    // fails mid-takeout due to pool contention from other routes.
    let mut reserved_conn = app_state.db_pool.get()
        .map_err(|e| TakeoutMaterializationError::Database(crate::db::DatabaseError::LockError))
        .map_err(|e| {
            tracing::error!("Failed to acquire reserved coordinator conn for takeout {}: {:?}", takeout_id, e);
            e
        })?;

    // Start folder materialization
    let folder_result = crate::db::takeout::materialize_folders(
        &mut reserved_conn,
        takeout_id,
        &app_state.fragments_dir,
        &session.siv_key,
        &session.siv_nonce,
    ).map_err(|e| TakeoutMaterializationError::Database(e))?;

    tracing::info!(
        "Folder materialization for takeout {} completed: {} succeeded, {} failed",
        takeout_id, folder_result.0, folder_result.1
    );

    // Start file materialization (streaming pipeline; status writes use reserved_conn)
    let file_result = crate::db::takeout::materialize_all_files(
        app_state,
        &mut reserved_conn,
        takeout_id,
        &app_state.fragments_dir,
        user_id,
    ).await.map_err(|e| TakeoutMaterializationError::Database(e))?;

    tracing::info!(
        "Complete takeout materialization for {} finished: {} folders ({} failed), {} files ({} failed)",
        takeout_id, folder_result.0, folder_result.1, file_result.0, file_result.1
    );

    // Create archive from materialized files
    tracing::info!("Starting archive creation for takeout {}", takeout_id);

    // Get list of materialized entries from database
    let archive_entries = crate::db::takeout::get_materialized_entries_for_archive(
        &reserved_conn,
        &app_state.fragments_dir,
        takeout_id,
        &session.siv_key,
        &session.siv_nonce,
    ).map_err(|e| TakeoutMaterializationError::Database(e))?;

    // Build the manifest emitted as the first tar entry.
    let manifest = crate::db::takeout::build_takeout_manifest(
        &reserved_conn,
        takeout_id,
        user_id,
        &session.siv_key,
        &session.siv_nonce,
    ).map_err(|e| TakeoutMaterializationError::Database(e))?;

    // Reserved conn is no longer needed; release the slot before archive I/O
    // and the final consensus submit.
    drop(reserved_conn);

    let manifest_bytes = manifest.to_archive_bytes()
        .map_err(|e| TakeoutMaterializationError::Serialization(format!("manifest json: {:?}", e)))?;

    // Create archive path
    let archive_path = format!("{}/takeouts/{}.tar.gz",
        app_state.fragments_dir, takeout_id.simple());

    // Create the archive and clean up staging files
    let archive_size = crate::takeout::archive::create_archive(
        &manifest_bytes,
        archive_entries,
        &archive_path,
        true, // delete_source_files = true for cleanup
    ).map_err(|e| TakeoutMaterializationError::Archive(e))?;

    tracing::info!("Archive created successfully for takeout {}: {} bytes at {}",
                  takeout_id, archive_size, archive_path);

    // Clean up the entire takeout directory (staging + uuid folder)
    let takeout_root = format!("{}/takeouts/{}",
        app_state.fragments_dir, takeout_id.simple());
    if let Err(e) = std::fs::remove_dir_all(&takeout_root) {
        tracing::warn!("Failed to remove takeout directory {}: {:?}", takeout_root, e);
        // Continue anyway - archive is created
    } else {
        tracing::debug!("Cleaned up takeout directory: {}", takeout_root);
    }

    // Update status to Ready via consensus
    let ready_payload = TakeoutStatusPayload {
        takeout_id: takeout_id.clone(),
        new_status: TakeoutStatus::Ready,
    };

    let encoded_ready_payload = bincode::serde::encode_to_vec(ready_payload, bincode::config::standard())
        .map_err(|e| TakeoutMaterializationError::Serialization(format!("Failed to encode ready status: {:?}", e)))?;

    let ready_transaction = crate::consensus::functions::create_signed_transaction(
        app_state,
        "update_takeout_status".to_string(),
        encoded_ready_payload,
    ).map_err(|_| TakeoutMaterializationError::Consensus("Failed to sign ready transaction".to_string()))?;

    // Submit ready status to consensus
    app_state.consensus_queue.submit(ready_transaction).await
        .map_err(|e| TakeoutMaterializationError::Consensus(format!("Failed to update to ready: {:?}", e)))?;

    tracing::info!("Takeout {} marked as ready for download", takeout_id);

    Ok(())
}

#[derive(Debug)]
pub enum TakeoutMaterializationError {
    Database(crate::db::DatabaseError),
    Consensus(String),
    Serialization(String),
    Archive(std::io::Error),
}

impl std::fmt::Display for TakeoutMaterializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TakeoutMaterializationError::Database(e) => write!(f, "Database error: {:?}", e),
            TakeoutMaterializationError::Consensus(e) => write!(f, "Consensus error: {}", e),
            TakeoutMaterializationError::Serialization(e) => write!(f, "Serialization error: {}", e),
            TakeoutMaterializationError::Archive(e) => write!(f, "Archive error: {}", e),
        }
    }
}

impl std::error::Error for TakeoutMaterializationError {}

/// POST /maintenance/takeout
/// Manual trigger for takeout maintenance (expiration checking and cleanup)
pub async fn post_takeout_maintenance(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> impl axum::response::IntoResponse {
    tracing::info!("Manual takeout maintenance trigger requested by user {}", user_id);

    // Call the takeout maintenance job directly
    match super::jobs::handle_takeout_maintenance(
        super::jobs::TakeoutMaintenanceJob,
        apalis_cron::CronContext::default(),
        apalis::prelude::Data::new(app_state),
    ).await {
        Ok(_) => {
            tracing::info!("Manual takeout maintenance completed successfully");
            (StatusCode::OK, Json(serde_json::json!({
                "status": "success",
                "message": "Takeout maintenance completed successfully"
            }))).into_response()
        }
        Err(e) => {
            tracing::error!("Manual takeout maintenance failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "status": "error",
                "message": format!("Takeout maintenance failed: {}", e)
            }))).into_response()
        }
    }
}

