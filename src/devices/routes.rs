use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware,
    routing::{get, post, delete},
    Extension, Json, Router,
};
use rand::Rng;
use crate::{auth::auth_middleware, AppState};
use crate::db::{devices::get_devices_for_user, CustomUUID, Blake3Hash};
use crate::consensus::functions::create_signed_user_transaction;
use crate::files::functions::{encrypt_part, decrypt_part};
use super::types::{
    RegisterDeviceRequest, RegisterDeviceResponse, RegisterDevicePayload,
    RevokeDevicePayload, DeviceInfo,
};

/// Build the devices management router (JWT-authenticated)
pub fn router(app_state: AppState) -> Router<AppState> {
    Router::new()
        .route("/", get(get_devices))
        .route("/register", post(post_register_device))
        .route("/{id}", delete(delete_device))
        .layer(middleware::from_fn_with_state(app_state, auth_middleware))
}

/// POST /devices/register
/// Generates new device token, submits to consensus, returns API key (shown once)
async fn post_register_device(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(request): Json<RegisterDeviceRequest>,
) -> Result<Json<RegisterDeviceResponse>, StatusCode> {
    // Generate device ID (UUIDv7 for timestamp encoding)
    let device_id = CustomUUID::new(None);

    // Generate cryptographically secure secret (32 bytes -> 64 hex chars)
    let secret: Vec<u8> = (0..32).map(|_| rand::rng().random::<u8>()).collect();
    let secret_hex = hex::encode(&secret);

    // Hash the secret for storage
    let api_key_hash = Blake3Hash::new(blake3::hash(secret_hex.as_bytes()));

    // Encrypt device name with user's SIV key
    let session = app_state.get_session(user_id).await?;
    let encrypted_device_name = encrypt_part(&request.device_name, &session.siv_key, &session.siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Build consensus payload
    let payload = RegisterDevicePayload {
        id: device_id.clone(),
        user_id,
        api_key_hash,
        encrypted_device_name,
    };

    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Create signed transaction with user authentication
    let transaction = create_signed_user_transaction(
        &app_state,
        "register_device".to_string(),
        encoded_payload,
        user_id,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Submit to consensus
    app_state.consensus_queue.submit(transaction)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Build full API key: {device_id}.{secret}
    let api_key = format!("{}.{}", device_id, secret_hex);

    Ok(Json(RegisterDeviceResponse {
        device_id,
        api_key,
    }))
}

/// GET /devices
/// List user's devices (decrypted names, no API keys)
async fn get_devices(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<Vec<DeviceInfo>>, StatusCode> {
    let db_lock = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let records = get_devices_for_user(&db_lock, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Get SIV keys for decryption
    let session = app_state.get_session(user_id).await?;

    let mut devices = Vec::with_capacity(records.len());
    for record in records {
        // Decrypt device name (strip leading / from encrypt_part format)
        let encrypted_name = record.encrypted_device_name.trim_start_matches('/');
        let device_name = decrypt_part(encrypted_name, &session.siv_key, &session.siv_nonce)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Extract timestamp from UUIDv7
        let created_at = record.id.extract_timestamp()
            .map(|dt| dt.timestamp())
            .unwrap_or(0);

        devices.push(DeviceInfo {
            id: record.id,
            device_name,
            created_at,
        });
    }

    Ok(Json(devices))
}

/// DELETE /devices/:id
/// Revoke a device token
async fn delete_device(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Path(device_id_str): Path<String>,
) -> StatusCode {
    // Parse device ID
    let device_id = match CustomUUID::from_str(&device_id_str) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    // Build consensus payload
    let payload = RevokeDevicePayload {
        device_id,
        user_id,
    };

    let encoded_payload = match bincode::serde::encode_to_vec(&payload, bincode::config::standard()) {
        Ok(data) => data,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Create signed transaction with user authentication
    let transaction = match create_signed_user_transaction(
        &app_state,
        "revoke_device".to_string(),
        encoded_payload,
        user_id,
    ).await {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Submit to consensus (idempotent - succeeds even if device doesn't exist)
    match app_state.consensus_queue.submit(transaction).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
