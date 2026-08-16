use super::types::{
    DeviceInfo, PairingInfoResponse, RegisterDevicePayload, RegisterDeviceRequest,
    RegisterDeviceResponse, RevokeDevicePayload,
};
use crate::consensus::dispatch::create_signed_user_transaction;
use crate::db::{Blake3Hash, CustomUUID, devices::get_devices_for_user};
use crate::storage_host::functions::{decrypt_part, encrypt_part};
use crate::{AppState, auth, auth::auth_middleware};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    http::StatusCode,
    middleware,
    routing::{delete, get, post},
};
use rand::RngExt;
use std::str::FromStr;

/// Build the devices management router (JWT-authenticated)
pub fn router(app_state: AppState) -> Router<AppState> {
    Router::new()
        .route("/", get(get_devices))
        .route("/register", post(post_register_device))
        .route("/pairing-info", get(get_pairing_info))
        .route("/{id}", delete(delete_device))
        .layer(middleware::from_fn_with_state(app_state, auth_middleware))
}

/// GET /devices/pairing-info
/// TLS listener facts for the pairing QR: the HTTPS port and the SPKI
/// fingerprint clients pin (docs/specs/pinned-https.md).
async fn get_pairing_info() -> Json<PairingInfoResponse> {
    let info = crate::tls::runtime_info();
    Json(PairingInfoResponse {
        tls_enabled: info.is_some(),
        https_port: info.map(|i| i.https_port),
        spki_sha256: info.map(|i| i.spki_sha256.clone()),
    })
}

/// Core device registration logic. Returns (device_id, api_key).
pub async fn register_device_internal(
    app_state: &AppState,
    user_id: i32,
    device_name: &str,
) -> Result<(CustomUUID, String), StatusCode> {
    // Generate device ID (UUIDv7 for timestamp encoding)
    let device_id = CustomUUID::new(None);

    // Generate cryptographically secure secret (32 bytes -> 64 hex chars)
    let secret: Vec<u8> = (0..32).map(|_| rand::rng().random::<u8>()).collect();
    let secret_hex = hex::encode(&secret);

    // Hash the secret for storage
    let api_key_hash = Blake3Hash::new(blake3::hash(secret_hex.as_bytes()));

    // Encrypt device name with user's SIV key
    let session = app_state.get_session(user_id).await?;
    let encrypted_device_name = encrypt_part(device_name, &session.siv_key, &session.siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Wrap user private key for device token session bootstrap
    let wrapped_user_key = auth::wrap_user_key_for_device(&secret, &session.user_keys.private_key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Build consensus payload
    let payload = RegisterDevicePayload {
        id: device_id.clone(),
        user_id,
        api_key_hash,
        encrypted_device_name,
        wrapped_user_key,
    };

    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Create signed transaction with user authentication
    let transaction = create_signed_user_transaction(
        app_state,
        "register_device".to_string(),
        encoded_payload,
        user_id,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Submit to consensus
    app_state
        .consensus_queue
        .submit(transaction)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Build full API key: {device_id}.{secret}
    let api_key = format!("{}.{}", device_id, secret_hex);

    Ok((device_id, api_key))
}

/// POST /devices/register
/// Generates new device token, submits to consensus, returns API key (shown once)
async fn post_register_device(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(request): Json<RegisterDeviceRequest>,
) -> Result<Json<RegisterDeviceResponse>, StatusCode> {
    let (device_id, api_key) =
        register_device_internal(&app_state, user_id, &request.device_name).await?;
    Ok(Json(RegisterDeviceResponse { device_id, api_key }))
}

/// Ensure a FileProvider device token exists in the macOS Keychain.
/// If no valid token exists (or a previous one was revoked), registers a new one.
#[cfg(target_os = "macos")]
pub async fn ensure_fileprovider_device_token(
    app_state: &AppState,
    user_id: i32,
) -> Result<(), StatusCode> {
    use crate::db::devices::get_device_by_id;
    use crate::fileprovider::keychain::{self, FileProviderConfig, KeychainEnvironment};

    // Check if a valid token already exists in keychain
    if let Ok(config) = keychain::load_config(KeychainEnvironment::Production)
        && let Some(dot_pos) = config.api_key.find('.')
    {
        let device_id_str = &config.api_key[..dot_pos];
        if let Ok(device_id) = CustomUUID::from_str(device_id_str) {
            let db_lock = app_state
                .db_pool
                .get()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            if let Ok(Some(_)) = get_device_by_id(&db_lock, &device_id) {
                return Ok(()); // Token still valid
            }
        }
    }

    // Register a new device token
    let (_device_id, api_key) =
        register_device_internal(app_state, user_id, "FileProvider").await?;

    // Store in keychain
    let config = FileProviderConfig::new(api_key, format!("http://localhost:{}", app_state.port));
    keychain::store_config(&config, KeychainEnvironment::Production)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(())
}

/// Ensure a photo-ingress device token exists in the macOS Keychain
/// (service `com.hopnet.desktop.photo-ingress`, read by the ingress
/// daemon's Swift shell). Mirrors `ensure_fileprovider_device_token`:
/// re-registers when the stored token's device row no longer exists
/// (revoked), otherwise leaves it untouched.
#[cfg(target_os = "macos")]
pub async fn ensure_photo_ingress_device_token(
    app_state: &AppState,
    user_id: i32,
) -> Result<(), StatusCode> {
    use crate::db::devices::get_device_by_id;
    use crate::fileprovider::keychain;

    if let Ok((api_key, _base_url)) = keychain::load_photo_ingress_config()
        && let Some(dot_pos) = api_key.find('.')
        && let Ok(device_id) = CustomUUID::from_str(&api_key[..dot_pos])
    {
        let db_lock = app_state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Ok(Some(_)) = get_device_by_id(&db_lock, &device_id) {
            return Ok(()); // Token still valid
        }
    }

    let (_device_id, api_key) =
        register_device_internal(app_state, user_id, "Photo Ingress").await?;
    // 127.0.0.1, not localhost — must match the ephemeral-port refresh in
    // main.rs so the daemon's credential-change detection sees ONE form.
    keychain::store_photo_ingress_config(&api_key, &format!("http://127.0.0.1:{}", app_state.port))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(())
}

/// GET /devices
/// List user's devices (decrypted names, no API keys)
async fn get_devices(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<Vec<DeviceInfo>>, StatusCode> {
    let db_lock = app_state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let records =
        get_devices_for_user(&db_lock, user_id).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Get SIV keys for decryption
    let session = app_state.get_session(user_id).await?;

    let mut devices = Vec::with_capacity(records.len());
    for record in records {
        // Decrypt device name (strip leading / from encrypt_part format)
        let encrypted_name = record.encrypted_device_name.trim_start_matches('/');
        let device_name = decrypt_part(encrypted_name, &session.siv_key, &session.siv_nonce)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Extract timestamp from UUIDv7
        let created_at = record
            .id
            .extract_timestamp()
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

/// Core device revocation: submit the revoke_device consensus transaction
/// (idempotent — succeeds even if the device doesn't exist). Shared by the
/// DELETE route and the photo-ingress disable flow.
pub async fn revoke_device_internal(
    app_state: &AppState,
    user_id: i32,
    device_id: CustomUUID,
) -> Result<(), StatusCode> {
    let payload = RevokeDevicePayload { device_id, user_id };

    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transaction = create_signed_user_transaction(
        app_state,
        "revoke_device".to_string(),
        encoded_payload,
        user_id,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    app_state
        .consensus_queue
        .submit(transaction)
        .await
        .map(|_| ())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// DELETE /devices/:id
/// Revoke a device token
async fn delete_device(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Path(device_id_str): Path<String>,
) -> StatusCode {
    let device_id = match CustomUUID::from_str(&device_id_str) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    match revoke_device_internal(&app_state, user_id, device_id).await {
        Ok(()) => StatusCode::OK,
        Err(status) => status,
    }
}
