//! Host-side FileProvider routes.
//!
//! Drive-owned (RFC-015, Stage D4): the FileProvider data surface —
//! enumerate/changes/download/item/delete/create/modify — lives in
//! `hopnet_drive::http::fileprovider`; the host mounts it in main.rs. Only
//! the handlers that touch host-only concerns remain here: health (setup
//! DB), and the test-mode endpoints (device-token registration via the
//! host's devices module; domain signal counters from the macOS glue).

use axum::{Json, extract::State, http::StatusCode};
use rand::RngExt;

use super::types::{HealthResponse, HealthStatus};
use crate::AppState;
use crate::db;
use hopnet_common::fileprovider::TestResponse;

/// Unauthenticated readiness endpoints for OS-integration clients, one
/// subrouter so main.rs carries a single nest line. Unauthenticated by
/// design: clients probe readiness before any token exists, and these
/// cannot live inside a DeviceToken projection mount (auth wraps the
/// whole router). Same handler for every integration — readiness is a
/// node property, not a per-surface one. Each route is individually
/// version-gated with its surface's resolved minimum (RFC-023 S3): the
/// probe is where a stale client learns it is stale, before any user
/// action — a version gate is not auth.
pub fn health_router(caps: &hopnet_projection::host::HostCapabilities) -> axum::Router<AppState> {
    use crate::client_compat::{SurfaceCompat, client_version_gate, resolved_min};
    use axum::routing::get;
    let gate = |surface: &'static str| {
        axum::middleware::from_fn_with_state(
            SurfaceCompat {
                surface,
                min_client: resolved_min(caps, surface),
            },
            client_version_gate,
        )
    };
    axum::Router::new()
        .route(
            "/fileprovider/health",
            get(get_health).layer(gate("/integrations/fileprovider")),
        )
        .route(
            "/documentprovider/health",
            get(get_health).layer(gate("/integrations/documentprovider")),
        )
        .route(
            "/mount/health",
            get(get_health).layer(gate("/integrations/mount")),
        )
}

/// Mount statfs (RFC-018 S8) — host-owned like health, but AUTHED: the
/// capacity reduction lives in `views::resilience`, which the drive crate
/// cannot see, so main.rs nests this one route under the same
/// device-token middleware that wraps the mount projection. The scan is
/// the resilience pane's full-table pass — spawn_blocking, as the view
/// route does.
pub async fn get_mount_statfs(
    State(app_state): State<AppState>,
) -> Result<Json<hopnet_common::mount::MountStatfsResponse>, StatusCode> {
    let pool = app_state.db_pool.clone();
    let (total_bytes, used_bytes) =
        tokio::task::spawn_blocking(move || crate::views::resilience::mount_statfs_bytes(&pool))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(hopnet_common::mount::MountStatfsResponse {
        total_bytes,
        used_bytes,
    }))
}

/// Health check endpoint for FileProvider extension
/// Returns ready if database setup is completed, not_ready otherwise
pub async fn get_health(State(app_state): State<AppState>) -> impl axum::response::IntoResponse {
    // Check if database setup is completed using the same pattern as /setup
    let status = match db::setup::get_initial_setup(app_state.db_pool.get()) {
        Ok(StatusCode::OK) => HealthStatus::Ready,
        // Not initialized, or database error: FileProvider cannot operate
        Ok(_) | Err(_) => HealthStatus::NotReady,
    };
    Json(HealthResponse {
        status,
        // RFC-023 S3: the probe carries the node's identity — what the
        // client's min_node check reads.
        node_version: crate::version::effective_running_code(),
    })
}

/// Test endpoint for FileProvider testing - only available in test mode
/// Registers a device token via consensus and returns it for test configuration
pub async fn get_test(State(app_state): State<AppState>) -> Result<Json<TestResponse>, StatusCode> {
    if !app_state.test_mode {
        return Err(StatusCode::NOT_FOUND);
    }

    let user_id = app_state.get_user_id()?;
    let session = app_state.get_session(user_id).await?;

    // Generate device token (same pattern as post_register_device)
    let device_id = crate::db::CustomUUID::new(None);
    let secret: Vec<u8> = (0..32).map(|_| rand::rng().random::<u8>()).collect();
    let secret_hex = hex::encode(&secret);
    let api_key_hash = crate::db::Blake3Hash::new(blake3::hash(secret_hex.as_bytes()));

    let encrypted_device_name = crate::storage_host::functions::encrypt_part(
        "test-fileprovider",
        &session.siv_key,
        &session.siv_nonce,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let wrapped_user_key =
        crate::auth::wrap_user_key_for_device(&secret, &session.user_keys.private_key)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Submit through consensus
    let payload = crate::devices::types::RegisterDevicePayload {
        id: device_id.clone(),
        user_id,
        api_key_hash,
        encrypted_device_name,
        wrapped_user_key,
    };

    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transaction = crate::consensus::dispatch::create_signed_user_transaction(
        &app_state,
        "register_device".to_string(),
        encoded_payload,
        user_id,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    app_state
        .consensus_queue
        .submit(transaction)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let api_key = format!("{}.{}", device_id, secret_hex);
    let backend_url = format!("http://localhost:{}", app_state.port);

    Ok(Json(TestResponse {
        api_key,
        backend_url,
    }))
}

/// Test endpoint to get FileProvider signal count - only available in test mode
pub async fn get_test_signals(State(app_state): State<AppState>) -> Result<String, StatusCode> {
    if !app_state.test_mode {
        return Err(StatusCode::NOT_FOUND);
    }

    let signal_count = crate::fileprovider::domain::get_signal_count();
    Ok(signal_count.to_string())
}
