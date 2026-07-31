//! Owner-only enablement routes: the future settings pane's backing API,
//! curl-testable in the meantime.
//!
//! POST /photo-ingress/enable   — mint token, provision keychain, register agent
//! POST /photo-ingress/disable  — unregister agent, clear keychain, revoke device
//! GET  /photo-ingress/status   — assemble provisioning + registration state
//!
//! Enable orders keychain-before-register so credentials and blob_root are
//! in place before launchd first spawns the daemon. Disable is best-effort
//! per step (a half-disabled state can simply be disabled again).

use std::str::FromStr;

use axum::{
    Extension, Json, Router,
    extract::State,
    http::StatusCode,
    middleware,
    routing::{get, post},
};
use tracing::warn;

use super::helpers::{build_status, device_id_from_token, validate_blob_root};
use super::service;
use super::{AgentRegistration, DisableRequest, DisableResponse, EnableRequest, PhotoIngressStatus};
use crate::db::CustomUUID;
use crate::fileprovider::keychain;
use crate::{AppState, auth::auth_middleware};

pub fn router(app_state: AppState) -> Router<AppState> {
    Router::new()
        .route("/enable", post(post_enable))
        .route("/disable", post(post_disable))
        .route("/status", get(get_status))
        .layer(middleware::from_fn_with_state(app_state, auth_middleware))
}

type Failure = (StatusCode, String);

/// Every route is owner-only: provisioning writes THIS Mac's keychain and
/// login items — meaningless (and confusing) for any other user of the node.
fn ensure_owner(app_state: &AppState, user_id: i32) -> Result<(), Failure> {
    match app_state.get_user_id() {
        Ok(owner) if owner == user_id => Ok(()),
        _ => Err((
            StatusCode::FORBIDDEN,
            "photo ingress provisioning is owner-only".into(),
        )),
    }
}

fn internal(msg: impl std::fmt::Display) -> Failure {
    (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string())
}

/// SMAppService calls are XPC-backed and may block.
async fn blocking_service<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Failure> {
    tokio::task::spawn_blocking(f).await.map_err(internal)
}

fn device_row_present(app_state: &AppState, device_id: Option<&str>) -> bool {
    let Some(id) = device_id.and_then(|s| CustomUUID::from_str(s).ok()) else {
        return false;
    };
    let Ok(db_lock) = app_state.db_pool.get() else {
        return false;
    };
    matches!(
        crate::db::devices::get_device_by_id(&db_lock, &id),
        Ok(Some(_))
    )
}

fn current_status(app_state: &AppState, registration: AgentRegistration) -> PhotoIngressStatus {
    let keychain_pair = keychain::load_photo_ingress_config().ok();
    let blob_root = keychain::load_photo_ingress_blob_root().ok();
    let present = device_row_present(
        app_state,
        keychain_pair
            .as_ref()
            .and_then(|(api_key, _)| device_id_from_token(api_key)),
    );
    build_status(registration, keychain_pair, blob_root, present)
}

async fn get_status(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<PhotoIngressStatus>, Failure> {
    ensure_owner(&app_state, user_id)?;
    let registration = blocking_service(service::agent_status).await?;
    Ok(Json(current_status(&app_state, registration)))
}

async fn post_enable(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(req): Json<EnableRequest>,
) -> Result<Json<PhotoIngressStatus>, Failure> {
    ensure_owner(&app_state, user_id)?;
    validate_blob_root(&req.blob_root).map_err(|msg| (StatusCode::BAD_REQUEST, msg))?;

    // 1. Token (mint, or heal a revoked one; no-op while valid).
    crate::devices::routes::ensure_photo_ingress_device_token(&app_state, user_id)
        .await
        .map_err(|status| (status, "device token provisioning failed".into()))?;
    // 2. Library provisioning — in the keychain BEFORE launchd can spawn
    //    the daemon, so its startup auto-bind sees it.
    keychain::store_photo_ingress_provisioning(&req.blob_root, req.sidecar_root_remote.as_deref())
        .map_err(internal)?;
    // 3. Lifecycle handoff to launchd. RequiresApproval is a success —
    //    surfaced in the status for the caller to act on.
    let registration = blocking_service(service::register_agent)
        .await?
        .map_err(internal)?;

    Ok(Json(current_status(&app_state, registration)))
}

async fn post_disable(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    body: Option<Json<DisableRequest>>,
) -> Result<Json<DisableResponse>, Failure> {
    ensure_owner(&app_state, user_id)?;
    let req = body.map(|Json(r)| r).unwrap_or_default();

    // Capture the device id BEFORE the keychain wipe destroys the token.
    let device_id = keychain::load_photo_ingress_config()
        .ok()
        .and_then(|(api_key, _)| device_id_from_token(&api_key).map(str::to_string));

    if let Err(e) = blocking_service(service::unregister_agent).await? {
        warn!("photo-ingress disable: unregister failed (continuing): {e}");
    }
    keychain::remove_photo_ingress_config();

    let mut device_revoked = false;
    if req.revoke_device.unwrap_or(true)
        && let Some(id) = device_id.as_deref().and_then(|s| CustomUUID::from_str(s).ok())
    {
        match crate::devices::routes::revoke_device_internal(&app_state, user_id, id).await {
            Ok(()) => device_revoked = true,
            Err(e) => warn!("photo-ingress disable: device revoke failed: {e:?}"),
        }
    }

    let registration = blocking_service(service::agent_status).await?;
    Ok(Json(DisableResponse {
        device_revoked,
        status: current_status(&app_state, registration),
    }))
}
