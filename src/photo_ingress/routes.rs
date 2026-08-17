//! Owner-only enablement routes: the future settings pane's backing API,
//! curl-testable in the meantime.
//!
//! POST /photo-ingress/enable   — mint token, provision keychain, register agent
//! POST /photo-ingress/disable  — unregister agent, clear keychain, revoke device
//! GET  /photo-ingress/status   — assemble provisioning + registration state
//!
//! The orchestration (and its ordering invariants) lives in `flow`, tested
//! on every platform; this module is the axum glue plus [`LiveDeps`], the
//! real SMAppService / keychain / consensus implementation.

use std::str::FromStr;

use axum::{
    Extension, Json, Router,
    extract::State,
    http::StatusCode,
    middleware,
    routing::{get, post},
};

use super::flow::{self, Failure, ProvisioningDeps};
use super::service;
use super::{AgentRegistration, DisableRequest, DisableResponse, PhotoIngressStatus};
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

struct LiveDeps {
    app_state: AppState,
}

impl ProvisioningDeps for LiveDeps {
    fn owner_user_id(&self) -> Option<i32> {
        self.app_state.get_user_id().ok()
    }

    async fn ensure_device_token(&self, user_id: i32) -> Result<(), StatusCode> {
        crate::devices::routes::ensure_photo_ingress_device_token(&self.app_state, user_id).await
    }

    async fn revoke_device(&self, user_id: i32, device_id: CustomUUID) -> Result<(), StatusCode> {
        crate::devices::routes::revoke_device_internal(&self.app_state, user_id, device_id).await
    }

    fn device_row_present(&self, device_id: Option<&str>) -> bool {
        let Some(id) = device_id.and_then(|s| CustomUUID::from_str(s).ok()) else {
            return false;
        };
        let Ok(db_lock) = self.app_state.db_pool.get() else {
            return false;
        };
        matches!(
            crate::db::devices::get_device_by_id(&db_lock, &id),
            Ok(Some(_))
        )
    }

    // SMAppService calls are XPC-backed and may block — off the async
    // threads with them.
    async fn agent_status(&self) -> Result<AgentRegistration, String> {
        tokio::task::spawn_blocking(service::agent_status)
            .await
            .map_err(|e| e.to_string())
    }

    async fn register_agent(&self) -> Result<AgentRegistration, String> {
        tokio::task::spawn_blocking(service::register_agent)
            .await
            .map_err(|e| e.to_string())?
    }

    async fn unregister_agent(&self) -> Result<(), String> {
        tokio::task::spawn_blocking(service::unregister_agent)
            .await
            .map_err(|e| e.to_string())?
    }

    fn load_config(&self) -> Option<(String, String)> {
        keychain::load_photo_ingress_config().ok()
    }

    fn remove_config(&self) {
        keychain::remove_photo_ingress_config();
    }

    fn current_bundle_path(&self) -> Option<String> {
        service::current_bundle_path()
    }

    fn stored_bundle_path(&self) -> Option<String> {
        std::fs::read_to_string(bundle_path_marker())
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn store_bundle_path(&self, path: &str) {
        if let Err(e) = std::fs::write(bundle_path_marker(), path) {
            tracing::warn!("photo-ingress bundle-path marker write failed: {e}");
        }
    }
}

/// Which bundle last registered the agent — plain file beside the database
/// so the marker survives keychain wipes (it describes launchd state, not
/// credentials).
fn bundle_path_marker() -> std::path::PathBuf {
    crate::paths::data_dir().join("photo-ingress-bundle-path")
}

/// Startup healer (RFC-026): re-register the agent when the running bundle
/// is not the one that registered it. Spawned once from GUI startup.
pub async fn reregister_if_moved_at_startup(app_state: AppState) {
    match flow::reregister_if_moved(&LiveDeps { app_state }).await {
        Ok(true) => tracing::info!("photo-ingress agent re-registered after bundle move"),
        Ok(false) => {}
        Err(e) => tracing::warn!("photo-ingress bundle-move check failed: {e}"),
    }
}

async fn get_status(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<PhotoIngressStatus>, Failure> {
    flow::status(&LiveDeps { app_state }, user_id)
        .await
        .map(Json)
}

async fn post_enable(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<PhotoIngressStatus>, Failure> {
    flow::enable(&LiveDeps { app_state }, user_id)
        .await
        .map(Json)
}

async fn post_disable(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    body: Option<Json<DisableRequest>>,
) -> Result<Json<DisableResponse>, Failure> {
    let req = body.map(|Json(r)| r).unwrap_or_default();
    flow::disable(&LiveDeps { app_state }, user_id, req)
        .await
        .map(Json)
}
