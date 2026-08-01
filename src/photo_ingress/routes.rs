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

    fn load_blob_root(&self) -> Option<String> {
        keychain::load_photo_ingress_blob_root().ok()
    }

    fn store_provisioning(
        &self,
        blob_root: &str,
        sidecar_root_remote: Option<&str>,
    ) -> Result<(), String> {
        keychain::store_photo_ingress_provisioning(blob_root, sidecar_root_remote)
            .map_err(|e| e.to_string())
    }

    fn remove_config(&self) {
        keychain::remove_photo_ingress_config();
    }
}

async fn get_status(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<PhotoIngressStatus>, Failure> {
    flow::status(&LiveDeps { app_state }, user_id).await.map(Json)
}

async fn post_enable(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(req): Json<EnableRequest>,
) -> Result<Json<PhotoIngressStatus>, Failure> {
    flow::enable(&LiveDeps { app_state }, user_id, req)
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
