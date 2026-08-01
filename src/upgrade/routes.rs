//! Manual trigger for the upgrade tick — the maintenance-route sibling
//! of the cron in jobs.rs, sharing the same core fn.

use axum::extract::State;
use axum::response::IntoResponse;
use axum::{Json, http::StatusCode};

use crate::AppState;

/// POST /maintenance/upgrade-tick — poll the provider and reconcile the
/// version attestation now; returns what happened.
pub async fn post_upgrade_tick(State(app_state): State<AppState>) -> impl IntoResponse {
    let report = crate::upgrade::jobs::run_upgrade_tick(&app_state).await;
    (StatusCode::OK, Json(report))
}
