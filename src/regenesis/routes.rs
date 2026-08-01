//! Operator routes for the regenesis boundary (RFC-019 S5). The human
//! gate for start/abort is the authenticated route itself (OQ2 v1: the
//! tx carries the seated validator's node signature; no admin role
//! exists). Mounted in the /consensus jwt-or-rpc block beside
//! post_leave, whose shape these follow.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Json};
use hopnet_common::views::{RegenesisRefusalView, RegenesisStatusView};
use serde::Deserialize;

use crate::AppState;
use crate::consensus::dispatch::create_signed_transaction;
use crate::consensus::queue::ConsensusSubmitError;
use crate::regenesis::{RegenesisAbort, RegenesisStart};

/// Build the structured 503 body from a queue refusal.
pub fn refusal_view(phase: &str, target_version_code: Option<u32>) -> RegenesisRefusalView {
    RegenesisRefusalView {
        phase: phase.to_string(),
        target_version: target_version_code.map(crate::version::format_code),
        message: "regenesis in progress: admission closed, retry after the boundary".to_string(),
    }
}

fn refusal_response(phase: &str, target_version_code: Option<u32>) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::RETRY_AFTER, "5".parse().unwrap());
    (
        StatusCode::SERVICE_UNAVAILABLE,
        headers,
        Json(refusal_view(phase, target_version_code)),
    )
        .into_response()
}

/// Submit a node-signed boundary tx and await the commit (post_leave
/// shape: queue-internal 120 s bound, HTTP capped at 60 s).
async fn submit_boundary_tx(
    app_state: &AppState,
    function: &str,
    payload: Vec<u8>,
    ok_body: serde_json::Value,
) -> axum::response::Response {
    let tx = match create_signed_transaction(app_state, function.to_string(), payload) {
        Ok(t) => t,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("sign: {e:?}")).into_response();
        }
    };
    match tokio::time::timeout(
        std::time::Duration::from_secs(60),
        app_state.consensus_queue.submit(tx),
    )
    .await
    {
        Ok(Ok(())) => (StatusCode::OK, Json(ok_body)).into_response(),
        Ok(Err(ConsensusSubmitError::Rejected(r))) => {
            (StatusCode::CONFLICT, format!("{function} refused: {r}")).into_response()
        }
        Ok(Err(ConsensusSubmitError::Moratorium {
            phase,
            target_version_code,
        })) => refusal_response(phase, target_version_code),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{function} failed: {e:?}"),
        )
            .into_response(),
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            format!("{function} not committed within 60s"),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct StartRequest {
    /// CalVer version string the next epoch requires, e.g. "2026.8.0".
    pub target_version: String,
}

/// POST /consensus/regenesis/start
pub async fn post_regenesis_start(
    State(app_state): State<AppState>,
    Json(body): Json<StartRequest>,
) -> impl IntoResponse {
    let Some(target_version_code) = crate::version::parse_code(&body.target_version) else {
        return (
            StatusCode::BAD_REQUEST,
            format!("not a CalVer version: {}", body.target_version),
        )
            .into_response();
    };
    let payload = match bincode::serde::encode_to_vec(
        &RegenesisStart {
            target_version_code,
        },
        bincode::config::standard(),
    ) {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response();
        }
    };
    submit_boundary_tx(
        &app_state,
        "regenesis_start",
        payload,
        serde_json::json!({ "started": true, "target_version": body.target_version }),
    )
    .await
}

/// POST /consensus/regenesis/abort
pub async fn post_regenesis_abort(State(app_state): State<AppState>) -> impl IntoResponse {
    let payload =
        match bincode::serde::encode_to_vec(&RegenesisAbort {}, bincode::config::standard()) {
            Ok(p) => p,
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response();
            }
        };
    submit_boundary_tx(
        &app_state,
        "regenesis_abort",
        payload,
        serde_json::json!({ "aborted": true }),
    )
    .await
}

/// GET /views/regenesis-status
pub async fn get_regenesis_status(
    State(app_state): State<AppState>,
) -> Result<Json<RegenesisStatusView>, StatusCode> {
    let pool = app_state.consensus_queue.pending_pool();
    let drained = pool.staged_len() == 0 && pool.inflight_len() == 0;

    let state = {
        let app_state = app_state.clone();
        tokio::task::spawn_blocking(move || {
            let conn = app_state
                .db_pool
                .get()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            crate::db::regenesis::read_regenesis_state(&conn)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??
    };

    Ok(Json(RegenesisStatusView {
        phase: crate::regenesis::gate::phase_str(state.phase).to_string(),
        target_version: state.target_version_code.map(crate::version::format_code),
        seal_height: state.seal_height.map(|h| h.to_string()),
        drained,
    }))
}
