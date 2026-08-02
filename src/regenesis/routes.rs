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

#[derive(Deserialize)]
pub struct RetrustRequest {
    /// The peer the operator vouches for.
    pub node_id: i32,
}

/// POST /consensus/regenesis/retrust
///
/// The escape hatch when validator churn moved past the overlap window
/// (RFC-019 S7): the operator points this node at a peer they trust and
/// it re-bootstraps from the current epoch, keeping its fragment store —
/// the same trust-on-first-use ceremony as the original join, re-invoked.
///
/// Waives the OVERLAP requirement only. Chain-id linkage, every hop's
/// quorum proof, and the snapshot's certified hash are still enforced,
/// so a peer named here still cannot serve arbitrary state.
///
/// Answers 202: the fetch runs in the background and can take minutes.
pub async fn post_regenesis_retrust(
    State(app_state): State<AppState>,
    Json(body): Json<RetrustRequest>,
) -> impl IntoResponse {
    // The pubkey comes from our own (possibly stale) node table — it only
    // names a key to dial, and everything fetched is verified anyway.
    let peer = {
        let Ok(conn) = app_state.db_pool.get() else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "db unavailable").into_response();
        };
        let found: Option<crate::db::PubKey> = conn
            .query_row(
                "SELECT pubkey FROM nodes WHERE node_id = ?",
                [body.node_id],
                |row| row.get(0),
            )
            .ok();
        match found {
            Some(pk) => hopnet_comms::PeerRef {
                node_id: body.node_id,
                pubkey: pk.0.to_bytes(),
            },
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    format!("node {} is not known to this node", body.node_id),
                )
                    .into_response();
            }
        }
    };

    if let Some(progress) = crate::regenesis::join::join_state()
        && app_state
            .epoch_join_inflight
            .load(std::sync::atomic::Ordering::Acquire)
    {
        return (StatusCode::CONFLICT, progress).into_response();
    }

    crate::regenesis::join::spawn_epoch_join(
        &app_state,
        crate::regenesis::join::JoinAnchor::Manual { peer },
        vec![peer],
    );
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "retrust": "started", "peer": body.node_id })),
    )
        .into_response()
}

/// POST /consensus/regenesis/rollback
///
/// Abandon a pending or just-crossed epoch boundary (RFC-019 S8): write
/// the rollback marker and restart, so the boot path restores the
/// retained database (or clears the seal in place, for a node that
/// parked without crossing) before anything else runs.
///
/// DESTRUCTIVE while the window is open — it discards the newer epoch's
/// database. Refused up front when there is nothing to abandon, so
/// invoking it on a healthy node is a no-op rather than an accident.
///
/// A valid rollback is MESH-WIDE. A node that rolls back beside peers
/// still in the newer epoch is pulled straight back across by the epoch
/// join, which is working as intended.
pub async fn post_regenesis_rollback(State(app_state): State<AppState>) -> impl IntoResponse {
    let db_path = crate::db::shared::get_database_path();
    let available = {
        let Ok(conn) = app_state.db_pool.get() else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "db unavailable").into_response();
        };
        crate::regenesis::boot::rollback_available(&db_path, &conn)
    };
    if !available {
        return (
            StatusCode::CONFLICT,
            "no boundary to abandon: this node is not sealed and retains no previous \
             epoch (the rollback window closes at the new epoch's first decide)",
        )
            .into_response();
    }

    crate::regenesis::boot::write_rollback_marker(&db_path);
    tracing::warn!("rollback requested: restarting to abandon the epoch boundary");
    app_state.restart_signal.notify_one();
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "rollback": "requested" })),
    )
        .into_response()
}

/// GET /views/regenesis-status
pub async fn get_regenesis_status(
    State(app_state): State<AppState>,
) -> Result<Json<RegenesisStatusView>, StatusCode> {
    let pool = app_state.consensus_queue.pending_pool();
    let drained = pool.staged_len() == 0 && pool.inflight_len() == 0;

    // One blocking hop for everything that touches the DB or the
    // filesystem (rollback-window file check).
    let (state, rollback_retained) = {
        let app_state = app_state.clone();
        tokio::task::spawn_blocking(move || {
            let conn = app_state
                .db_pool
                .get()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let state = crate::db::regenesis::read_regenesis_state(&conn)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let retained =
                crate::regenesis::boot::sealed_path(&crate::db::shared::get_database_path())
                    .exists();
            Ok::<_, StatusCode>((state, retained))
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??
    };

    let running_code = crate::version::effective_running_code();
    let awaiting_upgrade = state.phase == crate::db::regenesis::RegenesisPhase::Sealed
        && state.target_version_code != Some(running_code);

    Ok(Json(RegenesisStatusView {
        phase: crate::regenesis::gate::phase_str(state.phase).to_string(),
        target_version: state.target_version_code.map(crate::version::format_code),
        seal_height: state.seal_height.map(|h| h.to_string()),
        drained,
        epoch: app_state
            .epoch
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
        running_version: crate::version::format_code(running_code),
        awaiting_upgrade,
        boundary_error: crate::regenesis::boot::boundary_error(),
        rollback_retained,
        epoch_join: crate::regenesis::join::join_state(),
    }))
}
