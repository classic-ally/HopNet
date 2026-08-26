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

fn hex_lower(bytes: Vec<u8>) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Seat gate for the two routes that act LOCALLY and IMMEDIATELY.
///
/// `start`/`abort` submit a node-signed transaction, so consensus refuses
/// them unless the submitting node is seated (`require_seated` in
/// `handlers.rs`). `rollback` and `retrust` submit nothing, so they had no
/// equivalent — and `jwt_or_rpc_auth_middleware` accepts a signature from
/// ANY row in `nodes`, which is append-only in production: a node that
/// left voluntarily or was voted out still passed. This restores the same
/// requirement the consensus path enforces.
///
/// Deliberately narrow, and the residual is worth stating plainly. Only
/// the NODE-signature path is tightened; a mesh user's JWT still reaches
/// these routes, because `users` is consensus-replicated and any user's
/// passphrase authenticates on any node. That is a property of the whole
/// `/consensus` block rather than of these two routes, no admin or
/// operator role exists anywhere in the codebase to check instead, and it
/// is how operators actually drive rollback today. Closing it needs the
/// authorization class RFC-019 Open Question 2 still leaves open — not a
/// unilateral change here.
fn caller_seat_refusal(
    app_state: &AppState,
    caller: Option<&crate::consensus::routes::AuthenticatedNode>,
) -> Option<axum::response::Response> {
    let node_id = caller?.node_id;
    let Ok(conn) = app_state.db_pool.get() else {
        return Some((StatusCode::INTERNAL_SERVER_ERROR, "db unavailable").into_response());
    };
    let height = crate::db::consensus::get_current_consensus_height(&conn).unwrap_or(0);
    // The crate-level wrapper takes a Transaction (it is called from apply
    // paths); this is a plain read, so go to the consensus crate directly.
    match hopnet_consensus::validators::is_node_active(&conn, node_id, height) {
        Ok(true) => None,
        Ok(false) => {
            tracing::warn!(
                node_id,
                "refusing a local boundary op: signing node is not a seated validator"
            );
            Some(
                (
                    StatusCode::FORBIDDEN,
                    format!(
                        "node {node_id} is not a seated validator — a boundary operation that \
                         acts on this node immediately requires the same seat consensus \
                         requires of start/abort"
                    ),
                )
                    .into_response(),
            )
        }
        Err(_) => Some((StatusCode::INTERNAL_SERVER_ERROR, "seat lookup failed").into_response()),
    }
}

/// Strict 32-byte hex. Deliberately unforgiving about length: a truncated
/// fingerprint silently anchoring on a prefix would defeat the point.
fn parse_chain_id(hex: &str) -> Option<[u8; 32]> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

#[derive(Deserialize)]
pub struct RetrustRequest {
    /// The peer the operator vouches for.
    pub node_id: i32,
    /// Hex-encoded chain id of the epoch the operator expects to land on,
    /// read out of band from a node they already trust
    /// (`/views/regenesis-status` reports it). REQUIRED — see the route
    /// docstring for why naming a peer is not on its own a trust anchor.
    pub expect_chain_id: String,
}

/// POST /consensus/regenesis/retrust
///
/// The escape hatch when validator churn moved past the overlap window
/// (RFC-019 S7): the operator points this node at a peer they trust and
/// at the epoch identity they expect, and it re-bootstraps from there,
/// keeping its fragment store.
///
/// REPLACES the overlap requirement with a fingerprint; it does not waive
/// it. That distinction is the whole security of this route. A lineage
/// record is peer-supplied, and each hop's certificate is verified against
/// the validator set declared INSIDE that same record — so with no
/// external anchor the chain is self-certifying, and any peer holding a
/// registered node key could serve a wholly fabricated epoch signed by
/// validators it invented and have this node rebuild its entire database
/// from it. The operator's out-of-band chain id is what makes the request
/// meaningful: an operator who cannot obtain one from a node they already
/// trust has no basis for the request in the first place.
///
/// Chain-id linkage, every hop's quorum proof, and the snapshot's
/// certified hash are enforced on top, as before.
///
/// Answers 202: the fetch runs in the background and can take minutes.
pub async fn post_regenesis_retrust(
    State(app_state): State<AppState>,
    caller: Option<axum::Extension<crate::consensus::routes::AuthenticatedNode>>,
    Json(body): Json<RetrustRequest>,
) -> impl IntoResponse {
    if let Some(refusal) = caller_seat_refusal(&app_state, caller.as_deref()) {
        return refusal;
    }

    // Parse the fingerprint FIRST: it is the trust anchor, so a request
    // without a well-formed one has no anchor at all and must not start a
    // fetch. 400, not 202 — the operator has to fix the request.
    let expect_chain_id = match parse_chain_id(&body.expect_chain_id) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                "expect_chain_id must be 64 hex characters (32 bytes): the target epoch's \
                 chain id, read from a node you already trust. It replaces the overlap \
                 rule as this request's trust anchor and is not optional.",
            )
                .into_response();
        }
    };

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
        crate::regenesis::join::JoinAnchor::Manual {
            peer,
            expect_chain_id,
        },
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
pub async fn post_regenesis_rollback(
    State(app_state): State<AppState>,
    caller: Option<axum::Extension<crate::consensus::routes::AuthenticatedNode>>,
) -> impl IntoResponse {
    if let Some(refusal) = caller_seat_refusal(&app_state, caller.as_deref()) {
        return refusal;
    }

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

    // Signal AFTER this response has had a chance to reach the caller.
    // The binary races `serve()` against the restart signal in a
    // `tokio::select!`, so notifying inline cancels the server — and
    // takes this very response down with it, leaving the operator with
    // a transport error from a request that actually succeeded. Every
    // other restart request comes from background work with nobody
    // waiting on a socket; this one does not.
    let signal = app_state.restart_signal.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        signal.notify_one();
    });

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
    let (state, rollback_retained, chain_id, mesh_code, schema_ordinals, agreed_version) = {
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
            // Empty rather than an error before genesis: the view is also
            // the pre-setup health surface.
            let chain_id =
                hopnet_consensus::store::meta_get(&conn, hopnet_consensus::store::META_CHAIN_ID)
                    .ok()
                    .flatten()
                    .map(hex_lower)
                    .unwrap_or_default();
            // The operator-facing mesh code (RFC-025 S5): shown in the
            // Add Node flow, read by the orchestrator, entered on the
            // joining device. None pre-genesis.
            let mesh_code = crate::regenesis::genesis::mesh_magic(&conn, &crate::paths::data_dir())
                .ok()
                .map(|m| hopnet_comms::alpn::format_mesh_code(&m));
            // Empty rather than an error when the stamp table is absent
            // (legacy database parked mid-boundary): the view must keep
            // serving while an operator debugs exactly that state.
            let schema_ordinals = crate::db::chains::read_stamps(&conn)
                .unwrap_or_default()
                .into_iter()
                .map(
                    |(module, ordinal)| hopnet_common::views::SchemaOrdinalView { module, ordinal },
                )
                .collect::<Vec<_>>();
            // The mesh-agreed version this node runs (RFC-025): the
            // marker the seed guard clamps on, surfaced for operators
            // and the orchestrator gates. None = never joined.
            let agreed_version = crate::regenesis::boot::read_agreed_version(
                &crate::db::shared::get_database_path(),
            )
            .map(crate::version::format_code);
            Ok::<_, StatusCode>((
                state,
                retained,
                chain_id,
                mesh_code,
                schema_ordinals,
                agreed_version,
            ))
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
        chain_id,
        mesh_code,
        running_version: crate::version::format_code(running_code),
        agreed_version,
        awaiting_upgrade,
        boundary_error: crate::regenesis::boot::boundary_error(),
        rollback_retained,
        epoch_join: crate::regenesis::join::join_state(),
        schema_ordinals,
    }))
}
