//! Consensus HTTP surface (malachite engine).
//!
//! The bespoke engine's ballot/QC/TC processing, catch-up machinery, and
//! view-change plumbing were deleted at Stage 5b — the engine crate owns the
//! protocol now. What remains: inspection endpoints (compatibility shims
//! until the Stage-6 orchestrator retarget), validator activation, and the
//! node-RPC auth middlewares.

use crate::AppState;
use crate::db::consensus as db;
use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;

#[derive(Clone, Debug)]
pub struct AuthenticatedNode {
    pub node_id: i32,
}

// Route to get the consensus status.
//
// Malachite compatibility shim (full retarget is Stage 6): `view` is the
// engine height (current round's, or the pending height while paused
// on-demand), `leader` is the proposal target, `phase` is synthetic — the
// orchestrator parses leader.node_id / view / phase.
pub async fn get_consensus(State(app_state): State<AppState>) -> impl IntoResponse {
    let Some((height, round, proposer)) =
        crate::consensus::malachite::engine::proposal_target(&app_state)
    else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "consensus engine not active",
        )
            .into_response();
    };

    let leader = node_row(&app_state, proposer);
    let decided = app_state
        .malachite
        .get()
        .map(|e| *e.decided.borrow())
        .unwrap_or(0);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "leader": leader,
            "view": height,
            "round": round,
            "phase": "Propose",
            "last_decided_height": decided,
        })),
    )
        .into_response()
}

fn node_row(app_state: &AppState, node_id: i32) -> Option<crate::types::Node> {
    let conn = app_state.db_pool.get().ok()?;
    conn.query_row(
        "SELECT node_id, name, owner, pubkey FROM nodes WHERE node_id = ?",
        [node_id],
        |row| {
            Ok(crate::types::Node {
                node_id: row.get(0)?,
                name: row.get(1)?,
                owner: row.get(2)?,
                pubkey: row.get(3)?,
            })
        },
    )
    .ok()
}

// route to get acceptable validators for a given height
pub async fn get_validators(
    State(app_state): State<AppState>,
    Json(height): Json<i32>,
) -> impl IntoResponse {
    match db::get_validators(app_state.db_pool.get(), height) {
        Ok(nodes) => (StatusCode::OK, Json(nodes)).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get validators",
        )
            .into_response(),
    }
}

// Debug view state — the node's perspective on a specific height.
// Malachite shim: `view` IS the height; the leader is the deterministic
// round-0 proposer for that height.
#[derive(Serialize, Debug)]
pub struct DebugViewState {
    pub node_id: i32,
    pub queried_view: i32,
    pub height_at_view: i32,
    pub is_active_at_height: bool,
    /// This node's own latest departure kind at the queried height
    /// (height-scoped: stays "voted_out" at pre-readmission heights even
    /// after a later reactivation).
    pub last_departure_kind: Option<String>,
    pub validators_at_height: Vec<crate::types::Node>,
    pub leader_for_view: Option<crate::types::Node>,
}

pub async fn debug_view_state(
    State(app_state): State<AppState>,
    Json(view): Json<i32>,
) -> impl IntoResponse {
    let (node_id, is_active, last_departure_kind) = {
        let mut conn = match app_state.db_pool.get() {
            Ok(conn) => conn,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to get DB connection",
                )
                    .into_response();
            }
        };
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to start transaction",
                )
                    .into_response();
            }
        };
        let node_id: i32 = match tx.query_row(
            "SELECT node_id FROM this_node WHERE internal_id = 1",
            [],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get node ID")
                    .into_response();
            }
        };
        let is_active = db::is_node_active(&tx, node_id, view).unwrap_or(false);
        let last_departure_kind = db::last_departure(&tx, node_id, view)
            .ok()
            .flatten()
            .map(|k| k.as_str().to_string());
        (node_id, is_active, last_departure_kind)
    };

    let validators = match db::get_validators(app_state.db_pool.get(), view) {
        Ok(vals) => vals,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get validators",
            )
                .into_response();
        }
    };

    // Deterministic round-0 proposer: validators sorted node_id-asc,
    // index (height + round) % n — same rule as the engine context.
    let leader_for_view = if validators.is_empty() {
        None
    } else {
        let mut ids: Vec<i32> = validators.iter().map(|n| n.node_id).collect();
        ids.sort_unstable();
        let idx = (view.max(0) as usize) % ids.len();
        node_row(&app_state, ids[idx])
    };

    let response = DebugViewState {
        node_id,
        queried_view: view,
        height_at_view: view,
        is_active_at_height: is_active,
        last_departure_kind,
        validators_at_height: validators,
        leader_for_view,
    };

    (StatusCode::OK, Json(response)).into_response()
}

// View history entry for debugging/monitoring
#[derive(Serialize, Debug)]
pub struct ViewHistoryEntry {
    pub view: i32,
    pub height: i32,
    pub has_propose_qc: bool,
    pub has_lock_qc: bool,
    pub has_tc: bool,
    pub block_hash: Option<String>,
}

// Get consensus history showing height progression.
//
// Malachite compatibility shim: one row per decided height from the crate's
// decided_blocks/decided_certificates tables. `view` := height, both QC flags
// := certificate exists (one certificate per decide), `has_tc` := false
// (timeout certificates don't exist in Tendermint's commit path).
pub async fn get_consensus_history(State(app_state): State<AppState>) -> impl IntoResponse {
    let Ok(conn) = app_state.db_pool.get() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get DB connection",
        )
            .into_response();
    };
    let mut stmt = match conn.prepare(
        "SELECT b.height, b.block_hash, (c.height IS NOT NULL)
         FROM decided_blocks b
         LEFT JOIN decided_certificates c ON b.height = c.height
         ORDER BY b.height ASC",
    ) {
        Ok(stmt) => stmt,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get consensus history",
            )
                .into_response();
        }
    };
    let rows = stmt.query_map([], |row| {
        let height: i64 = row.get(0)?;
        let hash: Vec<u8> = row.get(1)?;
        let has_cert: bool = row.get(2)?;
        Ok(ViewHistoryEntry {
            view: height as i32,
            height: height as i32,
            has_propose_qc: has_cert,
            has_lock_qc: has_cert,
            has_tc: false,
            block_hash: Some(
                hash.iter()
                    .take(4)
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
            ),
        })
    });
    match rows {
        Ok(rows) => {
            let history: Vec<ViewHistoryEntry> = rows.flatten().collect();
            (StatusCode::OK, Json(history)).into_response()
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get consensus history",
        )
            .into_response(),
    }
}

/// Voluntary leave (RFC-CONSENSUS-002 S1): submit a self-signed
/// validator_leave transaction and await the commit. Under BFT the leave
/// block needs this node's own precommit (e.g. quorum(3) = 3), so an
/// orderly shutdown must await this response BEFORE stopping the node.
pub async fn post_leave(State(app_state): State<AppState>) -> impl IntoResponse {
    use crate::consensus::dispatch::create_signed_transaction;
    use crate::consensus::handlers::LeaveRequest;
    use crate::consensus::queue::ConsensusSubmitError;

    let Ok(node_id) = app_state.get_node_id() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "node identity not initialized",
        )
            .into_response();
    };
    let payload = match bincode::serde::encode_to_vec(
        &LeaveRequest { node_id },
        bincode::config::standard(),
    ) {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response();
        }
    };
    let tx = match create_signed_transaction(&app_state, "validator_leave".to_string(), payload) {
        Ok(t) => t,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("sign: {e:?}")).into_response();
        }
    };
    // submit() awaits the consensus commit (queue-internal 120 s bound);
    // cap the HTTP response at 60 s.
    match tokio::time::timeout(
        std::time::Duration::from_secs(60),
        app_state.consensus_queue.submit(tx),
    )
    .await
    {
        Ok(Ok(())) => (
            StatusCode::OK,
            Json(serde_json::json!({ "left": true, "node_id": node_id })),
        )
            .into_response(),
        Ok(Err(ConsensusSubmitError::Rejected(r))) => {
            (StatusCode::CONFLICT, format!("leave refused: {r}")).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("leave failed: {e:?}"),
        )
            .into_response(),
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            "leave not committed within 60s",
        )
            .into_response(),
    }
}

/// Re-activation trigger (legacy self-request path, kept through S1–S4;
/// RFC-CONSENSUS-002 S5 replaces it with mesh-initiated seating): submit a
/// validator_activation for this node at its current decided height.
pub async fn post_activate(State(app_state): State<AppState>) -> impl IntoResponse {
    let Ok(node_id) = app_state.get_node_id() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "node identity not initialized",
        )
            .into_response();
    };
    let current = match app_state
        .db_pool
        .get()
        .ok()
        .and_then(|c| db::get_current_consensus_height(&c).ok())
    {
        Some(h) => h,
        None => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "height unavailable").into_response();
        }
    };
    match request_activation(&app_state, node_id, current).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({ "activated": true, "node_id": node_id })),
        )
            .into_response(),
        Err(e) => (StatusCode::CONFLICT, format!("activation failed: {e}")).into_response(),
    }
}

/// Submit a validator-activation transaction through the consensus queue.
/// Called at the end of the join bootstrap (and by re-activation flows).
pub(crate) async fn request_activation(
    app_state: &AppState,
    node_id: i32,
    current_height: i32,
) -> Result<(), String> {
    use crate::consensus::dispatch::create_signed_transaction;
    use crate::consensus::handlers::ActivationRequest;

    // Create activation request (effective height computed deterministically during execution)
    let activation_req = ActivationRequest {
        node_id,
        current_height,
    };

    let payload = bincode::serde::encode_to_vec(&activation_req, bincode::config::standard())
        .map_err(|e| format!("encode activation request: {e}"))?;

    let transaction =
        create_signed_transaction(app_state, "validator_activation".to_string(), payload)
            .map_err(|e| format!("sign activation request: {e:?}"))?;

    app_state
        .consensus_queue
        .submit(transaction)
        .await
        .map_err(|e| format!("submit activation request: {e}"))?;

    tracing::info!(
        "Activation request submitted for node {} (effective height will be computed during execution)",
        node_id
    );

    Ok(())
}

pub async fn jwt_or_rpc_auth_middleware(
    State(app_state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    // First try JWT authentication
    if let Some(auth_header) = req.headers().get("Authorization")
        && let Ok(auth_str) = auth_header.to_str()
        && auth_str.starts_with("Bearer ")
    {
        // This looks like JWT auth, let the JWT middleware handle it
        match crate::auth::auth_middleware(State(app_state), req, next).await {
            Ok(response) => return response.into_response(),
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        }
    }

    // If no JWT, try RPC authentication
    rpc_auth_middleware(State(app_state), req, next)
        .await
        .into_response()
}

// RPC middleware for verifying node Ed25519 signatures on inter-node requests
pub async fn rpc_auth_middleware(
    State(app_state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    // Extract required headers
    let headers = req.headers();
    let node_id_header = headers.get("X-Node-ID");
    let node_signature_header = headers.get("X-Node-Signature");

    match (node_id_header, node_signature_header) {
        (Some(node_id_val), Some(node_sig_val)) => {
            // Parse node ID
            let node_id: i32 = match node_id_val.to_str().ok().and_then(|s| s.parse().ok()) {
                Some(id) => id,
                None => return StatusCode::BAD_REQUEST.into_response(),
            };

            // Parse node signature
            let node_signature = match node_sig_val
                .to_str()
                .ok()
                .and_then(|s| hex::decode(s).ok())
                .and_then(|bytes| {
                    if bytes.len() == 64 {
                        let mut sig_bytes = [0u8; 64];
                        sig_bytes.copy_from_slice(&bytes);
                        Some(ed25519_dalek::Signature::from_bytes(&sig_bytes))
                    } else {
                        None
                    }
                }) {
                Some(sig) => sig,
                None => return StatusCode::BAD_REQUEST.into_response(),
            };

            // Get node public key from database
            let node_pubkey = match db::get_node_pubkey(app_state.db_pool.get(), node_id) {
                Ok(pubkey) => pubkey,
                Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
            };

            // Extract and verify signatures against request body
            let (parts, body) = req.into_parts();
            let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
                Ok(bytes) => bytes,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };

            // Verify node signature only (user auth is now per-transaction)
            let verification_result = (|| -> Result<(), ()> {
                node_pubkey
                    .verify_strict(&body_bytes, &node_signature)
                    .map_err(|_| ())?;
                Ok(())
            })();

            if verification_result.is_err() {
                tracing::warn!("RPC signature verification failed for node {}", node_id);
                return StatusCode::UNAUTHORIZED.into_response();
            }

            tracing::debug!("RPC signature verified for node {}", node_id);

            // Reconstruct request with verified node info
            let auth_node = AuthenticatedNode { node_id };

            let mut new_req = Request::from_parts(parts, Body::from(body_bytes));
            new_req.extensions_mut().insert(auth_node);

            next.run(new_req).await
        }
        _ => {
            tracing::warn!("Missing required RPC headers: X-Node-ID, X-Node-Signature");
            StatusCode::UNAUTHORIZED.into_response()
        }
    }
}

/// GET /debug/state-snapshot — full-table content hashes for divergence checks
pub async fn get_state_snapshot(State(app_state): State<AppState>) -> impl IntoResponse {
    match crate::db::debug::compute_state_snapshot(app_state.db_pool.get()) {
        Ok(internal_snapshot) => {
            // Convert internal (Blake3Hash) to wire format (String)
            let wire_snapshot: hopnet_common::StateSnapshot = internal_snapshot.into();
            (axum::http::StatusCode::OK, Json(wire_snapshot)).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to compute state snapshot: {:?}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to compute state snapshot",
            )
                .into_response()
        }
    }
}

/// GET /debug/db-stats - SQLite sizing + active pragma snapshot for bench harness
pub async fn get_db_stats(State(app_state): State<AppState>) -> impl IntoResponse {
    match crate::db::debug::get_db_stats(app_state.db_pool.get()) {
        Ok(stats) => (axum::http::StatusCode::OK, Json(stats)).into_response(),
        Err(e) => {
            tracing::error!("Failed to get db stats: {:?}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get db stats",
            )
                .into_response()
        }
    }
}
