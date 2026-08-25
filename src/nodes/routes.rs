use std::time::Duration;

use axum::{Extension, Json, extract::State, http::StatusCode, response::IntoResponse};
use bincode::config;
use serde::Deserialize;

use hopnet_comms::Rpc;

use crate::AppState;
use crate::db::PubKey;
use crate::setup::{JoinDeliverRequest, SetupRequest, SetupResponse};
use crate::{consensus::types::Transaction, db::nodes, types::Node};

/// API payload for adding a new node (no ip/port needed — iroh uses pubkey-based addressing)
#[derive(Deserialize)]
pub struct NodeRegistration {
    pub name: String,
    pub owner: i32,
    pub pubkey: PubKey,
}

pub async fn get_nodes(State(app_state): State<AppState>) -> impl IntoResponse {
    match nodes::get_nodes(app_state.db_pool.get()) {
        Ok(nodes) => (StatusCode::OK, Json(nodes)),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::<Node>::new())),
    }
}

// route to add a new node
pub async fn post_nodes(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
    Json(payload): Json<NodeRegistration>,
) -> impl IntoResponse {
    // check if uid matches requester
    if uid != payload.owner {
        return StatusCode::FORBIDDEN;
    }

    // check if the session has user keys (our node needs to be set up)
    let session = match app_state.get_session(uid).await {
        Ok(s) => s,
        Err(_) => return StatusCode::NOT_ACCEPTABLE,
    };

    ///////////////
    // 1. Verify new node is reachable via iroh (replaces HTTP ping check).
    //    The iroh connection proves reachability AND pubkey ownership via TLS handshake.
    ///////////////
    let peer_pubkey = payload.pubkey.0.to_bytes();
    let ping_peer = hopnet_comms::PeerRef {
        node_id: -1,
        pubkey: peer_pubkey,
    };

    // Retry ping with backoff — iroh discovery (pkarr DNS) can take time for new nodes
    let mut ping_ok = false;
    for attempt in 0..6 {
        match app_state.comms.ping(&ping_peer).await {
            Ok(_rtt) => {
                tracing::info!("Successfully pinged new node via iroh (pubkey verified by TLS)");
                ping_ok = true;
                break;
            }
            Err(e) => {
                if attempt < 5 {
                    tracing::warn!("Ping attempt {} failed (retrying): {}", attempt + 1, e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                } else {
                    tracing::error!(
                        "Failed to reach new node via iroh after {} attempts: {}",
                        attempt + 1,
                        e
                    );
                }
            }
        }
    }
    if !ping_ok {
        return StatusCode::GATEWAY_TIMEOUT;
    }

    ///////////////
    // 2. Validate uniqueness and get next node ID — single connection checkout
    //    to avoid pool contention.
    ///////////////
    let (next_node_id, complete_node) = {
        let conn = match app_state.db_pool.get() {
            Ok(c) => c,
            Err(_) => return StatusCode::TOO_MANY_REQUESTS,
        };
        if nodes::pubkey_exists(&conn, &payload.pubkey) {
            tracing::warn!(
                "Rejecting node registration: pubkey {:?} already registered",
                payload.pubkey
            );
            return StatusCode::CONFLICT;
        }
        let next_id = match nodes::get_next_node_id_conn(&conn) {
            Ok(id) => id,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            next_id,
            Node {
                node_id: next_id,
                name: payload.name.clone(),
                owner: payload.owner,
                pubkey: payload.pubkey,
            },
        )
    };

    ///////////////
    // 3. Submit node addition to consensus
    ///////////////

    // Encode the complete node for consensus transaction
    match bincode::serde::encode_to_vec(&complete_node, config::standard()) {
        Ok(encoded_node) => {
            let transaction = match crate::consensus::dispatch::create_signed_user_transaction(
                &app_state,
                "insert_node".to_string(),
                encoded_node,
                uid,
            )
            .await
            {
                Ok(tx) => tx,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
            };
            // Submit to consensus queue
            match app_state.consensus_queue.submit(transaction).await {
                Ok(()) => {
                    tracing::info!(
                        "Consensus succeeded for node {}, waiting for database commit",
                        complete_node.node_id
                    );

                    // Poll database to confirm node was committed before proceeding to sync
                    let mut attempts = 0;
                    const MAX_ATTEMPTS: u32 = 50; // 5 seconds max wait
                    const POLL_INTERVAL_MS: u64 = 100;

                    loop {
                        match nodes::node_exists(app_state.db_pool.get(), complete_node.node_id) {
                            Ok(true) => {
                                tracing::info!(
                                    "Node {} confirmed in database after {} attempts",
                                    complete_node.node_id,
                                    attempts
                                );
                                break;
                            }
                            Ok(false) => {
                                attempts += 1;
                                if attempts >= MAX_ATTEMPTS {
                                    tracing::error!(
                                        "Node {} was not committed to database after {} attempts",
                                        complete_node.node_id,
                                        MAX_ATTEMPTS
                                    );
                                    return StatusCode::INTERNAL_SERVER_ERROR;
                                }
                                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                            }
                            Err(e) => {
                                tracing::error!("Database error checking node existence: {:?}", e);
                                return StatusCode::INTERNAL_SERVER_ERROR;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Consensus failed for node {}: {:?}",
                        complete_node.node_id,
                        e
                    );
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
        }
        Err(_) => {
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    ///////////////
    // 4. Post-consensus: Create JoinInfo and send to joining node via iroh
    ///////////////

    // Get current consensus height, quorum profile, and bootstrap validators
    // on a single connection checkout.
    let (current_height, quorum_profile, bootstrap_validators, epoch, anchor) = {
        let mut conn = match app_state.db_pool.get() {
            Ok(c) => c,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        };
        // The anchor (epoch-1) chain id rides JoinInfo so the joiner can
        // pre-flight its entered mesh code before writing anything
        // (RFC-025 S5).
        let anchor =
            match crate::regenesis::genesis::anchor_chain_id(&conn, &crate::paths::data_dir()) {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!("anchor chain id for JoinInfo: {e}");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            };
        let profile =
            hopnet_consensus::store::meta_get(&conn, hopnet_consensus::store::META_QUORUM_PROFILE)
                .ok()
                .flatten()
                .and_then(|b| String::from_utf8(b).ok())
                .unwrap_or_else(|| "bft".to_string());
        let height = {
            let tx = match conn.transaction() {
                Ok(tx) => tx,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
            };
            match crate::db::consensus::get_current_consensus_height(&tx) {
                Ok(h) => h,
                Err(e) => {
                    tracing::error!("Failed to get consensus height: {:?}", e);
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
        };
        let validators = match crate::db::consensus::get_validators_with_conn(&conn, height) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Failed to get validators for bootstrap: {:?}", e);
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        };
        let epoch = crate::regenesis::genesis::current_epoch(&conn);
        (height, profile, validators, epoch, anchor)
    };

    // Create JoinInfo structure
    let join_info = crate::types::JoinInfo {
        node_id: complete_node.node_id,
        user_id: uid,
        bootstrap_validators,
        quorum_profile,
        epoch,
        anchor,
    };

    ///////////////
    // 5. Deliver JoinInfo to the joining node via iroh — ASYNC with retries.
    // The node row is already committed through consensus; transient transport
    // failures (relay warm-up, holepunching) must not fail the registration.
    // The joining node stays passive until JoinInfo arrives, so retrying for
    // a few minutes is safe and idempotent (initialize_joining_node conflicts
    // are handled on its side by the setup-complete check).
    ///////////////
    tracing::info!(
        "Registering node {} complete; delivering JoinInfo in background ({} bootstrap validators)",
        complete_node.node_id,
        join_info.bootstrap_validators.len()
    );

    let comms = app_state.comms.clone();
    let node_id = complete_node.node_id;
    let deliver_peer = hopnet_comms::PeerRef {
        node_id,
        pubkey: peer_pubkey,
    };
    tokio::spawn(async move {
        const ATTEMPTS: u32 = 10;
        const RETRY_DELAY: Duration = Duration::from_secs(15);
        let payload = crate::net::encode_payload(&SetupRequest::JoinDeliver(JoinDeliverRequest {
            join_info,
        }));
        for attempt in 1..=ATTEMPTS {
            let reply = comms
                .rpc(
                    &deliver_peer,
                    "setup",
                    payload.clone(),
                    Duration::from_secs(30),
                )
                .await
                .and_then(|bytes| crate::net::decode_payload::<SetupResponse>(&bytes));
            match reply {
                Ok(SetupResponse::JoinAck { success: true }) => {
                    tracing::info!(
                        "Node {node_id} accepted JoinInfo (attempt {attempt}), bootstrap running"
                    );
                    return;
                }
                Ok(SetupResponse::Error { message }) => {
                    // The node answered and refused — retrying won't help.
                    tracing::error!("Node {node_id} rejected JoinInfo: {message}");
                    return;
                }
                Ok(other) => {
                    tracing::warn!(
                        "Unexpected JoinInfo response from node {node_id} (attempt {attempt}): {other:?}"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "JoinInfo delivery to node {node_id} failed (attempt {attempt}/{ATTEMPTS}): {e}"
                    );
                    comms.remove_connection(node_id).await;
                }
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
        tracing::error!(
            "JoinInfo delivery to node {node_id} gave up after {ATTEMPTS} attempts — the node can re-register"
        );
    });

    StatusCode::CREATED
}
