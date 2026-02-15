use std::time::Duration;

use axum::{
    Extension,
    extract::State,
    response::IntoResponse,
    http::StatusCode,
    Json
};
use bincode::config;
use serde::Deserialize;

use crate::db::{PubKey, DatabaseError};
use crate::{
    consensus::{
        functions::consensus_middleware,
        types::Transaction
    },
    db::nodes,
    types::Node
};
use crate::AppState;
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::setup::{JoinDeliverRequest, JoinAckResponse};

/// API payload for adding a new node (no ip/port needed — iroh uses pubkey-based addressing)
#[derive(Deserialize)]
pub struct NodeRegistration {
    pub name: String,
    pub owner: i32,
    pub pubkey: PubKey,
}

pub async fn get_nodes(
    State(app_state): State<AppState>
) -> impl IntoResponse {
    match nodes::get_nodes(app_state.db_pool.get()) {
        Ok(nodes) => return (StatusCode::OK, Json(nodes)),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::<Node>::new())),
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
        return StatusCode::FORBIDDEN
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
    let peer_iroh_id = payload.pubkey.to_iroh_node_id();

    // Retry ping with backoff — iroh discovery (pkarr DNS) can take time for new nodes
    let mut ping_ok = false;
    for attempt in 0..6 {
        match app_state.iroh_transport.ping(-1, peer_iroh_id).await {
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
                    tracing::error!("Failed to reach new node via iroh after {} attempts: {}", attempt + 1, e);
                }
            }
        }
    }
    if !ping_ok {
        return StatusCode::GATEWAY_TIMEOUT;
    }

    ///////////////
    // 2. Get next node ID and create complete node object
    ///////////////
    let next_node_id = match nodes::get_next_node_id(app_state.db_pool.get()) {
        Ok(id) => id,
        Err(DatabaseError::LockError) => {
            tracing::warn!("Database connection pool exhausted during get_next_node_id");
            return StatusCode::TOO_MANY_REQUESTS;
        },
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    let complete_node = Node {
        node_id: next_node_id,
        name: payload.name.clone(),
        owner: payload.owner,
        pubkey: payload.pubkey,
    };

    ///////////////
    // 3. Submit node addition to consensus
    ///////////////

    // Encode the complete node for consensus transaction
    match bincode::serde::encode_to_vec(&complete_node, config::standard()) {
        Ok(encoded_node) => {
            let transaction = match crate::consensus::functions::create_signed_user_transaction(
                &app_state,
                "insert_node".to_string(),
                encoded_node,
                uid,
            ).await {
                Ok(tx) => tx,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
            };
            let transactions = vec![transaction];

            // Submit to consensus middleware
            match consensus_middleware(&app_state, transactions).await {
                Ok(()) => {
                    tracing::info!("Consensus succeeded for node {}, waiting for database commit", complete_node.node_id);

                    // Poll database to confirm node was committed before proceeding to sync
                    let mut attempts = 0;
                    const MAX_ATTEMPTS: u32 = 50; // 5 seconds max wait
                    const POLL_INTERVAL_MS: u64 = 100;

                    loop {
                        match nodes::node_exists(app_state.db_pool.get(), complete_node.node_id) {
                            Ok(true) => {
                                tracing::info!("Node {} confirmed in database after {} attempts", complete_node.node_id, attempts);
                                break;
                            },
                            Ok(false) => {
                                attempts += 1;
                                if attempts >= MAX_ATTEMPTS {
                                    tracing::error!("Node {} was not committed to database after {} attempts", complete_node.node_id, MAX_ATTEMPTS);
                                    return StatusCode::INTERNAL_SERVER_ERROR;
                                }
                                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                            },
                            Err(e) => {
                                tracing::error!("Database error checking node existence: {:?}", e);
                                return StatusCode::INTERNAL_SERVER_ERROR;
                            }
                        }
                    }
                },
                Err(e) => {
                    tracing::error!("Consensus failed for node {}: {:?}", complete_node.node_id, e);
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
        },
        Err(_) => {
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    ///////////////
    // 4. Post-consensus: Create JoinInfo and send to joining node via iroh
    ///////////////

    // Get user's private key to send to joining node (from session fetched earlier)
    let user_private_key = session.user_keys.private_key.clone();

    // Get current consensus height
    let consensus_state = match crate::db::consensus::get_consensus(app_state.db_pool.get()) {
        Ok(state) => state,
        Err(e) => {
            tracing::error!("Failed to get consensus state: {:?}", e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    // Get all active validators for bootstrap list
    let bootstrap_validators = match crate::db::consensus::get_validators(
        app_state.db_pool.get(),
        consensus_state.committed_block.data.height
    ) {
        Ok(validators) => validators,
        Err(e) => {
            tracing::error!("Failed to get validators for bootstrap: {:?}", e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    // Create JoinInfo structure
    let join_info = crate::types::JoinInfo {
        node_id: complete_node.node_id,
        user_id: uid,
        user_privkey: user_private_key,
        bootstrap_validators,
    };

    ///////////////
    // 5. Send JoinInfo to the joining node via iroh
    ///////////////
    tracing::info!(
        "Sending JoinInfo to joining node {} via iroh (bootstrap validators: {})",
        complete_node.node_id,
        join_info.bootstrap_validators.len()
    );

    let req = IrohRequest::JoinDeliver(JoinDeliverRequest { join_info });
    match app_state.iroh_transport.request(
        complete_node.node_id,
        peer_iroh_id,
        &req,
        Duration::from_secs(30),
    ).await {
        Ok(IrohResponse::JoinAck(ack)) if ack.success => {
            tracing::info!(
                "Node {} accepted JoinInfo, catch-up running in background",
                complete_node.node_id
            );
            StatusCode::CREATED
        }
        Ok(IrohResponse::Error { message }) => {
            tracing::error!("Node {} rejected JoinInfo: {}", complete_node.node_id, message);
            StatusCode::BAD_GATEWAY
        }
        Ok(other) => {
            tracing::error!("Unexpected response from node {}: {:?}", complete_node.node_id, other);
            StatusCode::BAD_GATEWAY
        }
        Err(e) => {
            tracing::error!("Failed to send JoinInfo to node {}: {}", complete_node.node_id, e);
            StatusCode::GATEWAY_TIMEOUT
        }
    }

}
