use std::time::Duration;

use axum::{
    Extension,
    extract::State, 
    response::IntoResponse,
    http::StatusCode,
    Json
};
use reqwest::Client;
use tokio::sync::oneshot;
use bincode::config;

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
    Json(payload): Json<Node>,
) -> impl IntoResponse {

    // OVERALL LOGIC FLOW
    // 1. Check, can we ping other server? Is it already setup?     | MAIN THREAD
    // 2. Get the current DB state, dump into vecs                  | DB THREAD
    // 3. Compute next node, append to node vec                     | DB THREAD
    // 4. Send our sync message to main thread                      | DB THREAD -> MAIN THREAD
    // 5. Send the PUT of state to the new client                   | MAIN THREAD
    // 6. If PUT succeeds, send OK message to DB thread             | MAIN THREAD -> DB THREAD
    // 7. Write DB to disk                                          | DB THREAD, terminates
    // 8. Ok()                                                      | MAIN THREAD

    ///////////////
    // 0. Boilerplate (check perms)
    ///////////////

    // check if uid matches requester
    if uid != payload.owner {
        return StatusCode::FORBIDDEN
    }

    // check if the app state user keys are set up
    // (our node needs to be set up)
    let Ok(_) = app_state.get_user_keys() else {
        return StatusCode::NOT_ACCEPTABLE
    };

    ///////////////
    // 1. Check, can we ping other server? Is it already setup?
    ///////////////
    let client = Client::new();
    let timeout_duration = Duration::from_secs(10);
    let url = format!("http://{}:{}/setup", payload.ip_address, payload.port);
    match client.get(&url)
        .timeout(timeout_duration)
        .send()
        .await
    {
        Ok(response) => {
            if response.status() != StatusCode::NOT_FOUND {
                return StatusCode::BAD_GATEWAY
            }
            
            // Extract the response text (hex-encoded pubkey)
            match response.text().await {
                Ok(response_pubkey_str) => {
                    // Parse the hex string response (it's a JSON string containing hex)
                    match serde_json::from_str::<String>(&response_pubkey_str) {
                        Ok(hex_str) => {
                            // Convert hex string to PubKey
                            match PubKey::from_hex(&hex_str) {
                                Ok(response_pubkey) => {
                                    // Compare with the payload pubkey
                                    if response_pubkey.0 != *payload.pubkey {
                                        // Pubkey mismatch - the node's actual pubkey doesn't match what was claimed
                                        return StatusCode::UNAUTHORIZED
                                    }
                                }
                                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR
                            }
                        }
                        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR
                    }
                }
                Err(_) => return StatusCode::BAD_GATEWAY
            }
        }
        Err(_) => return StatusCode::GATEWAY_TIMEOUT
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
        ip_address: payload.ip_address.clone(),
        port: payload.port,
        owner: payload.owner,
        pubkey: payload.pubkey,
    };

    ///////////////
    // 3. Submit node addition to consensus FIRST
    ///////////////
    
    // Encode the complete node for consensus transaction
    match bincode::serde::encode_to_vec(&complete_node, config::standard()) {
        Ok(encoded_node) => {
            let transaction = match crate::consensus::functions::create_signed_user_transaction(
                &app_state,
                "insert_node".to_string(),
                encoded_node,
                uid,
            ) {
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
                                break; // Node is registered, proceed to sync
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
            // Encoding failed
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    ///////////////
    // 4. Post-consensus: Create JoinInfo and send to joining node
    ///////////////

    // Get user's private key to send to joining node
    let user_private_key = match app_state.get_user_keys() {
        Ok(keys) => keys.private_key.clone(),
        Err(_) => {
            tracing::error!("Failed to get user keys from app state");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

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
    // 5. Send JoinInfo to the joining node
    ///////////////
    tracing::info!(
        "Sending JoinInfo to joining node {} at {} (bootstrap validators: {})",
        complete_node.node_id,
        url,
        join_info.bootstrap_validators.len()
    );

    match client.put(&url)
        .json(&join_info)
        .send()
        .await
    {
        Ok(response) => {
            match response.status() {
                StatusCode::ACCEPTED => {
                    tracing::info!(
                        "Node {} accepted JoinInfo, catch-up running in background",
                        complete_node.node_id
                    );
                    StatusCode::CREATED
                }
                status => {
                    tracing::error!(
                        "Node {} rejected JoinInfo with status: {}",
                        complete_node.node_id,
                        status
                    );
                    StatusCode::BAD_GATEWAY
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to send JoinInfo to node {}: {:?}", complete_node.node_id, e);
            StatusCode::GATEWAY_TIMEOUT
        }
    }

}
