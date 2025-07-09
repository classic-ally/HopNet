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

use crate::db::PubKey;
use crate::{
    db::nodes,
    types::Node
};
use crate::AppState;

pub async fn get_nodes(
    State(app_state): State<AppState>
) -> impl IntoResponse {
    match nodes::get_nodes(&app_state.db) {
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
    // 2-3: occur in DB thread
    ///////////////
    // we are going to need one worker to do our database comms which we communicate back+forth with
    let (dump_tx, dump_rx) = oneshot::channel();
    let (confirm_write_tx, confirm_write_rx) = oneshot::channel(); // for confirm PUT

    // Clone the Arc to avoid moving the entire app_state
    let db_clone = app_state.db.clone();
    let user_private_key = app_state.get_user_keys().unwrap().private_key.clone();
    let db_task = tokio::task::spawn_blocking(move || {
        // Use spawn_blocking for database operations since DuckDB is not async-safe
        tokio::runtime::Handle::current().block_on(async move {
            nodes::insert_node(&db_clone, payload, dump_tx, confirm_write_rx, user_private_key).await
        })
    });

    ///////////////
    // 4. Get sync message from DB thread
    ///////////////
    let db_dump = match dump_rx.await {
        Ok(data) => data,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    ///////////////
    // 5. Send the PUT of state to the new client
    ///////////////
    dbg!("Attempting PUT");
    match client.put(&url)
        .json(&db_dump)
        .send()
        .await
    {
        Ok(response) => if response.status() != StatusCode::CREATED {
            return StatusCode::BAD_GATEWAY
        }
        Err(_) => return StatusCode::GATEWAY_TIMEOUT
    }

    ///////////////
    // 6. Send OK message to DB thread (PUT must succeed to be here)
    ///////////////
    let _ = confirm_write_tx.send(Ok(()));

    ///////////////
    // 8. Ok() if success
    ///////////////
    match db_task.await {
        Ok(Ok(())) => {
            // The task completed successfully (insert was confirmed)
            return StatusCode::CREATED;
        },
        Ok(Err(e)) => {
            // The task returned an error
            return StatusCode::INTERNAL_SERVER_ERROR;
        },
        Err(e) => {
            // The task panicked or was cancelled
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

}
