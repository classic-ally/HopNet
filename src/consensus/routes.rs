use super::*;

use axum::{
    extract::State,
    response::IntoResponse,
    http::StatusCode,
    Json
};

use crate::db::consensus as db;
/// CONSENSUS ARCHITECTURE
/// Key notes:
/// - Using ed25519 over threshold w/ distributed key generation (e.g. BLS)
///   - Verifier: O(nodes) signature verification vs O(1), but ed is ~10x faster per op
///   - Signer: both O(1), ed is ~5x faster per op
///   - ed25519_dalek library gives us batch verify, ~6-8x faster 128+ batch
///   - Conclusion: probably faster until nodes is multiple hundreds (O(nodes) dominates)
/// 
///   - Gives us audit trail (which node did what?) over BLS
///   - Simpler implementation (no secret sharing polynomial pain)
///   - But, more vulnerable to mistakes
///     - No cryptographic protection against committing undersigned changes
///   - O(nodes) sigs broadcast for changes introduces network overhead
///     - BLS: 96 bytes per sig
///     - ed: 64 bytes per sig -> 6.4kb per 100 nodes
/// 
///   - We may want to address this later based on % overhead stats


use crate::AppState;
use axum::middleware::{self, Next};
use axum::http::Request;
use axum::body::Body;

// Authenticated user information passed to routes
#[derive(Clone, Debug)]
pub struct AuthenticatedUser {
    pub user_id: i32,
    pub node_id: i32,
    pub user_owns_node: bool,
}

// route to get the consensus status
pub async fn get_consensus(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match db::get_consensus(app_state.db_pool.get()) {
        Ok(info) => (StatusCode::OK, Json(info)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get leader info").into_response(),
    }
}

// route to get acceptable validators for a given view
pub async fn get_validators(
    State(app_state): State<AppState>,
    Json(height): Json<i32>,
) -> impl IntoResponse {
    match db::get_validators(app_state.db_pool.get(), height) {
        Ok(nodes) => (StatusCode::OK, Json(nodes)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get validators").into_response(),
    }
}

// route to accept ballots and operate on them
pub async fn post_ballot(
    State(app_state): State<AppState>,
    Json(ballot): Json<Ballot>
) -> impl IntoResponse {
    // validate the ballot proposal
    match ballot.verify_proposal(&app_state) {
        Ok(()) => {
            tracing::debug!(
                "Ballot verified for view {} phase {:?} block {:?}",
                ballot.data.view, ballot.data.phase, ballot.block.block_hash
            );
            
            match ballot.sign(&app_state) {
                Ok(signoff) => {
                    // Only insert block during Propose phase, not Lock phase
                    if ballot.data.phase == ConsensusPhase::Propose {
                        tracing::debug!(
                            "Inserting new block {:?} for view {} into database",
                            ballot.block.block_hash, ballot.data.view
                        );
                        match db::insert_block(app_state.db_pool.get(), &ballot.block) {
                            Ok(()) => {
                                tracing::debug!(
                                    "Block {:?} saved and signed for view {} propose phase",
                                    ballot.block.block_hash, ballot.data.view
                                );
                                return (StatusCode::OK, Json(signoff)).into_response()
                            },
                            Err(_) => {
                                tracing::error!(
                                    "Failed to save block {:?} to database",
                                    ballot.block.block_hash
                                );
                                return (StatusCode::INTERNAL_SERVER_ERROR, "Error adding block to database").into_response()
                            },
                        }
                    } else {
                        // Lock phase - block should already exist, just return the signature
                        tracing::debug!(
                            "Lock phase vote signed for view {} block {:?}",
                            ballot.data.view, ballot.block.block_hash
                        );
                        return (StatusCode::OK, Json(signoff)).into_response()
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to sign ballot for view {} phase {:?}: {:?}",
                        ballot.data.view, ballot.data.phase, e
                    );
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Error signing ballot").into_response()
                },
            }
        }
        Err(e) => {
            tracing::warn!(
                "Ballot rejected for view {} phase {:?}: {:?}",
                ballot.data.view, ballot.data.phase, e
            );
            return (StatusCode::UNAUTHORIZED, "Ballot rejected").into_response()
        },
    }
}

// route to accept qcs and operate on them
pub async fn post_qc(
    State(app_state): State<AppState>,
    Json(qc): Json<QuorumCertificate>
) -> impl IntoResponse {
    tracing::debug!(
        "Received QC for view {} phase {:?} block {:?}",
        qc.view_number, qc.phase, qc.block_hash
    );
    
    match db::get_block(app_state.db_pool.get(), qc.block_hash) {
        Ok(block) => {
            tracing::debug!(
                "Found block {:?} for QC verification",
                qc.block_hash
            );
            
            match qc.verify(&app_state, &block) {
                Ok(()) => {
                    tracing::debug!(
                        "QC verified, inserting into database for view {} phase {:?}",
                        qc.view_number, qc.phase
                    );
                    
                    match db::insert_qc(app_state.db_pool.get(), qc.clone()) {
                        Ok(()) => {
                            // Process transactions if this is a Lock phase QC
                            if qc.phase == ConsensusPhase::Lock {
                                tracing::info!(
                                    "Lock phase QC committed for view {}, processing transactions",
                                    qc.view_number
                                );
                                crate::consensus::functions::process_transactions(&block.data.transactions, &app_state);
                            }
                            StatusCode::OK
                        },
                        Err(_) => {
                            tracing::error!(
                                "Failed to insert QC for view {} phase {:?}",
                                qc.view_number, qc.phase
                            );
                            StatusCode::INTERNAL_SERVER_ERROR
                        },
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "QC verification failed for view {} phase {:?}: {:?}",
                        qc.view_number, qc.phase, e
                    );
                    StatusCode::UNAUTHORIZED
                }
            }
        }
        Err(_) => {
            tracing::warn!(
                "Block {:?} not found for QC verification",
                qc.block_hash
            );
            StatusCode::NOT_FOUND
        }
    }
    
}

// route to receive timeout votes for distributed TC generation
pub async fn post_timeout_vote(
    State(app_state): State<AppState>,
    Json(timeout_vote): Json<TimeoutVote>,
) -> impl IntoResponse {
    match app_state.timeout_vote_collector.add_vote(timeout_vote, &app_state).await {
        Ok(Some(tc)) => {
            // TC was created - apply it locally first, then broadcast
            match apply_timeout_certificate(tc.clone(), &app_state).await {
                Ok(_) => {
                    // Now broadcast to other nodes
                    match broadcast_timeout_certificate(tc, &app_state).await {
                        Ok(_) => StatusCode::CREATED, // Applied locally and broadcast succeeded
                        Err(_) => StatusCode::CREATED, // Applied locally but broadcast failed (still success)
                    }
                }
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR, // Failed to apply locally
            }
        }
        Ok(None) => StatusCode::CREATED, // Vote added, no TC yet
        Err(CertificateError::ValidationError) => StatusCode::OK, // Duplicate vote
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR, // Other error
    }
}

// route to receive timeout certificates from other nodes
pub async fn post_tc(
    State(app_state): State<AppState>,
    Json(timeout_cert): Json<TimeoutCertificate>,
) -> impl IntoResponse {
    // Verify TC is valid
    match timeout_cert.verify(&app_state) {
        Ok(_) => {
            // Apply TC to advance consensus view
            match apply_timeout_certificate(timeout_cert, &app_state).await {
                Ok(_) => StatusCode::OK,
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }
        Err(_) => StatusCode::BAD_REQUEST, // Invalid TC
    }
}

// Helper function to broadcast timeout certificate to all validators
pub async fn broadcast_timeout_certificate(
    tc: TimeoutCertificate,
    app_state: &AppState,
) -> Result<(), CertificateError> {
    // Get all validators except ourselves
    let me = db::get_me(app_state.db_pool.get()).map_err(|_| CertificateError::DatabaseError)?;
    let validators = db::get_validators(app_state.db_pool.get(), tc.view_number)
        .map_err(|_| CertificateError::DatabaseError)?
        .into_iter()
        .filter(|node| node.node_id != me.node_id)
        .collect::<Vec<_>>();
    
    // Broadcast TC to all other validators
    let client = reqwest::Client::new();
    let mut broadcast_tasks = Vec::new();
    
    for validator in validators {
        let tc_clone = tc.clone();
        let client_clone = client.clone();
        let url = format!("http://{}:{}/consensus/tc", validator.ip_address, validator.port);
        
        let task = tokio::spawn(async move {
            client_clone
                .post(&url)
                .json(&tc_clone)
                .send()
                .await
        });
        broadcast_tasks.push(task);
    }
    
    // Wait for all broadcasts (but don't fail if some fail)
    for task in broadcast_tasks {
        if let Ok(result) = task.await {
            if let Err(e) = result {
                tracing::debug!("Failed to broadcast TC to validator: {}", e);
                // Continue with other broadcasts
            }
        }
    }
    
    Ok(())
}

// RPC middleware for verifying dual Ed25519 signatures (node + user) on inter-node requests
pub async fn rpc_auth_middleware(
    State(app_state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    // Extract required headers
    let headers = req.headers();
    let node_id_header = headers.get("X-Node-ID");
    let user_id_header = headers.get("X-User-ID");
    let node_signature_header = headers.get("X-Node-Signature");
    let user_signature_header = headers.get("X-User-Signature");
    
    match (node_id_header, user_id_header, node_signature_header, user_signature_header) {
        (Some(node_id_val), Some(user_id_val), Some(node_sig_val), Some(user_sig_val)) => {
            // Parse IDs
            let node_id: i32 = match node_id_val.to_str().ok().and_then(|s| s.parse().ok()) {
                Some(id) => id,
                None => return StatusCode::BAD_REQUEST.into_response(),
            };
            let user_id: i32 = match user_id_val.to_str().ok().and_then(|s| s.parse().ok()) {
                Some(id) => id,
                None => return StatusCode::BAD_REQUEST.into_response(),
            };
            
            // Parse signatures
            let node_signature = match node_sig_val.to_str().ok()
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
            let user_signature = match user_sig_val.to_str().ok()
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
            
            // Get authentication info from database
            let (node_pubkey, user_pubkey, user_owns_node) = match db::get_node_user_auth_info(
                app_state.db_pool.get(), 
                node_id, 
                user_id
            ) {
                Ok(info) => info,
                Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
            };
            
            // Extract and verify signatures against request body
            let (parts, body) = req.into_parts();
            let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
                Ok(bytes) => bytes,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
            
            // Verify both signatures
            let verification_result = (|| -> Result<(), ()> {
                // Verify node signature
                node_pubkey.verify_strict(&body_bytes, &node_signature).map_err(|_| ())?;
                // Verify user signature  
                user_pubkey.verify_strict(&body_bytes, &user_signature).map_err(|_| ())?;
                Ok(())
            })();
            
            if verification_result.is_err() {
                tracing::warn!(
                    "RPC signature verification failed for user {} on node {}",
                    user_id, node_id
                );
                return StatusCode::UNAUTHORIZED.into_response();
            }
            
            tracing::info!(
                "RPC signatures verified for user {} on node {} (owns_node: {})",
                user_id, node_id, user_owns_node
            );
            
            // Reconstruct request with verified auth info
            let auth_user = AuthenticatedUser {
                user_id,
                node_id,
                user_owns_node,
            };
            
            let mut new_req = Request::from_parts(parts, Body::from(body_bytes));
            new_req.extensions_mut().insert(auth_user);
            
            next.run(new_req).await
        }
        _ => {
            tracing::warn!("Missing required RPC headers: X-Node-ID, X-User-ID, X-Node-Signature, X-User-Signature");
            StatusCode::UNAUTHORIZED.into_response()
        }
    }
}

// Route for non-leaders to forward transactions to the leader (pre-authenticated by middleware)
pub async fn post_propose(
    State(app_state): State<AppState>,
    axum::Extension(auth_user): axum::Extension<AuthenticatedUser>,
    Json(transactions): Json<Vec<Transaction>>,
) -> impl IntoResponse {
    tracing::info!(
        "Processing authenticated consensus proposal from user {} on node {} (owns_node: {})",
        auth_user.user_id, auth_user.node_id, auth_user.user_owns_node
    );
    
    // Process transactions through consensus (already authenticated)
    match crate::consensus::functions::consensus_middleware(&app_state, transactions, auth_user.user_id).await {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// Helper function to apply timeout certificate and advance consensus view
pub async fn apply_timeout_certificate(
    tc: TimeoutCertificate,
    app_state: &AppState,
) -> Result<(), CertificateError> {
    // Get current consensus state to validate TC view
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|_| CertificateError::DatabaseError)?;
    
    // TC must be for our current view to maintain chain consistency
    if tc.view_number != consensus_state.view {
        if tc.view_number < consensus_state.view {
            tracing::warn!("Rejecting TC for old view {} (current view: {})", tc.view_number, consensus_state.view);
            return Err(CertificateError::ValidationError);
        } else {
            tracing::warn!("Rejecting TC for future view {} (current view: {})", tc.view_number, consensus_state.view);
            return Err(CertificateError::ValidationError);
        }
    }
    
    // Store the TC in database
    match db::insert_tc(app_state.db_pool.get(), tc.clone()) {
        Ok(_) => {
            tracing::info!("Applied timeout certificate for view {}", tc.view_number);
            Ok(())
        }
        Err(_) => Err(CertificateError::DatabaseError),
    }
}