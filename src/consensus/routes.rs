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
            // sign and return the response
            dbg!("Signing off on block hash {}", ballot.block.block_hash);
            match ballot.sign(&app_state) {
                Ok(signoff) => {
                    // Only insert block during Propose phase, not Lock phase
                    if ballot.data.phase == ConsensusPhase::Propose {
                        dbg!("Adding to database block hash {}", ballot.block.block_hash);
                        match db::insert_block(app_state.db_pool.get(), &ballot.block) {
                            Ok(()) => {
                                dbg!("Block saved!");
                                return (StatusCode::OK, Json(signoff)).into_response()
                            },
                            Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Error adding block to database").into_response(),
                        }
                    } else {
                        // Lock phase - block should already exist, just return the signature
                        dbg!("Lock phase - block already exists, returning signature");
                        return (StatusCode::OK, Json(signoff)).into_response()
                    }
                }
                Err(e) => {
                    dbg!(e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Error signing ballot").into_response()
                },
            }
        }
        Err(e) => {
            dbg!(e);
            return (StatusCode::UNAUTHORIZED, "Ballot rejected").into_response()
        },
    }
}

// route to accept qcs and operate on them
pub async fn post_qc(
    State(app_state): State<AppState>,
    Json(qc): Json<QuorumCertificate>
) -> impl IntoResponse {
    // validate the QC against internal block
    dbg!("Received QC");
    match db::get_block(app_state.db_pool.get(), qc.block_hash) {
        Ok(block) => {
            dbg!("We have the block, verifying...");
            match qc.verify(&app_state, &block) {
                Ok(()) => {
                    // save it to db
                    dbg!("QC looks good, committing");
                    match db::insert_qc(app_state.db_pool.get(), qc.clone()) {
                        Ok(()) => {
                            // Process transactions if this is a Lock phase QC
                            if qc.phase == ConsensusPhase::Lock {
                                dbg!("Lock phase QC committed, processing transactions");
                                crate::consensus::functions::process_transactions(&block.data.transactions, &app_state);
                            }
                            StatusCode::OK
                        },
                        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    }
                }
                Err(e) => {
                    dbg!("Don't like the QC, printing error");
                    dbg!(e);
                    StatusCode::UNAUTHORIZED
                }
            }
        }
        Err(_) => StatusCode::NOT_FOUND
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
                dbg!("Failed to broadcast TC to validator: {}", e);
                // Continue with other broadcasts
            }
        }
    }
    
    Ok(())
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