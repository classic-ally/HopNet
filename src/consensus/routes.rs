use super::*;

use axum::{
    extract::{State, Path},
    response::IntoResponse,
    http::StatusCode,
    Json,
    Extension
};

use crate::db::consensus as db;
use crate::consensus::functions::ConsensusError;
use std::cmp::Ordering;
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

#[derive(Clone, Debug)]
pub struct AuthenticatedNode {
    pub node_id: i32,
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

// route to get all consensus data for a specific view (RPC-protected for inter-node catch-up)
pub async fn get_view_consensus_data(
    State(app_state): State<AppState>,
    Path(view): Path<i32>,
    Extension(auth): Extension<AuthenticatedNode>,
) -> impl IntoResponse {
    
    match db::get_view_consensus_data(app_state.db_pool.get(), view) {
        Ok(view_data) => (StatusCode::OK, Json(view_data)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get view consensus data").into_response(),
    }
}

// route to accept ballots and operate on them
pub async fn post_ballot(
    State(app_state): State<AppState>,
    Json(ballot): Json<Ballot>
) -> impl IntoResponse {
    tracing::debug!(
        "Received ballot for view {} phase {:?} block {:?}",
        ballot.data.view, ballot.data.phase, ballot.block.block_hash
    );
    
    // Check if this is a future ballot that should trigger catch-up
    let consensus_state = match db::get_consensus(app_state.db_pool.get()) {
        Ok(state) => state,
        Err(crate::db::DatabaseError::LockError) => {
            tracing::warn!("Database connection pool exhausted during consensus state fetch");
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        },
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    
    match ballot.data.view.cmp(&consensus_state.view) {
        Ordering::Greater => {
            // Future ballot - trigger catch-up using existing mechanism
            let views_behind = ballot.data.view - consensus_state.view;
            tracing::info!(
                "Received future ballot for view {} (current: {}), {} views behind - initiating catch-up", 
                ballot.data.view, consensus_state.view, views_behind
            );
            
            // Use existing catch-up mechanism
            match perform_catch_up(&app_state, consensus_state.view, ballot.data.view).await {
                Ok(()) => {
                    tracing::info!("Successfully caught up to view {}", ballot.data.view);
                },
                Err(e) => {
                    tracing::warn!("Catch-up failed: {:?} - continuing with partial catch-up", e);
                    // Continue with ballot processing even if catch-up failed partially
                }
            }
            
            tracing::info!("Catch-up complete, processing original ballot for view {}", ballot.data.view);
        },
        Ordering::Less => {
            tracing::debug!("Received old ballot for view {} (current: {})", ballot.data.view, consensus_state.view);
            // Continue with normal processing - old ballots might still be valid
        },
        Ordering::Equal => {
            tracing::debug!("Received ballot for current view {}", ballot.data.view);
            // Continue with normal processing
        }
    }
    
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
                        match db::insert_block(app_state.db_pool.get(), &ballot.block, true) {
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
            
            // Provide detailed feedback for transaction validation errors
            match e {
                super::types::VoteError::TransactionValidationError(details) => {
                    return (StatusCode::BAD_REQUEST, format!("Transaction validation failed: {}", details)).into_response()
                },
                super::types::VoteError::InitiatorError => {
                    return (StatusCode::UNAUTHORIZED, "Invalid signature or unauthorized proposer").into_response()
                },
                super::types::VoteError::ProgressionError => {
                    return (StatusCode::CONFLICT, "Proposal conflicts with consensus state").into_response()
                },
                super::types::VoteError::BlockError => {
                    return (StatusCode::BAD_REQUEST, "Invalid block data").into_response()
                },
                _ => {
                    return (StatusCode::UNAUTHORIZED, "Ballot rejected").into_response()
                }
            }
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
    
    // Check if this is a future QC that should trigger catch-up
    let consensus_state = match db::get_consensus(app_state.db_pool.get()) {
        Ok(state) => state,
        Err(crate::db::DatabaseError::LockError) => {
            tracing::warn!("Database connection pool exhausted during consensus state fetch");
            return StatusCode::TOO_MANY_REQUESTS;
        },
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    
    match qc.view_number.cmp(&consensus_state.view) {
        Ordering::Greater => {
            // Future QC - trigger catch-up using existing mechanism
            let views_behind = qc.view_number - consensus_state.view;
            tracing::info!(
                "Received future QC for view {} (current: {}), {} views behind - initiating catch-up", 
                qc.view_number, consensus_state.view, views_behind
            );
            
            // Use existing catch-up mechanism
            match perform_catch_up(&app_state, consensus_state.view, qc.view_number).await {
                Ok(()) => {
                    tracing::info!("Successfully caught up to view {}", qc.view_number);
                },
                Err(e) => {
                    tracing::warn!("Catch-up failed: {:?} - continuing with partial catch-up", e);
                    // Continue with QC processing even if catch-up failed partially
                }
            }
            
            tracing::info!("Catch-up complete, processing original QC for view {}", qc.view_number);
        },
        Ordering::Less => {
            tracing::debug!("Received old QC for view {} (current: {})", qc.view_number, consensus_state.view);
            // Continue with normal processing - old QCs are still valid
        },
        Ordering::Equal => {
            tracing::debug!("Received QC for current view {}", qc.view_number);
            // Continue with normal processing
        }
    }
    
    // Normal QC processing (existing logic)
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
                                let _ = crate::consensus::functions::process_transactions(&block.data.transactions, &app_state, true);
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

#[derive(Debug)]
pub enum ViewComparison {
    Behind { our_view: i32, max_network_view: i32 },
    CaughtUp { view: i32 },
    Ahead { our_view: i32, sampled_max_view: i32 },
}

#[derive(Debug, Clone, Copy)]
pub enum CatchUpStrictness {
    Lenient,  // Allow 1 view behind (normal consensus operations might advance us)
    Strict,   // Always catch up regardless of gap (timeout scenarios)
}

#[derive(Debug)]
pub struct CatchUpNeeded {
    pub our_view: i32,
    pub target_view: i32,
}

/// Check if we're caught up with the network by polling a subset of validators
/// Returns the view comparison result for decision making
pub async fn check_view_status(app_state: &AppState) -> Result<ViewComparison, ConsensusError> {
    use crate::consensus::functions;
    
    // Get our current view (single DB call)
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|_| ConsensusError::DatabaseError)?;
    let our_view = consensus_state.view;
    
    // Poll network for max view (pass consensus_state to avoid duplicate DB call)
    let max_network_view = functions::poll_subset_for_max_view(app_state, &consensus_state).await?;
    
    // Compare and categorize the relationship
    use std::cmp::Ordering;
    let result = match max_network_view.cmp(&our_view) {
        Ordering::Greater => {
            ViewComparison::Behind { our_view, max_network_view }
        }
        Ordering::Equal => {
            ViewComparison::CaughtUp { view: our_view }
        }
        Ordering::Less => {
            ViewComparison::Ahead { our_view, sampled_max_view: max_network_view }
        }
    };
    
    tracing::debug!("View status check: {:?}", result);
    Ok(result)
}

/// Fetch consensus data for a specific view from a validator node
async fn fetch_view(
    view: i32,
    validator: &crate::types::Node,
    app_state: &AppState,
) -> Result<ViewConsensusData, ConsensusError> {
    let my_node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;

    // Sign empty body for GET request authentication
    let body = b"";
    let node_signature = app_state.private_key.try_sign(body).map_err(|_| ConsensusError::SigningError)?;

    let client = reqwest::Client::new();
    let url = format!("http://{}:{}/consensus/view/{}", validator.ip_address, validator.port, view);

    tracing::debug!("Fetching view {} data from validator {}", view, validator.node_id);

    let response = client
        .get(&url)
        .header("X-Node-ID", my_node_id.to_string())
        .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| {
            tracing::warn!("Network error fetching view {} from validator {}: {:?}", view, validator.node_id, e);
            ConsensusError::NetworkError
        })?;
    
    if !response.status().is_success() {
        tracing::warn!("Failed to fetch view {} data from validator {}: status {}", 
            view, validator.node_id, response.status());
        return Err(ConsensusError::NetworkError);
    }
    
    let view_data: ViewConsensusData = response.json().await
        .map_err(|e| {
            tracing::warn!("Failed to parse view {} data from validator {}: {:?}", view, validator.node_id, e);
            ConsensusError::NetworkError
        })?;
    
    tracing::debug!("Successfully fetched view {} data: TC={}, propose_QC={}, lock_QC={}, blocks={}",
        view,
        view_data.timeout_certificate.is_some(),
        view_data.propose_qc.is_some(),
        view_data.lock_qc.is_some(),
        view_data.blocks.len()
    );
    
    Ok(view_data)
}

/// Integrate fetched view data into our local database with validation
async fn integrate_view(
    view: i32,
    view_data: ViewConsensusData,
    app_state: &AppState,
) -> Result<(), ConsensusError> {
    tracing::info!("Integrating view {} data", view);
    
    // Insert blocks first (they're referenced by QCs and TCs)
    for block in &view_data.blocks {
        match db::insert_block(app_state.db_pool.get(), block, true) {
            Ok(_) => {
                tracing::debug!("Inserted block {:?} for view {}", block.block_hash, view);
            }
            Err(_) => {
                tracing::debug!("Block {:?} already exists for view {}", block.block_hash, view);
                // Continue - block might already exist
            }
        }
    }
    
    // Genesis bypass: view 0 QCs inserted without verification (trust coordinator)
    let is_genesis = view == 0;

    // Validate and insert propose QC if present
    if let Some(propose_qc) = &view_data.propose_qc {
        // Find the corresponding block for validation
        if let Some(block) = view_data.blocks.iter().find(|b| b.block_hash == propose_qc.block_hash) {
            // Skip verification for genesis, otherwise verify normally
            if !is_genesis {
                propose_qc.verify(app_state, block).map_err(|e| {
                    tracing::warn!("Invalid propose QC for view {}: {:?}", view, e);
                    ConsensusError::SigningError
                })?;
            }

            match db::insert_qc(app_state.db_pool.get(), propose_qc.clone()) {
                Ok(_) => {
                    if is_genesis {
                        tracing::debug!("Inserted genesis propose QC for view 0 (no verification)");
                    } else {
                        tracing::debug!("Validated and inserted propose QC for view {}", view);
                    }
                }
                Err(_) => {
                    tracing::debug!("Propose QC for view {} already exists", view);
                }
            }
        } else {
            tracing::warn!("Block not found for propose QC in view {}", view);
            return Err(ConsensusError::BlockError);
        }
    }

    // Validate and insert lock QC if present
    if let Some(lock_qc) = &view_data.lock_qc {
        // Find the corresponding block for validation
        if let Some(block) = view_data.blocks.iter().find(|b| b.block_hash == lock_qc.block_hash) {
            // Skip verification for genesis, otherwise verify normally
            if !is_genesis {
                lock_qc.verify(app_state, block).map_err(|e| {
                    tracing::warn!("Invalid lock QC for view {}: {:?}", view, e);
                    ConsensusError::SigningError
                })?;
            }

            match db::insert_qc(app_state.db_pool.get(), lock_qc.clone()) {
                Ok(_) => {
                    if is_genesis {
                        tracing::debug!("Inserted genesis lock QC for view 0 (no verification)");
                    } else {
                        tracing::debug!("Validated and inserted lock QC for view {}", view);
                    }

                    // Process transactions if this is a Lock phase QC (same logic as post_qc)
                    if lock_qc.phase == ConsensusPhase::Lock {
                        tracing::info!(
                            "Lock phase QC committed for view {}, processing transactions",
                            lock_qc.view_number
                        );
                        let _ = crate::consensus::functions::process_transactions(&block.data.transactions, app_state, true);
                    }
                }
                Err(_) => {
                    tracing::debug!("Lock QC for view {} already exists", view);
                }
            }
        } else {
            tracing::warn!("Block not found for lock QC in view {}", view);
            return Err(ConsensusError::BlockError);
        }
    }
    
    // Genesis should never have a timeout certificate
    if is_genesis && view_data.timeout_certificate.is_some() {
        tracing::error!("Genesis view 0 has timeout certificate - invalid");
        return Err(ConsensusError::BlockError);
    }

    // Validate and insert timeout certificate if present (this advances our view)
    if let Some(tc) = &view_data.timeout_certificate {
        tc.verify(app_state).map_err(|e| {
            tracing::warn!("Invalid timeout certificate for view {}: {:?}", view, e);
            ConsensusError::SigningError
        })?;

        match db::insert_tc(app_state.db_pool.get(), tc.clone()) {
            Ok(_) => {
                tracing::debug!("Validated and applied timeout certificate for view {}, advanced to view {}", view, view + 1);
            }
            Err(_) => {
                tracing::debug!("Timeout certificate for view {} already exists", view);
            }
        }
    }
    
    tracing::debug!("Successfully integrated view {} data", view);
    Ok(())
}

/// Fetch and validate view data with retry logic and validator rotation
async fn fetch_and_validate_with_retry(
    view: i32,
    target_view: i32,
    validators: &[crate::types::Node],
    app_state: &AppState,
) -> Result<ViewConsensusData, functions::CatchUpError> {
    use crate::consensus::functions::CatchUpError;
    use crate::consensus::types::validate_view_completeness;
    use rand::seq::SliceRandom;

    // Shuffle validators to distribute load randomly
    let mut shuffled_validators = validators.to_vec();
    shuffled_validators.shuffle(&mut rand::thread_rng());

    for (attempt, validator) in shuffled_validators.iter().enumerate() {
        // Attempt to fetch view data
        let view_data = match fetch_view(view, validator, app_state).await {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!(
                    "Attempt {}/{} failed to fetch view {} from validator {} ({}:{}): {:?}",
                    attempt + 1, shuffled_validators.len(), view, validator.node_id, validator.ip_address, validator.port, e
                );
                continue;
            }
        };

        // Validate that received view matches requested view (prevent Byzantine attacks)
        if view_data.view != view {
            tracing::warn!(
                "Attempt {}/{} view mismatch from validator {} ({}:{}): requested view {}, received view {}",
                attempt + 1, shuffled_validators.len(), validator.node_id, validator.ip_address, validator.port, view, view_data.view
            );
            continue;
        }

        // Validate completeness before returning
        match validate_view_completeness(&view_data, target_view) {
            Ok(_) => {
                tracing::debug!(
                    "Successfully fetched and validated view {} from validator {} (attempt {}/{})",
                    view, validator.node_id, attempt + 1, shuffled_validators.len()
                );
                return Ok(view_data);
            }
            Err(e) => {
                tracing::warn!(
                    "Attempt {}/{} view {} from validator {} failed validation: {:?}",
                    attempt + 1, shuffled_validators.len(), view, validator.node_id, e
                );
                continue;
            }
        }
    }

    // All validators exhausted
    tracing::error!("Exhausted all {} validators for view {}", shuffled_validators.len(), view);
    Err(CatchUpError::NetworkUnavailable)
}

/// Perform catch-up from our current view to the target view
pub async fn perform_catch_up(app_state: &AppState, our_view: i32, target_view: i32) -> Result<(), functions::CatchUpError> {
    use crate::consensus::functions::CatchUpError;
    use std::sync::Arc;

    tracing::info!("Starting catch-up: our_view={}, target_view={}", our_view, target_view);

    const FETCH_BATCH_SIZE: i32 = 50;
    let global_target_view = target_view;

    loop {
        // Re-query database for actual current view (handles incomplete views naturally)
        let consensus_state = db::get_consensus(app_state.db_pool.get())
            .map_err(|_| CatchUpError::Database)?;
        let our_view = consensus_state.view;

        if our_view > global_target_view {
            break; // Caught up beyond target
        }

        // Get available validators (refreshed each batch to pick up newly-added validators)
        let my_node_id = app_state.get_node_id().map_err(|_| CatchUpError::Database)?;
        let validators = db::get_validators(app_state.db_pool.get(), our_view)
            .map_err(|_| CatchUpError::Database)?;
        let other_validators: Vec<_> = validators.into_iter()
            .filter(|v| v.node_id != my_node_id)
            .collect();

        if other_validators.is_empty() {
            tracing::warn!("No other validators found for catch-up at view {}", our_view);
            return Err(CatchUpError::NetworkUnavailable);
        }

        // Determine batch range
        let batch_end = std::cmp::min(our_view + FETCH_BATCH_SIZE - 1, global_target_view);
        let views_to_fetch: Vec<i32> = (our_view..=batch_end).collect();

        tracing::info!(
            "Fetching batch: views {} to {} ({} validators available)",
            our_view, batch_end, other_validators.len()
        );

        // Wrap validators in Arc for efficient sharing across tasks
        let validators = Arc::new(other_validators);

        // Launch parallel fetch tasks for this batch
        let mut fetch_tasks = Vec::new();
        for view in &views_to_fetch {
            let view = *view;
            let validators = Arc::clone(&validators);
            let app_state = app_state.clone();
            let target_view = global_target_view;

            let task = tokio::spawn(async move {
                fetch_and_validate_with_retry(view, target_view, &validators, &app_state).await
            });

            fetch_tasks.push((view, task));
        }

        // Process views sequentially as their data becomes available
        for (expected_view, task) in fetch_tasks {
            match task.await {
                Ok(Ok(view_data)) => {
                    tracing::info!("Processing view {} data", expected_view);

                    match integrate_view(expected_view, view_data, app_state).await {
                        Ok(_) => {
                            tracing::debug!("Successfully integrated view {}", expected_view);
                        }
                        Err(ConsensusError::DatabaseError) => {
                            tracing::error!("Database error integrating view {}", expected_view);
                            return Err(CatchUpError::Database);
                        }
                        Err(ConsensusError::SigningError) | Err(ConsensusError::BlockError) => {
                            tracing::error!("Validation error integrating view {} (invalid QC/TC/block signatures)", expected_view);
                            return Err(CatchUpError::ValidationFailed(expected_view));
                        }
                        Err(e) => {
                            tracing::error!("Unexpected error integrating view {}: {:?}", expected_view, e);
                            return Err(CatchUpError::NetworkUnavailable);
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!("Failed to fetch view {} after all retries: {:?}", expected_view, e);
                    return Err(e);
                }
                Err(e) => {
                    tracing::error!("Fetch task panicked for view {}: {:?}", expected_view, e);
                    return Err(CatchUpError::NetworkUnavailable);
                }
            }
        }
    }

    tracing::info!("Catch-up completed: reached view {}", global_target_view);
    Ok(())
}

/// Ensure we're caught up with the network before participating in consensus
/// This middleware automatically catches up behind nodes and then allows consensus participation
pub async fn ensure_caught_up_middleware(
    State(app_state): State<AppState>,
    Extension(strictness): Extension<CatchUpStrictness>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    match check_view_status(&app_state).await {
        Ok(ViewComparison::Behind { our_view, max_network_view }) => {
            let views_behind = max_network_view - our_view;
            
            // Check if we should skip catch-up based on strictness
            let should_catch_up = match strictness {
                CatchUpStrictness::Strict => true,  // Always catch up
                CatchUpStrictness::Lenient => views_behind > 1,  // Only if more than 1 view behind
            };
            
            if should_catch_up {
                tracing::info!("Node is {} views behind: our_view={}, max_network_view={} - performing catch-up (strictness: {:?})", views_behind, our_view, max_network_view, strictness);
                
                match perform_catch_up(&app_state, our_view, max_network_view).await {
                    Ok(()) => {
                        tracing::info!("Catch-up completed successfully, proceeding with consensus operation");
                        next.run(req).await
                    }
                    Err(e) => {
                        tracing::error!("Catch-up failed: {:?} - rejecting consensus operation", e);
                        StatusCode::SERVICE_UNAVAILABLE.into_response()
                    }
                }
            } else {
                tracing::debug!("Node is {} views behind: our_view={}, max_network_view={} - allowing normal consensus progression (strictness: {:?})", views_behind, our_view, max_network_view, strictness);
                next.run(req).await
            }
        }
        Ok(ViewComparison::CaughtUp { view }) => {
            tracing::debug!("Node is caught up at view {}, proceeding with consensus operation", view);
            next.run(req).await
        }
        Ok(ViewComparison::Ahead { our_view, sampled_max_view }) => {
            tracing::debug!("Node is ahead of sampled validators: our_view={}, sampled_max_view={} - proceeding", our_view, sampled_max_view);
            next.run(req).await
        }
        Err(e) => {
            tracing::error!("Failed to check view status: {:?} - allowing operation to proceed", e);
            // On error, allow operation to proceed (fail open for availability)
            next.run(req).await
        }
    }
}

// Combined middleware that accepts either JWT auth (for users) or RPC auth (for nodes)
pub async fn jwt_or_rpc_auth_middleware(
    State(app_state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    // First try JWT authentication
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                // This looks like JWT auth, let the JWT middleware handle it
                match crate::auth::auth_middleware(State(app_state), req, next).await {
                    Ok(response) => return response.into_response(),
                    Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
                }
            }
        }
    }
    
    // If no JWT, try RPC authentication
    rpc_auth_middleware(State(app_state), req, next).await.into_response()
}

// RPC middleware for verifying node Ed25519 signatures on inter-node requests
pub async fn rpc_auth_middleware(
    State(app_state): State<AppState>,
    mut req: Request<Body>,
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

            // Get node public key from database
            let node_pubkey = match db::get_node_pubkey(
                app_state.db_pool.get(),
                node_id
            ) {
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
                node_pubkey.verify_strict(&body_bytes, &node_signature).map_err(|_| ())?;
                Ok(())
            })();
            
            if verification_result.is_err() {
                tracing::warn!(
                    "RPC signature verification failed for node {}",
                    node_id
                );
                return StatusCode::UNAUTHORIZED.into_response();
            }

            tracing::debug!(
                "RPC signature verified for node {}",
                node_id
            );

            // Reconstruct request with verified node info
            let auth_node = AuthenticatedNode {
                node_id,
            };
            
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

// Route for non-leaders to forward transactions to the leader (pre-authenticated by middleware)
pub async fn post_propose(
    State(app_state): State<AppState>,
    axum::Extension(auth_node): axum::Extension<AuthenticatedNode>,
    Json(transactions): Json<Vec<Transaction>>,
) -> impl IntoResponse {
    tracing::info!(
        "Processing authenticated consensus proposal from node {}",
        auth_node.node_id
    );

    // Process transactions through consensus (already authenticated)
    match crate::consensus::functions::consensus_middleware(&app_state, transactions).await {
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