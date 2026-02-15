use super::*;

use axum::{
    extract::State,
    response::IntoResponse,
    http::StatusCode,
    Json,
};

use crate::db::consensus as db;
use crate::consensus::functions::ConsensusError;
use serde::Serialize;
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
use axum::middleware::Next;
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

// Debug view state - shows node's perspective on a specific view
#[derive(Serialize, Debug)]
pub struct DebugViewState {
    pub node_id: i32,
    pub queried_view: i32,
    pub height_at_view: i32,
    pub is_active_at_height: bool,
    pub validators_at_height: Vec<crate::types::Node>,
    pub leader_for_view: Option<crate::types::Node>,
}

pub async fn debug_view_state(
    State(app_state): State<AppState>,
    Json(view): Json<i32>,
) -> impl IntoResponse {
    // Get database connection and transaction
    let mut conn = match app_state.db_pool.get() {
        Ok(conn) => conn,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get DB connection").into_response(),
    };

    let tx = match conn.transaction() {
        Ok(tx) => tx,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to start transaction").into_response(),
    };

    // Get this node's ID
    let node_id: i32 = match tx.query_row(
        "SELECT node_id FROM this_node WHERE internal_id = 1",
        [],
        |row| row.get(0)
    ) {
        Ok(id) => id,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get node ID").into_response(),
    };

    // Get height at the queried view using existing function
    let height_at_view = match db::get_height_at_view_tx(&tx, view) {
        Ok(h) => h,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get height at view").into_response(),
    };

    // Check if this node is active at that height using existing function
    let is_active = match db::is_node_active(&tx, node_id, height_at_view) {
        Ok(active) => active,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to check activation status").into_response(),
    };

    // Get leader for this view at this height using existing function
    let leader = match db::get_leader_for_view_tx(&tx, view, height_at_view) {
        Ok(l) => l,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get leader for view").into_response(),
    };

    // Drop transaction before calling get_validators (which needs its own connection)
    drop(tx);
    drop(conn);

    // Get list of validators at this height using existing function
    let validators = match db::get_validators(app_state.db_pool.get(), height_at_view) {
        Ok(vals) => vals,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get validators").into_response(),
    };

    let response = DebugViewState {
        node_id,
        queried_view: view,
        height_at_view,
        is_active_at_height: is_active,
        validators_at_height: validators,
        leader_for_view: leader,
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

// Get consensus history showing view progression
pub async fn get_consensus_history(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match db::get_consensus_history(app_state.db_pool.get()) {
        Ok(history) => (StatusCode::OK, Json(history)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get consensus history").into_response(),
    }
}

/// Core logic for processing an incoming ballot.
/// Called by the iroh handler in `consensus::rpc`.
/// Catch-up (cross-view and intra-view) is handled by the handler before dispatch.
/// Returns the signed vote on success.
pub async fn process_incoming_ballot(
    ballot: Ballot,
    app_state: &AppState,
) -> Result<VoteSignMessage, ConsensusError> {
    tracing::debug!(
        "Received ballot for view {} phase {:?} block {:?}",
        ballot.data.view, ballot.data.phase, ballot.block.block_hash
    );

    // Serialize ballot processing to prevent concurrent state modifications
    let _guard = app_state.consensus_lock.lock().await;

    // Bug #5 fix: Insert block BEFORE verification for Propose phase
    // This ensures block exists before ballot.verify_proposal() updates last_propose_vote_block_hash
    if ballot.data.phase == ConsensusPhase::Propose {
        tracing::debug!(
            "Inserting new block {:?} for view {} into database (before verification)",
            ballot.block.block_hash, ballot.data.view
        );
        match db::insert_block(app_state.db_pool.get(), &ballot.block) {
            Ok(()) => {
                tracing::debug!(
                    "Block {:?} inserted successfully",
                    ballot.block.block_hash
                );
            },
            Err(_) => {
                // Block insertion failed - check if it already exists
                match db::get_block(app_state.db_pool.get(), ballot.block.block_hash) {
                    Ok(_existing_block) => {
                        // Block already exists (duplicate ballot request), continue with verification
                        tracing::debug!(
                            "Block {:?} already exists for view {} propose phase, continuing with verification",
                            ballot.block.block_hash, ballot.data.view
                        );
                    },
                    Err(_) => {
                        // Block doesn't exist and couldn't be inserted - real error
                        tracing::error!(
                            "Failed to save block {:?} to database",
                            ballot.block.block_hash
                        );
                        return Err(ConsensusError::DatabaseError);
                    }
                }
            },
        }
    }

    // Validate and sign the ballot
    ballot.verify_proposal(app_state).map_err(|e| {
        tracing::warn!(
            "Ballot rejected for view {} phase {:?}: {:?}",
            ballot.data.view, ballot.data.phase, e
        );
        ConsensusError::SigningError
    })?;

    tracing::debug!(
        "Ballot verified for view {} phase {:?} block {:?}",
        ballot.data.view, ballot.data.phase, ballot.block.block_hash
    );

    let signoff = ballot.sign(app_state).map_err(|e| {
        tracing::error!(
            "Failed to sign ballot for view {} phase {:?}: {:?}",
            ballot.data.view, ballot.data.phase, e
        );
        ConsensusError::SigningError
    })?;

    // Store Lock evidence for potential Lock QC reconstruction via timeout votes
    if ballot.data.phase == ConsensusPhase::Lock {
        *app_state.lock_vote_evidence.lock().unwrap() = Some(LockVoteEvidence {
            vote_data: ballot.data.clone(),
            proposer_signature: ballot.initiator.clone(),
            voter_signature: signoff.clone(),
        });
    }

    tracing::debug!(
        "Ballot signed for view {} phase {:?} block {:?}",
        ballot.data.view, ballot.data.phase, ballot.block.block_hash
    );

    Ok(signoff)
}

/// Core logic for processing an incoming QC.
/// Called by the iroh handler in `consensus::rpc`.
/// Duplicate QCs (already in DB) are acked as Ok(()).
/// Verification failures return Err — the leader uses acks for quorum tracking,
/// so only genuinely applied QCs should count.
pub async fn process_incoming_qc(
    qc: QuorumCertificate,
    app_state: &AppState,
) -> Result<(), ConsensusError> {
    process_incoming_qc_with_guard(qc, app_state, None).await
}

/// Core QC processing logic. Accepts an optional guard to avoid deadlock when
/// the caller already holds consensus_lock (e.g. Lock QC reconstruction path).
pub async fn process_incoming_qc_with_guard(
    qc: QuorumCertificate,
    app_state: &AppState,
    guard: Option<tokio::sync::MutexGuard<'_, ()>>,
) -> Result<(), ConsensusError> {
    tracing::debug!(
        "Received QC for view {} phase {:?} block {:?}",
        qc.view_number, qc.phase, qc.block_hash
    );

    // Use provided guard or acquire lock
    let _guard = match guard {
        Some(g) => g,
        None => app_state.consensus_lock.lock().await,
    };

    let block = match db::get_block(app_state.db_pool.get(), qc.block_hash) {
        Ok(block) => block,
        Err(_) => {
            tracing::warn!(
                "Block {:?} not found for QC verification",
                qc.block_hash
            );
            return Err(ConsensusError::BlockError);
        }
    };

    if let Err(e) = qc.verify(&app_state, &block) {
        tracing::warn!(
            "QC verification failed for view {} phase {:?}: {:?}",
            qc.view_number, qc.phase, e
        );
        return Err(ConsensusError::SigningError);
    }

    // Get database connection and create transaction
    let mut conn = app_state.db_pool.get().map_err(|_| ConsensusError::DatabaseError)?;
    let db_tx = conn.transaction().map_err(|_| ConsensusError::DatabaseError)?;

    // Insert QC (updates consensus state)
    if let Err(_) = db::insert_qc_unsafe_tx(&db_tx, &qc) {
        // QC insertion failed - check if it already exists
        match db::get_quorum_certificate_by_hash(app_state.db_pool.get(), &qc.view_number, &qc.block_hash, &qc.phase) {
            Ok(_existing_qc) => {
                // QC already exists (duplicate broadcast), acknowledge success
                tracing::debug!(
                    "QC for view {} phase {:?} already exists, acknowledging",
                    qc.view_number, qc.phase
                );
                return Ok(());
            },
            Err(_) => {
                tracing::error!(
                    "Failed to insert QC for view {} phase {:?}",
                    qc.view_number, qc.phase
                );
                return Err(ConsensusError::DatabaseError);
            }
        }
    }

    // Process transactions if this is a Lock phase QC (atomically with QC insertion)
    if qc.phase == ConsensusPhase::Lock {
        tracing::info!(
            "Lock phase QC inserted for view {}, processing transactions",
            qc.view_number
        );

        if let Err(e) = crate::consensus::functions::process_transactions(&block.data.transactions, &app_state, true, &db_tx) {
            tracing::error!(
                "Failed to process transactions for view {}: {:?}",
                qc.view_number, e
            );
            return Err(ConsensusError::DatabaseError);
        }
    }

    // Commit transaction (QC insertion + transaction processing)
    db_tx.commit().map_err(|e| {
        tracing::error!(
            "Failed to commit QC/transaction processing for view {} phase {:?}: {:?}",
            qc.view_number, qc.phase, e
        );
        ConsensusError::DatabaseError
    })?;

    tracing::info!(
        "Successfully committed QC for view {} phase {:?}{}",
        qc.view_number,
        qc.phase,
        if qc.phase == ConsensusPhase::Lock { " with transaction processing" } else { "" }
    );

    Ok(())
}

/// Core logic for processing an incoming timeout vote.
/// Called by the iroh handler in `consensus::rpc`.
pub async fn process_incoming_timeout_vote(
    timeout_vote: TimeoutVote,
    app_state: &AppState,
) -> Result<(), ConsensusError> {
    // Optimized intra-view sync: only if incoming vote has higher QC than us
    let our_highest_qc_view = {
        let conn = app_state.db_pool.get().map_err(|_| ConsensusError::DatabaseError)?;
        let consensus_state = db::get_consensus(Ok(conn)).map_err(|_| ConsensusError::DatabaseError)?;
        consensus_state.highest_qc_block.data.view_number
    };

    // Only sync if incoming vote references a higher QC we don't have
    if timeout_vote.data.highest_qc_view > our_highest_qc_view {
        tracing::debug!(
            "Timeout vote references higher QC (vote_qc_view={}, our_qc_view={}) - syncing",
            timeout_vote.data.highest_qc_view, our_highest_qc_view
        );
        if let Err(e) = ensure_intra_view_synced(app_state).await {
            tracing::error!("Intra-view sync failed for timeout vote (view {}): {:?}", timeout_vote.data.view_number, e);
            return Err(ConsensusError::NetworkError);
        }

        // P1: Reissue our timeout vote if we already voted for this view
        // After syncing, our highest_qc_view is now higher, so we should issue a new vote
        // for the new bucket (different data_hash due to different QC reference).
        // The old vote remains in the old bucket - both coexist peacefully.
        // Cascade effect ensures bucket with highest QC reaches quorum first.
        let consensus_state = db::get_consensus(app_state.db_pool.get())
            .map_err(|_| ConsensusError::DatabaseError)?;

        if consensus_state.last_timeout_vote_view == timeout_vote.data.view_number {
            tracing::info!(
                "Reissuing timeout vote for view {} with updated QC (old_qc_view={}, new_qc_view={})",
                timeout_vote.data.view_number,
                our_highest_qc_view,
                consensus_state.highest_qc_block.data.view_number
            );

            // Reissue with updated QC reference (issue_timeout_vote is reissuance-safe)
            use crate::consensus::functions::issue_timeout_vote;
            if let Err(e) = issue_timeout_vote(timeout_vote.data.view_number, app_state, None).await {
                tracing::warn!("Failed to reissue timeout vote for view {}: {:?}", timeout_vote.data.view_number, e);
                // Continue processing incoming vote even if reissuance fails
            }
        }
    } else {
        tracing::trace!(
            "Timeout vote QC not higher than ours (vote_qc_view={}, our_qc_view={}) - skipping sync",
            timeout_vote.data.highest_qc_view, our_highest_qc_view
        );
    }

    match app_state.timeout_vote_collector.add_vote(timeout_vote.clone(), app_state).await {
        Ok(Some(TimeoutResolution::TC(tc))) => {
            // TC was created - apply locally and broadcast in parallel (Layer 2 defense)
            let apply_result = apply_timeout_certificate(tc.clone(), app_state, false, None);
            let broadcast_result = broadcast_timeout_certificate(tc, app_state);

            let (apply_res, broadcast_res) = tokio::join!(apply_result, broadcast_result);

            if let Err(_) = &apply_res {
                return Err(ConsensusError::DatabaseError);
            }
            if let Err(_) = broadcast_res {
                tracing::warn!("Applied TC locally but broadcast failed");
            }
            Ok(())
        }
        Ok(Some(TimeoutResolution::LockQC(qc))) => {
            tracing::info!(
                "Timeout vote collector reconstructed Lock QC for view {} block {:?}",
                qc.view_number, qc.block_hash
            );

            // Apply the reconstructed Lock QC (no guard held in follower path)
            let qc_clone = qc.clone();
            if let Err(e) = process_incoming_qc(qc_clone, app_state).await {
                tracing::error!("Failed to apply reconstructed Lock QC: {:?}", e);
                return Err(ConsensusError::DatabaseError);
            }

            // Broadcast to all validators (fire and forget)
            let app_state_clone = app_state.clone();
            tokio::spawn(async move {
                if let Err(e) = broadcast_quorum_certificate(qc, &app_state_clone).await {
                    tracing::warn!("Failed to broadcast reconstructed Lock QC: {:?}", e);
                }
            });

            Ok(())
        }
        Ok(None) => {
            // Cascade check: if futility detected after adding vote, issue our own timeout vote
            let _ = async {
                // Get committed height
                let height = {
                    let mut conn = app_state.db_pool.get().ok()?;
                    let tx = conn.transaction().ok()?;
                    db::get_current_consensus_height(&tx).ok()?
                };

                // Get validators
                let validators = db::get_validators(app_state.db_pool.get(), height).ok()?;

                // Check futility and cascade if needed (side effect: issues timeout vote if futile)
                // No guard to drop here (cascade doesn't hold consensus_lock)
                use crate::consensus::functions::abort_if_timing_out;
                let _ = abort_if_timing_out(timeout_vote.data.view_number, &validators, app_state, None).await;

                Some(())
            }.await;

            Ok(())
        }
        Err(CertificateError::ValidationError) => Ok(()), // Duplicate vote
        Err(_) => Err(ConsensusError::DatabaseError),
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
    let transport = &app_state.iroh_transport;
    let mut broadcast_tasks = Vec::new();

    for validator in validators {
        let tc_clone = tc.clone();
        let transport = transport.clone();
        let iroh_node_id = validator.pubkey.to_iroh_node_id();
        let node_id = validator.node_id;

        let task = tokio::spawn(async move {
            super::rpc::broadcast_tc(&transport, node_id, iroh_node_id, &tc_clone).await
        });
        broadcast_tasks.push(task);
    }

    // Wait for all broadcasts (but don't fail if some fail)
    for task in broadcast_tasks {
        if let Ok(result) = task.await {
            if let Err(e) = result {
                tracing::debug!("Failed to broadcast TC to node: {:?}", e);
            }
        }
    }

    Ok(())
}

// Helper function to broadcast a quorum certificate to all validators (fire and forget)
pub async fn broadcast_quorum_certificate(
    qc: QuorumCertificate,
    app_state: &AppState,
) -> Result<(), ConsensusError> {
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|_| ConsensusError::DatabaseError)?;
    let committed_height = consensus_state.committed_block.data.height;
    let my_node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;

    let validators: Vec<_> = db::get_validators(app_state.db_pool.get(), committed_height)
        .map_err(|_| ConsensusError::DatabaseError)?
        .into_iter()
        .filter(|n| n.node_id != my_node_id)
        .collect();
    let validators_elect: Vec<_> = db::get_validators_elect(app_state.db_pool.get(), committed_height)
        .unwrap_or_default()
        .into_iter()
        .filter(|n| n.node_id != my_node_id)
        .collect();

    crate::consensus::functions::broadcast_qc(&validators, &validators_elect, qc, app_state)
        .await
        .map_err(|_| ConsensusError::DatabaseError)
}

#[derive(Debug)]
pub enum ViewComparison {
    Behind { our_view: i32, max_network_view: i32 },
    CaughtUp { view: i32 },
    Ahead { our_view: i32, sampled_max_view: i32 },
}

#[derive(Debug, Clone, Copy)]
pub enum CatchUpMode {
    SingleShot,    // Fast path for small gaps (active validators receiving messages)
    Convergence,   // Iterative for large gaps, bootstrap, or extended downtime
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStatus {
    CaughtUp,                      // Fully caught up with network
    WithinTolerance { gap: i32 },  // Slightly behind but within acceptable tolerance
    Behind { gap: i32 },           // Too far behind, catch-up was needed/failed
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeReadiness {
    pub sync_status: SyncStatus,
    pub is_active: bool,
}

/// Check if we're caught up with the network by polling a subset of validators
/// Returns the view comparison result for decision making
pub async fn check_view_status(app_state: &AppState) -> Result<ViewComparison, ConsensusError> {
    use crate::consensus::functions;
    
    // Get our current view (single DB call)
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|_| ConsensusError::DatabaseError)?;
    let our_view = consensus_state.view;
    let our_height = consensus_state.committed_block.data.height;

    // Poll network for max view
    let max_network_view = functions::poll_subset_for_max_view(app_state, our_view, our_height, None).await?;
    
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
    let iroh_node_id = validator.pubkey.to_iroh_node_id();

    tracing::debug!("Fetching view {} data from validator {}", view, validator.node_id);

    let view_data = super::rpc::fetch_view_data(
        &app_state.iroh_transport, validator.node_id, iroh_node_id, view,
    ).await.map_err(|e| {
        tracing::warn!("Failed to fetch view {} from validator {}: {:?}", view, validator.node_id, e);
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
        match db::insert_block(app_state.db_pool.get(), block) {
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

            // Now safe (or verify skipped for genesis)

            // Get database connection and create transaction for Propose QC
            let mut conn = app_state.db_pool.get().map_err(|_| ConsensusError::DatabaseError)?;
            let db_tx = conn.transaction().map_err(|_| ConsensusError::DatabaseError)?;

            match db::insert_qc_unsafe_tx(&db_tx, &propose_qc) {
                Ok(_) => {
                    // Commit Propose QC insertion
                    db_tx.commit().map_err(|_| ConsensusError::DatabaseError)?;

                    if is_genesis {
                        tracing::debug!("Inserted genesis propose QC for view 0 (no verification)");
                    } else {
                        tracing::debug!("Validated and inserted propose QC for view {}", view);
                    }
                }
                Err(_) => {
                    // QC insertion failed - check if it already exists
                    match db::get_quorum_certificate_by_hash(app_state.db_pool.get(), &propose_qc.view_number, &propose_qc.block_hash, &propose_qc.phase) {
                        Ok(_existing_qc) => {
                            tracing::debug!("Propose QC for view {} already exists", view);
                        }
                        Err(_) => {
                            tracing::error!("Failed to insert propose QC for view {}", view);
                            return Err(ConsensusError::DatabaseError);
                        }
                    }
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

            // Now safe (or verify skipped for genesis)

            // Get database connection and create transaction for Lock QC
            let mut conn = app_state.db_pool.get().map_err(|_| ConsensusError::DatabaseError)?;
            let db_tx = conn.transaction().map_err(|_| ConsensusError::DatabaseError)?;

            match db::insert_qc_unsafe_tx(&db_tx, &lock_qc) {
                Ok(_) => {
                    // Process transactions if this is a Lock phase QC (atomically with QC insertion)
                    if lock_qc.phase == ConsensusPhase::Lock {
                        tracing::info!(
                            "Lock phase QC inserted for view {}, processing transactions",
                            lock_qc.view_number
                        );

                        if let Err(e) = crate::consensus::functions::process_transactions(&block.data.transactions, app_state, true, &db_tx) {
                            tracing::error!(
                                "Failed to process transactions for view {}: {:?}",
                                lock_qc.view_number, e
                            );
                            // Transaction auto-rolls back, consensus state changes are discarded
                            return Err(ConsensusError::DatabaseError);
                        }
                    }

                    // Commit transaction (QC insertion + transaction processing)
                    db_tx.commit().map_err(|_| ConsensusError::DatabaseError)?;

                    if is_genesis {
                        tracing::debug!("Inserted genesis lock QC for view 0 (no verification)");
                    } else {
                        tracing::info!(
                            "Successfully committed lock QC for view {}{}",
                            view,
                            if lock_qc.phase == ConsensusPhase::Lock { " with transaction processing" } else { "" }
                        );
                    }
                }
                Err(_) => {
                    // QC insertion failed - check if it already exists
                    match db::get_quorum_certificate_by_hash(app_state.db_pool.get(), &lock_qc.view_number, &lock_qc.block_hash, &lock_qc.phase) {
                        Ok(_existing_qc) => {
                            tracing::debug!("Lock QC for view {} already exists", view);
                        }
                        Err(_) => {
                            tracing::error!("Failed to insert lock QC for view {}", view);
                            return Err(ConsensusError::DatabaseError);
                        }
                    }
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

        match db::insert_tc_safe(app_state, tc.clone()) {
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

/// Fetch and validate view data from quorum (query all, break when quorum responds)
///
/// Queries ALL validators in parallel, returns as soon as required valid responses received.
/// Takes MAX QC from valid responses for Byzantine resistance.
///
/// `required_responses`: explicit threshold (typically network_quorum - 1, since caller counts themselves)
async fn fetch_and_validate_from_quorum(
    view: i32,
    validators: &[crate::types::Node],
    required_responses: usize,
    app_state: &AppState,
) -> Result<ViewConsensusData, functions::CatchUpError> {
    use crate::consensus::functions::CatchUpError;
    use crate::consensus::types::validate_view_completeness;

    tracing::debug!(
        "Quorum-based fetch for view {}: querying {} validators (need {} valid responses)",
        view, validators.len(), required_responses
    );

    // Launch parallel fetch tasks to ALL validators
    let mut fetch_tasks = Vec::new();
    for validator in validators {
        let validator = validator.clone();
        let app_state = app_state.clone();

        let task = tokio::spawn(async move {
            (validator.node_id, fetch_view(view, &validator, &app_state).await)
        });

        fetch_tasks.push(task);
    }

    // Collect responses until we have quorum valid responses
    let mut valid_responses = Vec::new();
    let mut completed = 0;

    for task in fetch_tasks {
        match task.await {
            Ok((node_id, Ok(view_data))) => {
                completed += 1;

                // Validate view number matches
                if view_data.view != view {
                    tracing::debug!(
                        "View mismatch from validator {}: requested {}, received {}",
                        node_id, view, view_data.view
                    );
                    continue;
                }

                // Validate completeness
                if let Err(e) = validate_view_completeness(&view_data, view) {
                    tracing::debug!(
                        "Incomplete view {} data from validator {}: {:?}",
                        view, node_id, e
                    );
                    continue;
                }

                tracing::debug!(
                    "Validator {} provided valid view {} data (propose_QC={}, lock_QC={}, TC={})",
                    node_id, view,
                    view_data.propose_qc.is_some(),
                    view_data.lock_qc.is_some(),
                    view_data.timeout_certificate.is_some()
                );

                valid_responses.push(view_data);

                // Break early if we have required responses
                if valid_responses.len() >= required_responses {
                    tracing::info!(
                        "Reached required threshold for view {}: {} valid responses from {} completed queries",
                        view, valid_responses.len(), completed
                    );
                    break;
                }
            }
            Ok((node_id, Err(e))) => {
                completed += 1;
                tracing::debug!("Failed to fetch view {} from validator {}: {:?}", view, node_id, e);
            }
            Err(e) => {
                completed += 1;
                tracing::debug!("Fetch task panicked: {:?}", e);
            }
        }
    }

    if valid_responses.len() < required_responses {
        tracing::error!(
            "Failed to reach required threshold for view {}: only {} valid responses (needed {})",
            view, valid_responses.len(), required_responses
        );
        return Err(CatchUpError::NetworkUnavailable);
    }

    // Find response with highest QC (Lock > Propose > none)
    // This protects against withholding attacks by taking MAX QC seen
    let best_response = valid_responses.into_iter()
        .max_by_key(|vd| {
            if vd.lock_qc.is_some() {
                (2, vd.lock_qc.as_ref().unwrap().view_number)
            } else if vd.propose_qc.is_some() {
                (1, vd.propose_qc.as_ref().unwrap().view_number)
            } else {
                (0, 0)
            }
        })
        .ok_or(CatchUpError::NetworkUnavailable)?;

    tracing::info!(
        "Selected view {} data with highest QC: propose_QC={}, lock_QC={}, TC={}",
        view,
        best_response.propose_qc.is_some(),
        best_response.lock_qc.is_some(),
        best_response.timeout_certificate.is_some()
    );

    Ok(best_response)
}

/// Fetch and validate view data with retry logic (sequential, for batch catch-up)
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
                    "Attempt {}/{} failed to fetch view {} from validator {}: {:?}",
                    attempt + 1, shuffled_validators.len(), view, validator.node_id, e
                );
                continue;
            }
        };

        // Validate that received view matches requested view (prevent Byzantine attacks)
        if view_data.view != view {
            tracing::warn!(
                "Attempt {}/{} view mismatch from validator {}: requested view {}, received view {}",
                attempt + 1, shuffled_validators.len(), validator.node_id, view, view_data.view
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

/// Perform catch-up with convergence loop to handle moving target problem
///
/// Repeatedly catches up and re-checks network height until converged within tolerance.
/// This is critical for new node bootstrap where the network may progress significantly
/// during the initial catch-up from genesis.
pub async fn perform_catch_up_with_convergence(
    app_state: &AppState,
    bootstrap_validators: Option<&[crate::types::Node]>,
) -> Result<(), functions::CatchUpError> {
    use crate::consensus::functions::CatchUpError;

    const MAX_CONVERGENCE_ITERATIONS: u32 = 10;
    const CONVERGENCE_TOLERANCE: i32 = 1;  // Must be <2 to ensure genesis is always processed

    for iteration in 1..=MAX_CONVERGENCE_ITERATIONS {
        // Query current view and height in a single transaction for consistency
        // Works even before processing genesis when there are no validators
        let (our_view, our_height) = {
            let mut conn = app_state.db_pool.get().map_err(|_| CatchUpError::Database)?;
            let tx = conn.transaction().map_err(|_| CatchUpError::Database)?;
            let view = db::get_current_view_tx(&tx).map_err(|_| CatchUpError::Database)?;
            let height = db::get_current_consensus_height(&tx).map_err(|_| CatchUpError::Database)?;
            (view, height)
        };

        // Poll network height
        let network_view = functions::poll_subset_for_max_view(app_state, our_view, our_height, bootstrap_validators)
            .await
            .map_err(|_| CatchUpError::NetworkUnavailable)?;

        let gap = network_view - our_view;

        // Check if converged
        if gap <= CONVERGENCE_TOLERANCE {
            tracing::info!(
                "Converged after {} iteration(s): within {} view(s) of network (our view: {}, network: {})",
                iteration, gap, our_view, network_view
            );
            return Ok(());
        }

        // Not converged, perform catch-up iteration
        tracing::info!(
            "Catch-up iteration {}/{}: closing gap of {} views (from view {} to {})",
            iteration, MAX_CONVERGENCE_ITERATIONS, gap, our_view, network_view
        );

        perform_catch_up(app_state, our_view, network_view, bootstrap_validators).await?;
    }

    // Failed to converge within max iterations
    tracing::error!(
        "Failed to converge after {} iterations - network may be progressing faster than catch-up rate",
        MAX_CONVERGENCE_ITERATIONS
    );
    Err(CatchUpError::NetworkUnavailable)
}

/// Unified entry point: catch up with network and optionally request activation if inactive
/// Returns node readiness including sync status and activation status
pub async fn ensure_caught_up_and_active(
    app_state: &AppState,
    mode: CatchUpMode,
    request_activation_if_needed: bool,
    tolerance_views: i32,
    bootstrap_validators: Option<&[crate::types::Node]>,
) -> Result<NodeReadiness, functions::CatchUpError> {
    use crate::consensus::functions::CatchUpError;

    // Serialize all catch-up operations to prevent concurrent consensus state modifications
    // Lock is scoped to catch-up operations only (released before activation request)
    let sync_status = {
        let _guard = app_state.consensus_lock.lock().await;

        let mut sync_status = SyncStatus::CaughtUp;

        // Perform appropriate catch-up based on mode
        match mode {
            CatchUpMode::SingleShot => {
                // Fast path: check if behind and perform single catch-up pass
                match check_view_status(app_state).await {
                    Ok(ViewComparison::Behind { our_view, max_network_view }) => {
                        let gap = max_network_view - our_view;

                        if gap > tolerance_views {
                            tracing::info!("Single-shot catch-up: closing gap of {} views (from {} to {})", gap, our_view, max_network_view);
                            perform_catch_up(app_state, our_view, max_network_view, bootstrap_validators).await?;
                            sync_status = SyncStatus::CaughtUp;
                        } else {
                            tracing::debug!("Gap of {} views within tolerance {}, skipping catch-up", gap, tolerance_views);
                            sync_status = SyncStatus::WithinTolerance { gap };
                        }
                    }
                    Ok(ViewComparison::CaughtUp { view }) => {
                        tracing::debug!("Already caught up at view {}", view);
                        sync_status = SyncStatus::CaughtUp;
                    }
                    Ok(ViewComparison::Ahead { our_view, sampled_max_view }) => {
                        tracing::debug!("Ahead of sampled validators: our_view={}, sampled_max_view={}", our_view, sampled_max_view);
                        sync_status = SyncStatus::CaughtUp;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to check view status during single-shot catch-up: {:?}", e);
                        return Err(CatchUpError::NetworkUnavailable);
                    }
                }
            }
            CatchUpMode::Convergence => {
                // Iterative convergence for large gaps or bootstrap (tolerance ignored)
                perform_catch_up_with_convergence(app_state, bootstrap_validators).await?;
                sync_status = SyncStatus::CaughtUp;
            }
        }

        sync_status
    }; // Lock released here - safe for activation request which takes its own lock

    // Check if we're active at current height
    // Scope database work to ensure transaction is dropped before async calls
    let (node_id, current_height, is_active) = {
        let mut conn = app_state.db_pool.get().map_err(|_| CatchUpError::Database)?;
        let tx = conn.transaction().map_err(|_| CatchUpError::Database)?;

        // Get node ID directly from transaction
        let node_id: i32 = tx.query_row(
            "SELECT node_id FROM this_node WHERE internal_id = 1",
            [],
            |row| row.get(0)
        ).map_err(|_| CatchUpError::Database)?;

        let current_height = db::get_current_consensus_height(&tx).map_err(|_| CatchUpError::Database)?;
        let is_active = db::is_node_active(&tx, node_id, current_height).map_err(|_| CatchUpError::Database)?;

        (node_id, current_height, is_active)
        // tx and conn automatically dropped here at end of scope
    };

    // If inactive and caller wants us to request activation
    if !is_active && request_activation_if_needed {
        tracing::info!(
            "Node {} is inactive at height {}, requesting activation",
            node_id,
            current_height
        );

        if let Err(e) = request_activation(app_state, node_id, current_height).await {
            tracing::warn!("Failed to request activation: {:?}", e);
            // Don't fail the whole operation - we still caught up
        }
    }

    Ok(NodeReadiness {
        sync_status,
        is_active,
    })
}

/// Request activation for this node - effective height computed automatically during execution
async fn request_activation(
    app_state: &AppState,
    node_id: i32,
    current_height: i32,
) -> Result<(), functions::CatchUpError> {
    use crate::consensus::handlers::ActivationRequest;
    use crate::consensus::functions::{create_signed_transaction, consensus_middleware, CatchUpError};

    // Create activation request (effective height computed deterministically during execution)
    let activation_req = ActivationRequest {
        node_id,
        current_height,
    };

    // Serialize to payload
    let payload = bincode::serde::encode_to_vec(&activation_req, bincode::config::standard())
        .map_err(|_| CatchUpError::NetworkUnavailable)?;

    // Create signed transaction
    let transaction = create_signed_transaction(
        app_state,
        "validator_activation".to_string(),
        payload,
    ).map_err(|_| CatchUpError::NetworkUnavailable)?;

    // Submit activation transaction via consensus
    consensus_middleware(app_state, vec![transaction])
        .await
        .map_err(|_| CatchUpError::NetworkUnavailable)?;

    tracing::info!(
        "Activation request submitted for node {} (effective height will be computed during execution)",
        node_id
    );

    Ok(())
}

/// Ensure we have all QCs within the current view (quorum-based intra-view sync)
///
/// This function addresses a critical gap in view-level synchronization:
/// - View-level sync only ensures we're at the right view number
/// - But within a view, we could be missing the Propose QC (or Lock QC)
/// - This causes Lock ballot rejection when highest_qc_block doesn't match
///
/// Byzantine resistance:
/// - Queries ALL validators in parallel
/// - Breaks early when (quorum - 1) respond (we count ourselves as part of quorum)
/// - Takes MAX QC from valid responses (protects against withholding attacks)
/// - ~99.99999% probability of discovering Lock QC if any quorum knows about it
///
/// Example failure without intra-view sync:
/// - Node at view 9 (view-level sync ✓)
/// - But missed Propose ballot → highest_qc_block is from view 8
/// - Receives Lock ballot for block X (view 9)
/// - ballot.verify_proposal() checks: X == highest_qc_block.hash? NO
/// - Ballot rejected with LockPhaseQcMismatch
pub async fn ensure_intra_view_synced(
    app_state: &AppState,
) -> Result<(), functions::CatchUpError> {
    use crate::consensus::functions::CatchUpError;
    use crate::consensus::types::calculate_quorum_threshold;

    // Get current consensus state
    let (current_view, highest_qc_view) = {
        let conn = app_state.db_pool.get().map_err(|_| CatchUpError::Database)?;
        let consensus_state = db::get_consensus(Ok(conn)).map_err(|_| CatchUpError::Database)?;
        (consensus_state.view, consensus_state.highest_qc_block.data.view_number)
    };

    // Check if we're missing the Propose QC within current view
    if highest_qc_view < current_view {
        tracing::debug!(
            "Intra-view sync required: current_view={}, highest_qc_view={} - querying quorum",
            current_view,
            highest_qc_view
        );

        // Get validators at committed height to query (exclude ourselves)
        let validators = {
            let mut conn = app_state.db_pool.get().map_err(|_| CatchUpError::Database)?;
            let tx = conn.transaction().map_err(|_| CatchUpError::Database)?;
            let height = db::get_current_consensus_height(&tx).map_err(|_| CatchUpError::Database)?;
            let my_node_id = app_state.get_node_id().map_err(|_| CatchUpError::Database)?;

            let all_validators = db::get_validators(Ok(app_state.db_pool.get().map_err(|_| CatchUpError::Database)?), height)
                .map_err(|_| CatchUpError::Database)?;

            all_validators.into_iter()
                .filter(|v| v.node_id != my_node_id)
                .collect::<Vec<_>>()
        };

        if validators.is_empty() {
            // Single-node network: no other validators to sync with
            // This is valid - we are the only validator and source of truth
            tracing::debug!("Single-node network detected, skipping intra-view sync");
            return Ok(());
        }

        // Calculate quorum based on total network size (including ourselves)
        let total_validator_count = validators.len() + 1;
        let network_quorum = calculate_quorum_threshold(total_validator_count);

        // We need (quorum - 1) responses from others since we count ourselves
        let required_responses = network_quorum - 1;

        tracing::debug!(
            "Intra-view sync: total_validators={}, network_quorum={}, querying {} others (need {} responses)",
            total_validator_count, network_quorum, validators.len(), required_responses
        );

        // Quorum-based fetch: query all, break when threshold met, take MAX QC
        let view_data = fetch_and_validate_from_quorum(
            current_view,
            &validators,
            required_responses,
            app_state
        ).await?;

        // Integrate the fetched data (includes highest QC discovered)
        integrate_view(current_view, view_data, app_state).await
            .map_err(|_| CatchUpError::ValidationFailed(current_view))?;

        tracing::debug!(
            "Intra-view sync completed: integrated view {} data with highest QC from quorum",
            current_view
        );
    } else {
        tracing::trace!(
            "Intra-view sync not needed: highest_qc_view={} >= current_view={}",
            highest_qc_view,
            current_view
        );
    }

    Ok(())
}

/// Perform catch-up from our current view to the target view
///
/// For new nodes bootstrapping from genesis, `bootstrap_validators` provides the initial
/// set of validators to fetch consensus data from. As catch-up progresses, newly-integrated
/// validators from the database are merged with bootstrap validators for subsequent batches.
pub async fn perform_catch_up(
    app_state: &AppState,
    our_view: i32,
    target_view: i32,
    bootstrap_validators: Option<&[crate::types::Node]>,
) -> Result<(), functions::CatchUpError> {
    use crate::consensus::functions::CatchUpError;
    use std::sync::Arc;
    use std::collections::HashSet;

    tracing::info!("Starting catch-up: our_view={}, target_view={}", our_view, target_view);

    const FETCH_BATCH_SIZE: i32 = 50;
    let global_target_view = target_view;

    loop {
        // Re-query database for actual current view and height (handles incomplete views naturally)
        let (our_view, our_height) = {
            let mut conn = app_state.db_pool.get().map_err(|_| CatchUpError::Database)?;
            let tx = conn.transaction().map_err(|_| CatchUpError::Database)?;
            let view = db::get_current_view_tx(&tx).map_err(|_| CatchUpError::Database)?;
            let height = db::get_current_consensus_height(&tx).map_err(|_| CatchUpError::Database)?;
            (view, height)
        };

        if our_view >= global_target_view {
            break; // Caught up to or beyond target
        }

        // Get available validators (refreshed each batch to pick up newly-added validators)
        let my_node_id = app_state.get_node_id().map_err(|_| CatchUpError::Database)?;
        let mut validators = db::get_validators(app_state.db_pool.get(), our_height)
            .map_err(|_| CatchUpError::Database)?;

        // Merge with bootstrap validators, removing duplicates by node_id
        if let Some(bootstrap) = bootstrap_validators {
            let existing_ids: HashSet<i32> = validators.iter().map(|v| v.node_id).collect();
            validators.extend(
                bootstrap.iter()
                    .filter(|node| !existing_ids.contains(&node.node_id))
                    .cloned()
            );
        }

        let other_validators: Vec<_> = validators.into_iter()
            .filter(|v| v.node_id != my_node_id)
            .collect();

        if other_validators.is_empty() {
            tracing::warn!("No validators available for catch-up at view {}", our_view);
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

// Helper function to apply timeout certificate and advance consensus view
pub async fn apply_timeout_certificate(
    tc: TimeoutCertificate,
    app_state: &AppState,
    skip_wait: bool,
    guard: Option<tokio::sync::MutexGuard<'_, ()>>,
) -> Result<(), CertificateError> {
    if app_state.test_mode {
        app_state.consensus_barriers.wait(
            crate::consensus::barriers::names::BEFORE_TC_GST_WAIT
        ).await;
    }

    // Layer 2: Post-TC bounded wait - if not skipping, wait GST for potential Lock QC arrival
    // IMPORTANT: For validator path (no guard), don't hold lock during wait to allow Lock QC to be processed
    // For leader path (with guard), hold lock through wait to synchronize with validators' GST
    if !skip_wait {
        const GST_MS: u64 = 500; // Global Stabilization Time assumption
        tracing::debug!(
            "Layer 2: Post-TC bounded wait - sleeping {}ms for potential Lock QC arrival (view {})",
            GST_MS, tc.view_number
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(GST_MS)).await;
    }

    // Use provided guard or acquire lock AFTER GST wait
    let _guard = match guard {
        Some(g) => g,
        None => app_state.consensus_lock.lock().await,
    };

    // Get current consensus state to validate TC view
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|_| CertificateError::DatabaseError)?;

    // TC must be for our current view to maintain chain consistency
    if tc.view_number != consensus_state.view {
        if tc.view_number < consensus_state.view {
            tracing::debug!("TC for view {} became stale during wait (current view: {}) - Lock QC likely won", tc.view_number, consensus_state.view);
            return Err(CertificateError::ValidationError);
        } else {
            tracing::warn!("Rejecting TC for future view {} (current view: {})", tc.view_number, consensus_state.view);
            return Err(CertificateError::ValidationError);
        }
    }

    // Layer 3: Quorum-based intra-view sync - actively verify no Lock QC exists
    // This complements GST wait (passive) with active quorum verification
    tracing::debug!(
        "Layer 3: Pre-TC quorum check - querying quorum to verify no Lock QC exists for view {}",
        tc.view_number
    );
    if let Err(e) = ensure_intra_view_synced(app_state).await {
        tracing::error!("Quorum check failed before applying TC for view {}: {:?}", tc.view_number, e);
        return Err(CertificateError::DatabaseError);
    }

    // Re-validate TC is still for our current view (might have changed during quorum check)
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|_| CertificateError::DatabaseError)?;

    if tc.view_number != consensus_state.view {
        if tc.view_number < consensus_state.view {
            tracing::info!("TC for view {} became stale during quorum check (current view: {}) - Lock QC discovered and applied", tc.view_number, consensus_state.view);
            return Err(CertificateError::ValidationError);
        } else {
            tracing::warn!("View advanced unexpectedly during quorum check: TC view {}, current view {}", tc.view_number, consensus_state.view);
            return Err(CertificateError::ValidationError);
        }
    }

    tracing::debug!("Quorum check complete - no Lock QC found, safe to apply TC for view {}", tc.view_number);

    if app_state.test_mode {
        app_state.consensus_barriers.wait(
            crate::consensus::barriers::names::BEFORE_TC_APPLICATION
        ).await;
    }

    // Store the TC in database with QC validation (Bug #6 and #7 fixes)
    match db::insert_tc_safe(app_state, tc.clone()) {
        Ok(_) => {
            tracing::info!("Applied timeout certificate for view {}", tc.view_number);
            Ok(())
        }
        Err(_) => Err(CertificateError::DatabaseError),
    }
}

/// GET /debug/state - Get hash-based state snapshot for divergence detection
pub async fn get_state_snapshot(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match crate::db::debug::compute_state_snapshot(app_state.db_pool.get()) {
        Ok(internal_snapshot) => {
            // Convert internal (Blake3Hash) to wire format (String)
            let wire_snapshot: hopnet_common::StateSnapshot = internal_snapshot.into();
            (axum::http::StatusCode::OK, Json(wire_snapshot)).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to compute state snapshot: {:?}", e);
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "Failed to compute state snapshot").into_response()
        }
    }
}