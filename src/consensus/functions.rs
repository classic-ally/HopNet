use super::*;

use crate::{db::consensus as db, handlers::HandlerResult};
use crate::db::MyNode;
use crate::types::Node;
use crate::DISPATCH_TABLE;
use tokio::sync::mpsc;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand_core::OsRng;
use std::collections::HashMap;
use tokio::sync::Mutex;
use rand::prelude::*;


pub fn generate_ed25519_key() -> (SigningKey, VerifyingKey) {
    let mut csprng = OsRng;
    let private_key= SigningKey::generate(&mut csprng);
    let public_key = private_key.verifying_key();

    return (private_key, public_key);
}

/// Force DuckDB to flush its write-ahead log and clear transaction metadata
/// Avoids write-write conflicts when performing high-speed state transition updates on singleton table
/// Uses FORCE CHECKPOINT to wait for any active transactions to complete
pub fn checkpoint_connection(conn: &duckdb::Connection) -> Result<(), ConsensusError> {
    conn.execute_batch("FORCE CHECKPOINT").map_err(|e| {
        tracing::error!("Failed to force checkpoint connection: {:?}", e);
        ConsensusError::DatabaseError
    })?;
    Ok(())
}

#[derive (Debug)]
pub enum ConsensusError {
    InsufficientVotes,
    BlockError,
    DatabaseError,
    SigningError,
    TimeoutError,
    MalformedReply,
    ThreadError,
    ForwardingError,
    NetworkError,
    NetworkTimeout,  // Network is timing out, leader should abandon
}

#[derive(Debug)]
pub enum CatchUpError {
    NetworkUnavailable,      // All validators unreachable/failing
    ValidationFailed(i32),   // View failed validation (for logging)
    Database,                // Database error
}

/// Check if the network is timing out for a given view
///
/// Returns Ok(()) if safe to proceed, Err(ConsensusError::NetworkTimeout) if insufficient nodes available.
/// This function can be called at multiple points in the consensus pipeline to abort early
/// when the network has already moved to timeout for this view.
///
/// The check is: if (validators.len() - timeout_count) < quorum_threshold, then we cannot
/// possibly collect enough votes to form a QC, so leader should abandon.
pub async fn check_leader_abandonment(
    view: i32,
    validators: &[Node],
    app_state: &AppState
) -> Result<(), ConsensusError> {
    let timeout_count = app_state.timeout_vote_collector.get_vote_count(view).await;
    let quorum_threshold = crate::consensus::types::calculate_quorum_threshold(validators.len());

    // Calculate how many nodes are still available to vote (haven't timed out)
    let available_votes = validators.len().saturating_sub(timeout_count);

    // If we can't possibly get enough votes, abandon
    if available_votes < quorum_threshold {
        tracing::warn!(
            "Leader abandonment: insufficient nodes available for view {} ({} available < {} needed, {} timed out)",
            view, available_votes, quorum_threshold, timeout_count
        );
        return Err(ConsensusError::NetworkTimeout);
    }

    Ok(())
}

/// Issue a timeout vote for the given view (unified timeout logic)
///
/// Called in four scenarios:
/// 1. Cron job: view hasn't progressed (natural timeout)
/// 2. Leader: futility detected before starting work (early abandonment)
/// 3. Follower: futility detected after receiving timeout vote (cascade)
/// 4. Reissuance: after syncing to higher QC or periodic resilience
///
/// Creates timeout vote from current consensus state (picks up latest QC if synced),
/// adds to collector, and if TC forms, applies and broadcasts.
/// Reissuance-safe: calling multiple times with updated QC creates votes for different buckets.
pub async fn issue_timeout_vote(
    view: i32,
    app_state: &AppState,
    guard: Option<tokio::sync::MutexGuard<'_, ()>>,
) -> Result<(), ConsensusError> {
    // Get current consensus state (reissuance-safe: picks up latest QC if synced)
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|_| ConsensusError::DatabaseError)?;

    tracing::info!("Issuing timeout vote for view {}", view);

    // Create timeout data for this view
    let timeout_data = TimeoutSignData::from_consensus_state(view, &consensus_state);

    // Get our node_id
    let my_node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;

    // Sign the timeout data using AppState's private key
    let signature = timeout_data.sign(&app_state.private_key)
        .map_err(|_| ConsensusError::SigningError)?;

    // Check if we have Lock evidence for this view (for Lock QC reconstruction)
    let lock_evidence = {
        let ev = app_state.lock_vote_evidence.lock().unwrap();
        match &*ev {
            Some(ev) if ev.vote_data.view == view => Some(ev.clone()),
            _ => None,
        }
    };

    // Create the timeout vote
    let timeout_vote = TimeoutVote {
        sender: VoteSignMessage {
            replica_id: my_node_id,
            signature,
        },
        data: timeout_data,
        lock_vote_evidence: lock_evidence,
    };

    // Mark as issued in DB (critical: prevents duplicate on retry)
    db::mark_timeout_vote_issued(app_state.db_pool.get(), view)
        .map_err(|_| ConsensusError::DatabaseError)?;

    // Add to collector (might form TC or reconstruct Lock QC)
    match app_state.timeout_vote_collector.add_vote(timeout_vote.clone(), app_state).await {
        Ok(Some(TimeoutResolution::TC(tc))) => {
            tracing::info!("Timeout vote for view {} formed TC, applying and broadcasting", view);

            // Apply and broadcast TC (Layer 2 defense pattern from routes.rs)
            use crate::consensus::routes::{apply_timeout_certificate, broadcast_timeout_certificate};
            let apply_result = apply_timeout_certificate(tc.clone(), app_state, false, guard);
            let broadcast_result = broadcast_timeout_certificate(tc, app_state);
            let (apply_res, broadcast_res) = tokio::join!(apply_result, broadcast_result);

            // Log failures but don't fail the function (TC formed successfully)
            if let Err(e) = apply_res {
                tracing::error!("Failed to apply TC for view {}: {:?}", view, e);
            }
            if let Err(e) = broadcast_res {
                tracing::warn!("Failed to broadcast TC for view {}: {:?}", view, e);
            }

            Ok(())
        }
        Ok(Some(TimeoutResolution::LockQC(qc))) => {
            tracing::info!(
                "Timeout vote for view {} reconstructed Lock QC for block {:?}, applying and broadcasting",
                view, qc.block_hash
            );

            // Apply the reconstructed Lock QC while holding the guard (prevents TC race)
            use crate::consensus::routes::{process_incoming_qc_with_guard, broadcast_quorum_certificate};
            let qc_clone = qc.clone();
            if let Err(e) = process_incoming_qc_with_guard(qc_clone, app_state, guard).await {
                tracing::error!("Failed to apply reconstructed Lock QC for view {}: {:?}", view, e);
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
            tracing::debug!("Timeout vote for view {} added, broadcasting to peers", view);

            // Broadcast our timeout vote to other validators (fire and forget)
            // Use committed height, not view (view can diverge from height due to timeouts)
            let committed_height = consensus_state.committed_block.data.height;
            let validators = db::get_validators(app_state.db_pool.get(), committed_height)
                .map_err(|_| ConsensusError::DatabaseError)?
                .into_iter()
                .filter(|node| node.node_id != my_node_id)
                .collect::<Vec<_>>();

            let transport = app_state.iroh_transport.clone();
            for validator in validators {
                let transport = transport.clone();
                let timeout_vote_clone = timeout_vote.clone();
                let iroh_node_id = validator.pubkey.to_iroh_node_id();
                let node_id = validator.node_id;

                tokio::spawn(async move {
                    if let Err(e) = super::rpc::broadcast_timeout_vote(
                        &transport, node_id, iroh_node_id, &timeout_vote_clone,
                    ).await {
                        tracing::warn!("Failed to send timeout vote to node {}: {:?}", node_id, e);
                    }
                });
            }

            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to add timeout vote to collector for view {}: {:?}", view, e);
            Err(ConsensusError::SigningError)
        }
    }
}

/// Check if network is timing out for this view, and if so, issue timeout vote and abort
///
/// Combines check_leader_abandonment + issue_timeout_vote pattern for repeated use
/// throughout consensus pipeline (before block creation, before each ballot round, etc).
///
/// Returns Ok(guard) with the guard passed back if safe to proceed.
/// Returns Err(ConsensusError::NetworkTimeout) if futile (guard is dropped before issuing vote).
///
/// The guard threading pattern prevents deadlock: when futility is detected, the guard is
/// dropped before calling issue_timeout_vote (which may call apply_timeout_certificate that
/// acquires the lock). In the happy path, the guard is returned to the caller to maintain
/// lock semantics.
///
/// Performance: ~50-100μs in happy path, ~2-10ms first timeout, ~1-5ms subsequent (idempotent)
pub async fn abort_if_timing_out<'a>(
    view: i32,
    validators: &[Node],
    app_state: &AppState,
    guard: Option<tokio::sync::MutexGuard<'a, ()>>
) -> Result<Option<tokio::sync::MutexGuard<'a, ()>>, ConsensusError> {
    if let Err(ConsensusError::NetworkTimeout) = check_leader_abandonment(view, validators, app_state).await {
        tracing::warn!("Futility detected for view {}, issuing timeout vote and aborting", view);

        // Pass guard through to issue_timeout_vote (may be Some or None)
        // If Some: held through GST wait to prevent race conditions
        // If None: acquired after GST wait to allow Lock QC to win
        let _ = issue_timeout_vote(view, app_state, guard).await;
        return Err(ConsensusError::NetworkTimeout);
    }

    // Not timing out - return guard back to caller so they can continue holding it
    Ok(guard)
}

// Create a node-only transaction (for automated operations)
pub fn create_signed_transaction(
    app_state: &AppState,
    function: String,
    payload: Vec<u8>
) -> Result<Transaction, ConsensusError> {
    let node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;
    Transaction::new(function, payload, node_id, &app_state.private_key)
        .map_err(|_| ConsensusError::SigningError)
}

// Create a user-initiated transaction (for user operations)
pub fn create_signed_user_transaction(
    app_state: &AppState,
    function: String,
    payload: Vec<u8>,
    user_id: i32,
) -> Result<Transaction, ConsensusError> {
    let node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;
    let user_keys = app_state.get_user_keys().map_err(|_| ConsensusError::DatabaseError)?;

    Transaction::new_with_user(
        function,
        payload,
        node_id,
        &app_state.private_key,
        user_id,
        &user_keys.private_key
    ).map_err(|_| ConsensusError::SigningError)
}

pub async fn consensus_middleware(app_state: &AppState, transactions: Vec<Transaction>) -> Result<(), ConsensusError> {
    tracing::debug!("Starting consensus middleware for {} transactions", transactions.len());

    let my_node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;

    tracing::debug!("Waiting for consensus_lock...");
    // Acquire consensus lock to prevent concurrent block creation race conditions
    // Only one consensus operation can proceed at a time
    let guard = app_state.consensus_lock.lock().await;
    tracing::debug!("Acquired consensus_lock");

    // Check if we are the current leader
    let consensus_state = db::get_consensus(app_state.db_pool.get()).map_err(|_| ConsensusError::DatabaseError)?;

    if consensus_state.leader.node_id != my_node_id {
        // Release lock before forwarding (prevents deadlock when waiting for leader's response)
        drop(guard);

        // Forward to leader instead of initiating consensus
        tracing::info!(
            "Not the leader (node {}), forwarding transactions to leader (node {})",
            my_node_id, consensus_state.leader.node_id
        );
        return forward_to_leader(consensus_state.leader, transactions, consensus_state.view, app_state).await;
    }

    // Check if we've already proposed in this view (double-proposal protection)
    // last_propose_vote_block_hash is cleared on view transitions (Lock QC or TC)
    if consensus_state.last_propose_vote_block_hash.is_some() {
        tracing::warn!(
            "Already proposed in view {}, rejecting duplicate proposal attempt",
            consensus_state.view
        );
        return Err(ConsensusError::SigningError);
    }

    // Safe to proceed - we're the leader and haven't proposed yet
    tracing::info!(
        "Acting as leader (node {}) for view {}, initiating consensus",
        my_node_id, consensus_state.view
    );

    // Use committed height (parent block) for validator set, not proposed height
    // This ensures consistency with middleware activation checks and leader calculation
    let committed_height = consensus_state.committed_block.data.height;
    let all_validators = db::get_validators(app_state.db_pool.get(), committed_height)
        .map_err(|_| ConsensusError::DatabaseError)?;

    // Early abandonment check: if too many nodes have timed out, don't waste resources
    // Pass the guard, get it back if not timing out (threads through to maintain lock)
    let guard = abort_if_timing_out(consensus_state.view, &all_validators, app_state, Some(guard))
        .await?
        .expect("guard must be Some since we passed Some");

    let block = Block::new_tip(&app_state, transactions).map_err(|_| ConsensusError::BlockError)?;

    // Construct MyNode from AppState
    let me = MyNode {
        node_id: my_node_id,
        privkey: app_state.private_key.clone(),
    };

    let validators = all_validators
        .iter()
        .filter(|node| node.node_id != me.node_id)
        .cloned()
        .collect();

    let validators_elect = db::get_validators_elect(app_state.db_pool.get(), committed_height).map_err(|_| ConsensusError::DatabaseError)?
        .iter()
        .filter(|node| node.node_id != me.node_id)
        .cloned()
        .collect();


    // Shared connection to ensure no lock contention with users reading on this node
    let mut conn = app_state.db_pool.get().map_err(|_| ConsensusError::DatabaseError)?;

    // Transaction 1: Record Propose vote and commit immediately (double-vote protection)
    let ballot_propose = {
        let tx = conn.transaction().map_err(|_| ConsensusError::DatabaseError)?;
        let result = Ballot::propose(block.clone(), ConsensusPhase::Propose, &me, tx)
            .map_err(|_| ConsensusError::SigningError)?;
        checkpoint_connection(&conn)?;
        result
    }; // tx and conn are dropped here

    // Async: Collect votes and create Propose QC (no transaction needed)
    // If ballot round fails, issue timeout vote to advance the view
    let qc1 = match ballot_round(ballot_propose, &validators, &validators_elect, app_state).await {
        Ok(qc) => qc,
        Err(e) => {
            let view = consensus_state.view;
            tracing::warn!("Propose ballot round failed in view {}, issuing timeout vote", view);
            // Pass guard through to hold lock during GST wait and prevent race
            let _ = issue_timeout_vote(view, app_state, Some(guard)).await;
            return Err(e);
        }
    };

    // Start broadcasting Propose QC immediately (critical network info even if we fail locally)
    // Keep JoinHandle so we can await completion before Lock ballot
    let broadcast_handle = {
        let validators_clone = validators.clone();
        let validators_elect_clone = validators_elect.clone();
        let qc1_clone = qc1.clone();
        let app_state = app_state.clone();
        tokio::spawn(async move {
            broadcast_qc(&validators_clone, &validators_elect_clone, qc1_clone, &app_state).await
        })
    };

    // Transaction 2: Insert Propose QC locally (synchronous, fast)
    // Get fresh connection for this transaction
    {
        let tx = conn.transaction().map_err(|_| ConsensusError::DatabaseError)?;
        db::insert_qc_unsafe_tx(&tx, &qc1).map_err(|e| {
            tracing::error!("QC insertion failed: {:?}", e);
            ConsensusError::DatabaseError
        })?;
        tx.commit().map_err(|e| {
            tracing::error!("Database commit failed for Propose QC in view {}: {:?}", qc1.view_number, e);
            ConsensusError::DatabaseError
        })?;
        checkpoint_connection(&conn)?;
    } // tx and conn are dropped here

    // Wait for broadcast to complete before creating Lock ballot
    // This ensures validators have Propose QC before receiving Lock ballot
    // Note: broadcast_qc never returns Err (tolerates partial failures), so this only catches task panics
    if let Err(e) = broadcast_handle.await {
        tracing::warn!("Propose QC broadcast task panicked: {:?}", e);
    }

    if app_state.test_mode {
        app_state.consensus_barriers.wait(
            super::barriers::names::AFTER_PROPOSE_QC_BROADCAST
        ).await;
    }

    // Create Lock ballot (no vote recording needed, Lock phase doesn't update last_propose_vote)
    // Get fresh connection for this transaction
    let ballot_lock = {
        let tx = conn.transaction().map_err(|_| ConsensusError::DatabaseError)?;
        let result = Ballot::propose(block.clone(), ConsensusPhase::Lock, &me, tx)
            .map_err(|_| ConsensusError::SigningError)?;
        checkpoint_connection(&conn)?;
        result
    }; // tx and conn are dropped here

    // Async: Collect votes and create Lock QC (no transaction needed)
    // If ballot round fails, issue timeout vote to advance the view
    let qc2 = match ballot_round(ballot_lock, &validators, &validators_elect, app_state).await {
        Ok(qc) => qc,
        Err(e) => {
            let view = consensus_state.view;
            tracing::warn!("Lock ballot round failed in view {}, issuing timeout vote", view);
            // Pass guard through to hold lock during GST wait and prevent race
            let _ = issue_timeout_vote(view, app_state, Some(guard)).await;
            return Err(e);
        }
    };

    if app_state.test_mode {
        app_state.consensus_barriers.wait(
            super::barriers::names::BEFORE_LOCK_QC_BROADCAST
        ).await;
    }

    // Broadcast Lock QC first (fire and forget in background)
    // This ensures network gets the QC even if we fail to integrate it locally
    {
        let validators_clone = validators.clone();
        let validators_elect_clone = validators_elect.clone();
        let qc2_clone = qc2.clone();
        let app_state = app_state.clone();
        tokio::spawn(async move {
            if let Err(e) = broadcast_qc(&validators_clone, &validators_elect_clone, qc2_clone, &app_state).await {
                tracing::warn!("Failed to broadcast Lock QC: {:?}", e);
            }
        });
    }

    // Transaction 4: Insert Lock QC + process transactions locally (synchronous, atomic)
    // Get fresh connection for this transaction
    {
        let db_tx = conn.transaction().map_err(|_| ConsensusError::DatabaseError)?;
        db::insert_qc_unsafe_tx(&db_tx, &qc2).map_err(|e| {
            tracing::error!("QC insertion failed: {:?}", e);
            ConsensusError::DatabaseError
        })?;
        process_transactions(&block.data.transactions, &app_state, true, &db_tx).map_err(|e| {
            tracing::error!("Transaction processing failed: {:?}", e);
            ConsensusError::DatabaseError
        })?;
        db_tx.commit().map_err(|e| {
            tracing::error!("Database commit failed for Lock QC + transactions in view {}: {:?}", qc2.view_number, e);
            ConsensusError::DatabaseError
        })?;
        checkpoint_connection(&conn)?;
    } // db_tx and conn are dropped here

    tracing::debug!("Consensus middleware complete, releasing consensus_lock");
    Ok(())
}

async fn ballot_round(
    ballot: Ballot,
    validators: &Vec<Node>,
    validators_elect: &Vec<Node>,
    app_state: &AppState,
) -> Result<QuorumCertificate, ConsensusError> {
    // Extract block and phase for QC creation
    let block = ballot.block.clone();
    let phase = ballot.data.phase;
    let leader_id = ballot.initiator.replica_id;

    // Broadcast ballot and collect votes from validators
    let voter_signatures = broadcast_and_collect_votes(ballot, validators, validators_elect, app_state).await?;

    // create() now includes verification - safe by default
    let qc = QuorumCertificate::create(
        &block,
        phase,
        leader_id,
        &app_state.private_key,
        voter_signatures,
        &validators,
        app_state
    ).await.map_err(|_| ConsensusError::SigningError)?;

    tracing::debug!(
        "QC created and verified for view {} phase {:?}",
        qc.view_number, qc.phase
    );

    Ok(qc)
}

async fn broadcast_and_collect_votes(
    ballot: Ballot,
    validators: &Vec<Node>,
    validators_elect: &Vec<Node>,
    app_state: &AppState,
) -> Result<Vec<VoteSignMessage>, ConsensusError> {
    // Handle single validator case (no other nodes to vote)
    if validators.is_empty() {
        return Ok(Vec::new());
    }

    let (votes_tx, mut votes_rx) = mpsc::channel::<VoteSignMessage>(100);

    // Calculate quorum threshold (dynamic based on validator count)
    // Note: validators list is filtered (excludes leader), but leader has implicit vote in QC
    // So we need: total_validators = validators.len() + 1, then subtract 1 for leader's implicit vote
    let total_validators = validators.len() + 1; // +1 for leader
    let quorum_threshold = crate::consensus::types::calculate_quorum_threshold(total_validators);
    let required_votes = quorum_threshold.saturating_sub(1); // Leader's vote is implicit in QC creation

    // Spawn tasks for each validator (quorum-tracked)
    let transport = &app_state.iroh_transport;
    for node in validators.clone() {
        let ballot_clone = ballot.clone();
        let votes_tx_clone = votes_tx.clone();
        let transport = transport.clone();
        let iroh_node_id = node.pubkey.to_iroh_node_id();
        let node_id = node.node_id;

        tokio::spawn(async move {
            match super::rpc::submit_ballot_to_peer(
                &transport, node_id, iroh_node_id, &ballot_clone,
            ).await {
                Ok(vote) => {
                    let _ = votes_tx_clone.send(vote).await;
                }
                Err(e) => {
                    tracing::debug!("Failed to submit ballot to node {}: {:?}", node_id, e);
                }
            }
        });
    }

    // NON-CRITICAL PATH: Inform validators elect (fire-and-forget, don't collect votes)
    for node in validators_elect.clone() {
        let ballot_clone = ballot.clone();
        let transport = transport.clone();
        let iroh_node_id = node.pubkey.to_iroh_node_id();
        let node_id = node.node_id;

        tokio::spawn(async move {
            if let Err(e) = super::rpc::submit_ballot_to_peer(
                &transport, node_id, iroh_node_id, &ballot_clone,
            ).await {
                tracing::debug!("Failed to send ballot to elect node {}: {:?}", node_id, e);
            }
        });
    }

    // Drop the original sender so the channel closes when all tasks complete
    drop(votes_tx);

    // Collect votes until we have enough for quorum or all tasks complete
    let mut voter_signatures = Vec::new();
    let timeout = tokio::time::sleep(std::time::Duration::from_secs(30));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            vote_opt = votes_rx.recv() => {
                match vote_opt {
                    Some(vote) => {
                        voter_signatures.push(vote);

                        // Early termination on quorum
                        if voter_signatures.len() >= required_votes {
                            return Ok(voter_signatures);
                        }
                    }
                    None => {
                        // Channel closed - all tasks completed
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                // Overall timeout reached
                break;
            }
        }
    }

    // Check if we have enough votes after timeout or all tasks complete
    if voter_signatures.len() >= required_votes {
        Ok(voter_signatures)
    } else {
        Err(ConsensusError::InsufficientVotes)
    }
}

pub(crate) async fn broadcast_qc(
    validators: &Vec<Node>,
    validators_elect: &Vec<Node>,
    qc: QuorumCertificate,
    app_state: &AppState,
) -> Result<(), ConsensusError> {
    // Handle single validator case (no other nodes to broadcast to)
    if validators.is_empty() {
        return Ok(());
    }

    let (confirmations_tx, mut confirmations_rx) = mpsc::channel::<()>(100);

    // Calculate quorum threshold (dynamic based on validator count)
    // Note: validators list is filtered (excludes leader), but leader broadcasts QC after creating it
    // So we need: total_validators = validators.len() + 1, then subtract 1 for leader who already has it
    let total_validators = validators.len() + 1; // +1 for leader
    let quorum_threshold = crate::consensus::types::calculate_quorum_threshold(total_validators);
    let required_confirmations = quorum_threshold.saturating_sub(1); // Leader already has QC locally

    // Spawn tasks for each validator
    let transport = &app_state.iroh_transport;
    for node in validators.clone() {
        let qc_clone = qc.clone();
        let confirmations_tx_clone = confirmations_tx.clone();
        let transport = transport.clone();
        let iroh_node_id = node.pubkey.to_iroh_node_id();
        let node_id = node.node_id;

        tokio::spawn(async move {
            match super::rpc::broadcast_qc_to_peer(
                &transport, node_id, iroh_node_id, &qc_clone,
            ).await {
                Ok(()) => {
                    let _ = confirmations_tx_clone.send(()).await;
                }
                Err(e) => {
                    tracing::debug!("Failed to send QC to node {}: {:?}", node_id, e);
                }
            }
        });
    }

    // NON-CRITICAL PATH: Inform validators elect (fire-and-forget, don't wait)
    for node in validators_elect.clone() {
        let qc_clone = qc.clone();
        let transport = transport.clone();
        let iroh_node_id = node.pubkey.to_iroh_node_id();
        let node_id = node.node_id;

        tokio::spawn(async move {
            if let Err(e) = super::rpc::broadcast_qc_to_peer(
                &transport, node_id, iroh_node_id, &qc_clone,
            ).await {
                tracing::debug!("Failed to send QC to elect node {}: {:?}", node_id, e);
            }
        });
    }

    // Drop the original sender so the channel closes when all tasks complete
    drop(confirmations_tx);

    // Collect confirmations until we have enough for quorum or all tasks complete
    let mut confirmations = 0;
    let timeout = tokio::time::sleep(std::time::Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            confirmation_opt = confirmations_rx.recv() => {
                match confirmation_opt {
                    Some(()) => {
                        confirmations += 1;

                        // Early termination on quorum
                        if confirmations >= required_confirmations {
                            tracing::debug!("QC broadcast achieved quorum ({}/{})", confirmations, required_confirmations);
                            return Ok(());
                        }
                    }
                    None => {
                        // Channel closed - all tasks completed
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                // Overall timeout reached
                break;
            }
        }
    }

    // Check if we have enough confirmations after timeout or all tasks complete
    if confirmations >= required_confirmations {
        tracing::debug!("QC broadcast achieved quorum ({}/{})", confirmations, required_confirmations);
        Ok(())
    } else {
        // Don't fail - just log and proceed (ballot round will validate if they have QC)
        tracing::warn!(
            "QC broadcast did not achieve quorum ({}/{}) - proceeding anyway (ballot round will validate)",
            confirmations, required_confirmations
        );
        Ok(())
    }
}

pub fn process_transactions(transactions: &Option<Transactions>, app_state: &AppState, execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
    if let Some(transactions) = transactions {
        for tx in transactions.iter() {
            match process_transaction(tx, app_state, execute, db_tx) {
                Ok(_) => {
                    tracing::debug!("Transaction {} successfully: {}", if execute { "processed" } else { "validated" }, &tx.rpc.function);
                }
                Err(e) => {
                    // Both validation and execution phases return error immediately
                    // Transaction auto-rolls back when db_tx is dropped
                    tracing::error!("Failed to {} transaction {}: {:?}",
                                   if execute { "process" } else { "validate" },
                                   &tx.rpc.function, e);
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

pub fn process_transaction(tx: &Transaction, app_state: &AppState, execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
    if let Some(handler) = DISPATCH_TABLE.get(tx.rpc.function.as_str()) {
        handler.process(app_state, tx, execute, db_tx)
    } else {
        tracing::warn!("No handler found for function: {}", &tx.rpc.function);
        Err(crate::db::DatabaseError::InvalidPayload)
    }
}

/// Try to reconstruct a Lock QC from Lock vote evidence carried in timeout votes.
///
/// If enough timeout voters carried Lock evidence (proposer_sig + voter_sig) to form
/// a quorum, we can reconstruct the Lock QC and commit the block — preventing the fork
/// that would occur if a TC were applied instead.
fn try_reconstruct_lock_qc(
    timeout_votes: &[TimeoutVote],
    app_state: &AppState,
) -> Option<QuorumCertificate> {
    // Collect all Lock evidence from timeout votes
    let evidence: Vec<&LockVoteEvidence> = timeout_votes.iter()
        .filter_map(|v| v.lock_vote_evidence.as_ref())
        .collect();

    if evidence.is_empty() {
        return None;
    }

    // Verify all evidence agrees on the same vote_data (same block, view, phase)
    let first = &evidence[0];
    if !evidence.iter().all(|e| {
        e.vote_data.block_hash == first.vote_data.block_hash
            && e.vote_data.view == first.vote_data.view
            && e.vote_data.phase == first.vote_data.phase
            && e.vote_data.block_height == first.vote_data.block_height
    }) {
        tracing::warn!("Lock evidence disagrees on vote_data, cannot reconstruct Lock QC");
        return None;
    }

    // Extract proposer signature (same across all evidence — leader's sig)
    let proposer_signature = first.proposer_signature.clone();

    // Collect unique voter signatures (dedup by replica_id)
    let mut seen_voters = std::collections::HashSet::new();
    let mut voter_signatures = Vec::new();
    for ev in &evidence {
        if seen_voters.insert(ev.voter_signature.replica_id) {
            voter_signatures.push(ev.voter_signature.clone());
        }
    }

    // Check quorum: proposer (1) + unique voters >= quorum
    let committed_height = {
        let consensus_state = db::get_consensus(app_state.db_pool.get()).ok()?;
        consensus_state.committed_block.data.height
    };
    let validators = db::get_validators(app_state.db_pool.get(), committed_height).ok()?;
    let quorum = crate::consensus::types::calculate_quorum_threshold(validators.len());

    let total_sigs = 1 + voter_signatures.len(); // proposer + voters
    if total_sigs < quorum {
        tracing::debug!(
            "Lock evidence insufficient for QC: {} sigs < {} quorum",
            total_sigs, quorum
        );
        return None;
    }

    // Construct the Lock QC
    let lock_qc = QuorumCertificate {
        view_number: first.vote_data.view,
        phase: ConsensusPhase::Lock,
        block_hash: first.vote_data.block_hash,
        proposer_signature,
        voter_signatures: VoteSignMessages(voter_signatures),
    };

    // Verify the reconstructed QC cryptographically
    let block = db::get_block(app_state.db_pool.get(), lock_qc.block_hash).ok()?;
    match lock_qc.verify(app_state, &block) {
        Ok(()) => {
            tracing::info!(
                "Reconstructed Lock QC from timeout vote evidence for view {} block {:?}",
                lock_qc.view_number, lock_qc.block_hash
            );
            Some(lock_qc)
        }
        Err(e) => {
            tracing::warn!(
                "Reconstructed Lock QC failed verification for view {}: {:?} — falling back to TC",
                first.vote_data.view, e
            );
            None
        }
    }
}

// Timeout Vote Collection for distributed TC generation
pub struct TimeoutVoteCollector {
    // Map: view_number -> HashMap<timeout_data_hash, Vec<TimeoutVote>>
    pending_votes: Mutex<HashMap<i32, HashMap<Vec<u8>, Vec<TimeoutVote>>>>,
}

impl TimeoutVoteCollector {
    pub fn new() -> Self {
        Self {
            pending_votes: Mutex::new(HashMap::new()),
        }
    }
    
    pub async fn add_vote(&self, vote: TimeoutVote, app_state: &AppState) -> Result<Option<TimeoutResolution>, CertificateError> {
        // Poll current consensus state for cleanup
        let consensus_state = db::get_consensus(app_state.db_pool.get())
            .map_err(|_| CertificateError::DatabaseError)?;
        let current_view = consensus_state.view;

        // Clean up old votes
        self.cleanup_old_votes(current_view).await;

        // Only reject timeout votes for old views (nodes might be ahead of us)
        if vote.data.view_number < current_view {
            tracing::debug!("Ignoring timeout vote for old view {} (current view: {})", vote.data.view_number, current_view);
            return Err(CertificateError::ValidationError);
        }

        // Verify the timeout vote signature
        self.verify_timeout_vote(&vote, app_state)?;

        // Add vote to pending collection
        let mut pending = self.pending_votes.lock().await;
        let view_votes = pending.entry(vote.data.view_number).or_insert_with(HashMap::new);

        // Group by timeout data hash
        let data_hash = vote.data.encode().map_err(|_| CertificateError::ValidationError)?;
        let data_votes = view_votes.entry(data_hash).or_insert_with(Vec::new);

        // Check for duplicate vote from same replica BEFORE adding
        if data_votes.iter().any(|v| v.sender.replica_id == vote.sender.replica_id) {
            return Err(CertificateError::ValidationError); // Duplicate vote
        }

        // Add vote to collection
        data_votes.push(vote);

        // Always try to create TC - let it decide if there's quorum
        let timeout_votes = data_votes.clone();
        drop(pending); // Release lock before TC creation

        // Try Lock QC reconstruction first (safety priority over TC)
        if let Some(lock_qc) = try_reconstruct_lock_qc(&timeout_votes, app_state) {
            return Ok(Some(TimeoutResolution::LockQC(lock_qc)));
        }

        // Fall through to normal TC creation
        match TimeoutCertificate::create(timeout_votes, app_state) {
            Ok(tc) => Ok(Some(TimeoutResolution::TC(tc))),
            Err(CertificateError::InsufficientVotes) => Ok(None), // Not enough votes yet
            Err(e) => Err(e), // Real error
        }
    }
    
    async fn cleanup_old_votes(&self, current_view: i32) {
        let mut pending = self.pending_votes.lock().await;
        pending.retain(|&view, _| view >= current_view);
    }
    
    fn verify_timeout_vote(&self, vote: &TimeoutVote, app_state: &AppState) -> Result<(), CertificateError> {
        // Get validators to find the public key
        let validators = db::get_validators(app_state.db_pool.get(), vote.data.view_number)
            .map_err(|_| CertificateError::DatabaseError)?;
        
        // Find validator's public key
        let validator = validators.iter()
            .find(|v| v.node_id == vote.sender.replica_id)
            .ok_or(CertificateError::SignerNotFound)?;
        
        // Verify signature
        let message = vote.data.encode().map_err(|_| CertificateError::ValidationError)?;
        
        match validator.pubkey.verify_strict(&message, &vote.sender.signature) {
            Ok(_) => Ok(()),
            Err(_) => Err(CertificateError::ValidationError),
        }
    }
    
    pub async fn get_vote_count(&self, view: i32) -> usize {
        let pending = self.pending_votes.lock().await;
        pending.get(&view)
            .map(|view_votes| view_votes.values().map(|votes| votes.len()).sum())
            .unwrap_or(0)
    }
}

// Leader forwarding function for non-leader nodes
async fn forward_to_leader(
    leader: crate::types::Node,
    transactions: Vec<Transaction>,
    view: i32,
    app_state: &AppState
) -> Result<(), ConsensusError> {
    let leader_iroh_id = leader.pubkey.to_iroh_node_id();

    tracing::info!(
        "Forwarding {} transactions to leader node {} over iroh (our view: {})",
        transactions.len(), leader.node_id, view
    );

    super::rpc::forward_transactions_to_leader(
        &app_state.iroh_transport,
        leader.node_id,
        leader_iroh_id,
        transactions,
        view,
    ).await.map_err(|e| {
        tracing::warn!("Failed to forward transactions to leader: {:?}", e);
        match e {
            crate::net::IrohError::Protocol(_) => ConsensusError::ForwardingError,
            _ => ConsensusError::NetworkError,
        }
    })
}

/// Poll a random subset of validators to get the maximum view in the network
/// This avoids O(N²) bandwidth while still detecting if we're behind
pub async fn poll_subset_for_max_view(
    app_state: &AppState,
    our_view: i32,
    our_height: i32,
    bootstrap_validators: Option<&[Node]>,
) -> Result<i32, ConsensusError> {
    use std::collections::HashSet;

    const MAX_ATTEMPTS: u32 = 3;
    const SUBSET_SIZE: usize = 5;

    let my_node_id = app_state.get_node_id()
        .map_err(|_| ConsensusError::DatabaseError)?;

    let mut all_validators = db::get_validators(app_state.db_pool.get(), our_height)
        .map_err(|_| ConsensusError::DatabaseError)?;

    // Merge with bootstrap validators, removing duplicates by node_id
    if let Some(bootstrap) = bootstrap_validators {
        let existing_ids: HashSet<i32> = all_validators.iter().map(|v| v.node_id).collect();
        all_validators.extend(
            bootstrap.iter()
                .filter(|node| !existing_ids.contains(&node.node_id))
                .cloned()
        );
    }

    // Filter out ourselves
    let other_validators: Vec<Node> = all_validators.into_iter()
        .filter(|v| v.node_id != my_node_id)
        .collect();

    if other_validators.is_empty() {
        // We're the only validator, so we're caught up by definition
        return Ok(our_view);
    }

    let transport = &app_state.iroh_transport;

    // Retry loop for handling unlucky validator selection
    for attempt in 1..=MAX_ATTEMPTS {
        let selected_validators: Vec<&Node> = other_validators
            .choose_multiple(&mut rand::rng(), SUBSET_SIZE)
            .collect();

        tracing::debug!("Attempt {}/{}: Polling {} validators for max view", attempt, MAX_ATTEMPTS, selected_validators.len());

        let mut tasks = Vec::new();

        for validator in selected_validators {
            let transport = transport.clone();
            let node_id = validator.node_id;
            let iroh_node_id = validator.pubkey.to_iroh_node_id();

            let task = tokio::spawn(async move {
                match super::rpc::poll_view(&transport, node_id, iroh_node_id).await {
                    Ok(view) => {
                        tracing::debug!("Validator {} is at view {}", node_id, view);
                        Some(view)
                    }
                    Err(e) => {
                        tracing::debug!("Failed to poll validator {}: {:?}", node_id, e);
                        None
                    }
                }
            });

            tasks.push(task);
        }

        // Wait for all requests to complete and collect views
        let mut received_views = Vec::new();

        for task in tasks {
            if let Ok(Some(view)) = task.await {
                received_views.push(view);
            }
        }

        // If we got at least one response, return the maximum view
        if !received_views.is_empty() {
            let max_view = *received_views.iter().max().unwrap();
            let max_view = std::cmp::max(max_view, our_view);
            tracing::debug!("Max view detected: {} (our view: {})", max_view, our_view);
            return Ok(max_view);
        }

        // All validators failed, try again with different subset
        tracing::debug!("Attempt {}/{}: no successful responses from validators", attempt, MAX_ATTEMPTS);
    }

    // All retry attempts exhausted
    tracing::warn!("Failed to poll network height after {} attempts", MAX_ATTEMPTS);
    Err(ConsensusError::NetworkError)
}