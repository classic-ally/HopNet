use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::TransactionBehavior;
use std::collections::HashSet;
use std::fmt;
use tokio::sync::{mpsc, oneshot};

use super::types::Transaction;
use crate::AppState;
use crate::DISPATCH_TABLE;
use crate::db::consensus as db;

// ============================================================================
// Public Types
// ============================================================================

/// Error type returned to callers submitting transactions through the queue.
#[derive(Debug)]
pub enum ConsensusSubmitError {
    /// Transaction content is permanently invalid (business logic rejection)
    Rejected(String),
    /// Timed out waiting for consensus (120s elapsed)
    Timeout,
    /// Queue is at capacity — caller should retry later
    QueueFull,
    /// Unexpected internal failure
    InternalError(String),
}

impl fmt::Display for ConsensusSubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(reason) => write!(f, "rejected: {}", reason),
            Self::Timeout => write!(f, "consensus timeout"),
            Self::QueueFull => write!(f, "queue full"),
            Self::InternalError(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

/// Result sent back to callers via oneshot channel.
enum ConsensusResult {
    Committed,
    Rejected(String),
    Failed(String),
}

/// A transaction waiting in the queue.
pub struct QueuedTransaction {
    tx: Transaction,
    notifier: oneshot::Sender<ConsensusResult>,
    /// Node IDs of leaders that have explicitly rejected this transaction.
    rejecting_leaders: HashSet<i32>,
}

// ============================================================================
// ConsensusQueue (submit handle)
// ============================================================================

/// Cloneable handle for submitting transactions to the consensus queue.
/// Stored in AppState; all route handlers use this instead of calling consensus_middleware directly.
///
/// Holds only the mpsc sender and a DB pool for pre-validation (no AppState reference,
/// which avoids recursive type issues since AppState contains this struct).
#[derive(Clone)]
pub struct ConsensusQueue {
    sender: mpsc::Sender<QueuedTransaction>,
    db_pool: Pool<SqliteConnectionManager>,
}

impl ConsensusQueue {
    /// Create a new ConsensusQueue. Returns the queue handle and the receiver for the batch processor.
    pub fn new(
        db_pool: Pool<SqliteConnectionManager>,
        capacity: usize,
    ) -> (Self, mpsc::Receiver<QueuedTransaction>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            ConsensusQueue {
                sender: tx,
                db_pool,
            },
            rx,
        )
    }

    /// Submit a single transaction. Pre-validates against committed state first.
    /// Returns when the transaction is committed, rejected, or times out.
    pub async fn submit(&self, transaction: Transaction) -> Result<(), ConsensusSubmitError> {
        // Pre-validate: check the handler exists
        if let Err(reason) = self.pre_validate(&transaction) {
            return Err(ConsensusSubmitError::Rejected(reason));
        }

        self.enqueue_one(transaction).await
    }

    /// Submit a pre-built batch. Returns per-transaction results.
    pub async fn submit_batch(
        &self,
        transactions: Vec<Transaction>,
    ) -> Vec<Result<(), ConsensusSubmitError>> {
        let mut results = Vec::with_capacity(transactions.len());
        let mut receivers = Vec::new();

        for transaction in transactions {
            // Pre-validate each transaction
            if let Err(reason) = self.pre_validate(&transaction) {
                results.push(Err(ConsensusSubmitError::Rejected(reason)));
                continue;
            }

            match self.enqueue_with_receiver(transaction).await {
                Ok((_, rx)) => {
                    receivers.push((results.len(), rx));
                    results.push(Ok(())); // placeholder
                }
                Err(e) => {
                    results.push(Err(e));
                }
            }
        }

        // Await all receivers with timeout
        for (idx, rx) in receivers {
            results[idx] = await_result(rx).await;
        }

        results
    }

    /// Enqueue forwarded transactions (from another node). Skips pre-validation
    /// since the submitting node already validated and the leader's preflight will catch issues.
    pub async fn enqueue_forwarded(
        &self,
        transactions: Vec<Transaction>,
    ) -> Vec<Result<(), ConsensusSubmitError>> {
        let mut results = Vec::with_capacity(transactions.len());
        let mut receivers = Vec::new();

        for transaction in transactions {
            match self.enqueue_with_receiver(transaction).await {
                Ok((_, rx)) => {
                    receivers.push((results.len(), rx));
                    results.push(Ok(())); // placeholder
                }
                Err(e) => {
                    results.push(Err(e));
                }
            }
        }

        for (idx, rx) in receivers {
            results[idx] = await_result(rx).await;
        }

        results
    }

    /// Pre-validate a transaction against current committed state.
    /// Currently checks that the handler exists in the dispatch table.
    /// Full preflight validation (with execute=true dry-run) happens on the leader.
    fn pre_validate(&self, transaction: &Transaction) -> Result<(), String> {
        if DISPATCH_TABLE
            .get(transaction.rpc.function.as_str())
            .is_some()
        {
            Ok(())
        } else {
            Err(format!(
                "no handler for function: {}",
                transaction.rpc.function
            ))
        }
    }

    async fn enqueue_one(&self, transaction: Transaction) -> Result<(), ConsensusSubmitError> {
        let (result_tx, result_rx) = oneshot::channel();
        let queued = QueuedTransaction {
            tx: transaction,
            notifier: result_tx,
            rejecting_leaders: HashSet::new(),
        };

        self.sender
            .send(queued)
            .await
            .map_err(|_| ConsensusSubmitError::InternalError("queue closed".into()))?;

        await_result(result_rx).await
    }

    async fn enqueue_with_receiver(
        &self,
        transaction: Transaction,
    ) -> Result<((), oneshot::Receiver<ConsensusResult>), ConsensusSubmitError> {
        let (result_tx, result_rx) = oneshot::channel();
        let queued = QueuedTransaction {
            tx: transaction,
            notifier: result_tx,
            rejecting_leaders: HashSet::new(),
        };

        self.sender
            .send(queued)
            .await
            .map_err(|_| ConsensusSubmitError::InternalError("queue closed".into()))?;

        Ok(((), result_rx))
    }
}

/// Await a consensus result with 120s timeout.
async fn await_result(rx: oneshot::Receiver<ConsensusResult>) -> Result<(), ConsensusSubmitError> {
    match tokio::time::timeout(std::time::Duration::from_secs(120), rx).await {
        Ok(Ok(ConsensusResult::Committed)) => Ok(()),
        Ok(Ok(ConsensusResult::Rejected(reason))) => Err(ConsensusSubmitError::Rejected(reason)),
        Ok(Ok(ConsensusResult::Failed(msg))) => Err(ConsensusSubmitError::InternalError(msg)),
        Ok(Err(_)) => Err(ConsensusSubmitError::InternalError(
            "result channel dropped".into(),
        )),
        Err(_) => Err(ConsensusSubmitError::Timeout),
    }
}

// ============================================================================
// Batch Processor Task
// ============================================================================

const MAX_BATCH_SIZE: usize = 100;
const RETRY_DELAY_SECS: u64 = 5;
const BATCH_LINGER_MS: u64 = 100;

/// Outcome from a dispatch attempt, determines how the batch processor gates
/// before the next drain cycle.
enum DispatchOutcome {
    /// Batch was committed — view already advanced, no gate needed.
    ViewAdvanced,
    /// Network-level failure (leader unreachable). Retry after a short delay.
    RetryAfterDelay,
    /// Leader acknowledged but can't process yet (e.g., "already proposed").
    /// Wait for view change (TC will advance the view).
    WaitForViewChange,
}

/// Long-lived task that drains the queue and dispatches batches through consensus.
/// Spawned once at startup; runs until the channel is closed (shutdown).
///
/// Gates the next drain cycle based on the dispatch outcome:
/// - ViewAdvanced: no gate — loop back immediately to drain more transactions
/// - WaitForViewChange: view-aware wait — check DB, only block if view hasn't moved
/// - RetryAfterDelay: short sleep, then retry (leader was unreachable)
pub async fn batch_processor(mut rx: mpsc::Receiver<QueuedTransaction>, app_state: AppState) {
    // Dedicated connection for the batch processor — checked out once, held for lifetime.
    // Makes consensus throughput independent of pool contention from background tasks.
    let mut conn = app_state
        .db_pool
        .get()
        .expect("batch_processor: failed to check out dedicated connection");

    let mut retry_holdback: Vec<QueuedTransaction> = Vec::new();

    loop {
        // ── Drain: merge held-back retries with new channel items ──
        let mut batch: Vec<QueuedTransaction> = retry_holdback.drain(..).collect();

        if batch.is_empty() {
            // Nothing held back — block until at least one new tx arrives
            let Some(first) = rx.recv().await else {
                tracing::info!("Consensus queue channel closed, batch processor shutting down");
                break;
            };
            batch.push(first);
        }

        // Drain any additional queued transactions (non-blocking)
        while batch.len() < MAX_BATCH_SIZE {
            match rx.try_recv() {
                Ok(queued) => batch.push(queued),
                Err(_) => break,
            }
        }

        tracing::debug!("Processing batch of {} transactions", batch.len());

        let my_node_id = match app_state.get_node_id() {
            Ok(id) => id,
            Err(_) => {
                for queued in batch {
                    let _ = queued
                        .notifier
                        .send(ConsensusResult::Failed("node not initialized".into()));
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        let consensus_state = match db::get_consensus_with_conn(&conn) {
            Ok(state) => state,
            Err(_) => {
                for queued in batch {
                    let _ = queued.notifier.send(ConsensusResult::Failed(
                        "failed to get consensus state".into(),
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        let pre_dispatch_view = consensus_state.view;

        // ── Dispatch ──
        let (holdback, outcome) = if consensus_state.leader.node_id == my_node_id {
            // Leader: linger to collect forwarded transactions from other nodes
            let deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(BATCH_LINGER_MS);
            while batch.len() < MAX_BATCH_SIZE {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(queued)) => batch.push(queued),
                    _ => break,
                }
            }
            handle_as_leader(&app_state, batch, &consensus_state, &mut conn).await
        } else {
            handle_as_forwarder(&app_state, batch, &consensus_state, &mut conn).await
        };
        retry_holdback = holdback;

        // ── Gate: view-aware wait before next drain cycle ──
        match outcome {
            DispatchOutcome::ViewAdvanced => {
                // View advanced during dispatch — loop back immediately to drain more
            }
            DispatchOutcome::WaitForViewChange => {
                // Loop until view genuinely advances — notify_waiters() is a broadcast
                // that can fire from unrelated consensus activity (spurious wakeups).
                loop {
                    let notified = app_state.view_changed.notified();
                    let current_view = db::get_consensus_with_conn(&conn)
                        .map(|s| s.view)
                        .unwrap_or(pre_dispatch_view);
                    if current_view != pre_dispatch_view {
                        break; // View genuinely advanced
                    }
                    notified.await;
                }
            }
            DispatchOutcome::RetryAfterDelay => {
                // Network failure — leader unreachable, no view consumed.
                // Short delay to avoid tight-looping, then retry.
                tokio::time::sleep(std::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
            }
        }
    }
}

/// Leader path: preflight-validate, run consensus, notify callers.
/// Returns (retries, outcome).
async fn handle_as_leader(
    app_state: &AppState,
    mut batch: Vec<QueuedTransaction>,
    consensus_state: &super::types::ConsensusState,
    conn: &mut r2d2::PooledConnection<SqliteConnectionManager>,
) -> (Vec<QueuedTransaction>, DispatchOutcome) {
    // Early check: if we've already proposed in this view, return all as retries
    if consensus_state.last_propose_vote_block_hash.is_some() {
        tracing::debug!(
            "Already proposed in view {}, holding {} transactions for next view",
            consensus_state.view,
            batch.len()
        );
        return (batch, DispatchOutcome::WaitForViewChange);
    }

    // Nonce dedup: check if any transactions were already committed
    {
        let nonces: Vec<_> = batch.iter().map(|q| q.tx.nonce.clone()).collect();
        if let Ok(committed) = db::check_committed_nonces(conn, &nonces) {
            if !committed.is_empty() {
                let mut i = 0;
                while i < batch.len() {
                    if committed.contains(&batch[i].tx.nonce.to_string()) {
                        let queued = batch.remove(i);
                        let _ = queued.notifier.send(ConsensusResult::Committed);
                    } else {
                        i += 1;
                    }
                }
                if batch.is_empty() {
                    return (Vec::new(), DispatchOutcome::ViewAdvanced);
                }
            }
        }
    }

    // Preflight validation: SAVEPOINT-based single-pass
    // Each tx validates against cumulative state from prior successful txs in the batch.
    // SAVEPOINTs allow individual tx rollback within a single transaction, solving
    // inter-tx dependency resolution that the old restart-on-failure loop could not handle.
    let rejected_indices = {
        let _wg = app_state.write_gate.guard();
        let db_tx = match conn.transaction_with_behavior(TransactionBehavior::Immediate) {
            Ok(tx) => tx,
            Err(_) => {
                return (batch, DispatchOutcome::RetryAfterDelay);
            }
        };

        let mut rejected = Vec::new();

        for (i, queued) in batch.iter().enumerate() {
            let sp = format!("preflight_{}", i);

            if let Err(e) = db_tx.execute_batch(&format!("SAVEPOINT {}", sp)) {
                tracing::error!("Failed to create savepoint {}: {:?}", sp, e);
                rejected.push((i, format!("savepoint error: {:?}", e)));
                continue;
            }

            if let Some(handler) = DISPATCH_TABLE.get(queued.tx.rpc.function.as_str()) {
                match handler.process(app_state, &queued.tx, false, &db_tx) {
                    Ok(()) => {
                        // Keep mutations visible for subsequent txs
                        let _ = db_tx.execute_batch(&format!("RELEASE {}", sp));
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Preflight rejected tx {}: {:?} (function: {})",
                            i,
                            e,
                            queued.tx.rpc.function
                        );
                        let _ = db_tx.execute_batch(&format!("ROLLBACK TO {}", sp));
                        let _ = db_tx.execute_batch(&format!("RELEASE {}", sp));
                        rejected.push((i, format!("{:?}", e)));
                    }
                }
            } else {
                let _ = db_tx.execute_batch(&format!("ROLLBACK TO {}", sp));
                let _ = db_tx.execute_batch(&format!("RELEASE {}", sp));
                rejected.push((i, format!("no handler: {}", queued.tx.rpc.function)));
            }
        }

        // Transaction auto-rollbacks on drop — this was a dry run
        rejected
    };

    // Remove rejected txs and notify submitters (reverse order to maintain valid indices)
    if !rejected_indices.is_empty() {
        for (idx, reason) in rejected_indices.into_iter().rev() {
            let queued = batch.remove(idx);
            let _ = queued.notifier.send(ConsensusResult::Rejected(reason));
        }
    }

    if batch.is_empty() {
        return (Vec::new(), DispatchOutcome::ViewAdvanced);
    }

    // Extract transactions for consensus
    let mut transactions: Vec<Transaction> = batch.iter().map(|q| q.tx.clone()).collect();

    // Inject nonce cleanup transaction every 97 views (prime interval for leader rotation diversity)
    const NONCE_CLEANUP_INTERVAL: u64 = 97;
    if (consensus_state.view as u64) % NONCE_CLEANUP_INTERVAL == 0 {
        let cutoff_ts = (chrono::Utc::now() - chrono::Duration::hours(1)).timestamp() as u64;
        let cutoff = hopnet_common::CustomUUID::new(Some(&uuid::Timestamp::from_unix(
            uuid::NoContext,
            cutoff_ts,
            0,
        )));
        let payload =
            bincode::serde::encode_to_vec(&cutoff, bincode::config::standard()).unwrap_or_default();
        if let Ok(cleanup_tx) = super::functions::create_signed_transaction(
            app_state,
            "system.cleanup_nonces".to_string(),
            payload,
        ) {
            transactions.push(cleanup_tx);
        }
    }

    // Run consensus (leader-only path)
    match super::functions::run_consensus(app_state, transactions, conn).await {
        Ok(()) => {
            for queued in batch {
                let _ = queued.notifier.send(ConsensusResult::Committed);
            }
            (Vec::new(), DispatchOutcome::ViewAdvanced)
        }
        Err(e) => {
            tracing::warn!(
                "run_consensus failed: {:?}, holding {} transactions for retry",
                e,
                batch.len()
            );
            // We attempted a proposal — wait for TC to advance the view
            (batch, DispatchOutcome::WaitForViewChange)
        }
    }
}

/// Non-leader path: forward batch to leader using two-phase ACK protocol.
/// Returns (retries, outcome).
async fn handle_as_forwarder(
    app_state: &AppState,
    batch: Vec<QueuedTransaction>,
    consensus_state: &super::types::ConsensusState,
    conn: &mut r2d2::PooledConnection<SqliteConnectionManager>,
) -> (Vec<QueuedTransaction>, DispatchOutcome) {
    // If we've already voted on a proposal this view, the leader has already proposed —
    // forwarding would just get a Retry response. Skip the round-trip.
    if consensus_state.last_propose_vote_block_hash.is_some() {
        tracing::debug!(
            "Already voted on proposal in view {}, holding {} transactions for next view",
            consensus_state.view,
            batch.len()
        );
        return (batch, DispatchOutcome::WaitForViewChange);
    }

    let leader = &consensus_state.leader;
    let leader_iroh_id = leader.pubkey.to_iroh_node_id();
    let view = consensus_state.view;

    let transactions: Vec<Transaction> = batch.iter().map(|q| q.tx.clone()).collect();

    tracing::debug!(
        "Forwarding {} transactions to leader node {} (view: {})",
        transactions.len(),
        leader.node_id,
        view
    );

    let forward_result = super::rpc::forward_transactions_with_ack(
        &app_state.iroh_transport,
        leader.node_id,
        leader_iroh_id,
        transactions,
        view,
        &app_state.view_changed,
    )
    .await;

    match forward_result {
        Ok(super::rpc::ForwardAckResult::NoAck) => {
            // Leader never received our request — safe to retry
            tracing::warn!(
                "No ACK from leader node {} — evicting connection and retrying",
                leader.node_id
            );
            app_state
                .iroh_transport
                .remove_connection(leader.node_id)
                .await;
            (batch, DispatchOutcome::RetryAfterDelay)
        }
        Ok(super::rpc::ForwardAckResult::AckedWithResult(results)) => {
            // Got full results — process them
            process_forward_results(app_state, batch, results, consensus_state, conn)
        }
        Ok(super::rpc::ForwardAckResult::AckedViewChanged) => {
            // View advanced before leader result — nonces are committed in local DB.
            // Synthesize results from nonce table and process through unified codepath.
            tracing::debug!(
                "Leader node {} ACKed, view changed — resolving via nonce table",
                leader.node_id
            );
            let results = synthesize_results_from_nonces(&batch, conn);
            process_forward_results(app_state, batch, results, consensus_state, conn)
        }
        Ok(super::rpc::ForwardAckResult::AckedNoResult) => {
            // Safety timeout — view hasn't changed, leader still processing.
            // Wait for view change; next cycle will resolve via nonce dedup.
            tracing::debug!(
                "Leader node {} ACKed but safety timeout — waiting for view change",
                leader.node_id
            );
            (batch, DispatchOutcome::WaitForViewChange)
        }
        Ok(super::rpc::ForwardAckResult::NotLeader { view: leader_view }) => {
            // Our view is stale — the node we sent to is not the leader.
            // WaitForViewChange will break immediately if catch-up already happened
            // (triggered by the actual leader's ballot arriving at our RPC handler).
            // Potentially later we'll want this to be event-driven by the leader_view,
            // matching the pattern other handlers have when receiving ensure_caught_up_for_message
            tracing::debug!(
                "Node {} not leader (at view {}, we're at {}) — waiting for catch-up",
                leader.node_id,
                leader_view,
                view
            );
            (batch, DispatchOutcome::WaitForViewChange)
        }
        Ok(super::rpc::ForwardAckResult::Busy) => {
            // Leader's consensus lock is held — wait for next view
            tracing::debug!(
                "Leader node {} busy in view {} — waiting for next view",
                leader.node_id,
                view
            );
            (batch, DispatchOutcome::WaitForViewChange)
        }
        Err(e) => {
            // Network failure — leader unreachable
            tracing::warn!("Failed to forward transactions to leader: {:?}", e);
            (batch, DispatchOutcome::RetryAfterDelay)
        }
    }
}

/// Synthesize forward results from the nonce table when no leader result is available.
/// Committed nonces → Committed, uncommitted → Retry.
fn synthesize_results_from_nonces(
    batch: &[QueuedTransaction],
    conn: &r2d2::PooledConnection<SqliteConnectionManager>,
) -> Vec<super::rpc::TransactionForwardResult> {
    let nonces: Vec<_> = batch.iter().map(|q| q.tx.nonce.clone()).collect();
    let committed = db::check_committed_nonces(conn, &nonces).unwrap_or_default();
    batch
        .iter()
        .map(|q| {
            if committed.contains(&q.tx.nonce.to_string()) {
                super::rpc::TransactionForwardResult::Committed
            } else {
                super::rpc::TransactionForwardResult::Retry {
                    reason: "view changed before leader result".into(),
                }
            }
        })
        .collect()
}

/// Process per-transaction results from the leader forward response.
fn process_forward_results(
    app_state: &AppState,
    batch: Vec<QueuedTransaction>,
    results: Vec<super::rpc::TransactionForwardResult>,
    consensus_state: &super::types::ConsensusState,
    conn: &mut r2d2::PooledConnection<SqliteConnectionManager>,
) -> (Vec<QueuedTransaction>, DispatchOutcome) {
    let leader = &consensus_state.leader;
    let mut retries = Vec::new();
    let mut got_retry = false;

    for (queued, result) in batch.into_iter().zip(results.into_iter()) {
        match result {
            super::rpc::TransactionForwardResult::Committed => {
                let _ = queued.notifier.send(ConsensusResult::Committed);
            }
            super::rpc::TransactionForwardResult::Rejected { reason } => {
                let mut rejecting_leaders = queued.rejecting_leaders;
                rejecting_leaders.insert(leader.node_id);

                // Check Byzantine rejection threshold
                let validator_count = match db::get_validators_with_conn(
                    conn,
                    consensus_state.committed_block.data.height,
                ) {
                    Ok(v) => v.len(),
                    Err(_) => {
                        let _ = queued
                            .notifier
                            .send(ConsensusResult::Failed("failed to get validators".into()));
                        continue;
                    }
                };

                let max_byzantine = max_byzantine_faults(validator_count);
                if rejecting_leaders.len() > max_byzantine {
                    let _ = queued.notifier.send(ConsensusResult::Rejected(reason));
                } else {
                    retries.push(QueuedTransaction {
                        tx: queued.tx,
                        notifier: queued.notifier,
                        rejecting_leaders,
                    });
                }
            }
            super::rpc::TransactionForwardResult::Retry { reason } => {
                got_retry = true;
                tracing::debug!(
                    "Transient forward failure from leader node {} (view {}): {}",
                    leader.node_id,
                    consensus_state.view,
                    reason
                );
                retries.push(QueuedTransaction {
                    tx: queued.tx,
                    notifier: queued.notifier,
                    rejecting_leaders: queued.rejecting_leaders,
                });
            }
        }
    }

    let outcome = if retries.is_empty() {
        DispatchOutcome::ViewAdvanced
    } else if got_retry {
        DispatchOutcome::WaitForViewChange
    } else {
        DispatchOutcome::WaitForViewChange
    };

    (retries, outcome)
}

/// Maximum number of Byzantine faults the network can tolerate.
/// For n ≤ 6: n/2 - 1 (relaxed mode)
/// For n > 6: (n-1)/3 (full BFT mode)
/// Need f+1 distinct leader rejections to confirm permanent rejection.
fn max_byzantine_faults(validator_count: usize) -> usize {
    if validator_count <= 6 {
        (validator_count / 2).saturating_sub(1)
    } else {
        (validator_count - 1) / 3
    }
}
