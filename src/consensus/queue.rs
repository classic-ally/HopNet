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
// PendingPool (malachite engine path)
// ============================================================================

/// A queued transaction that made it into one of our proposals; resolved
/// against `committed_tx_nonces` when the decided watch advances.
struct InflightTransaction {
    entry: QueuedTransaction,
    /// The height we proposed it at. Once a later height decides without the
    /// nonce landing, the proposal lost its round — re-pool for retry.
    proposed_at: u64,
}

/// Shared staging area between the queue's intake and the engine driver.
///
/// The batch processor drains the submit channel here when this node is the
/// proposer; the driver empties it on `NeedValue` (build_value → Propose) and
/// resolves submitter notifiers when heights decide. Everything is sync-locked
/// — touched only at proposal/decide frequency, never on hot paths.
#[derive(Default)]
pub struct PendingPool {
    queued: std::sync::Mutex<Vec<QueuedTransaction>>,
    inflight: std::sync::Mutex<Vec<InflightTransaction>>,
    /// Wake signal for the engine driver (on-demand heights): staged work
    /// means the pending height should start. `notify_one` stores a permit,
    /// so a push before the driver listens is not lost.
    work: tokio::sync::Notify,
}

impl PendingPool {
    /// Stage a transaction for the next proposal this node makes.
    pub fn push(&self, entry: QueuedTransaction) {
        self.queued.lock().unwrap().push(entry);
        self.work.notify_one();
    }

    /// Wait for staged work (engine driver wake rule 1). Consumes at most one
    /// stored permit; callers loop.
    pub async fn work_available(&self) {
        self.work.notified().await;
    }

    /// Number of staged (not yet proposed) transactions.
    pub fn staged_len(&self) -> usize {
        self.queued.lock().unwrap().len()
    }

    /// Take up to `max` staged transactions for a proposal at `height`.
    /// Returns the bare transactions for `build_value`; entries are parked
    /// in a caller-held ticket to be resolved by [`Self::settle`].
    pub fn take_for_proposal(&self, max: usize) -> Vec<QueuedTransaction> {
        let mut queued = self.queued.lock().unwrap();
        let n = queued.len().min(max);
        queued.drain(..n).collect()
    }

    /// Return unproposed entries to the front of the staging area (build
    /// failure — nothing was proposed).
    pub fn restage(&self, entries: Vec<QueuedTransaction>) {
        {
            let mut queued = self.queued.lock().unwrap();
            let rest = std::mem::take(&mut *queued);
            *queued = entries.into_iter().chain(rest).collect();
        }
        self.work.notify_one();
    }

    /// Park entries that made it into a proposal at `height`, awaiting decide.
    pub fn mark_inflight(&self, entries: Vec<QueuedTransaction>, height: u64) {
        let mut inflight = self.inflight.lock().unwrap();
        inflight.extend(entries.into_iter().map(|entry| InflightTransaction {
            entry,
            proposed_at: height,
        }));
    }

    /// Resolve a rejected candidate (preflight refusal) — notifies the
    /// submitter immediately.
    pub fn reject(&self, entry: QueuedTransaction, reason: String) {
        let _ = entry.notifier.send(ConsensusResult::Rejected(reason));
    }

    /// Resolve an entry whose nonce is already committed (duplicate submit or
    /// a retry that raced its own earlier commit) — success for the submitter.
    pub fn resolve_committed(&self, entry: QueuedTransaction) {
        let _ = entry.notifier.send(ConsensusResult::Committed);
    }

    /// Settle inflight entries after `decided_height` landed: nonces present
    /// in `committed_tx_nonces` resolve as committed; entries whose proposal
    /// height has passed without committing go back to staging for a retry.
    ///
    /// QUEUED entries are settled too: a forwarded copy of a staged tx can
    /// commit via ANOTHER node's proposal while the local entry waits for our
    /// next NeedValue (forward → transient retry → local stage → lost round →
    /// repool). Without this pass those notifiers strand until client timeout
    /// even though the tx committed.
    pub fn settle(
        &self,
        conn: &r2d2::PooledConnection<SqliteConnectionManager>,
        decided_height: u64,
    ) {
        let mut inflight = self.inflight.lock().unwrap();
        if !inflight.is_empty() {
            let nonces: Vec<_> = inflight.iter().map(|i| i.entry.tx.nonce.clone()).collect();
            let committed = match db::check_committed_nonces(conn, &nonces) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("PendingPool::settle nonce check failed: {:?}", e);
                    return;
                }
            };
            let mut keep = Vec::new();
            let mut repool = Vec::new();
            for item in inflight.drain(..) {
                if committed.contains(&item.entry.tx.nonce.to_string()) {
                    let _ = item.entry.notifier.send(ConsensusResult::Committed);
                } else if item.proposed_at <= decided_height {
                    // Our proposal lost the round — retry in a later proposal.
                    repool.push(item.entry);
                } else {
                    keep.push(item);
                }
            }
            *inflight = keep;
            drop(inflight);
            if !repool.is_empty() {
                tracing::debug!(
                    "PendingPool: re-staging {} transaction(s) after lost round",
                    repool.len()
                );
                self.queued.lock().unwrap().extend(repool);
            }
        } else {
            drop(inflight);
        }

        // Second pass: resolve queued entries whose nonces already committed
        // (via another proposer), and re-arm the driver wake while work
        // remains — the Resume that the original stage/repool fired may have
        // been consumed mid-height, leaving the next height paused forever.
        let mut queued = self.queued.lock().unwrap();
        if queued.is_empty() {
            return;
        }
        let nonces: Vec<_> = queued.iter().map(|q| q.tx.nonce.clone()).collect();
        match db::check_committed_nonces(conn, &nonces) {
            Ok(committed) => {
                let mut keep = Vec::new();
                for entry in queued.drain(..) {
                    if committed.contains(&entry.tx.nonce.to_string()) {
                        let _ = entry.notifier.send(ConsensusResult::Committed);
                    } else {
                        keep.push(entry);
                    }
                }
                *queued = keep;
            }
            Err(e) => {
                tracing::error!("PendingPool::settle queued nonce check failed: {:?}", e);
            }
        }
        let still_has_work = !queued.is_empty();
        drop(queued);
        if still_has_work {
            self.work.notify_one();
        }
    }
}

impl QueuedTransaction {
    /// The wrapped transaction (for building proposals / forwarding).
    pub fn transaction(&self) -> &Transaction {
        &self.tx
    }
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
    /// Staging area shared with the malachite engine driver (proposer path).
    pending: std::sync::Arc<PendingPool>,
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
                pending: std::sync::Arc::new(PendingPool::default()),
            },
            rx,
        )
    }

    /// The staging pool the engine driver drains on `NeedValue`.
    pub fn pending_pool(&self) -> std::sync::Arc<PendingPool> {
        self.pending.clone()
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
    /// Stage forwarded transactions DIRECTLY into the PendingPool, bypassing
    /// the submit channel. The batch was already drained and batched by the
    /// forwarding peer's processor, and it was sent HERE because this node is
    /// the proposer — re-queueing it behind the local channel backlog under
    /// load rots it past its round (near-empty blocks, NotProposer
    /// ping-pong). The pool push fires the driver's work signal, which
    /// resumes the engine (wake rule 1).
    pub async fn enqueue_forwarded(
        &self,
        transactions: Vec<Transaction>,
    ) -> Vec<Result<(), ConsensusSubmitError>> {
        let mut receivers = Vec::with_capacity(transactions.len());
        for transaction in transactions {
            let (result_tx, result_rx) = oneshot::channel();
            self.pending_pool().push(QueuedTransaction {
                tx: transaction,
                notifier: result_tx,
                rejecting_leaders: HashSet::new(),
            });
            receivers.push(result_rx);
        }
        let mut results = Vec::with_capacity(receivers.len());
        for rx in receivers {
            results.push(await_result(rx).await);
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

/// Outcome from a dispatch attempt, determines how the batch processor gates
/// before the next drain cycle.
enum DispatchOutcome {
    /// Batch fully resolved (staged locally or answered) — no gate needed.
    Resolved,
    /// Network-level failure (proposer unreachable). Retry after a short delay.
    RetryAfterDelay,
    /// Proposer acknowledged but can't resolve yet, or refused as
    /// not-proposer. Wait for the engine to make progress before retrying.
    WaitForProgress,
    /// State already changed (a decide raced the proposer's result) — the
    /// wait-for-progress trigger has ALREADY fired; retry immediately.
    RetryNow,
}

/// Long-lived task that drains the queue and routes batches toward the
/// current proposer. Spawned once at startup; runs until the channel closes.
///
/// Dedicated runtime for the consensus DRIVE plane: the batch processor, the
/// engine driver (NeedValue → build_value → propose, sync client, Resume
/// signalling) and the settler.
///
/// WHY: these tasks are what make the engine ADVANCE. On the shared main
/// runtime they starve under API burst load right along with the HTTP
/// handlers — engines sat idle-paused with full pools because the driver's
/// Resume send never got polled. Unlike the iroh net runtime (see
/// hopnet_comms::net_rt), blocking DB work is ALLOWED here — these tasks
/// own dedicated connections and do SAVEPOINT preflights by design; they get
/// their own threads precisely so that blocking never competes with anything
/// liveness-critical.
pub fn queue_rt() -> &'static tokio::runtime::Runtime {
    static QUEUE_RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    QUEUE_RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("consensus-queue")
            .enable_all()
            .build()
            .expect("failed to build consensus queue runtime")
    })
}

/// Malachite path: if WE are the proposer for the engine's current (or
/// pending, when paused on-demand) round, stage the batch in the PendingPool
/// — pushing wakes the engine driver, which builds and proposes on NeedValue.
/// Otherwise forward to the proposer with the two-phase ACK protocol and
/// resolve notifiers from its per-transaction results.
pub async fn batch_processor(mut rx: mpsc::Receiver<QueuedTransaction>, app_state: AppState) {
    // Dedicated connection for the batch processor — checked out once, held for lifetime.
    // Makes consensus throughput independent of pool contention from background tasks.
    let mut conn = app_state
        .db_pool
        .get()
        .expect("batch_processor: failed to check out dedicated connection");

    let pool = app_state.consensus_queue.pending_pool();
    let mut retry_holdback: Vec<QueuedTransaction> = Vec::new();

    loop {
        // ── Drain: merge held-back retries with new channel items ──
        let mut batch: Vec<QueuedTransaction> = std::mem::take(&mut retry_holdback);

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

        // Engine not started yet (setup or join bootstrap in progress) —
        // hold the batch and re-check shortly.
        let Some(engine) = app_state.malachite.get().cloned() else {
            retry_holdback = batch;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        };

        let Some((height, round, proposer)) =
            super::malachite::engine::proposal_target(&app_state)
        else {
            retry_holdback = batch;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        };

        // ── Dispatch ──
        let (holdback, outcome) = if proposer == my_node_id {
            // We propose: stage everything — the push wakes the engine driver
            // (on-demand Resume), which lingers, builds, and proposes.
            for queued in batch {
                pool.push(queued);
            }
            (Vec::new(), DispatchOutcome::Resolved)
        } else {
            handle_as_forwarder(
                &app_state,
                &engine,
                batch,
                height,
                round,
                proposer,
                &mut conn,
            )
            .await
        };
        retry_holdback = holdback;

        // ── Gate before the next cycle ──
        match outcome {
            DispatchOutcome::Resolved => {}
            // Both gates wake on engine progress (round advance or decide),
            // with a delay backstop so a stalled engine can't wedge the
            // queue. RetryAfterDelay (transport failure to the proposer) must
            // NOT sleep blindly: after resume_own_engine the round can rotate
            // to US within one propose timeout, and a blind sleep would leave
            // the pool empty at our own NeedValue — deciding an empty block
            // and wasting the height.
            // A raced decide IS the progress signal — gating here re-waits
            // for the NEXT decide and costs whole heights on an idle mesh.
            DispatchOutcome::RetryNow => {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            DispatchOutcome::WaitForProgress | DispatchOutcome::RetryAfterDelay => {
                let mut round_rx = engine.round.clone();
                let mut decided_rx = engine.decided.clone();
                round_rx.borrow_and_update();
                decided_rx.borrow_and_update();
                tokio::select! {
                    _ = round_rx.changed() => {}
                    _ = decided_rx.changed() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_secs(RETRY_DELAY_SECS)) => {}
                }
            }
        }
    }
}

/// Non-proposer path: forward the batch to the proposer with two-phase ACK.
/// Returns (retries, outcome). Any undeliverable outcome ALSO resumes our own
/// engine (on-demand wake rule: we hold work we can't deliver — starting our
/// height advances rounds past a dead proposer).
async fn handle_as_forwarder(
    app_state: &AppState,
    engine: &super::malachite::EngineHandle,
    batch: Vec<QueuedTransaction>,
    height: u64,
    round: u32,
    proposer: i32,
    conn: &mut r2d2::PooledConnection<SqliteConnectionManager>,
) -> (Vec<QueuedTransaction>, DispatchOutcome) {
    let proposer_pubkey = {
        let pubkeys = match db::get_all_node_pubkeys(conn) {
            Ok(map) => map,
            Err(_) => return (batch, DispatchOutcome::RetryAfterDelay),
        };
        match pubkeys.get(&proposer) {
            Some(pubkey) => pubkey.clone(),
            None => {
                tracing::error!("proposer node {} not in nodes table", proposer);
                return (batch, DispatchOutcome::RetryAfterDelay);
            }
        }
    };
    let proposer_ref = hopnet_comms::PeerRef {
        node_id: proposer,
        pubkey: proposer_pubkey.0.to_bytes(),
    };

    let transactions: Vec<Transaction> = batch.iter().map(|q| q.tx.clone()).collect();

    tracing::debug!(
        "Forwarding {} transactions to proposer node {} (height {}, round {})",
        transactions.len(),
        proposer,
        height,
        round
    );

    // Wake rule 1: we HOLD work, so start our pending height now, in
    // parallel with the forward (Resume is idempotent — a no-op on a running
    // height). A responsive proposer still proposes at round 0 and we are
    // simply ready to vote; a wedged one gets rotated past by OUR timeouts
    // while the forward RPC is still in flight. Resuming only on hard forward
    // failure (the previous shape) left every engine paused for the full
    // connect+ack+result timeout budget under load, stalling all heights.
    let resume_own_engine = || {
        let input_tx = engine.input_tx.clone();
        tokio::spawn(async move {
            let _ = input_tx
                .send(hopnet_consensus::shell::HostInput::Resume)
                .await;
        });
    };
    resume_own_engine();

    let mut decided_rx = engine.decided.clone();
    let forward_result = super::rpc::forward_transactions_with_ack(
        &app_state.comms,
        &proposer_ref,
        transactions,
        height,
        &mut decided_rx,
    )
    .await;

    // Reachability evidence (RFC-CONSENSUS-002): any reply — including a
    // NotProposer rejection — proves an authenticated exchange with the
    // proposer. NoAck and transport errors prove nothing.
    if !matches!(
        forward_result,
        Err(_) | Ok(super::rpc::ForwardAckResult::NoAck)
    ) {
        app_state.evidence.record_contact(proposer);
    }

    match forward_result {
        Ok(super::rpc::ForwardAckResult::NoAck) => {
            tracing::warn!(
                "No ACK from proposer node {} — evicting connection and retrying",
                proposer
            );
            app_state.comms.remove_connection(proposer).await;
            resume_own_engine();
            (batch, DispatchOutcome::RetryAfterDelay)
        }
        Ok(super::rpc::ForwardAckResult::AckedWithResult(results)) => {
            process_forward_results(batch, results, proposer, conn)
        }
        Ok(super::rpc::ForwardAckResult::AckedDecided) => {
            // A height decided before the proposer's result — the nonce table
            // already knows which of ours landed. Anything still pending
            // retries IMMEDIATELY: the decide that raced us is the very
            // progress the WaitForProgress gate would sit waiting for.
            tracing::debug!(
                "Proposer node {} ACKed, height decided — resolving via nonce table",
                proposer
            );
            let results = synthesize_results_from_nonces(&batch, conn);
            let (retries, outcome) = process_forward_results(batch, results, proposer, conn);
            let outcome = match outcome {
                DispatchOutcome::WaitForProgress => DispatchOutcome::RetryNow,
                other => other,
            };
            (retries, outcome)
        }
        Ok(super::rpc::ForwardAckResult::AckedNoResult) => {
            tracing::debug!(
                "Proposer node {} ACKed but safety timeout — waiting for progress",
                proposer
            );
            (batch, DispatchOutcome::WaitForProgress)
        }
        Ok(super::rpc::ForwardAckResult::NotProposer {
            height: their_height,
            round: their_round,
        }) => {
            // Target disagrees about who proposes — either we're stale or it
            // is. The receiver kicks a decided-value sync when it's the one
            // behind (kick_sync_if_behind fires on the forward we just sent),
            // so it catches up within tens of ms — retry promptly instead of
            // camping in the 5s progress gate while live heights decide
            // empty around the held batch.
            tracing::debug!(
                "Node {} says not proposer (at {}/{}, we targeted {}/{}) — retargeting",
                proposer,
                their_height,
                their_round,
                height,
                round
            );
            // Symmetric wake rule: a HIGHER height in the reply means WE are
            // the stale side — kick our own decided-value sync toward it.
            // Without this, a node that missed a decide (e.g. fresh join that
            // raced its own activation block) retargets the same stale height
            // every 250ms until malachite's slow internal sync timer fires
            // (~minutes), stranding the forwarded batch.
            if their_height > height {
                crate::consensus::malachite::engine::kick_sync_if_behind(
                    app_state,
                    their_height,
                    proposer,
                );
            }
            resume_own_engine();
            (batch, DispatchOutcome::RetryNow)
        }
        Err(e) => {
            tracing::warn!("Failed to forward transactions to proposer: {:?}", e);
            resume_own_engine();
            (batch, DispatchOutcome::RetryAfterDelay)
        }
    }
}

/// Synthesize forward results from the nonce table when no proposer result is
/// available. Committed nonces → Committed, uncommitted → Retry.
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
                    reason: "height decided before proposer result".into(),
                }
            }
        })
        .collect()
}

/// Process per-transaction results from the proposer's forward response.
fn process_forward_results(
    batch: Vec<QueuedTransaction>,
    results: Vec<super::rpc::TransactionForwardResult>,
    proposer: i32,
    conn: &mut r2d2::PooledConnection<SqliteConnectionManager>,
) -> (Vec<QueuedTransaction>, DispatchOutcome) {
    let mut retries = Vec::new();

    for (queued, result) in batch.into_iter().zip(results.into_iter()) {
        match result {
            super::rpc::TransactionForwardResult::Committed => {
                let _ = queued.notifier.send(ConsensusResult::Committed);
            }
            super::rpc::TransactionForwardResult::Rejected { reason } => {
                let mut rejecting_leaders = queued.rejecting_leaders;
                rejecting_leaders.insert(proposer);

                // Byzantine rejection threshold: one proposer's word isn't
                // final — retry via other proposers until f+1 agree.
                let current_height = {
                    let tx = conn.transaction().ok();
                    tx.and_then(|t| db::get_current_consensus_height(&t).ok())
                        .unwrap_or(0)
                };
                let validator_count = match db::get_validators_with_conn(conn, current_height) {
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
                tracing::debug!(
                    "Transient forward failure from proposer node {}: {}",
                    proposer,
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
        DispatchOutcome::Resolved
    } else {
        DispatchOutcome::WaitForProgress
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
