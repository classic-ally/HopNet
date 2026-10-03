//! The reconciler (RFC-STORAGE-003 S3): pull by the responsible nodes.
//!
//! One level-triggered loop owns all fragment movement. Every node's
//! standing obligation is derived from the blob's GOAL (`desired_placement_
//! height` → the view in force at that height → the assignment): the
//! classes this node is responsible for and does not hold are owed, and
//! the one primitive that discharges them is fetch-with-recovery — request
//! the class from its attested holders (then any peer); if none serves,
//! rebuild it from any K live classes. First distribution, rebalance
//! handoffs and steady-state loss are the same duty.
//!
//! Kicks are latency hints, never correctness: the host's consensus apply
//! (`on_decided` → `notify_blob_committed`) wakes the worker for a decided
//! blob, declare-apply wakes it for a moved goal, and the policy tick
//! re-kicks the in-flight set — the level-triggered check discovers
//! anything a hint missed. One serial worker per node bounds memory and
//! bandwidth (the model's ladder as queue order: urgent re-encode > pulls >
//! lazy re-encode).
//!
//! After a pull check that changed this node's disk — or for a
//! never-confirmed blob it holds (the origin at birth) — the node attests
//! promptly (a blob-scoped `self_check_fragments` belief, then disk truth) and,
//! when the goal's evidence is complete, proposes `ConfirmPlacement`: the
//! latency path for uploads and real moves. Re-goaled blobs already held
//! here submit nothing from the worker; the tick's fulfillment pass
//! confirms them in batches (RFC-STORAGE-003 S4). Racing proposers are
//! harmless: apply validation carries the safety.
//!
//! The push pipeline (origin-push worker pool, send permits, the failure
//! threshold, the blind placement batcher) is gone. The engine owns NO
//! runtime: the host passes a handle at spawn.

pub mod evidence;
pub mod fetch;
pub mod policy;
pub mod reencode;

use crate::admission::{self, SpaceGuard, WriteClass};
use crate::error::StorageError;
use crate::fragstore;
use crate::traits::{LocalStateSink, StateReader, Transport, TxSubmitter};
use crate::types::BlobId;
use fetch::{FetchMiss, FetchScheduler, PullLimits};
use hopnet_common::Blake3Hash;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// The four host seams, bundled. Fields are Arcs so the bundle clones
/// cheaply into worker tasks.
pub struct Seams<T, S, X, L> {
    pub transport: Arc<T>,
    pub state: Arc<S>,
    pub submitter: Arc<X>,
    pub local_state: Arc<L>,
}

impl<T, S, X, L> Clone for Seams<T, S, X, L> {
    fn clone(&self) -> Self {
        Seams {
            transport: self.transport.clone(),
            state: self.state.clone(),
            submitter: self.submitter.clone(),
            local_state: self.local_state.clone(),
        }
    }
}

/// Engine configuration supplied by the host at spawn.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Local fragment store root.
    pub fragments_dir: String,
    /// Pull concurrency (`fetch::PullLimits::from_env` in production).
    pub limits: PullLimits,
    /// The replica-write floor; `None` = the process guard
    /// (`admission::SpaceGuard::global`, configured at boot).
    pub space: Option<Arc<crate::admission::SpaceGuard>>,
}

/// While held back for space, re-probe free space this often.
pub const SPACE_REPROBE: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug)]
pub enum EngineError {
    /// Seam/state failure (DB checkout, query).
    State(StorageError),
    /// A fragment could not be sourced or stored.
    Transfer(String),
    /// Below the free-space floor for this write (`admission::SpaceGuard`):
    /// held, not failed.
    NoSpace,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::State(e) => write!(f, "state error: {}", e),
            EngineError::Transfer(m) => write!(f, "fragment transfer error: {}", m),
            EngineError::NoSpace => write!(f, "held for space (below the pull floor)"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<StorageError> for EngineError {
    fn from(e: StorageError) -> Self {
        EngineError::State(e)
    }
}

/// Outcome of one pull check for one blob on this node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PullOutcome {
    /// Classes this node owed under the goal and did not hold.
    pub owed: usize,
    /// Owed classes fetched from a holder.
    pub pulled: usize,
    /// Owed classes rebuilt from K live classes (no holder served).
    pub rebuilt: usize,
    /// Owed classes still missing after both — retried on the next kick.
    pub failed: usize,
    /// Owed classes not attempted (or not written) because this node is
    /// below its pull floor: held, not failed — no rebuild, no park.
    pub held_for_space: usize,
    /// This blob's belief, disk truth and confirmation check went to the
    /// evidence lane (births and moved bytes only).
    pub evidence_queued: bool,
    /// Waiting for a rebuild slot: offer the blob again after this long.
    pub retry_after: Option<std::time::Duration>,
}

/// Aggregate of many pull checks (the tick's re-kick, the operator drain).
#[derive(Debug, Default, Clone, Copy)]
pub struct PullStats {
    pub checked: usize,
    pub owed: usize,
    pub pulled: usize,
    pub rebuilt: usize,
    pub failed: usize,
    /// Classes held back by the pull floor.
    pub held_for_space: usize,
    /// Blobs whose evidence went to the evidence lane.
    pub evidence_queued: usize,
}

/// One re-encode work item for the serial worker (RFC-STORAGE-001 Repair:
/// this node was elected repairer of the chunk).
#[derive(Debug)]
pub struct ReencodeCmd {
    pub blob_id: BlobId,
    pub chunk_number: u32,
    pub missing_classes: Vec<u32>,
}

type PullRequest = BlobId;

/// Urgent re-encodes queued or running, by (blob, chunk): the tick re-sends
/// its whole urgent set every pass, and a chunk already waiting is not
/// queued again.
type UrgentPending = Arc<std::sync::Mutex<std::collections::HashSet<(BlobId, u32)>>>;

/// What the latest policy tick owes urgently: (blob, chunk) → the classes
/// owed; `None` until a tick publishes one.
pub type UrgentOwed = HashMap<(BlobId, u32), Vec<u32>>;
type UrgentLatest = Arc<std::sync::Mutex<Option<UrgentOwed>>>;

/// Blobs queued for (or in) a pull check, each with the callers waiting
/// on its outcome. One entry per blob: a second request for a queued blob
/// waits on the same check instead of queueing a duplicate.
type PullQueue = Arc<std::sync::Mutex<HashMap<BlobId, Vec<oneshot::Sender<PullOutcome>>>>>;

/// Takes a blob out of the queue when its check ends — normally by
/// `finish`, which answers the waiters; if the check panics, on drop, so
/// the blob can be queued again (its waiters see the engine as gone).
struct QueuedEntry {
    queue: PullQueue,
    blob_id: Option<BlobId>,
}

impl QueuedEntry {
    fn finish(mut self, outcome: PullOutcome) {
        if let Some(blob_id) = self.blob_id.take() {
            let waiters = self.queue.lock().unwrap().remove(&blob_id);
            for waiter in waiters.into_iter().flatten() {
                let _ = waiter.send(outcome);
            }
        }
    }
}

impl Drop for QueuedEntry {
    fn drop(&mut self) {
        if let Some(blob_id) = self.blob_id.take() {
            self.queue.lock().unwrap().remove(&blob_id);
        }
    }
}

/// Queue a pull check unless one is already queued; see
/// `EngineHandle::enqueue`.
fn enqueue_on(
    queue: &PullQueue,
    pull_tx: &mpsc::UnboundedSender<PullRequest>,
    blob_id: BlobId,
    waiter: Option<oneshot::Sender<PullOutcome>>,
) -> bool {
    let mut queued = queue.lock().unwrap();
    if let Some(waiters) = queued.get_mut(&blob_id) {
        waiters.extend(waiter);
        return false;
    }
    if pull_tx.send(blob_id.clone()).is_err() {
        return false;
    }
    queued.insert(blob_id, waiter.into_iter().collect());
    true
}

/// Held by the dispatch loop: however it ends, every queued blob is
/// dropped and every waiter sees the engine as gone (`None`) instead of
/// hanging, and the closed channel refuses later requests.
struct DrainOnExit(PullQueue);

impl Drop for DrainOnExit {
    fn drop(&mut self) {
        let drained = std::mem::take(&mut *self.0.lock().unwrap_or_else(|e| e.into_inner()));
        if !drained.is_empty() {
            tracing::error!(
                queued = drained.len(),
                "pull: dispatch loop ended; queued checks dropped"
            );
        }
    }
}

/// Handle to the running engine. Cheap to clone; the host stores one in its
/// app state (mirrors the consensus EngineHandle pattern).
#[derive(Clone)]
pub struct EngineHandle {
    pull_tx: mpsc::UnboundedSender<PullRequest>,
    reencode_urgent_tx: mpsc::UnboundedSender<ReencodeCmd>,
    reencode_lazy_tx: mpsc::UnboundedSender<ReencodeCmd>,
    /// Blobs waiting for (or in) a pull check: hints for a blob already
    /// queued are dropped, and the planner paces itself on the count.
    queued: PullQueue,
    urgent_pending: UrgentPending,
    urgent_latest: UrgentLatest,
    sched: Arc<FetchScheduler>,
}

impl EngineHandle {
    /// Latency hint for a decided blob (or a moved goal): check this node's
    /// pull duties for it soon. NON-BLOCKING (unbounded send) — safe from
    /// the host's consensus apply path. Carries no correctness weight: the
    /// pull planner's pass over the in-flight set discovers anything a
    /// hint missed. A blob already queued is not queued twice.
    pub fn notify_blob_committed(&self, blob_id: BlobId) {
        self.offer(blob_id);
    }

    /// Queue a pull check for `blob_id` unless one is already queued.
    /// Returns whether it was queued.
    pub fn offer(&self, blob_id: BlobId) -> bool {
        self.enqueue(blob_id, None)
    }

    /// The one way into the queue: a blob already queued gains a waiter
    /// (if any) and is not sent again. Returns whether it was newly queued.
    fn enqueue(&self, blob_id: BlobId, waiter: Option<oneshot::Sender<PullOutcome>>) -> bool {
        enqueue_on(&self.queued, &self.pull_tx, blob_id, waiter)
    }

    /// Pull checks queued or running — the planner keeps this near its
    /// target instead of flooding the channel.
    pub fn queued_len(&self) -> usize {
        self.queued.lock().unwrap().len()
    }

    /// The fetch scheduler's live state: window, fetches in flight per
    /// peer, parked peers and blobs.
    pub fn scheduler_stats(&self) -> fetch::SchedulerStats {
        self.sched.stats(std::time::Instant::now())
    }

    /// Parked on unreachable sources: the planner skips it until its
    /// backoff passes.
    pub fn blob_parked(&self, blob_id: &BlobId) -> bool {
        self.sched.blob_parked(blob_id, std::time::Instant::now())
    }

    /// Drop park entries nobody has re-parked for an hour (blobs that went
    /// quiescent or were deleted while parked). Returns how many.
    pub fn prune_parks(&self) -> usize {
        self.sched.prune_parks(std::time::Instant::now())
    }

    /// Pull check for one blob, run in the window; resolves when it
    /// completes. `None` = engine gone.
    pub async fn pull_blob(&self, blob_id: BlobId) -> Option<PullOutcome> {
        let (tx, rx) = oneshot::channel();
        // A blob already queued is waited on, not queued twice.
        self.enqueue(blob_id, Some(tx));
        rx.await.ok()
    }

    /// Pull checks for a batch of blobs, aggregated. All are queued at once
    /// (the window runs them in parallel), then awaited. Engine-gone counts
    /// as a failure — a later pass retries the blob.
    pub async fn pull_blobs(&self, blob_ids: impl IntoIterator<Item = BlobId> + Send) -> PullStats {
        let mut stats = PullStats::default();
        let mut waiting = Vec::new();
        for blob_id in blob_ids {
            let (tx, rx) = oneshot::channel();
            self.enqueue(blob_id.clone(), Some(tx));
            waiting.push((blob_id, rx));
        }
        for (blob_id, rx) in waiting {
            stats.checked += 1;
            match rx.await.ok() {
                Some(o) => {
                    stats.owed += o.owed;
                    stats.pulled += o.pulled;
                    stats.rebuilt += o.rebuilt;
                    stats.failed += o.failed;
                    stats.held_for_space += o.held_for_space;
                    stats.evidence_queued += o.evidence_queued as usize;
                }
                None => {
                    stats.failed += 1;
                    tracing::error!("pull: engine gone while checking {}", blob_id);
                }
            }
        }
        stats
    }

    /// Enqueue one chunk re-encode on the serial worker. Urgent (live
    /// classes below the watermark) preempts pulls and lazy work; lazy
    /// items drain one at a time behind everything else. An urgent chunk
    /// already queued or running is not queued again. Returns whether it
    /// was queued.
    pub fn enqueue_reencode(&self, cmd: ReencodeCmd, urgent: bool) -> bool {
        let key = (cmd.blob_id.clone(), cmd.chunk_number);
        let tx = if urgent {
            if !self.urgent_pending.lock().unwrap().insert(key.clone()) {
                return false;
            }
            &self.reencode_urgent_tx
        } else {
            &self.reencode_lazy_tx
        };
        if tx.send(cmd).is_err() {
            if urgent {
                self.urgent_pending.lock().unwrap().remove(&key);
            }
            tracing::error!("re-encode: engine gone — command dropped");
            return false;
        }
        true
    }

    /// Urgent re-encodes queued or running.
    pub fn urgent_reencodes_pending(&self) -> usize {
        self.urgent_pending.lock().unwrap().len()
    }

    /// What the latest policy tick owes urgently, per chunk with its
    /// classes, replacing the last. The tick publishes it every pass
    /// (empty included) BEFORE queueing it. At dequeue a queued urgent
    /// re-encode runs the classes the latest tick owes for its chunk, not
    /// the ones it was queued with, and is dropped when the latest tick
    /// owes the chunk nothing: a backlog built on a wrong view drains on
    /// the next tick that sees the holders back.
    pub fn set_urgent_reencodes(&self, owed: UrgentOwed) {
        *self.urgent_latest.lock().unwrap() = Some(owed);
    }

    /// Spawn the engine on `data_rt`: one dispatch loop running the duty
    /// ladder as priority order — urgent re-encode > pull admission > lazy
    /// re-encode. Pulls run in a bounded window of blobs whose fetches
    /// share the scheduler's global and per-peer caps (`fetch`), so memory
    /// and bandwidth stay bounded per node; re-encodes run on the loop
    /// itself (one chunk of shards at a time) while admitted pulls proceed,
    /// and an urgent one stops admission until it is done.
    pub fn spawn<T, S, X, L>(
        seams: Seams<T, S, X, L>,
        config: EngineConfig,
        data_rt: tokio::runtime::Handle,
    ) -> EngineHandle
    where
        T: Transport + 'static,
        S: StateReader + 'static,
        X: TxSubmitter + 'static,
        L: LocalStateSink + 'static,
    {
        let (pull_tx, mut pull_rx) = mpsc::unbounded_channel::<PullRequest>();
        let (reencode_urgent_tx, mut reencode_urgent_rx) = mpsc::unbounded_channel::<ReencodeCmd>();
        let (reencode_lazy_tx, mut reencode_lazy_rx) = mpsc::unbounded_channel::<ReencodeCmd>();

        let fragments_dir = config.fragments_dir;
        let queued: PullQueue = Default::default();
        let worker_queued = queued.clone();
        let urgent_pending: UrgentPending = Default::default();
        let worker_urgent = urgent_pending.clone();
        let urgent_latest: UrgentLatest = Default::default();
        let worker_latest = urgent_latest.clone();
        let sched = match config.space {
            Some(space) => FetchScheduler::with_space(config.limits, space),
            None => FetchScheduler::new(config.limits),
        };
        let worker_sched = sched.clone();
        // Weak, so the loop still ends when every handle is dropped.
        let retry_tx = pull_tx.downgrade();
        let lane =
            evidence::EvidenceLane::spawn(seams.state.clone(), seams.submitter.clone(), &data_rt);
        data_rt.spawn(async move {
            let _drain = DrainOnExit(worker_queued.clone());
            let mut running: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
            let mut reprobe = tokio::time::interval(SPACE_REPROBE);
            reprobe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                // Below the pull floor nothing is taken off the pull queue
                // and lazy re-encode waits: owed blobs stay queued in the
                // planner's order (no failing, no parking) until free
                // space is back at the resume mark. Serving never stops.
                let held_back = worker_sched.space.paused();
                tokio::select! {
                    biased;
                    cmd = reencode_urgent_rx.recv() => {
                        let Some(cmd) = cmd else { break };
                        let key = (cmd.blob_id.clone(), cmd.chunk_number);
                        if let Some(cmd) = still_owed(&worker_latest, cmd) {
                            run_reencode_guarded(&seams, &fragments_dir, cmd, WriteClass::Repair, &worker_sched.space).await;
                        } else {
                            tracing::debug!(
                                "re-encode: blob {} chunk {} no longer owed — dropped",
                                key.0,
                                key.1
                            );
                        }
                        worker_urgent.lock().unwrap().remove(&key);
                    }
                    Some(_) = running.join_next(), if !running.is_empty() => {}
                    _ = reprobe.tick(), if held_back => {
                        worker_sched.space.reprobe(&fragments_dir);
                    }
                    admitted = async {
                        let permit = worker_sched.window.clone().acquire_owned().await;
                        (permit, pull_rx.recv().await)
                    }, if !held_back => {
                        let (Ok(permit), Some(blob_id)) = admitted else { break };
                        let entry = QueuedEntry {
                            queue: worker_queued.clone(),
                            blob_id: Some(blob_id.clone()),
                        };
                        if worker_sched.blob_parked(&blob_id, std::time::Instant::now()) {
                            // Its sources are dark: let the window move on.
                            entry.finish(PullOutcome::default());
                            continue;
                        }
                        let (seams, dir, lane, sched, queue, retry_tx) = (
                            seams.clone(),
                            fragments_dir.clone(),
                            lane.clone(),
                            worker_sched.clone(),
                            worker_queued.clone(),
                            retry_tx.clone(),
                        );
                        running.spawn(async move {
                            // Dropped on panic too: the blob leaves the queue.
                            let entry = entry;
                            let outcome = match pull_owed(&seams, &dir, &blob_id, &lane, &sched).await {
                                Ok(o) => o,
                                Err(e) => {
                                    tracing::warn!("pull: blob {blob_id} failed: {e}");
                                    PullOutcome::default()
                                }
                            };
                            drop(permit);
                            entry.finish(outcome);
                            // Waiting for a rebuild slot: back as soon as the
                            // short park ends, not when the planner's cursor
                            // next reaches this blob's slice.
                            if let Some(after) = outcome.retry_after {
                                tokio::spawn(async move {
                                    tokio::time::sleep(after).await;
                                    if let Some(tx) = retry_tx.upgrade() {
                                        enqueue_on(&queue, &tx, blob_id, None);
                                    }
                                });
                            }
                        });
                    }
                    cmd = reencode_lazy_rx.recv(), if !held_back => {
                        let Some(cmd) = cmd else { break };
                        run_reencode_guarded(&seams, &fragments_dir, cmd, WriteClass::Pull, &worker_sched.space).await;
                    }
                }
            }
        });

        EngineHandle {
            pull_tx,
            reencode_urgent_tx,
            reencode_lazy_tx,
            queued,
            urgent_pending,
            urgent_latest,
            sched,
        }
    }
}

/// The queued urgent command as the latest tick still owes it: with the
/// classes the latest tick owes for its chunk (a class the tick no longer
/// owes is not rebuilt; one it newly owes rides along), or `None` when it
/// owes the chunk nothing. Before any tick has published, as queued.
fn still_owed(latest: &UrgentLatest, mut cmd: ReencodeCmd) -> Option<ReencodeCmd> {
    let latest = latest.lock().unwrap();
    let Some(owed) = latest.as_ref() else {
        return Some(cmd);
    };
    let classes = owed.get(&(cmd.blob_id.clone(), cmd.chunk_number))?;
    if classes.is_empty() {
        return None;
    }
    cmd.missing_classes.clone_from(classes);
    Some(cmd)
}

/// Run one re-encode command as its own task and wait for it: still one
/// at a time on the dispatch loop, but a panic in it ends that task, not
/// the loop that every pull depends on.
async fn run_reencode_guarded<T, S, X, L>(
    seams: &Seams<T, S, X, L>,
    fragments_dir: &str,
    cmd: ReencodeCmd,
    class: WriteClass,
    space: &Arc<SpaceGuard>,
) where
    T: Transport + 'static,
    S: StateReader + 'static,
    X: TxSubmitter + 'static,
    L: LocalStateSink + 'static,
{
    let (seams, dir, space) = (seams.clone(), fragments_dir.to_string(), space.clone());
    let (blob_id, chunk) = (cmd.blob_id.clone(), cmd.chunk_number);
    if let Err(e) =
        tokio::spawn(async move { run_reencode_cmd(&seams, &dir, cmd, class, &space).await }).await
    {
        tracing::error!("re-encode: blob {blob_id} chunk {chunk} task failed: {e}");
    }
}

/// Run one re-encode command (errors logged, not propagated — the next
/// tick's scan re-elects and retries). Urgent repair writes under
/// `WriteClass::Repair` (down to the ingest floor), lazy under `Pull`.
async fn run_reencode_cmd<T, S, X, L>(
    seams: &Seams<T, S, X, L>,
    fragments_dir: &str,
    cmd: ReencodeCmd,
    class: WriteClass,
    space: &SpaceGuard,
) where
    T: Transport + 'static,
    S: StateReader,
    X: TxSubmitter,
    L: LocalStateSink,
{
    match reencode::reencode_chunk(
        &seams.transport,
        seams.state.as_ref(),
        seams.local_state.as_ref(),
        fragments_dir,
        &cmd.blob_id,
        cmd.chunk_number,
        &cmd.missing_classes,
        (space, class),
    )
    .await
    {
        Ok(_) => {}
        Err(EngineError::NoSpace) => tracing::debug!(
            "re-encode: blob {} chunk {} held for space",
            cmd.blob_id,
            cmd.chunk_number
        ),
        Err(e) => tracing::warn!(
            "re-encode: blob {} chunk {} failed: {e}",
            cmd.blob_id,
            cmd.chunk_number
        ),
    }
}

/// The pull check for one blob on this node: derive the owed classes from
/// the goal, fetch each with recovery, then content-verify the blob's
/// fragments and hand the evidence to the lane — which reports belief and
/// disk truth and proposes confirmation in pages. The worker never waits
/// on consensus.
async fn pull_owed<T, S, X, L>(
    seams: &Seams<T, S, X, L>,
    fragments_dir: &str,
    blob_id: &BlobId,
    lane: &evidence::EvidenceLane,
    sched: &Arc<FetchScheduler>,
) -> Result<PullOutcome, EngineError>
where
    T: Transport + 'static,
    S: StateReader + 'static,
    X: TxSubmitter,
    L: LocalStateSink + 'static,
{
    let mut outcome = PullOutcome::default();
    let Some(target) = seams.state.pull_target(blob_id)? else {
        // Unknown blob (raced a delete), or the record does not reach the
        // goal yet: nothing is owed until it does.
        sched.unpark_blob(blob_id);
        return Ok(outcome);
    };
    let Some(manifest) = seams.state.blob_manifest(blob_id)? else {
        sched.unpark_blob(blob_id);
        return Ok(outcome);
    };
    let me = seams
        .state
        .local_node_id()
        .ok_or_else(|| EngineError::Transfer("local node id not set".to_string()))?;

    // Owed: classes assigned to me under the goal that are not on disk.
    let mut owed: BTreeMap<u32, Vec<(u32, Blake3Hash)>> = BTreeMap::new();
    let mut holds_any = false;
    for (chunk, (originals, recovery)) in &manifest.chunks {
        for map in [originals, recovery] {
            for (local_index, (hash, _, stored_locally)) in map {
                if *stored_locally {
                    holds_any = true;
                    continue;
                }
                if target.assignment.get(*local_index).copied() == Some(me) {
                    owed.entry(*chunk)
                        .or_default()
                        .push((*local_index as u32, *hash));
                }
            }
        }
    }
    outcome.owed = owed.values().map(Vec::len).sum();
    if outcome.owed == 0 {
        // Owing nothing (pulled since, or re-goaled away): it has no
        // business in the park book.
        sched.unpark_blob(blob_id);
    }

    if outcome.owed > 0 {
        let hashes: Vec<Blake3Hash> = owed.values().flatten().map(|(_, h)| *h).collect();
        let mut sources = seams.state.fragment_sources(&hashes)?;
        let all_peers = seams.state.all_peers()?;
        // Sources the host's liveness evidence calls dark are skipped like
        // parked ones: the blob parks instead of holding window slots.
        let dark: HashSet<i32> = all_peers
            .iter()
            .map(|p| p.node_id)
            .filter(|n| !seams.state.peer_reachable(*n))
            .collect();
        let others: Arc<Vec<crate::traits::PeerRef>> = Arc::new(
            all_peers
                .into_iter()
                .filter(|p| !dark.contains(&p.node_id))
                .collect(),
        );
        // Inventory rows of nodes that have left the storage view (decayed
        // out on availability) outlive them: they are not holders to wait for. A
        // class held only by non-members is unserved, so it rebuilds; a
        // live non-member is still reached through discovery.
        let members = sched.members(&seams.state, std::time::Instant::now()).await;
        let is_member = |node: i32| {
            members
                .as_ref()
                .is_none_or(|m: &Arc<HashSet<i32>>| m.contains(&node))
        };

        // Every owed class at once, at most `per_blob` in flight, each under
        // the scheduler's global and per-peer caps.
        let per_blob = Arc::new(tokio::sync::Semaphore::new(sched.limits.per_blob()));
        let mut fetches: tokio::task::JoinSet<(u32, u32, Result<(), FetchMiss>)> =
            tokio::task::JoinSet::new();
        for (chunk, classes) in &owed {
            for (class, hash) in classes {
                let known: Vec<crate::traits::PeerRef> = sources
                    .remove(hash)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|p| is_member(p.node_id))
                    .collect();
                let attested: Vec<crate::traits::PeerRef> = known
                    .iter()
                    .copied()
                    .filter(|p| !dark.contains(&p.node_id))
                    .collect();
                let all_dark = !known.is_empty() && attested.is_empty();
                let (chunk, class, hash) = (*chunk, *class, *hash);
                let (transport, local_state, sched, per_blob, others, dir) = (
                    seams.transport.clone(),
                    seams.local_state.clone(),
                    sched.clone(),
                    per_blob.clone(),
                    others.clone(),
                    fragments_dir.to_string(),
                );
                fetches.spawn(async move {
                    let Ok(_slot) = per_blob.acquire_owned().await else {
                        return (chunk, class, Err(FetchMiss::NotServed));
                    };
                    if all_dark {
                        return (chunk, class, Err(FetchMiss::Unreachable));
                    }
                    // Below the pull floor: don't download what can't be
                    // written.
                    if sched.space.paused() {
                        return (chunk, class, Err(FetchMiss::NoSpace));
                    }
                    let fetch::Fetched { data, slot } =
                        match fetch::fetch_class(&transport, &sched, &hash, &attested, &others)
                            .await
                        {
                            Ok(fetched) => fetched,
                            Err(miss) => return (chunk, class, Err(miss)),
                        };
                    // The bytes are reserved against the pull floor before
                    // they land (a refusal pauses the guard), and the
                    // reservation and the global slot are held until they
                    // are on disk.
                    let Ok(reserved) = sched.space.reserve(
                        &dir,
                        admission::fragment_file_bytes(data.len()),
                        WriteClass::Pull,
                    ) else {
                        return (chunk, class, Err(FetchMiss::NoSpace));
                    };
                    let stored = tokio::task::spawn_blocking(move || {
                        fragstore::store_fragment(&dir, &hash, data)
                    })
                    .await;
                    drop((slot, reserved));
                    match stored {
                        Ok(Ok(())) => {
                            local_state.mark_local(hash).await;
                            (chunk, class, Ok(()))
                        }
                        // The disk filled under us (another writer): held
                        // for space, never read as the holder not serving
                        // it, which would rebuild — fetch K shards to
                        // write more.
                        Ok(Err(e)) if admission::is_disk_full(&e) => {
                            sched.space.note_disk_full();
                            (chunk, class, Err(FetchMiss::NoSpace))
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("pull: store fragment {} failed: {e}", hash.to_hex());
                            (chunk, class, Err(FetchMiss::NotServed))
                        }
                        Err(e) => {
                            tracing::warn!("pull: store fragment {} join: {e}", hash.to_hex());
                            (chunk, class, Err(FetchMiss::NotServed))
                        }
                    }
                });
            }
        }
        // chunk -> (classes to rebuild, classes waiting on a member holder)
        let mut unserved_by_chunk: BTreeMap<u32, (Vec<u32>, Vec<u32>)> = BTreeMap::new();
        while let Some(joined) = fetches.join_next().await {
            match joined {
                Ok((_, _, Ok(()))) => outcome.pulled += 1,
                Ok((_, _, Err(FetchMiss::NoSpace))) => outcome.held_for_space += 1,
                Ok((chunk, class, Err(miss))) => {
                    let entry = unserved_by_chunk.entry(chunk).or_default();
                    if miss == FetchMiss::Unreachable {
                        entry.1.push(class);
                    } else {
                        entry.0.push(class);
                    }
                }
                Err(e) => {
                    tracing::warn!("pull: blob {blob_id}: fetch task failed: {e}");
                    outcome.failed += 1;
                }
            }
        }

        let mut park = false;
        let mut wait_for_rebuild_slot = false;
        // The rebuild rule: a class is rebuilt only when every holder has
        // left the storage view (filtered out above, so it arrives here as
        // unserved) or every reachable holder says it does not have it.
        // A member that is dark, parked, slow or busy is never rebuilt
        // around: the blob parks with backoff. Its classes become
        // rebuildable only once it leaves the storage view, and storage
        // membership is derived from replicated availability
        // (`membership::derive_view` over the availability history, the
        // MAX of `available` across observers), not from consensus
        // vote-out. KNOWN LIMITATION (2026.10.8): a node that still
        // answers availability probes but cannot serve fragments (wedged
        // store, full disk) stays a member, so its classes are never
        // rebuilt around and its blobs stay parked. The next release makes
        // availability reflect serving.
        // The rule is per class: a class waiting on a member holder parks
        // the blob, but does not hold back a sibling class in the same
        // chunk whose reachable holders all said they do not have it.
        for (chunk, (mut unserved, waiting)) in unserved_by_chunk {
            if !waiting.is_empty() {
                outcome.failed += waiting.len();
                park = true;
            }
            if unserved.is_empty() {
                continue;
            }
            unserved.sort_unstable();
            // Held for space: a rebuild fetches K shards to write more.
            if sched.space.paused() {
                outcome.held_for_space += unserved.len();
                continue;
            }
            // Genuinely unserved: rebuild from any K live classes (local
            // shards first), a bounded number at once, with every shard
            // fetch under the scheduler's caps and deadline and dark or
            // parked peers skipped. No rebuild slot free: park and retry
            // rather than hold a window slot while queued for one.
            let Ok(_rebuild) = sched.rebuild.clone().try_acquire_owned() else {
                outcome.failed += unserved.len();
                wait_for_rebuild_slot = true;
                continue;
            };
            let (transport, dark, view) = (&seams.transport, &dark, &members);
            let rebuilt = reencode::reencode_chunk_via(
                seams.state.as_ref(),
                seams.local_state.as_ref(),
                fragments_dir,
                blob_id,
                chunk,
                &unserved,
                (&sched.space, WriteClass::Pull),
                |hash, hint, view_members| async move {
                    let usable = |p: &crate::traits::PeerRef| {
                        !dark.contains(&p.node_id)
                            && view.as_ref().is_none_or(|m| m.contains(&p.node_id))
                    };
                    let attested: Vec<_> = hint.into_iter().filter(usable).collect();
                    let others: Vec<_> = view_members.into_iter().filter(usable).collect();
                    fetch::fetch_class(transport, sched, &hash, &attested, &others)
                        .await
                        .ok()
                        .map(|fetched| fetched.data)
                },
            )
            .await;
            match rebuilt {
                Ok(r) => {
                    outcome.rebuilt += r.regenerated;
                    outcome.failed += unserved.len().saturating_sub(r.regenerated);
                }
                Err(EngineError::NoSpace) => outcome.held_for_space += unserved.len(),
                Err(e) => {
                    tracing::warn!(
                        "pull: blob {blob_id} chunk {chunk}: {} classes unsourceable and rebuild failed: {e}",
                        unserved.len()
                    );
                    outcome.failed += unserved.len();
                }
            }
        }

        // A class whose known holders are all dark parks the blob so the
        // window admits blobs that can move. A blob with nothing left
        // failing leaves the park book.
        let now = std::time::Instant::now();
        if park {
            sched.park_blob(blob_id, now);
            tracing::debug!("pull: blob {blob_id} parked: its sources are unreachable");
        } else if wait_for_rebuild_slot {
            outcome.retry_after = sched.park_blob_for_rebuild(blob_id, now);
            tracing::debug!("pull: blob {blob_id} waits for a rebuild slot");
        } else if outcome.failed == 0 {
            sched.unpark_blob(blob_id);
        }
    }

    // Prompt evidence is for births and moved bytes only: a never-confirmed
    // blob this node holds (the origin's classes at birth), or a blob this
    // pull just changed on disk. A re-goaled blob whose classes were
    // already here submits nothing — its rows are the sweep's, its
    // confirmation is the fulfillment floor's batched one (RFC-STORAGE-003:
    // fulfillment is the bulk path; awaiting a consensus round per rubber
    // stamp on the serial worker is what capped the 500-blob drain at
    // ~30 confirms a minute).
    let in_flight = target.placement_height != Some(target.desired);
    let moved_bytes = outcome.pulled + outcome.rebuilt > 0;
    let birth_holder = target.placement_height.is_none() && holds_any;
    if in_flight && (moved_bytes || birth_holder) {
        // The height is read BEFORE the rehash, so the attestation never
        // claims a fragment was seen later than it was (the rolling
        // sweep's rule). Content-verifying every fragment of the blob reads
        // and hashes each file: blocking work, off the async worker.
        let height = seams.state.current_height()?;
        let candidates: Vec<Blake3Hash> = manifest
            .chunks
            .values()
            .flat_map(|(o, r)| o.values().chain(r.values()))
            .map(|(hash, _, _)| *hash)
            .collect();
        let dir = fragments_dir.to_string();
        let present: Vec<Blake3Hash> = tokio::task::spawn_blocking(move || {
            candidates
                .into_iter()
                .filter(|hash| fragstore::fragment_exists_and_valid(&dir, hash))
                .collect()
        })
        .await
        .map_err(|e| EngineError::Transfer(format!("attestation rehash join: {e}")))?;
        // Belief (computed at flush, after the marks landed), truth and the
        // confirmation check ride the lane's pages: no consensus round is
        // awaited here.
        if !present.is_empty() {
            outcome.evidence_queued = lane.push(evidence::EvidenceItem {
                blob_id: blob_id.clone(),
                height,
                present,
            });
        }
    }

    if outcome.owed > 0 {
        tracing::info!(
            "pull: blob {} owed {} classes — pulled {}, rebuilt {}, failed {}{}{}",
            blob_id,
            outcome.owed,
            outcome.pulled,
            outcome.rebuilt,
            outcome.failed,
            if outcome.held_for_space > 0 {
                format!(", held for space {}", outcome.held_for_space)
            } else {
                String::new()
            },
            if outcome.evidence_queued {
                " (evidence queued)"
            } else {
                ""
            }
        );
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{ConfirmPlacement, CONFIRM_TX_FN};
    use crate::traits::{
        PeerRef, PlacementInputs, PullTarget, StoreResult, SubmitError, TransportError,
    };
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Mutex;

    /// Seams for pull tests: one remote peer (node 2) serving fragments
    /// from a map; the goal assigns every class to node 1 (me).
    struct PullNet {
        served: Mutex<HashMap<Blake3Hash, Vec<u8>>>,
        manifest: Mutex<Option<crate::store::BlobManifest>>,
        target: Option<PullTarget>,
        ready: Option<u64>,
        marked_local: Mutex<Vec<Blake3Hash>>,
        submitted: Mutex<Vec<&'static str>>,
        /// Hashes consensus already believes this node holds — what the
        /// blob-scoped report filters against.
        inventoried: Mutex<std::collections::HashSet<Blake3Hash>>,
        /// Every submitted (function, payload), for decoding in asserts.
        payloads: Mutex<Vec<(&'static str, Vec<u8>)>>,
    }

    fn peers(ids: &[i32]) -> Vec<PeerRef> {
        ids.iter()
            .map(|&node_id| PeerRef {
                node_id,
                pubkey: [node_id as u8; 32],
            })
            .collect()
    }

    impl Transport for PullNet {
        async fn store_fragment(
            &self,
            _peer: &PeerRef,
            _fragment_hash: &Blake3Hash,
            _data: Vec<u8>,
        ) -> Result<StoreResult, TransportError> {
            Err(TransportError::Transport("not used".into()))
        }
        async fn fetch_fragment(
            &self,
            _peer: &PeerRef,
            fragment_hash: &Blake3Hash,
        ) -> Result<Vec<u8>, TransportError> {
            self.served
                .lock()
                .unwrap()
                .get(fragment_hash)
                .cloned()
                .ok_or_else(|| TransportError::Peer("fragment not found".into()))
        }
        async fn fragment_health(
            &self,
            _peer: &PeerRef,
            fragment_hash: &Blake3Hash,
        ) -> Result<bool, TransportError> {
            Ok(self.served.lock().unwrap().contains_key(fragment_hash))
        }
    }

    impl StateReader for PullNet {
        fn placement_inputs(&self) -> Result<PlacementInputs, StorageError> {
            Ok(PlacementInputs {
                height: 9,
                validators: peers(&[1, 2]),
                metrics: vec![],
            })
        }
        fn placement_inputs_at(&self, height: u64) -> Result<PlacementInputs, StorageError> {
            Ok(PlacementInputs {
                height,
                validators: peers(&[2]),
                metrics: vec![],
            })
        }
        fn fragment_sources(
            &self,
            _fragment_hashes: &[Blake3Hash],
        ) -> Result<HashMap<Blake3Hash, Vec<PeerRef>>, StorageError> {
            Ok(HashMap::new())
        }
        fn all_peers(&self) -> Result<Vec<PeerRef>, StorageError> {
            Ok(peers(&[2]))
        }
        fn pull_target(&self, _blob_id: &BlobId) -> Result<Option<PullTarget>, StorageError> {
            Ok(self.target.clone())
        }
        fn self_check_report(&self) -> Result<crate::types::SelfCheckFragments, StorageError> {
            // Report the fragments marked local so far as newly attested.
            Ok(crate::types::SelfCheckFragments {
                node_id: 1,
                self_verified_height: 9,
                previous_count: 0,
                fragments_added: self.marked_local.lock().unwrap().clone(),
                fragments_removed: vec![],
            })
        }
        fn blob_self_check_report(
            &self,
            _blob_id: &BlobId,
        ) -> Result<crate::types::SelfCheckFragments, StorageError> {
            // What the host query computes: the manifest's classes this node
            // holds (flagged before the pull, or marked local by it) that
            // consensus has no row for yet.
            let inventoried = self.inventoried.lock().unwrap();
            let marked: std::collections::HashSet<Blake3Hash> =
                self.marked_local.lock().unwrap().iter().copied().collect();
            let mut fragments_added = Vec::new();
            if let Some(manifest) = self.manifest.lock().unwrap().as_ref() {
                for (hash, _, local) in manifest
                    .chunks
                    .values()
                    .flat_map(|(o, r)| o.values().chain(r.values()))
                {
                    if (*local || marked.contains(hash)) && !inventoried.contains(hash) {
                        fragments_added.push(*hash);
                    }
                }
            }
            Ok(crate::types::SelfCheckFragments {
                node_id: 1,
                self_verified_height: 9,
                previous_count: 0,
                fragments_added,
                fragments_removed: vec![],
            })
        }
        fn confirm_ready(&self, _blob_id: &BlobId) -> Result<Option<u64>, StorageError> {
            Ok(self.ready)
        }
        fn current_height(&self) -> Result<u64, StorageError> {
            Ok(9)
        }
        fn blob_manifest(
            &self,
            _blob_id: &BlobId,
        ) -> Result<Option<crate::store::BlobManifest>, StorageError> {
            Ok(self.manifest.lock().unwrap().clone())
        }
        fn local_node_id(&self) -> Option<i32> {
            Some(1)
        }
    }

    impl TxSubmitter for PullNet {
        async fn submit(
            &self,
            function: &'static str,
            payload: Vec<u8>,
        ) -> Result<(), SubmitError> {
            self.submitted.lock().unwrap().push(function);
            self.payloads.lock().unwrap().push((function, payload));
            Ok(())
        }
    }

    /// The seam a duplicate belief hits in production: the submitter
    /// refuses the self-check, everything else goes through.
    struct RejectSelfCheck(Arc<PullNet>);

    impl TxSubmitter for RejectSelfCheck {
        async fn submit(
            &self,
            function: &'static str,
            payload: Vec<u8>,
        ) -> Result<(), SubmitError> {
            if function == policy::SELF_CHECK_FN {
                return Err(SubmitError::Rejected("already believed".into()));
            }
            self.0.submit(function, payload).await
        }
    }

    /// The hashes a self-check payload asserts, as a set.
    fn self_check_hashes(
        payloads: &[(&'static str, Vec<u8>)],
    ) -> std::collections::HashSet<Blake3Hash> {
        payloads
            .iter()
            .filter(|(f, _)| *f == policy::SELF_CHECK_FN)
            .flat_map(|(_, bytes)| {
                let (report, _): (crate::types::SelfCheckFragments, _) =
                    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                        .expect("self-check payload decodes");
                report.fragments_added
            })
            .collect()
    }

    /// One pull with a test lane: the outcome and the evidence it queued.
    async fn pull_with_lane<T, S, X, L>(
        seams: &Seams<T, S, X, L>,
        dir: &str,
        blob_id: &BlobId,
    ) -> (PullOutcome, Vec<evidence::EvidenceItem>)
    where
        T: Transport + 'static,
        S: StateReader + 'static,
        X: TxSubmitter,
        L: LocalStateSink + 'static,
    {
        let sched = FetchScheduler::new(PullLimits::default());
        pull_with_sched(seams, dir, blob_id, &sched).await
    }

    async fn pull_with_sched<T, S, X, L>(
        seams: &Seams<T, S, X, L>,
        dir: &str,
        blob_id: &BlobId,
        sched: &Arc<FetchScheduler>,
    ) -> (PullOutcome, Vec<evidence::EvidenceItem>)
    where
        T: Transport + 'static,
        S: StateReader + 'static,
        X: TxSubmitter,
        L: LocalStateSink + 'static,
    {
        let (lane, mut rx) = evidence::EvidenceLane::channel();
        let outcome = pull_owed(seams, dir, blob_id, &lane, sched).await.unwrap();
        let mut items = Vec::new();
        while let Ok(item) = rx.try_recv() {
            items.push(item);
        }
        (outcome, items)
    }

    /// PullNet's state, except that every fragment is attested on node 2,
    /// which the host's liveness evidence calls reachable or not.
    struct HeldOnTwo {
        net: Arc<PullNet>,
        two_reachable: bool,
        /// Node 2 is in the storage view (else it departed, rows remain).
        two_member: bool,
        /// Reading any manifest panics (a re-encode that blows up).
        panic_manifest: bool,
        /// Deriving the storage view takes this long (a cold, slow node).
        view_delay: std::time::Duration,
        /// Fragments attested on node 3 instead, a reachable member (in
        /// the view whenever this is non-empty).
        held_on_three: HashSet<Blake3Hash>,
        /// Deriving the storage view fails (a locked or broken database).
        view_fails: std::sync::atomic::AtomicBool,
        /// Storage-view derivations attempted.
        view_calls: std::sync::atomic::AtomicUsize,
    }

    impl StateReader for HeldOnTwo {
        fn placement_inputs(&self) -> Result<PlacementInputs, StorageError> {
            self.net.placement_inputs()
        }
        fn storage_view(&self) -> Result<crate::traits::StorageView, StorageError> {
            std::thread::sleep(self.view_delay);
            self.view_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.view_fails.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(StorageError::Host("view unreadable".into()));
            }
            let mut members = if self.two_member {
                peers(&[1, 2])
            } else {
                peers(&[1])
            };
            if !self.held_on_three.is_empty() {
                members.extend(peers(&[3]));
            }
            Ok(crate::traits::StorageView {
                height: 9,
                watermark: 1,
                tiers: HashMap::new(),
                weights: HashMap::new(),
                online: members.iter().map(|p| p.node_id).collect(),
                absence: Default::default(),
                grid_step_secs: 0,
                members,
            })
        }
        fn placement_inputs_at(&self, height: u64) -> Result<PlacementInputs, StorageError> {
            self.net.placement_inputs_at(height)
        }
        fn fragment_sources(
            &self,
            fragment_hashes: &[Blake3Hash],
        ) -> Result<HashMap<Blake3Hash, Vec<PeerRef>>, StorageError> {
            Ok(fragment_hashes
                .iter()
                .map(|h| {
                    let holder = if self.held_on_three.contains(h) { 3 } else { 2 };
                    (*h, peers(&[holder]))
                })
                .collect())
        }
        fn all_peers(&self) -> Result<Vec<PeerRef>, StorageError> {
            self.net.all_peers()
        }
        fn pull_target(&self, blob_id: &BlobId) -> Result<Option<PullTarget>, StorageError> {
            self.net.pull_target(blob_id)
        }
        fn self_check_report(&self) -> Result<crate::types::SelfCheckFragments, StorageError> {
            self.net.self_check_report()
        }
        fn blob_self_check_report(
            &self,
            blob_id: &BlobId,
        ) -> Result<crate::types::SelfCheckFragments, StorageError> {
            self.net.blob_self_check_report(blob_id)
        }
        fn confirm_ready(&self, blob_id: &BlobId) -> Result<Option<u64>, StorageError> {
            self.net.confirm_ready(blob_id)
        }
        fn current_height(&self) -> Result<u64, StorageError> {
            self.net.current_height()
        }
        fn blob_manifest(
            &self,
            blob_id: &BlobId,
        ) -> Result<Option<crate::store::BlobManifest>, StorageError> {
            assert!(!self.panic_manifest, "manifest read panics");
            self.net.blob_manifest(blob_id)
        }
        fn local_node_id(&self) -> Option<i32> {
            self.net.local_node_id()
        }
        fn peer_reachable(&self, node_id: i32) -> bool {
            node_id != 2 || self.two_reachable
        }
    }

    // Impact: review of #96 — each of the 64 window tasks rebuilt chunks
    // when a holder was merely dark, holding ~40-120 MB of shards apiece
    // outside every cap, for classes the holder would serve once back.
    // Should: park a blob whose unserved classes are held only by dark
    // peers, without rebuilding.
    // Should: still try a rebuild (and not park) when the holders answer
    // but do not serve the class.
    #[tokio::test(flavor = "multi_thread")]
    async fn dark_holders_park_the_blob_instead_of_rebuilding() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-dark-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, _outcome, manifest) = encoded_blob(&dir_src).await;
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });

        for (two_reachable, parks) in [(false, true), (true, false)] {
            let seams = Seams {
                transport: net.clone(),
                state: Arc::new(HeldOnTwo {
                    net: net.clone(),
                    two_reachable,
                    two_member: true,
                    panic_manifest: false,
                    view_delay: std::time::Duration::ZERO,
                    held_on_three: HashSet::new(),
                    view_fails: false.into(),
                    view_calls: 0.into(),
                }),
                submitter: net.clone(),
                local_state: net.clone(),
            };
            let sched = FetchScheduler::new(PullLimits::default());
            let (result, _) = pull_with_sched(&seams, &dir_dst, &blob_id, &sched).await;
            assert_eq!(result.failed, 30);
            assert_eq!(result.rebuilt, 0);
            assert_eq!(
                sched.blob_parked(&blob_id, std::time::Instant::now()),
                parks,
                "node 2 reachable: {two_reachable}"
            );
        }
        let _ = std::fs::remove_dir_all(&base);
    }

    // Impact: inventory rows of departed nodes counted as holders to wait
    // for, so their classes were never rebuilt; and rebuilding around a
    // member that is merely dark spends memory and makes copies its holder
    // will serve again once back (decided with Allison: a member is rebuilt
    // around only once it decays out of the availability-derived storage
    // view; a wedged-but-available member is a known 10.8 limitation).
    // Should: rebuild (not park) a class whose only holder has left the
    // storage view, even while that node is dark.
    // Should not: rebuild around a member holder, however long it has been
    // dark: the blob stays parked.
    #[tokio::test(flavor = "multi_thread")]
    async fn departed_holders_rebuild_and_dark_members_do_not() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-escal-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, _outcome, manifest) = encoded_blob(&dir_src).await;
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        let seams_for = |two_member: bool| Seams {
            transport: net.clone(),
            state: Arc::new(HeldOnTwo {
                net: net.clone(),
                two_reachable: false,
                two_member,
                panic_manifest: false,
                view_delay: std::time::Duration::ZERO,
                held_on_three: HashSet::new(),
                view_fails: false.into(),
                view_calls: 0.into(),
            }),
            submitter: net.clone(),
            local_state: net.clone(),
        };

        // Departed: its rows are not holders; the rebuild runs (and, with
        // no shards anywhere, fails), and the blob is not parked.
        let sched = FetchScheduler::new(PullLimits::default());
        pull_with_sched(&seams_for(false), &dir_dst, &blob_id, &sched).await;
        assert!(!sched.blob_parked(&blob_id, std::time::Instant::now()));

        // A member dark for a long time (its park entry hours old): still
        // parked, never rebuilt around.
        let now = std::time::Instant::now();
        let sched = FetchScheduler::new(PullLimits::default());
        if let Some(long_ago) = now.checked_sub(std::time::Duration::from_secs(4 * 3600)) {
            sched.park_blob(&blob_id, long_ago);
        }
        let (result, _) = pull_with_sched(&seams_for(true), &dir_dst, &blob_id, &sched).await;
        assert_eq!(result.rebuilt, 0);
        assert!(sched.blob_parked(&blob_id, std::time::Instant::now()));
        let _ = std::fs::remove_dir_all(&base);
    }

    // Impact: final review of #96 — the wait-for-a-member flag was per
    // chunk, so one class on a dark member held back the rebuild of every
    // sibling class in that chunk, even ones whose reachable holders had
    // all said they do not have it.
    // Should: rebuild a class whose reachable member holders do not serve
    // it, while another class of the same chunk waits on a dark member.
    // Should: still park the blob for the class on the dark member.
    // Should not: rebuild the class held by the dark member.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_class_on_a_dark_member_does_not_hold_back_its_siblings() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-split-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, outcome, manifest) = encoded_blob(&dir_src).await;
        let mut chunk0: Vec<_> = outcome
            .fragments
            .iter()
            .filter(|f| f.chunk_number == 0)
            .map(|f| f.fragment_hash)
            .collect();
        chunk0.sort_unstable_by_key(|h| h.to_hex());
        // Class `on_dark` stays on dark member 2; `lost` is attested on
        // node 3, which answers but no longer has it; node 3 serves the rest.
        let (on_dark, lost) = (chunk0[0], chunk0[1]);
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        for f in &outcome.fragments {
            if f.fragment_hash != on_dark && f.fragment_hash != lost {
                let bytes = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
                net.served.lock().unwrap().insert(f.fragment_hash, bytes);
            }
        }
        let held_on_three = outcome
            .fragments
            .iter()
            .map(|f| f.fragment_hash)
            .filter(|h| *h != on_dark)
            .collect();
        let seams = Seams {
            transport: net.clone(),
            state: Arc::new(HeldOnTwo {
                net: net.clone(),
                two_reachable: false,
                two_member: true,
                panic_manifest: false,
                view_delay: std::time::Duration::ZERO,
                held_on_three,
                view_fails: false.into(),
                view_calls: 0.into(),
            }),
            submitter: net.clone(),
            local_state: net.clone(),
        };
        let sched = FetchScheduler::new(PullLimits::default());
        let (result, _) = pull_with_sched(&seams, &dir_dst, &blob_id, &sched).await;
        assert_eq!(result.rebuilt, 1, "the lost class is rebuilt");
        assert_eq!(result.failed, 1, "the dark member's class waits");
        assert!(fragstore::read_fragment(&dir_dst, &lost).is_ok());
        assert!(fragstore::read_fragment(&dir_dst, &on_dark).is_err());
        assert!(sched.blob_parked(&blob_id, std::time::Instant::now()));
        let _ = std::fs::remove_dir_all(&base);
    }

    // Impact: final review of #96 — the 30 s rebuild-slot park did not
    // bring the blob back after 30 s: the planner skips parked blobs and
    // the feeder drops them, so it waited for the cursor to revisit its
    // slice (a full walk of the in-flight set).
    // Should: offer a blob that waited for a rebuild slot again once the
    // short park ends, and rebuild it then.
    #[tokio::test(start_paused = true)]
    async fn a_blob_waiting_for_a_rebuild_slot_comes_back_by_itself() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-retry-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, outcome, manifest) = encoded_blob(&dir_src).await;
        let lost = outcome.fragments[0].fragment_hash;
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        for f in &outcome.fragments[1..] {
            let bytes = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
            net.served.lock().unwrap().insert(f.fragment_hash, bytes);
        }
        let seams = Seams {
            transport: net.clone(),
            state: Arc::new(HeldOnTwo {
                net: net.clone(),
                two_reachable: true,
                two_member: false,
                panic_manifest: false,
                view_delay: std::time::Duration::ZERO,
                held_on_three: outcome.fragments.iter().map(|f| f.fragment_hash).collect(),
                view_fails: false.into(),
                view_calls: 0.into(),
            }),
            submitter: net.clone(),
            local_state: net.clone(),
        };
        let engine = EngineHandle::spawn(
            seams,
            EngineConfig {
                space: None,
                fragments_dir: dir_dst.clone(),
                limits: PullLimits {
                    rebuilds: 1,
                    ..PullLimits::default()
                },
            },
            tokio::runtime::Handle::current(),
        );

        // Every rebuild slot busy: the lost class waits.
        let busy = engine.sched.rebuild.clone().try_acquire_owned().unwrap();
        let first = engine.pull_blob(blob_id.clone()).await.unwrap();
        assert_eq!((first.rebuilt, first.failed), (0, 1));
        assert_eq!(first.retry_after, Some(fetch::REBUILD_WAIT_PARK));
        drop(busy);
        // The park book runs on the wall clock, which the paused test clock
        // does not move: end the short park by hand.
        assert!(engine.blob_parked(&blob_id));
        engine.sched.unpark_blob(&blob_id);

        // Nobody offers it again; it is back and rebuilt within the wait.
        let started = tokio::time::Instant::now();
        while fragstore::read_fragment(&dir_dst, &lost).is_err() {
            assert!(
                started.elapsed() < fetch::REBUILD_WAIT_PARK * 2,
                "not offered again after its rebuild wait"
            );
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        let _ = std::fs::remove_dir_all(&base);
    }

    // Impact: third review of #96 — the membership cache derived the
    // storage view (pool checkout plus 30 days of availability history)
    // under its lock inside the async pull, so on every expiry each window
    // task blocked behind one slow derivation.
    // Should: refresh a stale membership in the background, once, and let
    // every caller use the stale value meanwhile.
    // Should not: make a caller wait for the derivation once any value
    // exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_membership_is_refreshed_without_blocking_callers() {
        let delay = std::time::Duration::from_millis(800);
        let state = Arc::new(HeldOnTwo {
            net: idle_net(),
            two_reachable: true,
            two_member: true,
            panic_manifest: false,
            view_delay: delay,
            held_on_three: HashSet::new(),
            view_fails: false.into(),
            view_calls: 0.into(),
        });
        let sched = FetchScheduler::new(PullLimits::default());
        let t0 = std::time::Instant::now();
        let first = sched.members(&state, t0).await.unwrap();
        assert_eq!(*first, [1, 2].into_iter().collect::<HashSet<i32>>());

        // Stale: every caller returns at once with the stale value.
        let stale_at = std::time::Instant::now() + fetch::MEMBERS_TTL;
        let started = std::time::Instant::now();
        for _ in 0..8 {
            assert!(sched.members(&state, stale_at).await.is_some());
        }
        assert!(
            started.elapsed() < delay / 2,
            "callers waited {:?}",
            started.elapsed()
        );
    }

    // Impact: final review of #96 — a failed membership refresh was retried
    // on the very next call (a storage-view derivation per pull while the
    // database is unhappy), and the last good list stayed in force forever,
    // filtering holders by a membership that may be hours old.
    // Should: back off refresh retries after a failure, doubling.
    // Should: fail open (no membership filter) once the last good list is
    // older than the stale bound.
    // Should: use a fresh list again as soon as a refresh succeeds.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_membership_refresh_backs_off_and_fails_open() {
        use std::sync::atomic::Ordering;
        let state = Arc::new(HeldOnTwo {
            net: idle_net(),
            two_reachable: true,
            two_member: true,
            panic_manifest: false,
            view_delay: std::time::Duration::ZERO,
            held_on_three: HashSet::new(),
            view_fails: false.into(),
            view_calls: 0.into(),
        });
        let sched = FetchScheduler::new(PullLimits::default());
        let calls = || state.view_calls.load(Ordering::SeqCst);
        let settle = || async {
            while !sched.members_refresh_idle() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        };
        let secs = std::time::Duration::from_secs;
        let t0 = std::time::Instant::now();
        assert!(sched.members(&state, t0).await.is_some());
        assert_eq!(calls(), 1);

        // Stale and failing: the old list is used, the refresh fails...
        state.view_fails.store(true, Ordering::SeqCst);
        let t1 = t0 + fetch::MEMBERS_TTL + secs(1);
        assert!(sched.members(&state, t1).await.is_some());
        settle().await;
        assert_eq!(calls(), 2);
        // ...and is not retried until its backoff passes.
        assert!(sched.members(&state, t1 + secs(10)).await.is_some());
        settle().await;
        assert_eq!(calls(), 2, "retried inside the backoff");
        let t2 = t1 + fetch::MEMBERS_RETRY_BASE + secs(1);
        assert!(sched.members(&state, t2).await.is_some());
        settle().await;
        assert_eq!(calls(), 3);
        // The second failure doubles the wait.
        sched
            .members(&state, t2 + fetch::MEMBERS_RETRY_BASE + secs(1))
            .await;
        settle().await;
        assert_eq!(calls(), 3, "the backoff doubles");

        // Too old to trust: no filter, rather than an old list.
        let t3 = t0 + fetch::MEMBERS_STALE_MAX + secs(1);
        assert!(sched.members(&state, t3).await.is_none());

        // Readable again: a fresh list once the backoff passes.
        state.view_fails.store(false, Ordering::SeqCst);
        let t4 = t3 + fetch::MEMBERS_RETRY_CAP + secs(1);
        let fresh = sched.members(&state, t4).await.unwrap();
        assert_eq!(*fresh, [1, 2].into_iter().collect::<HashSet<i32>>());
    }

    // Impact: re-review of #96 — window tasks waited for a rebuild slot
    // with no timeout while holding their window slot, so a few long
    // rebuilds could stall every blob in the window behind them.
    // Should: park the blob when no rebuild slot is free.
    // Should not: wait for one (the pull returns at once).
    #[tokio::test(flavor = "multi_thread")]
    async fn no_free_rebuild_slot_parks_instead_of_waiting() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-noslot-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, _outcome, manifest) = encoded_blob(&dir_src).await;
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        let sched = FetchScheduler::new(PullLimits::default());
        let _all = sched
            .rebuild
            .clone()
            .acquire_many_owned(PullLimits::default().rebuilds as u32)
            .await
            .unwrap();
        let pulled = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pull_with_sched(&seams(net.clone()), &dir_dst, &blob_id, &sched),
        )
        .await
        .expect("the pull must not wait for a rebuild slot");
        assert_eq!(pulled.0.rebuilt, 0);
        let now = std::time::Instant::now();
        assert!(sched.blob_parked(&blob_id, now));
        assert!(
            !sched.blob_parked(&blob_id, now + fetch::REBUILD_WAIT_PARK),
            "a short flat wait, not the unreachable backoff"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    /// Flush queued evidence the way the lane task does.
    async fn flush_items<X: TxSubmitter>(
        state: &PullNet,
        submitter: &X,
        items: Vec<evidence::EvidenceItem>,
    ) -> evidence::LaneFlush {
        let mut buf = evidence::LaneBuffer::default();
        for item in items {
            buf.push(item, 0);
        }
        evidence::flush(state, submitter, &mut buf).await
    }

    impl LocalStateSink for PullNet {
        async fn mark_local(&self, fragment_hash: Blake3Hash) {
            self.marked_local.lock().unwrap().push(fragment_hash);
        }
        async fn mark_remote_batch(&self, _fragment_hashes: Vec<Blake3Hash>) {}
    }

    fn seams(net: Arc<PullNet>) -> Seams<PullNet, PullNet, PullNet, PullNet> {
        Seams {
            transport: net.clone(),
            state: net.clone(),
            submitter: net.clone(),
            local_state: net,
        }
    }

    fn all_mine(desired: u64) -> PullTarget {
        PullTarget {
            placement_height: None,
            desired,
            assignment: vec![1; crate::rs::TOTAL_FRAGMENTS_PER_CHUNK],
        }
    }

    async fn encoded_blob(
        dir: &str,
    ) -> (BlobId, crate::api::PutOutcome, crate::store::BlobManifest) {
        let key: chacha20poly1305::Key = [0x55u8; 32].into();
        let blob_id = crate::CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a5").unwrap();
        let plaintext: Vec<u8> = (0..50_000u32).map(|i| (i % 233) as u8).collect();
        let outcome = crate::api::put(
            plaintext.as_slice(),
            plaintext.len(),
            blob_id.clone(),
            &key,
            dir,
        )
        .await
        .unwrap();
        let mut chunks: HashMap<u32, crate::store::ChunkFragmentMaps> = HashMap::new();
        for f in &outcome.fragments {
            let entry = chunks.entry(f.chunk_number).or_default();
            let bucket = if f.recovery {
                &mut entry.1
            } else {
                &mut entry.0
            };
            bucket.insert(
                f.local_index as usize,
                (f.fragment_hash, f.fragment_id.clone(), false),
            );
        }
        let manifest = crate::store::BlobManifest {
            blob_id: blob_id.clone(),
            integrity_hash: outcome.integrity_hash,
            added_bytes: outcome.added_bytes,
            file_size: plaintext.len() as u64,
            placement_height: None,
            chunks,
        };
        (blob_id, outcome, manifest)
    }

    fn idle_net() -> Arc<PullNet> {
        Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(None),
            target: None,
            ready: None,
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        })
    }

    // Impact: the planner paces itself on the queue depth, and consensus
    // apply hints the same blobs again; without coalescing the queue grows
    // with duplicates of work already waiting.
    // Should: queue a blob once however often it is offered, count it until
    // its check finishes, and accept it again after.
    #[tokio::test]
    async fn duplicate_kicks_are_coalesced() {
        let engine = EngineHandle::spawn(
            seams(idle_net()),
            EngineConfig {
                space: None,
                fragments_dir: String::new(),
                limits: PullLimits::default(),
            },
            tokio::runtime::Handle::current(),
        );
        let a = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        let b = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c902").unwrap();

        // The current-thread runtime runs no worker step until we await.
        assert!(engine.offer(a.clone()));
        assert!(!engine.offer(a.clone()), "already queued");
        engine.notify_blob_committed(a.clone());
        assert!(engine.offer(b.clone()));
        assert_eq!(engine.queued_len(), 2);

        // A caller waiting on a queued blob waits on that same check.
        let (tx, rx) = oneshot::channel();
        assert!(!engine.enqueue(a.clone(), Some(tx)), "waits, not re-queued");
        assert_eq!(engine.queued_len(), 2);
        rx.await.unwrap();
        engine.pull_blob(b).await.unwrap();
        assert_eq!(engine.queued_len(), 0);
        assert!(engine.offer(a), "re-offered after its check finished");
    }

    // Impact: review of #96 — a panicking pull task left its blob in the
    // queue forever, so it could never be offered again and the planner's
    // queue depth only grew.
    // Should: take the blob out of the queue when its check panics.
    #[tokio::test]
    async fn a_panicking_check_leaves_the_queue() {
        let queue: PullQueue = Default::default();
        let a = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        queue.lock().unwrap().insert(a.clone(), Vec::new());
        let entry = QueuedEntry {
            queue: queue.clone(),
            blob_id: Some(a.clone()),
        };
        let task = tokio::spawn(async move {
            let _entry = entry;
            panic!("pull check panicked");
        });
        assert!(task.await.is_err());
        assert!(queue.lock().unwrap().is_empty());
    }

    // Impact: re-review of #96 — waiters live in the shared queue map; if
    // the dispatch loop died, pull_blob callers hung and new requests
    // joined dead entries.
    // Should: drop every queued blob and wake every waiter with "engine
    // gone" when the dispatch loop ends.
    #[tokio::test]
    async fn a_dead_dispatch_loop_releases_its_waiters() {
        let queue: PullQueue = Default::default();
        let a = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        let (tx, rx) = oneshot::channel();
        queue.lock().unwrap().insert(a, vec![tx]);
        drop(DrainOnExit(queue.clone()));
        assert!(rx.await.is_err(), "the waiter is woken, not left hanging");
        assert!(queue.lock().unwrap().is_empty());
    }

    // Impact: re-review of #96 — a panic in a re-encode ran on the
    // dispatch loop itself and would have ended it, stranding every pull.
    // Should: keep serving pulls after a re-encode panics.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_reencode_does_not_stop_the_engine() {
        let net = idle_net();
        let seams = Seams {
            transport: net.clone(),
            state: Arc::new(HeldOnTwo {
                net: net.clone(),
                two_reachable: true,
                two_member: true,
                panic_manifest: true,
                view_delay: std::time::Duration::ZERO,
                held_on_three: HashSet::new(),
                view_fails: false.into(),
                view_calls: 0.into(),
            }),
            submitter: net.clone(),
            local_state: net.clone(),
        };
        let engine = EngineHandle::spawn(
            seams,
            EngineConfig {
                space: None,
                fragments_dir: String::new(),
                limits: PullLimits::default(),
            },
            tokio::runtime::Handle::current(),
        );
        let a = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        engine.enqueue_reencode(
            ReencodeCmd {
                blob_id: a.clone(),
                chunk_number: 0,
                missing_classes: vec![1],
            },
            true,
        );
        // No goal on record (target None): the pull returns before any
        // manifest read, so only the re-encode panics.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), engine.pull_blob(a))
            .await
            .expect("the engine still answers");
        assert_eq!(outcome, Some(PullOutcome::default()));
        // Should: release the chunk's pending mark even when its re-encode
        // panics, so a later tick can queue it again.
        assert_eq!(engine.urgent_reencodes_pending(), 0);
    }

    /// A handle with no dispatch loop: whatever is enqueued stays in the
    /// returned receivers.
    fn bare_handle() -> (
        EngineHandle,
        mpsc::UnboundedReceiver<ReencodeCmd>,
        mpsc::UnboundedReceiver<ReencodeCmd>,
    ) {
        let (pull_tx, _) = mpsc::unbounded_channel();
        let (reencode_urgent_tx, urgent_rx) = mpsc::unbounded_channel();
        let (reencode_lazy_tx, lazy_rx) = mpsc::unbounded_channel();
        let handle = EngineHandle {
            pull_tx,
            reencode_urgent_tx,
            reencode_lazy_tx,
            queued: Default::default(),
            urgent_pending: Default::default(),
            urgent_latest: Default::default(),
            sched: FetchScheduler::new(PullLimits::default()),
        };
        (handle, urgent_rx, lazy_rx)
    }

    fn reencode_cmd(blob: &BlobId, chunk_number: u32) -> ReencodeCmd {
        ReencodeCmd {
            blob_id: blob.clone(),
            chunk_number,
            missing_classes: vec![1],
        }
    }

    // Impact: the tick re-sends its whole urgent set every five minutes
    // into an unbounded channel; without dedup a slow backlog grew by a
    // full copy of itself per tick (2026.10.8 crossing).
    // Should: queue an urgent chunk once while it is still waiting, and a
    // different chunk of the same blob separately.
    // Should not: deduplicate lazy picks (one per tick, run in order).
    #[tokio::test]
    async fn an_urgent_reencode_already_pending_is_not_queued_twice() {
        let (engine, mut urgent_rx, mut lazy_rx) = bare_handle();
        let a = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        assert!(engine.enqueue_reencode(reencode_cmd(&a, 0), true));
        assert!(!engine.enqueue_reencode(reencode_cmd(&a, 0), true));
        assert!(engine.enqueue_reencode(reencode_cmd(&a, 1), true));
        assert_eq!(engine.urgent_reencodes_pending(), 2);
        assert_eq!(urgent_rx.recv().await.unwrap().chunk_number, 0);
        assert_eq!(urgent_rx.recv().await.unwrap().chunk_number, 1);
        assert!(urgent_rx.try_recv().is_err());

        assert!(engine.enqueue_reencode(reencode_cmd(&a, 0), false));
        assert!(engine.enqueue_reencode(reencode_cmd(&a, 0), false));
        assert!(lazy_rx.recv().await.is_some());
        assert!(lazy_rx.recv().await.is_some());
    }

    // Impact: 2026.10.8 crossing — urgent re-encodes queued while peers
    // rebooted kept running long after the next tick saw them back,
    // starving pulls for hours.
    // Should: drop, unrun, a queued urgent re-encode that the latest tick
    // no longer owes, and release its pending mark.
    // Should: run one the latest tick still owes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_queued_urgent_reencode_missing_from_the_latest_tick_is_dropped() {
        let base = std::env::temp_dir().join(format!("hopnet-stale-urgent-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, _outcome, manifest) = encoded_blob(&dir_src).await;
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        // A re-encode that runs finds no local shards and derives the
        // storage view to look for peers: `view_calls` says it ran.
        let state = Arc::new(HeldOnTwo {
            net: net.clone(),
            two_reachable: true,
            two_member: true,
            panic_manifest: false,
            view_delay: std::time::Duration::ZERO,
            held_on_three: HashSet::new(),
            view_fails: false.into(),
            view_calls: 0.into(),
        });
        let seams = Seams {
            transport: net.clone(),
            state: state.clone(),
            submitter: net.clone(),
            local_state: net.clone(),
        };
        let engine = EngineHandle::spawn(
            seams,
            EngineConfig {
                space: None,
                fragments_dir: dir_dst,
                limits: PullLimits::default(),
            },
            tokio::runtime::Handle::current(),
        );
        // A blob with no goal: its pull check answers without reading
        // anything, after the urgent branch has had its turn.
        let other = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c902").unwrap();

        engine.set_urgent_reencodes(HashMap::new());
        assert!(engine.enqueue_reencode(reencode_cmd(&blob_id, 0), true));
        engine.pull_blob(other.clone()).await.unwrap();
        assert_eq!(
            state.view_calls.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(engine.urgent_reencodes_pending(), 0);

        engine.set_urgent_reencodes(HashMap::from([((blob_id.clone(), 0), vec![1])]));
        assert!(engine.enqueue_reencode(reencode_cmd(&blob_id, 0), true));
        engine.pull_blob(other).await.unwrap();
        assert!(state.view_calls.load(std::sync::atomic::Ordering::SeqCst) > 0);
        let _ = std::fs::remove_dir_all(&base);
    }

    // Impact: review of #99 — the stale-command filter keyed only on the
    // chunk, so a command queued when a tick owed classes [1, 2] still
    // rebuilt class 1 after the next tick owed only [2].
    // Should: run a queued urgent command with the classes the latest tick
    // owes for its chunk, not those it was queued with.
    // Should not: run it when the latest tick owes the chunk nothing, or
    // before that, filter anything until a tick has published.
    #[test]
    fn a_queued_urgent_reencode_runs_only_the_classes_the_latest_tick_owes() {
        let a = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        let queued = || ReencodeCmd {
            blob_id: a.clone(),
            chunk_number: 0,
            missing_classes: vec![1, 2],
        };
        let latest: UrgentLatest = Default::default();
        assert_eq!(
            still_owed(&latest, queued()).unwrap().missing_classes,
            vec![1, 2]
        );

        *latest.lock().unwrap() = Some(HashMap::from([((a.clone(), 0), vec![2])]));
        assert_eq!(
            still_owed(&latest, queued()).unwrap().missing_classes,
            vec![2]
        );

        *latest.lock().unwrap() = Some(HashMap::from([((a.clone(), 1), vec![2])]));
        assert!(still_owed(&latest, queued()).is_none());
    }

    // Impact: review of #96 — park entries for blobs that stopped being
    // owed were never removed, so the park book grew without bound.
    // Should: drop a blob's park entry once a check finds it owes nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_blob_owing_nothing_leaves_the_park_book() {
        let base = std::env::temp_dir().join(format!("hopnet-unpark-{}", std::process::id()));
        let dir = base.join("held").to_str().unwrap().to_string();
        let (blob_id, _outcome, mut manifest) = encoded_blob(&dir).await;
        for (originals, recovery) in manifest.chunks.values_mut() {
            for entry in originals.values_mut().chain(recovery.values_mut()) {
                entry.2 = true;
            }
        }
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            target: Some(PullTarget {
                placement_height: Some(5),
                desired: 9,
                assignment: vec![1; crate::rs::TOTAL_FRAGMENTS_PER_CHUNK],
            }),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        let sched = FetchScheduler::new(PullLimits::default());
        sched.park_blob(&blob_id, std::time::Instant::now());
        let (result, _) = pull_with_sched(&seams(net), &dir, &blob_id, &sched).await;
        assert_eq!(result.owed, 0);
        assert_eq!(sched.stats(std::time::Instant::now()).parked_blobs, 0);
        let _ = std::fs::remove_dir_all(&base);
    }

    // Should: pull every class this node owes under the goal from a
    // serving holder, settle each through the (awaited) sink, and queue the
    // blob's evidence for the lane, which then reports belief, attests and
    // proposes confirmation once the goal's evidence is complete.
    // Should not: submit anything to consensus from the pull itself;
    // propose confirmation when the evidence is incomplete; pull anything
    // for a blob whose goal assigns this node nothing.
    // Impact: this is the whole distribution path now — a missed duty
    // strands a class on the origin; a premature confirm lapses obligations
    // against holders that do not exist; a consensus wait on the worker
    // capped production at ~6 blobs a minute.
    #[tokio::test(flavor = "multi_thread")]
    async fn pulls_owed_classes_attests_and_confirms() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-test-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, outcome, manifest) = encoded_blob(&dir_src).await;

        let net = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest.clone())),
            target: Some(all_mine(9)),
            ready: Some(9),
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        });
        for f in &outcome.fragments {
            let data = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
            net.served.lock().unwrap().insert(f.fragment_hash, data);
        }

        let (result, items) = pull_with_lane(&seams(net.clone()), &dir_dst, &blob_id).await;
        assert_eq!(result.owed, 30);
        assert_eq!(result.pulled, 30);
        assert_eq!(result.rebuilt, 0);
        assert_eq!(result.failed, 0);
        assert!(result.evidence_queued);
        assert_eq!(net.marked_local.lock().unwrap().len(), 30);
        assert!(
            net.submitted.lock().unwrap().is_empty(),
            "the pull awaits no consensus round"
        );
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].height, 9, "read before the rehash");
        assert_eq!(items[0].present.len(), 30);

        let flushed = flush_items(&net, net.as_ref(), items).await;
        assert_eq!(flushed.confirmed, 1);
        assert_eq!(
            *net.submitted.lock().unwrap(),
            vec![policy::SELF_CHECK_FN, policy::ATTEST_FN, CONFIRM_TX_FN],
            "belief, then truth, then the confirm proposal"
        );
        let believed = self_check_hashes(&net.payloads.lock().unwrap());
        let landed: std::collections::HashSet<Blake3Hash> =
            outcome.fragments.iter().map(|f| f.fragment_hash).collect();
        assert_eq!(
            believed, landed,
            "the prompt belief carries exactly the classes this pull landed"
        );
        for f in &outcome.fragments {
            assert!(fragstore::fragment_exists_and_valid(
                &dir_dst,
                &f.fragment_hash
            ));
        }

        // Evidence incomplete: no confirm proposal.
        let dir_dst2 = base.join("dst2").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst2).unwrap();
        let net2 = Arc::new(PullNet {
            served: Mutex::new(net.served.lock().unwrap().clone()),
            manifest: Mutex::new(Some(manifest.clone())),
            target: Some(all_mine(9)),
            ready: None,
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        });
        let (result, items) = pull_with_lane(&seams(net2.clone()), &dir_dst2, &blob_id).await;
        assert_eq!(result.pulled, 30);
        let flushed = flush_items(&net2, net2.as_ref(), items).await;
        assert_eq!(flushed.confirmed, 0);
        assert_eq!(
            *net2.submitted.lock().unwrap(),
            vec![policy::SELF_CHECK_FN, policy::ATTEST_FN]
        );

        // Nothing assigned to me: nothing owed, nothing pulled.
        let net3 = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest.clone())),
            target: Some(PullTarget {
                placement_height: None,
                desired: 9,
                assignment: vec![2; crate::rs::TOTAL_FRAGMENTS_PER_CHUNK],
            }),
            ready: None,
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        });
        let (result, items) = pull_with_lane(&seams(net3), &dir_dst2, &blob_id).await;
        assert_eq!(result, PullOutcome::default());
        assert!(items.is_empty());

        let _ = std::fs::remove_dir_all(&base);
    }

    // Should: submit nothing for a re-goaled blob whose classes this node
    // already holds — no self-check, no attestation, no confirm proposal;
    // the sweep owns its rows and the fulfillment floor its confirmation.
    // Should: still attest and propose for a never-confirmed blob this
    // node holds (the origin at birth), so uploads confirm promptly, and
    // assert belief for its classes when consensus has no rows for them (a
    // photos origin, a failed upload attestation).
    // Should not: buy a self-check round at birth when the rows already
    // exist.
    // Impact: the serial worker awaited a consensus round per rubber stamp
    // after every view transition (~30 confirms/min); the 500-blob cutover
    // rehearsal never converged. Only births and moved bytes buy a round.
    #[tokio::test(flavor = "multi_thread")]
    async fn regoal_with_nothing_owed_submits_nothing() {
        let base = std::env::temp_dir().join(format!("hopnet-regoal-test-{}", std::process::id()));
        let dir = base.join("held").to_str().unwrap().to_string();
        let (blob_id, _outcome, mut manifest) = encoded_blob(&dir).await;
        for (originals, recovery) in manifest.chunks.values_mut() {
            for entry in originals.values_mut().chain(recovery.values_mut()) {
                entry.2 = true; // every class already on this disk
            }
        }

        // Re-goal: confirmed at 5, declared to 9, everything still mine.
        let regoal = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest.clone())),
            target: Some(PullTarget {
                placement_height: Some(5),
                desired: 9,
                assignment: vec![1; crate::rs::TOTAL_FRAGMENTS_PER_CHUNK],
            }),
            ready: Some(9),
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        });
        let (result, items) = pull_with_lane(&seams(regoal.clone()), &dir, &blob_id).await;
        assert_eq!(result, PullOutcome::default(), "nothing owed, nothing done");
        assert!(
            items.is_empty() && regoal.submitted.lock().unwrap().is_empty(),
            "a rubber stamp queues no evidence"
        );

        // Birth: never confirmed, held here, no belief rows yet — the
        // origin's prompt evidence asserts its classes first.
        let all_hashes: std::collections::HashSet<Blake3Hash> = manifest
            .chunks
            .values()
            .flat_map(|(o, r)| o.values().chain(r.values()))
            .map(|(hash, _, _)| *hash)
            .collect();
        let birth = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest.clone())),
            target: Some(all_mine(9)),
            ready: Some(9),
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        });
        let (result, items) = pull_with_lane(&seams(birth.clone()), &dir, &blob_id).await;
        assert_eq!(result.owed, 0);
        assert!(result.evidence_queued);
        let flushed = flush_items(&birth, birth.as_ref(), items).await;
        assert_eq!(flushed.confirmed, 1);
        assert_eq!(
            *birth.submitted.lock().unwrap(),
            vec![policy::SELF_CHECK_FN, policy::ATTEST_FN, CONFIRM_TX_FN]
        );
        assert_eq!(
            self_check_hashes(&birth.payloads.lock().unwrap()),
            all_hashes,
            "every held class without a row is asserted"
        );

        // Birth with belief already on record (the upload attestation
        // landed): no self-check round, disk truth and the proposal only.
        let believed = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ready: Some(9),
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(all_hashes),
            payloads: Mutex::new(Vec::new()),
        });
        let (result, items) = pull_with_lane(&seams(believed.clone()), &dir, &blob_id).await;
        assert!(result.evidence_queued);
        flush_items(&believed, believed.as_ref(), items).await;
        assert_eq!(
            *believed.submitted.lock().unwrap(),
            vec![policy::ATTEST_FN, CONFIRM_TX_FN]
        );

        let _ = std::fs::remove_dir_all(&base);
    }

    // Should: rebuild the classes no holder serves from any K live classes
    // (recovery is the fetch fallback), and count as failed only what
    // neither path could produce.
    // Impact: steady-state loss and first distribution share one
    // primitive; a class with no serving holder must still converge.
    #[tokio::test(flavor = "multi_thread")]
    async fn rebuilds_unserved_classes_from_k_live() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-rebuild-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, outcome, manifest) = encoded_blob(&dir_src).await;

        // The peer serves only the K originals; the 20 recovery classes
        // must be rebuilt locally from those.
        let net = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest.clone())),
            target: Some(all_mine(9)),
            ready: None,
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        });
        for f in outcome.fragments.iter().filter(|f| !f.recovery) {
            let data = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
            net.served.lock().unwrap().insert(f.fragment_hash, data);
        }
        let (result, _) = pull_with_lane(&seams(net.clone()), &dir_dst, &blob_id).await;
        assert_eq!(result.owed, 30);
        assert_eq!(result.pulled, 10);
        assert_eq!(result.rebuilt, 20);
        assert_eq!(result.failed, 0);
        for f in &outcome.fragments {
            assert!(fragstore::fragment_exists_and_valid(
                &dir_dst,
                &f.fragment_hash
            ));
        }

        // Peer serves nothing at all: below K, nothing can be rebuilt.
        let dir_dst2 = base.join("dst2").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst2).unwrap();
        let dead = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ready: None,
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        });
        let (result, items) = pull_with_lane(&seams(dead.clone()), &dir_dst2, &blob_id).await;
        assert_eq!(result.failed, 30);
        assert!(items.is_empty(), "nothing to attest");

        let _ = std::fs::remove_dir_all(&base);
    }

    // Should: still attest disk truth and propose confirmation when the
    // lane's belief page is refused — belief is the sweep's to repair, the
    // evidence the confirmation needs must not wait on it.
    // Should not: fail the pull or skip the attestation on that refusal.
    #[tokio::test(flavor = "multi_thread")]
    async fn prompt_self_check_rejection_does_not_block_attestation() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-reject-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, outcome, manifest) = encoded_blob(&dir_src).await;

        let net = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ready: Some(9),
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
            inventoried: Mutex::new(Default::default()),
            payloads: Mutex::new(Vec::new()),
        });
        for f in &outcome.fragments {
            let data = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
            net.served.lock().unwrap().insert(f.fragment_hash, data);
        }
        let seams = Seams {
            transport: net.clone(),
            state: net.clone(),
            submitter: Arc::new(RejectSelfCheck(net.clone())),
            local_state: net.clone(),
        };

        let (result, items) = pull_with_lane(&seams, &dir_dst, &blob_id).await;
        assert_eq!(result.pulled, 30);
        let flushed = flush_items(&net, seams.submitter.as_ref(), items).await;
        assert_eq!(
            (
                flushed.belief_failed,
                flushed.truth_pages,
                flushed.confirmed
            ),
            (1, 1, 1)
        );
        assert_eq!(
            *net.submitted.lock().unwrap(),
            vec![policy::ATTEST_FN, CONFIRM_TX_FN],
            "the refused belief is skipped, disk truth and the proposal still go out"
        );

        let _ = std::fs::remove_dir_all(&base);
    }

    fn item(n: u8, height: u64, hashes: &[u8]) -> evidence::EvidenceItem {
        evidence::EvidenceItem {
            blob_id: BlobId::from_str(&format!("01890a5d-ac96-774b-b9aa-9f8b24f0c9{n:02x}"))
                .unwrap(),
            height,
            present: hashes
                .iter()
                .map(|b| Blake3Hash::from_bytes([*b; 32]))
                .collect(),
        }
    }

    // Impact: a page stamped with a later height than one of its hashes was
    // seen at overstates freshness for the confirmation's recency check.
    // Should: stamp an attestation page with the lowest height among the
    // pulls whose hashes it carries.
    #[tokio::test]
    async fn page_height_is_the_lowest_observation() {
        let net = idle_net();
        let flushed = flush_items(
            &net,
            net.as_ref(),
            vec![item(1, 12, &[1]), item(2, 7, &[2])],
        )
        .await;
        assert_eq!(flushed.truth_pages, 1);
        let payloads = net.payloads.lock().unwrap();
        let (_, bytes) = payloads
            .iter()
            .find(|(f, _)| *f == policy::ATTEST_FN)
            .unwrap();
        let (attestation, _): (crate::types::FragmentAttestation, _) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard()).unwrap();
        assert_eq!(attestation.height, 7);
        assert_eq!(attestation.present.len(), 2);
    }

    // Impact: one consensus round per confirmed blob was the old worker's
    // ceiling; the lane batches every ready blob it attested.
    // Should: propose one ConfirmPlacement carrying every buffered blob
    // whose evidence is complete.
    // Should not: queue the same blob twice when two pulls report it.
    #[tokio::test]
    async fn confirm_proposed_for_every_ready_buffered_blob_in_one_tx() {
        let net = Arc::new(PullNet {
            ready: Some(9),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        let flushed = flush_items(
            &net,
            net.as_ref(),
            vec![item(1, 9, &[1]), item(2, 9, &[2]), item(1, 9, &[3])],
        )
        .await;
        assert_eq!(flushed.confirmed, 2);
        let payloads = net.payloads.lock().unwrap();
        let confirms: Vec<_> = payloads
            .iter()
            .filter(|(f, _)| *f == CONFIRM_TX_FN)
            .collect();
        assert_eq!(confirms.len(), 1, "one transaction");
        let (payload, _): (ConfirmPlacement, _) =
            bincode::serde::decode_from_slice(&confirms[0].1, bincode::config::standard()).unwrap();
        assert_eq!(payload.confirmations.len(), 2);
    }

    // Impact: the lane's first cut waited out the full minute for a lone
    // upload, and placement tests that allow ~30 s failed.
    // Should: flush once the input has been quiet for the full gap, flush a
    // steady stream once its oldest entry is a minute old, and flush at
    // once when a full page is waiting.
    // Should not: flush while items keep arriving inside the quiet gap, or
    // early because the push landed late in a whole second (review of #96:
    // second-granular stamps could fire after ~1 s).
    #[test]
    fn lane_flushes_at_size_quiet_or_age() {
        let t0 = 100_999; // late in second 100
        let mut buf = evidence::LaneBuffer::default();
        assert!(!buf.due(t0));
        buf.push(item(1, 9, &[1]), t0);
        assert!(!buf.due(t0));
        assert!(
            !buf.due(t0 + 1_001),
            "one second boundary later is not quiet"
        );
        assert!(!buf.due(t0 + policy::EVIDENCE_QUIET_MS - 1));
        assert!(buf.due(t0 + policy::EVIDENCE_QUIET_MS), "quiet");

        // A steady stream keeps it open until the age bound.
        let mut stream = evidence::LaneBuffer::default();
        let mut t = t0;
        while t < t0 + policy::EVIDENCE_MAX_AGE_SECS * 1000 {
            stream.push(item(2, 9, &[2]), t);
            assert!(!stream.due(t), "still streaming at {t}");
            t += 500;
        }
        stream.push(item(2, 9, &[2]), t);
        assert!(stream.due(t), "age bound");

        let mut full = evidence::LaneBuffer::default();
        full.push(
            evidence::EvidenceItem {
                present: vec![Blake3Hash::from_bytes([7; 32]); policy::ATTEST_PAGE_SIZE],
                ..item(2, 9, &[])
            },
            t0,
        );
        assert!(full.due(t0));
    }

    /// Bytes of every file under `dir` (a fake volume's used space).
    fn dir_bytes(dir: &std::path::Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .flatten()
            .map(|e| match e.metadata() {
                Ok(m) if m.is_dir() => dir_bytes(&e.path()),
                Ok(m) => m.len(),
                Err(_) => 0,
            })
            .sum()
    }

    /// A pull floor of `floor` bytes (no share) with `gap` to resume, over
    /// a fake volume: `budget` bytes free minus what is written under the
    /// probed directory, plus whatever `extra` holds.
    fn space_over(
        budget: u64,
        floor: u64,
        gap: u64,
        extra: Arc<std::sync::atomic::AtomicU64>,
    ) -> Arc<crate::admission::SpaceGuard> {
        crate::admission::SpaceGuard::new(
            crate::admission::PullFloor {
                min_free_bytes: floor,
                min_free_basis_points: 0,
                resume_gap_bytes: Some(gap),
            },
            Some(1),
            Box::new(move |dir| {
                let free = (budget + extra.load(std::sync::atomic::Ordering::Acquire))
                    .saturating_sub(dir_bytes(dir));
                Ok((free, 1 << 40))
            }),
            Box::leak(Box::new(std::sync::atomic::AtomicU64::new(0))),
        )
    }

    // Impact: the 2026-10-03 macbook incident — pulls at ~30 fetches/s took
    // its volume from 2 GB free to 116 MB in a minute; and a store that
    // failed for space was read as the holder not serving the class, which
    // rebuilt it (fetching K shards to write more).
    // Should: pull only what fits above the floor and hold the rest for
    // space, pausing the guard.
    // Should not: rebuild, fail, park the blob or strike the peer for the
    // classes held for space.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_node_never_fills_its_disk_by_pulling() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-floor-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();
        let (blob_id, outcome, manifest) = encoded_blob(&dir_src).await;
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        let mut one_file = 0;
        for f in &outcome.fragments {
            let bytes = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
            one_file = admission::fragment_file_bytes(bytes.len());
            net.served.lock().unwrap().insert(f.fragment_hash, bytes);
        }
        // Room for about eight of the thirty classes above the floor.
        let floor = 1 << 20;
        let space = space_over(floor + 8 * one_file, floor, 1 << 20, Arc::default());
        let sched = FetchScheduler::with_space(PullLimits::default(), space.clone());
        let (result, _) = pull_with_sched(&seams(net), &dir_dst, &blob_id, &sched).await;

        assert_eq!(result.owed, 30);
        assert!(result.pulled > 0 && result.pulled < 30, "{result:?}");
        assert_eq!(result.held_for_space, 30 - result.pulled);
        assert_eq!((result.rebuilt, result.failed), (0, 0));
        assert!(space.paused());
        let now = std::time::Instant::now();
        assert!(!sched.blob_parked(&blob_id, now));
        assert!(!sched.peer_parked(2, now));
        // Reservations count against the floor, so nothing overshot it.
        let written = dir_bytes(std::path::Path::new(&dir_dst));
        assert!(written <= 8 * one_file, "{written} written");
        let _ = std::fs::remove_dir_all(&base);
    }

    // Impact: a node below the floor that kept dequeuing would fail every
    // owed blob, churn parks and reorder the at-risk-first queue.
    // Should: take nothing off the pull queue while held back for space.
    // Should: take it again once a re-probe sees the resume mark.
    #[tokio::test(start_paused = true)]
    async fn a_paused_engine_takes_no_pulls_and_resumes_at_the_mark() {
        let base = std::env::temp_dir().join(format!("hopnet-pull-pause-{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        let extra = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let space = space_over(10, 100, 50, extra.clone());
        assert!(space
            .reserve(base.to_str().unwrap(), 1, WriteClass::Pull)
            .is_err());
        assert!(space.paused());
        let engine = EngineHandle::spawn(
            seams(idle_net()),
            EngineConfig {
                space: Some(space.clone()),
                fragments_dir: base.to_str().unwrap().to_string(),
                limits: PullLimits::default(),
            },
            tokio::runtime::Handle::current(),
        );
        let a = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        assert!(engine.offer(a));
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        assert_eq!(engine.queued_len(), 1, "held while paused");

        extra.store(1000, std::sync::atomic::Ordering::Release);
        tokio::time::sleep(SPACE_REPROBE + std::time::Duration::from_secs(1)).await;
        assert!(!space.paused());
        assert_eq!(engine.queued_len(), 0, "taken after the resume");
        let _ = std::fs::remove_dir_all(&base);
    }

    // Should: hold a lazy re-encode for space without fetching a shard.
    // Should: let an urgent re-encode write into the repair reserve while
    // the guard is paused.
    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_reencode_waits_urgent_reencode_runs_while_paused() {
        let base = std::env::temp_dir().join(format!("hopnet-reenc-floor-{}", std::process::id()));
        let dir = base.to_str().unwrap().to_string();
        let (blob_id, outcome, mut manifest) = encoded_blob(&dir).await;
        // Two classes lost here; the other 28 on disk.
        let lost: Vec<u32> = vec![3, 17];
        let chunk = manifest.chunks.get_mut(&0).unwrap();
        for map in [&mut chunk.0, &mut chunk.1] {
            for (idx, entry) in map.iter_mut() {
                entry.2 = !lost.contains(&(*idx as u32));
            }
        }
        for f in &outcome.fragments {
            if lost.contains(&f.local_index) {
                fragstore::delete_fragment(&dir, &f.fragment_hash).unwrap();
            }
        }
        let net = Arc::new(PullNet {
            manifest: Mutex::new(Some(manifest)),
            ..Arc::try_unwrap(idle_net()).ok().unwrap()
        });
        // Free space sits between the repair floor (1 byte) and the pull
        // floor, and the guard is paused.
        let space = space_over(1 << 30, 1 << 40, 0, Arc::default());
        assert!(space.reserve(&dir, 1, WriteClass::Pull).is_err());

        // Every shard the rebuild needs is local: nothing is fetched.
        let fetch = |_, _, _| std::future::ready(None);
        let lazy = reencode::reencode_chunk_via(
            net.as_ref(),
            net.as_ref(),
            &dir,
            &blob_id,
            0,
            &lost,
            (&space, WriteClass::Pull),
            fetch,
        )
        .await;
        assert!(matches!(lazy, Err(EngineError::NoSpace)), "{lazy:?}");

        let urgent = reencode::reencode_chunk_via(
            net.as_ref(),
            net.as_ref(),
            &dir,
            &blob_id,
            0,
            &lost,
            (&space, WriteClass::Repair),
            fetch,
        )
        .await
        .unwrap();
        assert_eq!(urgent.regenerated, 2);
        let _ = std::fs::remove_dir_all(&base);
    }
}
