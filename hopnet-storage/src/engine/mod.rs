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

use crate::error::StorageError;
use crate::fragstore;
use crate::traits::{LocalStateSink, StateReader, Transport, TxSubmitter};
use crate::types::BlobId;
use fetch::{FetchMiss, FetchScheduler, PullLimits};
use hopnet_common::Blake3Hash;
use std::collections::{BTreeMap, HashSet};
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
}

#[derive(Debug)]
pub enum EngineError {
    /// Seam/state failure (DB checkout, query).
    State(StorageError),
    /// A fragment could not be sourced or stored.
    Transfer(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::State(e) => write!(f, "state error: {}", e),
            EngineError::Transfer(m) => write!(f, "fragment transfer error: {}", m),
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
    /// This blob's belief, disk truth and confirmation check went to the
    /// evidence lane (births and moved bytes only).
    pub evidence_queued: bool,
}

/// Aggregate of many pull checks (the tick's re-kick, the operator drain).
#[derive(Debug, Default, Clone, Copy)]
pub struct PullStats {
    pub checked: usize,
    pub owed: usize,
    pub pulled: usize,
    pub rebuilt: usize,
    pub failed: usize,
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

type PullRequest = (BlobId, Option<oneshot::Sender<PullOutcome>>);

/// Handle to the running engine. Cheap to clone; the host stores one in its
/// app state (mirrors the consensus EngineHandle pattern).
#[derive(Clone)]
pub struct EngineHandle {
    pull_tx: mpsc::UnboundedSender<PullRequest>,
    reencode_urgent_tx: mpsc::UnboundedSender<ReencodeCmd>,
    reencode_lazy_tx: mpsc::UnboundedSender<ReencodeCmd>,
    /// Blobs waiting for (or in) a pull check: hints for a blob already
    /// queued are dropped, and the planner paces itself on the count.
    queued: Arc<std::sync::Mutex<HashSet<BlobId>>>,
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
        if !self.queued.lock().unwrap().insert(blob_id.clone()) {
            return false;
        }
        if self.pull_tx.send((blob_id.clone(), None)).is_err() {
            self.queued.lock().unwrap().remove(&blob_id);
            return false;
        }
        true
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

    /// Pull check for one blob, run in the window; resolves when it
    /// completes. `None` = engine gone.
    pub async fn pull_blob(&self, blob_id: BlobId) -> Option<PullOutcome> {
        let (tx, rx) = oneshot::channel();
        // Always queued (the caller waits on this exact check); counted so
        // the planner's pacing sees operator work too.
        self.queued.lock().unwrap().insert(blob_id.clone());
        self.pull_tx.send((blob_id, Some(tx))).ok()?;
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
            self.queued.lock().unwrap().insert(blob_id.clone());
            if self.pull_tx.send((blob_id.clone(), Some(tx))).is_err() {
                stats.checked += 1;
                stats.failed += 1;
                continue;
            }
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
    /// items drain one at a time behind everything else.
    pub fn enqueue_reencode(&self, cmd: ReencodeCmd, urgent: bool) {
        let tx = if urgent {
            &self.reencode_urgent_tx
        } else {
            &self.reencode_lazy_tx
        };
        if tx.send(cmd).is_err() {
            tracing::error!("re-encode: engine gone — command dropped");
        }
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
        let queued: Arc<std::sync::Mutex<HashSet<BlobId>>> = Default::default();
        let worker_queued = queued.clone();
        let sched = FetchScheduler::new(config.limits);
        let worker_sched = sched.clone();
        let lane =
            evidence::EvidenceLane::spawn(seams.state.clone(), seams.submitter.clone(), &data_rt);
        data_rt.spawn(async move {
            let mut running: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    cmd = reencode_urgent_rx.recv() => {
                        let Some(cmd) = cmd else { break };
                        run_reencode_cmd(&seams, &fragments_dir, cmd).await;
                    }
                    Some(_) = running.join_next(), if !running.is_empty() => {}
                    admitted = async {
                        let permit = worker_sched.window.clone().acquire_owned().await;
                        (permit, pull_rx.recv().await)
                    } => {
                        let (Ok(permit), Some((blob_id, reply))) = admitted else { break };
                        if worker_sched.blob_parked(&blob_id, std::time::Instant::now()) {
                            // Its sources are dark: let the window move on.
                            worker_queued.lock().unwrap().remove(&blob_id);
                            if let Some(reply) = reply {
                                let _ = reply.send(PullOutcome::default());
                            }
                            continue;
                        }
                        let (seams, dir, lane, sched, queued) = (
                            seams.clone(),
                            fragments_dir.clone(),
                            lane.clone(),
                            worker_sched.clone(),
                            worker_queued.clone(),
                        );
                        running.spawn(async move {
                            let outcome = match pull_owed(&seams, &dir, &blob_id, &lane, &sched).await {
                                Ok(o) => o,
                                Err(e) => {
                                    tracing::warn!("pull: blob {blob_id} failed: {e}");
                                    PullOutcome::default()
                                }
                            };
                            drop(permit);
                            queued.lock().unwrap().remove(&blob_id);
                            if let Some(reply) = reply {
                                let _ = reply.send(outcome);
                            }
                        });
                    }
                    cmd = reencode_lazy_rx.recv() => {
                        let Some(cmd) = cmd else { break };
                        run_reencode_cmd(&seams, &fragments_dir, cmd).await;
                    }
                }
            }
        });

        EngineHandle {
            pull_tx,
            reencode_urgent_tx,
            reencode_lazy_tx,
            queued,
            sched,
        }
    }
}

/// Run one re-encode command on the serial worker (errors logged, not
/// propagated — the next tick's scan re-elects and retries).
async fn run_reencode_cmd<T, S, X, L>(
    seams: &Seams<T, S, X, L>,
    fragments_dir: &str,
    cmd: ReencodeCmd,
) where
    T: Transport + 'static,
    S: StateReader,
    X: TxSubmitter,
    L: LocalStateSink,
{
    if let Err(e) = reencode::reencode_chunk(
        &seams.transport,
        seams.state.as_ref(),
        seams.local_state.as_ref(),
        fragments_dir,
        &cmd.blob_id,
        cmd.chunk_number,
        &cmd.missing_classes,
    )
    .await
    {
        tracing::warn!(
            "re-encode: blob {} chunk {} failed: {e}",
            cmd.blob_id,
            cmd.chunk_number
        );
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
    S: StateReader,
    X: TxSubmitter,
    L: LocalStateSink + 'static,
{
    let mut outcome = PullOutcome::default();
    let Some(target) = seams.state.pull_target(blob_id)? else {
        // Unknown blob (raced a delete), or the record does not reach the
        // goal yet: nothing is owed until it does.
        return Ok(outcome);
    };
    let Some(manifest) = seams.state.blob_manifest(blob_id)? else {
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

        // Every owed class at once, at most `per_blob` in flight, each under
        // the scheduler's global and per-peer caps.
        let per_blob = Arc::new(tokio::sync::Semaphore::new(sched.limits.per_blob()));
        let mut fetches: tokio::task::JoinSet<(u32, u32, Result<(), FetchMiss>)> =
            tokio::task::JoinSet::new();
        for (chunk, classes) in &owed {
            for (class, hash) in classes {
                let known = sources.remove(hash).unwrap_or_default();
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
                    let data =
                        match fetch::fetch_class(&transport, &sched, &hash, &attested, &others)
                            .await
                        {
                            Ok(data) => data,
                            Err(miss) => return (chunk, class, Err(miss)),
                        };
                    let stored = tokio::task::spawn_blocking(move || {
                        fragstore::store_fragment(&dir, &hash, data)
                    })
                    .await;
                    match stored {
                        Ok(Ok(())) => {
                            local_state.mark_local(hash).await;
                            (chunk, class, Ok(()))
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
        let mut unserved_by_chunk: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        let mut unreachable = false;
        while let Some(joined) = fetches.join_next().await {
            match joined {
                Ok((_, _, Ok(()))) => outcome.pulled += 1,
                Ok((chunk, class, Err(miss))) => {
                    unreachable |= miss == FetchMiss::Unreachable;
                    unserved_by_chunk.entry(chunk).or_default().push(class);
                }
                Err(e) => {
                    tracing::warn!("pull: blob {blob_id}: fetch task failed: {e}");
                    outcome.failed += 1;
                }
            }
        }

        let mut rebuild_failed = false;
        for (chunk, mut unserved) in unserved_by_chunk {
            unserved.sort_unstable();
            let chunk = &chunk;
            // Recovery is the fetch fallback: rebuild the classes nobody
            // served from any K live classes (local shards first).
            match reencode::reencode_chunk(
                &seams.transport,
                seams.state.as_ref(),
                seams.local_state.as_ref(),
                fragments_dir,
                blob_id,
                *chunk,
                &unserved,
            )
            .await
            {
                Ok(r) => {
                    outcome.rebuilt += r.regenerated;
                    outcome.failed += unserved.len().saturating_sub(r.regenerated);
                }
                Err(e) => {
                    tracing::warn!(
                        "pull: blob {blob_id} chunk {chunk}: {} classes unsourceable and rebuild failed: {e}",
                        unserved.len()
                    );
                    outcome.failed += unserved.len();
                    rebuild_failed = true;
                }
            }
        }

        // A class whose known holders are all dark, and no rebuild: park the
        // blob so the window admits blobs that can move. Progress (or a
        // blob owing nothing more) clears the park.
        let now = std::time::Instant::now();
        if unreachable && rebuild_failed && outcome.pulled + outcome.rebuilt == 0 {
            sched.park_blob(blob_id, now);
            tracing::debug!("pull: blob {blob_id} parked: its sources are unreachable");
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
            "pull: blob {} owed {} classes — pulled {}, rebuilt {}, failed {}{}",
            blob_id,
            outcome.owed,
            outcome.pulled,
            outcome.rebuilt,
            outcome.failed,
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
        S: StateReader,
        X: TxSubmitter,
        L: LocalStateSink + 'static,
    {
        let (lane, mut rx) = evidence::EvidenceLane::channel();
        let sched = FetchScheduler::new(PullLimits::default());
        let outcome = pull_owed(seams, dir, blob_id, &lane, &sched).await.unwrap();
        let mut items = Vec::new();
        while let Ok(item) = rx.try_recv() {
            items.push(item);
        }
        (outcome, items)
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

        // A reply-carrying check queues behind them; once it resolves the
        // worker has drained everything ahead of it.
        let c = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c903").unwrap();
        engine.pull_blob(c).await.unwrap();
        assert_eq!(engine.queued_len(), 0);
        assert!(engine.offer(a), "re-offered after its check finished");
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
    // Should: flush once the input has been quiet for a moment, flush a
    // steady stream once its oldest entry is a minute old, and flush at
    // once when a full page is waiting.
    // Should not: flush while items keep arriving inside the quiet gap.
    #[test]
    fn lane_flushes_at_size_quiet_or_age() {
        let mut buf = evidence::LaneBuffer::default();
        assert!(!buf.due(100));
        buf.push(item(1, 9, &[1]), 100);
        assert!(!buf.due(100));
        assert!(buf.due(100 + policy::EVIDENCE_QUIET_SECS), "quiet");

        // A steady stream keeps it open until the age bound.
        let mut stream = evidence::LaneBuffer::default();
        let mut t = 100;
        while t < 100 + policy::EVIDENCE_MAX_AGE_SECS {
            stream.push(item(2, 9, &[2]), t);
            assert!(!stream.due(t), "still streaming at {t}");
            t += 1;
        }
        stream.push(item(2, 9, &[2]), t);
        assert!(stream.due(t), "age bound");

        let mut full = evidence::LaneBuffer::default();
        full.push(
            evidence::EvidenceItem {
                present: vec![Blake3Hash::from_bytes([7; 32]); policy::ATTEST_PAGE_SIZE],
                ..item(2, 9, &[])
            },
            100,
        );
        assert!(full.due(100));
    }
}
