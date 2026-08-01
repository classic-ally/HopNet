//! The distribution engine (RFC-014): event-driven fragment placement.
//!
//! Blob ids arrive from the host's consensus apply (`on_decided` →
//! `EngineHandle::notify_blob_committed`, non-blocking). A global worker
//! pool drains them: the origin node (the one holding the full local
//! fragment set) pushes each fragment to its placement-selected peers over
//! the `Transport` seam, settles `stored_locally` through the
//! `LocalStateSink`, and enqueues a placement commit that the batcher folds
//! into ONE `update_placement_heights` consensus tx per flush window.
//!
//! The engine owns NO runtime: the host passes a data-plane handle (fragment
//! sends) and a control-plane handle (placement batcher) at spawn. All
//! tunables and pure batching decisions live in `policy`.

pub mod policy;
pub mod reencode;

use crate::error::StorageError;
use crate::fragstore;
use crate::placement;
use crate::store::DistributableBlob;
use crate::traits::{
    LocalStateSink, PeerRef, StateReader, SubmitError, Transport, TransportError, TxSubmitter,
};
use crate::types::{BlobId, PlacementUpdate};
use hopnet_common::Blake3Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Semaphore};

/// Process-wide send bound (held across a send's domain retries; no DB conn
/// is ever held here).
static SEND_PERMITS: Semaphore = Semaphore::const_new(policy::SEND_PERMITS);

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
}

#[derive(Debug)]
pub enum EngineError {
    /// Seam/state failure (DB checkout, query).
    State(StorageError),
    /// Too many fragments could not be placed, or local read failure.
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

/// Outcome of one blob's tier-1 repair.
#[derive(Debug, Clone, Copy)]
pub enum RepairOutcome {
    /// Placement is unchanged since the blob's commit height (or the blob
    /// is unplaced/unknown) — nothing to do.
    Unchanged,
    /// Placement moved: fragments this node should now hold were pulled,
    /// and the new primary enqueued a placement re-commit.
    Repaired {
        fragments_pulled: usize,
        recommitted: bool,
    },
    /// Some needed fragment could not be sourced — retried on a later scan.
    Failed { fragments_pulled: usize },
}

/// Aggregate outcome of a tier-1 repair sweep over many blobs.
#[derive(Debug, Default)]
pub struct RepairStats {
    pub checked: usize,
    pub repaired: usize,
    pub failed: usize,
    pub fragments_pulled: usize,
}

/// One re-encode work item for the serial repair worker
/// (RFC-STORAGE-001 Repair: this node was elected repairer of the chunk).
#[derive(Debug)]
pub struct ReencodeCmd {
    pub blob_id: BlobId,
    pub chunk_number: u32,
    pub missing_classes: Vec<u32>,
}

/// Handle to the running engine. Cheap to clone; the host stores one in its
/// app state (mirrors the consensus EngineHandle pattern).
#[derive(Clone)]
pub struct EngineHandle {
    distribution_tx: mpsc::UnboundedSender<BlobId>,
    repair_tx: mpsc::UnboundedSender<(BlobId, tokio::sync::oneshot::Sender<RepairOutcome>)>,
    reencode_urgent_tx: mpsc::UnboundedSender<ReencodeCmd>,
    reencode_lazy_tx: mpsc::UnboundedSender<ReencodeCmd>,
}

impl EngineHandle {
    /// Kick distribution for a decided blob. NON-BLOCKING (unbounded send) —
    /// safe from the host's consensus apply path (shell-thread adjacency:
    /// no DB, no awaits). Every node kicks every decided blob; the origin
    /// filter in the workers no-ops the rest.
    pub fn notify_blob_committed(&self, blob_id: BlobId) {
        let _ = self.distribution_tx.send(blob_id);
    }

    /// Tier-1 repair for one blob: if placement moved since its commit
    /// height, pull the fragments this node should now hold and (from the
    /// new primary) re-commit the placement. Serialized on the engine's
    /// repair worker; resolves when the blob's repair completes. `None` =
    /// engine gone.
    pub async fn repair_blob(&self, blob_id: BlobId) -> Option<RepairOutcome> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.repair_tx.send((blob_id, tx)).ok()?;
        rx.await.ok()
    }

    /// Enqueue one chunk re-encode on the serial repair worker. Urgent
    /// (live classes below the watermark) preempts migration pulls and
    /// lazy work; lazy items drain one at a time behind everything else.
    /// The single worker bounds re-encode memory and bandwidth mesh-wide.
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

    /// Tier-1 repair for a batch of blobs, serialized on the repair worker;
    /// aggregates the per-blob outcomes. Engine-gone counts as a failure —
    /// a later scan retries the blob.
    pub async fn repair_blobs(
        &self,
        blob_ids: impl IntoIterator<Item = BlobId> + Send,
    ) -> RepairStats {
        let mut stats = RepairStats::default();
        for blob_id in blob_ids {
            stats.checked += 1;
            match self.repair_blob(blob_id.clone()).await {
                Some(RepairOutcome::Unchanged) => {}
                Some(RepairOutcome::Repaired {
                    fragments_pulled,
                    recommitted,
                }) => {
                    stats.repaired += 1;
                    stats.fragments_pulled += fragments_pulled;
                    tracing::info!(
                        "repair: blob {} pulled {} fragments (recommitted: {})",
                        blob_id,
                        fragments_pulled,
                        recommitted
                    );
                }
                Some(RepairOutcome::Failed { fragments_pulled }) => {
                    stats.failed += 1;
                    stats.fragments_pulled += fragments_pulled;
                }
                None => {
                    stats.failed += 1;
                    tracing::error!("repair: engine gone while repairing {}", blob_id);
                }
            }
        }
        stats
    }

    /// Spawn the engine: the global distribution worker pool on `data_rt`
    /// and the placement-commit batcher on `control_rt`.
    pub fn spawn<T, S, X, L>(
        seams: Seams<T, S, X, L>,
        config: EngineConfig,
        data_rt: tokio::runtime::Handle,
        control_rt: tokio::runtime::Handle,
    ) -> EngineHandle
    where
        T: Transport + 'static,
        S: StateReader + 'static,
        X: TxSubmitter + 'static,
        L: LocalStateSink + 'static,
    {
        let (distribution_tx, distribution_rx) = mpsc::unbounded_channel::<BlobId>();
        let (placement_tx, placement_rx) = mpsc::unbounded_channel::<PlacementUpdate>();
        let (repair_tx, mut repair_rx) =
            mpsc::unbounded_channel::<(BlobId, tokio::sync::oneshot::Sender<RepairOutcome>)>();
        let (reencode_urgent_tx, mut reencode_urgent_rx) = mpsc::unbounded_channel::<ReencodeCmd>();
        let (reencode_lazy_tx, mut reencode_lazy_rx) = mpsc::unbounded_channel::<ReencodeCmd>();

        control_rt.spawn(placement_batcher(placement_rx, seams.submitter.clone()));

        // Serial repair worker (RFC-STORAGE-001 Repair): ONE worker for
        // migration pulls and re-encodes so repair memory (~one 40MB chunk
        // of shards in flight) and bandwidth stay bounded. Priority is
        // biased: urgent re-encode (below watermark) > migration pulls >
        // lazy re-encode.
        {
            let seams = seams.clone();
            let fragments_dir = config.fragments_dir.clone();
            let placement_tx = placement_tx.clone();
            data_rt.spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        cmd = reencode_urgent_rx.recv() => {
                            let Some(cmd) = cmd else { break };
                            run_reencode_cmd(&seams, &fragments_dir, cmd).await;
                        }
                        item = repair_rx.recv() => {
                            let Some((blob_id, reply)) = item else { break };
                            let outcome =
                                repair_one(&seams, &fragments_dir, &placement_tx, &blob_id).await;
                            let outcome = match outcome {
                                Ok(o) => o,
                                Err(e) => {
                                    tracing::warn!("repair: blob {blob_id} failed: {e}");
                                    RepairOutcome::Failed {
                                        fragments_pulled: 0,
                                    }
                                }
                            };
                            let _ = reply.send(outcome);
                        }
                        cmd = reencode_lazy_rx.recv() => {
                            let Some(cmd) = cmd else { break };
                            run_reencode_cmd(&seams, &fragments_dir, cmd).await;
                        }
                    }
                }
            });
        }

        let distribution_rx = Arc::new(Mutex::new(distribution_rx));
        let config = Arc::new(config);
        for worker in 0..policy::DISTRIBUTION_WORKERS {
            let seams = seams.clone();
            let config = config.clone();
            let rx = distribution_rx.clone();
            let placement_tx = placement_tx.clone();
            data_rt.spawn(async move {
                loop {
                    let blob_id = {
                        let mut rx = rx.lock().await;
                        rx.recv().await
                    };
                    let Some(blob_id) = blob_id else { break };
                    if let Err(e) =
                        distribute_one(&seams, &config, &placement_tx, blob_id.clone()).await
                    {
                        tracing::error!("distribution worker {worker}: blob {blob_id} failed: {e}");
                    }
                }
            });
        }

        EngineHandle {
            distribution_tx,
            repair_tx,
            reencode_urgent_tx,
            reencode_lazy_tx,
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

/// Class → responsible node for one blob under the balanced capped
/// rendezvous placement (RFC-STORAGE-001): seeded selection first (all
/// members at ≤30, scored top-30 above), then the balanced assignment over
/// the selected ids. Returns the selected peers (pull-candidate pool)
/// alongside the class map.
pub fn assign_for_blob(
    seed: &[u8; 32],
    members: Vec<PeerRef>,
    metrics: Vec<crate::placement::MetricsRow>,
    weights: &std::collections::HashMap<i32, u64>,
) -> (Vec<PeerRef>, Vec<i32>) {
    let selected: Vec<PeerRef> = placement::select_nodes_for_blob(members, metrics, seed);
    let ids: Vec<i32> = selected.iter().map(|p| p.node_id).collect();
    let assignment = placement::assign_fragment_classes(
        seed,
        &ids,
        weights,
        crate::rs::TOTAL_FRAGMENTS_PER_CHUNK as u32,
    );
    (selected, assignment)
}

/// Tier-1 repair for one placed blob (RFC-STORAGE-001 pull-migration):
/// diff the PER-CLASS responsible targets between the blob's placement
/// height and the current member view; if any class moved, PULL every
/// fragment this node is newly responsible for (inventory sources → the
/// OLD holders as candidates), and let the responsible node of class 0
/// re-commit the placement through the batcher. No pushes, no deletions —
/// stale copies become surplus until eviction/self-check account for them.
async fn repair_one<T, S, X, L>(
    seams: &Seams<T, S, X, L>,
    fragments_dir: &str,
    placement_tx: &mpsc::UnboundedSender<PlacementUpdate>,
    blob_id: &BlobId,
) -> Result<RepairOutcome, EngineError>
where
    T: Transport + 'static,
    S: StateReader,
    X: TxSubmitter,
    L: LocalStateSink,
{
    let Some(manifest) = seams.state.blob_manifest(blob_id)? else {
        return Ok(RepairOutcome::Unchanged); // unknown blob (raced a delete)
    };
    let Some(old_height) = manifest.placement_height else {
        // Unplaced blobs belong to distribution (NULL-placement
        // reconciliation is the RFC-014 follow-up), not repair.
        return Ok(RepairOutcome::Unchanged);
    };
    let my_node_id = seams
        .state
        .local_node_id()
        .ok_or_else(|| EngineError::Transfer("local node id not set".to_string()))?;

    let seed = placement::placement_seed(blob_id);
    // Old side: validators at the placement height approximate the member
    // view the placement was computed against (candidates + diff baseline);
    // weights re-derived from the same historical metrics.
    let old_inputs = seams.state.placement_inputs_at(old_height)?;
    let old_weights: std::collections::HashMap<i32, u64> = old_inputs
        .metrics
        .iter()
        .map(|m| (m.node_id, placement::quantized_weight(m)))
        .collect();
    let (old_selected, old_assignment) = assign_for_blob(
        &seed,
        old_inputs.validators,
        old_inputs.metrics,
        &old_weights,
    );
    // New side: the decay-tiered member view.
    let view = seams.state.storage_view()?;
    let new_height = view.height;
    let (_, new_assignment) = assign_for_blob(&seed, view.members, view.metrics, &view.weights);

    // Per-class diff — a selected-set comparison is blind to assignment
    // moves when the set is unchanged (always, on ≤30-node meshes).
    if old_assignment == new_assignment {
        return Ok(RepairOutcome::Unchanged);
    }

    tracing::info!(
        "repair: blob {} placement moved ({} → {}, {} classes renumbered)",
        blob_id,
        old_height,
        new_height,
        old_assignment
            .iter()
            .zip(&new_assignment)
            .filter(|(a, b)| a != b)
            .count()
    );

    // Pull phase: fragments whose class this node is NEWLY responsible for
    // and which are not on local disk.
    let mut wanted: Vec<(u32, Blake3Hash)> = Vec::new();
    for (originals, recovery) in manifest.chunks.values() {
        for map in [originals, recovery] {
            for (local_index, (hash, _, stored_locally)) in map {
                if *stored_locally {
                    continue;
                }
                if new_assignment.get(*local_index).copied() == Some(my_node_id) {
                    wanted.push((*local_index as u32, *hash));
                }
            }
        }
    }

    let mut pulled = 0usize;
    let mut failed = 0usize;
    if !wanted.is_empty() {
        let hashes: Vec<Blake3Hash> = wanted.iter().map(|(_, h)| *h).collect();
        let mut sources = seams.state.fragment_sources(&hashes)?;
        for (_, hash) in &wanted {
            let hint = sources.remove(hash);
            match crate::api::find_fragment_via(&seams.transport, hash, &old_selected, hint).await {
                Some(data) => {
                    if let Err(e) = fragstore::store_fragment(fragments_dir, hash, data) {
                        tracing::warn!("repair: store fragment {} failed: {e}", hash.to_hex());
                        failed += 1;
                        continue;
                    }
                    seams.local_state.mark_local(*hash);
                    pulled += 1;
                }
                None => {
                    tracing::warn!("repair: fragment {} not sourceable", hash.to_hex());
                    failed += 1;
                }
            }
        }
    }

    if failed > 0 {
        // Leave placement_height at the old commit — the blob stays
        // repair-eligible and a later scan retries.
        return Ok(RepairOutcome::Failed {
            fragments_pulled: pulled,
        });
    }

    // Re-commit: exactly one deterministic submitter — the responsible
    // node of class 0. (If it is down, a later scan on the next epoch
    // re-runs; gets fall back through the ladder meanwhile.)
    let recommitted = new_assignment.first().copied() == Some(my_node_id);
    if recommitted
        && placement_tx
            .send(PlacementUpdate {
                blob_id: blob_id.clone(),
                placement_height: new_height,
            })
            .is_err()
    {
        tracing::error!("repair: placement batcher gone — re-commit dropped");
    }

    Ok(RepairOutcome::Repaired {
        fragments_pulled: pulled,
        recommitted,
    })
}

/// Distribute one decided blob if THIS node holds its fragments. The kick
/// arrives via the host's own apply — no polling, no forwarder race: state
/// is committed before the kick fires.
async fn distribute_one<T, S, X, L>(
    seams: &Seams<T, S, X, L>,
    config: &EngineConfig,
    placement_tx: &mpsc::UnboundedSender<PlacementUpdate>,
    blob_id: BlobId,
) -> Result<(), EngineError>
where
    T: Transport + 'static,
    S: StateReader,
    X: TxSubmitter,
    L: LocalStateSink,
{
    let Some(blob) = seams.state.distributable_blob(&blob_id)? else {
        // Not ours to distribute (fragments not local), already placed, or
        // already handled — the common case on non-origin nodes.
        return Ok(());
    };

    tracing::info!("Starting fragment distribution for blob {}", blob_id);

    let placement_height = distribute_blob_fragments(seams, config, &blob).await?;

    // Enqueue the placement commit — batched with other blobs' placements
    // into ONE consensus tx per window (RFC-014 control-plane discipline).
    // Fire-and-forget: distribution success means "fragments placed + commit
    // enqueued"; the batcher owns delivery. A terminally-dropped batch
    // leaves placement_height NULL — the blob stays downloadable via
    // inventory discovery. (NULL-placement reconciliation is an RFC-014
    // follow-up.)
    if placement_tx
        .send(PlacementUpdate {
            blob_id: blob_id.clone(),
            placement_height,
        })
        .is_err()
    {
        tracing::error!("placement batcher gone — placement commit dropped");
    }

    tracing::info!(
        "Fragment distribution complete for {} (placement commit enqueued)",
        blob_id
    );
    Ok(())
}

/// Push every fragment of one blob to its placement-selected peers. Returns
/// the consensus height the placement was computed against.
async fn distribute_blob_fragments<T, S, X, L>(
    seams: &Seams<T, S, X, L>,
    config: &EngineConfig,
    blob: &DistributableBlob,
) -> Result<i32, EngineError>
where
    T: Transport + 'static,
    S: StateReader,
    X: TxSubmitter,
    L: LocalStateSink,
{
    let my_node_id = seams
        .state
        .local_node_id()
        .ok_or_else(|| EngineError::Transfer("local node id not set".to_string()))?;

    // One consistent snapshot of the decay-tiered member view (the host
    // reads everything on one scoped checkout, dropped before any send).
    let view = seams.state.storage_view()?;
    let view_height = view.height;

    // Placement seeds from the blob id (RFC-014): deterministic, public,
    // zero plaintext-derived input. Balanced capped rendezvous assigns one
    // responsible node per class (RFC-STORAGE-001) — no backup fallbacks:
    // a failed send keeps the fragment origin-held as surplus, and the
    // responsible node pulls it through the migration rung later.
    let seed = placement::placement_seed(&blob.blob_id);
    let (selected_nodes, assignment) =
        assign_for_blob(&seed, view.members, view.metrics, &view.weights);

    tracing::debug!(
        "Fragment distribution using {} selected nodes at consensus height {} for blob {}",
        selected_nodes.len(),
        view_height,
        blob.blob_id
    );

    let total_fragments = blob.fragments.len();
    let num_workers = total_fragments.clamp(1, policy::BLOB_SEND_WORKERS);
    tracing::info!(
        "Starting parallel distribution of {} fragments with {} workers",
        total_fragments,
        num_workers
    );

    // Work queue + per-blob send workers. Workers scale with the BLOB
    // (small cap); actual sends are bounded PROCESS-WIDE by SEND_PERMITS —
    // under an upload burst, concurrency tracks mesh bandwidth, not upload
    // count (RFC-014 engine rule).
    let work_queue: Arc<Mutex<Vec<(u32, Blake3Hash)>>> =
        Arc::new(Mutex::new(blob.fragments.clone()));
    let (failure_tx, mut failure_rx) = mpsc::unbounded_channel::<Blake3Hash>();
    let (remote_tx, mut remote_rx) = mpsc::unbounded_channel::<Blake3Hash>();
    let successful_placements = Arc::new(AtomicUsize::new(0));

    // class → responsible peer, resolved once for all workers.
    let targets: std::collections::HashMap<i32, PeerRef> =
        selected_nodes.iter().map(|p| (p.node_id, *p)).collect();
    let assignment = Arc::new(assignment);
    let targets = Arc::new(targets);
    let mut worker_handles = Vec::new();
    for worker_id in 0..num_workers {
        let failure_tx = failure_tx.clone();
        let remote_tx = remote_tx.clone();
        let queue = work_queue.clone();
        let transport = seams.transport.clone();
        let assignment = assignment.clone();
        let targets = targets.clone();
        let successes = successful_placements.clone();
        let fragments_dir = config.fragments_dir.clone();

        worker_handles.push(tokio::spawn(async move {
            tracing::debug!("Worker {} starting fragment distribution", worker_id);
            loop {
                let next_work = {
                    let mut queue_lock = queue.lock().await;
                    queue_lock.pop()
                };
                let Some((local_index, fragment_hash)) = next_work else {
                    tracing::debug!("Worker {} finished - queue empty", worker_id);
                    break;
                };

                // Single responsible node per class (balanced capped
                // rendezvous) — no backup fallbacks.
                let Some(responsible) = assignment
                    .get(local_index as usize)
                    .and_then(|id| targets.get(id))
                else {
                    tracing::warn!(
                        "Worker {} has no responsible node for class {} — keeping locally",
                        worker_id,
                        local_index
                    );
                    let _ = failure_tx.send(fragment_hash);
                    continue;
                };

                if responsible.node_id == my_node_id {
                    tracing::debug!(
                        "Fragment {} placed on local node {}, keeping locally",
                        fragment_hash,
                        my_node_id
                    );
                    successes.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Process-wide send bound (held across the send's
                // domain retries; no DB conn is held here).
                let _permit = SEND_PERMITS.acquire().await;
                match send_fragment_to_peer(
                    transport.as_ref(),
                    responsible,
                    &fragment_hash,
                    &fragments_dir,
                )
                .await
                {
                    Ok(()) => {
                        let _ = remote_tx.send(fragment_hash);
                        tracing::debug!(
                            "Successfully sent fragment {} to node {}",
                            fragment_hash,
                            responsible.node_id
                        );
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        // Origin keeps the fragment as surplus; the
                        // responsible node pulls it via the migration rung.
                        tracing::warn!(
                            "Worker {} failed to send fragment {} to responsible node {}: {} — keeping locally",
                            worker_id,
                            fragment_hash,
                            responsible.node_id,
                            e
                        );
                        let _ = failure_tx.send(fragment_hash);
                    }
                }
            }
            tracing::debug!("Worker {} completed", worker_id);
        }));
    }

    drop(failure_tx);
    drop(remote_tx);
    for handle in worker_handles {
        let _ = handle.await;
    }

    let mut failed_fragments = Vec::new();
    while let Some(fragment_hash) = failure_rx.recv().await {
        failed_fragments.push(fragment_hash);
    }

    // Settle all remotely-placed fragments in one batch through the sink.
    let mut remotely_placed = Vec::new();
    while let Some(fragment_hash) = remote_rx.recv().await {
        remotely_placed.push(fragment_hash);
    }
    if !remotely_placed.is_empty() {
        seams.local_state.mark_remote_batch(remotely_placed);
    }

    let successful = successful_placements.load(Ordering::Relaxed);
    let failure_rate = (failed_fragments.len() as f64 / total_fragments as f64) * 100.0;
    if !failed_fragments.is_empty() {
        tracing::warn!(
            "Failed to distribute {} of {} fragments ({:.1}%), fragments remain stored locally",
            failed_fragments.len(),
            total_fragments,
            failure_rate
        );
    }
    if failure_rate > policy::FAILURE_THRESHOLD_PERCENT {
        return Err(EngineError::Transfer(format!(
            "Fragment distribution failed: {}/{} fragments ({:.1}%) could not be placed (threshold: {:.1}%)",
            failed_fragments.len(),
            total_fragments,
            failure_rate,
            policy::FAILURE_THRESHOLD_PERCENT
        )));
    }

    tracing::info!(
        "Successfully distributed {} of {} fragments ({:.1}% success rate)",
        successful,
        total_fragments,
        (successful as f64 / total_fragments as f64) * 100.0
    );
    Ok(view_height)
}

/// Send one fragment to one peer with domain-level retry. Peer-side errors
/// (hash mismatch, size limit) retry with a delay; transport errors
/// propagate — the host's transport already did its own retry, the caller
/// moves on to the next candidate.
async fn send_fragment_to_peer<T: Transport>(
    transport: &T,
    peer: &PeerRef,
    fragment_hash: &Blake3Hash,
    fragments_dir: &str,
) -> Result<(), EngineError> {
    let fragment_data = fragstore::fetch_and_verify_fragment(fragment_hash, fragments_dir)
        .map_err(|e| {
            EngineError::Transfer(format!("Failed to read fragment {}: {}", fragment_hash, e))
        })?;

    for attempt in 0..policy::SEND_MAX_RETRIES {
        match transport
            .store_fragment(peer, fragment_hash, fragment_data.clone())
            .await
        {
            Ok(result) => {
                tracing::debug!(
                    "Successfully sent fragment {} to node {} on attempt {}{}",
                    fragment_hash,
                    peer.node_id,
                    attempt + 1,
                    if result.already_existed {
                        " (already existed)"
                    } else {
                        ""
                    }
                );
                return Ok(());
            }
            Err(TransportError::Peer(msg)) => {
                tracing::warn!(
                    "Fragment store attempt {} for {} on node {} failed: {}",
                    attempt + 1,
                    fragment_hash,
                    peer.node_id,
                    msg
                );
                if attempt < policy::SEND_MAX_RETRIES - 1 {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        policy::SEND_RETRY_DELAY_MS,
                    ))
                    .await;
                    continue;
                }
                return Err(EngineError::Transfer(format!(
                    "Fragment store failed after {} attempts: {}",
                    policy::SEND_MAX_RETRIES,
                    msg
                )));
            }
            Err(TransportError::Transport(msg)) => {
                return Err(EngineError::Transfer(format!(
                    "Transport error sending fragment {} to node {}: {}",
                    fragment_hash, peer.node_id, msg
                )));
            }
        }
    }

    Err(EngineError::Transfer(format!(
        "Failed to send fragment {} to node {} after {} attempts",
        fragment_hash,
        peer.node_id,
        policy::SEND_MAX_RETRIES
    )))
}

/// Collect placement updates and submit them as ONE consensus tx per window.
async fn placement_batcher<X: TxSubmitter>(
    mut rx: mpsc::UnboundedReceiver<PlacementUpdate>,
    submitter: Arc<X>,
) {
    // (update, attempts) — retried entries carry their attempt count.
    let mut pending: Vec<(PlacementUpdate, u8)> = Vec::new();
    loop {
        // Wait for the first entry of a window (or carry-over retries).
        if pending.is_empty() {
            match rx.recv().await {
                Some(u) => pending.push((u, 0)),
                None => return,
            }
        }
        // Collect until the window closes or the batch fills.
        let window =
            tokio::time::sleep(std::time::Duration::from_millis(policy::PLACEMENT_FLUSH_MS));
        tokio::pin!(window);
        loop {
            tokio::select! {
                more = rx.recv() => match more {
                    Some(u) => {
                        pending.push((u, 0));
                        if pending.len() >= policy::PLACEMENT_FLUSH_MAX {
                            break;
                        }
                    }
                    None => break,
                },
                _ = &mut window => break,
            }
        }

        let batch = policy::dedup_window(std::mem::take(&mut pending));
        let updates: Vec<PlacementUpdate> = batch.iter().map(|(u, _)| u.clone()).collect();

        let encoded = match bincode::serde::encode_to_vec(&updates, bincode::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("placement batch encode failed (dropped): {e}");
                continue;
            }
        };

        match submitter.submit(policy::PLACEMENT_COMMIT_FN, encoded).await {
            Ok(()) => {
                tracing::debug!("placement batch committed ({} blobs)", updates.len());
            }
            Err(SubmitError::Rejected(reason)) => {
                // Permanent rejection (business validation OR signing) —
                // dropping is correct.
                tracing::error!("placement batch rejected (dropped): {reason}");
            }
            Err(SubmitError::Transient(e)) => {
                // Transient (timeout / backpressure / internal) — re-stage
                // with attempt caps.
                tracing::warn!("placement batch submit failed, will retry: {e}");
                for (u, attempts) in batch {
                    if attempts + 1 < policy::PLACEMENT_FLUSH_ATTEMPTS {
                        pending.push((u, attempts + 1));
                    } else {
                        tracing::error!(
                            "placement commit for {} dropped after {} attempts",
                            u.blob_id,
                            policy::PLACEMENT_FLUSH_ATTEMPTS
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{PlacementInputs, StoreResult, SubmitError};
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Mutex;

    /// Seams for repair tests: one remote peer serving fragments from a
    /// map; old placement = the peer, new placement = configurable.
    struct RepairNet {
        served: Mutex<HashMap<Blake3Hash, Vec<u8>>>,
        manifest: Mutex<Option<crate::store::BlobManifest>>,
        /// node ids returned by placement_inputs() (current height 9).
        new_nodes: Vec<i32>,
        marked_local: Mutex<Vec<Blake3Hash>>,
    }

    fn peers(ids: &[i32]) -> Vec<PeerRef> {
        ids.iter()
            .map(|&node_id| PeerRef {
                node_id,
                pubkey: [node_id as u8; 32],
            })
            .collect()
    }

    impl Transport for RepairNet {
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

    impl StateReader for RepairNet {
        fn placement_inputs(&self) -> Result<PlacementInputs, StorageError> {
            Ok(PlacementInputs {
                height: 9,
                validators: peers(&self.new_nodes),
                metrics: vec![],
            })
        }
        fn placement_inputs_at(&self, height: i32) -> Result<PlacementInputs, StorageError> {
            // Historical placement: the remote peer (node 2) only.
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
        fn distributable_blob(
            &self,
            _blob_id: &BlobId,
        ) -> Result<Option<crate::store::DistributableBlob>, StorageError> {
            Ok(None)
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

    impl TxSubmitter for RepairNet {
        async fn submit(
            &self,
            _function: &'static str,
            _payload: Vec<u8>,
        ) -> Result<(), SubmitError> {
            Ok(())
        }
    }

    impl LocalStateSink for RepairNet {
        fn mark_local(&self, fragment_hash: Blake3Hash) {
            self.marked_local.lock().unwrap().push(fragment_hash);
        }
        fn mark_remote_batch(&self, _fragment_hashes: Vec<Blake3Hash>) {}
    }

    fn seams(net: Arc<RepairNet>) -> Seams<RepairNet, RepairNet, RepairNet, RepairNet> {
        Seams {
            transport: net.clone(),
            state: net.clone(),
            submitter: net.clone(),
            local_state: net,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn repair_pulls_moved_fragments_and_recommits_from_new_primary() {
        // Should: when the seeded selection at the current height differs
        // from the selection at the blob's placement height, pull every
        // fragment this node now owns (all of them — single-validator new
        // set), settle stored_locally through the sink, and enqueue ONE
        // placement re-commit at the new height (this node is the new
        // primary). When the selection is unchanged, do nothing.
        // Should not: re-commit when a fragment can't be sourced (Failed
        // leaves the old placement height so a later scan retries).
        // Impact: tier-1 repair is the only healing path after membership
        // change — a wrong no-op strands fragments on departed nodes; a
        // premature re-commit points gets at holders that don't exist.
        let base = std::env::temp_dir().join(format!("hopnet-repair-test-{}", std::process::id()));
        let dir_src = base.join("src").to_str().unwrap().to_string();
        let dir_dst = base.join("dst").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_dst).unwrap();

        let key: chacha20poly1305::Key = [0x55u8; 32].into();
        let blob_id = crate::CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a5").unwrap();
        let plaintext: Vec<u8> = (0..50_000u32).map(|i| (i % 233) as u8).collect();
        let outcome = crate::api::put(
            plaintext.as_slice(),
            plaintext.len(),
            blob_id.clone(),
            &key,
            &dir_src,
        )
        .await
        .unwrap();

        // Manifest: placed at height 3, nothing local on THIS node.
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
            placement_height: Some(3),
            chunks,
        };

        let net = Arc::new(RepairNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest.clone())),
            new_nodes: vec![1], // placement moved: node 1 (me) now owns it
            marked_local: Mutex::new(Vec::new()),
        });
        for f in &outcome.fragments {
            let data = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
            net.served.lock().unwrap().insert(f.fragment_hash, data);
        }

        let (placement_tx, mut placement_rx) = mpsc::unbounded_channel();
        let result = repair_one(&seams(net.clone()), &dir_dst, &placement_tx, &blob_id)
            .await
            .unwrap();
        match result {
            RepairOutcome::Repaired {
                fragments_pulled,
                recommitted,
            } => {
                assert_eq!(fragments_pulled, 30, "single-node set owns everything");
                assert!(recommitted, "this node is the new primary of fragment 0");
            }
            other => panic!("expected Repaired, got {:?}", other),
        }
        assert_eq!(net.marked_local.lock().unwrap().len(), 30);
        let update = placement_rx.try_recv().expect("re-commit enqueued");
        assert_eq!(update.blob_id, blob_id);
        assert_eq!(update.placement_height, 9);
        // Pulled fragments verify on disk
        for f in &outcome.fragments {
            assert!(fragstore::fragment_exists_and_valid(
                &dir_dst,
                &f.fragment_hash
            ));
        }

        // Unchanged selection → no-op (old and new both = node 2)
        let net2 = Arc::new(RepairNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest.clone())),
            new_nodes: vec![2],
            marked_local: Mutex::new(Vec::new()),
        });
        let result = repair_one(&seams(net2), &dir_dst, &placement_tx, &blob_id)
            .await
            .unwrap();
        assert!(matches!(result, RepairOutcome::Unchanged));

        // Unsourceable fragment → Failed, NO re-commit
        let mut broken = manifest.clone();
        for (originals, _) in broken.chunks.values_mut() {
            for entry in originals.values_mut() {
                entry.2 = false;
            }
        }
        let net3 = Arc::new(RepairNet {
            served: Mutex::new(HashMap::new()), // peer serves NOTHING
            manifest: Mutex::new(Some(broken)),
            new_nodes: vec![1],
            marked_local: Mutex::new(Vec::new()),
        });
        let (placement_tx3, mut placement_rx3) = mpsc::unbounded_channel();
        let result = repair_one(&seams(net3), &dir_dst, &placement_tx3, &blob_id)
            .await
            .unwrap();
        assert!(matches!(result, RepairOutcome::Failed { .. }));
        assert!(placement_rx3.try_recv().is_err(), "no re-commit on failure");

        let _ = std::fs::remove_dir_all(&base);
    }
}
