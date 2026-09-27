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
//! promptly (its `self_check_fragments` differential, then disk truth) and,
//! when the goal's evidence is complete, proposes `ConfirmPlacement`: the
//! latency path for uploads and real moves. Re-goaled blobs already held
//! here submit nothing from the worker; the tick's fulfillment pass
//! confirms them in batches (RFC-STORAGE-003 S4). Racing proposers are
//! harmless: apply validation carries the safety.
//!
//! The push pipeline (origin-push worker pool, send permits, the failure
//! threshold, the blind placement batcher) is gone. The engine owns NO
//! runtime: the host passes a handle at spawn.

pub mod policy;
pub mod reencode;

use crate::error::StorageError;
use crate::fragstore;
use crate::lifecycle::{ConfirmPlacement, PlacementConfirmation, CONFIRM_TX_FN};
use crate::traits::{LocalStateSink, StateReader, SubmitError, Transport, TxSubmitter};
use crate::types::BlobId;
use hopnet_common::Blake3Hash;
use std::collections::BTreeMap;
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
    /// A prompt attestation was submitted for this node's new copies.
    pub attested: bool,
    /// A ConfirmPlacement was proposed (the goal's evidence was complete).
    pub confirm_proposed: bool,
}

/// Aggregate of many pull checks (the tick's re-kick, the operator drain).
#[derive(Debug, Default, Clone, Copy)]
pub struct PullStats {
    pub checked: usize,
    pub owed: usize,
    pub pulled: usize,
    pub rebuilt: usize,
    pub failed: usize,
    pub confirms_proposed: usize,
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
}

impl EngineHandle {
    /// Latency hint for a decided blob (or a moved goal): check this node's
    /// pull duties for it soon. NON-BLOCKING (unbounded send) — safe from
    /// the host's consensus apply path. Carries no correctness weight: the
    /// tick's level-triggered re-kick discovers anything a hint missed.
    pub fn notify_blob_committed(&self, blob_id: BlobId) {
        let _ = self.pull_tx.send((blob_id, None));
    }

    /// Pull check for one blob, serialized on the worker; resolves when it
    /// completes. `None` = engine gone.
    pub async fn pull_blob(&self, blob_id: BlobId) -> Option<PullOutcome> {
        let (tx, rx) = oneshot::channel();
        self.pull_tx.send((blob_id, Some(tx))).ok()?;
        rx.await.ok()
    }

    /// Pull checks for a batch of blobs, aggregated. Engine-gone counts as
    /// a failure — a later tick retries the blob.
    pub async fn pull_blobs(&self, blob_ids: impl IntoIterator<Item = BlobId> + Send) -> PullStats {
        let mut stats = PullStats::default();
        for blob_id in blob_ids {
            stats.checked += 1;
            match self.pull_blob(blob_id.clone()).await {
                Some(o) => {
                    stats.owed += o.owed;
                    stats.pulled += o.pulled;
                    stats.rebuilt += o.rebuilt;
                    stats.failed += o.failed;
                    stats.confirms_proposed += o.confirm_proposed as usize;
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

    /// Spawn the engine: ONE serial worker on `data_rt` running the duty
    /// ladder as queue order — urgent re-encode > pull checks > lazy
    /// re-encode — so repair memory (~one chunk of shards in flight) and
    /// bandwidth stay bounded per node.
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
        data_rt.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    cmd = reencode_urgent_rx.recv() => {
                        let Some(cmd) = cmd else { break };
                        run_reencode_cmd(&seams, &fragments_dir, cmd).await;
                    }
                    item = pull_rx.recv() => {
                        let Some((blob_id, reply)) = item else { break };
                        let outcome = match pull_owed(&seams, &fragments_dir, &blob_id).await {
                            Ok(o) => o,
                            Err(e) => {
                                tracing::warn!("pull: blob {blob_id} failed: {e}");
                                PullOutcome::default()
                            }
                        };
                        if let Some(reply) = reply {
                            let _ = reply.send(outcome);
                        }
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
/// the goal, fetch each with recovery, then attest and (if the goal's
/// evidence is complete) propose confirmation.
async fn pull_owed<T, S, X, L>(
    seams: &Seams<T, S, X, L>,
    fragments_dir: &str,
    blob_id: &BlobId,
) -> Result<PullOutcome, EngineError>
where
    T: Transport + 'static,
    S: StateReader,
    X: TxSubmitter,
    L: LocalStateSink,
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
        let candidates = seams.state.all_peers()?;
        for (chunk, classes) in &owed {
            let mut unserved: Vec<u32> = Vec::new();
            for (class, hash) in classes {
                let hint = sources.remove(hash);
                match crate::api::find_fragment_via(&seams.transport, hash, &candidates, hint).await
                {
                    Some(data) => match fragstore::store_fragment(fragments_dir, hash, data) {
                        Ok(()) => {
                            seams.local_state.mark_local(*hash).await;
                            outcome.pulled += 1;
                        }
                        Err(e) => {
                            tracing::warn!("pull: store fragment {} failed: {e}", hash.to_hex());
                            unserved.push(*class);
                        }
                    },
                    None => unserved.push(*class),
                }
            }
            if unserved.is_empty() {
                continue;
            }
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
                }
            }
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
        // Belief first (rows for what we hold), then disk truth (S5):
        // every fragment of this blob on our disk is content-verified
        // right now and attested with the current height, so the
        // confirmation's recency check has fresh evidence — the origin's
        // classes included, which no pull ever touches.
        let report = seams.state.self_check_report()?;
        if !report.is_empty() {
            let encoded = bincode::serde::encode_to_vec(&report, bincode::config::standard())
                .map_err(|e| EngineError::Transfer(format!("self-check encode: {e}")))?;
            match seams.submitter.submit(policy::SELF_CHECK_FN, encoded).await {
                Ok(()) => outcome.attested = true,
                Err(SubmitError::Rejected(r)) => tracing::warn!("prompt attestation rejected: {r}"),
                Err(SubmitError::Transient(e)) => {
                    tracing::debug!("prompt attestation deferred to the self-check cron: {e}")
                }
            }
        }
        let present: Vec<Blake3Hash> = manifest
            .chunks
            .values()
            .flat_map(|(o, r)| o.values().chain(r.values()))
            .map(|(hash, _, _)| *hash)
            .filter(|hash| fragstore::fragment_exists_and_valid(fragments_dir, hash))
            .collect();
        if !present.is_empty() {
            let attestation = crate::types::FragmentAttestation {
                node_id: me,
                height: seams.state.current_height()?,
                present,
                suspect: Vec::new(),
            };
            let encoded = bincode::serde::encode_to_vec(&attestation, bincode::config::standard())
                .map_err(|e| EngineError::Transfer(format!("attestation encode: {e}")))?;
            match seams.submitter.submit(policy::ATTEST_FN, encoded).await {
                Ok(()) => outcome.attested = true,
                Err(SubmitError::Rejected(r)) => {
                    tracing::warn!("disk-truth attestation rejected: {r}")
                }
                Err(SubmitError::Transient(e)) => {
                    tracing::debug!("disk-truth attestation deferred to the sweep: {e}")
                }
            }
        }
        if let Some(height) = seams.state.confirm_ready(blob_id)? {
            let payload = ConfirmPlacement {
                confirmations: vec![PlacementConfirmation {
                    blob_id: blob_id.clone(),
                    height,
                }],
            };
            let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
                .map_err(|e| EngineError::Transfer(format!("confirm encode: {e}")))?;
            match seams.submitter.submit(CONFIRM_TX_FN, encoded).await {
                Ok(()) => outcome.confirm_proposed = true,
                Err(SubmitError::Rejected(r)) => tracing::warn!("confirm proposal rejected: {r}"),
                Err(SubmitError::Transient(e)) => {
                    tracing::debug!("confirm proposal deferred to the tick: {e}")
                }
            }
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
            if outcome.confirm_proposed {
                " (confirm proposed)"
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
    use crate::traits::{PeerRef, PlacementInputs, PullTarget, StoreResult, TransportError};
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
            _payload: Vec<u8>,
        ) -> Result<(), SubmitError> {
            self.submitted.lock().unwrap().push(function);
            Ok(())
        }
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

    // Should: pull every class this node owes under the goal from a
    // serving holder, settle each through the (awaited) sink, attest
    // promptly, and propose confirmation once the goal's evidence is
    // complete.
    // Should not: propose confirmation when the evidence is incomplete, nor
    // pull anything for a blob whose goal assigns this node nothing.
    // Impact: this is the whole distribution path now — a missed duty
    // strands a class on the origin; a premature confirm lapses obligations
    // against holders that do not exist.
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
        });
        for f in &outcome.fragments {
            let data = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
            net.served.lock().unwrap().insert(f.fragment_hash, data);
        }

        let result = pull_owed(&seams(net.clone()), &dir_dst, &blob_id)
            .await
            .unwrap();
        assert_eq!(result.owed, 30);
        assert_eq!(result.pulled, 30);
        assert_eq!(result.rebuilt, 0);
        assert_eq!(result.failed, 0);
        assert!(result.attested);
        assert!(result.confirm_proposed);
        assert_eq!(net.marked_local.lock().unwrap().len(), 30);
        assert_eq!(
            *net.submitted.lock().unwrap(),
            vec![policy::SELF_CHECK_FN, policy::ATTEST_FN, CONFIRM_TX_FN],
            "attestation lands before the confirm proposal"
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
        });
        let result = pull_owed(&seams(net2.clone()), &dir_dst2, &blob_id)
            .await
            .unwrap();
        assert_eq!(result.pulled, 30);
        assert!(!result.confirm_proposed);
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
        });
        let result = pull_owed(&seams(net3), &dir_dst2, &blob_id).await.unwrap();
        assert_eq!(result, PullOutcome::default());

        let _ = std::fs::remove_dir_all(&base);
    }

    // Should: submit nothing for a re-goaled blob whose classes this node
    // already holds — no self-check, no attestation, no confirm proposal;
    // the sweep owns its rows and the fulfillment floor its confirmation.
    // Should: still attest and propose for a never-confirmed blob this
    // node holds (the origin at birth), so uploads confirm promptly.
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
        });
        let result = pull_owed(&seams(regoal.clone()), &dir, &blob_id)
            .await
            .unwrap();
        assert_eq!(result, PullOutcome::default(), "nothing owed, nothing done");
        assert!(
            regoal.submitted.lock().unwrap().is_empty(),
            "a rubber stamp buys no consensus round on the worker"
        );

        // Birth: never confirmed, held here — the origin's prompt evidence.
        let birth = Arc::new(PullNet {
            served: Mutex::new(HashMap::new()),
            manifest: Mutex::new(Some(manifest)),
            target: Some(all_mine(9)),
            ready: Some(9),
            marked_local: Mutex::new(Vec::new()),
            submitted: Mutex::new(Vec::new()),
        });
        let result = pull_owed(&seams(birth.clone()), &dir, &blob_id)
            .await
            .unwrap();
        assert_eq!(result.owed, 0);
        assert!(result.attested);
        assert!(result.confirm_proposed);
        // No pull → the mock's self-check differential is empty and is
        // skipped; disk truth and the proposal still go out.
        assert_eq!(
            *birth.submitted.lock().unwrap(),
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
        });
        for f in outcome.fragments.iter().filter(|f| !f.recovery) {
            let data = fragstore::read_fragment(&dir_src, &f.fragment_hash).unwrap();
            net.served.lock().unwrap().insert(f.fragment_hash, data);
        }
        let result = pull_owed(&seams(net.clone()), &dir_dst, &blob_id)
            .await
            .unwrap();
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
        });
        let result = pull_owed(&seams(dead.clone()), &dir_dst2, &blob_id)
            .await
            .unwrap();
        assert_eq!(result.failed, 30);
        assert!(
            dead.submitted.lock().unwrap().is_empty(),
            "nothing to attest"
        );

        let _ = std::fs::remove_dir_all(&base);
    }
}
