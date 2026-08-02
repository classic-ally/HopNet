//! Epoch join (RFC-019 S7): the ONLINE half of a straggler's rejoin.
//!
//! A node that slept through a regenesis wakes with a sealed epoch's
//! history and peers that refuse it — the structured epoch-mismatch
//! refusal is its signpost here. This module fetches the lineage chain
//! and the boundary snapshot from peers, verifies both against the
//! node's own last-trusted state (never against the server), STAGES the
//! verified inputs beside the database, and requests a restart. The
//! rebuild itself belongs to the boot path: swapping the live database
//! is only safe before the pool opens, and the machinery to do it —
//! certified import, node-local carry, atomic swap, rollback window —
//! already exists there from S6.
//!
//! Nothing here ever touches the live database. The worst outcome of a
//! lying peer is a wasted download: no staged input is acted on until
//! the boot path re-verifies it.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use hopnet_comms::{BoxFuture, PeerRef, Rpc};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::net::{decode_payload, encode_payload};
use crate::regenesis::genesis::{self, ChainAnchor, LineageRecord};
use crate::regenesis::rpc::{RegenesisNetRequest, RegenesisNetResponse, SNAPSHOT_CHUNK_MAX};

/// Staging lives beside the database, like the awaiting-upgrade marker.
pub const JOIN_STAGING_DIR: &str = "join-staging";

/// Written LAST: its presence means every other staged file is complete
/// and verified. Anything else on disk is a resumable partial.
const MANIFEST_FILENAME: &str = "manifest.bin";
const SNAPSHOT_FILENAME: &str = "snapshot.bin";
const SNAPSHOT_PARTIAL_FILENAME: &str = "snapshot.bin.partial";

/// Per-request timeouts. Lineage records are small; a snapshot chunk is
/// up to 4MiB and may cross a slow link.
const INFO_TIMEOUT: Duration = Duration::from_secs(10);
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30);

/// What the boot path needs to know about a completed staging, and the
/// completion marker itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedJoinManifest {
    pub format_version: u16,
    /// The epoch this node was on when it staged (the anchor's epoch).
    pub from_epoch: u64,
    pub target_epoch: u64,
    /// Contiguous `from_epoch + 1 ..= target_epoch`.
    pub lineage_epochs: Vec<u64>,
    pub snapshot_hash: [u8; 32],
    pub snapshot_len: u64,
    /// Operator re-trust: the boot path skips the OVERLAP check only.
    /// Chain-id linkage, per-hop quorum, and the snapshot hash are still
    /// enforced — this waives weak subjectivity, not verification.
    pub manual_anchor: bool,
}

/// One request/response against a peer's "regenesis" scope. Abstracted
/// so the fetch, resume, and verification logic is exercised without a
/// mesh — the wire hop itself is covered by the in-process mesh tests.
pub trait JoinTransport: Send + Sync {
    fn request(
        &self,
        peer: PeerRef,
        req: RegenesisNetRequest,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<RegenesisNetResponse, String>>;
}

/// Production transport: the "regenesis" comms scope.
pub struct CommsTransport {
    pub comms: hopnet_comms::IrohComms,
}

impl JoinTransport for CommsTransport {
    fn request(
        &self,
        peer: PeerRef,
        req: RegenesisNetRequest,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<RegenesisNetResponse, String>> {
        Box::pin(async move {
            let reply = self
                .comms
                .rpc(&peer, "regenesis", encode_payload(&req), timeout)
                .await
                .map_err(|e| e.to_string())?;
            decode_payload(&reply).map_err(|e| e.to_string())
        })
    }
}

/// Where the trust root comes from.
pub enum JoinAnchor {
    /// This node's own last-trusted state (the ordinary straggler).
    OwnDb,
    /// An operator named a peer they trust: fetch from it alone and
    /// waive the overlap requirement (the join ceremony, re-invoked).
    Manual { peer: PeerRef },
}

/// Progress/last-error for the status surface — latest wins, exactly
/// like the boot path's boundary error.
static JOIN_STATE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

pub fn join_state() -> Option<String> {
    JOIN_STATE.lock().ok().and_then(|g| g.clone())
}

fn set_state(s: impl Into<String>) {
    let s = s.into();
    tracing::info!("epoch join: {s}");
    if let Ok(mut slot) = JOIN_STATE.lock() {
        *slot = Some(s);
    }
}

pub fn staging_path(db_path: &str) -> PathBuf {
    Path::new(db_path)
        .parent()
        .map(|p| p.join(JOIN_STAGING_DIR))
        .unwrap_or_else(|| PathBuf::from(JOIN_STAGING_DIR))
}

pub fn manifest_path(staging: &Path) -> PathBuf {
    staging.join(MANIFEST_FILENAME)
}

pub fn staged_snapshot_path(staging: &Path) -> PathBuf {
    staging.join(SNAPSHOT_FILENAME)
}

fn staged_partial_path(staging: &Path) -> PathBuf {
    staging.join(SNAPSHOT_PARTIAL_FILENAME)
}

fn staged_lineage_path(staging: &Path, epoch: u64) -> PathBuf {
    staging.join(format!("epoch-{epoch}.bin"))
}

/// Read a COMPLETE staging's manifest, if one is present.
pub fn read_manifest(staging: &Path) -> Option<StagedJoinManifest> {
    let bytes = std::fs::read(manifest_path(staging)).ok()?;
    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .ok()
        .map(|(m, _)| m)
}

/// Read the staged lineage records named by a manifest, in order.
pub fn read_staged_lineage(
    staging: &Path,
    manifest: &StagedJoinManifest,
) -> Result<Vec<LineageRecord>, String> {
    manifest
        .lineage_epochs
        .iter()
        .map(|&epoch| {
            let path = staged_lineage_path(staging, epoch);
            let bytes = std::fs::read(&path)
                .map_err(|e| format!("staged lineage epoch {epoch}: {e}"))?;
            genesis::decode_lineage(&bytes)
        })
        .collect()
}

pub fn clear_staging(staging: &Path) {
    if staging.exists() && let Err(e) = std::fs::remove_dir_all(staging) {
        tracing::warn!(path = %staging.display(), "join staging cleanup failed: {e}");
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&tmp, bytes).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename to {}: {e}", path.display()))
}

/// The whole online side: fetch, verify, stage, request restart. Never
/// touches the live database.
///
/// `Ok(())` means staging is complete — either the restart was signalled
/// or, on a version mismatch, the awaiting-upgrade marker was written and
/// the staged inputs wait for the upgraded binary (the boot path's
/// version gate runs before anything schema-touching, so the old binary
/// never builds the new epoch's database).
pub async fn run_epoch_join(
    app_state: &AppState,
    db_path: &str,
    anchor: JoinAnchor,
    peers: Vec<PeerRef>,
) -> Result<(), String> {
    let transport = CommsTransport {
        comms: app_state.comms.clone(),
    };
    run_epoch_join_with(
        app_state,
        db_path,
        anchor,
        peers,
        &transport,
        crate::version::effective_running_code(),
    )
    .await
}

/// `run_epoch_join` over an explicit transport, with `running_code`
/// injected by the caller — the same shape as `boot_transition`, so the
/// version gate is testable without touching process env.
pub async fn run_epoch_join_with(
    app_state: &AppState,
    db_path: &str,
    anchor: JoinAnchor,
    peers: Vec<PeerRef>,
    transport: &dyn JoinTransport,
    running_code: u32,
) -> Result<(), String> {
    let staging = staging_path(db_path);

    // Idempotent: a complete staging is already verified — re-signal and
    // return rather than refetching (a restart that raced the signal, or
    // a second trigger firing).
    if let Some(m) = read_manifest(&staging) {
        set_state(format!(
            "staged for epoch {} — awaiting restart",
            m.target_epoch
        ));
        app_state.restart_signal.notify_one();
        return Ok(());
    }
    if peers.is_empty() {
        return Err("no peers to join from".into());
    }

    let manual_anchor = matches!(anchor, JoinAnchor::Manual { .. });
    let chain_anchor = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| format!("db conn: {e}"))?;
        let mut a = ChainAnchor::from_db(&conn)?;
        if manual_anchor {
            // The operator vouches for the peer; weak subjectivity is
            // exactly what they are overriding.
            a.trusted = None;
        }
        a
    };
    let my_epoch = chain_anchor.epoch;

    set_state(format!("fetching lineage from epoch {}", my_epoch + 1));
    let records = fetch_lineage_chain(transport, &peers, my_epoch).await?;
    let target = genesis::verify_lineage_chain(&records, chain_anchor)?;
    let target_record = &target.record;
    set_state(format!(
        "verified lineage to epoch {} ({} hop(s))",
        target_record.epoch,
        records.len()
    ));

    // The version gate is the boot path's job (it must hold across a
    // crash too), but checking here keeps a mismatched node from
    // downloading a snapshot it cannot import.
    let required = target_record.required_version_code;
    let running = running_code;

    std::fs::create_dir_all(&staging).map_err(|e| format!("staging dir: {e}"))?;
    if running == required {
        let len = fetch_snapshot(
            transport,
            &peers,
            target_record.epoch,
            &target_record.snapshot_hash,
            &staging,
        )
        .await?;
        stage_manifest(&staging, &records, target_record, my_epoch, len, manual_anchor)?;
        set_state(format!(
            "staged for epoch {} — requesting restart",
            target_record.epoch
        ));
        app_state.restart_signal.notify_one();
    } else {
        // Stage the snapshot anyway: the download is the slow part and
        // the bytes are certified, so the upgraded binary boots straight
        // into the rebuild.
        let len = fetch_snapshot(
            transport,
            &peers,
            target_record.epoch,
            &target_record.snapshot_hash,
            &staging,
        )
        .await?;
        stage_manifest(&staging, &records, target_record, my_epoch, len, manual_anchor)?;
        set_state(format!(
            "staged for epoch {}, but it requires version {} (running {}) — awaiting upgrade",
            target_record.epoch,
            crate::version::format_code(required),
            crate::version::format_code(running),
        ));
        crate::regenesis::boot::write_awaiting_marker(db_path, required);
    }
    Ok(())
}

fn stage_manifest(
    staging: &Path,
    records: &[LineageRecord],
    target: &genesis::EpochGenesisRecord,
    from_epoch: u64,
    snapshot_len: u64,
    manual_anchor: bool,
) -> Result<(), String> {
    let mut lineage_epochs = Vec::with_capacity(records.len());
    for lr in records {
        let bytes = bincode::serde::encode_to_vec(lr, bincode::config::standard())
            .map_err(|e| format!("lineage encode: {e}"))?;
        write_atomic(&staged_lineage_path(staging, lr.record.epoch), &bytes)?;
        lineage_epochs.push(lr.record.epoch);
    }
    let manifest = StagedJoinManifest {
        format_version: 1,
        from_epoch,
        target_epoch: target.epoch,
        lineage_epochs,
        snapshot_hash: target.snapshot_hash,
        snapshot_len,
        manual_anchor,
    };
    let bytes = bincode::serde::encode_to_vec(&manifest, bincode::config::standard())
        .map_err(|e| format!("manifest encode: {e}"))?;
    // LAST — the completion marker.
    write_atomic(&manifest_path(staging), &bytes)
}

/// Fetch lineage records from `my_epoch + 1` up to whatever the mesh's
/// current epoch is, rotating peers on failure and looping batches until
/// a peer reports it has no more.
async fn fetch_lineage_chain(
    transport: &dyn JoinTransport,
    peers: &[PeerRef],
    my_epoch: u64,
) -> Result<Vec<LineageRecord>, String> {
    let mut records: Vec<LineageRecord> = Vec::new();
    let mut last_err = "no peer served lineage".to_string();

    'outer: loop {
        let next_epoch = my_epoch + 1 + records.len() as u64;
        let mut progressed = false;
        for peer in peers {
            match request(
                transport,
                *peer,
                RegenesisNetRequest::LineageFetch {
                    from_epoch: next_epoch,
                },
                INFO_TIMEOUT,
            )
            .await
            {
                Ok(RegenesisNetResponse::Lineage { records: raw }) => {
                    for bytes in &raw {
                        records.push(genesis::decode_lineage(bytes)?);
                    }
                    progressed = !raw.is_empty();
                    if progressed {
                        continue 'outer;
                    }
                }
                Ok(RegenesisNetResponse::NotAvailable { reason }) => {
                    // No record at next_epoch: this peer has nothing
                    // further. With records already in hand that is the
                    // end of the chain.
                    last_err = reason;
                    if !records.is_empty() {
                        break 'outer;
                    }
                }
                Ok(other) => last_err = format!("node {}: unexpected {other:?}", peer.node_id),
                Err(e) => last_err = format!("node {}: {e}", peer.node_id),
            }
        }
        if !progressed {
            break;
        }
    }

    if records.is_empty() {
        return Err(format!("lineage fetch: {last_err}"));
    }
    Ok(records)
}

/// Download the boundary artifact into staging and verify it against the
/// certified hash. Resumes from whatever the partial already holds —
/// across peer rotations AND across restarts.
async fn fetch_snapshot(
    transport: &dyn JoinTransport,
    peers: &[PeerRef],
    epoch: u64,
    expected: &[u8; 32],
    staging: &Path,
) -> Result<u64, String> {
    let partial = staged_partial_path(staging);
    let mut last_err = "no peer served the snapshot".to_string();

    for peer in peers {
        let total = match request(
            transport,
            *peer,
            RegenesisNetRequest::SnapshotInfo { epoch },
            INFO_TIMEOUT,
        )
        .await
        {
            Ok(RegenesisNetResponse::SnapshotInfo {
                total_len,
                snapshot_hash,
                ..
            }) => {
                if &snapshot_hash != expected {
                    last_err = format!(
                        "node {} offers a snapshot the lineage does not certify",
                        peer.node_id
                    );
                    continue;
                }
                total_len
            }
            Ok(RegenesisNetResponse::NotAvailable { reason }) => {
                last_err = format!("node {}: {reason}", peer.node_id);
                continue;
            }
            Ok(other) => {
                last_err = format!("node {}: unexpected {other:?}", peer.node_id);
                continue;
            }
            Err(e) => {
                last_err = format!("node {}: {e}", peer.node_id);
                continue;
            }
        };

        match download_from(transport, peer, epoch, total, &partial).await {
            Ok(()) => {}
            Err(e) => {
                last_err = format!("node {}: {e}", peer.node_id);
                continue;
            }
        }

        let bytes = std::fs::read(&partial).map_err(|e| format!("read staged snapshot: {e}"))?;
        if blake3::hash(&bytes).as_bytes() != expected {
            // Certified bytes did not materialize: discard and rotate —
            // never import what the lineage does not vouch for.
            let _ = std::fs::remove_file(&partial);
            last_err = format!("node {} served a snapshot failing its hash", peer.node_id);
            continue;
        }
        std::fs::rename(&partial, staged_snapshot_path(staging))
            .map_err(|e| format!("stage snapshot: {e}"))?;
        return Ok(bytes.len() as u64);
    }
    Err(format!("snapshot fetch: {last_err}"))
}

async fn download_from(
    transport: &dyn JoinTransport,
    peer: &PeerRef,
    epoch: u64,
    total: u64,
    partial: &Path,
) -> Result<(), String> {
    use std::io::Write as _;

    let mut have = std::fs::metadata(partial).map(|m| m.len()).unwrap_or(0);
    if have > total {
        // A shorter artifact than we already hold: the partial belongs to
        // a different attempt. Start over.
        let _ = std::fs::remove_file(partial);
        have = 0;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(partial)
        .map_err(|e| format!("open partial: {e}"))?;

    while have < total {
        let len = SNAPSHOT_CHUNK_MAX.min(total - have);
        let resp = request(
            transport,
            *peer,
            RegenesisNetRequest::SnapshotChunk {
                epoch,
                offset: have,
                len,
            },
            CHUNK_TIMEOUT,
        )
        .await?;
        let RegenesisNetResponse::SnapshotChunk { data } = resp else {
            return Err(format!("unexpected chunk response: {resp:?}"));
        };
        if data.is_empty() {
            return Err(format!("empty chunk at offset {have} of {total}"));
        }
        file.write_all(&data).map_err(|e| format!("append: {e}"))?;
        have += data.len() as u64;
        set_state(format!(
            "snapshot {}/{} KiB from node {}",
            have / 1024,
            total / 1024,
            peer.node_id
        ));
    }
    file.flush().map_err(|e| format!("flush: {e}"))?;
    Ok(())
}

async fn request(
    transport: &dyn JoinTransport,
    peer: PeerRef,
    req: RegenesisNetRequest,
    timeout: Duration,
) -> Result<RegenesisNetResponse, String> {
    transport.request(peer, req, timeout).await
}

/// Spawn an epoch join unless one is already inflight. The trigger seam
/// shared by the sync classification, the tip poll, and the probe pong —
/// all of which can observe an epoch-ahead peer concurrently.
pub fn spawn_epoch_join(app_state: &AppState, anchor: JoinAnchor, peers: Vec<PeerRef>) {
    if app_state
        .epoch_join_inflight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let app_state = app_state.clone();
    crate::consensus::queue::queue_rt().spawn(async move {
        let db_path = crate::db::shared::get_database_path();
        if let Err(e) = run_epoch_join(&app_state, &db_path, anchor, peers).await {
            set_state(format!("failed: {e}"));
            tracing::warn!("epoch join attempt failed: {e}");
        }
        app_state.epoch_join_inflight.store(false, Ordering::Release);
    });
}

/// The parked node's retry loop. A node parked by a boot gate has no
/// engine, so no tip poll and no probe scheduler ever run — this is its
/// ONLY path back to the mesh. Ends as soon as staging completes (the
/// restart follows), or when the boundary resolves some other way.
pub fn spawn_parked_epoch_join(app_state: &AppState) {
    const RETRY: Duration = Duration::from_secs(30);
    let app_state = app_state.clone();
    crate::consensus::queue::queue_rt().spawn(async move {
        loop {
            let db_path = crate::db::shared::get_database_path();
            if read_manifest(&staging_path(&db_path)).is_some() {
                app_state.restart_signal.notify_one();
                return;
            }
            let my_id = app_state.get_node_id().unwrap_or(-1);
            let peers =
                crate::consensus::malachite::sync::peer_list(&app_state.db_pool, my_id, None);
            if !peers.is_empty()
                && let Err(e) = run_epoch_join(&app_state, &db_path, JoinAnchor::OwnDb, peers).await
            {
                set_state(format!("failed: {e}"));
                tracing::warn!("parked epoch join attempt failed (retrying): {e}");
            }
            if read_manifest(&staging_path(&db_path)).is_some() {
                return;
            }
            tokio::time::sleep(RETRY).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::regenesis::boot::tests::{TARGET, sealed_db};
    use crate::regenesis::rpc::serve_request;

    /// Serve one request straight from a peer's database directory —
    /// the wire hop is exercised by the in-process mesh tests; here the
    /// client logic is what matters.
    fn served(db_path: &str, req: RegenesisNetRequest) -> RegenesisNetResponse {
        serve_request(db_path, req)
    }

    fn transitioned(dir: &Path) -> String {
        let db_path = sealed_db(dir);
        match crate::regenesis::boot::boot_transition(&db_path, TARGET) {
            crate::regenesis::boot::BootOutcome::Transitioned { epoch: 2 } => {}
            other => panic!("fixture transition failed: {other:?}"),
        }
        db_path
    }

    // Impact: the manifest IS the completion marker — if it could be
    // read while a staged file is missing, the boot path would build
    // from an incomplete download.
    // Should: round-trip a manifest and the lineage records it names.
    #[test]
    fn manifest_round_trips_with_its_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let server = transitioned(dir.path());
        let raw = match served(&server, RegenesisNetRequest::LineageFetch { from_epoch: 2 }) {
            RegenesisNetResponse::Lineage { records } => records,
            other => panic!("unexpected: {other:?}"),
        };
        let records: Vec<LineageRecord> = raw
            .iter()
            .map(|b| genesis::decode_lineage(b).unwrap())
            .collect();

        let staging = dir.path().join("staging-under-test");
        std::fs::create_dir_all(&staging).unwrap();
        stage_manifest(&staging, &records, &records[0].record, 1, 4096, false).unwrap();

        let manifest = read_manifest(&staging).expect("manifest readable");
        assert_eq!(manifest.target_epoch, 2);
        assert_eq!(manifest.lineage_epochs, vec![2]);
        assert_eq!(manifest.snapshot_hash, records[0].record.snapshot_hash);
        assert!(!manifest.manual_anchor);

        let back = read_staged_lineage(&staging, &manifest).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].record, records[0].record);
    }

    // Should: report no manifest while staging holds only partials, and
    // report one once staging completes.
    // Should not: treat a staging directory with a missing lineage file
    // as usable.
    #[test]
    fn incomplete_staging_is_not_complete() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join(JOIN_STAGING_DIR);
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staged_partial_path(&staging), b"half a snapshot").unwrap();
        assert!(read_manifest(&staging).is_none());

        let manifest = StagedJoinManifest {
            format_version: 1,
            from_epoch: 1,
            target_epoch: 2,
            lineage_epochs: vec![2],
            snapshot_hash: [0; 32],
            snapshot_len: 0,
            manual_anchor: false,
        };
        let bytes =
            bincode::serde::encode_to_vec(&manifest, bincode::config::standard()).unwrap();
        write_atomic(&manifest_path(&staging), &bytes).unwrap();
        let read = read_manifest(&staging).expect("manifest present");
        // The named lineage file was never staged: reading it must fail
        // rather than silently yield a short chain.
        assert!(read_staged_lineage(&staging, &read).is_err());
    }

    // Should: place staging beside the database, next to the other
    // boundary files.
    #[test]
    fn staging_sits_beside_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("database.db").to_string_lossy().into_owned();
        assert_eq!(staging_path(&db_path), dir.path().join(JOIN_STAGING_DIR));
    }

    // ------------------------------------------------------------------
    // The fetch/verify/stage path, driven over a local transport: one
    // "peer" per database directory, answering from the real scope
    // handler. The wire hop is covered by the in-process mesh tests.
    // ------------------------------------------------------------------

    /// Serves from a per-peer database, optionally truncating snapshot
    /// chunks (a slow or dying link) or corrupting them (a lying peer).
    struct LocalTransport {
        peers: Vec<(i32, String)>,
        /// Serve at most this many bytes per chunk, forcing resumes.
        chunk_cap: Option<u64>,
        /// Peers that corrupt every chunk they serve.
        liars: Vec<i32>,
        calls: std::sync::Mutex<Vec<(i32, String)>>,
    }

    impl LocalTransport {
        fn new(peers: Vec<(i32, String)>) -> Self {
            LocalTransport {
                peers,
                chunk_cap: None,
                liars: Vec::new(),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn db_of(&self, node_id: i32) -> Option<&str> {
            self.peers
                .iter()
                .find(|(id, _)| *id == node_id)
                .map(|(_, p)| p.as_str())
        }

        fn chunk_calls(&self) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, kind)| kind == "chunk")
                .count()
        }
    }

    impl JoinTransport for LocalTransport {
        fn request(
            &self,
            peer: PeerRef,
            req: RegenesisNetRequest,
            _timeout: Duration,
        ) -> BoxFuture<'_, Result<RegenesisNetResponse, String>> {
            let kind = match &req {
                RegenesisNetRequest::EpochInfo => "epoch",
                RegenesisNetRequest::LineageFetch { .. } => "lineage",
                RegenesisNetRequest::SnapshotInfo { .. } => "info",
                RegenesisNetRequest::SnapshotChunk { .. } => "chunk",
            };
            self.calls
                .lock()
                .unwrap()
                .push((peer.node_id, kind.to_string()));

            let Some(db) = self.db_of(peer.node_id) else {
                return Box::pin(async { Err("unreachable peer".to_string()) });
            };
            // Cap the requested length to force multi-chunk downloads.
            let req = match (req, self.chunk_cap) {
                (
                    RegenesisNetRequest::SnapshotChunk { epoch, offset, len },
                    Some(cap),
                ) => RegenesisNetRequest::SnapshotChunk {
                    epoch,
                    offset,
                    len: len.min(cap),
                },
                (other, _) => other,
            };
            let lying = self.liars.contains(&peer.node_id);
            let resp = served(db, req);
            let resp = match (resp, lying) {
                (RegenesisNetResponse::SnapshotChunk { mut data }, true) => {
                    for b in data.iter_mut() {
                        *b ^= 0xFF;
                    }
                    RegenesisNetResponse::SnapshotChunk { data }
                }
                (other, _) => other,
            };
            Box::pin(async move { Ok(resp) })
        }
    }

    /// A straggler: the S6 sealed fixture left UNTRANSITIONED, so it is
    /// still on epoch 1 with the seated set it last trusted.
    fn straggler(dir: &Path) -> (AppState, String) {
        let db_path = sealed_db(dir);
        // Clear the seal so the node looks like an ordinary epoch-1 node
        // that simply slept through the boundary.
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            crate::db::shared::apply_connection_pragmas(&conn).unwrap();
            conn.execute("DELETE FROM regenesis_state", []).unwrap();
            conn.execute(
                "DELETE FROM consensus_meta WHERE key = ?",
                [crate::regenesis::seal::META_SEALED_AT],
            )
            .unwrap();
        }
        let signing = crate::db::PrivKey(ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]));
        let verifying = crate::db::PubKey(signing.0.verifying_key());
        let app_state = crate::consensus::tests::create_test_app_state_file_backed(
            signing, verifying, &db_path,
        );
        (app_state, db_path)
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn peer(node_id: i32) -> PeerRef {
        PeerRef {
            node_id,
            pubkey: [0u8; 32],
        }
    }

    // Impact: this is the straggler's whole online half — everything the
    // boot path later acts on is produced here, and the manifest is what
    // tells it the download finished.
    // Should: fetch and verify the lineage chain, download the certified
    // snapshot, stage both with the manifest last, and request a restart.
    #[test]
    fn stages_a_verified_join_and_requests_restart() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());
        let client_dir = tempfile::tempdir().unwrap();
        let (app_state, db_path) = straggler(client_dir.path());

        let transport = LocalTransport::new(vec![(2, server.clone())]);
        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2)],
            &transport,
            TARGET,
        ))
        .expect("join stages");

        let staging = staging_path(&db_path);
        let manifest = read_manifest(&staging).expect("staging completed");
        assert_eq!(manifest.target_epoch, 2);
        assert_eq!(manifest.from_epoch, 1);
        assert!(!manifest.manual_anchor);

        let snapshot = std::fs::read(staged_snapshot_path(&staging)).unwrap();
        assert_eq!(blake3::hash(&snapshot).as_bytes(), &manifest.snapshot_hash);
        assert_eq!(snapshot.len() as u64, manifest.snapshot_len);
        let records = read_staged_lineage(&staging, &manifest).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.epoch, 2);

        // The restart was requested: Notify holds one permit, so a
        // listener registered after the fact still completes.
        rt().block_on(async {
            tokio::time::timeout(Duration::from_millis(50), app_state.restart_signal.notified())
                .await
                .expect("restart requested");
        });
    }

    // Impact: a snapshot can be large and links die mid-download —
    // restarting from zero every attempt would strand a straggler on a
    // bad link forever.
    // Should: resume a partial download from the bytes already on disk.
    #[test]
    fn snapshot_download_resumes_from_a_partial() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());
        let client_dir = tempfile::tempdir().unwrap();
        let (app_state, db_path) = straggler(client_dir.path());

        // Pre-seed a partial with the first 512 bytes of the real
        // artifact, exactly as an interrupted attempt would leave it.
        let staging = staging_path(&db_path);
        std::fs::create_dir_all(&staging).unwrap();
        let full = {
            match served(&server, RegenesisNetRequest::SnapshotInfo { epoch: 2 }) {
                RegenesisNetResponse::SnapshotInfo { .. } => {}
                other => panic!("unexpected: {other:?}"),
            }
            std::fs::read(server_dir.path().join(crate::regenesis::seal::SEAL_ARTIFACT_FILENAME))
                .unwrap()
        };
        std::fs::write(staged_partial_path(&staging), &full[..512]).unwrap();

        let mut transport = LocalTransport::new(vec![(2, server.clone())]);
        transport.chunk_cap = Some(256);
        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2)],
            &transport,
            TARGET,
        ))
        .expect("join stages");

        let staged = std::fs::read(staged_snapshot_path(&staging)).unwrap();
        assert_eq!(staged, full, "resumed download reassembles exactly");
        // 256-byte chunks over the REMAINDER only: the pre-seeded prefix
        // was never refetched.
        let expected_chunks = (full.len() - 512).div_ceil(256);
        assert_eq!(transport.chunk_calls(), expected_chunks);
    }

    // Impact: the certified hash is the only thing standing between a
    // lying peer and an imported forgery.
    // Should: discard a snapshot failing its certified hash and rotate
    // to another peer.
    // Should not: stage anything from the lying peer.
    #[test]
    fn corrupt_snapshot_is_discarded_and_the_peer_rotated() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());
        let client_dir = tempfile::tempdir().unwrap();
        let (app_state, db_path) = straggler(client_dir.path());

        // Peer 2 lies, peer 3 serves the same honest database.
        let mut transport = LocalTransport::new(vec![(2, server.clone()), (3, server.clone())]);
        transport.liars = vec![2];
        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2), peer(3)],
            &transport,
            TARGET,
        ))
        .expect("the honest peer completes the join");

        let staging = staging_path(&db_path);
        let manifest = read_manifest(&staging).expect("staging completed");
        let snapshot = std::fs::read(staged_snapshot_path(&staging)).unwrap();
        assert_eq!(blake3::hash(&snapshot).as_bytes(), &manifest.snapshot_hash);
    }

    // Should: refuse when no peer can serve a verifying chain.
    // Should not: leave a manifest behind after a failed attempt.
    #[test]
    fn a_failed_join_stages_nothing() {
        let client_dir = tempfile::tempdir().unwrap();
        let (app_state, db_path) = straggler(client_dir.path());

        // The only "peer" is an epoch-1 node with no lineage at all.
        let peer_dir = tempfile::tempdir().unwrap();
        let flat = sealed_db(peer_dir.path());
        let transport = LocalTransport::new(vec![(2, flat)]);
        let err = rt()
            .block_on(run_epoch_join_with(
                &app_state,
                &db_path,
                JoinAnchor::OwnDb,
                vec![peer(2)],
                &transport,
                TARGET,
            ))
            .expect_err("no lineage to join");
        assert!(err.contains("lineage"), "{err}");
        assert!(read_manifest(&staging_path(&db_path)).is_none());
    }

    // Impact: a second trigger firing (probe pong plus sync
    // classification) must not restart a completed download.
    // Should: re-signal the restart and return without refetching when
    // staging is already complete.
    #[test]
    fn complete_staging_is_idempotent() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());
        let client_dir = tempfile::tempdir().unwrap();
        let (app_state, db_path) = straggler(client_dir.path());

        let transport = LocalTransport::new(vec![(2, server.clone())]);
        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2)],
            &transport,
            TARGET,
        ))
        .unwrap();
        let first_calls = transport.calls.lock().unwrap().len();

        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2)],
            &transport,
            TARGET,
        ))
        .unwrap();
        assert_eq!(
            transport.calls.lock().unwrap().len(),
            first_calls,
            "a complete staging refetches nothing"
        );
    }

    // Impact: an upgrade-epoch straggler must not be told to restart
    // into a binary that cannot build the epoch — but the download is
    // the slow part, so it is kept for the upgraded binary to boot into.
    // Should: stage the verified inputs and write the awaiting-upgrade
    // marker when the target epoch requires another version.
    // Should not: request a restart.
    #[test]
    fn version_mismatch_stages_and_parks() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());
        let client_dir = tempfile::tempdir().unwrap();
        let (app_state, db_path) = straggler(client_dir.path());

        let transport = LocalTransport::new(vec![(2, server.clone())]);
        // Run as a binary that is NOT the epoch's required version.
        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2)],
            &transport,
            TARGET + 100,
        ))
        .unwrap();

        let staging = staging_path(&db_path);
        let manifest = read_manifest(&staging).expect("inputs still staged for the upgrade");
        let snapshot = std::fs::read(staged_snapshot_path(&staging)).unwrap();
        assert_eq!(blake3::hash(&snapshot).as_bytes(), &manifest.snapshot_hash);

        let marker = crate::regenesis::boot::awaiting_upgrade_path(&db_path);
        assert_eq!(
            std::fs::read_to_string(&marker).unwrap(),
            crate::version::format_code(TARGET),
            "the marker names the version the epoch requires"
        );

        // No restart was requested — restarting into this binary would
        // only park again at the boot path's version gate.
        rt().block_on(async {
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(50),
                    app_state.restart_signal.notified()
                )
                .await
                .is_err(),
                "a version-mismatched node must not request a restart"
            );
        });
    }
}
