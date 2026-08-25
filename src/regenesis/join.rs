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
    /// The version the target epoch requires. Carried here so the boot
    /// path can run its version gate before reading, verifying, or
    /// importing anything — an old binary must never build the new
    /// epoch's database.
    pub required_version_code: u32,
    pub snapshot_hash: [u8; 32],
    pub snapshot_len: u64,
    /// Operator re-trust: the epoch identity the operator named out of
    /// band. `Some` swaps the OVERLAP check for a fingerprint match on the
    /// epoch actually reached; chain-id linkage, per-hop quorum and the
    /// snapshot hash are enforced either way.
    ///
    /// Persisted rather than recomputed because the boot path re-verifies
    /// the staged chain from scratch after a restart, and must reach the
    /// same verdict — dropping the anchor to "trust anything" there would
    /// reopen the hole the online side just closed.
    pub manual_fingerprint: Option<[u8; 32]>,
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
            // Plain rpc is enough while generations 0 and 1 are
            // byte-identical for this scope (the compat_g0 equality
            // goldens); the first divergent mint moves this to
            // rpc_negotiated and decodes per generation, like
            // status_probe.
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
    /// An operator named a peer AND the epoch identity they expect: fetch
    /// from that peer alone, and root the chain in the fingerprint instead
    /// of the overlap rule (the join ceremony, re-invoked).
    ///
    /// The fingerprint is required, not optional. Without it the record's
    /// own declared validator set would be verifying the record's own
    /// certificate, so any peer could serve a wholly fabricated epoch and
    /// have it accepted — the overlap rule was the only thing making a
    /// peer-supplied record trustworthy.
    Manual {
        peer: PeerRef,
        /// Expected chain id of the target epoch, supplied out of band.
        expect_chain_id: [u8; 32],
    },
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
            let bytes =
                std::fs::read(&path).map_err(|e| format!("staged lineage epoch {epoch}: {e}"))?;
            genesis::decode_lineage(&bytes)
        })
        .collect()
}

pub fn clear_staging(staging: &Path) {
    if staging.exists()
        && let Err(e) = std::fs::remove_dir_all(staging)
    {
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

    // Idempotent: a complete staging is already verified, so re-signal
    // rather than refetching (a restart that raced the signal, or a
    // second trigger firing — several can observe the same epoch-ahead
    // peer at once).
    //
    // The version gate applies here EXACTLY as it does on the fetch
    // path. Signalling unconditionally would restart a node into a
    // binary that can only park again: the boot path's staged-join gate
    // refuses the epoch, so the node exits, boots, parks, and is briefly
    // unreachable for nothing.
    if let Some(m) = read_manifest(&staging) {
        if running_code == m.required_version_code {
            set_state(format!(
                "staged for epoch {} — awaiting restart",
                m.target_epoch
            ));
            app_state.restart_signal.notify_one();
        } else {
            set_state(format!(
                "staged for epoch {}, but it requires version {} (running {}) — awaiting upgrade",
                m.target_epoch,
                crate::version::format_code(m.required_version_code),
                crate::version::format_code(running_code),
            ));
            crate::regenesis::boot::write_awaiting_marker(db_path, m.required_version_code);
        }
        return Ok(());
    }
    if peers.is_empty() {
        return Err("no peers to join from".into());
    }

    let manual_fingerprint = match anchor {
        JoinAnchor::Manual {
            expect_chain_id, ..
        } => Some(expect_chain_id),
        JoinAnchor::OwnDb => None,
    };
    let chain_anchor = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| format!("db conn: {e}"))?;
        let mut a = ChainAnchor::from_db(&conn)?;
        if let Some(fp) = manual_fingerprint {
            // The operator is overriding weak subjectivity, so their
            // fingerprint takes its place as the root. NOT a waiver: the
            // chain must terminate in exactly this identity.
            a.trust = genesis::ChainTrust::Fingerprint(fp);
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
        stage_manifest(
            &staging,
            &records,
            target_record,
            my_epoch,
            len,
            manual_fingerprint,
        )?;
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
        stage_manifest(
            &staging,
            &records,
            target_record,
            my_epoch,
            len,
            manual_fingerprint,
        )?;
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
    manual_fingerprint: Option<[u8; 32]>,
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
        required_version_code: target.required_version_code,
        snapshot_hash: target.snapshot_hash,
        snapshot_len,
        manual_fingerprint,
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

/// A brand-new node joining a mesh that is already past epoch 1
/// (RFC-019 S7): epoch join SUBSUMES the height-0 bootstrap.
///
/// Unlike a straggler this runs IN PROCESS, with no restart. There is no
/// file to swap and no pool-visible state to invalidate: a joining
/// database holds exactly one row (`this_node`), and the import refuses
/// outright unless every exported table is empty, so the precondition is
/// machine-checked rather than assumed.
///
/// Trust is rooted in the join ceremony itself — the same TOFU the
/// original height-0 bootstrap uses — and from there the FULL lineage
/// chain is verified, so the joiner ends up with every record it needs
/// to answer the next straggler.
pub async fn epoch_join_bootstrap(
    app_state: &AppState,
    data_dir: &Path,
    target_epoch: u64,
    peers: &[PeerRef],
    running_code: u32,
) -> Result<(), crate::consensus::malachite::engine::JoinError> {
    let transport = CommsTransport {
        comms: app_state.comms.clone(),
    };
    epoch_join_bootstrap_with(
        app_state,
        data_dir,
        target_epoch,
        peers,
        running_code,
        &transport,
    )
    .await
}

/// Genesis + fresh consensus meta for an in-process join — one shape
/// for the direct and scratch paths.
fn install_join_genesis(
    tx: &rusqlite::Connection,
    block: &hopnet_consensus::types::Block,
    cert: &hopnet_consensus::codec::WireCommitCertificate,
    profile: &hopnet_consensus::config::QuorumProfile,
    record: &genesis::EpochGenesisRecord,
) -> Result<(), String> {
    // Belt-and-braces (RFC-020 S6): the fresh in-process path never
    // wipes consensus_meta wholesale (unlike the epoch build's prune),
    // and a join lands verified state — any dissent record is stale.
    // Unreachable by a dissenter today (this path refuses non-empty
    // exported tables), but cheap; the clear_seal_state precedent.
    tx.execute(
        "DELETE FROM consensus_meta WHERE key = ?1",
        [crate::regenesis::seal::META_DISSENT_AT],
    )
    .map_err(|e| format!("dissent clear: {e}"))?;
    hopnet_consensus::store::install_genesis(tx, block, cert)
        .map_err(|e| format!("install genesis: {e}"))?;
    hopnet_consensus::store::meta_put(
        tx,
        hopnet_consensus::store::META_CHAIN_ID,
        block.block_hash.as_bytes().as_slice(),
    )
    .map_err(|e| format!("chain id: {e}"))?;
    hopnet_consensus::store::meta_put(
        tx,
        hopnet_consensus::store::META_QUORUM_PROFILE,
        profile.as_str().as_bytes(),
    )
    .map_err(|e| format!("quorum profile: {e}"))?;
    hopnet_consensus::store::meta_put(tx, genesis::META_EPOCH, &record.epoch.to_be_bytes())
        .map_err(|e| format!("epoch: {e}"))?;
    hopnet_consensus::store::meta_put(
        tx,
        genesis::META_EPOCH_GENESIS_HEIGHT,
        &record.seal_height.to_be_bytes(),
    )
    .map_err(|e| format!("epoch genesis height: {e}"))?;
    Ok(())
}

/// `epoch_join_bootstrap` over an explicit transport.
pub async fn epoch_join_bootstrap_with(
    app_state: &AppState,
    data_dir: &Path,
    target_epoch: u64,
    peers: &[PeerRef],
    running_code: u32,
    transport: &dyn JoinTransport,
) -> Result<(), crate::consensus::malachite::engine::JoinError> {
    set_state(format!("joining epoch {target_epoch} from scratch"));

    // The whole chain from the first boundary: cheap (records are bytes),
    // and it leaves this node able to serve any straggler afterwards.
    let records = fetch_lineage_chain(transport, peers, 1).await?;

    // Install-time anchor check (RFC-025 S5): the first record's
    // back-pointer IS the epoch-1 chain id — the same field mesh_magic
    // later derives the boot magic from, so agreement with the entered
    // code holds by construction. Defense against a lying coordinator;
    // gated on a code being present (stragglers never entered one).
    if let Some(entered) = app_state.entered_join_code.get() {
        let installed = records[0].record.prev_chain_id;
        if installed[..4] != entered[..] {
            return Err(
                crate::consensus::malachite::engine::JoinError::AnchorMismatch {
                    installed,
                    entered: *entered,
                },
            );
        }
    }

    let target = genesis::verify_lineage_chain(&records, ChainAnchor::tofu(&records[0]))?;
    let record = &target.record;
    if record.epoch != target_epoch {
        return Err(format!(
            "mesh offered epoch {} but the coordinator said {target_epoch}",
            record.epoch
        )
        .into());
    }
    if running_code != record.required_version_code {
        return Err(format!(
            "epoch {} requires version {} (running {})",
            record.epoch,
            crate::version::format_code(record.required_version_code),
            crate::version::format_code(running_code),
        )
        .into());
    }

    // Reuse the straggler's staging for the download: resumable, and
    // cleaned up once imported.
    let staging = data_dir.join(JOIN_STAGING_DIR);
    std::fs::create_dir_all(&staging).map_err(|e| format!("staging dir: {e}"))?;
    fetch_snapshot(
        transport,
        peers,
        record.epoch,
        &record.snapshot_hash,
        &staging,
    )
    .await?;
    let artifact = std::fs::read(staged_snapshot_path(&staging))
        .map_err(|e| format!("staged snapshot: {e}"))?;

    let block = genesis::genesis_block_for(record)?;
    let cert = genesis::synthetic_genesis_cert(&block);
    let profile = hopnet_consensus::config::QuorumProfile::parse(&record.quorum_profile)
        .ok_or_else(|| format!("unknown quorum profile {:?}", record.quorum_profile))?;

    // The artifact's shape decides the path (RFC-020 S5): an artifact
    // at every module's head imports directly (the common case — every
    // epoch not following a schema migration); anything older-shaped
    // (the pre-split cutover artifact, or back-ordinal sections after
    // a migration boundary) is built and verified at ITS shape in a
    // scratch database and transplanted — all in-process, no restart.
    let headers = hopnet_common::snapshot::read_section_headers(&artifact)
        .map_err(|e| format!("artifact headers: {e}"))?;
    let plan = crate::db::snapshot::resolve_import_plan(&headers)
        .map_err(|e| format!("import plan: {e}"))?;
    let at_head = !plan.pre_split
        && crate::db::chains::chains().iter().all(|c| {
            plan.targets
                .get(c.module)
                .is_none_or(|target| *target == c.head())
        });

    {
        let mut conn = app_state.db_pool.get().map_err(|e| e.to_string())?;
        if at_head {
            // ONE transaction, mirroring the height-0 bootstrap's shape.
            let tx = conn.transaction().map_err(|e| format!("tx: {e}"))?;
            let report = crate::db::snapshot::import_snapshot_tx(&tx, &artifact)
                .map_err(|e| format!("import: {e}"))?;
            if !report.skipped.is_empty() {
                // Same rule as the boot rebuild: a skipped section means
                // this binary and the artifact disagree about the schema,
                // and a partial import is not a state anyone can verify.
                return Err(format!(
                    "snapshot import skipped sections {:?} — refusing a partial epoch join",
                    report.skipped
                )
                .into());
            }
            install_join_genesis(&tx, &block, &cert, &profile, record)?;
            crate::db::shared::commit_timed(tx).map_err(|e| format!("join commit: {e}"))?;
        } else {
            if plan.pre_split {
                tracing::info!(
                    "fresh join consuming a pre-split (cutover) artifact via the host@3 mapping"
                );
            }
            let scratch_path = staging.join("scratch.db");
            let _ = std::fs::remove_file(&scratch_path);
            {
                let sconn = rusqlite::Connection::open(&scratch_path)
                    .map_err(|e| format!("scratch open: {e}"))?;
                crate::db::shared::apply_connection_pragmas(&sconn)
                    .map_err(|e| format!("scratch pragmas: {e}"))?;
                crate::db::chains::build_artifact_db(
                    &sconn,
                    &plan,
                    &artifact,
                    &record.snapshot_hash,
                )
                .map_err(|e| format!("artifact build: {e}"))?;
                sconn
                    .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .map_err(|e| format!("scratch checkpoint: {e}"))?;
            }
            conn.execute(
                "ATTACH DATABASE ?1 AS scratch",
                [scratch_path.to_string_lossy()],
            )
            .map_err(|e| format!("attach scratch: {e}"))?;
            // FK enforcement off for the whole-table replacement (the
            // SQLite-recommended shape; node-local rows reference the
            // rows being replaced); the explicit foreign_key_check is
            // the integrity gate.
            conn.execute_batch("PRAGMA foreign_keys = OFF;")
                .map_err(|e| format!("fk off: {e}"))?;
            let spliced = (|| -> Result<(), String> {
                let tx = conn.transaction().map_err(|e| format!("tx: {e}"))?;
                crate::db::chains::transplant_from_scratch(&tx, &plan)
                    .map_err(|e| format!("transplant: {e}"))?;
                install_join_genesis(&tx, &block, &cert, &profile, record)?;
                crate::db::chains::assert_fk_clean(&tx).map_err(|e| e.to_string())?;
                crate::db::shared::commit_timed(tx).map_err(|e| format!("join commit: {e}"))
            })();
            // The pooled connection outlives this join: ALWAYS restore
            // enforcement and detach, success or not.
            let _ = conn.execute_batch("PRAGMA foreign_keys = ON;");
            let _ = conn.execute_batch("DETACH DATABASE scratch");
            let _ = std::fs::remove_file(&scratch_path);
            spliced?;
        }
    }

    // Keep every verified record — a joiner becomes a server.
    for lr in &records {
        let bytes = bincode::serde::encode_to_vec(lr, bincode::config::standard())
            .map_err(|e| format!("lineage encode: {e}"))?;
        genesis::write_lineage_bytes(data_dir, lr.record.epoch, &bytes)?;
    }
    app_state.epoch.store(record.epoch, Ordering::Relaxed);
    clear_staging(&staging);

    // A re-registered node may already hold fragments from a previous
    // life; the imported inventory does not know that yet.
    if let Ok(conn) = app_state.db_pool.get()
        && let Err(e) = reconcile_fragment_store(&conn, &app_state.fragments_dir, now_unix())
    {
        tracing::warn!("fragment reconcile after join failed (harmless): {e}");
    }

    set_state(format!(
        "joined epoch {} at height {}",
        record.epoch, record.seal_height
    ));
    Ok(())
}

/// Reconcile the fragment store against a freshly imported inventory
/// (RFC-019 S7), in both directions:
///
/// - a fragment the new epoch's inventory backs but that imported with
///   `stored_locally = 0` is re-marked if the bytes are on disk and hash
///   correctly (a joiner has no old database to carry the flag from);
/// - a fragment on disk that the new inventory does not back at all is
///   an orphan and is deleted.
///
/// Direct SQL, deliberately NOT the attestation path: `apply_self_check`
/// guards on an exact previous count, which is right for a live
/// attestation and wrong for a wholesale post-import reconciliation.
/// `self_verified_height` is left NULL — the existing self-check cron
/// re-attests over time, at its own pace.
///
/// Runs at boot before the engine starts, so the zero grace period on
/// the orphan scan is safe: there are no in-flight stores to race. The
/// scan still only considers fragments strictly older than `now_unix`,
/// so anything written in the current second survives to the next pass —
/// under-collecting orphans is harmless, over-collecting is not.
/// Returns `(remarked, orphans_deleted)`.
pub fn reconcile_fragment_store(
    conn: &rusqlite::Connection,
    fragments_dir: &str,
    now_unix: u64,
) -> Result<(usize, usize), String> {
    let unmarked: Vec<(String, i64, i64, Vec<u8>)> = {
        let mut stmt = conn
            .prepare(
                "SELECT data_block_id, chunk_number, local_index, fragment_hash
                 FROM fragment_hashes WHERE stored_locally = 0",
            )
            .map_err(|e| format!("unmarked query: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .map_err(|e| format!("unmarked rows: {e}"))?;
        rows.collect::<Result<_, _>>()
            .map_err(|e| format!("unmarked collect: {e}"))?
    };

    let mut remarked = 0usize;
    for (block_id, chunk, index, hash_bytes) in unmarked {
        let Ok(raw) = <[u8; 32]>::try_from(hash_bytes.as_slice()) else {
            continue;
        };
        let hash = hopnet_storage::Blake3Hash::from_bytes(raw);
        if !hopnet_storage::fragstore::fragment_exists_and_valid(fragments_dir, &hash) {
            continue;
        }
        conn.execute(
            "UPDATE fragment_hashes SET stored_locally = 1
             WHERE data_block_id = ? AND chunk_number = ? AND local_index = ?",
            rusqlite::params![block_id, chunk, index],
        )
        .map_err(|e| format!("re-mark: {e}"))?;
        remarked += 1;
    }

    let scan =
        hopnet_storage::maintenance::scan_orphaned_fragments(conn, fragments_dir, 0, now_unix)
            .map_err(|e| format!("orphan scan: {e:?}"))?;
    let cleanup = hopnet_storage::maintenance::cleanup_orphaned_fragments(
        fragments_dir,
        scan,
        now_unix as i64,
    )
    .map_err(|e| format!("orphan cleanup: {e:?}"))?;
    Ok((remarked, cleanup.deleted_count))
}

/// Seconds since the epoch, for the reconcile clock.
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        app_state
            .epoch_join_inflight
            .store(false, Ordering::Release);
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
        stage_manifest(&staging, &records, &records[0].record, 1, 4096, None).unwrap();

        let manifest = read_manifest(&staging).expect("manifest readable");
        assert_eq!(manifest.target_epoch, 2);
        assert_eq!(manifest.lineage_epochs, vec![2]);
        assert_eq!(manifest.snapshot_hash, records[0].record.snapshot_hash);
        assert!(manifest.manual_fingerprint.is_none());

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
            required_version_code: TARGET,
            snapshot_hash: [0; 32],
            snapshot_len: 0,
            manual_fingerprint: None,
        };
        let bytes = bincode::serde::encode_to_vec(&manifest, bincode::config::standard()).unwrap();
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
        let db_path = dir
            .path()
            .join("database.db")
            .to_string_lossy()
            .into_owned();
        assert_eq!(staging_path(&db_path), dir.path().join(JOIN_STAGING_DIR));
    }

    // Impact: a rejoining node's fragment store survives the boundary
    // but the inventory it is measured against is replaced wholesale —
    // without this pass a joiner reports holding nothing it actually
    // holds, and keeps bytes the new epoch no longer knows about.
    // Should: re-mark on-disk fragments the new inventory backs, and
    // delete on-disk fragments it does not.
    // Should not: re-mark a row whose bytes are absent or corrupt.
    #[test]
    fn reconcile_remarks_local_fragments_and_drops_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());
        let frag_dir = dir.path().join("fragments");
        std::fs::create_dir_all(&frag_dir).unwrap();
        let frags = frag_dir.to_string_lossy().into_owned();

        let backed = b"a fragment the new epoch still backs".to_vec();
        let backed_hash = hopnet_storage::Blake3Hash::from_bytes(*blake3::hash(&backed).as_bytes());
        let orphan = b"a fragment nothing backs any more".to_vec();
        let orphan_hash = hopnet_storage::Blake3Hash::from_bytes(*blake3::hash(&orphan).as_bytes());
        hopnet_storage::fragstore::store_fragment(&frags, &backed_hash, backed).unwrap();
        hopnet_storage::fragstore::store_fragment(&frags, &orphan_hash, orphan).unwrap();

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        crate::db::shared::apply_connection_pragmas(&conn).unwrap();
        // The imported inventory backs the first fragment but, having
        // come from a snapshot, claims nothing is stored locally. Add a
        // second row whose bytes were never on disk.
        conn.execute(
            "UPDATE fragment_hashes SET fragment_hash = ?, stored_locally = 0",
            rusqlite::params![backed_hash.0.as_bytes().to_vec()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fragment_hashes
             (data_block_id, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally)
             VALUES ('blob1', 0, 1, 'frag2', ?, 0, 0)",
            rusqlite::params![vec![0xEEu8; 32]],
        )
        .unwrap();

        // The scan only sees fragments strictly older than the clock it
        // is given, so look from one second in the future.
        let (remarked, orphans) = reconcile_fragment_store(&conn, &frags, now_unix() + 1).unwrap();
        assert_eq!(remarked, 1, "only the fragment actually on disk");
        assert_eq!(orphans, 1, "the unbacked file is deleted");

        let marked: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fragment_hashes WHERE stored_locally = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(marked, 1);
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags,
            &backed_hash
        ));
        assert!(!hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags,
            &orphan_hash
        ));
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
                (RegenesisNetRequest::SnapshotChunk { epoch, offset, len }, Some(cap)) => {
                    RegenesisNetRequest::SnapshotChunk {
                        epoch,
                        offset,
                        len: len.min(cap),
                    }
                }
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
        assert!(manifest.manual_fingerprint.is_none());

        let snapshot = std::fs::read(staged_snapshot_path(&staging)).unwrap();
        assert_eq!(blake3::hash(&snapshot).as_bytes(), &manifest.snapshot_hash);
        assert_eq!(snapshot.len() as u64, manifest.snapshot_len);
        let records = read_staged_lineage(&staging, &manifest).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.epoch, 2);

        // The restart was requested: Notify holds one permit, so a
        // listener registered after the fact still completes.
        rt().block_on(async {
            tokio::time::timeout(
                Duration::from_millis(50),
                app_state.restart_signal.notified(),
            )
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
            std::fs::read(
                server_dir
                    .path()
                    .join(crate::regenesis::seal::SEAL_ARTIFACT_FILENAME),
            )
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

    // Impact: without this a brand-new node simply cannot join a mesh
    // that has ever crossed a boundary — the trusted height-0 genesis it
    // would ask for no longer exists to serve.
    // Should: verify the full lineage chain from a TOFU anchor, import
    // the boundary snapshot in process, and land on the mesh's exact
    // certified state with the epoch meta set.
    // Should not: need a restart — there is no database to swap.
    #[test]
    fn fresh_node_joins_an_epoch_two_mesh() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());

        // A fresh joiner: schema initialized, this_node only — exactly
        // what initialize_joining_node leaves behind.
        let joiner_dir = tempfile::tempdir().unwrap();
        let joiner_db = joiner_dir
            .path()
            .join("database.db")
            .to_string_lossy()
            .into_owned();
        let signing = crate::db::PrivKey(ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]));
        let verifying = crate::db::PubKey(signing.0.verifying_key());
        let app_state = crate::consensus::tests::create_test_app_state_file_backed(
            signing.clone(),
            verifying,
            &joiner_db,
        );
        {
            let conn = app_state.db_pool.get().unwrap();
            conn.execute(
                "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (1, 7, ?)",
                rusqlite::params![signing],
            )
            .unwrap();
        }

        let transport = LocalTransport::new(vec![(2, server.clone())]);
        rt().block_on(epoch_join_bootstrap_with(
            &app_state,
            joiner_dir.path(),
            2,
            &[peer(2)],
            TARGET,
            &transport,
        ))
        .expect("fresh node joins epoch 2");

        let exported = |path: &str| -> Vec<u8> {
            let mut conn = rusqlite::Connection::open(path).unwrap();
            crate::db::shared::apply_connection_pragmas(&conn).unwrap();
            let tx = conn.transaction().unwrap();
            let h = crate::db::snapshot::compute_artifact_hash_tx(&tx).unwrap();
            tx.commit().unwrap();
            h.0.as_bytes().to_vec()
        };
        assert_eq!(
            exported(&joiner_db),
            exported(&server),
            "the joiner holds exactly the mesh's certified state"
        );

        let conn = app_state.db_pool.get().unwrap();
        assert_eq!(genesis::current_epoch(&conn), 2);
        assert_eq!(genesis::epoch_genesis_height(&conn), Some(7));
        assert_eq!(app_state.epoch.load(Ordering::Relaxed), 2);
        // Its own identity is untouched, and the chain it verified is
        // kept so it can serve the next joiner.
        let nid: i32 = conn
            .query_row("SELECT node_id FROM this_node", [], |r| r.get(0))
            .unwrap();
        assert_eq!(nid, 7);
        assert!(genesis::lineage_path(joiner_dir.path(), 2).exists());
        assert!(!staging_path(&joiner_db).exists(), "staging cleared");
    }

    // Impact: the install-time anchor check (RFC-025 S5) is the defense
    // against a coordinator that lied in JoinInfo — the FETCHED chain's
    // epoch-1 identity is compared against the OPERATOR's entered code,
    // and the same field later derives the boot magic, so agreement
    // holds by construction.
    // Should: abort the join with the typed AnchorMismatch when the
    // fetched back-pointer disagrees with the entered code, installing
    // nothing.
    // Should not: fire for a straggler that never entered a code.
    #[test]
    fn fresh_join_aborts_on_anchor_mismatch() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());
        let joiner_dir = tempfile::tempdir().unwrap();
        let joiner_db = joiner_dir
            .path()
            .join("database.db")
            .to_string_lossy()
            .into_owned();
        let signing = crate::db::PrivKey(ed25519_dalek::SigningKey::from_bytes(&[44u8; 32]));
        let verifying = crate::db::PubKey(signing.0.verifying_key());
        let app_state = crate::consensus::tests::create_test_app_state_file_backed(
            signing, verifying, &joiner_db,
        );
        // The operator entered a code that does NOT match the mesh.
        app_state
            .entered_join_code
            .set([0xde, 0xad, 0xbe, 0xef])
            .unwrap();

        let transport = LocalTransport::new(vec![(2, server.clone())]);
        let err = rt()
            .block_on(epoch_join_bootstrap_with(
                &app_state,
                joiner_dir.path(),
                2,
                &[peer(2)],
                TARGET,
                &transport,
            ))
            .expect_err("anchor mismatch must abort");
        match err {
            crate::consensus::malachite::engine::JoinError::AnchorMismatch { entered, .. } => {
                assert_eq!(entered, [0xde, 0xad, 0xbe, 0xef])
            }
            other => panic!("expected AnchorMismatch, got {other:?}"),
        }
        // Nothing was imported.
        let conn = app_state.db_pool.get().unwrap();
        assert_eq!(genesis::current_epoch(&conn), 1);
    }

    // Should: refuse to join an epoch that requires another binary.
    #[test]
    fn fresh_join_refuses_a_version_it_cannot_run() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());
        let joiner_dir = tempfile::tempdir().unwrap();
        let joiner_db = joiner_dir
            .path()
            .join("database.db")
            .to_string_lossy()
            .into_owned();
        let signing = crate::db::PrivKey(ed25519_dalek::SigningKey::from_bytes(&[43u8; 32]));
        let verifying = crate::db::PubKey(signing.0.verifying_key());
        let app_state = crate::consensus::tests::create_test_app_state_file_backed(
            signing, verifying, &joiner_db,
        );

        let transport = LocalTransport::new(vec![(2, server.clone())]);
        let err = rt()
            .block_on(epoch_join_bootstrap_with(
                &app_state,
                joiner_dir.path(),
                2,
                &[peer(2)],
                TARGET + 100,
                &transport,
            ))
            .expect_err("version mismatch must refuse");
        assert!(err.to_string().contains("requires version"), "{err}");
        // Nothing was imported.
        let conn = app_state.db_pool.get().unwrap();
        assert_eq!(genesis::current_epoch(&conn), 1);
    }

    // Impact: several triggers can observe the same epoch-ahead peer at
    // once, so the idempotent path runs often — and it must honour the
    // version gate the fetch path just honoured. Signalling regardless
    // restarts a node into a binary that can only park again, taking it
    // offline for nothing. Caught on real containers, where a parked
    // node staged correctly and was then restarted by a second trigger.
    // Should: re-signal only when this binary can actually build the
    // staged epoch.
    // Should not: request a restart when the staged epoch needs another
    // version — write the awaiting-upgrade marker instead.
    #[test]
    fn idempotent_restage_still_honours_the_version_gate() {
        let server_dir = tempfile::tempdir().unwrap();
        let server = transitioned(server_dir.path());
        let client_dir = tempfile::tempdir().unwrap();
        let (app_state, db_path) = straggler(client_dir.path());
        let transport = LocalTransport::new(vec![(2, server.clone())]);

        // First pass on the WRONG version: stages, marks, no restart.
        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2)],
            &transport,
            TARGET + 100,
        ))
        .unwrap();
        assert!(read_manifest(&staging_path(&db_path)).is_some());

        // Second trigger over the SAME complete staging, same wrong
        // version: still no restart.
        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2)],
            &transport,
            TARGET + 100,
        ))
        .unwrap();
        rt().block_on(async {
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(50),
                    app_state.restart_signal.notified()
                )
                .await
                .is_err(),
                "a re-staged node on the wrong version must not restart"
            );
        });

        // On the right binary, the same staging DOES ask for a restart.
        rt().block_on(run_epoch_join_with(
            &app_state,
            &db_path,
            JoinAnchor::OwnDb,
            vec![peer(2)],
            &transport,
            TARGET,
        ))
        .unwrap();
        rt().block_on(async {
            tokio::time::timeout(
                Duration::from_millis(50),
                app_state.restart_signal.notified(),
            )
            .await
            .expect("the correct version restarts into the staged epoch");
        });
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
