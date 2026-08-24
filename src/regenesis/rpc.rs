//! The "regenesis" comms scope (RFC-019 S7): lineage records and the
//! snapshot artifact, served to stragglers and joiners crossing an epoch
//! boundary. Consensus-support plane — the same liveness class as
//! DecidedFetch (rejoin must not be starved by API load), and served
//! WITHOUT a live engine: a parked or sealed node answering a straggler
//! is load-bearing for rejoin. Everything served here is verified by the
//! REQUESTER against committed hashes (lineage certificates, the
//! record's snapshot_hash) — the server is never trusted.

use hopnet_comms::{BoxFuture, PeerRef, RpcHandler};

use crate::AppState;
use crate::net::{decode_payload, encode_payload};
use crate::regenesis::{boot, genesis, seal};

// The wire vocabulary is generation-1 frozen inventory (RFC-025); the
// handler below speaks the head types via these re-exports.
pub use super::compat_g1::{
    LINEAGE_FETCH_MAX, RegenesisNetRequest, RegenesisNetResponse, SNAPSHOT_CHUNK_MAX,
};

pub struct RegenesisScope {
    pub(crate) app_state: AppState,
}

impl RegenesisScope {
    pub(crate) async fn serve(&self, peer: PeerRef, payload: Vec<u8>) -> RegenesisNetResponse {
        let request: RegenesisNetRequest = match decode_payload(&payload) {
            Ok(r) => r,
            Err(e) => {
                return RegenesisNetResponse::Error {
                    message: format!("bad regenesis request: {e}"),
                };
            }
        };
        // Reachability evidence: an authenticated, well-formed exchange —
        // a straggler asking for lineage is very much reachable.
        self.app_state.evidence.record_contact(peer.node_id);
        // Consensus-support plane: file + DB reads on the QUEUE runtime,
        // never the net runtime, never gated on an engine handle.
        let db_path = crate::db::shared::get_database_path();
        crate::consensus::queue::queue_rt()
            .spawn(async move { serve_request(&db_path, request) })
            .await
            .expect("regenesis serve task panicked")
    }
}

impl RpcHandler for RegenesisScope {
    fn handle(&self, peer: PeerRef, payload: Vec<u8>) -> BoxFuture<'_, Vec<u8>> {
        Box::pin(async move { encode_payload(&self.serve(peer, payload).await) })
    }
}

/// The whole server side over explicit paths — plain connections, no pool
/// (a parked node's serving must not depend on app wiring, and tests
/// drive it over temp-dir databases directly).
pub(crate) fn serve_request(db_path: &str, request: RegenesisNetRequest) -> RegenesisNetResponse {
    let data_dir = match std::path::Path::new(db_path).parent() {
        Some(p) => p.to_path_buf(),
        None => {
            return RegenesisNetResponse::Error {
                message: "database path has no parent directory".into(),
            };
        }
    };
    let conn = match open_read_only(db_path) {
        Ok(c) => c,
        Err(e) => {
            return RegenesisNetResponse::Error {
                message: format!("database unavailable: {e}"),
            };
        }
    };
    match request {
        RegenesisNetRequest::EpochInfo => {
            let decided_height = hopnet_consensus::store::last_decided_height(&conn)
                .ok()
                .flatten()
                .map(|h| h.0)
                .unwrap_or(0);
            RegenesisNetResponse::EpochInfo {
                epoch: genesis::current_epoch(&conn),
                decided_height,
                epoch_genesis_height: genesis::epoch_genesis_height(&conn),
                lineage_from: genesis::lowest_lineage_epoch(&data_dir),
            }
        }
        RegenesisNetRequest::LineageFetch { from_epoch } => {
            let mut records = Vec::new();
            // Contiguous from from_epoch: the requester verifies hop by
            // hop, so a gap would only produce an unverifiable chain.
            for epoch in from_epoch.. {
                if records.len() as u64 >= LINEAGE_FETCH_MAX {
                    break;
                }
                let path = genesis::lineage_path(&data_dir, epoch);
                match std::fs::read(&path) {
                    Ok(bytes) => records.push(bytes),
                    Err(_) => break,
                }
            }
            if records.is_empty() {
                return RegenesisNetResponse::NotAvailable {
                    reason: format!("no lineage record for epoch {from_epoch}"),
                };
            }
            RegenesisNetResponse::Lineage { records }
        }
        RegenesisNetRequest::SnapshotInfo { epoch } => {
            let expected = match served_snapshot_hash(&conn, &data_dir, epoch) {
                Ok(h) => h,
                Err(resp) => return resp,
            };
            match resolve_artifact(db_path, &data_dir, &conn, &expected) {
                Ok(total_len) => RegenesisNetResponse::SnapshotInfo {
                    epoch,
                    total_len,
                    snapshot_hash: expected,
                },
                Err(reason) => RegenesisNetResponse::NotAvailable { reason },
            }
        }
        RegenesisNetRequest::SnapshotChunk { epoch, offset, len } => {
            if let Err(resp) = served_snapshot_hash(&conn, &data_dir, epoch) {
                return resp;
            }
            // No per-chunk re-hash: the requester's whole-artifact blake3
            // against the record's snapshot_hash is the gate, and the file
            // was verified when SnapshotInfo materialized it. A racing
            // overwrite surfaces there, not as corrupt state.
            let path = data_dir.join(seal::SEAL_ARTIFACT_FILENAME);
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(_) => {
                    return RegenesisNetResponse::NotAvailable {
                        reason: "no artifact file (SnapshotInfo prepares it)".into(),
                    };
                }
            };
            let start = (offset as usize).min(bytes.len());
            let end = start
                .saturating_add(len.min(SNAPSHOT_CHUNK_MAX) as usize)
                .min(bytes.len());
            RegenesisNetResponse::SnapshotChunk {
                data: bytes[start..end].to_vec(),
            }
        }
    }
}

fn open_read_only(db_path: &str) -> Result<rusqlite::Connection, String> {
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("open: {e}"))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| format!("busy_timeout: {e}"))?;
    Ok(conn)
}

/// The snapshot identity this node serves for `epoch`, or the honest
/// refusal. v1 serves the LATEST snapshot only: the current epoch's own
/// boundary artifact, whose hash the current lineage record certifies.
fn served_snapshot_hash(
    conn: &rusqlite::Connection,
    data_dir: &std::path::Path,
    epoch: u64,
) -> Result<[u8; 32], RegenesisNetResponse> {
    let current = genesis::current_epoch(conn);
    if epoch != current {
        return Err(RegenesisNetResponse::NotAvailable {
            reason: format!("only the current epoch's snapshot is served (current={current})"),
        });
    }
    if current == 1 {
        return Err(RegenesisNetResponse::NotAvailable {
            reason: "epoch 1 has no boundary snapshot (bootstrap from height 0)".into(),
        });
    }
    let path = genesis::lineage_path(data_dir, current);
    let lineage = genesis::read_lineage(&path).map_err(|e| RegenesisNetResponse::Error {
        message: format!("own lineage record unreadable: {e}"),
    })?;
    Ok(lineage.record.snapshot_hash)
}

/// Make the artifact file exist with the expected bytes, in preference
/// order, and return its length:
/// 1. the file already on disk, if its blake3 matches;
/// 2. recompute from the retained `.sealed` database (rollback window);
/// 3. re-serialize the live database if nothing decided past H — valid
///    by the boot transition's roundtrip gate;
/// 4. honestly unavailable (the requester rotates peers; any one node
///    that kept its artifact — or is still at H — serves the mesh).
fn resolve_artifact(
    db_path: &str,
    data_dir: &std::path::Path,
    conn: &rusqlite::Connection,
    expected: &[u8; 32],
) -> Result<u64, String> {
    // RFC-020 S5 note: the two recompute fallbacks below serialize at
    // the CURRENT binary's shape. An artifact sealed by an older
    // binary (the pre-split cutover artifact especially) can therefore
    // only be served from the on-disk file — a recompute would produce
    // new-shaped bytes, fail the hash check, and fall through to
    // NotAvailable. Every node writes the file at seal and nothing
    // deletes it, so the unavailable case needs mesh-wide file loss.
    let path = data_dir.join(seal::SEAL_ARTIFACT_FILENAME);
    if let Ok(bytes) = std::fs::read(&path)
        && blake3::hash(&bytes).as_bytes() == expected
    {
        return Ok(bytes.len() as u64);
    }

    let sealed = boot::sealed_path(db_path);
    if sealed.exists()
        && let Ok(mut old) = rusqlite::Connection::open_with_flags(
            &sealed,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        && let Ok(artifact) = seal::serialize_verified_artifact(&mut old)
        && blake3::hash(&artifact).as_bytes() == expected
    {
        return write_artifact(&path, &artifact);
    }

    let decided = hopnet_consensus::store::last_decided_height(conn)
        .ok()
        .flatten()
        .map(|h| h.0);
    if decided.is_some() && decided == genesis::epoch_genesis_height(conn) {
        let recomputed = (|| -> Result<Vec<u8>, String> {
            // Read transaction on a second read-only connection — WAL
            // readers never block the live pool.
            let mut live = open_read_only(db_path)?;
            let tx = live.transaction().map_err(|e| format!("tx: {e}"))?;
            let (artifact, _manifest) =
                hopnet_common::snapshot::serialize_snapshot(&tx, &crate::db::snapshot::sections())
                    .map_err(|e| format!("serialize: {e}"))?;
            Ok(artifact)
        })();
        if let Ok(artifact) = recomputed
            && blake3::hash(&artifact).as_bytes() == expected
        {
            return write_artifact(&path, &artifact);
        }
    }

    Err("artifact lost and state has advanced past H (rollback window closed)".into())
}

fn write_artifact(path: &std::path::Path, artifact: &[u8]) -> Result<u64, String> {
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&tmp, artifact).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename to {}: {e}", path.display()))?;
    Ok(artifact.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::regenesis::boot::tests::{TARGET, sealed_db};

    /// A transitioned epoch-2 database in `dir`: the S6 fixture crossed
    /// through the real boot transition. Leaves `database.db` at epoch 2
    /// (decided == H), `database.db.sealed`, and `lineage/epoch-2.bin` —
    /// but NO artifact file (the transition never writes one).
    fn transitioned_db(dir: &std::path::Path) -> String {
        let db_path = sealed_db(dir);
        match crate::regenesis::boot::boot_transition(&db_path, TARGET) {
            crate::regenesis::boot::BootOutcome::Transitioned { epoch: 2 } => {}
            other => panic!("fixture transition failed: {other:?}"),
        }
        db_path
    }

    fn snapshot_hash_of(dir: &std::path::Path) -> [u8; 32] {
        genesis::read_lineage(&genesis::lineage_path(dir, 2))
            .unwrap()
            .record
            .snapshot_hash
    }

    fn bump_decided(db_path: &str, height: u64) {
        let conn = rusqlite::Connection::open(db_path).unwrap();
        crate::db::shared::apply_connection_pragmas(&conn).unwrap();
        // The row exists (install_genesis wrote it) as an SQL integer —
        // the same shape the engine's set_last_decided writes.
        let updated = conn
            .execute(
                "UPDATE consensus_meta SET value = ? WHERE key = 'last_decided_height'",
                rusqlite::params![height as i64],
            )
            .unwrap();
        assert_eq!(updated, 1);
    }

    // Impact: the cross-generation parity gate for the rejoin path — a
    // reshape that breaks generation-0 dialers fails here at mint time.
    // Should: serve generation-0-encoded requests through the head
    // handler (the identity registration) and produce responses the
    // frozen generation-0 decoder reads back exactly.
    #[test]
    fn regenesis_g0_roundtrip() {
        use crate::regenesis::compat_g0 as g0;
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());

        let raw = crate::net::encode_payload(&g0::RegenesisNetRequest::EpochInfo);
        let request: RegenesisNetRequest = crate::net::decode_payload(&raw).unwrap();
        let response = serve_request(&db_path, request);
        let bytes = crate::net::encode_payload(&response);
        match crate::net::decode_payload::<g0::RegenesisNetResponse>(&bytes).unwrap() {
            g0::RegenesisNetResponse::EpochInfo {
                epoch,
                lineage_from,
                ..
            } => {
                assert_eq!(epoch, 2);
                assert_eq!(lineage_from, Some(2));
            }
            _ => panic!("expected EpochInfo"),
        }

        let raw =
            crate::net::encode_payload(&g0::RegenesisNetRequest::LineageFetch { from_epoch: 2 });
        let request: RegenesisNetRequest = crate::net::decode_payload(&raw).unwrap();
        let response = serve_request(&db_path, request);
        let bytes = crate::net::encode_payload(&response);
        match crate::net::decode_payload::<g0::RegenesisNetResponse>(&bytes).unwrap() {
            g0::RegenesisNetResponse::Lineage { records } => {
                assert_eq!(records.len(), 1);
                // The blob is the straggler-parsed encoding — it must
                // decode under the same codec the old binary uses.
                genesis::decode_lineage(&records[0]).unwrap();
            }
            _ => panic!("expected Lineage"),
        }
    }

    // Should: report the epoch, decided height, genesis height, and the
    // lowest lineage record for a transitioned database.
    #[test]
    fn epoch_info_reports_boundary_identity() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());
        match serve_request(&db_path, RegenesisNetRequest::EpochInfo) {
            RegenesisNetResponse::EpochInfo {
                epoch,
                decided_height,
                epoch_genesis_height,
                lineage_from,
            } => {
                assert_eq!(epoch, 2);
                assert_eq!(decided_height, 7);
                assert_eq!(epoch_genesis_height, Some(7));
                assert_eq!(lineage_from, Some(2));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Should: report epoch 1 with no lineage on a pre-boundary database.
    #[test]
    fn epoch_info_on_epoch_one() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());
        match serve_request(&db_path, RegenesisNetRequest::EpochInfo) {
            RegenesisNetResponse::EpochInfo {
                epoch,
                lineage_from,
                epoch_genesis_height,
                ..
            } => {
                assert_eq!(epoch, 1);
                assert_eq!(lineage_from, None);
                assert_eq!(epoch_genesis_height, None);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Should: serve lineage records ascending and contiguous from the
    // requested epoch, stopping at the first gap.
    // Should not: serve anything for an epoch it has no record of.
    #[test]
    fn lineage_fetch_is_contiguous_and_capped() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());

        // Synthesize epochs 3 and 5 (gap at 4): serving is passthrough of
        // on-disk bytes, so raw stand-ins are fine.
        for epoch in [3u64, 5] {
            let path = genesis::lineage_path(dir.path(), epoch);
            std::fs::write(&path, vec![epoch as u8; 8]).unwrap();
        }

        match serve_request(
            &db_path,
            RegenesisNetRequest::LineageFetch { from_epoch: 2 },
        ) {
            RegenesisNetResponse::Lineage { records } => {
                assert_eq!(records.len(), 2, "epochs 2 and 3, stop at the gap");
                assert_eq!(records[1], vec![3u8; 8]);
            }
            other => panic!("unexpected: {other:?}"),
        }

        match serve_request(
            &db_path,
            RegenesisNetRequest::LineageFetch { from_epoch: 6 },
        ) {
            RegenesisNetResponse::NotAvailable { .. } => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Impact: the ladder's step 2 is what lets a node that never wrote
    // (or lost) its artifact file still rescue stragglers during the
    // rollback window.
    // Should: recompute the artifact from the retained sealed database
    // and materialize the file for subsequent chunk reads.
    #[test]
    fn snapshot_info_recomputes_from_retained_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());
        let expected = snapshot_hash_of(dir.path());

        assert!(!dir.path().join(seal::SEAL_ARTIFACT_FILENAME).exists());
        match serve_request(&db_path, RegenesisNetRequest::SnapshotInfo { epoch: 2 }) {
            RegenesisNetResponse::SnapshotInfo {
                epoch,
                total_len,
                snapshot_hash,
            } => {
                assert_eq!(epoch, 2);
                assert_eq!(snapshot_hash, expected);
                let file = std::fs::read(dir.path().join(seal::SEAL_ARTIFACT_FILENAME)).unwrap();
                assert_eq!(file.len() as u64, total_len);
                assert_eq!(blake3::hash(&file).as_bytes(), &expected);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Should: re-serialize the live database when the retained one is
    // gone but nothing has decided past H (the roundtrip gate's promise).
    #[test]
    fn snapshot_info_reserializes_live_database_at_h() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());
        let expected = snapshot_hash_of(dir.path());
        std::fs::remove_file(crate::regenesis::boot::sealed_path(&db_path)).unwrap();

        match serve_request(&db_path, RegenesisNetRequest::SnapshotInfo { epoch: 2 }) {
            RegenesisNetResponse::SnapshotInfo { snapshot_hash, .. } => {
                assert_eq!(snapshot_hash, expected);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Should: prefer a matching on-disk artifact file over any recompute.
    // Should not: serve a stale artifact file whose hash no longer
    // matches the current lineage record.
    #[test]
    fn snapshot_info_verifies_the_file_before_serving_it() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());
        let expected = snapshot_hash_of(dir.path());

        // A stale/corrupt artifact file must be bypassed (ladder falls
        // through to the sealed-db recompute and REPLACES the file).
        std::fs::write(dir.path().join(seal::SEAL_ARTIFACT_FILENAME), b"garbage").unwrap();
        match serve_request(&db_path, RegenesisNetRequest::SnapshotInfo { epoch: 2 }) {
            RegenesisNetResponse::SnapshotInfo { snapshot_hash, .. } => {
                assert_eq!(snapshot_hash, expected);
                let file = std::fs::read(dir.path().join(seal::SEAL_ARTIFACT_FILENAME)).unwrap();
                assert_eq!(blake3::hash(&file).as_bytes(), &expected);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Impact: the honest-refusal clause — a server must never fabricate
    // an artifact it cannot certify; the requester rotates peers.
    // Should not: serve a snapshot once the artifact is lost, the
    // rollback window closed, and state has advanced past H.
    #[test]
    fn snapshot_unavailable_when_lost_and_advanced() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());
        std::fs::remove_file(crate::regenesis::boot::sealed_path(&db_path)).unwrap();
        bump_decided(&db_path, 9);

        match serve_request(&db_path, RegenesisNetRequest::SnapshotInfo { epoch: 2 }) {
            RegenesisNetResponse::NotAvailable { reason } => {
                assert!(reason.contains("advanced past H"), "reason: {reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Should: refuse epochs it does not serve — non-current requests and
    // epoch-1 databases (which have no boundary snapshot).
    #[test]
    fn snapshot_epoch_gates() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());
        match serve_request(&db_path, RegenesisNetRequest::SnapshotInfo { epoch: 3 }) {
            RegenesisNetResponse::NotAvailable { reason } => {
                assert!(reason.contains("current=2"), "reason: {reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }

        let dir1 = tempfile::tempdir().unwrap();
        let sealed = sealed_db(dir1.path());
        match serve_request(&sealed, RegenesisNetRequest::SnapshotInfo { epoch: 1 }) {
            RegenesisNetResponse::NotAvailable { reason } => {
                assert!(reason.contains("epoch 1"), "reason: {reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Should: serve chunked ranges that reassemble to the exact artifact,
    // clamp oversized requests, and answer EOF reads with empty data.
    #[test]
    fn snapshot_chunks_reassemble_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = transitioned_db(dir.path());
        let expected = snapshot_hash_of(dir.path());

        let total_len =
            match serve_request(&db_path, RegenesisNetRequest::SnapshotInfo { epoch: 2 }) {
                RegenesisNetResponse::SnapshotInfo { total_len, .. } => total_len,
                other => panic!("unexpected: {other:?}"),
            };

        let mut assembled = Vec::new();
        let step = 64u64; // tiny chunks to exercise many boundaries
        let mut offset = 0u64;
        while offset < total_len {
            match serve_request(
                &db_path,
                RegenesisNetRequest::SnapshotChunk {
                    epoch: 2,
                    offset,
                    len: step,
                },
            ) {
                RegenesisNetResponse::SnapshotChunk { data } => {
                    assert!(!data.is_empty());
                    offset += data.len() as u64;
                    assembled.extend_from_slice(&data);
                }
                other => panic!("unexpected: {other:?}"),
            }
        }
        assert_eq!(assembled.len() as u64, total_len);
        assert_eq!(blake3::hash(&assembled).as_bytes(), &expected);

        match serve_request(
            &db_path,
            RegenesisNetRequest::SnapshotChunk {
                epoch: 2,
                offset: total_len,
                len: step,
            },
        ) {
            RegenesisNetResponse::SnapshotChunk { data } => assert!(data.is_empty()),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
