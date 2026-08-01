//! Node-local seal work (RFC-019 S5): the durable marker and the snapshot
//! artifact, produced after the regenesis commit decides. Everything here
//! is a derived, idempotent recomputation from sealed local state — a
//! crash at any point recovers by recomputing on wake, never by peers
//! (seal contract, "What the engine must implement").

use crate::AppState;
use crate::db::regenesis::RegenesisPhase;

/// consensus_meta key: the terminal height H, big-endian u64. Written by
/// the seal work, read by spawn_engine's boot gate. Node-local by design
/// (consensus_meta sits outside the snapshot universe).
pub const META_SEALED_AT: &str = "regenesis_sealed_at";

/// The artifact next to the database (spec: Snapshot & Certificate).
pub const SEAL_ARTIFACT_FILENAME: &str = "regenesis-snapshot.bin";

/// Terminal height from the durable marker, if this node sealed.
pub fn sealed_marker(conn: &rusqlite::Connection) -> Option<u64> {
    let bytes = hopnet_consensus::store::meta_get(conn, META_SEALED_AT).ok()??;
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

/// Where the artifact lives: the database file's parent directory.
pub fn artifact_path() -> std::path::PathBuf {
    std::path::Path::new(&crate::db::shared::get_database_path())
        .parent()
        .map(|p| p.join(SEAL_ARTIFACT_FILENAME))
        .unwrap_or_else(|| std::path::PathBuf::from(SEAL_ARTIFACT_FILENAME))
}

/// The full seal work, in durability order: marker FIRST (the boot gate
/// must hold even if the artifact write is interrupted — it recomputes on
/// wake), then the artifact. Errors are logged, never fatal: the work is
/// re-derivable for as long as the sealed database exists.
pub fn run_seal_work(app_state: &AppState, seal_height: u64) {
    match app_state.db_pool.get() {
        Ok(conn) => {
            if let Err(e) = hopnet_consensus::store::meta_put(
                &conn,
                META_SEALED_AT,
                &seal_height.to_be_bytes(),
            ) {
                tracing::error!("seal marker write failed: {e}");
            }
        }
        Err(e) => tracing::error!("seal marker conn: {e}"),
    }
    match write_seal_artifact_to(app_state, &artifact_path()) {
        Ok(path) => tracing::info!(path = %path.display(), "epoch sealed at {seal_height}"),
        Err(e) => {
            tracing::error!("seal artifact write failed (recomputed on next boot): {e}");
        }
    }
}

/// Serialize the canonical snapshot from sealed local state and write it
/// atomically (tmp + rename). The recomputed top hash is verified against
/// the COMMITTED snapshot_hash first: a mismatch means this replica is
/// the anomaly (diverged after voting, or synced past the seal) — it must
/// rebuild via epoch join (S7), never publish a wrong artifact.
pub fn write_seal_artifact_to(
    app_state: &AppState,
    path: &std::path::Path,
) -> Result<std::path::PathBuf, String> {
    let mut conn = app_state.db_pool.get().map_err(|e| format!("conn: {e}"))?;
    let tx = conn.transaction().map_err(|e| format!("tx: {e}"))?;

    let state =
        crate::db::regenesis::read_regenesis_state(&tx).map_err(|e| format!("state: {e:?}"))?;
    if state.phase != RegenesisPhase::Sealed {
        return Err("epoch is not sealed".into());
    }
    let committed_hash = state.snapshot_hash.ok_or("sealed state carries no hash")?;

    let (artifact, _manifest) =
        hopnet_common::snapshot::serialize_snapshot(&tx, &crate::db::snapshot::sections())
            .map_err(|e| format!("serialize: {e}"))?;
    drop(tx);

    // The commit certified blake3 over the ARTIFACT bytes (Exported
    // tables only — stable across the seal transition; the boundary
    // machinery mutates only divergence-only state).
    let recomputed = blake3::hash(&artifact);
    if recomputed.as_bytes() != committed_hash.as_slice() {
        return Err(format!(
            "recomputed artifact hash {} != committed {} — this replica diverged; rebuild via epoch join",
            recomputed.to_hex(),
            hex::encode(&committed_hash),
        ));
    }

    // Unique tmp per writer: concurrent recomputes (multiple in-process
    // nodes in tests; a crashed-then-woken node racing on_decided) each
    // rename their own COMPLETE bytes — last rename wins, and every
    // writer's bytes are identical (certified above).
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = path.with_extension(format!(
        "tmp.{}.{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::write(&tmp, &artifact).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename to {}: {e}", path.display()))?;
    Ok(path.to_path_buf())
}
