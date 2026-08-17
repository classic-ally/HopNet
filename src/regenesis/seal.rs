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

/// Terminal height H if this node sealed — from the node-local marker
/// when present, otherwise DERIVED from committed state.
///
/// The derivation is not belt-and-braces; without it a crash window
/// strands the node. `phase = Sealed` is committed inside the decide
/// transaction, but the marker is written afterwards, on a different
/// pooled connection, from a detached thread. Die in between — SIGKILL,
/// power loss, `docker stop` — or merely fail that write (a busy timeout,
/// or a pool checkout timeout, both of which are logged and stepped past)
/// and the marker is absent while the phase is durably committed.
///
/// Read naively, that combination looks exactly like a healthy node:
/// `boot_transition` takes its State A branch (and deletes the `.next`
/// build), `spawn_engine` declines to park, and the engine starts on the
/// RETIRED chain at H+1 — where `admissible_in_phase` refuses every
/// submission and `validate_inner` refuses every block. Nothing else in
/// the tree writes `META_SEALED_AT`, so nothing recovers it. A single
/// victim is rescued by the S7 epoch join, but a synchronised seal means
/// a whole quorum can land here together, and then no peer is ahead.
///
/// Note the inverse was already guarded: marker present with a
/// non-Sealed phase parks loudly as corrupted. Only this direction was
/// silent. The committed row holds `seal_height` beside the phase, so the
/// meta key is a cache, and treating it as the source of truth is what
/// broke — the module's own contract is that sealed artifacts are derived,
/// idempotent recomputations from sealed local state.
///
/// Fresh and post-crossing databases still read None: the new epoch is
/// born with no `regenesis_state` row at all, which decodes as Normal.
pub fn sealed_marker(conn: &rusqlite::Connection) -> Option<u64> {
    if let Ok(Some(bytes)) = hopnet_consensus::store::meta_get(conn, META_SEALED_AT)
        && let Ok(be) = <[u8; 8]>::try_from(bytes.as_slice())
    {
        return Some(u64::from_be_bytes(be));
    }
    let state = crate::db::regenesis::read_regenesis_state(conn).ok()?;
    if state.phase != RegenesisPhase::Sealed {
        return None;
    }
    tracing::warn!(
        "sealed marker absent but the committed phase is Sealed — deriving H from \
         regenesis_state (the seal work did not finish; the boundary still stands)"
    );
    state.seal_height
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
            if let Err(e) =
                hopnet_consensus::store::meta_put(&conn, META_SEALED_AT, &seal_height.to_be_bytes())
            {
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

    // Restart derivation (RFC-019 S6): DERIVED, never declared — compare
    // the committed target with what this binary runs. Match → request a
    // process restart (the binary listens on the signal and exits with
    // the restart code; the boot transition crosses the boundary on the
    // way back up). Mismatch → park awaiting upgrade: marker file for
    // operators, process alive, engine off.
    let target = app_state
        .db_pool
        .get()
        .ok()
        .and_then(|conn| crate::db::regenesis::read_regenesis_state(&conn).ok())
        .and_then(|s| s.target_version_code);
    let Some(target) = target else {
        tracing::error!("restart derivation: sealed but no committed target version");
        return;
    };
    let running = crate::version::effective_running_code();
    if running == target {
        tracing::info!(
            version = %crate::version::format_code(target),
            "sealed at the target version: requesting process restart"
        );
        app_state.restart_signal.notify_one();
    } else if crate::upgrade::ActivationEnv::from_env().is_some_and(|env| {
        match env.try_activate(target) {
            Ok(()) => true,
            Err(reason) => {
                tracing::warn!(%reason, "staged-generation activation failed; parking");
                false
            }
        }
    }) {
        // RFC-021/026: the staged generation for the target is now behind
        // the profile — the same restart the == arm requests re-execs into
        // it.
        tracing::info!(
            version = %crate::version::format_code(target),
            "sealed and activated the staged generation: requesting process restart"
        );
        app_state.restart_signal.notify_one();
    } else {
        tracing::warn!(
            required = %crate::version::format_code(target),
            running = %crate::version::format_code(running),
            "sealed for a different version: parking awaiting upgrade"
        );
        crate::regenesis::boot::write_awaiting_marker(
            &crate::db::shared::get_database_path(),
            target,
        );
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
    let artifact = serialize_verified_artifact(&mut conn)?;

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

/// Serialize the canonical artifact from a SEALED database on any plain
/// connection — the pool-free entry the boot transition uses (the pool
/// does not exist yet at boot) — and verify it against the COMMITTED
/// snapshot_hash. A mismatch means this replica is the anomaly (diverged
/// after voting, or synced past the seal): it must rebuild via epoch
/// join (S7), never publish or import wrong bytes.
pub fn serialize_verified_artifact(conn: &mut rusqlite::Connection) -> Result<Vec<u8>, String> {
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
    Ok(artifact)
}
