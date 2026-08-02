//! The epoch boot transition (RFC-019 S6): runs in `run_server` BEFORE
//! the connection pool opens — the one window where the database is a
//! plain file nothing holds open, so a build-then-rename swap is safe.
//! Everything here is a pure function of local sealed state; no peer is
//! ever involved (transfer-free by design — S7 owns fetch paths).
//!
//! Boot gates, in spec order: VERSION (exact match, refusal parks the
//! node awaiting upgrade), LINEAGE (certificate chain), IMPORT (fresh
//! database, certified artifact, recompute-and-compare), NODE-LOCAL
//! CARRY. The (epoch, version) HANDSHAKE gate lives at the network
//! layer, not here.
//!
//! Crash-safety contract: any failure before the first rename leaves
//! the old database byte-identical (the fresh build is delete-and-
//! rebuild, never resumed); the window between the two renames is
//! recovered by state C below; after the second rename the boundary is
//! crossed and the retained database only awaits rollback-window
//! cleanup.

use std::path::{Path, PathBuf};

use crate::db::regenesis::{RegenesisPhase, read_regenesis_state};
use crate::regenesis::genesis;
use crate::regenesis::seal;

/// `database.db.next` — the fresh epoch-N+1 database under construction.
/// Never trusted across a crash: always deleted and rebuilt.
pub const NEXT_SUFFIX: &str = "next";

/// `database.db.sealed` — the retained epoch-N database (rollback
/// window: kept until the new epoch's first decide).
pub const SEALED_SUFFIX: &str = "sealed";

/// Marker file (beside the database) that this node is parked awaiting a
/// binary upgrade; contains the required CalVer string. For operators
/// and service scripts — the status API derives the same fact from
/// committed state.
pub const AWAITING_UPGRADE_FILENAME: &str = "awaiting-upgrade";

/// Marker (beside the database) requesting that a pending or
/// just-crossed epoch boundary be ABANDONED on the next boot — the
/// operator's rollback request (RFC-019 S8). Honoured before every other
/// boot path, and deleted only once the rollback is complete.
pub const ROLLBACK_MARKER_FILENAME: &str = "rollback-epoch";

#[derive(Debug)]
pub enum BootOutcome {
    /// No boundary pending: normal boot.
    NoBoundary,
    /// The boundary was crossed: `database.db` is now the epoch-`epoch`
    /// database and the engine will start at H+1.
    Transitioned { epoch: u64 },
    /// A rollback request was honoured: the boundary was abandoned and
    /// this node is back on `epoch`. DESTRUCTIVE — the newer epoch's
    /// database is gone.
    RolledBack { epoch: u64 },
    /// A gate refused; the node stays up on the OLD sealed database
    /// (HTTP + status served, engine parked by the sealed marker).
    Parked(ParkReason),
    /// The on-disk state is unrecoverable without an operator (e.g. the
    /// live database is missing but a retained one exists). The caller
    /// must NOT continue booting — continuing would initialize an empty
    /// database over a mesh member's identity.
    Fatal(String),
}

#[derive(Debug)]
pub enum ParkReason {
    /// Gate 1: this binary is not the version the new epoch requires.
    AwaitingUpgrade { required: u32, running: u32 },
    /// Gate 2/3 failure: refused to cross, old database untouched;
    /// retried on next boot.
    GateFailed { gate: &'static str, detail: String },
}

/// Last boundary error, for the status surface (latest wins — in
/// production a parked node runs at most one transition per boot).
static BOUNDARY_ERROR: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

pub fn boundary_error() -> Option<String> {
    BOUNDARY_ERROR.lock().ok().and_then(|g| g.clone())
}

pub fn next_path(db_path: &str) -> PathBuf {
    PathBuf::from(format!("{db_path}.{NEXT_SUFFIX}"))
}

pub fn sealed_path(db_path: &str) -> PathBuf {
    PathBuf::from(format!("{db_path}.{SEALED_SUFFIX}"))
}

pub fn awaiting_upgrade_path(db_path: &str) -> PathBuf {
    Path::new(db_path)
        .parent()
        .map(|p| p.join(AWAITING_UPGRADE_FILENAME))
        .unwrap_or_else(|| PathBuf::from(AWAITING_UPGRADE_FILENAME))
}

/// Write the awaiting-upgrade marker (idempotent; content = required
/// version string). Called from gate 1 here and from the seal work when
/// the restart derivation finds a version mismatch.
pub fn write_awaiting_marker(db_path: &str, required: u32) {
    let path = awaiting_upgrade_path(db_path);
    if let Err(e) = std::fs::write(&path, crate::version::format_code(required)) {
        tracing::error!(path = %path.display(), "awaiting-upgrade marker write failed: {e}");
    }
}

pub fn rollback_marker_path(db_path: &str) -> PathBuf {
    Path::new(db_path)
        .parent()
        .map(|p| p.join(ROLLBACK_MARKER_FILENAME))
        .unwrap_or_else(|| PathBuf::from(ROLLBACK_MARKER_FILENAME))
}

/// Request that the next boot abandon the boundary. Durable and
/// explicit: the operator's intent survives a crash, and the boot path
/// owns the surgery.
pub fn write_rollback_marker(db_path: &str) {
    let path = rollback_marker_path(db_path);
    if let Err(e) = std::fs::write(&path, "abandon the pending epoch boundary\n") {
        tracing::error!(path = %path.display(), "rollback marker write failed: {e}");
    }
}

/// Is there a boundary to abandon? True when a retained previous-epoch
/// database exists (we crossed and the window is still open) or this
/// database is sealed (we sealed but never crossed). False means a
/// rollback request would be a no-op — the window closed, or there was
/// never a boundary — and the route refuses rather than acting.
pub fn rollback_available(db_path: &str, conn: &rusqlite::Connection) -> bool {
    sealed_path(db_path).exists() || seal::sealed_marker(conn).is_some()
}

/// SQLite sidecars for a database file path.
fn sidecars(path: &Path) -> [PathBuf; 2] {
    let base = path.to_string_lossy();
    [
        PathBuf::from(format!("{base}-wal")),
        PathBuf::from(format!("{base}-shm")),
    ]
}

fn remove_with_sidecars(path: &Path) {
    let _ = std::fs::remove_file(path);
    for s in sidecars(path) {
        let _ = std::fs::remove_file(s);
    }
}

fn park(gate: &'static str, detail: String) -> BootOutcome {
    tracing::error!(gate, "epoch boot gate refused: {detail}");
    if let Ok(mut slot) = BOUNDARY_ERROR.lock() {
        *slot = Some(format!("{gate}: {detail}"));
    }
    BootOutcome::Parked(ParkReason::GateFailed { gate, detail })
}

/// The boot transition. `running_code` is injected by the caller
/// (`version::effective_running_code()` in production) so gate tests
/// never touch process env.
pub fn boot_transition(db_path: &str, running_code: u32) -> BootOutcome {
    let db = Path::new(db_path);
    let next = next_path(db_path);
    let sealed = sealed_path(db_path);
    let awaiting = awaiting_upgrade_path(db_path);

    // A rollback request outranks every other boot path (RFC-019 S8).
    // BEFORE the missing-database dispatch, because a crash midway
    // through a restore leaves exactly the state-D arrangement, which
    // would otherwise be fatal; and before the staged-join branch,
    // because a leftover staging would otherwise drag this node forward
    // again immediately.
    if let Some(outcome) = rollback_transition(db_path) {
        return outcome;
    }

    if !db.exists() {
        // State C: crashed between the two renames. The .next file is
        // complete by construction (built, committed, checkpointed and
        // closed before the first rename) — finish the swap.
        if next.exists() && sealed.exists() {
            if let Err(e) = std::fs::rename(&next, db) {
                return BootOutcome::Fatal(format!(
                    "completing interrupted epoch swap: rename {} -> {db_path}: {e}",
                    next.display()
                ));
            }
            for s in sidecars(&next) {
                let _ = std::fs::remove_file(s);
            }
            let _ = std::fs::remove_file(&awaiting);
            let epoch = match read_epoch_of(db_path) {
                Ok(e) => e,
                Err(e) => return BootOutcome::Fatal(format!("post-swap epoch read: {e}")),
            };
            tracing::info!(epoch, "completed interrupted epoch swap");
            return BootOutcome::Transitioned { epoch };
        }
        // State D: no live database, no complete build — but a retained
        // epoch database exists. Never boot fresh over a mesh identity.
        if sealed.exists() {
            // A bare `mv` is NOT a rollback: the retained database still
            // carries the sealed marker and the committed Sealed phase, so
            // the next boot would re-cross the boundary (same binary) or
            // park with no consensus (older binary). Point at the marker,
            // which makes the boot path do it properly.
            return BootOutcome::Fatal(format!(
                "database.db is missing but {} exists — manual recovery required. \
                 To abandon the boundary and return to the retained epoch, create \
                 the rollback marker and restart: touch {}",
                sealed.display(),
                rollback_marker_path(db_path).display()
            ));
        }
        // Fresh node: nothing to do.
        return BootOutcome::NoBoundary;
    }

    // The live database exists — read the sealed marker over a plain
    // connection (the pool does not exist yet).
    let mut conn = match rusqlite::Connection::open(db_path) {
        Ok(c) => c,
        Err(e) => return BootOutcome::Fatal(format!("open {db_path}: {e}")),
    };
    if let Err(e) = crate::db::shared::apply_connection_pragmas(&conn) {
        return BootOutcome::Fatal(format!("pragmas on {db_path}: {e}"));
    }
    // A staged epoch join (RFC-019 S7) is checked BEFORE both the
    // state-A cleanup below and the sealed gates: a straggler is not
    // sealed, so state A would return NoBoundary and it would never
    // cross; and a node parked by a failed gate must be able to rebuild
    // from peers rather than fail the same local gate forever.
    if let Some(outcome) = staged_join_transition(db_path, running_code, &mut conn) {
        return outcome;
    }

    if seal::sealed_marker(&conn).is_none() {
        // State A: normal boot. Clean anything a crashed transition or a
        // completed upgrade left behind — but NEVER the retained
        // database (rollback window; the cleanup task owns it), and
        // never join staging (an in-progress download resumes).
        remove_with_sidecars(&next);
        let _ = std::fs::remove_file(&awaiting);
        return BootOutcome::NoBoundary;
    }

    // State B: sealed — run the gates.
    let state = match read_regenesis_state(&conn) {
        Ok(s) => s,
        Err(e) => return park("lineage", format!("regenesis state read: {e:?}")),
    };
    if state.phase != RegenesisPhase::Sealed {
        return park(
            "lineage",
            format!(
                "sealed marker present but committed phase is {:?} — corrupted boundary state",
                state.phase
            ),
        );
    }

    // Gate 1: VERSION, exact. Refusal parks the node; the old database
    // stays live, the engine stays parked on the sealed marker.
    let required = match state.target_version_code {
        Some(t) => t,
        None => return park("lineage", "sealed row missing target_version_code".into()),
    };
    if running_code != required {
        tracing::warn!(
            required = %crate::version::format_code(required),
            running = %crate::version::format_code(running_code),
            "epoch requires a different version: parking awaiting upgrade"
        );
        write_awaiting_marker(db_path, required);
        return BootOutcome::Parked(ParkReason::AwaitingUpgrade {
            required,
            running: running_code,
        });
    }

    // Gate 2: LINEAGE — construct the genesis and verify our own
    // evidence for it against the seated set we already trusted.
    let epoch_genesis = match genesis::build_epoch_genesis(&conn) {
        Ok(g) => g,
        Err(e) => return park("lineage", e),
    };
    let valset = match genesis::record_valset(&epoch_genesis.record) {
        Ok(v) => v,
        Err(e) => return park("lineage", e),
    };
    let profile = match hopnet_consensus::config::QuorumProfile::parse(
        &epoch_genesis.record.quorum_profile,
    ) {
        Some(p) => p,
        None => {
            return park(
                "lineage",
                format!(
                    "unknown quorum profile {:?}",
                    epoch_genesis.record.quorum_profile
                ),
            );
        }
    };
    if let Err(e) = genesis::verify_lineage(
        &epoch_genesis.record,
        &epoch_genesis.final_block,
        &epoch_genesis.final_cert,
        &valset,
        &profile,
    ) {
        return park("lineage", e);
    }

    // Gate 3: IMPORT — the certified artifact into a fresh database.
    // File first; a missing or non-matching file falls back to
    // recomputation from the sealed database (same rule as the
    // spawn_engine artifact recovery).
    let artifact_file = Path::new(db_path)
        .parent()
        .map(|p| p.join(seal::SEAL_ARTIFACT_FILENAME))
        .unwrap_or_else(|| PathBuf::from(seal::SEAL_ARTIFACT_FILENAME));
    let committed = epoch_genesis.record.snapshot_hash;
    let artifact = match std::fs::read(&artifact_file) {
        Ok(bytes) if *blake3::hash(&bytes).as_bytes() == committed => bytes,
        _ => match seal::serialize_verified_artifact(&mut conn) {
            Ok(bytes) => bytes,
            Err(e) => return park("import", format!("artifact recompute: {e}")),
        },
    };

    remove_with_sidecars(&next);
    if let Err(e) = build_next(&next, db_path, &artifact, &epoch_genesis) {
        remove_with_sidecars(&next);
        return park("import", e);
    }

    // The lineage record survives the boundary forever — written before
    // the swap so a crash never loses it (rewrite is idempotent).
    let lineage_dir = Path::new(db_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    if let Err(e) = genesis::write_lineage(lineage_dir, &epoch_genesis) {
        remove_with_sidecars(&next);
        return park("import", format!("lineage write: {e}"));
    }

    if let Err(e) = checkpoint_and_swap(db_path, conn) {
        return match e {
            SwapError::Refused(detail) => {
                remove_with_sidecars(&next);
                park("import", detail)
            }
            SwapError::Interrupted(detail) => BootOutcome::Fatal(detail),
        };
    }

    let epoch = epoch_genesis.record.epoch;
    tracing::info!(
        epoch,
        seal_height = epoch_genesis.record.seal_height,
        chain_id = %epoch_genesis.block.block_hash,
        "epoch boundary crossed: fresh database installed, engine will start at H+1"
    );
    BootOutcome::Transitioned { epoch }
}

/// Abandon a pending or just-crossed epoch boundary (RFC-019 S8).
/// Returns `None` when there is no rollback request to honour.
///
/// Restoring the retained database by hand is NOT enough: it still
/// carries the sealed marker and the committed Sealed phase, so the very
/// next boot would either re-cross the boundary (same binary — the
/// silent-undo bug) or park with the engine refusing to start (older
/// binary). Rolling back therefore means clearing that state too, which
/// is what this owns.
///
/// Three arrangements, which together also make this resumable — the
/// marker is deleted LAST, so a crash re-enters the machine one case
/// further along:
///
/// 1. a retained database exists: discard the newer epoch's database,
///    restore the retained one, clear its seal state;
/// 2. no retained database but this one is sealed: the node sealed and
///    parked without ever crossing — clear the seal state in place;
/// 3. neither: nothing to abandon (the window closed, or the rollback
///    already completed). Refuse, drop the marker, boot normally.
///
/// Clearing the committed `regenesis_state` row is a DELIBERATE mutation
/// of consensus-committed state outside consensus. It is the only way the
/// mesh runs again — a Sealed phase refuses every submission — and the
/// spec sanctions it for exactly this window. Every node performs it
/// identically, and the row is divergence-only, so it never enters the
/// exported state hash. Recovery from a rollback is another regenesis,
/// FORWARD; the abandoned boundary is never retried.
fn rollback_transition(db_path: &str) -> Option<BootOutcome> {
    let marker = rollback_marker_path(db_path);
    if !marker.exists() {
        return None;
    }
    let db = Path::new(db_path);
    let sealed = sealed_path(db_path);

    if sealed.exists() {
        // Case 1. The epoch we are abandoning, named before its database
        // is discarded, so its lineage record can go with it.
        let abandoned = read_epoch_of(db_path).ok();
        tracing::warn!(
            retained = %sealed.display(),
            abandoned_epoch = ?abandoned,
            "rollback requested: discarding this epoch's database and restoring the retained one"
        );
        remove_with_sidecars(db);
        if let Err(e) = std::fs::rename(&sealed, db) {
            // Nothing else can proceed: the live database is gone and the
            // retained one could not take its place.
            return Some(BootOutcome::Fatal(format!(
                "rollback: restoring {} -> {db_path}: {e}",
                sealed.display()
            )));
        }
        if let Some(epoch) = abandoned {
            let dir = Path::new(db_path)
                .parent()
                .unwrap_or_else(|| Path::new("."));
            // The abandoned boundary is not part of this mesh's history:
            // keeping its record would have us answer a joiner with a
            // lineage whose snapshot we then refuse to serve.
            let _ = std::fs::remove_file(genesis::lineage_path(dir, epoch));
        }
    }

    match clear_seal_state(db_path) {
        Ok(true) => {}
        Ok(false) if sealed.exists() => {}
        Ok(false) => {
            // Case 3: nothing was sealed and nothing was retained.
            tracing::warn!(
                "rollback requested but there is no boundary to abandon \
                 (the window has closed, or the rollback already completed)"
            );
            if let Ok(mut slot) = BOUNDARY_ERROR.lock() {
                *slot = Some("rollback: no boundary to abandon".to_string());
            }
            let _ = std::fs::remove_file(&marker);
            return None;
        }
        Err(e) => return Some(BootOutcome::Fatal(format!("rollback: {e}"))),
    }

    // A staged join would otherwise carry this node straight back across.
    crate::regenesis::join::clear_staging(&crate::regenesis::join::staging_path(db_path));
    remove_with_sidecars(&next_path(db_path));
    let _ = std::fs::remove_file(awaiting_upgrade_path(db_path));

    let epoch = read_epoch_of(db_path).unwrap_or(1);
    // LAST: until this is gone the rollback is still in progress.
    let _ = std::fs::remove_file(&marker);
    tracing::warn!(epoch, "rollback complete: the epoch boundary was abandoned");
    Some(BootOutcome::RolledBack { epoch })
}

/// Clear the seal state from the database at `db_path`: the node-local
/// sealed marker and the committed phase row. Returns whether anything
/// was actually sealed. Idempotent — a resumed rollback re-runs it.
fn clear_seal_state(db_path: &str) -> Result<bool, String> {
    let conn = rusqlite::Connection::open(db_path).map_err(|e| format!("open: {e}"))?;
    crate::db::shared::apply_connection_pragmas(&conn).map_err(|e| format!("pragmas: {e}"))?;
    let was_sealed = seal::sealed_marker(&conn).is_some();
    conn.execute(
        "DELETE FROM consensus_meta WHERE key = ?",
        [seal::META_SEALED_AT],
    )
    .map_err(|e| format!("clearing the sealed marker: {e}"))?;
    // Plain DELETE rather than clear_to_normal_tx: absence is the
    // canonical Normal encoding, so a second pass must not be an error.
    conn.execute("DELETE FROM regenesis_state WHERE internal_id = 1", [])
        .map_err(|e| format!("clearing the boundary phase: {e}"))?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|e| format!("checkpoint: {e}"))?;
    Ok(was_sealed)
}

/// How a swap failed: before anything moved (the old database is intact
/// and the boundary can be retried), or between the two renames (state C
/// on the next boot completes it).
enum SwapError {
    Refused(String),
    Interrupted(String),
}

/// Retire the old database and swap the freshly built `.next` in.
///
/// Checkpoint and close the old database FIRST so its WAL is empty and
/// its sidecars can be dropped — a leftover `database.db-wal` would
/// otherwise be adopted by the NEW `database.db` after rename 2.
fn checkpoint_and_swap(db_path: &str, conn: rusqlite::Connection) -> Result<(), SwapError> {
    let db = Path::new(db_path);
    let next = next_path(db_path);
    let sealed = sealed_path(db_path);

    if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
        return Err(SwapError::Refused(format!("old database checkpoint: {e}")));
    }
    drop(conn);
    for s in sidecars(db) {
        let _ = std::fs::remove_file(s);
    }
    if let Err(e) = std::fs::rename(db, &sealed) {
        return Err(SwapError::Refused(format!("retain rename: {e}")));
    }
    if let Err(e) = std::fs::rename(&next, db) {
        return Err(SwapError::Interrupted(format!(
            "epoch swap interrupted after retain: rename {} -> {db_path}: {e} \
             (next boot completes the swap)",
            next.display()
        )));
    }
    let _ = std::fs::remove_file(awaiting_upgrade_path(db_path));
    Ok(())
}

/// Cross an epoch boundary from inputs a peer served and this node
/// already verified once online (RFC-019 S7). Returns `None` to fall
/// through to the ordinary boot paths.
///
/// Everything staged is re-verified here over the same immutable bytes:
/// the online pass proves the download is worth keeping, this pass is
/// what the swap actually trusts. A straggler may also have sealed
/// itself in the meantime (a still-sealed peer can serve it the final
/// block); the staged path takes precedence and builds the same
/// certified state either way.
fn staged_join_transition(
    db_path: &str,
    running_code: u32,
    conn: &mut rusqlite::Connection,
) -> Option<BootOutcome> {
    use crate::regenesis::join;

    let staging = join::staging_path(db_path);
    if !staging.exists() {
        return None;
    }
    let Some(manifest) = join::read_manifest(&staging) else {
        // Incomplete: a download was interrupted. KEEP the partials —
        // the next online attempt resumes from them.
        tracing::info!(
            path = %staging.display(),
            "join staging is incomplete: keeping partial download for resume"
        );
        return None;
    };

    // A leftover from a transition that already completed (the swap
    // happens before the cleanup).
    if genesis::current_epoch(conn) >= manifest.target_epoch {
        join::clear_staging(&staging);
        return None;
    }

    // Gate 1: VERSION, before anything touches schema — a straggler
    // coming back across an UPGRADE boundary must not build the new
    // epoch's database with the old binary. Staging is kept: the
    // download is certified and the upgraded binary boots into it.
    if running_code != manifest.required_version_code {
        let required = manifest.required_version_code;
        tracing::warn!(
            required = %crate::version::format_code(required),
            running = %crate::version::format_code(running_code),
            "staged epoch join requires a different version: parking awaiting upgrade"
        );
        write_awaiting_marker(db_path, required);
        return Some(BootOutcome::Parked(ParkReason::AwaitingUpgrade {
            required,
            running: running_code,
        }));
    }

    let records = match join::read_staged_lineage(&staging, &manifest) {
        Ok(r) => r,
        Err(e) => return Some(poison(&staging, format!("staged lineage unreadable: {e}"))),
    };

    // Gate 2: CHAIN + OVERLAP against our OWN last-trusted state. This
    // passed online over these same bytes, so a failure here means the
    // staging was corrupted on disk — discard it and refetch.
    let mut anchor = match genesis::ChainAnchor::from_db(conn) {
        Ok(a) => a,
        Err(e) => return Some(poison(&staging, format!("anchor: {e}"))),
    };
    if manifest.manual_anchor {
        // Operator re-trust: weak subjectivity is exactly what they
        // overrode. Linkage, per-hop quorum and the snapshot hash below
        // are all still enforced.
        anchor.trusted = None;
    }
    let target = match genesis::verify_lineage_chain(&records, anchor) {
        Ok(t) => t,
        Err(e) => return Some(poison(&staging, e)),
    };
    if target.record.epoch != manifest.target_epoch {
        return Some(poison(
            &staging,
            format!(
                "manifest claims epoch {} but the chain ends at {}",
                manifest.target_epoch, target.record.epoch
            ),
        ));
    }

    // Gate 3: SNAPSHOT — the artifact must be exactly what the verified
    // chain certifies.
    let artifact = match std::fs::read(join::staged_snapshot_path(&staging)) {
        Ok(bytes) => bytes,
        Err(e) => return Some(poison(&staging, format!("staged snapshot: {e}"))),
    };
    if *blake3::hash(&artifact).as_bytes() != target.record.snapshot_hash {
        return Some(poison(
            &staging,
            "staged snapshot fails its certified hash".into(),
        ));
    }

    // Rebuild with the SAME machinery the sealed path uses: certified
    // import, node-local carry, genesis at H, fresh meta, roundtrip gate.
    let epoch_genesis = match staged_epoch_genesis(target) {
        Ok(g) => g,
        Err(e) => return Some(poison(&staging, e)),
    };
    let next = next_path(db_path);
    remove_with_sidecars(&next);
    if let Err(e) = build_next(&next, db_path, &artifact, &epoch_genesis) {
        remove_with_sidecars(&next);
        return Some(park("staged-join", e));
    }

    // Every verified record is kept forever — that is what lets a node
    // that arrived by join answer the next straggler.
    let dir = Path::new(db_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    for lr in &records {
        match bincode::serde::encode_to_vec(lr, bincode::config::standard()) {
            Ok(bytes) => {
                if let Err(e) = genesis::write_lineage_bytes(dir, lr.record.epoch, &bytes) {
                    remove_with_sidecars(&next);
                    return Some(park("staged-join", format!("lineage write: {e}")));
                }
            }
            Err(e) => {
                remove_with_sidecars(&next);
                return Some(park("staged-join", format!("lineage encode: {e}")));
            }
        }
    }

    let owned = std::mem::replace(
        conn,
        match rusqlite::Connection::open_in_memory() {
            Ok(c) => c,
            Err(e) => {
                remove_with_sidecars(&next);
                return Some(park("staged-join", format!("placeholder connection: {e}")));
            }
        },
    );
    if let Err(e) = checkpoint_and_swap(db_path, owned) {
        return Some(match e {
            SwapError::Refused(detail) => {
                remove_with_sidecars(&next);
                park("staged-join", detail)
            }
            SwapError::Interrupted(detail) => BootOutcome::Fatal(detail),
        });
    }

    // Post-swap, pre-engine: the fragment store survived untouched but
    // the inventory it is measured against was just replaced. Log-only —
    // a reconcile failure must never strand a node that just rejoined.
    match rusqlite::Connection::open(db_path) {
        Ok(fresh) => {
            let fragments_dir =
                hopnet_storage::fragstore::get_fragments_dir().unwrap_or_else(|_| String::new());
            match join::reconcile_fragment_store(&fresh, &fragments_dir, join::now_unix()) {
                Ok((remarked, orphans)) => tracing::info!(
                    remarked,
                    orphans,
                    "fragment store reconciled against the joined epoch"
                ),
                Err(e) => tracing::warn!("fragment reconcile failed (harmless): {e}"),
            }
        }
        Err(e) => tracing::warn!("fragment reconcile skipped: {e}"),
    }

    join::clear_staging(&staging);
    let epoch = epoch_genesis.record.epoch;
    tracing::info!(
        epoch,
        seal_height = epoch_genesis.record.seal_height,
        hops = records.len(),
        "epoch join complete: rebuilt from peer-served lineage and snapshot"
    );
    Some(BootOutcome::Transitioned { epoch })
}

/// A staged chain's target record is a full genesis: the canonical block
/// derives from the record, and the lineage evidence rides along.
fn staged_epoch_genesis(target: &genesis::LineageRecord) -> Result<genesis::EpochGenesis, String> {
    let final_block = hopnet_consensus::codec::decode(&target.final_block)
        .map_err(|e| format!("target final block: {e:?}"))?;
    let final_cert = hopnet_consensus::codec::decode(&target.final_cert)
        .map_err(|e| format!("target final cert: {e:?}"))?;
    Ok(genesis::EpochGenesis {
        block: genesis::genesis_block_for(&target.record)?,
        record: target.record.clone(),
        final_block,
        final_cert,
    })
}

/// A staged input failed verification at boot after passing online: the
/// staging is corrupt, so discard it and let the next attempt refetch.
fn poison(staging: &Path, detail: String) -> BootOutcome {
    crate::regenesis::join::clear_staging(staging);
    park("staged-join", detail)
}

/// Build the fresh epoch database at `next`: schema, certified import,
/// node-local carry, genesis install and meta — ONE transaction, then
/// checkpoint and close. Any error leaves `next` to be deleted by the
/// caller; the old database is never written.
fn build_next(
    next: &Path,
    old_db_path: &str,
    artifact: &[u8],
    epoch_genesis: &genesis::EpochGenesis,
) -> Result<(), String> {
    let mut conn =
        rusqlite::Connection::open(next).map_err(|e| format!("open {}: {e}", next.display()))?;
    crate::db::shared::apply_connection_pragmas(&conn).map_err(|e| format!("pragmas: {e}"))?;
    crate::db::shared::initialize(&conn).map_err(|e| format!("schema install: {e}"))?;

    // ATTACH must precede the transaction (SQLite refuses ATTACH inside
    // one). Gate 1's exact version match is what makes blind `SELECT *`
    // carries safe: both files carry the same binary's schema.
    conn.execute("ATTACH DATABASE ?1 AS old", [old_db_path])
        .map_err(|e| format!("attach old: {e}"))?;

    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(|e| format!("tx: {e}"))?;

    // IMPORT: every section must land — a skipped section (unknown name,
    // format version mismatch) would silently drop state, so it is fatal
    // here even though the importer itself just reports it.
    let report = crate::db::snapshot::import_snapshot_tx(&tx, artifact)
        .map_err(|e| format!("import: {e}"))?;
    if !report.skipped.is_empty() {
        return Err(format!(
            "import skipped sections (refusing to cross with partial state): {:?}",
            report.skipped
        ));
    }

    // NODE-LOCAL CARRY: whole tables owned by this node — everything in
    // the node-local universe except the consensus trio (WAL and
    // certificates die with the epoch; consensus_meta is written fresh
    // below so the new epoch never inherits the sealed marker).
    for table in crate::db::snapshot::node_local_tables() {
        if hopnet_consensus::store::NODE_LOCAL_TABLES.contains(&table) {
            continue;
        }
        tx.execute(
            &format!("INSERT INTO {table} SELECT * FROM old.{table}"),
            [],
        )
        .map_err(|e| format!("carry {table}: {e}"))?;
    }
    // Node-local COLUMNS of exported tables: the import restored their
    // DDL defaults, but the local fragment store is untouched across the
    // boundary — carry by primary-key join rather than rescanning disk
    // (a rescan would reset self-verification and trigger a mesh-wide
    // decrypt/verify storm at the worst possible moment).
    tx.execute_batch(
        "
        UPDATE fragment_hashes SET stored_locally = COALESCE(
            (SELECT o.stored_locally FROM old.fragment_hashes o
             WHERE o.data_block_id = fragment_hashes.data_block_id
               AND o.chunk_number = fragment_hashes.chunk_number
               AND o.local_index = fragment_hashes.local_index),
            0);
        UPDATE fragment_inventory SET self_verified_height =
            (SELECT o.self_verified_height FROM old.fragment_inventory o
             WHERE o.fragment_hash = fragment_inventory.fragment_hash
               AND o.node_id = fragment_inventory.node_id);
    ",
    )
    .map_err(|e| format!("carry fragment columns: {e}"))?;

    // Genesis at H + fresh consensus meta. regenesis_state stays ABSENT
    // — the canonical Normal encoding: the new epoch is born open.
    let cert = genesis::synthetic_genesis_cert(&epoch_genesis.block);
    hopnet_consensus::store::install_genesis(&tx, &epoch_genesis.block, &cert)
        .map_err(|e| format!("genesis install: {e}"))?;
    hopnet_consensus::store::meta_put(
        &tx,
        hopnet_consensus::store::META_CHAIN_ID,
        epoch_genesis.block.block_hash.0.as_bytes(),
    )
    .map_err(|e| format!("chain id: {e}"))?;
    hopnet_consensus::store::meta_put(
        &tx,
        hopnet_consensus::store::META_QUORUM_PROFILE,
        epoch_genesis.record.quorum_profile.as_bytes(),
    )
    .map_err(|e| format!("quorum profile: {e}"))?;
    hopnet_consensus::store::meta_put(
        &tx,
        genesis::META_EPOCH,
        &epoch_genesis.record.epoch.to_be_bytes(),
    )
    .map_err(|e| format!("epoch meta: {e}"))?;
    hopnet_consensus::store::meta_put(
        &tx,
        genesis::META_EPOCH_GENESIS_HEIGHT,
        &epoch_genesis.record.seal_height.to_be_bytes(),
    )
    .map_err(|e| format!("genesis height meta: {e}"))?;

    // The strongest cross-check last: the fresh database must reproduce
    // the certified artifact byte-for-byte (the roundtrip gate the S1
    // tests prove, enforced at every real boundary).
    let (roundtrip, _manifest) =
        hopnet_common::snapshot::serialize_snapshot(&tx, &crate::db::snapshot::sections())
            .map_err(|e| format!("roundtrip serialize: {e}"))?;
    if *blake3::hash(&roundtrip).as_bytes() != epoch_genesis.record.snapshot_hash {
        return Err("fresh database does not reproduce the certified artifact".into());
    }

    tx.commit().map_err(|e| format!("commit: {e}"))?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|e| format!("checkpoint: {e}"))?;
    Ok(())
}

/// Epoch of the database at `path` (plain connection; used by the
/// state-C recovery where the genesis record is not in hand).
fn read_epoch_of(db_path: &str) -> Result<u64, String> {
    let conn = rusqlite::Connection::open(db_path).map_err(|e| format!("open: {e}"))?;
    Ok(genesis::current_epoch(&conn))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use hopnet_consensus::codec::WireCommitCertificate;
    use hopnet_consensus::context::Height;
    use hopnet_consensus::types::{Blake3Hash, Block, BlockData, PrivKey, Transactions};
    use hopnet_consensus::verify::wire_commit_signature;
    use rusqlite::params;

    const H: u64 = 7;
    pub(crate) const TARGET: u32 = 20260800;
    const PREV_CHAIN: [u8; 32] = [3; 32];

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pubkey_blob(k: &SigningKey) -> Vec<u8> {
        bincode::serde::encode_to_vec(k.verifying_key(), bincode::config::standard()).unwrap()
    }

    /// A file-backed sealed epoch-1 database: two seated validators with
    /// a really-signed final certificate, node-local rows to carry, one
    /// fragment with node-local column state, the sealed marker, and a
    /// committed snapshot_hash that matches the real artifact recompute.
    /// Shared with the sibling regenesis test modules (rpc, join).
    pub(crate) fn sealed_db(dir: &Path) -> String {
        let db_path = dir.join("database.db").to_string_lossy().into_owned();
        let mut conn = rusqlite::Connection::open(&db_path).unwrap();
        crate::db::shared::apply_connection_pragmas(&conn).unwrap();
        crate::db::shared::initialize(&conn).unwrap();

        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'test', ?, ?, ?, ?)",
            params![pubkey_blob(&key(9)), vec![0u8; 32], vec![0u8; 44], vec![0u8; 16]],
        )
        .unwrap();
        for id in [1i32, 2i32] {
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                params![id, format!("node{id}"), pubkey_blob(&key(id as u8))],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (1, ?, 1)",
                params![id],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (1, 1, ?)",
            params![vec![7u8; 32]],
        )
        .unwrap();

        // One blob with a locally-stored fragment: exported rows plus
        // node-local column state the carry must preserve.
        conn.execute(
            "INSERT INTO data_blocks (id, modified_at, file_hash, fragment_count, added_bytes, placement_height, file_size)
             VALUES ('blob1', 'now', ?, 1, 100, 3, 100)",
            params![vec![1u8; 32]],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally)
             VALUES ('blob1', 0, 0, 'frag1', ?, 0, 1)",
            params![vec![4u8; 32]],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height)
             VALUES (?, 1, 5)",
            params![vec![4u8; 32]],
        )
        .unwrap();

        hopnet_consensus::store::meta_put(
            &conn,
            hopnet_consensus::store::META_CHAIN_ID,
            &PREV_CHAIN,
        )
        .unwrap();
        hopnet_consensus::store::meta_put(
            &conn,
            hopnet_consensus::store::META_QUORUM_PROFILE,
            b"majority",
        )
        .unwrap();

        let final_block = Block::new(BlockData {
            height: H,
            round: 0,
            parent_hash: Some(Blake3Hash::from_bytes([2; 32])),
            transactions: Transactions(Vec::new()),
        })
        .unwrap();
        let chain = Blake3Hash::from_bytes(PREV_CHAIN);
        let cert = WireCommitCertificate {
            height: H,
            round: 0,
            value_id: final_block.block_hash,
            signatures: vec![
                wire_commit_signature(
                    &chain,
                    &PrivKey(key(1)),
                    Height(H),
                    final_block.block_hash,
                    1,
                ),
                wire_commit_signature(
                    &chain,
                    &PrivKey(key(2)),
                    Height(H),
                    final_block.block_hash,
                    2,
                ),
            ],
        };
        hopnet_consensus::store::install_genesis(&conn, &final_block, &cert).unwrap();

        // Sealed committed state with the REAL artifact identity, then
        // the node-local marker — the exact end state S5's seal leaves.
        conn.execute(
            "INSERT INTO regenesis_state (internal_id, phase, target_version_code, snapshot_hash, seal_height)
             VALUES (1, 2, ?, ?, ?)",
            params![TARGET, vec![0u8; 32], H as i64],
        )
        .unwrap();
        let real_hash = {
            let tx = conn.transaction().unwrap();
            let h = crate::db::snapshot::compute_artifact_hash_tx(&tx).unwrap();
            tx.commit().unwrap();
            h
        };
        conn.execute(
            "UPDATE regenesis_state SET snapshot_hash = ?",
            params![real_hash.as_bytes().to_vec()],
        )
        .unwrap();
        hopnet_consensus::store::meta_put(&conn, seal::META_SEALED_AT, &H.to_be_bytes()).unwrap();

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);
        db_path
    }

    fn open(db_path: &str) -> rusqlite::Connection {
        let conn = rusqlite::Connection::open(db_path).unwrap();
        crate::db::shared::apply_connection_pragmas(&conn).unwrap();
        conn
    }

    fn count(conn: &rusqlite::Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    // ------------------------------------------------------------------
    // Staged epoch join (RFC-019 S7): the boot half of a rejoin.
    // ------------------------------------------------------------------

    use crate::regenesis::join::{self, StagedJoinManifest};

    /// A "mesh" database that already crossed into epoch 2, and the
    /// staged inputs a straggler would have downloaded from it.
    struct JoinFixture {
        /// The straggler's own (unsealed, epoch-1) database.
        db_path: String,
        staging: PathBuf,
        target_epoch: u64,
        snapshot: Vec<u8>,
        lineage: Vec<u8>,
    }

    /// Build a straggler beside a transitioned peer: the straggler is the
    /// S6 sealed fixture with its seal cleared (it simply slept through
    /// the boundary), and staging holds the peer's real lineage record
    /// and artifact.
    fn join_fixture(client_dir: &Path, server_dir: &Path) -> JoinFixture {
        let server = sealed_db(server_dir);
        match boot_transition(&server, TARGET) {
            BootOutcome::Transitioned { epoch: 2 } => {}
            other => panic!("server fixture failed: {other:?}"),
        }
        // The transition leaves no artifact file; ask the serving path
        // for it exactly as a joiner would.
        match crate::regenesis::rpc::serve_request(
            &server,
            crate::regenesis::rpc::RegenesisNetRequest::SnapshotInfo { epoch: 2 },
        ) {
            crate::regenesis::rpc::RegenesisNetResponse::SnapshotInfo { .. } => {}
            other => panic!("server cannot serve its snapshot: {other:?}"),
        }
        let snapshot = std::fs::read(server_dir.join(seal::SEAL_ARTIFACT_FILENAME)).unwrap();
        let lineage = std::fs::read(genesis::lineage_path(server_dir, 2)).unwrap();

        let db_path = sealed_db(client_dir);
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            crate::db::shared::apply_connection_pragmas(&conn).unwrap();
            conn.execute("DELETE FROM regenesis_state", []).unwrap();
            conn.execute(
                "DELETE FROM consensus_meta WHERE key = ?",
                [seal::META_SEALED_AT],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
        }

        let staging = join::staging_path(&db_path);
        JoinFixture {
            db_path,
            staging,
            target_epoch: 2,
            snapshot,
            lineage,
        }
    }

    impl JoinFixture {
        /// Write a complete staging (manifest last), as the online join
        /// would leave it.
        fn stage(&self, manual_anchor: bool) {
            self.stage_with(manual_anchor, &self.snapshot, &self.lineage);
        }

        fn stage_with(&self, manual_anchor: bool, snapshot: &[u8], lineage: &[u8]) {
            std::fs::create_dir_all(&self.staging).unwrap();
            std::fs::write(self.staging.join("epoch-2.bin"), lineage).unwrap();
            std::fs::write(join::staged_snapshot_path(&self.staging), snapshot).unwrap();
            let record = genesis::decode_lineage(&self.lineage).unwrap().record;
            let manifest = StagedJoinManifest {
                format_version: 1,
                from_epoch: 1,
                target_epoch: self.target_epoch,
                lineage_epochs: vec![2],
                required_version_code: record.required_version_code,
                snapshot_hash: record.snapshot_hash,
                snapshot_len: snapshot.len() as u64,
                manual_anchor,
            };
            let bytes =
                bincode::serde::encode_to_vec(&manifest, bincode::config::standard()).unwrap();
            std::fs::write(join::manifest_path(&self.staging), bytes).unwrap();
        }
    }

    // Impact: this is the straggler's rebuild — a node that missed the
    // boundary entirely ends up byte-for-byte on the new epoch's
    // certified state, keeping its own identity and fragment bookkeeping.
    // Should: cross into the target epoch from staged inputs, retain the
    // old database for rollback, persist the lineage record, and clear
    // the staging.
    #[test]
    fn staged_join_crosses_the_boundary() {
        let client = tempfile::tempdir().unwrap();
        let server = tempfile::tempdir().unwrap();
        let fx = join_fixture(client.path(), server.path());
        fx.stage(false);

        match boot_transition(&fx.db_path, TARGET) {
            BootOutcome::Transitioned { epoch: 2 } => {}
            other => panic!("expected a staged crossing, got {other:?}"),
        }

        let conn = open(&fx.db_path);
        assert_eq!(genesis::current_epoch(&conn), 2);
        assert_eq!(genesis::epoch_genesis_height(&conn), Some(H));
        // The genesis sits at H and the old epoch's history is gone.
        assert_eq!(
            hopnet_consensus::store::last_decided_height(&conn).unwrap(),
            Some(Height(H))
        );
        // Node-local identity carried; the seal did NOT.
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM this_node"), 1);
        assert!(seal::sealed_marker(&conn).is_none());
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM regenesis_state"), 0);

        assert!(
            sealed_path(&fx.db_path).exists(),
            "rollback window retained"
        );
        assert!(
            genesis::lineage_path(client.path(), 2).exists(),
            "lineage kept"
        );
        assert!(!fx.staging.exists(), "staging cleared after the crossing");
    }

    // Impact: an upgrade-boundary straggler running the old binary must
    // never build the new epoch's database — the node-local carry copies
    // rows blind, and only the exact-version gate makes that safe.
    // Should: park awaiting upgrade and KEEP the staged inputs, then
    // cross once the right binary boots.
    // Should not: touch the live database while parked.
    #[test]
    fn staged_join_version_gate_parks_and_keeps_staging() {
        let client = tempfile::tempdir().unwrap();
        let server = tempfile::tempdir().unwrap();
        let fx = join_fixture(client.path(), server.path());
        fx.stage(false);
        let before = std::fs::read(&fx.db_path).unwrap();

        match boot_transition(&fx.db_path, TARGET + 100) {
            BootOutcome::Parked(ParkReason::AwaitingUpgrade { required, .. }) => {
                assert_eq!(required, TARGET)
            }
            other => panic!("expected an awaiting-upgrade park, got {other:?}"),
        }
        assert!(join::read_manifest(&fx.staging).is_some(), "staging kept");
        assert_eq!(
            std::fs::read(&fx.db_path).unwrap(),
            before,
            "database untouched"
        );
        assert!(awaiting_upgrade_path(&fx.db_path).exists());

        // The upgraded binary completes it.
        match boot_transition(&fx.db_path, TARGET) {
            BootOutcome::Transitioned { epoch: 2 } => {}
            other => panic!("expected the crossing after upgrade, got {other:?}"),
        }
        assert!(!awaiting_upgrade_path(&fx.db_path).exists());
    }

    // Impact: staged bytes passed verification once online, so a failure
    // at boot means the staging rotted on disk — retrying it forever
    // would wedge the node.
    // Should: park and DISCARD the staging so the next attempt refetches.
    #[test]
    fn corrupt_staging_is_discarded() {
        for (name, corrupt) in [("snapshot", true), ("lineage", false)] {
            let client = tempfile::tempdir().unwrap();
            let server = tempfile::tempdir().unwrap();
            let fx = join_fixture(client.path(), server.path());
            if corrupt {
                let mut bad = fx.snapshot.clone();
                bad[0] ^= 0xFF;
                fx.stage_with(false, &bad, &fx.lineage);
            } else {
                let mut bad = genesis::decode_lineage(&fx.lineage).unwrap();
                bad.record.prev_chain_id = [0x5A; 32];
                let bytes =
                    bincode::serde::encode_to_vec(&bad, bincode::config::standard()).unwrap();
                fx.stage_with(false, &fx.snapshot, &bytes);
            }
            let before = std::fs::read(&fx.db_path).unwrap();

            match boot_transition(&fx.db_path, TARGET) {
                BootOutcome::Parked(ParkReason::GateFailed { gate, .. }) => {
                    assert_eq!(gate, "staged-join", "{name}")
                }
                other => panic!("{name}: expected a park, got {other:?}"),
            }
            assert!(!fx.staging.exists(), "{name}: staging discarded");
            assert_eq!(
                std::fs::read(&fx.db_path).unwrap(),
                before,
                "{name}: database untouched"
            );
        }
    }

    // Impact: an operator re-trust is the escape hatch when churn moved
    // past the overlap window — it must waive weak subjectivity and
    // nothing else.
    // Should: cross when the staged chain overlaps nothing this node
    // trusted, provided the manifest says an operator asked for it.
    // Should not: cross on the same inputs without that flag.
    #[test]
    fn manual_anchor_waives_only_the_overlap() {
        let client = tempfile::tempdir().unwrap();
        let server = tempfile::tempdir().unwrap();
        let fx = join_fixture(client.path(), server.path());

        // Re-seat the straggler under validators the boundary
        // certificate knows nothing about: churn beyond overlap.
        {
            let conn = rusqlite::Connection::open(&fx.db_path).unwrap();
            crate::db::shared::apply_connection_pragmas(&conn).unwrap();
            conn.execute("DELETE FROM validators", []).unwrap();
            for id in [8i32, 9i32] {
                conn.execute(
                    "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                    params![id, format!("node{id}"), pubkey_blob(&key(id as u8))],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO validators (effective_height, node_id, is_active) VALUES (1, ?, 1)",
                    params![id],
                )
                .unwrap();
            }
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
        }

        fx.stage(false);
        match boot_transition(&fx.db_path, TARGET) {
            BootOutcome::Parked(ParkReason::GateFailed { gate, detail }) => {
                assert_eq!(gate, "staged-join");
                assert!(detail.contains("overlap"), "{detail}");
            }
            other => panic!("expected an overlap refusal, got {other:?}"),
        }

        fx.stage(true);
        match boot_transition(&fx.db_path, TARGET) {
            BootOutcome::Transitioned { epoch: 2 } => {}
            other => panic!("expected the operator-anchored crossing, got {other:?}"),
        }
    }

    // Impact: a download interrupted by a crash is the common case on a
    // bad link — deleting the partial would restart it from zero every
    // boot.
    // Should: leave an incomplete staging alone and boot normally.
    #[test]
    fn incomplete_staging_survives_a_normal_boot() {
        let client = tempfile::tempdir().unwrap();
        let server = tempfile::tempdir().unwrap();
        let fx = join_fixture(client.path(), server.path());
        std::fs::create_dir_all(&fx.staging).unwrap();
        let partial = fx.staging.join("snapshot.bin.partial");
        std::fs::write(&partial, &fx.snapshot[..64]).unwrap();

        match boot_transition(&fx.db_path, TARGET) {
            BootOutcome::NoBoundary => {}
            other => panic!("expected a normal boot, got {other:?}"),
        }
        assert!(partial.exists(), "partial download kept for resume");
    }

    // Impact: the swap happens before the cleanup, so a crash in between
    // leaves staging behind on an already-transitioned database.
    // Should: discard leftover staging for an epoch already reached and
    // boot normally.
    #[test]
    fn staging_for_an_epoch_already_reached_is_cleared() {
        let client = tempfile::tempdir().unwrap();
        let server = tempfile::tempdir().unwrap();
        let fx = join_fixture(client.path(), server.path());
        fx.stage(false);
        assert!(matches!(
            boot_transition(&fx.db_path, TARGET),
            BootOutcome::Transitioned { epoch: 2 }
        ));

        // Re-stage the same (now redundant) inputs and boot again.
        fx.stage(false);
        match boot_transition(&fx.db_path, TARGET) {
            BootOutcome::NoBoundary => {}
            other => panic!("expected a normal boot, got {other:?}"),
        }
        assert!(!fx.staging.exists(), "redundant staging cleared");
    }

    // ------------------------------------------------------------------
    // Rollback: abandoning a boundary (RFC-019 S8).
    // ------------------------------------------------------------------

    /// A node that has CROSSED: the S6 fixture put through the real
    /// transition, so `database.db` is epoch 2 and `.sealed` is the
    /// retained epoch-1 database.
    fn crossed(dir: &Path) -> String {
        let db_path = sealed_db(dir);
        match boot_transition(&db_path, TARGET) {
            BootOutcome::Transitioned { epoch: 2 } => {}
            other => panic!("fixture failed to cross: {other:?}"),
        }
        db_path
    }

    fn seal_state_present(db_path: &str) -> (bool, i64) {
        let conn = open(db_path);
        (
            seal::sealed_marker(&conn).is_some(),
            count(&conn, "SELECT COUNT(*) FROM regenesis_state"),
        )
    }

    // Impact: this is the escape hatch the spec promises for a bad epoch,
    // and until now nothing had ever exercised it. A rollback that leaves
    // the seal state behind is not a rollback — see the sibling test.
    // Should: restore the retained database, return to the previous
    // epoch, and clear both the sealed marker and the committed phase.
    // Should not: keep the abandoned epoch's database or lineage record.
    #[test]
    fn rollback_restores_the_retained_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = crossed(dir.path());
        assert!(sealed_path(&db_path).exists());
        assert!(genesis::lineage_path(dir.path(), 2).exists());

        write_rollback_marker(&db_path);
        match boot_transition(&db_path, TARGET) {
            BootOutcome::RolledBack { epoch: 1 } => {}
            other => panic!("expected a rollback to epoch 1, got {other:?}"),
        }

        let conn = open(&db_path);
        assert_eq!(genesis::current_epoch(&conn), 1);
        assert_eq!(
            hopnet_consensus::store::last_decided_height(&conn).unwrap(),
            Some(Height(H)),
            "back on the epoch-1 tip"
        );
        assert_eq!(seal_state_present(&db_path), (false, 0));
        assert!(
            !sealed_path(&db_path).exists(),
            "retained database consumed"
        );
        assert!(
            !genesis::lineage_path(dir.path(), 2).exists(),
            "the abandoned boundary is not part of this mesh's history"
        );
        assert!(!rollback_marker_path(&db_path).exists());
        assert!(!awaiting_upgrade_path(&db_path).exists());
    }

    // Impact: THIS is the bug that made a first-class mechanism
    // necessary. Restoring the retained file by hand leaves the sealed
    // marker and the committed Sealed phase in place, so the next boot
    // re-runs the gates and silently crosses again — undoing the
    // rollback with no diagnostic at all.
    // Should: leave a rolled-back database that boots normally, forever.
    // Should not: re-cross the boundary on any subsequent boot.
    #[test]
    fn a_rolled_back_database_does_not_re_cross() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = crossed(dir.path());
        write_rollback_marker(&db_path);
        assert!(matches!(
            boot_transition(&db_path, TARGET),
            BootOutcome::RolledBack { epoch: 1 }
        ));

        // Two more boots on the same binary that originally crossed.
        for _ in 0..2 {
            match boot_transition(&db_path, TARGET) {
                BootOutcome::NoBoundary => {}
                other => panic!("a rolled-back node must boot normally, got {other:?}"),
            }
        }
        let conn = open(&db_path);
        assert_eq!(genesis::current_epoch(&conn), 1);
        assert!(!sealed_path(&db_path).exists());

        // For contrast: the same restore WITHOUT clearing the seal state
        // is what the old documented `mv` amounted to, and it re-crosses.
        let dir2 = tempfile::tempdir().unwrap();
        let db2 = crossed(dir2.path());
        std::fs::remove_file(&db2).unwrap();
        std::fs::rename(sealed_path(&db2), &db2).unwrap();
        assert!(
            matches!(
                boot_transition(&db2, TARGET),
                BootOutcome::Transitioned { epoch: 2 }
            ),
            "a bare file move re-crosses — which is why the marker exists"
        );
    }

    // Impact: on an upgrade boundary the nodes that park never cross, so
    // a whole-mesh rollback has to work for them too — otherwise the
    // mesh is left half-abandoned and frozen.
    // Should: clear the seal state in place when there is no retained
    // database, returning the node to a normal epoch-1 boot.
    #[test]
    fn rollback_clears_a_parked_node_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());
        // Sealed for a version this binary does not run: parked, never
        // crossed, so nothing is retained.
        assert!(matches!(
            boot_transition(&db_path, TARGET + 100),
            BootOutcome::Parked(ParkReason::AwaitingUpgrade { .. })
        ));
        assert!(!sealed_path(&db_path).exists());
        assert_eq!(seal_state_present(&db_path), (true, 1));

        write_rollback_marker(&db_path);
        match boot_transition(&db_path, TARGET + 100) {
            BootOutcome::RolledBack { epoch: 1 } => {}
            other => panic!("expected an in-place rollback, got {other:?}"),
        }
        assert_eq!(seal_state_present(&db_path), (false, 0));
        // And it now boots normally even on the version it was sealed for.
        assert!(matches!(
            boot_transition(&db_path, TARGET),
            BootOutcome::NoBoundary
        ));
    }

    // Impact: the marker is deleted last precisely so a crash mid-restore
    // resumes rather than wedging — and the half-restored arrangement is
    // exactly the one state D would otherwise call fatal.
    // Should: complete the rollback from either interruption point.
    #[test]
    fn rollback_resumes_after_a_crash() {
        // Crash after discarding the live database, before the rename.
        let dir = tempfile::tempdir().unwrap();
        let db_path = crossed(dir.path());
        write_rollback_marker(&db_path);
        remove_with_sidecars(Path::new(&db_path));
        assert!(sealed_path(&db_path).exists());
        match boot_transition(&db_path, TARGET) {
            BootOutcome::RolledBack { epoch: 1 } => {}
            other => panic!("expected a resumed rollback, got {other:?}"),
        }
        assert_eq!(seal_state_present(&db_path), (false, 0));

        // Crash after the rename, before the seal state was cleared.
        let dir2 = tempfile::tempdir().unwrap();
        let db2 = crossed(dir2.path());
        write_rollback_marker(&db2);
        std::fs::remove_file(&db2).unwrap();
        std::fs::rename(sealed_path(&db2), &db2).unwrap();
        assert_eq!(
            seal_state_present(&db2),
            (true, 1),
            "seal state still there"
        );
        match boot_transition(&db2, TARGET) {
            BootOutcome::RolledBack { epoch: 1 } => {}
            other => panic!("expected the in-place completion, got {other:?}"),
        }
        assert_eq!(seal_state_present(&db2), (false, 0));
    }

    // Impact: a stale or mistaken rollback request must not be
    // destructive — the window closing is the forward-only clause, and
    // past it there is nothing to go back to.
    // Should: refuse, drop the marker, surface the reason, boot normally.
    // Should not: touch the database.
    #[test]
    fn rollback_refused_when_there_is_no_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = crossed(dir.path());
        // The window closed: the cleanup task deleted the retained file.
        std::fs::remove_file(sealed_path(&db_path)).unwrap();
        let before = std::fs::read(&db_path).unwrap();

        write_rollback_marker(&db_path);
        match boot_transition(&db_path, TARGET) {
            BootOutcome::NoBoundary => {}
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert!(!rollback_marker_path(&db_path).exists(), "marker dropped");
        assert_eq!(
            std::fs::read(&db_path).unwrap(),
            before,
            "database untouched"
        );
        assert!(
            boundary_error().is_some_and(|e| e.contains("no boundary to abandon")),
            "the refusal is surfaced"
        );
    }

    // Impact: a staged join would otherwise carry a just-rolled-back node
    // straight back across the boundary it was told to abandon.
    // Should: clear join staging as part of the rollback.
    #[test]
    fn rollback_clears_staged_join_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = crossed(dir.path());
        let staging = crate::regenesis::join::staging_path(&db_path);
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("snapshot.bin.partial"), b"stale").unwrap();

        write_rollback_marker(&db_path);
        assert!(matches!(
            boot_transition(&db_path, TARGET),
            BootOutcome::RolledBack { epoch: 1 }
        ));
        assert!(!staging.exists(), "staged join discarded with the boundary");
    }

    // Should: report a boundary as available to abandon while retained or
    // sealed, and not otherwise — the route's precondition.
    #[test]
    fn rollback_availability_tracks_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = sealed_db(dir.path());
        // Sealed but not yet crossed.
        assert!(rollback_available(&sealed, &open(&sealed)));

        let dir2 = tempfile::tempdir().unwrap();
        let db2 = crossed(dir2.path());
        // Crossed, window open (retained database present).
        assert!(rollback_available(&db2, &open(&db2)));
        // Window closed.
        std::fs::remove_file(sealed_path(&db2)).unwrap();
        assert!(!rollback_available(&db2, &open(&db2)));
    }

    // Impact: this is the boundary itself — every assertion here is a
    // clause of the spec's Restart & Validity Gates section.
    // Should: cross the boundary on a version match — fresh database at
    // epoch 2 with the genesis at H, carried meta and node-local state,
    // no inherited seal, and the old database retained for rollback.
    // Should not: treat the transitioned state as a boundary again on
    // the next boot, nor delete the retained database (the rollback
    // window belongs to the cleanup task).
    #[test]
    fn happy_path_crosses_the_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());

        let outcome = boot_transition(&db_path, TARGET);
        let BootOutcome::Transitioned { epoch } = outcome else {
            panic!("expected Transitioned, got {outcome:?}");
        };
        assert_eq!(epoch, 2);
        assert!(sealed_path(&db_path).exists(), "old database retained");
        assert!(!next_path(&db_path).exists());

        let conn = open(&db_path);
        assert_eq!(genesis::current_epoch(&conn), 2);
        assert_eq!(genesis::epoch_genesis_height(&conn), Some(H));
        assert_eq!(
            hopnet_consensus::store::last_decided_height(&conn).unwrap(),
            Some(Height(H))
        );
        // New signing domain: chain id is the genesis block hash.
        let chain =
            hopnet_consensus::store::meta_get(&conn, hopnet_consensus::store::META_CHAIN_ID)
                .unwrap()
                .unwrap();
        assert_ne!(chain.as_slice(), &PREV_CHAIN[..]);
        let profile =
            hopnet_consensus::store::meta_get(&conn, hopnet_consensus::store::META_QUORUM_PROFILE)
                .unwrap()
                .unwrap();
        assert_eq!(profile, b"majority");
        // Born open: no committed boundary state, no node-local marker.
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM regenesis_state"), 0);
        assert!(seal::sealed_marker(&conn).is_none());
        // The genesis pair is the ONLY decided state; the WAL is empty.
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM decided_blocks"), 1);
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM decided_certificates"), 1);
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM consensus_wal"), 0);
        // Node-local carry: identity and fragment column state survive.
        let (nid, privkey): (i32, Vec<u8>) = conn
            .query_row("SELECT node_id, privkey FROM this_node", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!((nid, privkey), (1, vec![7u8; 32]));
        assert_eq!(
            count(&conn, "SELECT stored_locally FROM fragment_hashes"),
            1
        );
        assert_eq!(
            count(&conn, "SELECT self_verified_height FROM fragment_inventory"),
            5
        );
        // Replicated state imported.
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM nodes"), 2);
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM users"), 1);
        // Lineage record readable, forever.
        let lineage = genesis::read_lineage(&genesis::lineage_path(dir.path(), 2)).unwrap();
        assert_eq!(lineage.record.epoch, 2);
        drop(conn);

        // Next boot: state A — normal, and the retained file is kept.
        let again = boot_transition(&db_path, TARGET);
        assert!(matches!(again, BootOutcome::NoBoundary), "got {again:?}");
        assert!(sealed_path(&db_path).exists());
    }

    // Should: park awaiting upgrade on a version mismatch — marker file
    // written with the required version, database untouched — and cross
    // normally once the running version matches (marker cleaned).
    #[test]
    fn version_mismatch_parks_then_upgrade_crosses() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());

        let outcome = boot_transition(&db_path, TARGET + 1);
        assert!(
            matches!(
                outcome,
                BootOutcome::Parked(ParkReason::AwaitingUpgrade {
                    required: TARGET,
                    ..
                })
            ),
            "got {outcome:?}"
        );
        let marker = awaiting_upgrade_path(&db_path);
        assert_eq!(
            std::fs::read_to_string(&marker).unwrap(),
            crate::version::format_code(TARGET)
        );
        // Still sealed, still epoch 1, no build residue.
        let conn = open(&db_path);
        assert_eq!(seal::sealed_marker(&conn), Some(H));
        assert_eq!(genesis::current_epoch(&conn), 1);
        drop(conn);
        assert!(!next_path(&db_path).exists());

        // The "binary swap": the matching version crosses and cleans up.
        let outcome = boot_transition(&db_path, TARGET);
        assert!(matches!(outcome, BootOutcome::Transitioned { epoch: 2 }));
        assert!(!marker.exists());
    }

    // Impact: "nothing is ever lost by a failed gate" — a refused
    // boundary must leave the sealed database byte-identical so abort/
    // retry/manual recovery all remain possible.
    // Should: park on an import-gate failure (diverged replica: committed
    // hash matches no recomputation) with the old database unchanged.
    #[test]
    fn import_gate_failure_leaves_old_database_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());
        // Corrupt the committed identity: no artifact (file or recompute)
        // can ever match — this replica is "diverged".
        {
            let conn = open(&db_path);
            conn.execute(
                "UPDATE regenesis_state SET snapshot_hash = ?",
                params![vec![9u8; 32]],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
        }
        let before = std::fs::read(&db_path).unwrap();

        let outcome = boot_transition(&db_path, TARGET);
        assert!(
            matches!(
                outcome,
                BootOutcome::Parked(ParkReason::GateFailed { gate: "import", .. })
            ),
            "got {outcome:?}"
        );
        assert!(!next_path(&db_path).exists(), "failed build cleaned up");
        assert!(!sealed_path(&db_path).exists(), "no retain on failure");
        assert_eq!(std::fs::read(&db_path).unwrap(), before, "old db untouched");
        // Surface populated for the status route (content asserted via
        // the ParkReason above — the global races with parallel tests).
        assert!(boundary_error().is_some());
    }

    // Should: park on lineage-gate failure — a certificate that does not
    // verify against the trusted seated set refuses the boundary.
    #[test]
    fn lineage_gate_refuses_bad_certificate() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());
        // Swap node 1's pubkey: the stored certificate's signature no
        // longer verifies against the (now different) seated set.
        {
            let conn = open(&db_path);
            conn.execute(
                "UPDATE nodes SET pubkey = ? WHERE node_id = 1",
                params![pubkey_blob(&key(8))],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
        }
        let outcome = boot_transition(&db_path, TARGET);
        assert!(
            matches!(
                outcome,
                BootOutcome::Parked(ParkReason::GateFailed {
                    gate: "lineage",
                    ..
                })
            ),
            "got {outcome:?}"
        );
    }

    // Impact: the two renames cannot be atomic together; the recovery of
    // the window between them is what makes the swap crash-safe.
    // Should: complete an interrupted swap (live db missing, complete
    // .next + retained .sealed present) without re-running the gates.
    #[test]
    fn state_c_completes_interrupted_swap() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());
        assert!(matches!(
            boot_transition(&db_path, TARGET),
            BootOutcome::Transitioned { epoch: 2 }
        ));
        // Reconstruct the between-renames state: the new database back
        // to .next, the retained old database still in place.
        std::fs::rename(&db_path, next_path(&db_path)).unwrap();
        assert!(sealed_path(&db_path).exists());

        let outcome = boot_transition(&db_path, TARGET);
        assert!(
            matches!(outcome, BootOutcome::Transitioned { epoch: 2 }),
            "got {outcome:?}"
        );
        assert!(Path::new(&db_path).exists());
        assert!(!next_path(&db_path).exists());
        let conn = open(&db_path);
        assert_eq!(genesis::current_epoch(&conn), 2);
    }

    // Should: refuse to boot fresh when only the retained database
    // remains — that state needs an operator, not an empty mesh identity.
    #[test]
    fn missing_live_db_with_retained_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = sealed_db(dir.path());
        std::fs::rename(&db_path, sealed_path(&db_path)).unwrap();
        assert!(matches!(
            boot_transition(&db_path, TARGET),
            BootOutcome::Fatal(_)
        ));
    }

    // Should: treat an unsealed database as a normal boot and clean the
    // residue of any crashed earlier transition (stale .next, marker).
    #[test]
    fn normal_boot_cleans_stale_residue() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir
            .path()
            .join("database.db")
            .to_string_lossy()
            .into_owned();
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            crate::db::shared::apply_connection_pragmas(&conn).unwrap();
            crate::db::shared::initialize(&conn).unwrap();
        }
        std::fs::write(next_path(&db_path), b"garbage").unwrap();
        std::fs::write(awaiting_upgrade_path(&db_path), b"2026.8.0").unwrap();

        assert!(matches!(
            boot_transition(&db_path, TARGET),
            BootOutcome::NoBoundary
        ));
        assert!(!next_path(&db_path).exists());
        assert!(!awaiting_upgrade_path(&db_path).exists());
    }

    // Should: do nothing on a fresh node (no database at all).
    #[test]
    fn fresh_node_is_no_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir
            .path()
            .join("database.db")
            .to_string_lossy()
            .into_owned();
        assert!(matches!(
            boot_transition(&db_path, TARGET),
            BootOutcome::NoBoundary
        ));
        assert!(!Path::new(&db_path).exists());
    }
}
