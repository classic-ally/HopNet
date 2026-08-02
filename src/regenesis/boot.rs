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

#[derive(Debug)]
pub enum BootOutcome {
    /// No boundary pending: normal boot.
    NoBoundary,
    /// The boundary was crossed: `database.db` is now the epoch-`epoch`
    /// database and the engine will start at H+1.
    Transitioned { epoch: u64 },
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
            return BootOutcome::Fatal(format!(
                "database.db is missing but {} exists — manual recovery required \
                 (to roll back: mv {} {db_path})",
                sealed.display(),
                sealed.display()
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
    if seal::sealed_marker(&conn).is_none() {
        // State A: normal boot. Clean anything a crashed transition or a
        // completed upgrade left behind — but NEVER the retained
        // database (rollback window; the cleanup task owns it).
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
    let lineage_dir = Path::new(db_path).parent().unwrap_or_else(|| Path::new("."));
    if let Err(e) = genesis::write_lineage(lineage_dir, &epoch_genesis) {
        remove_with_sidecars(&next);
        return park("import", format!("lineage write: {e}"));
    }

    // Swap. Checkpoint and close the old database first so its WAL is
    // empty and its sidecars can be dropped — a leftover database.db-wal
    // would otherwise be adopted by the NEW database.db after rename 2.
    if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
        remove_with_sidecars(&next);
        return park("import", format!("old database checkpoint: {e}"));
    }
    drop(conn);
    for s in sidecars(db) {
        let _ = std::fs::remove_file(s);
    }
    if let Err(e) = std::fs::rename(db, &sealed) {
        remove_with_sidecars(&next);
        return park("import", format!("retain rename: {e}"));
    }
    if let Err(e) = std::fs::rename(&next, db) {
        // Recoverable on next boot as state C — do not touch anything.
        return BootOutcome::Fatal(format!(
            "epoch swap interrupted after retain: rename {} -> {db_path}: {e} \
             (next boot completes the swap)",
            next.display()
        ));
    }
    let _ = std::fs::remove_file(&awaiting);

    let epoch = epoch_genesis.record.epoch;
    tracing::info!(
        epoch,
        seal_height = epoch_genesis.record.seal_height,
        chain_id = %epoch_genesis.block.block_hash,
        "epoch boundary crossed: fresh database installed, engine will start at H+1"
    );
    BootOutcome::Transitioned { epoch }
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
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use hopnet_consensus::context::Height;
    use hopnet_consensus::types::{Blake3Hash, Block, BlockData, PrivKey, Transactions};
    use hopnet_consensus::codec::WireCommitCertificate;
    use hopnet_consensus::verify::wire_commit_signature;
    use rusqlite::params;

    const H: u64 = 7;
    const TARGET: u32 = 20260800;
    const PREV_CHAIN: [u8; 32] = [3; 32];

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pubkey_blob(k: &SigningKey) -> Vec<u8> {
        bincode::serde::encode_to_vec(&k.verifying_key(), bincode::config::standard()).unwrap()
    }

    /// A file-backed sealed epoch-1 database: two seated validators with
    /// a really-signed final certificate, node-local rows to carry, one
    /// fragment with node-local column state, the sealed marker, and a
    /// committed snapshot_hash that matches the real artifact recompute.
    fn sealed_db(dir: &Path) -> String {
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

        hopnet_consensus::store::meta_put(&conn, hopnet_consensus::store::META_CHAIN_ID, &PREV_CHAIN)
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
                wire_commit_signature(&chain, &PrivKey(key(1)), Height(H), final_block.block_hash, 1),
                wire_commit_signature(&chain, &PrivKey(key(2)), Height(H), final_block.block_hash, 2),
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

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
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
        let chain = hopnet_consensus::store::meta_get(&conn, hopnet_consensus::store::META_CHAIN_ID)
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
                BootOutcome::Parked(ParkReason::AwaitingUpgrade { required: TARGET, .. })
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
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
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
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        }
        let outcome = boot_transition(&db_path, TARGET);
        assert!(
            matches!(
                outcome,
                BootOutcome::Parked(ParkReason::GateFailed { gate: "lineage", .. })
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
        assert!(matches!(outcome, BootOutcome::Transitioned { epoch: 2 }), "got {outcome:?}");
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
        let db_path = dir.path().join("database.db").to_string_lossy().into_owned();
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
        let db_path = dir.path().join("database.db").to_string_lossy().into_owned();
        assert!(matches!(
            boot_transition(&db_path, TARGET),
            BootOutcome::NoBoundary
        ));
        assert!(!Path::new(&db_path).exists());
    }
}
