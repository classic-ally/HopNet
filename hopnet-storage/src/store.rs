//! Substrate state: apply functions for the blob control plane (RFC-014).
//!
//! These run INSIDE the host's one-SQLite-transaction consensus apply
//! (`Application::apply_block` → handler → here) — the same crate/host
//! relationship as hopnet-consensus's `install_genesis`. The main crate
//! keeps thin inventory-registered shim handlers (envelope decode,
//! authorization, projection half); the substrate half of every blob
//! transaction lands through this module.
//!
//! stored_locally invariant: `fragment_hashes.stored_locally` is a
//! NODE-LOCAL value inside a consensus-replicated table — each node probes
//! its own disk during apply. This is legal ONLY because the divergence
//! checker excludes the column from state hashing; nothing derived from the
//! probe may ever feed replicated, hashed state.

use crate::error::StorageError;
use crate::fragstore;
use crate::types::{BlobAccess, BlobId, SelfCheckFragments};
use hopnet_common::height::{height_from_db, height_to_db};
use hopnet_common::Blake3Hash;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// One fragment's replicated metadata (the substrate half of the legacy
/// FragmentHash — stored_locally is probed at apply, never carried).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FragmentMeta {
    pub blob_id: BlobId,
    pub chunk_number: u32,
    pub local_index: u32,
    pub fragment_id: hopnet_common::CustomUUID,
    pub fragment_hash: Blake3Hash,
    /// false = original shard, true = Reed-Solomon recovery shard.
    /// SQL encoding matches the legacy ChunkType integer (0/1).
    pub recovery: bool,
}

/// The substrate half of a blob-creating transaction: registers the blob,
/// its fragment set, and the initial recipient wraps — atomic with the
/// projection's inode half because both run in the same handler tx.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobInsertOp {
    pub blob_id: BlobId,
    pub integrity_hash: Blake3Hash,
    pub added_bytes: u8,
    pub file_size: u64,
    pub fragments: Vec<FragmentMeta>,
    pub access: Vec<BlobAccess>,
}

/// Apply-time context supplied by the host. Projections obtain one from
/// the handler context (`From<&HandlerCtx>` in hopnet-projection) rather
/// than assembling it — the substrate decides what an apply needs.
pub struct ApplyCtx<'a> {
    /// Local fragment store root — used for the stored_locally probe.
    pub fragments_dir: &'a str,
    /// The block height this apply runs under (RFC-STORAGE-003: a newborn
    /// blob's goal is stamped with its inserting block).
    pub height: u64,
}

/// Storage infrastructure codes (contention, disk full, unopenable file,
/// I/O) become `StorageError::Transient` so the host keeps them out of
/// consensus verdicts; everything else is a real I/O-shaped failure.
pub(crate) fn db_err(what: &'static str) -> impl Fn(rusqlite::Error) -> StorageError {
    move |e| {
        tracing::error!("apply: failed to {what}: {e:?}");
        match e.sqlite_error_code() {
            Some(code) if hopnet_common::db_impl::sqlite_code_is_infrastructure(code) => {
                StorageError::Transient(code)
            }
            _ => StorageError::Io(std::io::Error::other(format!("{what}: {e}"))),
        }
    }
}

/// This substrate's section of the canonical state snapshot (RFC-019 S1).
///
/// mesh_key/mesh_key_access are public replicated state — losing them
/// across an epoch boundary would strand every all-users blob, so they
/// are covered. stored_locally and self_verified_height are node-local
/// columns of otherwise-replicated tables, excluded from canonical bytes.
pub const SNAPSHOT_SECTION: hopnet_common::SectionSpec = hopnet_common::SectionSpec {
    name: "storage",
    // v2 (RFC-STORAGE-003 S1): data_blocks.desired_placement_height and
    // the storage_view_transitions record — both replicated (derived at
    // apply from replicated inputs on every node).
    // v3 (RFC-STORAGE-003 S5): fragment_inventory.verified_height /
    // provenance / suspect — the replicated disk-truth record (stamped by
    // attest_fragments); the legacy self_verified_height stays excluded.
    // v4 (operational fixes 2026-10): storage step 0004 adds an index only;
    // the covered set is unchanged, but the format_version is part of the
    // serialized bytes, so storage@3 artifacts import through the frozen
    // v3 spec below (re-serializing at 4 cannot reproduce their hash).
    format_version: 4,
    tables: STORAGE_TABLES,
};

/// The covered tables shared by every spec since v3: the index-only v4
/// bump changed no table or column.
const STORAGE_TABLES: &[hopnet_common::TableSpec] = &[
    hopnet_common::TableSpec::exported("data_blocks"),
    hopnet_common::TableSpec::exported("storage_view_transitions"),
    hopnet_common::TableSpec::exported("blob_access"),
    hopnet_common::TableSpec::exported("mesh_key"),
    hopnet_common::TableSpec::exported("mesh_key_access"),
    hopnet_common::TableSpec {
        name: "fragment_hashes",
        role: hopnet_common::TableRole::Exported,
        excluded_columns: &["stored_locally"],
    },
    hopnet_common::TableSpec {
        name: "fragment_inventory",
        role: hopnet_common::TableRole::Exported,
        excluded_columns: &["self_verified_height"],
    },
    hopnet_common::TableSpec::exported("hopnet_storage_policy"),
];

/// The storage section as sealed by S5 through 2026.10.4 binaries
/// (ordinal 3). FROZEN — the import mapping for storage@3 artifacts, such
/// as the epoch-10 seal a 2026.10.5 joiner rebuilds from. Identical
/// tables; only the format_version differs, and it is hashed.
pub const PRE_SCAN_INDEX_SNAPSHOT_SECTION: hopnet_common::SectionSpec =
    hopnet_common::SectionSpec {
        name: "storage",
        format_version: 3,
        tables: STORAGE_TABLES,
    };

/// The storage section as sealed by S1–S4 binaries (ordinal 2): the v3
/// covered set, columns as of ordinal 2. FROZEN — the import mapping for
/// storage@2 artifacts (same precedent as the ordinal-1 freeze below).
pub const PRE_DISK_TRUTH_SNAPSHOT_SECTION: hopnet_common::SectionSpec =
    hopnet_common::SectionSpec {
        name: "storage",
        format_version: 2,
        tables: &[
            hopnet_common::TableSpec::exported("data_blocks"),
            hopnet_common::TableSpec::exported("storage_view_transitions"),
            hopnet_common::TableSpec::exported("blob_access"),
            hopnet_common::TableSpec::exported("mesh_key"),
            hopnet_common::TableSpec::exported("mesh_key_access"),
            hopnet_common::TableSpec {
                name: "fragment_hashes",
                role: hopnet_common::TableRole::Exported,
                excluded_columns: &["stored_locally"],
            },
            hopnet_common::TableSpec {
                name: "fragment_inventory",
                role: hopnet_common::TableRole::Exported,
                excluded_columns: &["self_verified_height"],
            },
            hopnet_common::TableSpec::exported("hopnet_storage_policy"),
        ],
    };

/// The storage section as sealed by pre-lifecycle binaries (ordinal 1):
/// the covered set without `storage_view_transitions`. FROZEN — the
/// import mapping for storage@1 artifacts (every mesh crossing into the
/// RFC-STORAGE-003 release): the joiner materializes storage at ordinal
/// 1, imports and verifies with THIS spec, then fast-forwards. Adding a
/// table to a released section is a covered-set change, which RFC-020
/// handles by a frozen spec per historical shape (the pre-split
/// consensus precedent), never by editing the live spec's history.
pub const PRE_LIFECYCLE_SNAPSHOT_SECTION: hopnet_common::SectionSpec = hopnet_common::SectionSpec {
    name: "storage",
    format_version: 1,
    tables: &[
        hopnet_common::TableSpec::exported("data_blocks"),
        hopnet_common::TableSpec::exported("blob_access"),
        hopnet_common::TableSpec::exported("mesh_key"),
        hopnet_common::TableSpec::exported("mesh_key_access"),
        hopnet_common::TableSpec {
            name: "fragment_hashes",
            role: hopnet_common::TableRole::Exported,
            excluded_columns: &["stored_locally"],
        },
        hopnet_common::TableSpec {
            name: "fragment_inventory",
            role: hopnet_common::TableRole::Exported,
            excluded_columns: &["self_verified_height"],
        },
        hopnet_common::TableSpec::exported("hopnet_storage_policy"),
    ],
};

/// Node-local tables — outside the snapshot universe entirely.
pub const NODE_LOCAL_TABLES: &[&str] = &["hopnet_storage_pins"];

/// This module's schema chain (RFC-020): replay is the only installer.
/// Head ordinal == SNAPSHOT_SECTION.format_version, pinned by host
/// registry tests.
pub static CHAIN: hopnet_common::Chain = hopnet_common::Chain {
    module: "storage",
    steps: &[
        hopnet_common::Step::sql(
            1,
            "init",
            include_str!("../migrations/storage/0001_init.sql"),
        ),
        hopnet_common::Step::sql(
            2,
            "block_lifecycle",
            include_str!("../migrations/storage/0002_block_lifecycle.sql"),
        ),
        hopnet_common::Step::sql(
            3,
            "disk_truth",
            include_str!("../migrations/storage/0003_disk_truth.sql"),
        ),
        hopnet_common::Step::sql(
            4,
            "scan_indexes",
            include_str!("../migrations/storage/0004_scan_indexes.sql"),
        ),
    ],
};

/// Seed/overwrite mesh policy rows (genesis apply; later a settings tx).
pub fn apply_policy_rows(
    db_tx: &rusqlite::Transaction,
    rows: &[(String, String)],
) -> Result<(), rusqlite::Error> {
    let mut stmt = db_tx
        .prepare("INSERT OR REPLACE INTO hopnet_storage_policy (key, value) VALUES (?1, ?2)")?;
    for (key, value) in rows {
        stmt.execute(rusqlite::params![key, value])?;
    }
    Ok(())
}

/// Resolve the replicated mesh policy (code defaults for absent keys).
pub fn read_policy(
    conn: &rusqlite::Connection,
) -> Result<crate::membership::StoragePolicy, rusqlite::Error> {
    let mut stmt = conn.prepare("SELECT key, value FROM hopnet_storage_policy")?;
    let rows: Vec<(String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    Ok(crate::membership::StoragePolicy::from_rows(&rows))
}

/// Register a blob: data_blocks row + fragment_hashes rows (stored_locally
/// probed against THIS node's disk) + blob_access wraps. Birth is a
/// declaration (RFC-STORAGE-003): the goal is stamped with the inserting
/// block's height, so a newborn blob is already in flight toward the
/// current view; placement_height starts NULL (never confirmed).
pub fn apply_blob_insert(
    db_tx: &rusqlite::Transaction,
    op: &BlobInsertOp,
    ctx: &ApplyCtx<'_>,
) -> Result<(), StorageError> {
    db_tx
        .execute(
            "INSERT INTO data_blocks (id, modified_at, file_hash, fragment_count, added_bytes, placement_height, file_size, desired_placement_height) VALUES (?, NULL, ?, ?, ?, NULL, ?, ?)",
            params![
                op.blob_id,
                op.integrity_hash,
                op.fragments.len() as i32,
                op.added_bytes,
                op.file_size as i64,
                height_to_db(ctx.height)
            ],
        )
        .map_err(db_err("insert data_block"))?;

    for fragment in &op.fragments {
        let stored_locally =
            fragstore::fragment_exists_and_valid(ctx.fragments_dir, &fragment.fragment_hash);
        db_tx
            .execute(
                "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally) VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    fragment.blob_id,
                    fragment.chunk_number,
                    fragment.local_index,
                    fragment.fragment_id,
                    fragment.fragment_hash,
                    fragment.recovery as i32,
                    stored_locally
                ],
            )
            .map_err(db_err("insert fragment_hash"))?;
    }

    apply_blob_access_add(db_tx, &op.access)?;
    Ok(())
}

/// Install recipient wraps (blob creation, sharing, mesh-key grants ride
/// their own table). Idempotent per (blob, recipient): re-wraps replace.
pub fn apply_blob_access_add(
    db_tx: &rusqlite::Transaction,
    entries: &[BlobAccess],
) -> Result<(), StorageError> {
    for access in entries {
        db_tx
            .execute(
                "INSERT OR REPLACE INTO blob_access (blob_id, recipient_pubkey, ephemeral_pubkey, wrapped_key) VALUES (?, ?, ?, ?)",
                params![
                    access.blob_id,
                    access.recipient_pubkey.to_vec(),
                    access.ephemeral_pubkey.to_vec(),
                    access.wrapped_key
                ],
            )
            .map_err(db_err("insert blob_access"))?;
    }
    Ok(())
}

/// Batched placement commit: set placement_height for each blob (the
/// distribution engine's settling-window flush; one tx per window).
pub fn apply_placement_commit(
    db_tx: &rusqlite::Transaction,
    updates: &[(BlobId, u64)],
) -> Result<usize, StorageError> {
    let mut applied = 0;
    for (blob_id, height) in updates {
        applied += db_tx
            .execute(
                "UPDATE data_blocks SET placement_height = ? WHERE id = ?",
                params![height_to_db(*height), blob_id],
            )
            .map_err(db_err("update placement_height"))?;
    }
    Ok(applied)
}

/// What one self-check apply changed. Logging only — never consulted for a
/// verdict: every node applies the same SQL to the same replicated state
/// and reaches the same rows.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SelfCheckApplied {
    /// Rows the report's removals deleted.
    pub removed: usize,
    /// Removals skipped because the row carries disk-verified evidence
    /// newer than the report's view (a stale differential).
    pub kept: usize,
    /// Rows asserted — inserted or re-asserted; always `added.len()`.
    pub asserted: usize,
}

/// Batched inventory belief (self_check_fragments): remove, then assert.
///
/// Idempotent and race-tolerant by construction. Two reports built from one
/// snapshot (the prompt per-pull report and the sweep's), or the same report
/// applied twice, converge on the same rows: additions are an upsert and
/// removals are a per-row compare-and-swap. The old exact-count guard made
/// ordinary concurrency a permanent rejection — 201 reports a day were
/// dropped on one node (2026-10-02) and the belief rows an attestation needs
/// never existed, so nothing could confirm.
///
/// The removal CAS keys on `verified_height`, a REPLICATED column: a
/// flag-derived removal never discards a row disk-verified at or after the
/// report's own view. It must not key on `self_verified_height`, which is
/// node-local (excluded from the canonical snapshot, NULL on a joiner,
/// carried on a member) — a predicate over it would make row presence
/// diverge between nodes and break the seal's section hash.
///
/// `previous_count` is informational since this change; it stays on the
/// wire for stability and is only compared under DEBUG logging. A hash in
/// both lists converges to present (removal runs first) on every node.
pub fn apply_self_check(
    db_tx: &rusqlite::Transaction,
    node_id: i32,
    previous_count: u32,
    self_verified_height: u64,
    added: &[Blake3Hash],
    removed: &[Blake3Hash],
) -> Result<SelfCheckApplied, StorageError> {
    let mut applied = SelfCheckApplied::default();
    let height_db = height_to_db(self_verified_height);

    // Observability only, and only when someone is listening: the COUNT
    // walks the node's whole inventory index and runs in both the dry-run
    // and the apply.
    if tracing::enabled!(tracing::Level::DEBUG) {
        let current: i64 = db_tx
            .query_row(
                "SELECT COUNT(*) FROM fragment_inventory WHERE node_id = ?",
                params![node_id],
                |r| r.get(0),
            )
            .map_err(db_err("count fragment_inventory"))?;
        if current as u32 != previous_count {
            tracing::debug!(
                node_id,
                previous_count,
                current,
                "self-check: inventory moved between build and apply"
            );
        }
    }

    let mut remove = db_tx
        .prepare_cached(
            "DELETE FROM fragment_inventory
             WHERE node_id = ? AND fragment_hash = ?
               AND (verified_height IS NULL OR verified_height <= ?)",
        )
        .map_err(db_err("prepare inventory removal"))?;
    for hash in removed {
        let n = remove
            .execute(params![node_id, hash, height_db])
            .map_err(db_err("remove inventory fragment"))?;
        applied.removed += n;
        applied.kept += 1 - n;
    }

    // No blanket restamp (RFC-STORAGE-003 S5): a self-check reads the flag,
    // not the disk, so it verifies nothing. `verified_height`, `provenance`
    // and `suspect` are stamped only by disk-verified attestations
    // (`apply_attestation`); the legacy self_verified_height keeps the
    // newest assertion.
    let mut assert_row = db_tx
        .prepare_cached(
            "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height)
             VALUES (?, ?, ?)
             ON CONFLICT(fragment_hash, node_id) DO UPDATE SET
               self_verified_height = MAX(COALESCE(self_verified_height, 0),
                                          excluded.self_verified_height)",
        )
        .map_err(db_err("prepare inventory assertion"))?;
    for hash in added {
        assert_row
            .execute(params![hash, node_id, height_db])
            .map_err(db_err("insert inventory fragment"))?;
        applied.asserted += 1;
    }

    Ok(applied)
}

/// Disk-truth attestation apply (RFC-STORAGE-003 S5): stamp the rows this
/// node verified on its own disk (`verified_height = height`, provenance
/// self-scan, suspect cleared) and flag the rows it marks suspect. Only
/// rows that exist are touched — attestation never creates belief, the
/// self-check does; unknown hashes are ignored. Idempotent.
///
/// The stamp is the report's height capped at `deciding_height`, and it
/// only ever rises: the payload's height is the submitter's word, so an
/// uncapped one could keep a row inside the recency window indefinitely,
/// and a page that commits late with an older height must not age out a
/// newer stamp.
pub fn apply_attestation(
    db_tx: &rusqlite::Transaction,
    node_id: i32,
    height: u64,
    deciding_height: u64,
    present: &[Blake3Hash],
    suspect: &[Blake3Hash],
) -> Result<usize, StorageError> {
    let mut stamped = 0usize;
    let height_db = height_to_db(height.min(deciding_height));
    for chunk in present.chunks(500) {
        let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "UPDATE fragment_inventory
             SET verified_height = MAX(COALESCE(verified_height, 0), ?),
                 provenance = 0, suspect = 0
             WHERE node_id = ? AND fragment_hash IN ({placeholders})"
        );
        let mut stmt = db_tx
            .prepare(&query)
            .map_err(db_err("prepare attestation stamp"))?;
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&height_db, &node_id];
        params.extend(chunk.iter().map(|h| h as &dyn rusqlite::ToSql));
        stamped += stmt
            .execute(params.as_slice())
            .map_err(db_err("stamp attestation"))?;
    }
    for chunk in suspect.chunks(500) {
        let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "UPDATE fragment_inventory SET suspect = 1
             WHERE node_id = ? AND fragment_hash IN ({placeholders})"
        );
        let mut stmt = db_tx
            .prepare(&query)
            .map_err(db_err("prepare suspect mark"))?;
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&node_id];
        params.extend(chunk.iter().map(|h| h as &dyn rusqlite::ToSql));
        stmt.execute(params.as_slice())
            .map_err(db_err("mark suspect"))?;
    }
    Ok(stamped)
}

/// Read the node's current inventory count and which of `candidates` are
/// already present. Returns `(previous_count, existing_set)`.
pub fn query_inventory_state(
    tx: &rusqlite::Transaction,
    node_id: i32,
    candidates: &[Blake3Hash],
) -> Result<(u32, HashSet<Blake3Hash>), rusqlite::Error> {
    let previous_count: u32 = {
        let mut stmt = tx.prepare("SELECT COUNT(*) FROM fragment_inventory WHERE node_id = ?")?;
        let count: i64 = stmt.query_row(rusqlite::params![node_id], |row| row.get(0))?;
        count as u32
    };

    if candidates.is_empty() {
        return Ok((previous_count, HashSet::new()));
    }

    let placeholders = candidates
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT fragment_hash FROM fragment_inventory \
         WHERE node_id = ? AND fragment_hash IN ({})",
        placeholders
    );

    let mut stmt = tx.prepare(&query)?;
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(node_id)];
    for hash in candidates {
        params.push(Box::new(*hash));
    }
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut rows = stmt.query(param_refs.as_slice())?;
    let mut set = HashSet::new();
    while let Some(row) = rows.next()? {
        let hash: Blake3Hash = row.get(0)?;
        set.insert(hash);
    }
    Ok((previous_count, set))
}

/// Get the current fragment count using a transaction
fn get_node_fragment_count_tx(
    tx: &rusqlite::Transaction<'_>,
    node_id: i32,
) -> Result<u32, rusqlite::Error> {
    let mut stmt = tx.prepare("SELECT COUNT(*) FROM fragment_inventory WHERE node_id = ?")?;
    let count: i64 = stmt.query_row(params![node_id], |row| row.get(0))?;
    Ok(count as u32)
}

/// Compute the differential between inventory and local fragments for a node.
/// Returns a complete SelfCheckFragments struct ready for consensus
/// submission. Uses high-performance EXCEPT queries; the caller supplies the
/// transaction (consistent snapshot) and the consensus height read inside it.
pub fn compute_inventory_differential(
    tx: &rusqlite::Transaction<'_>,
    node_id: i32,
    self_verified_height: u64,
) -> Result<SelfCheckFragments, rusqlite::Error> {
    // Get current inventory count
    let previous_count = get_node_fragment_count_tx(tx, node_id)?;

    // Fragments we have locally but not in inventory (to be added)
    let fragments_added = {
        let mut stmt = tx.prepare(
            "SELECT DISTINCT fragment_hash FROM fragment_hashes WHERE stored_locally = true
                     EXCEPT
                     SELECT fragment_hash FROM fragment_inventory WHERE node_id = ?",
        )?;
        let rows = stmt.query_map(params![node_id], |row| {
            let fragment_hash: Blake3Hash = row.get(0)?;
            Ok(fragment_hash)
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    // Fragments in inventory but not stored locally (to be removed)
    let fragments_removed = {
        let mut stmt = tx.prepare(
            "SELECT fragment_hash FROM fragment_inventory WHERE node_id = ?
                     EXCEPT
                     SELECT DISTINCT fragment_hash FROM fragment_hashes WHERE stored_locally = true",
        )?;
        let rows = stmt.query_map(params![node_id], |row| {
            let fragment_hash: Blake3Hash = row.get(0)?;
            Ok(fragment_hash)
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    // Assemble complete SelfCheckFragments struct
    Ok(SelfCheckFragments {
        node_id,
        self_verified_height,
        previous_count,
        fragments_added,
        fragments_removed,
    })
}

/// Blob-scoped belief for the prompt path (RFC-STORAGE-003 S3/S5): the
/// hashes of ONE blob this node holds (`stored_locally`) that have no
/// inventory row for it yet — the classes a pull just landed or rebuilt
/// and, at birth, the origin's. Indexed on both sides (fragment_hashes PK
/// prefix, fragment_inventory PK), so the per-pull cost is the blob's class
/// count, not the node's whole inventory; filtered against existing rows,
/// so it is empty (no consensus round) when belief is already on record.
///
/// Removals stay the sweep's: a pull has evidence that bytes landed, never
/// that they vanished. `previous_count` is informational and sent as 0.
pub fn compute_blob_inventory_differential(
    conn: &rusqlite::Connection,
    node_id: i32,
    blob_id: &BlobId,
    self_verified_height: u64,
) -> Result<SelfCheckFragments, rusqlite::Error> {
    let mut stmt = conn.prepare_cached(
        "SELECT fh.fragment_hash FROM fragment_hashes fh
         WHERE fh.data_block_id = ? AND fh.stored_locally = 1
           AND NOT EXISTS (SELECT 1 FROM fragment_inventory fi
                           WHERE fi.fragment_hash = fh.fragment_hash
                             AND fi.node_id = ?)
         ORDER BY fh.chunk_number, fh.local_index",
    )?;
    let fragments_added = stmt
        .query_map(params![blob_id, node_id], |row| row.get::<_, Blake3Hash>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SelfCheckFragments {
        node_id,
        self_verified_height,
        previous_count: 0,
        fragments_added,
        fragments_removed: Vec::new(),
    })
}

/// Delete orphaned blobs: the fragment_inventory, fragment_hashes,
/// blob_access and data_blocks rows, child-first. Returns the locally-
/// stored fragment hashes so the host can opportunistically remove the
/// files post-commit. LIVENESS GATES (takeout in flight, reference
/// providers) are the HOST's responsibility — this deletes unconditionally.
///
/// The inventory rows go for every node (S6 closure): once the blob's
/// manifest and access rows are gone a row for one of its hashes can
/// support no recovery. A live node's self-check would reap its own rows
/// next cycle; a departed node's would leak forever.
pub fn apply_delete_orphaned(
    db_tx: &rusqlite::Transaction,
    blob_ids: &[BlobId],
) -> Result<Vec<Blake3Hash>, StorageError> {
    if blob_ids.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = vec!["?"; blob_ids.len()].join(", ");
    let id_params: Vec<&dyn rusqlite::ToSql> = blob_ids
        .iter()
        .map(|id| id as &dyn rusqlite::ToSql)
        .collect();

    // Collect locally-stored fragment hashes for post-commit file cleanup
    let mut stmt = db_tx
        .prepare(&format!(
            "SELECT fragment_hash FROM fragment_hashes WHERE data_block_id IN ({placeholders}) AND stored_locally = TRUE"
        ))
        .map_err(db_err("prepare local fragment selection"))?;
    let local_hashes: Vec<Blake3Hash> = stmt
        .query_map(id_params.as_slice(), |row| row.get(0))
        .map_err(db_err("query local fragment hashes"))?
        .collect::<Result<_, _>>()
        .map_err(db_err("collect local fragment hashes"))?;
    drop(stmt);

    let inventory_deleted = db_tx
        .execute(
            &format!(
                "DELETE FROM fragment_inventory WHERE fragment_hash IN
                 (SELECT fragment_hash FROM fragment_hashes WHERE data_block_id IN ({placeholders}))"
            ),
            id_params.as_slice(),
        )
        .map_err(db_err("delete fragment_inventory"))?;
    let fragments_deleted = db_tx
        .execute(
            &format!("DELETE FROM fragment_hashes WHERE data_block_id IN ({placeholders})"),
            id_params.as_slice(),
        )
        .map_err(db_err("delete fragment_hashes"))?;
    let access_deleted = db_tx
        .execute(
            &format!("DELETE FROM blob_access WHERE blob_id IN ({placeholders})"),
            id_params.as_slice(),
        )
        .map_err(db_err("delete blob_access"))?;
    let blocks_deleted = db_tx
        .execute(
            &format!("DELETE FROM data_blocks WHERE id IN ({placeholders})"),
            id_params.as_slice(),
        )
        .map_err(db_err("delete data_blocks"))?;

    tracing::debug!(
        "Blob deletion applied: {blocks_deleted} blobs, {fragments_deleted} fragments, \
         {inventory_deleted} inventory rows, {access_deleted} access entries"
    );
    Ok(local_hashes)
}

/// Batch-update the node-local stored_locally flags (write-gate drain path:
/// fragment receipt / local deletion outside consensus). The OTHER writer is
/// the apply-time probe in apply_blob_insert — these two are the only
/// stored_locally writers (see the module-header invariant).
pub fn mark_local_state_batch(
    db_tx: &rusqlite::Transaction,
    fragment_hashes: &[Blake3Hash],
    stored_locally: bool,
) -> Result<usize, StorageError> {
    let mut total_rows = 0;
    let mut stmt = db_tx
        .prepare_cached("UPDATE fragment_hashes SET stored_locally = ? WHERE fragment_hash = ?")
        .map_err(db_err("prepare stored_locally batch update"))?;
    for hash in fragment_hashes {
        total_rows += stmt
            .execute(params![stored_locally, hash])
            .map_err(db_err("update stored_locally"))?;
    }
    Ok(total_rows)
}

/// One fragment as observed at reassembly time: (hash, id, stored-locally
/// flag).
pub type FragmentEntry = (Blake3Hash, hopnet_common::CustomUUID, bool);

/// Per-chunk fragment maps keyed by local_index: (originals, recovery).
pub type ChunkFragmentMaps = (
    std::collections::HashMap<usize, FragmentEntry>,
    std::collections::HashMap<usize, FragmentEntry>,
);

/// A blob's reassembly manifest: the replicated fragment layout plus this
/// node's local availability, grouped per chunk. The substrate half of the
/// get path — projections resolve their own reference (path → inode →
/// blob_id) and recipients separately.
#[derive(Debug, Clone)]
pub struct BlobManifest {
    pub blob_id: BlobId,
    /// Keyed whole-blob integrity hash (verifiable only by key holders).
    pub integrity_hash: Blake3Hash,
    /// Padding on the LAST chunk (stripped post-reconstruction).
    pub added_bytes: u8,
    pub file_size: u64,
    /// Height the placement commit was computed against; None = unplaced.
    pub placement_height: Option<u64>,
    /// chunk_number → (originals_by_index, recovery_by_index).
    pub chunks: std::collections::HashMap<u32, ChunkFragmentMaps>,
}

/// Read a blob's reassembly manifest. `None` when the blob id is unknown
/// (projections treat that as their own not-found).
pub fn blob_manifest(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
) -> Result<Option<BlobManifest>, StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT db.file_hash, db.added_bytes, db.placement_height, db.file_size,
                    fh.chunk_number, fh.local_index, fh.fragment_id, fh.fragment_hash,
                    fh.chunk_type, fh.stored_locally
             FROM data_blocks db
             JOIN fragment_hashes fh ON db.id = fh.data_block_id
             WHERE db.id = ?
             ORDER BY fh.chunk_number, fh.local_index",
        )
        .map_err(db_err("prepare blob manifest query"))?;

    let mut header: Option<(Blake3Hash, u8, Option<u64>, u64)> = None;
    let mut chunks: std::collections::HashMap<u32, ChunkFragmentMaps> =
        std::collections::HashMap::new();

    let rows = stmt
        .query_map(params![blob_id], |row| {
            Ok((
                row.get::<_, Blake3Hash>(0)?,
                row.get::<_, u8>(1)?,
                row.get::<_, Option<i64>>(2)?.map(height_from_db),
                row.get::<_, i64>(3).unwrap_or(0) as u64,
                row.get::<_, u32>(4)?,
                row.get::<_, u32>(5)?,
                row.get::<_, hopnet_common::CustomUUID>(6)?,
                row.get::<_, Blake3Hash>(7)?,
                row.get::<_, i32>(8)?,
                row.get::<_, bool>(9)?,
            ))
        })
        .map_err(db_err("query blob manifest"))?;

    for row in rows {
        let (
            integrity_hash,
            added_bytes,
            placement_height,
            file_size,
            chunk_number,
            local_index,
            fragment_id,
            fragment_hash,
            chunk_type,
            stored_locally,
        ) = row.map_err(db_err("read blob manifest row"))?;

        if header.is_none() {
            header = Some((integrity_hash, added_bytes, placement_height, file_size));
        }

        let entry = chunks.entry(chunk_number).or_default();
        let target = if chunk_type == 0 {
            &mut entry.0 // original
        } else {
            &mut entry.1 // recovery
        };
        target.insert(
            local_index as usize,
            (fragment_hash, fragment_id, stored_locally),
        );
    }

    Ok(header.map(
        |(integrity_hash, added_bytes, placement_height, file_size)| BlobManifest {
            blob_id: blob_id.clone(),
            integrity_hash,
            added_bytes,
            file_size,
            placement_height,
            chunks,
        },
    ))
}

/// Look up one recipient's wrap for a blob, by pubkey. Projections resolve
/// their own principal → pubkey mapping (users table etc.) — the substrate
/// never sees user ids. `None` = recipient has no access.
pub fn get_blob_access(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
    recipient_pubkey: &[u8; 32],
) -> Result<Option<BlobAccess>, StorageError> {
    use rusqlite::OptionalExtension;
    conn.query_row(
        "SELECT blob_id, recipient_pubkey, ephemeral_pubkey, wrapped_key
         FROM blob_access
         WHERE blob_id = ? AND recipient_pubkey = ?",
        params![blob_id, recipient_pubkey.as_slice()],
        row_to_blob_access,
    )
    .optional()
    .map_err(db_err("query blob access"))
}

/// Map a blob_access row (blob_id, recipient_pubkey, ephemeral_pubkey,
/// wrapped_key) into BlobAccess.
pub fn row_to_blob_access(row: &rusqlite::Row<'_>) -> Result<BlobAccess, rusqlite::Error> {
    let recipient: Vec<u8> = row.get(1)?;
    let ephemeral: Vec<u8> = row.get(2)?;
    let to_arr = |v: Vec<u8>, idx: usize| -> Result<[u8; 32], rusqlite::Error> {
        v.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Blob,
                "expected 32-byte X25519 key".into(),
            )
        })
    };
    Ok(BlobAccess {
        blob_id: row.get(0)?,
        recipient_pubkey: to_arr(recipient, 1)?,
        ephemeral_pubkey: to_arr(ephemeral, 2)?,
        wrapped_key: row.get(3)?,
    })
}

/// Blobs never confirmed (`placement_height IS NULL`), ignoring any drain
/// limit — the operator drain's backlog figure and the pane's unplaced
/// count.
pub fn count_unplaced_blobs(conn: &rusqlite::Connection) -> Result<i64, rusqlite::Error> {
    conn.query_row(
        "SELECT COUNT(*) FROM data_blocks WHERE placement_height IS NULL",
        [],
        |row| row.get(0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;
    use std::str::FromStr;

    fn test_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE data_blocks (
                id TEXT PRIMARY KEY, modified_at TEXT, file_hash BLOB,
                fragment_count INTEGER, added_bytes INTEGER,
                placement_height INTEGER, file_size INTEGER,
                desired_placement_height INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE fragment_hashes (
                data_block_id TEXT, chunk_number INTEGER, local_index INTEGER,
                fragment_id TEXT, fragment_hash BLOB, chunk_type INTEGER,
                stored_locally INTEGER
            );
            CREATE TABLE blob_access (
                blob_id TEXT, recipient_pubkey BLOB, ephemeral_pubkey BLOB,
                wrapped_key BLOB, PRIMARY KEY (blob_id, recipient_pubkey)
            );",
        )
        .unwrap();
        conn
    }

    // Should: resolve code defaults from an empty table, and genesis-seeded
    // rows once applied (INSERT OR REPLACE semantics for later settings tx).
    // Impact: nodes resolving different policies from the same replicated
    // rows would derive divergent member views — silent placement
    // divergence.
    #[test]
    fn policy_rows_roundtrip() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE hopnet_storage_policy (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();

        assert_eq!(
            read_policy(&conn).unwrap(),
            crate::membership::StoragePolicy::default()
        );

        let tx = conn.transaction().unwrap();
        apply_policy_rows(
            &tx,
            &[
                ("decay_tiers".to_string(), "60,120,180,240".to_string()),
                ("burst_cap".to_string(), "2".to_string()),
            ],
        )
        .unwrap();
        tx.commit().unwrap();

        let policy = read_policy(&conn).unwrap();
        assert_eq!(policy.decay_tiers, vec![60, 120, 180, 240]);
        assert_eq!(policy.b_max, 2);
        assert_eq!(policy.sigma, 1); // unseeded key stays code default
    }

    #[test]
    fn blob_insert_applies_all_three_tables() {
        // Should: one apply writes the blob row, its fragments (with a real
        // stored_locally probe), and its wraps; placement starts NULL and a
        // batched commit sets it.
        // Impact: this is the consensus-replicated truth for every blob.
        let dir = std::env::temp_dir().join(format!("hopnet-store-test-{}", std::process::id()));
        let dir_s = dir.to_str().unwrap().to_string();

        let blob_id = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap();
        let frag_data = b"stored fragment".to_vec();
        let on_disk_hash = Blake3Hash::new(blake3::hash(&frag_data));
        fragstore::store_fragment(&dir_s, &on_disk_hash, frag_data).unwrap();
        let missing_hash = Blake3Hash::from_bytes([9u8; 32]);

        let op = BlobInsertOp {
            blob_id: blob_id.clone(),
            integrity_hash: Blake3Hash::from_bytes([1u8; 32]),
            added_bytes: 4,
            file_size: 1000,
            fragments: vec![
                FragmentMeta {
                    blob_id: blob_id.clone(),
                    chunk_number: 0,
                    local_index: 0,
                    fragment_id: hopnet_common::CustomUUID::new(None),
                    fragment_hash: on_disk_hash,
                    recovery: false,
                },
                FragmentMeta {
                    blob_id: blob_id.clone(),
                    chunk_number: 0,
                    local_index: 10,
                    fragment_id: hopnet_common::CustomUUID::new(None),
                    fragment_hash: missing_hash,
                    recovery: true,
                },
            ],
            access: vec![BlobAccess {
                blob_id: blob_id.clone(),
                recipient_pubkey: [2u8; 32],
                ephemeral_pubkey: [3u8; 32],
                wrapped_key: vec![0u8; 48],
            }],
        };

        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        apply_blob_insert(
            &tx,
            &op,
            &ApplyCtx {
                fragments_dir: &dir_s,
                height: 42,
            },
        )
        .unwrap();

        // Should: stamp the goal with the inserting height (birth is a
        // declaration) while the confirmed epoch starts NULL.
        let (count, placement, desired): (i32, Option<i32>, i64) = tx
            .query_row(
                "SELECT fragment_count, placement_height, desired_placement_height FROM data_blocks WHERE id = ?",
                params![blob_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(placement, None);
        assert_eq!(desired, 42);

        // stored_locally probed: on-disk fragment true, missing false;
        // recovery flag round-trips as the legacy 0/1 encoding.
        let rows: Vec<(i32, bool)> = tx
            .prepare("SELECT chunk_type, stored_locally FROM fragment_hashes ORDER BY local_index")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows, vec![(0, true), (1, false)]);

        let wraps: i32 = tx
            .query_row("SELECT COUNT(*) FROM blob_access", [], |r| r.get(0))
            .unwrap();
        assert_eq!(wraps, 1);

        let applied = apply_placement_commit(&tx, &[(blob_id.clone(), 7)]).unwrap();
        assert_eq!(applied, 1);
        let placement: Option<i32> = tx
            .query_row(
                "SELECT placement_height FROM data_blocks WHERE id = ?",
                params![blob_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(placement, Some(7));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_and_access_reads_round_trip() {
        // Should: blob_manifest returns the header once and groups
        // fragments per chunk into (originals, recovery) keyed by
        // local_index; get_blob_access resolves exactly the requested
        // recipient's wrap.
        // Should not: return a manifest for an unknown blob id, or a wrap
        // for a pubkey that was never granted.
        // Impact: this is the substrate half of the get path — a grouping
        // or key-matching bug breaks reconstruction or leaks a wrong wrap
        // to the unwrap step (which would then fail AEAD, but waste the
        // fetch).
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let blob_id = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap();
        tx.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes,
             placement_height, file_size) VALUES (?, ?, 3, 4, 9, 1000)",
            params![blob_id, Blake3Hash::from_bytes([1u8; 32])],
        )
        .unwrap();
        for (chunk, idx, chunk_type, stored) in
            [(0u32, 0u32, 0i32, true), (0, 10, 1, false), (1, 0, 0, true)]
        {
            tx.execute(
                "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index,
                 fragment_id, fragment_hash, chunk_type, stored_locally)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    blob_id,
                    chunk,
                    idx,
                    hopnet_common::CustomUUID::new(None),
                    Blake3Hash::from_bytes([idx as u8 + chunk as u8 * 100; 32]),
                    chunk_type,
                    stored
                ],
            )
            .unwrap();
        }
        apply_blob_access_add(
            &tx,
            &[BlobAccess {
                blob_id: blob_id.clone(),
                recipient_pubkey: [2u8; 32],
                ephemeral_pubkey: [3u8; 32],
                wrapped_key: vec![0u8; 48],
            }],
        )
        .unwrap();

        let manifest = blob_manifest(&tx, &blob_id).unwrap().unwrap();
        assert_eq!(manifest.integrity_hash, Blake3Hash::from_bytes([1u8; 32]));
        assert_eq!(manifest.added_bytes, 4);
        assert_eq!(manifest.placement_height, Some(9));
        assert_eq!(manifest.file_size, 1000);
        assert_eq!(manifest.chunks.len(), 2);
        let chunk0 = &manifest.chunks[&0];
        assert_eq!(chunk0.0.len(), 1); // one original at index 0
        assert_eq!(chunk0.1.len(), 1); // one recovery at index 10
        assert!(chunk0.0[&0].2); // stored locally
        assert!(!chunk0.1[&10].2);
        assert_eq!(manifest.chunks[&1].0.len(), 1);

        let unknown = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap();
        assert!(blob_manifest(&tx, &unknown).unwrap().is_none());

        let wrap = get_blob_access(&tx, &blob_id, &[2u8; 32]).unwrap().unwrap();
        assert_eq!(wrap.ephemeral_pubkey, [3u8; 32]);
        assert!(get_blob_access(&tx, &blob_id, &[7u8; 32])
            .unwrap()
            .is_none());
    }

    /// Insert a bare data_blocks row with the given placement height.
    fn seed_block(conn: &rusqlite::Connection, id: &str, placement: Option<i64>) {
        conn.execute(
            "INSERT INTO data_blocks
                 (id, modified_at, file_hash, fragment_count, added_bytes,
                  placement_height, file_size)
             VALUES (?, '', X'00', 30, 0, ?, 0)",
            params![BlobId::from_str(id).unwrap(), placement],
        )
        .unwrap();
    }
    // Should: count every never-confirmed blob, and none once confirmed.
    #[test]
    fn unplaced_count_tracks_never_confirmed_blobs() {
        let conn = test_conn();
        for i in 1..=5 {
            seed_block(
                &conn,
                &format!("01890a5d-000{i}-7000-8000-00000000000{i}"),
                None,
            );
        }
        assert_eq!(count_unplaced_blobs(&conn).unwrap(), 5);

        conn.execute("UPDATE data_blocks SET placement_height = 1", [])
            .unwrap();
        assert_eq!(count_unplaced_blobs(&conn).unwrap(), 0);
    }

    // Impact: S6 closure — a row for a hash whose blob no longer exists can
    // support no recovery, and a departed node never self-checks it away.
    // Should: delete the deleted blobs' inventory rows on every node and
    // still return this node's local hashes for file removal.
    // Should not: touch a surviving blob's rows.
    #[test]
    fn delete_orphaned_removes_inventory_rows_for_every_node() {
        let mut conn = test_conn();
        conn.execute_batch(
            "CREATE TABLE fragment_inventory (
                fragment_hash BLOB NOT NULL, node_id INTEGER NOT NULL,
                self_verified_height INTEGER, verified_height INTEGER,
                provenance INTEGER, suspect INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (fragment_hash, node_id)
            );",
        )
        .unwrap();
        let gone = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap();
        let kept = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap();
        for (id, hash, local) in [(&gone, 1u8, true), (&gone, 2, false), (&kept, 3, true)] {
            conn.execute(
                "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, file_size) VALUES (?, X'00', 1, 0, 1)",
                params![id],
            )
            .ok();
            conn.execute(
                "INSERT INTO fragment_hashes VALUES (?, 0, ?, 'f', ?, 0, ?)",
                params![id, hash, vec![hash; 32], local],
            )
            .unwrap();
            for node in [1, 2, 9] {
                conn.execute(
                    "INSERT INTO fragment_inventory (fragment_hash, node_id) VALUES (?, ?)",
                    params![vec![hash; 32], node],
                )
                .unwrap();
            }
        }

        let tx = conn.transaction().unwrap();
        let local = apply_delete_orphaned(&tx, std::slice::from_ref(&gone)).unwrap();
        assert_eq!(local, vec![Blake3Hash::from_bytes([1u8; 32])]);
        let count = |hash: u8| -> i64 {
            tx.query_row(
                "SELECT COUNT(*) FROM fragment_inventory WHERE fragment_hash = ?",
                params![vec![hash; 32]],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(
            (count(1), count(2)),
            (0, 0),
            "deleted blob's rows gone on every node"
        );
        assert_eq!(count(3), 3, "surviving blob untouched");
        let blobs: i64 = tx
            .query_row("SELECT COUNT(*) FROM data_blocks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(blobs, 1);
    }

    /// The production `fragment_inventory` shape (0001 + 0003 columns).
    fn inventory_schema(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS fragment_inventory (
                fragment_hash BLOB NOT NULL, node_id INTEGER NOT NULL,
                self_verified_height INTEGER, verified_height INTEGER,
                provenance INTEGER, suspect INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (fragment_hash, node_id)
            );",
        )
        .unwrap();
    }

    fn inventory_row(
        conn: &rusqlite::Connection,
        node: i32,
        hash: &Blake3Hash,
    ) -> Option<(Option<i64>, Option<i64>, i64)> {
        conn.query_row(
            "SELECT self_verified_height, verified_height, suspect
             FROM fragment_inventory WHERE node_id = ? AND fragment_hash = ?",
            params![node, hash],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .unwrap()
    }

    // Impact: 201 self-check transactions a day were dropped on one node as
    // "UNIQUE constraint failed" — two reports built from one snapshot (the
    // prompt per-pull report and the sweep's) — and the belief row the
    // attestation needs never existed, so no placement could confirm.
    // Should: apply the same additions twice, and two reports carrying the
    // same hash, without error, leaving one row per (hash, node) with the
    // newest self_verified_height.
    // Should: leave verified_height and suspect untouched on a re-assertion;
    // only attestations stamp disk truth.
    // Should not: consult previous_count — a report with 0 and one with a
    // wildly wrong count both apply.
    #[test]
    fn self_check_apply_is_idempotent_and_merges_concurrent_reports() {
        let mut conn = test_conn();
        inventory_schema(&conn);
        let hash = Blake3Hash::from_bytes([7u8; 32]);
        conn.execute(
            "INSERT INTO fragment_inventory
             (fragment_hash, node_id, self_verified_height, verified_height, suspect)
             VALUES (?, 1, 5, 7, 1)",
            params![hash],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let first = apply_self_check(&tx, 1, 0, 5, &[hash], &[]).unwrap();
        let second = apply_self_check(&tx, 1, 999, 9, &[hash], &[]).unwrap();
        tx.commit().unwrap();

        assert_eq!(first.asserted, 1);
        assert_eq!(second.asserted, 1);
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fragment_inventory WHERE node_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            rows, 1,
            "one row per (hash, node) however many reports assert it"
        );
        assert_eq!(
            inventory_row(&conn, 1, &hash),
            Some((Some(9), Some(7), 1)),
            "newest assertion height, disk-truth columns untouched"
        );
    }

    // Impact: a sweep differential built before a pull landed carried the
    // pulled hash under fragments_removed; applied after the prompt
    // attestation, an unconditional DELETE erased disk-verified evidence.
    // The CAS must also key on replicated state only: self_verified_height
    // is NULL on a joiner and carried on a member, so a predicate over it
    // would make row presence diverge between nodes.
    // Should: delete a row whose verified_height is NULL or at most the
    // report's height.
    // Should not: delete a row disk-verified after the report's view; count
    // it as kept.
    // Should not: let self_verified_height decide — a row with it NULL but a
    // newer verified_height survives.
    #[test]
    fn self_check_removal_keeps_rows_verified_after_the_report() {
        let mut conn = test_conn();
        inventory_schema(&conn);
        let never = Blake3Hash::from_bytes([1u8; 32]);
        let older = Blake3Hash::from_bytes([2u8; 32]);
        let newer = Blake3Hash::from_bytes([3u8; 32]);
        for (hash, self_h, verified) in [
            (&never, None::<i64>, None::<i64>),
            (&older, Some(3), Some(4)),
            (&newer, None, Some(8)),
        ] {
            conn.execute(
                "INSERT INTO fragment_inventory
                 (fragment_hash, node_id, self_verified_height, verified_height)
                 VALUES (?, 1, ?, ?)",
                params![hash, self_h, verified],
            )
            .unwrap();
        }

        let tx = conn.transaction().unwrap();
        let applied = apply_self_check(&tx, 1, 3, 5, &[], &[never, older, newer]).unwrap();
        tx.commit().unwrap();

        assert_eq!(
            applied,
            SelfCheckApplied {
                removed: 2,
                kept: 1,
                asserted: 0
            }
        );
        assert!(inventory_row(&conn, 1, &never).is_none());
        assert!(inventory_row(&conn, 1, &older).is_none());
        assert_eq!(
            inventory_row(&conn, 1, &newer),
            Some((None, Some(8), 0)),
            "disk-verified after the report: the removal is stale"
        );
    }

    // Impact: the sweep's pages commit over minutes and a prompt pull
    // attestation can land between them; an overwrite let an older page
    // age a newer stamp back out of the recency window.
    // Should: keep the newer stamp when an older attestation lands later.
    // Should: still clear the suspect flag on the older attestation.
    #[test]
    fn attestation_never_lowers_a_stamp() {
        let mut conn = test_conn();
        inventory_schema(&conn);
        let hash = Blake3Hash::from_bytes([4u8; 32]);
        conn.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height, suspect)
             VALUES (?, 1, 50, 1)",
            params![hash],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let stamped = apply_attestation(&tx, 1, 40, 60, &[hash], &[]).unwrap();
        tx.commit().unwrap();

        assert_eq!(stamped, 1);
        assert_eq!(inventory_row(&conn, 1, &hash), Some((None, Some(50), 0)));
    }

    // Impact: the attestation's height is the submitter's word and the
    // confirmation window trusts it; a far-future height would keep a row
    // fresh indefinitely.
    // Should: stamp no later than the height the attestation is applied at.
    #[test]
    fn attestation_height_is_clamped_to_the_deciding_height() {
        let mut conn = test_conn();
        inventory_schema(&conn);
        let hash = Blake3Hash::from_bytes([5u8; 32]);
        conn.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id) VALUES (?, 1)",
            params![hash],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        apply_attestation(&tx, 1, 1_000_000, 70, &[hash], &[]).unwrap();
        tx.commit().unwrap();

        assert_eq!(inventory_row(&conn, 1, &hash), Some((None, Some(70), 0)));
    }

    // Impact: the whole-node differential ran once per pull (~5×/min over
    // ~768k rows on a large node) and bounded pull throughput; a birth
    // without an upload attestation had no belief rows until the next sweep.
    // Should: list only THIS blob's stored_locally hashes that lack a row
    // for this node, in (chunk, index) order, with no removals and a zero
    // previous_count.
    // Should not: list another blob's hashes, an un-stored hash, or a hash
    // already inventoried for this node — another node's row does not count.
    #[test]
    fn blob_inventory_differential_is_scoped_and_filtered() {
        let conn = test_conn();
        inventory_schema(&conn);
        let mine = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap();
        let other = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap();
        let h = |b: u8| Blake3Hash::from_bytes([b; 32]);
        // (blob, chunk, index, hash, stored_locally)
        for (blob, chunk, index, hash, local) in [
            (&mine, 0, 2, h(1), true),  // held, no row → listed
            (&mine, 0, 0, h(2), true),  // held, no row → listed first
            (&mine, 0, 1, h(3), false), // not held → skipped
            (&mine, 1, 0, h(4), true),  // held, my row exists → skipped
            (&mine, 1, 1, h(5), true),  // held, only another node's row → listed
            (&other, 0, 0, h(6), true), // another blob → skipped
        ] {
            conn.execute(
                "INSERT INTO fragment_hashes
                 (data_block_id, chunk_number, local_index, fragment_hash, stored_locally)
                 VALUES (?, ?, ?, ?, ?)",
                params![blob, chunk, index, hash, local],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id) VALUES (?, 1), (?, 2)",
            params![h(4), h(5)],
        )
        .unwrap();

        let report = compute_blob_inventory_differential(&conn, 1, &mine, 42).unwrap();
        assert_eq!(report.node_id, 1);
        assert_eq!(report.self_verified_height, 42);
        assert_eq!(report.previous_count, 0);
        assert!(report.fragments_removed.is_empty());
        assert_eq!(report.fragments_added, vec![h(2), h(1), h(5)]);
    }
}
