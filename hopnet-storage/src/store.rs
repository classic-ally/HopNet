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

/// Apply-time context supplied by the host.
pub struct ApplyCtx<'a> {
    /// Local fragment store root — used for the stored_locally probe.
    pub fragments_dir: &'a str,
}

pub(crate) fn db_err(what: &'static str) -> impl Fn(rusqlite::Error) -> StorageError {
    move |e| {
        tracing::error!("apply: failed to {what}: {e:?}");
        match e.sqlite_error_code() {
            Some(
                code @ (rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked),
            ) => StorageError::Transient(code),
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
    steps: &[hopnet_common::Step::sql(
        1,
        "init",
        include_str!("../migrations/storage/0001_init.sql"),
    )],
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
/// probed against THIS node's disk) + blob_access wraps.
pub fn apply_blob_insert(
    db_tx: &rusqlite::Transaction,
    op: &BlobInsertOp,
    ctx: &ApplyCtx<'_>,
) -> Result<(), StorageError> {
    db_tx
        .execute(
            "INSERT INTO data_blocks (id, modified_at, file_hash, fragment_count, added_bytes, placement_height, file_size) VALUES (?, NULL, ?, ?, ?, NULL, ?)",
            params![
                op.blob_id,
                op.integrity_hash,
                op.fragments.len() as i32,
                op.added_bytes,
                op.file_size as i64
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

/// Batched inventory attestation (self_check_fragments): verify the reported
/// previous count against current state, then remove / re-height / add.
/// Addition-only reports tolerate concurrent growth; removal reports require
/// an exact count match (we must not remove against a stale view).
pub fn apply_self_check(
    db_tx: &rusqlite::Transaction,
    node_id: i32,
    previous_count: u32,
    self_verified_height: u64,
    added: &[Blake3Hash],
    removed: &[Blake3Hash],
) -> Result<(), StorageError> {
    let current_count: i64 = db_tx
        .query_row(
            "SELECT COUNT(*) FROM fragment_inventory WHERE node_id = ?",
            params![node_id],
            |r| r.get(0),
        )
        .map_err(db_err("count fragment_inventory"))?;
    let current_count = current_count as u32;

    if removed.is_empty() {
        if current_count < previous_count {
            tracing::error!(
                "Fragment inventory count decreased unexpectedly for node {node_id}: expected >= {previous_count}, found {current_count}"
            );
            return Err(StorageError::Rs);
        }
    } else if current_count != previous_count {
        tracing::error!(
            "Fragment inventory state mismatch for node {node_id} (removal requires exact count): expected {previous_count}, found {current_count}"
        );
        return Err(StorageError::Rs);
    }

    for hash in removed {
        db_tx
            .execute(
                "DELETE FROM fragment_inventory WHERE node_id = ? AND fragment_hash = ?",
                params![node_id, hash],
            )
            .map_err(db_err("remove inventory fragment"))?;
    }

    db_tx
        .execute(
            "UPDATE fragment_inventory SET self_verified_height = ? WHERE node_id = ?",
            params![height_to_db(self_verified_height), node_id],
        )
        .map_err(db_err("update inventory verified height"))?;

    for hash in added {
        db_tx
            .execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height) VALUES (?, ?, ?)",
                params![hash, node_id, height_to_db(self_verified_height)],
            )
            .map_err(db_err("insert inventory fragment"))?;
    }

    Ok(())
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

/// Delete orphaned blobs: fragment_hashes + blob_access + data_blocks rows,
/// child-first. Returns the locally-stored fragment hashes so the host can
/// opportunistically remove the files post-commit. LIVENESS GATES (takeout
/// in flight, reference providers) are the HOST's responsibility — this
/// deletes unconditionally.
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

    tracing::info!(
        "Blob deletion applied: {blocks_deleted} blobs, {fragments_deleted} fragments, {access_deleted} access entries"
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

/// A blob this node should distribute: its full local fragment set, ordered
/// by (chunk_number, local_index).
#[derive(Debug, Clone)]
pub struct DistributableBlob {
    pub blob_id: BlobId,
    /// (local_index, fragment_hash) per fragment.
    pub fragments: Vec<(u32, Blake3Hash)>,
}

/// Origin filter for the distribution engine: return the blob's fragments
/// IFF it is unplaced (`placement_height IS NULL`) and EVERY fragment is
/// stored locally — i.e. this node holds the complete set (the origin).
/// `None` is the cheap common case on non-origin nodes.
pub fn get_distributable_blob(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
) -> Result<Option<DistributableBlob>, StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT fh.local_index, fh.fragment_hash
             FROM data_blocks db
             JOIN fragment_hashes fh ON db.id = fh.data_block_id
             WHERE db.id = ?
               AND db.placement_height IS NULL
               AND fh.stored_locally = TRUE
               AND (SELECT COUNT(*) FROM fragment_hashes
                    WHERE data_block_id = db.id AND stored_locally = TRUE)
                   = db.fragment_count
             ORDER BY fh.chunk_number, fh.local_index",
        )
        .map_err(db_err("prepare distributable blob query"))?;
    let fragments: Vec<(u32, Blake3Hash)> = stmt
        .query_map(params![blob_id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(db_err("query distributable blob"))?
        .collect::<Result<_, _>>()
        .map_err(db_err("read distributable blob row"))?;
    if fragments.is_empty() {
        Ok(None)
    } else {
        Ok(Some(DistributableBlob {
            blob_id: blob_id.clone(),
            fragments,
        }))
    }
}

/// A rebalance candidate: one placed blob with its full fragment layout.
#[derive(Debug, Clone)]
pub struct DataBlockRebalanceInfo {
    pub data_block_id: BlobId,
    pub placement_height: u64,
    pub fragments: Vec<FragmentInfo>,
}

/// One fragment of a rebalance candidate: hash + decoded chunk-type label.
#[derive(Debug, Clone)]
pub struct FragmentInfo {
    pub fragment_hash: Blake3Hash,
    pub chunk_type: String,
}

/// Get data blocks that need rebalancing (distributed before a certain
/// height). Returns data blocks with their fragments, ordered by
/// placement_height (oldest first); blobs with an incomplete fragment set
/// are skipped.
pub fn get_data_blocks_for_rebalancing(
    conn: &rusqlite::Connection,
    max_placement_height: u64,
    limit: i32,
) -> Result<Vec<DataBlockRebalanceInfo>, rusqlite::Error> {
    // Get data blocks that were placed before the specified height
    let query = "SELECT DISTINCT db.id, db.placement_height, db.fragment_count
         FROM data_blocks db
         WHERE db.placement_height IS NOT NULL
           AND db.placement_height < ?
         ORDER BY db.placement_height ASC
         LIMIT ?";
    let mut stmt = conn.prepare(query)?;
    let data_blocks: Vec<(BlobId, u64, i32)> = stmt
        .query_map(params![height_to_db(max_placement_height), limit], |row| {
            Ok((row.get(0)?, row.get(1).map(height_from_db)?, row.get(2)?))
        })?
        .collect::<Result<_, _>>()?;

    // For each data block, get all its fragments
    let fragment_query = "SELECT fragment_hash, chunk_type
             FROM fragment_hashes
             WHERE data_block_id = ?
             ORDER BY chunk_number";
    let mut fragment_stmt = conn.prepare(fragment_query)?;

    let mut result = Vec::new();
    for (data_block_id, placement_height, total_fragments) in data_blocks {
        let fragments: Vec<FragmentInfo> = fragment_stmt
            .query_map(params![&data_block_id], |row| {
                let fragment_hash: Blake3Hash = row.get(0)?;
                // fragment_hashes.chunk_type is the storage schema's 0/1
                // encoding (see install_schema) — decoded to its label here.
                let chunk_type = match row.get::<_, i32>(1)? {
                    0 => "original".to_string(),
                    1 => "recovery".to_string(),
                    other => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Integer,
                            format!("invalid chunk_type {other}").into(),
                        ));
                    }
                };
                Ok(FragmentInfo {
                    fragment_hash,
                    chunk_type,
                })
            })?
            .collect::<Result<_, _>>()?;

        // Only include data blocks where we have all fragments
        if fragments.len() == total_fragments as usize {
            result.push(DataBlockRebalanceInfo {
                data_block_id,
                placement_height,
                fragments,
            });
        } else {
            tracing::warn!(
                "Data block {} has {} fragments but expected {}, skipping",
                data_block_id,
                fragments.len(),
                total_fragments
            );
        }
    }

    tracing::info!(
        "Found {} complete data blocks for rebalancing",
        result.len()
    );
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE data_blocks (
                id TEXT PRIMARY KEY, modified_at TEXT, file_hash BLOB,
                fragment_count INTEGER, added_bytes INTEGER,
                placement_height INTEGER, file_size INTEGER
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
            },
        )
        .unwrap();

        let (count, placement): (i32, Option<i32>) = tx
            .query_row(
                "SELECT fragment_count, placement_height FROM data_blocks WHERE id = ?",
                params![blob_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(placement, None);

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
}
