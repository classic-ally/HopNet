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
use crate::types::{BlobAccess, BlobId};
use hopnet_common::Blake3Hash;
use rusqlite::params;
use serde::{Deserialize, Serialize};

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

fn db_err(what: &'static str) -> impl Fn(rusqlite::Error) -> StorageError {
    move |e| {
        tracing::error!("apply: failed to {what}: {e:?}");
        StorageError::Io(std::io::Error::other(format!("{what}: {e}")))
    }
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
    updates: &[(BlobId, i32)],
) -> Result<usize, StorageError> {
    let mut applied = 0;
    for (blob_id, height) in updates {
        applied += db_tx
            .execute(
                "UPDATE data_blocks SET placement_height = ? WHERE id = ?",
                params![height, blob_id],
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
    self_verified_height: i32,
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
            params![self_verified_height, node_id],
        )
        .map_err(db_err("update inventory verified height"))?;

    for hash in added {
        db_tx
            .execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height) VALUES (?, ?, ?)",
                params![hash, node_id, self_verified_height],
            )
            .map_err(db_err("insert inventory fragment"))?;
    }

    Ok(())
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
    let id_params: Vec<&dyn rusqlite::ToSql> =
        blob_ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();

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
        apply_blob_insert(&tx, &op, &ApplyCtx { fragments_dir: &dir_s }).unwrap();

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

        let applied =
            apply_placement_commit(&tx, &[(blob_id.clone(), 7)]).unwrap();
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
}
