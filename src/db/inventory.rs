use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use std::collections::HashMap;
use tracing::debug;

use crate::db::DatabaseError;
use crate::types::{Blake3Hash, NodeConnectionInfo};
use hopnet_storage::SelfCheckFragments;

// apply_self_check_updates moved to crate::storage_host::db_apply
// (RFC-016 Stage 6) — it lives beside its consensus-handler caller.

/// Compute the differential between inventory and local fragments for a node
/// Returns a complete SelfCheckFragments struct ready for consensus submission
/// Uses transaction semantics for consistency guarantees in distributed environment
pub fn compute_inventory_differential(
    db_connection: Result<PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    node_id: i32,
) -> Result<SelfCheckFragments, DatabaseError> {
    match db_connection {
        Ok(mut conn) => {
            // Use transaction for consistent snapshot
            let tx = conn.transaction().map_err(|_| DatabaseError::LockError)?;

            // The consensus height is host state, read inside the same
            // snapshot; the EXCEPT queries + previous-count read are
            // substrate-owned (RFC-017 Stage 5).
            let self_verified_height = crate::db::consensus::get_current_consensus_height(&tx)?;
            let differential = hopnet_storage::store::compute_inventory_differential(
                &tx,
                node_id,
                self_verified_height,
            )
            .map_err(|_| DatabaseError::RecallError)?;

            // Read-only transaction — auto-rollback on drop
            drop(tx);

            Ok(differential)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Batch query fragment inventory to find nodes that claim to have specific fragments
/// Returns a map from fragment hash to list of nodes, ordered by verification recency
/// Optimized for minimal database round-trips when looking up many fragments at once
///
/// Stays host: joins the host-owned nodes table for connection info; consumed
/// behind StateReader::fragment_sources.
///
/// # Parameters
/// * `max_nodes_per_fragment` - Limit nodes returned per fragment (default: 3 most recent)
pub fn batch_query_fragment_inventory(
    db_connection: Result<PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    fragment_hashes: &[Blake3Hash],
    max_nodes_per_fragment: Option<usize>,
) -> Result<HashMap<Blake3Hash, Vec<NodeConnectionInfo>>, DatabaseError> {
    if fragment_hashes.is_empty() {
        return Ok(HashMap::new());
    }

    let max_nodes = max_nodes_per_fragment.unwrap_or(3);

    match db_connection {
        Ok(conn) => {
            // Build parameterized query with window function to limit nodes per fragment
            let placeholders = fragment_hashes
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "SELECT fragment_hash, node_id, pubkey
                 FROM (
                     SELECT fi.fragment_hash, fi.node_id, n.pubkey,
                            ROW_NUMBER() OVER (PARTITION BY fi.fragment_hash
                                               ORDER BY fi.self_verified_height DESC) as rn
                     FROM fragment_inventory fi
                     JOIN nodes n ON fi.node_id = n.node_id
                     WHERE fi.fragment_hash IN ({})
                 )
                 WHERE rn <= {}",
                placeholders, max_nodes
            );

            let mut stmt = conn
                .prepare(&query)
                .map_err(|_| DatabaseError::ProcessingError)?;

            // Execute query with all fragment hashes as parameters
            let mut rows = stmt
                .query(rusqlite::params_from_iter(fragment_hashes.iter()))
                .map_err(|_| DatabaseError::RecallError)?;

            // Group results by fragment hash, constructing NodeConnectionInfo directly
            let mut result: HashMap<Blake3Hash, Vec<NodeConnectionInfo>> = HashMap::new();

            while let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
                let fragment_hash: Blake3Hash =
                    row.get(0).map_err(|_| DatabaseError::RecallError)?;
                let node_info = NodeConnectionInfo {
                    node_id: row.get(1).map_err(|_| DatabaseError::RecallError)?,
                    pubkey: row.get(2).map_err(|_| DatabaseError::RecallError)?,
                };

                result.entry(fragment_hash).or_default().push(node_info);
            }

            debug!(
                "Batch inventory query: {} hashes requested, {} found in inventory (max {} nodes each)",
                fragment_hashes.len(),
                result.len(),
                max_nodes
            );

            Ok(result)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// How a missing class's holders relate to the membership view
/// (RFC-STORAGE-002 S4: two-tier repair urgency inputs).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MissingHolderState {
    /// Some holder is offline but still within its decay tier — the copy
    /// may return; re-encode lazily.
    Lazy,
    /// No holder, or every holder has decayed out of the member view —
    /// nothing is coming back; re-encode is the only path.
    Hopeless,
}

#[derive(Debug)]
pub struct RepairCandidate {
    pub blob_id: hopnet_storage::BlobId,
    pub chunk_number: u32,
    /// Classes with at least one ONLINE holder.
    pub live_classes: usize,
    pub missing: Vec<(u32, MissingHolderState)>,
}

/// Chunks with missing classes, classified for the repair tick
/// (RFC-STORAGE-001 Repair): a class is live if any ONLINE node attests a
/// copy; missing classes are lazy while some offline-within-tier holder
/// may still return, hopeless otherwise. Holder liveness is judged against
/// the availability view, never raw inventory rows — departed nodes' rows
/// linger forever (pruning deferred).
///
/// Full-scan aggregation (fragment_hashes LEFT JOIN fragment_inventory);
/// fine at current mesh scale, revisit with an index if the tick shows up
/// in db-stats.
pub fn find_chunks_with_missing_classes(
    conn: &rusqlite::Connection,
    online_nodes: &std::collections::HashSet<i32>,
    member_nodes: &std::collections::HashSet<i32>,
) -> Result<Vec<RepairCandidate>, DatabaseError> {
    let mut stmt = conn
        .prepare(
            "SELECT fh.data_block_id, fh.chunk_number, fh.local_index,
                    COALESCE(GROUP_CONCAT(fi.node_id), '')
             FROM fragment_hashes fh
             LEFT JOIN fragment_inventory fi ON fi.fragment_hash = fh.fragment_hash
             GROUP BY fh.data_block_id, fh.chunk_number, fh.local_index",
        )
        .map_err(|_| DatabaseError::RecallError)?;
    let rows: Vec<(String, u32, u32, String)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .map_err(|_| DatabaseError::RecallError)?
        .collect::<Result<_, _>>()
        .map_err(|_| DatabaseError::ProcessingError)?;

    use std::collections::BTreeMap;
    let mut chunks: BTreeMap<(String, u32), Vec<(u32, Vec<i32>)>> = BTreeMap::new();
    for (blob, chunk, class, holders) in rows {
        let holder_ids: Vec<i32> = holders
            .split(',')
            .filter_map(|s| s.parse().ok())
            .collect();
        chunks
            .entry((blob, chunk))
            .or_default()
            .push((class, holder_ids));
    }

    let mut candidates = Vec::new();
    for ((blob, chunk_number), classes) in chunks {
        let mut live = 0usize;
        let mut missing = Vec::new();
        for (class, holders) in classes {
            if holders.iter().any(|n| online_nodes.contains(n)) {
                live += 1;
            } else if holders.iter().any(|n| member_nodes.contains(n)) {
                missing.push((class, MissingHolderState::Lazy));
            } else {
                missing.push((class, MissingHolderState::Hopeless));
            }
        }
        if !missing.is_empty() {
            use std::str::FromStr;
            let Ok(blob_id) = hopnet_storage::BlobId::from_str(&blob) else {
                debug!("repair scan: unparsable blob id {blob}");
                continue;
            };
            missing.sort_unstable_by_key(|(c, _)| *c);
            candidates.push(RepairCandidate {
                blob_id,
                chunk_number,
                live_classes: live,
                missing,
            });
        }
    }
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SqliteConnectionManager;
    use rusqlite::params;

    // Should: classify each missing class by its holders' relation to the
    // availability/member views — online holder = live, offline-within-
    // tier holder = lazy, decayed-or-no holder = hopeless — and never
    // trust raw inventory rows as liveness.
    // Should not: emit chunks with no missing classes.
    // Impact: lazy/hopeless drives the two-tier repair urgency; treating
    // a departed node's lingering inventory row as live would silently
    // skip re-encode until the durability cliff.
    #[test]
    fn classifies_missing_holders_by_view() {
        let manager = SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        let conn = pool.get().unwrap();

        let key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let pubkey = crate::db::PubKey(key.verifying_key());
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'u', ?, ?, ?, ?)",
            params![&pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]],
        )
        .unwrap();
        for n in 1..=3 {
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                params![n, format!("n{n}"), &pubkey],
            )
            .unwrap();
        }
        let blob = "01890a5d-ac96-774b-b9aa-9f8b24f0c9a1";
        conn.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, file_size)
             VALUES (?, X'00', 4, 0, 100)",
            params![blob],
        )
        .unwrap();
        // class 0: held by node 1 (online). class 1: node 2 (offline,
        // member). class 2: node 3 (decayed). class 3: nobody.
        for (class, holder) in [(0i64, Some(1i64)), (1, Some(2)), (2, Some(3)), (3, None)] {
            let hash = vec![class as u8; 32];
            conn.execute(
                "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index,
                 fragment_id, fragment_hash, chunk_type, stored_locally)
                 VALUES (?, 0, ?, ?, ?, 0, 0)",
                params![blob, class, format!("f{class}"), &hash],
            )
            .unwrap();
            if let Some(node) = holder {
                conn.execute(
                    "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height)
                     VALUES (?, ?, 1)",
                    params![&hash, node],
                )
                .unwrap();
            }
        }

        let online = std::collections::HashSet::from([1]);
        let members = std::collections::HashSet::from([1, 2]);
        let candidates = find_chunks_with_missing_classes(&conn, &online, &members).unwrap();
        assert_eq!(candidates.len(), 1);
        let c = &candidates[0];
        assert_eq!(c.live_classes, 1);
        assert_eq!(
            c.missing,
            vec![
                (1, MissingHolderState::Lazy),
                (2, MissingHolderState::Hopeless),
                (3, MissingHolderState::Hopeless),
            ]
        );
    }
}
