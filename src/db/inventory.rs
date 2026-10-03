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

/// Blob-scoped belief for the prompt path after a pull: this node's held
/// classes of `blob_id` with no inventory row yet (the pulled, rebuilt and
/// origin classes). One indexed query under a consistent snapshot; the
/// whole-node differential above stays the sweep's.
pub fn compute_blob_inventory_differential(
    db_connection: Result<PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    node_id: i32,
    blob_id: &hopnet_storage::BlobId,
) -> Result<SelfCheckFragments, DatabaseError> {
    let mut conn = db_connection.map_err(|_| DatabaseError::LockError)?;
    let tx = conn.transaction().map_err(|_| DatabaseError::LockError)?;
    let self_verified_height = crate::db::consensus::get_current_consensus_height(&tx)?;
    let report = hopnet_storage::store::compute_blob_inventory_differential(
        &tx,
        node_id,
        blob_id,
        self_verified_height,
    )
    .map_err(|_| DatabaseError::RecallError)?;
    drop(tx);
    Ok(report)
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
                                               ORDER BY fi.verified_height DESC NULLS LAST) as rn
                     FROM fragment_inventory fi
                     JOIN nodes n ON fi.node_id = n.node_id
                     WHERE fi.fragment_hash IN ({})
                       AND fi.suspect = 0
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
    /// Every class's attested holders (raw inventory rows), ascending by
    /// class — the reconciler's ladder input (RFC-STORAGE-003 S4).
    pub classes: Vec<(u32, Vec<i32>)>,
}

/// Chunks with missing classes, classified for the repair tick
/// (RFC-STORAGE-001 Repair): a class is live if any ONLINE node attests a
/// copy; missing classes are lazy while some offline-within-tier holder
/// may still return, hopeless otherwise. Holder liveness is judged against
/// the availability view, never raw inventory rows — departed nodes' rows
/// linger forever (pruning deferred).
///
/// Two passes, both indexed. First, the chunks that have at least one
/// class with no non-suspect ONLINE holder — one walk of `fragment_hashes`
/// with a primary-key probe per row, emitting only the chunks that need
/// classifying (a handful, not every chunk-class in the mesh). Then, for
/// those chunks only, every class's raw holders. The previous shape — a
/// GROUP_CONCAT over the full join, filtered in Rust — returned 2.1M rows
/// and ran past 15 minutes on a 5-minute cron (2026-10-02).
pub fn find_chunks_with_missing_classes(
    conn: &rusqlite::Connection,
    online_nodes: &std::collections::HashSet<i32>,
    member_nodes: &std::collections::HashSet<i32>,
) -> Result<Vec<RepairCandidate>, DatabaseError> {
    // The online set is bound inline; `IN (NULL)` matches nothing, so with
    // no one online every class is missing — which is the truth.
    let mut online_sorted: Vec<i32> = online_nodes.iter().copied().collect();
    online_sorted.sort_unstable();
    let placeholders = if online_sorted.is_empty() {
        "NULL".to_string()
    } else {
        vec!["?"; online_sorted.len()].join(",")
    };
    let chunk_sql = format!(
        "SELECT DISTINCT fh.data_block_id, fh.chunk_number
         FROM fragment_hashes fh
         WHERE NOT EXISTS (SELECT 1 FROM fragment_inventory o
                           WHERE o.fragment_hash = fh.fragment_hash
                             AND o.suspect = 0
                             AND o.node_id IN ({placeholders}))
         ORDER BY fh.data_block_id, fh.chunk_number"
    );
    let mut chunk_stmt = conn
        .prepare(&chunk_sql)
        .map_err(|_| DatabaseError::RecallError)?;
    let chunk_keys: Vec<(String, u32)> = chunk_stmt
        .query_map(rusqlite::params_from_iter(online_sorted.iter()), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(|_| DatabaseError::RecallError)?
        .collect::<Result<_, _>>()
        .map_err(|_| DatabaseError::ProcessingError)?;

    let mut classes_stmt = conn
        .prepare_cached(
            "SELECT fh.local_index, COALESCE(GROUP_CONCAT(fi.node_id), '')
             FROM fragment_hashes fh
             LEFT JOIN fragment_inventory fi
               ON fi.fragment_hash = fh.fragment_hash AND fi.suspect = 0
             WHERE fh.data_block_id = ? AND fh.chunk_number = ?
             GROUP BY fh.local_index",
        )
        .map_err(|_| DatabaseError::RecallError)?;

    let mut candidates = Vec::new();
    for (blob, chunk_number) in chunk_keys {
        let rows: Vec<(u32, String)> = classes_stmt
            .query_map(rusqlite::params![&blob, chunk_number], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(|_| DatabaseError::RecallError)?
            .collect::<Result<_, _>>()
            .map_err(|_| DatabaseError::ProcessingError)?;
        let mut classes: Vec<(u32, Vec<i32>)> = rows
            .into_iter()
            .map(|(class, holders)| {
                let holder_ids: Vec<i32> =
                    holders.split(',').filter_map(|s| s.parse().ok()).collect();
                (class, holder_ids)
            })
            .collect();
        let mut live = 0usize;
        let mut missing = Vec::new();
        for (class, holders) in &classes {
            if holders.iter().any(|n| online_nodes.contains(n)) {
                live += 1;
            } else if holders.iter().any(|n| member_nodes.contains(n)) {
                missing.push((*class, MissingHolderState::Lazy));
            } else {
                missing.push((*class, MissingHolderState::Hopeless));
            }
        }
        if !missing.is_empty() {
            use std::str::FromStr;
            let Ok(blob_id) = hopnet_storage::BlobId::from_str(&blob) else {
                debug!("repair scan: unparsable blob id {blob}");
                continue;
            };
            missing.sort_unstable_by_key(|(c, _)| *c);
            classes.sort_unstable_by_key(|(c, _)| *c);
            candidates.push(RepairCandidate {
                blob_id,
                chunk_number,
                live_classes: live,
                missing,
                classes,
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
        crate::db::chains::install(&pool.get().unwrap()).unwrap();
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
            // nodes.pubkey is UNIQUE — each fixture node needs its own key.
            let node_key = ed25519_dalek::SigningKey::from_bytes(&[100 + n as u8; 32]);
            let node_pubkey = crate::db::PubKey(node_key.verifying_key());
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                params![n, format!("n{n}"), &node_pubkey],
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
        // Should: carry every class's raw holders for the ladder.
        assert_eq!(
            c.classes,
            vec![(0, vec![1]), (1, vec![2]), (2, vec![3]), (3, vec![])]
        );
    }

    // Impact: the scan used to return every chunk-class row in the mesh
    // (2.1M) and filter in Rust, running past 15 minutes on a 5-minute
    // cron; it must emit only what needs repair and treat a suspect row as
    // no holder at all.
    // Should: return only the chunks with at least one class lacking a
    // non-suspect online holder, with live_classes and classes intact.
    // Should not: return a chunk whose every class has an online holder,
    // nor count a suspect row as live.
    #[test]
    fn repair_scan_returns_only_chunks_with_a_missing_class() {
        let manager = SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::chains::install(&pool.get().unwrap()).unwrap();
        let conn = pool.get().unwrap();

        let key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let pubkey = crate::db::PubKey(key.verifying_key());
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'u', ?, ?, ?, ?)",
            params![&pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]],
        )
        .unwrap();
        for n in 1..=2 {
            let node_key = ed25519_dalek::SigningKey::from_bytes(&[100 + n as u8; 32]);
            let node_pubkey = crate::db::PubKey(node_key.verifying_key());
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                params![n, format!("n{n}"), &node_pubkey],
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
        // Chunk 0: both classes held by online node 1 → fully live.
        // Chunk 1: class 0 held by node 1; class 1 held only as a SUSPECT
        //          row by node 1 → missing (hopeless: no member holds it).
        for (chunk, class, suspect) in [(0i64, 0i64, 0i64), (0, 1, 0), (1, 0, 0), (1, 1, 1)] {
            let hash = vec![(chunk * 10 + class) as u8; 32];
            conn.execute(
                "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index,
                 fragment_id, fragment_hash, chunk_type, stored_locally)
                 VALUES (?, ?, ?, ?, ?, 0, 0)",
                params![blob, chunk, class, format!("f{chunk}{class}"), &hash],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height, suspect)
                 VALUES (?, 1, 1, ?)",
                params![&hash, suspect],
            )
            .unwrap();
        }

        let online = std::collections::HashSet::from([1]);
        let members = std::collections::HashSet::from([1, 2]);
        let candidates = find_chunks_with_missing_classes(&conn, &online, &members).unwrap();
        assert_eq!(candidates.len(), 1, "{candidates:?}");
        let c = &candidates[0];
        assert_eq!(c.chunk_number, 1);
        assert_eq!(c.live_classes, 1);
        assert_eq!(c.missing, vec![(1, MissingHolderState::Hopeless)]);
        assert_eq!(c.classes, vec![(0, vec![1]), (1, vec![])]);

        // Nobody online: every class of every chunk is missing.
        let nobody = std::collections::HashSet::new();
        let all = find_chunks_with_missing_classes(&conn, &nobody, &members).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].live_classes, 0);
    }
}
