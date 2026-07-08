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
