use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Transaction, params};
use std::collections::HashMap;
use tracing::debug;

use crate::db::DatabaseError;
use crate::files::types::SelfCheckFragments;
use crate::types::{Blake3Hash, NodeConnectionInfo};

/// Apply differential fragment inventory updates from a self-check report
/// Called by consensus middleware when processing SelfCheckFragments transactions
///
/// The execute flag controls whether to actually apply changes (true) or just validate (false)
pub fn apply_self_check_updates(
    db_tx: &rusqlite::Transaction,
    report: &SelfCheckFragments,
) -> Result<(), DatabaseError> {
    // Verify the previous count matches our current state
    let current_count = get_node_fragment_count_tx(db_tx, report.node_id)?;

    // For operations that only add fragments (upload attestations), we can tolerate
    // the count being higher due to concurrent additions from other uploads.
    // For operations that remove fragments (periodic self-checks), we need exact match
    // to ensure we're removing the right things.
    if report.fragments_removed.is_empty() {
        // Addition-only operation: allow count to be equal or higher
        if current_count < report.previous_count {
            tracing::error!(
                "Fragment inventory count decreased unexpectedly for node {}: expected >= {}, found {}",
                report.node_id,
                report.previous_count,
                current_count
            );
            return Err(DatabaseError::ProcessingError);
        }
        if current_count > report.previous_count {
            tracing::debug!(
                "Fragment inventory count increased due to concurrent operations for node {}: expected {}, found {} (this is OK for addition-only operations)",
                report.node_id,
                report.previous_count,
                current_count
            );
        }
    } else {
        // Removal operation: require exact match for safety
        if current_count != report.previous_count {
            tracing::error!(
                "Fragment inventory state mismatch for node {} (removal operation requires exact count): expected {} fragments, found {}",
                report.node_id,
                report.previous_count,
                current_count
            );
            return Err(DatabaseError::ProcessingError);
        }
    }

    // Apply changes in optimal order to avoid double operations:

    // 1. Remove fragments that no longer exist
    if !report.fragments_removed.is_empty() {
        remove_fragments_tx(db_tx, report.node_id, &report.fragments_removed)?;

        debug!(
            "Removed {} fragments from inventory for node {}",
            report.fragments_removed.len(),
            report.node_id
        );
    }

    // 2. Update self_verified_height for all remaining fragments owned by this node
    update_verified_height_tx(db_tx, report.node_id, report.self_verified_height)?;

    debug!(
        "Updated self_verified_height to {} for all fragments of node {}",
        report.self_verified_height, report.node_id
    );

    // 3. Insert newly discovered fragments (already have correct verified_height)
    if !report.fragments_added.is_empty() {
        insert_fragments_tx(
            db_tx,
            report.node_id,
            &report.fragments_added,
            report.self_verified_height,
        )?;

        debug!(
            "Added {} fragments to inventory for node {}",
            report.fragments_added.len(),
            report.node_id
        );
    }

    Ok(())
}

/// Get the current fragment count using a transaction
fn get_node_fragment_count_tx(tx: &Transaction, node_id: i32) -> Result<u32, DatabaseError> {
    let mut stmt = tx
        .prepare("SELECT COUNT(*) FROM fragment_inventory WHERE node_id = ?")
        .map_err(|_| DatabaseError::ProcessingError)?;

    let count: i64 = stmt
        .query_row(params![node_id], |row| row.get(0))
        .map_err(|_| DatabaseError::RecallError)?;

    Ok(count as u32)
}

/// Insert fragments into inventory using a transaction
fn insert_fragments_tx(
    tx: &Transaction,
    node_id: i32,
    fragments: &[Blake3Hash],
    verified_height: i32,
) -> Result<(), DatabaseError> {
    for fragment_hash in fragments {
        tx.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height)
             VALUES (?, ?, ?)",
            params![fragment_hash, node_id, verified_height],
        )
        .map_err(|_| DatabaseError::InsertError)?;
    }

    Ok(())
}

/// Remove fragments from inventory using a transaction
fn remove_fragments_tx(
    tx: &Transaction,
    node_id: i32,
    fragments: &[Blake3Hash],
) -> Result<(), DatabaseError> {
    for fragment_hash in fragments {
        tx.execute(
            "DELETE FROM fragment_inventory WHERE fragment_hash = ? AND node_id = ?",
            params![fragment_hash, node_id],
        )
        .map_err(|_| DatabaseError::ProcessingError)?;
    }

    Ok(())
}

/// Update verified height for all fragments of a node using a transaction
fn update_verified_height_tx(
    tx: &Transaction,
    node_id: i32,
    verified_height: i32,
) -> Result<(), DatabaseError> {
    tx.execute(
        "UPDATE fragment_inventory
         SET self_verified_height = ?
         WHERE node_id = ?",
        params![verified_height, node_id],
    )
    .map_err(|_| DatabaseError::ProcessingError)?;

    Ok(())
}

/// Compute the differential between inventory and local fragments for a node
/// Returns a complete SelfCheckFragments struct ready for consensus submission
/// Uses high-performance EXCEPT queries optimized for DuckDB's columnar architecture
/// Uses transaction semantics for consistency guarantees in distributed environment
pub fn compute_inventory_differential(
    db_connection: Result<PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    node_id: i32,
) -> Result<SelfCheckFragments, DatabaseError> {
    match db_connection {
        Ok(mut conn) => {
            // Use transaction for consistent snapshot
            let tx = conn.transaction().map_err(|_| DatabaseError::LockError)?;

            // Get current inventory count and consensus height
            let previous_count = get_node_fragment_count_tx(&tx, node_id)?;
            let self_verified_height = crate::db::consensus::get_current_consensus_height(&tx)?;

            // Fragments we have locally but not in inventory (to be added)
            let fragments_added = {
                let mut stmt = tx.prepare(
                    "SELECT DISTINCT fragment_hash FROM fragment_hashes WHERE stored_locally = true
                     EXCEPT
                     SELECT fragment_hash FROM fragment_inventory WHERE node_id = ?"
                ).map_err(|_| DatabaseError::ProcessingError)?;
                stmt.query_map(params![node_id], |row| {
                    let fragment_hash: Blake3Hash = row.get(0)?;
                    Ok(fragment_hash)
                })
                .map_err(|_| DatabaseError::RecallError)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DatabaseError::RecallError)?
            };

            // Fragments in inventory but not stored locally (to be removed)
            let fragments_removed = {
                let mut stmt = tx.prepare(
                    "SELECT fragment_hash FROM fragment_inventory WHERE node_id = ?
                     EXCEPT
                     SELECT DISTINCT fragment_hash FROM fragment_hashes WHERE stored_locally = true"
                ).map_err(|_| DatabaseError::ProcessingError)?;
                stmt.query_map(params![node_id], |row| {
                    let fragment_hash: Blake3Hash = row.get(0)?;
                    Ok(fragment_hash)
                })
                .map_err(|_| DatabaseError::RecallError)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DatabaseError::RecallError)?
            };

            // Read-only transaction — auto-rollback on drop
            drop(tx);

            // Assemble complete SelfCheckFragments struct
            Ok(SelfCheckFragments {
                node_id,
                self_verified_height,
                previous_count,
                fragments_added,
                fragments_removed,
            })
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Batch query fragment inventory to find nodes that claim to have specific fragments
/// Returns a map from fragment hash to list of nodes, ordered by verification recency
/// Optimized for minimal database round-trips when looking up many fragments at once
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

                result
                    .entry(fragment_hash)
                    .or_default()
                    .push(node_info);
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
