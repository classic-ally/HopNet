use crate::db::{CustomUUID, DatabaseError};
use crate::reference_providers::DataBlockReferenceProvider;
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;

/// Find orphaned data blocks with no references from any provider, ordered by age (oldest first)
/// Returns data block IDs older than the cutoff UUID, limited by batch size
///
/// Stays host: iterates the projection-layer DataBlockReferenceProvider
/// registry, which the storage substrate can never see.
pub fn find_orphaned_data_blocks(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    cutoff_uuid: &CustomUUID,
    limit: i32,
) -> Result<Vec<CustomUUID>, DatabaseError> {
    match db_connection {
        Ok(conn) => {
            let mut exclusions: Vec<String> = Vec::new();
            for provider in inventory::iter::<&'static dyn DataBlockReferenceProvider> {
                exclusions.push(format!(
                    "AND db.id NOT IN ({})",
                    provider.referenced_data_blocks_subquery()
                ));
            }
            let exclusion_clause = exclusions.join("\n                   ");

            let query = format!(
                "SELECT db.id
                 FROM data_blocks db
                 WHERE db.id < ?
                   {}
                 ORDER BY db.id ASC
                 LIMIT ?",
                exclusion_clause
            );

            let mut stmt = conn
                .prepare(&query)
                .map_err(|_| DatabaseError::RecallError)?;

            let data_blocks = stmt
                .query_map(rusqlite::params![cutoff_uuid, limit], |row| {
                    let data_block_id: CustomUUID = row.get(0)?;
                    Ok(data_block_id)
                })
                .map_err(|_| DatabaseError::RecallError)?;

            data_blocks
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DatabaseError::RecallError)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

#[derive(Debug, PartialEq)]
pub enum AvailabilityClass {
    BelowAverage, // Clean historical first, keep redundant copies
    AboveAverage, // Clean redundant first, keep historical data
}

/// Get node's availability and classify it relative to network average
/// Returns (node_availability, classification)
///
/// Stays host: reads the host-owned metrics table.
pub fn get_node_availability_classification(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    node_id: i32,
    days: i32,
) -> Result<(f64, AvailabilityClass), DatabaseError> {
    match db_connection {
        Ok(conn) => {
            // First try to get network average
            let network_mean = conn
                .prepare(
                    "SELECT AVG(CAST(available AS REAL)) as network_mean
                 FROM metrics
                 WHERE start_time > datetime('now', '-' || ? || ' days')",
                )
                .and_then(|mut stmt| {
                    stmt.query_row(rusqlite::params![days], |row| {
                        let mean: Option<f64> = row.get(0)?;
                        Ok(mean)
                    })
                })
                .unwrap_or(None);

            // Then try to get node availability
            let node_availability = conn
                .prepare(
                    "SELECT AVG(CAST(available AS REAL)) as node_availability
                 FROM metrics
                 WHERE to_node = ? AND start_time > datetime('now', '-' || ? || ' days')",
                )
                .and_then(|mut stmt| {
                    stmt.query_row(rusqlite::params![node_id, days], |row| {
                        let avail: Option<f64> = row.get(0)?;
                        Ok(avail)
                    })
                })
                .unwrap_or(None);

            // Use defaults if no metrics available
            let node_availability = node_availability.unwrap_or_else(|| {
                tracing::warn!(
                    "No metrics found for node {}, using default availability 0.8",
                    node_id
                );
                0.8
            });
            let network_mean = network_mean.unwrap_or_else(|| {
                tracing::warn!("No network metrics found, using default network mean 0.8");
                0.8
            });

            tracing::debug!(
                "Node {} availability: {:.1}%, network mean: {:.1}%",
                node_id,
                node_availability * 100.0,
                network_mean * 100.0
            );

            let classification = if node_availability < network_mean {
                AvailabilityClass::BelowAverage
            } else {
                AvailabilityClass::AboveAverage
            };

            Ok((node_availability, classification))
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

// delete_orphaned_data_blocks_consensus moved to
// crate::storage_host::db_apply (RFC-016 Stage 6) — it lives beside its
// consensus-handler caller.

// get_data_blocks_for_rebalancing (+ DataBlockRebalanceInfo/FragmentInfo)
// moved to hopnet_storage::store (RFC-017 Stage 5) — it touches only
// storage-owned tables (data_blocks, fragment_hashes).

/// Batch-update local storage state for multiple fragments in a single transaction.
/// Acquires the write lock once instead of once per fragment, avoiding sustained
/// lock contention with consensus operations.
pub fn mark_fragments_local_state_batch(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    fragment_hashes: &[crate::types::Blake3Hash],
    stored_locally: bool,
) -> Result<usize, DatabaseError> {
    if fragment_hashes.is_empty() {
        return Ok(0);
    }
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|e| {
                tracing::error!(
                    "Failed to begin transaction for batch fragment update: {:?}",
                    e
                );
                DatabaseError::LockError
            })?;
            // Substrate-owned write (RFC-014 stored_locally invariant) —
            // the host owns only the conn + commit telemetry.
            let total_rows =
                hopnet_storage::store::mark_local_state_batch(&tx, fragment_hashes, stored_locally)
                    .map_err(|e| {
                        tracing::error!("mark_local_state_batch failed: {e}");
                        DatabaseError::ProcessingError
                    })?;
            crate::db::shared::commit_timed(tx).map_err(|e| {
                tracing::error!("Failed to commit batch fragment update: {:?}", e);
                DatabaseError::InsertError
            })?;

            let state_text = if stored_locally {
                "stored locally"
            } else {
                "not stored locally"
            };
            tracing::debug!(
                "Batch-marked {} fragment records ({} hashes) as {}",
                total_rows,
                fragment_hashes.len(),
                state_text
            );
            Ok(total_rows)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get count of fragments stored locally on this node
pub fn get_local_fragment_count(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<i64, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let count = db_lock
                .query_row(
                    "SELECT COUNT(*) FROM fragment_hashes WHERE stored_locally = TRUE",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|e| {
                    tracing::error!("Error querying local fragment count: {:?}", e);
                    DatabaseError::RecallError
                })?;

            tracing::debug!("Found {} fragments stored locally", count);
            Ok(count)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// One disk fragment's control-plane facts for eviction classification
/// (RFC-STORAGE-002 S5).
#[derive(Debug)]
pub struct DiskFragmentInfo {
    pub blob_id: String,
    pub local_index: u32,
}

/// Look up (blob, class) for on-disk fragment hashes. Hashes absent from
/// fragment_hashes are orphans — the orphan GC flow owns those, not
/// eviction.
pub fn lookup_disk_fragments(
    conn: &rusqlite::Connection,
    hashes: &[crate::types::Blake3Hash],
) -> Result<std::collections::HashMap<crate::types::Blake3Hash, DiskFragmentInfo>, DatabaseError> {
    let mut out = std::collections::HashMap::new();
    for chunk in hashes.chunks(500) {
        let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "SELECT fragment_hash, data_block_id, local_index
             FROM fragment_hashes WHERE fragment_hash IN ({placeholders})"
        );
        let mut stmt = conn
            .prepare(&query)
            .map_err(|_| DatabaseError::RecallError)?;
        let params: Vec<&dyn rusqlite::ToSql> =
            chunk.iter().map(|h| h as &dyn rusqlite::ToSql).collect();
        let mut rows = stmt
            .query(params.as_slice())
            .map_err(|_| DatabaseError::RecallError)?;
        while let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
            let hash: crate::types::Blake3Hash =
                row.get(0).map_err(|_| DatabaseError::ProcessingError)?;
            out.insert(
                hash,
                DiskFragmentInfo {
                    blob_id: row.get(1).map_err(|_| DatabaseError::ProcessingError)?,
                    local_index: row.get(2).map_err(|_| DatabaseError::ProcessingError)?,
                },
            );
        }
    }
    Ok(out)
}

/// Per-hash count of OTHER member nodes attesting a copy in the replicated
/// inventory — the eviction guard's input. Filtered by the member view:
/// departed nodes' lingering rows must never count as holders.
pub fn member_holder_counts(
    conn: &rusqlite::Connection,
    hashes: &[crate::types::Blake3Hash],
    member_nodes: &std::collections::HashSet<i32>,
    my_node_id: i32,
) -> Result<std::collections::HashMap<crate::types::Blake3Hash, usize>, DatabaseError> {
    let mut out = std::collections::HashMap::new();
    for chunk in hashes.chunks(500) {
        let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "SELECT fragment_hash, node_id FROM fragment_inventory
             WHERE fragment_hash IN ({placeholders})"
        );
        let mut stmt = conn
            .prepare(&query)
            .map_err(|_| DatabaseError::RecallError)?;
        let params: Vec<&dyn rusqlite::ToSql> =
            chunk.iter().map(|h| h as &dyn rusqlite::ToSql).collect();
        let mut rows = stmt
            .query(params.as_slice())
            .map_err(|_| DatabaseError::RecallError)?;
        while let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
            let hash: crate::types::Blake3Hash =
                row.get(0).map_err(|_| DatabaseError::ProcessingError)?;
            let node: i32 = row.get(1).map_err(|_| DatabaseError::ProcessingError)?;
            if node != my_node_id && member_nodes.contains(&node) {
                *out.entry(hash).or_insert(0) += 1;
            }
        }
    }
    Ok(out)
}
