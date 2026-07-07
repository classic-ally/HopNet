use crate::db::{CustomUUID, DatabaseError};
use crate::reference_providers::DataBlockReferenceProvider;
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;

/// Find orphaned data blocks with no references from any provider, ordered by age (oldest first)
/// Returns data block IDs older than the cutoff UUID, limited by batch size
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

/// Get data blocks that need rebalancing (distributed before a certain height)
/// Returns data blocks with their fragments, ordered by placement_height (oldest first)
pub fn get_data_blocks_for_rebalancing(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    max_placement_height: i32,
    limit: i32,
) -> Result<Vec<DataBlockRebalanceInfo>, DatabaseError> {
    let conn = db_connection.map_err(|e| {
        tracing::error!("Failed to get database connection for rebalancing: {:?}", e);
        DatabaseError::LockError
    })?;

    tracing::debug!(
        "Getting data blocks for rebalancing with max_placement_height={}, limit={}",
        max_placement_height,
        limit
    );

    // Get data blocks that were placed before the specified height
    let query = "SELECT DISTINCT db.id, db.placement_height, db.fragment_count
         FROM data_blocks db
         WHERE db.placement_height IS NOT NULL 
           AND db.placement_height < ?
         ORDER BY db.placement_height ASC
         LIMIT ?";

    tracing::debug!("Preparing rebalancing query: {}", query);
    let mut stmt = conn.prepare(query).map_err(|e| {
        tracing::error!("Failed to prepare rebalancing query: {:?}", e);
        DatabaseError::RecallError
    })?;

    tracing::debug!(
        "Executing query with params: max_placement_height={}, limit={}",
        max_placement_height,
        limit
    );
    let data_blocks = stmt
        .query_map(rusqlite::params![max_placement_height, limit], |row| {
            tracing::debug!("Parsing data block row");
            let data_block_id: CustomUUID = row.get(0).map_err(|e| {
                tracing::error!("Failed to get data_block_id from row: {:?}", e);
                e
            })?;
            let placement_height: i32 = row.get(1).map_err(|e| {
                tracing::error!("Failed to get placement_height from row: {:?}", e);
                e
            })?;
            let total_fragments: i32 = row.get(2).map_err(|e| {
                tracing::error!("Failed to get fragment_count from row: {:?}", e);
                e
            })?;
            tracing::debug!(
                "Successfully parsed data block: id={}, height={}, fragments={}",
                data_block_id,
                placement_height,
                total_fragments
            );
            Ok((data_block_id, placement_height, total_fragments))
        })
        .map_err(|e| {
            tracing::error!("Failed to execute rebalancing query: {:?}", e);
            DatabaseError::RecallError
        })?;

    let mut result = Vec::new();
    let mut data_block_count = 0;

    // For each data block, get all its fragments
    for data_block_result in data_blocks {
        let (data_block_id, placement_height, total_fragments) =
            data_block_result.map_err(|e| {
                tracing::error!("Failed to process data block result: {:?}", e);
                DatabaseError::RecallError
            })?;

        data_block_count += 1;
        tracing::debug!(
            "Processing data block {} ({}/{}): id={}, height={}, expected_fragments={}",
            data_block_count,
            data_block_count,
            "unknown",
            data_block_id,
            placement_height,
            total_fragments
        );

        // Get all fragments for this data block
        let fragment_query = "SELECT fragment_hash, chunk_type
             FROM fragment_hashes
             WHERE data_block_id = ?
             ORDER BY chunk_number";

        tracing::debug!(
            "Preparing fragment query for data block {}: {}",
            data_block_id,
            fragment_query
        );
        let mut fragment_stmt = conn.prepare(fragment_query).map_err(|e| {
            tracing::error!(
                "Failed to prepare fragment query for data block {}: {:?}",
                data_block_id,
                e
            );
            DatabaseError::RecallError
        })?;

        tracing::debug!("Executing fragment query for data block {}", data_block_id);
        let fragments = fragment_stmt
            .query_map(rusqlite::params![&data_block_id], |row| {
                let fragment_hash: crate::db::Blake3Hash = row.get(0).map_err(|e| {
                    tracing::error!("Failed to get fragment_hash from row: {:?}", e);
                    e
                })?;
                let chunk_type: crate::db::ChunkType = row.get(1).map_err(|e| {
                    tracing::error!("Failed to get chunk_type from row: {:?}", e);
                    e
                })?;
                let chunk_type_str = match chunk_type {
                    crate::db::ChunkType::Original => "original".to_string(),
                    crate::db::ChunkType::Recovery => "recovery".to_string(),
                };
                tracing::debug!(
                    "Found fragment: hash={}, type={}",
                    fragment_hash.to_hex(),
                    chunk_type_str
                );
                Ok((fragment_hash, chunk_type_str))
            })
            .map_err(|e| {
                tracing::error!(
                    "Failed to execute fragment query for data block {}: {:?}",
                    data_block_id,
                    e
                );
                DatabaseError::RecallError
            })?;

        let mut fragment_list = Vec::new();
        for fragment_result in fragments {
            let (hash, chunk_type) = fragment_result.map_err(|e| {
                tracing::error!(
                    "Failed to process fragment result for data block {}: {:?}",
                    data_block_id,
                    e
                );
                DatabaseError::RecallError
            })?;
            fragment_list.push(FragmentInfo {
                fragment_hash: hash,
                chunk_type,
            });
        }

        tracing::debug!(
            "Data block {} has {} fragments (expected {})",
            data_block_id,
            fragment_list.len(),
            total_fragments
        );

        // Only include data blocks where we have all fragments
        if fragment_list.len() == total_fragments as usize {
            tracing::debug!(
                "Data block {} has complete fragment set, adding to result",
                data_block_id
            );
            result.push(DataBlockRebalanceInfo {
                data_block_id,
                placement_height,
                fragments: fragment_list,
            });
        } else {
            tracing::warn!(
                "Data block {} has {} fragments but expected {}, skipping",
                data_block_id,
                fragment_list.len(),
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

#[derive(Debug, Clone)]
pub struct DataBlockRebalanceInfo {
    pub data_block_id: CustomUUID,
    pub placement_height: i32,
    pub fragments: Vec<FragmentInfo>,
}

#[derive(Debug, Clone)]
pub struct FragmentInfo {
    pub fragment_hash: crate::db::Blake3Hash,
    pub chunk_type: String,
}
