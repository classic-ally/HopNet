use crate::db::{DatabaseError, CustomUUID};
use duckdb::DuckdbConnectionManager;
use r2d2;

/// Find orphaned data blocks with no inode references, ordered by age (oldest first)
/// Returns data block IDs older than the cutoff UUID, limited by batch size
pub fn find_orphaned_data_blocks(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    cutoff_uuid: &CustomUUID,
    limit: i32,
) -> Result<Vec<CustomUUID>, DatabaseError> {
    match db_connection {
        Ok(conn) => {
            let mut stmt = conn.prepare(
                "SELECT db.id
                 FROM data_blocks db
                 LEFT JOIN inodes i ON db.id = i.data_id
                 WHERE i.data_id IS NULL
                   AND db.id < ?
                 ORDER BY db.id ASC
                 LIMIT ?"
            ).map_err(|_| DatabaseError::RecallError)?;
            
            let data_blocks = stmt.query_map(duckdb::params![cutoff_uuid, limit], |row| {
                let data_block_id: CustomUUID = row.get(0)?;
                Ok(data_block_id)
            }).map_err(|_| DatabaseError::RecallError)?;
            
            data_blocks.collect::<Result<Vec<_>, _>>()
                .map_err(|_| DatabaseError::RecallError)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

#[derive(Debug, PartialEq)]
pub enum AvailabilityClass {
    BelowAverage,  // Clean historical first, keep redundant copies
    AboveAverage,  // Clean redundant first, keep historical data
}

/// Get node's availability and classify it relative to network average
/// Returns (node_availability, classification)
pub fn get_node_availability_classification(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    node_id: i32,
    days: i32,
) -> Result<(f64, AvailabilityClass), DatabaseError> {
    match db_connection {
        Ok(conn) => {
            // First try to get network average
            let network_mean = conn.prepare(
                "SELECT AVG(CAST(available AS DOUBLE)) as network_mean
                 FROM metrics 
                 WHERE timestamp > NOW() - INTERVAL ? DAY"
            ).and_then(|mut stmt| {
                stmt.query_row(duckdb::params![days], |row| {
                    let mean: Option<f64> = row.get(0)?;
                    Ok(mean)
                })
            }).unwrap_or(None);
            
            // Then try to get node availability
            let node_availability = conn.prepare(
                "SELECT AVG(CAST(available AS DOUBLE)) as node_availability
                 FROM metrics 
                 WHERE node_id = ? AND timestamp > NOW() - INTERVAL ? DAY"
            ).and_then(|mut stmt| {
                stmt.query_row(duckdb::params![node_id, days], |row| {
                    let avail: Option<f64> = row.get(0)?;
                    Ok(avail)
                })
            }).unwrap_or(None);
            
            // Use defaults if no metrics available
            let node_availability = node_availability.unwrap_or_else(|| {
                tracing::warn!("No metrics found for node {}, using default availability 0.8", node_id);
                0.8
            });
            let network_mean = network_mean.unwrap_or_else(|| {
                tracing::warn!("No network metrics found, using default network mean 0.8");
                0.8
            });
            
            tracing::debug!("Node {} availability: {:.1}%, network mean: {:.1}%", 
                           node_id, node_availability * 100.0, network_mean * 100.0);
            
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

/// Delete orphaned data blocks and their associated fragment_hashes records
/// Uses explicit deletion (not CASCADE) for visibility and control
/// The execute parameter controls whether to actually delete or just validate
/// Returns fragment hashes that were deleted (for opportunistic local cleanup)
pub fn delete_orphaned_data_blocks_consensus(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    data_block_ids: Vec<CustomUUID>,
    execute: bool,
) -> Result<Vec<crate::db::Blake3Hash>, DatabaseError> {
    match db_connection {
        Ok(mut conn) => {
            if data_block_ids.is_empty() {
                return Ok(Vec::new());
            }

            let mut deleted_fragment_hashes = Vec::new();
            
            if execute {
                // Use two-transaction approach to avoid DuckDB foreign key constraint timing issues
                // Transaction 1: Delete all reference records
                tracing::debug!("Starting reference deletion transaction");
                let tx1 = conn.transaction().map_err(|e| {
                    tracing::error!("Failed to begin reference deletion transaction: {:?}", e);
                    DatabaseError::LockError
                })?;
                
                // Build parameter placeholders for the IN clause
                let placeholders: Vec<String> = (0..data_block_ids.len()).map(|_| "?".to_string()).collect();
                let placeholders_str = placeholders.join(", ");
                
                // First collect fragment hashes that are stored locally (for opportunistic cleanup)
                let select_local_fragments_query = format!(
                    "SELECT fragment_hash FROM fragment_hashes WHERE data_block_id IN ({}) AND stored_locally = TRUE", 
                    placeholders_str
                );
                let select_params: Vec<&dyn duckdb::ToSql> = data_block_ids.iter()
                    .map(|id| id as &dyn duckdb::ToSql)
                    .collect();
                
                let mut stmt = tx1.prepare(&select_local_fragments_query).map_err(|e| {
                    tracing::error!("Failed to prepare local fragment selection query: {:?}", e);
                    DatabaseError::ProcessingError
                })?;
                
                let fragment_hashes = stmt.query_map(select_params.as_slice(), |row| {
                    let hash: crate::db::Blake3Hash = row.get(0)?;
                    Ok(hash)
                }).map_err(|e| {
                    tracing::error!("Failed to query local fragment hashes: {:?}", e);
                    DatabaseError::RecallError
                })?;
                
                for hash_result in fragment_hashes {
                    deleted_fragment_hashes.push(hash_result.map_err(|_| DatabaseError::ProcessingError)?);
                }
                
                tracing::debug!("Found {} locally stored fragment hashes for deletion", deleted_fragment_hashes.len());
                
                // Now delete fragment_hashes records
                let fragment_query = format!(
                    "DELETE FROM fragment_hashes WHERE data_block_id IN ({})", 
                    placeholders_str
                );
                let fragment_params: Vec<&dyn duckdb::ToSql> = data_block_ids.iter()
                    .map(|id| id as &dyn duckdb::ToSql)
                    .collect();
                
                tracing::debug!("Executing fragment deletion query: {}", fragment_query);
                tracing::debug!("Fragment deletion parameters: {:?}", data_block_ids);
                
                let fragments_deleted = tx1.execute(&fragment_query, fragment_params.as_slice())
                    .map_err(|e| {
                        tracing::error!("Failed to delete fragment_hashes: {:?}", e);
                        DatabaseError::ProcessingError
                    })?;
                
                tracing::info!("Deleted {} fragment_hashes records", fragments_deleted);
                
                // Then delete file_access records (encrypted file keys)
                let access_query = format!(
                    "DELETE FROM file_access WHERE data_block_id IN ({})", 
                    placeholders_str
                );
                let access_params: Vec<&dyn duckdb::ToSql> = data_block_ids.iter()
                    .map(|id| id as &dyn duckdb::ToSql)
                    .collect();
                
                tracing::debug!("Executing file_access deletion query: {}", access_query);
                tracing::debug!("File_access deletion parameters: {:?}", data_block_ids);
                
                let access_deleted = tx1.execute(&access_query, access_params.as_slice())
                    .map_err(|e| {
                        tracing::error!("Failed to delete file_access: {:?}", e);
                        DatabaseError::ProcessingError
                    })?;
                
                tracing::info!("Deleted {} file_access records", access_deleted);
                
                // Commit transaction 1 (reference deletions)
                tx1.commit().map_err(|e| {
                    tracing::error!("Failed to commit reference deletion transaction: {:?}", e);
                    DatabaseError::ProcessingError
                })?;
                
                tracing::debug!("Reference deletion transaction committed successfully");
                
                // Transaction 2: Delete data_blocks records
                tracing::debug!("Starting data_blocks deletion transaction");
                let tx2 = conn.transaction().map_err(|e| {
                    tracing::error!("Failed to begin data_blocks deletion transaction: {:?}", e);
                    DatabaseError::LockError
                })?;
                
                // Then delete data_blocks records
                let blocks_query = format!(
                    "DELETE FROM data_blocks WHERE id IN ({})", 
                    placeholders_str
                );
                let block_params: Vec<&dyn duckdb::ToSql> = data_block_ids.iter()
                    .map(|id| id as &dyn duckdb::ToSql)
                    .collect();
                
                tracing::debug!("Executing data_blocks deletion query: {}", blocks_query);
                tracing::debug!("Data_blocks deletion parameters: {:?}", data_block_ids);
                    
                let blocks_deleted = tx2.execute(&blocks_query, block_params.as_slice())
                    .map_err(|e| {
                        tracing::error!("Failed to delete data_blocks: {:?}", e);
                        DatabaseError::ProcessingError
                    })?;
                
                // Commit transaction 2 (data_blocks deletion)
                tx2.commit().map_err(|e| {
                    tracing::error!("Failed to commit data_blocks deletion transaction: {:?}", e);
                    DatabaseError::ProcessingError
                })?;
                
                tracing::debug!("Data_blocks deletion transaction committed successfully");
                
                tracing::info!("Consensus deletion completed: {} data blocks, {} fragments, {} file access entries", 
                              blocks_deleted, fragments_deleted, access_deleted);
            } else {
                // Validation mode - create transaction but roll it back
                let tx = conn.transaction().map_err(|e| {
                    tracing::error!("Failed to begin validation transaction: {:?}", e);
                    DatabaseError::LockError
                })?;
                
                // Check if the data blocks exist and are truly orphaned
                for data_block_id in &data_block_ids {
                    // Verify the data block exists
                    let exists: bool = tx.query_row(
                        "SELECT COUNT(*) > 0 FROM data_blocks WHERE id = ?",
                        duckdb::params![data_block_id],
                        |row| row.get(0)
                    ).map_err(|_| DatabaseError::RecallError)?;
                    
                    if !exists {
                        tracing::warn!("Data block {} does not exist, skipping", data_block_id);
                        continue;
                    }
                    
                    // Verify it's truly orphaned (no inode references)
                    let has_inodes: bool = tx.query_row(
                        "SELECT COUNT(*) > 0 FROM inodes WHERE data_id = ?",
                        duckdb::params![data_block_id],
                        |row| row.get(0)
                    ).map_err(|_| DatabaseError::RecallError)?;
                    
                    if has_inodes {
                        tracing::error!("Data block {} is not orphaned, has active inode references", data_block_id);
                        return Err(DatabaseError::ProcessingError);
                    }
                }
                
                // Rollback the validation transaction
                tx.rollback().map_err(|e| {
                    tracing::error!("Failed to rollback validation transaction: {:?}", e);
                    DatabaseError::LockError
                })?;
                
                tracing::debug!("Validation passed: {} data blocks are orphaned and can be deleted (rolled back)", 
                               data_block_ids.len());
            }
            
            Ok(deleted_fragment_hashes)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}