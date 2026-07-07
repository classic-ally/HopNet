//! Takeout DB shim (RFC-015 Stage D5b): the takeout-owned SQL moved to
//! `hopnet_takeout::db::takeout`; re-exported here so the snapshotter and
//! host call sites are unchanged. The two fns below stay HOST-side: they
//! read host domains (node metrics + filesystem capacity) and projection
//! sizing SQL, and reach the takeout core through `TakeoutHooks`.

pub use hopnet_takeout::db::entries::MaterializationStatus;
pub use hopnet_takeout::db::takeout::*;

use crate::db::DatabaseError;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OptionalExtension, params};

/// Calculate total user data size in bytes
pub fn calculate_user_data_size(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    user_id: i32,
) -> Result<u64, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let total_size: Option<i64> = db_lock
                .query_row(
                    "SELECT COALESCE(SUM(db.file_size), 0) FROM inodes i
                 INNER JOIN data_blocks db ON i.data_id = db.id
                 WHERE i.owner_id = ? AND i.type = 0",
                    params![user_id],
                    |row| row.get(0),
                )
                .map_err(|_| DatabaseError::RecallError)?;

            Ok(total_size.unwrap_or(0) as u64)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get current node's available storage capacity in bytes
/// If no recent metrics available, calculates storage directly from filesystem
pub async fn get_node_available_storage(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    app_state: &crate::AppState,
    node_id: i32,
) -> Result<Option<u64>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // First, try to get existing storage metrics
            let storage_info: Option<(Option<u32>, Option<u32>)> = db_lock
                .query_row(
                    "SELECT storage_total_gb, storage_used_gb FROM metrics
                 WHERE to_node = ? AND storage_total_gb IS NOT NULL
                 ORDER BY start_time DESC LIMIT 1",
                    params![node_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|_| DatabaseError::RecallError)?;

            match storage_info {
                Some((Some(total_gb), Some(used_gb))) => {
                    let available_gb = total_gb.saturating_sub(used_gb);
                    Ok(Some(available_gb as u64 * 1024 * 1024 * 1024)) // Convert GB to bytes
                }
                _ => {
                    // No storage metrics available - calculate directly from filesystem
                    tracing::warn!(
                        "No storage metrics found for node {}, calculating from filesystem",
                        node_id
                    );

                    // Use the local storage calculation function
                    match crate::metrics::routes::calculate_storage_usage(&app_state.fragments_dir)
                        .await
                    {
                        Ok(storage_response) => {
                            let available_gb = storage_response
                                .total_gb
                                .saturating_sub(storage_response.used_gb);
                            tracing::info!(
                                "Calculated fresh storage metrics: {}/{} GB available",
                                available_gb,
                                storage_response.total_gb
                            );
                            Ok(Some(available_gb as u64 * 1024 * 1024 * 1024)) // Convert GB to bytes
                        }
                        Err(e) => {
                            tracing::error!("Failed to calculate storage usage: {}", e);
                            Ok(None) // Return None if calculation fails
                        }
                    }
                }
            }
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}
