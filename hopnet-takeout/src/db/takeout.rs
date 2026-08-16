//! Takeout-side DB operations. Moved from the host's `db::takeout` at
//! RFC-015 Stage D5b. The consensus apply halves live here (pure DB — the
//! handlers in `crate::handlers` drive them); decision 3 removed the
//! in-apply inode snapshot, so `apply_takeout_creation` is now validation +
//! the `takeouts` row only, and enumeration happens in the scheduled
//! materialization task.

use chrono::{DateTime, Utc};
use hopnet_common::height::{height_from_db, height_to_db};
use hopnet_common::{CustomUUID, TakeoutRecord, TakeoutStatus};
use hopnet_projection::DatabaseError;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use serde::{Deserialize, Serialize};

use super::entries;
use hopnet_projection::CustomDateTime;

/// Unified payload for takeout operations (creation, updates, sync).
/// Field order is the bincode wire shape — do not reorder.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TakeoutPayload {
    pub takeout_id: CustomUUID,
    pub user_id: i32,
    pub owner_node_id: i32,
    pub status: TakeoutStatus,
    pub expires_at: CustomDateTime,
    pub consensus_height: u64,
}

/// Payload for takeout status updates (consensus-tracked)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TakeoutStatusPayload {
    pub takeout_id: CustomUUID,
    pub new_status: TakeoutStatus,
}

impl TakeoutPayload {
    /// Convert to TakeoutRecord for frontend API
    pub fn to_record(&self) -> TakeoutRecord {
        // Extract creation timestamp from UUIDv7
        let created_at = match self.takeout_id.get_timestamp() {
            Some(ts) => {
                let unix_ts = ts.to_unix();
                chrono::DateTime::from_timestamp(unix_ts.0 as i64, unix_ts.1)
                    .unwrap_or_else(chrono::Utc::now)
            }
            None => chrono::Utc::now(),
        };

        TakeoutRecord {
            id: self.takeout_id.to_string(),
            user_id: self.user_id,
            owner_node_id: self.owner_node_id,
            status: self.status.clone(),
            created_at,
            expires_at: *self.expires_at,
            consensus_height: self.consensus_height,
        }
    }
}

/// Apply a takeout creation on the shared block transaction (consensus
/// transaction processing). Pure DB half — no host state.
///
/// RFC-015 D5 decision 3: apply = validation + the `takeouts` row ONLY. The
/// old in-apply inode snapshot (temp table INSERT..SELECT) is gone —
/// enumeration now happens inside the scheduled materialization task, per
/// registered exporter. The handler still schedules
/// `ctx.work.schedule("takeout.materialize", …)` on the owner node.
pub fn apply_takeout_creation(
    db_tx: &rusqlite::Transaction,
    payload: &TakeoutPayload,
    current_node_id: Option<i32>,
    execute: bool,
) -> Result<(), DatabaseError> {
    tracing::debug!(
        "Processing takeout creation for user_id: {} (execute={})",
        payload.user_id,
        execute
    );

    // Check if user already has an active takeout (validation for all nodes)
    if has_active_takeout_tx(db_tx, Some(payload.user_id))? {
        tracing::debug!("User {} already has an active takeout", payload.user_id);
        return Err(DatabaseError::ConflictError);
    }

    // Insert the takeout record (all nodes do this)
    db_tx.execute(
        "INSERT INTO takeouts (id, user_id, owner_node_id, status, expires_at, consensus_height) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            payload.takeout_id,
            payload.user_id,
            payload.owner_node_id,
            payload.status,
            payload.expires_at,
            height_to_db(payload.consensus_height)
        ]
    ).map_err(|e| {
        tracing::error!("Failed to insert takeout record: {:?}", e);
        DatabaseError::classified(&e, DatabaseError::InsertError)
    })?;
    tracing::debug!("Takeout record inserted successfully");

    if execute {
        tracing::info!(
            "Node {:?} processed takeout {} for user {} (owner: node {})",
            current_node_id,
            payload.takeout_id,
            payload.user_id,
            payload.owner_node_id
        );
    } else {
        tracing::debug!("Validation phase completed for takeout creation");
    }

    Ok(())
}

/// Check if there are active takeouts using an existing transaction
/// If user_id is provided, checks only for that user; otherwise checks all users
pub fn has_active_takeout_tx(
    tx: &rusqlite::Transaction,
    user_id: Option<i32>,
) -> Result<bool, DatabaseError> {
    let count: i32 = match user_id {
        Some(uid) => {
            tx.query_row(
                "SELECT COUNT(*) FROM takeouts WHERE user_id = ? AND expires_at > CURRENT_TIMESTAMP AND status IN (0, 1, 2)",
                params![uid],
                |row| row.get(0)
            ).map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))?
        },
        None => {
            tx.query_row(
                "SELECT COUNT(*) FROM takeouts WHERE expires_at > CURRENT_TIMESTAMP AND status IN (0, 1, 2)",
                [],
                |row| row.get(0)
            ).map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))?
        }
    };

    Ok(count > 0)
}

/// Check if there are active takeouts (not expired)
/// If user_id is provided, checks only for that user; otherwise checks all users
pub fn has_active_takeout(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    user_id: Option<i32>,
) -> Result<bool, DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock
                .transaction()
                .map_err(|_| DatabaseError::LockError)?;
            has_active_takeout_tx(&tx, user_id)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get a specific takeout by ID
pub fn get_takeout_by_id(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    takeout_id: &CustomUUID,
) -> Result<Option<TakeoutPayload>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let result = db_lock.query_row(
                "SELECT id, user_id, owner_node_id, status, expires_at, consensus_height
                 FROM takeouts WHERE id = ?",
                params![takeout_id],
                |row| {
                    Ok(TakeoutPayload {
                        takeout_id: row.get(0)?,
                        user_id: row.get(1)?,
                        owner_node_id: row.get(2)?,
                        status: row.get(3)?,
                        expires_at: row.get(4)?,
                        consensus_height: row.get::<_, i64>(5).map(height_from_db)?,
                    })
                },
            );

            match result {
                Ok(takeout) => Ok(Some(takeout)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(DatabaseError::classified(&e, DatabaseError::RecallError)),
            }
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get all takeouts for a user (including expired/cancelled for history)
pub fn get_takeouts_by_user(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    user_id: i32,
) -> Result<Vec<TakeoutRecord>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT id, user_id, owner_node_id, status, expires_at, consensus_height FROM takeouts
                 WHERE user_id = ?
                 ORDER BY id DESC"  // UUIDv7 ordering gives us newest first
            ).map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))?;

            let takeout_iter = stmt
                .query_map(params![user_id], |row| {
                    let id: CustomUUID = row.get(0)?;

                    // Extract creation timestamp from UUIDv7
                    let created_at = match id.get_timestamp() {
                        Some(ts) => {
                            let unix_ts = ts.to_unix();
                            DateTime::from_timestamp(unix_ts.0 as i64, unix_ts.1)
                                .unwrap_or_else(Utc::now) // Fallback to current time if parsing fails
                        }
                        None => Utc::now(), // Fallback for non-v7 UUIDs
                    };

                    let expires_at_custom: CustomDateTime = row.get(4)?;
                    Ok(TakeoutRecord {
                        id: id.to_string(),
                        user_id: row.get(1)?,
                        owner_node_id: row.get(2)?,
                        status: row.get(3)?,
                        created_at,
                        expires_at: *expires_at_custom,
                        consensus_height: row.get::<_, i64>(5).map(height_from_db)?,
                    })
                })
                .map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))?;

            let mut takeouts = Vec::new();
            for takeout_result in takeout_iter {
                takeouts.push(
                    takeout_result
                        .map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))?,
                );
            }

            Ok(takeouts)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Apply a takeout status update on the shared block transaction (consensus
/// transaction processing). Pure DB half — no host state; handler context
/// manages commit/rollback.
///
/// Returns `Some(owner_node_id)` when the update EXECUTED a transition to a
/// terminal state (Expired/Cancelled) — the handler schedules
/// `"takeout.cleanup"` via `ctx.work` if this node is the owner (RFC-015
/// Stage D5a). `None` otherwise.
pub fn apply_takeout_status_update(
    db_tx: &rusqlite::Transaction,
    payload: &TakeoutStatusPayload,
    execute: bool,
) -> Result<Option<i32>, DatabaseError> {
    tracing::debug!(
        "Processing takeout status update for {}: {:?} (execute={})",
        payload.takeout_id,
        payload.new_status,
        execute
    );

    // Verify the takeout exists (validation for all nodes)
    let exists: bool = db_tx
        .query_row(
            "SELECT COUNT(*) > 0 FROM takeouts WHERE id = ?",
            params![payload.takeout_id],
            |row| row.get(0),
        )
        .map_err(|e| {
            tracing::error!("Failed to check takeout existence: {:?}", e);
            DatabaseError::classified(&e, DatabaseError::RecallError)
        })?;

    if !exists {
        tracing::debug!("Takeout {} does not exist", payload.takeout_id);
        return Err(DatabaseError::RecallError);
    }

    // Update the takeout status (all nodes do this)
    db_tx
        .execute(
            "UPDATE takeouts SET status = ? WHERE id = ?",
            params![payload.new_status, payload.takeout_id],
        )
        .map_err(|e| {
            tracing::error!("Failed to update takeout status: {:?}", e);
            DatabaseError::classified(&e, DatabaseError::ProcessingError)
        })?;

    // Only surface the cleanup trigger during execution phase
    if execute {
        tracing::info!(
            "Updated takeout {} status to {:?}",
            payload.takeout_id,
            payload.new_status
        );

        // If status changed to a terminal state (Expired or Cancelled), the
        // owner node should trigger local cleanup — report the owner so the
        // handler can schedule it.
        if matches!(
            payload.new_status,
            TakeoutStatus::Expired | TakeoutStatus::Cancelled
        ) {
            // Get takeout owner using same transaction
            let owner_node_id: i32 = db_tx
                .query_row(
                    "SELECT owner_node_id FROM takeouts WHERE id = ?",
                    params![payload.takeout_id],
                    |row| row.get(0),
                )
                .map_err(|e| {
                    tracing::error!("Failed to get takeout owner for cleanup: {:?}", e);
                    DatabaseError::classified(&e, DatabaseError::RecallError)
                })?;

            return Ok(Some(owner_node_id));
        }
    } else {
        tracing::debug!("Status update validation phase completed");
    }

    Ok(None)
}

/// Read the source username for the manifest header. Narrow READ over the
/// host-owned `users` table (precedent: hopnet-drive's `db::users` helpers —
/// the boundary is code ownership, not SQL).
pub(crate) fn get_username(
    conn: &rusqlite::Connection,
    user_id: i32,
) -> Result<Option<String>, DatabaseError> {
    use rusqlite::OptionalExtension;
    conn.query_row(
        "SELECT username FROM users WHERE user_id = ?",
        params![user_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))
}

/// Clean up files associated with an expired or cancelled takeout
/// This removes both the archive file and any staging directories
pub async fn cleanup_expired_takeout_files(
    takeout_id: &CustomUUID,
    fragments_dir: &str,
) -> Result<(), std::io::Error> {
    tracing::info!("Starting cleanup for takeout {}", takeout_id);

    let mut cleaned_items = 0;
    let mut failed_items = 0;

    // Clean up archive file if it exists
    let archive_path = format!("{}/takeouts/{}.tar.gz", fragments_dir, takeout_id.simple());
    if tokio::fs::metadata(&archive_path).await.is_ok() {
        match tokio::fs::remove_file(&archive_path).await {
            Ok(_) => {
                tracing::debug!("Removed archive file: {}", archive_path);
                cleaned_items += 1;
            }
            Err(e) => {
                tracing::warn!("Failed to remove archive file {}: {:?}", archive_path, e);
                failed_items += 1;
            }
        }
    }

    // Clean up staging directory if it exists
    let staging_path = format!("{}/takeouts/{}", fragments_dir, takeout_id.simple());
    if tokio::fs::metadata(&staging_path).await.is_ok() {
        match tokio::fs::remove_dir_all(&staging_path).await {
            Ok(_) => {
                tracing::debug!("Removed staging directory: {}", staging_path);
                cleaned_items += 1;
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to remove staging directory {}: {:?}",
                    staging_path,
                    e
                );
                failed_items += 1;
            }
        }
    }

    if cleaned_items > 0 || failed_items > 0 {
        tracing::info!(
            "Takeout {} cleanup completed: {} items cleaned, {} failures",
            takeout_id,
            cleaned_items,
            failed_items
        );
    } else {
        tracing::debug!("No files found to clean up for takeout {}", takeout_id);
    }

    // Return success even if some cleanups failed - this is best effort
    Ok(())
}

/// Clean up the work table associated with a takeout
/// This drops the entries table created during materialization
pub fn cleanup_takeout_table(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    takeout_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let temp_table_name = entries::table_name(takeout_id);

            db_lock
                .execute(&format!("DROP TABLE IF EXISTS {}", temp_table_name), [])
                .map_err(|e| {
                    tracing::error!("Failed to drop table {}: {:?}", temp_table_name, e);
                    DatabaseError::classified(&e, DatabaseError::ProcessingError)
                })?;

            tracing::debug!("Dropped takeout table: {}", temp_table_name);
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get takeouts that are past their expiry time but not marked as Expired or Cancelled
/// This finds takeouts network-wide (not just this node's) that need status updates
pub fn get_expired_takeouts_needing_status_update(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<Vec<CustomUUID>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock
                .prepare(
                    "SELECT id FROM takeouts
                 WHERE expires_at < CURRENT_TIMESTAMP
                 AND status NOT IN (3, 4)
                 ORDER BY expires_at ASC",
                )
                .map_err(|e| {
                    tracing::error!("Failed to prepare expired takeouts query: {:?}", e);
                    DatabaseError::classified(&e, DatabaseError::ProcessingError)
                })?;

            let takeout_rows = stmt
                .query_map([], |row| row.get::<_, CustomUUID>(0))
                .map_err(|e| {
                    tracing::error!("Failed to execute expired takeouts query: {:?}", e);
                    DatabaseError::classified(&e, DatabaseError::ProcessingError)
                })?;

            let mut expired_takeouts = Vec::new();
            for takeout_result in takeout_rows {
                match takeout_result {
                    Ok(takeout_id) => expired_takeouts.push(takeout_id),
                    Err(e) => {
                        tracing::error!("Failed to process expired takeout row: {:?}", e);
                        // Continue with other rows
                    }
                }
            }

            tracing::debug!(
                "Found {} expired takeouts needing status update",
                expired_takeouts.len()
            );
            Ok(expired_takeouts)
        }
        Err(e) => {
            tracing::error!(
                "Failed to acquire database connection for expired takeouts query: {:?}",
                e
            );
            Err(DatabaseError::LockError)
        }
    }
}
