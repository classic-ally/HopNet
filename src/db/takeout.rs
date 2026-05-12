use super::*;
use crate::db::{CustomDateTime, CustomUUID, consensus};
use chrono::{DateTime, Duration, Utc};
use hopnet_common::{InodeType, TakeoutRecord, TakeoutStatus};
use rusqlite::{
    OptionalExtension, ToSql, params,
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};

/// Status of file/folder materialization in takeout process
#[derive(Debug, Clone, PartialEq)]
pub enum MaterializationStatus {
    Pending,
    Success,
    Failed,
}

impl std::fmt::Display for MaterializationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MaterializationStatus::Pending => write!(f, "pending"),
            MaterializationStatus::Success => write!(f, "success"),
            MaterializationStatus::Failed => write!(f, "failed"),
        }
    }
}

impl ToSql for MaterializationStatus {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        let val = match self {
            MaterializationStatus::Pending => 0i32,
            MaterializationStatus::Success => 1i32,
            MaterializationStatus::Failed => 2i32,
        };
        Ok(val.into())
    }
}

impl FromSql for MaterializationStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(MaterializationStatus::Pending),
            1 => Ok(MaterializationStatus::Success),
            2 => Ok(MaterializationStatus::Failed),
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

/// Unified payload for takeout operations (creation, updates, sync)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TakeoutPayload {
    pub takeout_id: CustomUUID,
    pub user_id: i32,
    pub owner_node_id: i32,
    pub status: TakeoutStatus,
    pub expires_at: CustomDateTime,
    pub consensus_height: i32,
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

/// Process a takeout creation using a shared transaction (for consensus transaction processing)
/// - Owner node: Creates temporary inode snapshot table
/// - Other nodes: Just records the takeout
/// - execute flag: Handler context manages commit/rollback
pub fn process_takeout_creation(
    state: &crate::AppState,
    payload: &TakeoutPayload,
    current_node_id: i32,
    execute: bool,
    db_tx: &rusqlite::Transaction,
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
            payload.consensus_height
        ]
    ).map_err(|e| {
        tracing::error!("Failed to insert takeout record: {:?}", e);
        DatabaseError::InsertError
    })?;
    tracing::debug!("Takeout record inserted successfully");

    // Only the owner node creates the temporary inode snapshot
    if current_node_id == payload.owner_node_id {
        let temp_table_name = format!("takeout_inodes_{}", payload.takeout_id.simple());
        tracing::debug!("Owner node creating temporary table: {}", temp_table_name);

        db_tx
            .execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {} (
                id TEXT NOT NULL,
                path TEXT NOT NULL,
                type INTEGER NOT NULL CHECK(type IN (0, 1)),
                data_id TEXT,
                materialization_status INTEGER DEFAULT 0 CHECK(materialization_status IN (0, 1, 2)),
                error_message TEXT
            )",
                temp_table_name
            ))
            .map_err(|e| {
                tracing::error!("Failed to create table {}: {:?}", temp_table_name, e);
                DatabaseError::InsertError
            })?;

        // Populate with user's current inodes
        let insert_query = format!(
            "INSERT INTO {} (id, path, type, data_id, materialization_status)
             SELECT id, path, type, data_id, 0
             FROM inodes
             WHERE owner_id = ?",
            temp_table_name
        );

        db_tx
            .execute(&insert_query, params![payload.user_id])
            .map_err(|e| {
                tracing::error!("Failed to populate temporary table with inodes: {:?}", e);
                DatabaseError::InsertError
            })?;

        tracing::debug!("Owner node populated temporary table with user inodes");
    }

    // Only trigger async materialization during execution phase
    if execute && current_node_id == payload.owner_node_id {
        let state_clone = state.clone();
        let takeout_id = payload.takeout_id.clone();
        let user_id = payload.user_id;

        tracing::info!(
            "Owner node will trigger materialization for takeout {} after transaction commit",
            takeout_id
        );

        // Spawn async task that doesn't block consensus
        tokio::spawn(async move {
            if let Err(e) = crate::takeout::routes::execute_takeout_materialization(
                &state_clone,
                &takeout_id,
                user_id,
            )
            .await
            {
                tracing::error!(
                    "Failed to trigger materialization for takeout {}: {:?}",
                    takeout_id,
                    e
                );
                // Don't panic - fallback job will catch this later
            }
        });
    }

    if execute {
        tracing::info!(
            "Node {} processed takeout {} for user {} (owner: node {})",
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
            ).map_err(|_| DatabaseError::RecallError)?
        },
        None => {
            tx.query_row(
                "SELECT COUNT(*) FROM takeouts WHERE expires_at > CURRENT_TIMESTAMP AND status IN (0, 1, 2)",
                [],
                |row| row.get(0)
            ).map_err(|_| DatabaseError::RecallError)?
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
                        consensus_height: row.get(5)?,
                    })
                },
            );

            match result {
                Ok(takeout) => Ok(Some(takeout)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(_) => Err(DatabaseError::RecallError),
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
            ).map_err(|_| DatabaseError::RecallError)?;

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
                        consensus_height: row.get(5)?,
                    })
                })
                .map_err(|_| DatabaseError::RecallError)?;

            let mut takeouts = Vec::new();
            for takeout_result in takeout_iter {
                takeouts.push(takeout_result.map_err(|_| DatabaseError::RecallError)?);
            }

            Ok(takeouts)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

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

/// Materialize all folder structure for a takeout
/// Creates directories in staging area and updates materialization status
pub fn materialize_folders(
    conn: &mut rusqlite::Connection,
    takeout_id: &CustomUUID,
    fragments_dir: &str,
    siv_key: &aes_siv::Key<aes_siv::siv::Aes256Siv>,
    siv_nonce: &aes_siv::Nonce,
) -> Result<(u32, u32), DatabaseError> {
    let temp_table_name = format!("takeout_inodes_{}", takeout_id.simple());
    tracing::info!("Starting folder materialization for takeout {}", takeout_id);

    // Create staging directory structure
    let staging_dir = format!(
        "{}/takeouts/{}/staging/files",
        fragments_dir,
        takeout_id.simple()
    );
    std::fs::create_dir_all(&staging_dir).map_err(|e| {
        tracing::error!(
            "Failed to create staging directory {}: {:?}",
            staging_dir,
            e
        );
        DatabaseError::ProcessingError
    })?;

    let tx = conn.transaction().map_err(|_| DatabaseError::LockError)?;

    // Query all folders ordered by path depth (parents before children)
    let query = format!(
        "SELECT id, path FROM {}
         WHERE type = 1 AND materialization_status = 0
         ORDER BY LENGTH(path) - LENGTH(REPLACE(path, '/', ''))",
        temp_table_name
    );

    let folders: Vec<(CustomUUID, String)> = {
        let mut stmt = tx.prepare(&query).map_err(|e| {
            tracing::error!("Failed to prepare folder query: {:?}", e);
            DatabaseError::RecallError
        })?;
        stmt.query_map([], |row| {
            let id: CustomUUID = row.get(0)?;
            let encrypted_path: String = row.get(1)?;
            Ok((id, encrypted_path))
        })
        .map_err(|e| {
            tracing::error!("Failed to execute folder query: {:?}", e);
            DatabaseError::RecallError
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| DatabaseError::RecallError)?
    };

    let mut materialized_count = 0;
    let mut failed_count = 0;

    for (folder_id, encrypted_path) in folders {
        // Decrypt the path segments
        let decrypted_path =
            match crate::files::functions::decrypt_path(encrypted_path.clone(), siv_key, siv_nonce)
            {
                Ok(path) => path,
                Err(e) => {
                    tracing::error!("Failed to decrypt path {}: {:?}", encrypted_path, e);

                    // Mark folder as failed
                    let update_query = format!(
                        "UPDATE {} SET materialization_status = 2, error_message = ? WHERE id = ?",
                        temp_table_name
                    );
                    let _ = tx.execute(&update_query, params!["Path decryption failed", folder_id]);
                    failed_count += 1;
                    continue;
                }
            };

        // Create directory in staging area
        let full_staging_path =
            format!("{}/{}", staging_dir, decrypted_path.trim_start_matches('/'));
        match std::fs::create_dir_all(&full_staging_path) {
            Ok(_) => {
                tracing::debug!("Created directory: {}", full_staging_path);

                // Mark folder as successfully materialized
                let update_query = format!(
                    "UPDATE {} SET materialization_status = 1 WHERE id = ?",
                    temp_table_name
                );
                if let Err(e) = tx.execute(&update_query, params![folder_id]) {
                    tracing::error!("Failed to update folder status: {:?}", e);
                    failed_count += 1;
                } else {
                    materialized_count += 1;
                }
            }
            Err(e) => {
                tracing::error!("Failed to create directory {}: {:?}", full_staging_path, e);

                // Mark folder as failed
                let update_query = format!(
                    "UPDATE {} SET materialization_status = 2, error_message = ? WHERE id = ?",
                    temp_table_name
                );
                let error_msg = format!("Directory creation failed: {}", e);
                let _ = tx.execute(&update_query, params![error_msg, folder_id]);
                failed_count += 1;
            }
        }
    }

    crate::db::shared::commit_timed(tx).map_err(|e| {
        tracing::error!("Failed to commit folder materialization: {:?}", e);
        DatabaseError::ProcessingError
    })?;

    tracing::info!(
        "Folder materialization completed: {} succeeded, {} failed",
        materialized_count,
        failed_count
    );

    Ok((materialized_count, failed_count))
}

/// Process a takeout status update using a shared transaction (for consensus transaction processing)
/// - Handler context manages commit/rollback
pub fn process_takeout_status_update(
    state: &crate::AppState,
    payload: &TakeoutStatusPayload,
    execute: bool,
    db_tx: &rusqlite::Transaction,
) -> Result<(), DatabaseError> {
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
            DatabaseError::RecallError
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
            DatabaseError::ProcessingError
        })?;

    // Only trigger cleanup during execution phase
    if execute {
        tracing::info!(
            "Updated takeout {} status to {:?}",
            payload.takeout_id,
            payload.new_status
        );

        // If status changed to a terminal state (Expired or Cancelled), trigger local cleanup immediately
        if matches!(
            payload.new_status,
            hopnet_common::TakeoutStatus::Expired | hopnet_common::TakeoutStatus::Cancelled
        ) {
            // Get current node ID to check ownership
            let current_node_id = match state.get_node_id() {
                Ok(id) => id,
                Err(_) => {
                    tracing::warn!("Node ID not initialized, skipping cleanup trigger");
                    return Ok(()); // Continue, don't fail consensus
                }
            };

            // Get takeout owner using same transaction
            let owner_node_id: i32 = db_tx
                .query_row(
                    "SELECT owner_node_id FROM takeouts WHERE id = ?",
                    params![payload.takeout_id],
                    |row| row.get(0),
                )
                .map_err(|e| {
                    tracing::error!("Failed to get takeout owner for cleanup: {:?}", e);
                    DatabaseError::RecallError
                })?;

            // Only trigger cleanup if this node owns the takeout
            if current_node_id == owner_node_id {
                let takeout_id = payload.takeout_id.clone();
                let fragments_dir = state.fragments_dir.clone();
                let db_pool = state.db_pool.clone();

                tracing::info!(
                    "Owner node will trigger cleanup for takeout {} (status: {:?})",
                    takeout_id,
                    payload.new_status
                );

                // Spawn async task that doesn't block consensus
                tokio::spawn(async move {
                    if let Err(e) = cleanup_expired_takeout_files(&takeout_id, &fragments_dir).await
                    {
                        tracing::error!("Failed to clean up takeout files {}: {:?}", takeout_id, e);
                    }
                    if let Err(e) = cleanup_takeout_table(db_pool.get(), &takeout_id) {
                        tracing::error!("Failed to clean up takeout table {}: {:?}", takeout_id, e);
                    }
                });
            } else {
                tracing::debug!(
                    "Non-owner node ignoring cleanup for takeout {} owned by node {}",
                    payload.takeout_id,
                    owner_node_id
                );
            }
        }
    } else {
        tracing::debug!("Status update validation phase completed");
    }

    Ok(())
}

/// Warn above this count: in-memory list grows large, consider paginating.
const PENDING_FILES_WARN_THRESHOLD: usize = 50_000;

/// All pending files for a takeout in one query, ordered by path.
/// JOIN against `data_blocks` surfaces `file_hash` for integrity check.
pub fn list_pending_files(
    conn: &rusqlite::Connection,
    takeout_id: &CustomUUID,
) -> Result<Vec<(CustomUUID, String, CustomUUID, crate::types::Blake3Hash)>, DatabaseError> {
    let temp_table_name = format!("takeout_inodes_{}", takeout_id.simple());

    let query = format!(
        "SELECT t.id, t.path, t.data_id, d.file_hash
         FROM {} t
         JOIN data_blocks d ON t.data_id = d.id
         WHERE t.type = 0 AND t.materialization_status = 0 AND t.data_id IS NOT NULL
         ORDER BY t.path",
        temp_table_name
    );

    let mut stmt = conn.prepare(&query).map_err(|e| {
        tracing::error!("Failed to prepare pending files query: {:?}", e);
        DatabaseError::RecallError
    })?;

    let file_iter = stmt
        .query_map([], |row| {
            let id: CustomUUID = row.get(0)?;
            let encrypted_path: String = row.get(1)?;
            let data_id: CustomUUID = row.get(2)?;
            let file_hash: crate::types::Blake3Hash = row.get(3)?;
            Ok((id, encrypted_path, data_id, file_hash))
        })
        .map_err(|e| {
            tracing::error!("Failed to execute pending files query: {:?}", e);
            DatabaseError::RecallError
        })?;

    let mut files = Vec::new();
    for file_result in file_iter {
        files.push(file_result.map_err(|_| DatabaseError::RecallError)?);
    }

    if files.len() > PENDING_FILES_WARN_THRESHOLD {
        tracing::warn!(
            "Takeout {} has {} pending files (above {} threshold)",
            takeout_id,
            files.len(),
            PENDING_FILES_WARN_THRESHOLD
        );
    } else {
        tracing::debug!(
            "Listed {} pending files for takeout {}",
            files.len(),
            takeout_id
        );
    }

    Ok(files)
}

/// Update file materialization status in temporary table using an existing transaction
/// This prevents spawning additional database connections from the pool
pub fn update_file_status(
    tx: &rusqlite::Transaction,
    temp_table_name: &str,
    file_id: &CustomUUID,
    status: MaterializationStatus,
    error_message: Option<&str>,
) -> Result<(), DatabaseError> {
    if let Some(error_msg) = error_message {
        let update_query = format!(
            "UPDATE {} SET materialization_status = ?, error_message = ? WHERE id = ?",
            temp_table_name
        );
        tx.execute(&update_query, params![status, error_msg, file_id])
    } else {
        let update_query = format!(
            "UPDATE {} SET materialization_status = ? WHERE id = ?",
            temp_table_name
        );
        tx.execute(&update_query, params![status, file_id])
    }
    .map_err(|e| {
        tracing::error!("Failed to update file status: {:?}", e);
        DatabaseError::ProcessingError
    })?;

    tracing::debug!("Updated file {} status to {}", file_id, status);
    Ok(())
}

/// Materialize all files for a takeout via streaming pipeline.
///
/// Reconstruction workers (CPU + network bound) run with bounded concurrency.
/// Each worker returns its result; the coordinator applies status updates on
/// `reserved_conn` so the takeout never competes with itself for pool slots
/// on the write path. Per-file status is committed individually for durability.
pub async fn materialize_all_files(
    app_state: &crate::AppState,
    reserved_conn: &mut rusqlite::Connection,
    takeout_id: &CustomUUID,
    fragments_dir: &str,
    user_id: i32,
) -> Result<(u32, u32), DatabaseError> {
    let pending = list_pending_files(reserved_conn, takeout_id)?;
    let total = pending.len();
    if total == 0 {
        tracing::info!("No pending files to materialize for takeout {}", takeout_id);
        return Ok((0, 0));
    }

    let concurrency = crate::db::db_worker_concurrency_budget().max(1).min(total);
    tracing::info!(
        "Starting streaming materialization for takeout {}: {} files, concurrency {}",
        takeout_id,
        total,
        concurrency
    );

    let temp_table_name = format!("takeout_inodes_{}", takeout_id.simple());
    let mut set: tokio::task::JoinSet<(CustomUUID, MaterializationStatus, Option<String>)> =
        tokio::task::JoinSet::new();
    let mut iter = pending.into_iter();
    let mut total_materialized = 0u32;
    let mut total_failed = 0u32;

    // Helper to spawn a single reconstruction task
    let spawn_one =
        |set: &mut tokio::task::JoinSet<_>,
         work: (CustomUUID, String, CustomUUID, crate::types::Blake3Hash)| {
            let (file_id, encrypted_path, data_id, expected_file_hash) = work;
            let app_state = app_state.clone();
            let takeout_id = takeout_id.clone();
            let fragments_dir = fragments_dir.to_string();
            set.spawn(async move {
                crate::takeout::materialization::materialize_single_file(
                    &app_state,
                    &takeout_id,
                    file_id,
                    encrypted_path,
                    data_id,
                    expected_file_hash,
                    &fragments_dir,
                    user_id,
                )
                .await
            });
        };

    // Prime the pipeline up to the concurrency cap
    for _ in 0..concurrency {
        if let Some(work) = iter.next() {
            spawn_one(&mut set, work);
        }
    }

    // Drain results, persist on reserved_conn, replenish the pipeline
    while let Some(join_result) = set.join_next().await {
        let (file_id, status, error_msg) = match join_result {
            Ok(triple) => triple,
            Err(e) => {
                tracing::error!("Reconstruction task panicked or was cancelled: {:?}", e);
                total_failed += 1;
                if let Some(work) = iter.next() {
                    spawn_one(&mut set, work);
                }
                continue;
            }
        };

        let tx = reserved_conn.transaction().map_err(|e| {
            tracing::error!(
                "Failed to open status-update tx for file {}: {:?}",
                file_id,
                e
            );
            DatabaseError::ProcessingError
        })?;
        if let Err(e) = update_file_status(
            &tx,
            &temp_table_name,
            &file_id,
            status.clone(),
            error_msg.as_deref(),
        ) {
            tracing::error!("Failed to update status for file {}: {:?}", file_id, e);
        } else if let Err(e) = crate::db::shared::commit_timed(tx) {
            tracing::error!(
                "Failed to commit status update for file {}: {:?}",
                file_id,
                e
            );
        }

        match status {
            MaterializationStatus::Success => total_materialized += 1,
            _ => total_failed += 1,
        }

        if let Some(work) = iter.next() {
            spawn_one(&mut set, work);
        }
    }

    tracing::info!(
        "File materialization completed for takeout {}: {} succeeded, {} failed",
        takeout_id,
        total_materialized,
        total_failed
    );

    Ok((total_materialized, total_failed))
}

/// Build the takeout manifest emitted as the first tar entry of an archive.
/// Reads successfully materialized folders and files, decrypts their paths,
/// and projects per-file `file_size` + `file_hash` directly from `data_blocks`
/// so the hash formula stays consistent with `src/files/routes.rs:239`.
///
/// Acquires one pool connection and reuses it across all three queries
/// (username, folders, files). Runs after materialization completes and
/// before archive assembly — sequential section of `execute_takeout_materialization`
/// where the concurrent batch tasks have already released their conns.
pub fn build_takeout_manifest(
    conn: &rusqlite::Connection,
    takeout_id: &CustomUUID,
    user_id: i32,
    siv_key: &aes_siv::Key<aes_siv::siv::Aes256Siv>,
    siv_nonce: &aes_siv::Nonce,
) -> Result<crate::takeout::manifest::TakeoutManifest, DatabaseError> {
    use crate::takeout::manifest::{
        MANIFEST_VERSION, TakeoutManifest, TakeoutManifestFile, TakeoutManifestFolder,
    };

    let temp_table_name = format!("takeout_inodes_{}", takeout_id.simple());

    // Source username — displayed on the import side for diagnostic context.
    let source_username = match crate::db::users::get_user_by_userid_conn(conn, user_id)? {
        Some(user) => user.username,
        None => {
            tracing::error!(
                "User {} not found when building manifest for takeout {}",
                user_id,
                takeout_id
            );
            return Err(DatabaseError::RecallError);
        }
    };

    // Folders: success-status folder inodes, decrypted, sorted depth-ascending then lex.
    // Spec § Manifest Format § Ordering requires this so import creates parents first.
    let folder_query = format!(
        "SELECT path FROM {} WHERE materialization_status = ? AND type = ?",
        temp_table_name
    );
    let mut folder_stmt = conn.prepare(&folder_query).map_err(|e| {
        tracing::error!("Failed to prepare manifest folder query: {:?}", e);
        DatabaseError::ProcessingError
    })?;
    let folder_rows = folder_stmt
        .query_map(
            params![MaterializationStatus::Success, InodeType::Folder],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| {
            tracing::error!("Failed to execute manifest folder query: {:?}", e);
            DatabaseError::ProcessingError
        })?;

    let mut folders: Vec<TakeoutManifestFolder> = Vec::new();
    for row in folder_rows {
        let encrypted_path = row.map_err(|_| DatabaseError::ProcessingError)?;
        let decrypted =
            match crate::files::functions::decrypt_path(encrypted_path, siv_key, siv_nonce) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("Failed to decrypt folder path for manifest: {:?}", e);
                    continue;
                }
            };
        let trimmed = decrypted.trim_start_matches('/').to_string();
        folders.push(TakeoutManifestFolder { path: trimmed });
    }
    folders.sort_by(|a, b| {
        let depth_a = a.path.matches('/').count();
        let depth_b = b.path.matches('/').count();
        depth_a.cmp(&depth_b).then_with(|| a.path.cmp(&b.path))
    });

    // Files: success-status file inodes joined to data_blocks for size + hash + source dataid.
    let file_query = format!(
        "SELECT t.path, t.data_id, d.file_size, d.file_hash
         FROM {} t
         JOIN data_blocks d ON t.data_id = d.id
         WHERE t.materialization_status = ? AND t.type = ?
         ORDER BY t.path",
        temp_table_name
    );
    let mut file_stmt = conn.prepare(&file_query).map_err(|e| {
        tracing::error!("Failed to prepare manifest file query: {:?}", e);
        DatabaseError::ProcessingError
    })?;
    let file_rows = file_stmt
        .query_map(
            params![MaterializationStatus::Success, InodeType::File],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, CustomUUID>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, crate::types::Blake3Hash>(3)?,
                ))
            },
        )
        .map_err(|e| {
            tracing::error!("Failed to execute manifest file query: {:?}", e);
            DatabaseError::ProcessingError
        })?;

    let mut files: Vec<TakeoutManifestFile> = Vec::new();
    let mut total_bytes: u64 = 0;
    for row in file_rows {
        let (encrypted_path, data_id, file_size, file_hash) =
            row.map_err(|_| DatabaseError::ProcessingError)?;
        let decrypted =
            match crate::files::functions::decrypt_path(encrypted_path, siv_key, siv_nonce) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("Failed to decrypt file path for manifest: {:?}", e);
                    continue;
                }
            };
        let trimmed = decrypted.trim_start_matches('/').to_string();
        let size = file_size as u64;
        total_bytes = total_bytes.saturating_add(size);
        files.push(TakeoutManifestFile {
            path: trimmed,
            size,
            source_data_block_id: data_id,
            file_hash,
        });
    }

    // UUIDv7 encodes creation time — no separate DB query needed for created_at.
    let created_at = takeout_id.extract_timestamp().unwrap_or_else(Utc::now);

    Ok(TakeoutManifest {
        version: MANIFEST_VERSION,
        takeout_id: takeout_id.clone(),
        created_at,
        source_username,
        total_files: files.len() as u64,
        total_folders: folders.len() as u64,
        total_bytes,
        folders,
        files,
    })
}

/// Get list of successfully materialized files and folders for archive creation
pub fn get_materialized_entries_for_archive(
    conn: &rusqlite::Connection,
    fragments_dir: &str,
    takeout_id: &CustomUUID,
    siv_key: &aes_siv::Key<aes_siv::siv::Aes256Siv>,
    siv_nonce: &aes_siv::Nonce,
) -> Result<Vec<crate::takeout::archive::ArchiveEntry>, DatabaseError> {
    let temp_table_name = format!("takeout_inodes_{}", takeout_id.simple());

    let mut entries = Vec::new();

    // Get successfully materialized files and folders
    let query = format!(
        "SELECT path, type FROM {} WHERE materialization_status = ? ORDER BY path",
        temp_table_name
    );

    let mut stmt = conn.prepare(&query).map_err(|e| {
        tracing::error!("Failed to prepare materialized entries query: {:?}", e);
        DatabaseError::ProcessingError
    })?;

    let entry_rows = stmt
        .query_map(params![MaterializationStatus::Success], |row| {
            Ok((
                row.get::<_, String>(0)?,    // path
                row.get::<_, InodeType>(1)?, // type
            ))
        })
        .map_err(|e| {
            tracing::error!("Failed to execute materialized entries query: {:?}", e);
            DatabaseError::ProcessingError
        })?;

    // Process each entry
    for entry_result in entry_rows {
        let (encrypted_path, inode_type) = entry_result.map_err(|e| {
            tracing::error!("Failed to read entry from database: {:?}", e);
            DatabaseError::ProcessingError
        })?;

        // Decrypt the path to get the original file path
        let decrypted_path =
            match crate::files::functions::decrypt_path(encrypted_path, siv_key, siv_nonce) {
                Ok(path) => path,
                Err(e) => {
                    tracing::error!("Failed to decrypt path: {:?}", e);
                    continue; // Skip this entry but continue with others
                }
            };

        // Build staging path based on type
        let is_folder = inode_type == InodeType::Folder;
        let staging_path = if is_folder {
            format!(
                "{}/takeouts/{}/staging/folders/{}",
                fragments_dir,
                takeout_id.simple(),
                decrypted_path.trim_start_matches('/')
            )
        } else {
            format!(
                "{}/takeouts/{}/staging/files/{}",
                fragments_dir,
                takeout_id.simple(),
                decrypted_path.trim_start_matches('/')
            )
        };

        // Archive path is the decrypted path without leading slash
        let archive_path = decrypted_path.trim_start_matches('/').to_string();

        entries.push(crate::takeout::archive::ArchiveEntry {
            staging_path,
            archive_path,
            is_directory: is_folder,
        });
    }

    tracing::debug!("Found {} materialized entries for archive", entries.len());
    Ok(entries)
}

/// Clean up files associated with an expired or cancelled takeout
/// This removes both the archive file and any staging directories
async fn cleanup_expired_takeout_files(
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

/// Clean up database table associated with a takeout
/// This drops the inode snapshot table created during takeout creation
pub fn cleanup_takeout_table(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    takeout_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let temp_table_name = format!("takeout_inodes_{}", takeout_id.simple());

            db_lock
                .execute(&format!("DROP TABLE IF EXISTS {}", temp_table_name), [])
                .map_err(|e| {
                    tracing::error!("Failed to drop table {}: {:?}", temp_table_name, e);
                    DatabaseError::ProcessingError
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
                    DatabaseError::ProcessingError
                })?;

            let takeout_rows = stmt
                .query_map([], |row| Ok(row.get::<_, CustomUUID>(0)?))
                .map_err(|e| {
                    tracing::error!("Failed to execute expired takeouts query: {:?}", e);
                    DatabaseError::ProcessingError
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
