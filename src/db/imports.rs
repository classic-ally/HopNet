use crate::db::{CustomUUID, DatabaseError};
use hopnet_common::{ImportRecord, ImportStatus};
use rusqlite::params;
use serde::{Deserialize, Serialize};

/// Unified payload for import operations (creation, sync)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ImportPayload {
    pub import_id: CustomUUID,
    pub user_id: i32,
    pub owner_node_id: i32,
    pub status: ImportStatus,
}

/// Payload for import status updates (consensus-tracked)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ImportStatusPayload {
    pub import_id: CustomUUID,
    pub new_status: ImportStatus,
}

impl ImportPayload {
    /// Convert to ImportRecord for frontend API.
    /// `created_at` derives from the UUIDv7 id; mirrors `TakeoutPayload::to_record`.
    pub fn to_record(&self) -> ImportRecord {
        let created_at = self
            .import_id
            .extract_timestamp()
            .unwrap_or_else(chrono::Utc::now);
        ImportRecord {
            id: self.import_id.clone(),
            user_id: self.user_id,
            owner_node_id: self.owner_node_id,
            status: self.status.clone(),
            created_at,
        }
    }
}

/// Whether the user has an in-flight import. Distinct from `is_import_eligible`:
/// eligibility blocks Completed too (v1 forbids re-import) but Completed users
/// can still write to their tree, so the write-gate uses this narrower check.
pub fn has_active_import(conn: &rusqlite::Connection, user_id: i32) -> Result<bool, DatabaseError> {
    let count: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM imports WHERE user_id = ? AND status IN (0, 1)",
            params![user_id],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;
    Ok(count > 0)
}

/// Whether the user is eligible to start a new import.
///
/// Eligibility requires both:
///   1. No "blocking" import record — `status IN (Pending, Importing, Completed)`.
///      `Failed` is excluded (retryable). The Completed case enforces v1 scope:
///      once a user has imported, re-importing requires login→import semantics
///      that don't exist yet (spec § Phase 3 § Scope).
///   2. No existing inodes for the user. v1 requires an empty user tree because
///      merge semantics aren't designed yet — without this check, a user who
///      populated their tree via FileProvider could trigger an import that
///      collides with their existing files.
///
/// Takes `&Connection` — `&Transaction` derefs to `&Connection`, so this works
/// from both consensus handlers (passing `&db_tx`) and route handlers (passing
/// a freshly-acquired pool conn). Pure read; deterministic across consensus
/// phases — safe to call in both validation and execute.
pub fn is_import_eligible(
    conn: &rusqlite::Connection,
    user_id: i32,
) -> Result<bool, DatabaseError> {
    let blocking_imports: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM imports WHERE user_id = ? AND status IN (0, 1, 2)",
            params![user_id],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;
    if blocking_imports > 0 {
        return Ok(false);
    }

    let inode_count: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM inodes WHERE owner_id = ?",
            params![user_id],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;

    Ok(inode_count == 0)
}

/// Process an import creation through consensus.
/// Validates no blocking import exists for the user, then inserts the row.
///
/// `state` and `current_node_id` will be added back in Phase 3.3 when archive
/// extraction spawning lands on the owner node.
pub fn process_import_creation(
    payload: &ImportPayload,
    execute: bool,
    db_tx: &rusqlite::Transaction,
) -> Result<(), DatabaseError> {
    tracing::debug!(
        "Processing import creation for user_id: {} (execute={})",
        payload.user_id,
        execute
    );

    if !is_import_eligible(db_tx, payload.user_id)? {
        tracing::debug!(
            "User {} is not eligible to start an import",
            payload.user_id
        );
        return Err(DatabaseError::ConflictError);
    }

    db_tx
        .execute(
            "INSERT INTO imports (id, user_id, owner_node_id, status) VALUES (?, ?, ?, ?)",
            params![
                payload.import_id,
                payload.user_id,
                payload.owner_node_id,
                payload.status,
            ],
        )
        .map_err(|e| {
            tracing::error!("Failed to insert import record: {:?}", e);
            DatabaseError::InsertError
        })?;

    if execute {
        tracing::info!(
            "Import {} created for user {} (owner node {})",
            payload.import_id,
            payload.user_id,
            payload.owner_node_id
        );
    } else {
        tracing::debug!("Validation phase completed for import creation");
    }

    Ok(())
}

/// Process an import status update through consensus.
/// Mirrors `apply_takeout_status_update`. Pure DB write on `db_tx` state,
/// deterministic across phases.
pub fn process_import_status_update(
    payload: &ImportStatusPayload,
    execute: bool,
    db_tx: &rusqlite::Transaction,
) -> Result<(), DatabaseError> {
    tracing::debug!(
        "Processing import status update for {} -> {:?} (execute={})",
        payload.import_id,
        payload.new_status,
        execute
    );

    let rows = db_tx
        .execute(
            "UPDATE imports SET status = ? WHERE id = ?",
            params![payload.new_status, payload.import_id],
        )
        .map_err(|e| {
            tracing::error!("Failed to update import status: {:?}", e);
            DatabaseError::ProcessingError
        })?;

    if rows == 0 {
        tracing::warn!(
            "update_import_status: no row matched id {}",
            payload.import_id
        );
        return Err(DatabaseError::RecallError);
    }

    if execute {
        tracing::info!(
            "Import {} status updated to {:?}",
            payload.import_id,
            payload.new_status
        );
    }

    Ok(())
}

/// Sum the most recently reported available storage across the active
/// validator set at `height`. Used by the import upload route to reject
/// over-quota archives before consensus submission.
///
/// Returns the **raw sum** in bytes — callers apply the RS expansion
/// multiplier (×3) and `STORAGE_SAFETY_MARGIN_BYTES` at the comparison site
/// per spec § 3.3.
///
/// Bootstrap fallback: a fresh mesh has no `metrics` rows until the cron
/// fires (~10 min). When aggregation finds zero, we fall back to the owner
/// node's filesystem capacity scaled by the validator count. Conservative
/// — assumes peers have similar capacity to self. Logs a warning so
/// operators can see the bootstrap path was hit.
pub async fn get_total_validator_storage_available(
    state: &crate::AppState,
    height: i32,
) -> Result<u64, DatabaseError> {
    let db_lock = state.db_pool.get().map_err(|_| DatabaseError::LockError)?;
    let validators = crate::db::consensus::get_validators_with_conn(&db_lock, height)?;
    if validators.is_empty() {
        return Ok(0);
    }

    let metrics_bytes: u64 = {
        let placeholders = validators.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            "WITH latest AS (
                 SELECT to_node, storage_total_gb, storage_used_gb,
                        ROW_NUMBER() OVER (PARTITION BY to_node ORDER BY start_time DESC) AS rn
                 FROM metrics
                 WHERE to_node IN ({}) AND storage_total_gb IS NOT NULL
             )
             SELECT COALESCE(SUM(storage_total_gb - storage_used_gb), 0)
             FROM latest WHERE rn = 1",
            placeholders
        );
        let params: Vec<Box<dyn rusqlite::ToSql>> = validators
            .iter()
            .map(|v| Box::new(v.node_id) as Box<dyn rusqlite::ToSql>)
            .collect();
        let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let total_gb: i64 = db_lock
            .query_row(&query, refs.as_slice(), |row| row.get(0))
            .map_err(|_| DatabaseError::RecallError)?;
        (total_gb.max(0) as u64) * 1024 * 1024 * 1024
    };
    if metrics_bytes > 0 {
        return Ok(metrics_bytes);
    }

    let validator_count = validators.len() as u64;
    drop(db_lock);
    tracing::warn!(
        "No validator storage metrics yet (height={}); bootstrapping quota from owner filesystem × {} validators",
        height,
        validator_count
    );
    match crate::metrics::routes::calculate_storage_usage(&state.fragments_dir).await {
        Ok(s) => {
            let per_node = (s.total_gb.saturating_sub(s.used_gb) as u64) * 1024 * 1024 * 1024;
            Ok(per_node.saturating_mul(validator_count))
        }
        Err(e) => {
            tracing::error!("Bootstrap fs storage calc failed: {:?}", e);
            Ok(0)
        }
    }
}

/// Returns the user's current or most-recently-created import as a singleton.
/// Active imports (Pending, Importing) take precedence over terminal records;
/// among non-active records, the most recently created (id descending —
/// UUIDv7 sorts by creation time) wins. Backs `GET /takeouts/import`.
pub fn get_current_import_for_user(
    conn: &rusqlite::Connection,
    user_id: i32,
) -> Result<Option<ImportRecord>, DatabaseError> {
    let result = conn.query_row(
        "SELECT id, user_id, owner_node_id, status
         FROM imports
         WHERE user_id = ?
         ORDER BY (CASE WHEN status IN (0, 1) THEN 0 ELSE 1 END), id DESC
         LIMIT 1",
        params![user_id],
        |row| {
            Ok(ImportPayload {
                import_id: row.get(0)?,
                user_id: row.get(1)?,
                owner_node_id: row.get(2)?,
                status: row.get(3)?,
            })
        },
    );

    match result {
        Ok(payload) => Ok(Some(payload.to_record())),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => {
            tracing::error!(
                "Failed to fetch current import for user {}: {:?}",
                user_id,
                e
            );
            Err(DatabaseError::RecallError)
        }
    }
}
