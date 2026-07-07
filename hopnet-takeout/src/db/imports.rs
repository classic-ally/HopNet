//! Import-side DB operations. Moved verbatim from the host's `db::imports`
//! at RFC-015 Stage D5b (the network-storage aggregation stayed host-side —
//! it needs the host's validator/metrics machinery and reaches core through
//! `TakeoutHooks::available_storage_bytes`).

use hopnet_common::{CustomUUID, ImportRecord, ImportStatus};
use hopnet_projection::DatabaseError;
use rusqlite::params;
use serde::{Deserialize, Serialize};

/// Unified payload for import operations (creation, sync).
/// Field order is the bincode wire shape — do not reorder.
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
/// NOTE (RFC-015 residual coupling): the emptiness probe reads the DRIVE's
/// `inodes` table — moved verbatim to keep consensus validation
/// byte-identical. Generalizing "user tree is empty" across projections is
/// deferred; a mesh without the drive projection would fail this read.
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
