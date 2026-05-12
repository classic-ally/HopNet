//! Per-import path table — owner-node-local progress and reporting.
//!
//! One table is created per import (`import_paths_{import_id_simple}`), seeded
//! during Phase 3.4 extraction from the manifest. Phase 3.5 walks the table
//! to drive `create_folder` / `create_file_with_fragments`; Phase 3.7 sweeps
//! terminal imports to drop the table after final cleanup.
//!
//! Naming uses `CustomUUID::simple()` (hex only) → SQL-injection-safe.

use chrono::Utc;
use hopnet_common::{CustomUUID, ImportPathCounts, ImportPathRow, ImportPathStatus, InodeType};
use rusqlite::{Connection, Transaction, params};

use crate::db::DatabaseError;

/// Build the per-import table name. Hex-only output keeps formatted SQL safe.
pub fn table_name(import_id: &CustomUUID) -> String {
    format!("import_paths_{}", import_id.simple())
}

/// Create the per-import path table if it does not exist. Schema mirrors spec
/// § 3.2 lines 346-355 with the `TEMPORARY` qualifier dropped so the table
/// survives owner restart for Phase 3.7 resume.
pub fn create_import_paths_table(
    conn: &Connection,
    import_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    let table = table_name(import_id);
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {} (
            path                  TEXT NOT NULL,
            type                  INTEGER NOT NULL CHECK(type IN (0, 1)),
            size_bytes            INTEGER,
            source_data_block_id  TEXT,
            status                INTEGER NOT NULL DEFAULT 0 CHECK(status IN (0, 1, 2, 3)),
            error_code            TEXT,
            error_message         TEXT,
            processed_at          TEXT,
            PRIMARY KEY (path)
        )",
        table
    ))
    .map_err(|e| {
        tracing::error!("Failed to create {}: {:?}", table, e);
        DatabaseError::InsertError
    })?;
    Ok(())
}

/// Insert a path row at status `Pending`. Used to seed the table from manifest
/// entries before extraction reads any content. `INSERT OR IGNORE` so re-seeding
/// (e.g. on restart in 3.7) is idempotent.
pub fn insert_path_pending(
    tx: &Transaction,
    import_id: &CustomUUID,
    path: &str,
    path_type: &InodeType,
    size_bytes: Option<u64>,
    source_data_block_id: Option<&CustomUUID>,
) -> Result<(), DatabaseError> {
    let table = table_name(import_id);
    let size = size_bytes.map(|v| v as i64);
    tx.execute(
        &format!(
            "INSERT OR IGNORE INTO {} (path, type, size_bytes, source_data_block_id, status)
             VALUES (?, ?, ?, ?, ?)",
            table
        ),
        params![
            path,
            path_type,
            size,
            source_data_block_id,
            ImportPathStatus::Pending
        ],
    )
    .map_err(|e| {
        tracing::error!("insert_path_pending {} failed: {:?}", table, e);
        DatabaseError::InsertError
    })?;
    Ok(())
}

/// Mark a path row imported. Clears `error_code` / `error_message` so retries
/// don't carry stale failure metadata, and stamps `processed_at`.
pub fn mark_path_imported(
    tx: &Transaction,
    import_id: &CustomUUID,
    path: &str,
) -> Result<(), DatabaseError> {
    let table = table_name(import_id);
    let now = Utc::now().to_rfc3339();
    let rows = tx
        .execute(
            &format!(
                "UPDATE {} SET status = ?, error_code = NULL, error_message = NULL, processed_at = ?
                 WHERE path = ?",
                table
            ),
            params![ImportPathStatus::Imported, now, path],
        )
        .map_err(|e| {
            tracing::error!("mark_path_imported {} failed: {:?}", table, e);
            DatabaseError::ProcessingError
        })?;
    if rows == 0 {
        tracing::warn!(
            "mark_path_imported: no row matched path {} in {}",
            path,
            table
        );
    }
    Ok(())
}

/// Mark a path row failed with a structured error code (e.g. `"hash_mismatch"`).
/// Sets `processed_at` to current UTC.
pub fn mark_path_failed(
    tx: &Transaction,
    import_id: &CustomUUID,
    path: &str,
    error_code: &str,
    error_message: Option<&str>,
) -> Result<(), DatabaseError> {
    let table = table_name(import_id);
    let now = Utc::now().to_rfc3339();
    let rows = tx
        .execute(
            &format!(
                "UPDATE {} SET status = ?, error_code = ?, error_message = ?, processed_at = ?
                 WHERE path = ?",
                table
            ),
            params![
                ImportPathStatus::Failed,
                error_code,
                error_message,
                now,
                path
            ],
        )
        .map_err(|e| {
            tracing::error!("mark_path_failed {} failed: {:?}", table, e);
            DatabaseError::ProcessingError
        })?;
    if rows == 0 {
        tracing::warn!(
            "mark_path_failed: no row matched path {} in {}",
            path,
            table
        );
    }
    Ok(())
}

/// Aggregate counts grouped by status for a single import's per-path table.
/// Returns zeros if the table doesn't exist (handles "no extraction yet" case).
pub fn count_paths_by_status(
    conn: &Connection,
    import_id: &CustomUUID,
) -> Result<ImportPathCounts, DatabaseError> {
    let table = table_name(import_id);
    let exists: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            params![table],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;
    if exists == 0 {
        return Ok(ImportPathCounts::default());
    }

    let mut counts = ImportPathCounts::default();
    let query = format!("SELECT status, COUNT(*) FROM {} GROUP BY status", table);
    let mut stmt = conn
        .prepare(&query)
        .map_err(|_| DatabaseError::RecallError)?;
    let rows = stmt
        .query_map([], |row| {
            let status: ImportPathStatus = row.get(0)?;
            let n: i64 = row.get(1)?;
            Ok((status, n as u32))
        })
        .map_err(|_| DatabaseError::RecallError)?;
    for r in rows {
        let (status, n) = r.map_err(|_| DatabaseError::RecallError)?;
        match status {
            ImportPathStatus::Pending => counts.pending = n,
            ImportPathStatus::Imported => counts.imported = n,
            ImportPathStatus::Skipped => counts.skipped = n,
            ImportPathStatus::Failed => counts.failed = n,
        }
        counts.total += n;
    }
    Ok(counts)
}

/// Read all rows. Caller decides ordering at the API boundary.
pub fn list_paths(
    conn: &Connection,
    import_id: &CustomUUID,
) -> Result<Vec<ImportPathRow>, DatabaseError> {
    let table = table_name(import_id);
    let mut stmt = conn
        .prepare(&format!(
            "SELECT path, type, size_bytes, source_data_block_id,
                    status, error_code, error_message, processed_at
             FROM {}
             ORDER BY type ASC, path ASC",
            table
        ))
        .map_err(|e| {
            tracing::error!("list_paths prepare {} failed: {:?}", table, e);
            DatabaseError::RecallError
        })?;

    let rows = stmt
        .query_map([], |row| {
            let size_bytes: Option<i64> = row.get(2)?;
            let processed_at_str: Option<String> = row.get(7)?;
            let processed_at = processed_at_str.and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s)
                    .ok()
                    .map(|dt| dt.with_timezone(&Utc))
            });
            Ok(ImportPathRow {
                path: row.get(0)?,
                path_type: row.get(1)?,
                size_bytes: size_bytes.map(|v| v as u64),
                source_data_block_id: row.get(3)?,
                status: row.get(4)?,
                error_code: row.get(5)?,
                error_message: row.get(6)?,
                processed_at,
            })
        })
        .map_err(|e| {
            tracing::error!("list_paths query {} failed: {:?}", table, e);
            DatabaseError::RecallError
        })?;

    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|_| DatabaseError::RecallError)?);
    }
    Ok(out)
}
