//! Per-import path table — owner-node-local progress and reporting.
//!
//! One table is created per import (`import_paths_{import_id_simple}`), seeded
//! during extraction from the manifest. The creation walk drives each Pending
//! row through the matching projection's `import_entry`; the terminal sweep
//! drops the table after final cleanup.
//!
//! RFC-015 D5 decision 7: the table gains a `projection` column (the PK is
//! now `(projection, path)`) so one import spans every manifest section, and
//! a `metadata` column persisting the entry's sidecar payload so the
//! creation walk (including a post-restart resume, which has no manifest in
//! hand) can hand the FULL entry to `import_entry`.
//!
//! Naming uses `CustomUUID::simple()` (hex only) → SQL-injection-safe.

use chrono::Utc;
use hopnet_common::{CustomUUID, ImportPathCounts, ImportPathRow, ImportPathStatus, InodeType};
use rusqlite::{params, Connection, Transaction};

use hopnet_projection::DatabaseError;

/// Build the per-import table name. Hex-only output keeps formatted SQL safe.
pub fn table_name(import_id: &CustomUUID) -> String {
    format!("import_paths_{}", import_id.simple())
}

/// Create the per-import path table if it does not exist. Survives owner
/// restart (deliberately not TEMPORARY) for resume.
pub fn create_import_paths_table(
    conn: &Connection,
    import_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    let table = table_name(import_id);
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {} (
            projection            TEXT NOT NULL,
            path                  TEXT NOT NULL,
            type                  INTEGER NOT NULL CHECK(type IN (0, 1)),
            size_bytes            INTEGER,
            source_data_block_id  TEXT,
            metadata              TEXT,
            status                INTEGER NOT NULL DEFAULT 0 CHECK(status IN (0, 1, 2, 3)),
            error_code            TEXT,
            error_message         TEXT,
            processed_at          TEXT,
            PRIMARY KEY (projection, path)
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
/// (e.g. on restart) is idempotent.
#[allow(clippy::too_many_arguments)]
pub fn insert_path_pending(
    tx: &Transaction,
    import_id: &CustomUUID,
    projection: &str,
    path: &str,
    path_type: &InodeType,
    size_bytes: Option<u64>,
    source_data_block_id: Option<&CustomUUID>,
    metadata: Option<&str>,
) -> Result<(), DatabaseError> {
    let table = table_name(import_id);
    let size = size_bytes.map(|v| v as i64);
    tx.execute(
        &format!(
            "INSERT OR IGNORE INTO {} (projection, path, type, size_bytes, source_data_block_id, metadata, status)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            table
        ),
        params![
            projection,
            path,
            path_type,
            size,
            source_data_block_id,
            metadata,
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
    projection: &str,
    path: &str,
) -> Result<(), DatabaseError> {
    let table = table_name(import_id);
    let now = Utc::now().to_rfc3339();
    let rows = tx
        .execute(
            &format!(
                "UPDATE {} SET status = ?, error_code = NULL, error_message = NULL, processed_at = ?
                 WHERE projection = ? AND path = ?",
                table
            ),
            params![ImportPathStatus::Imported, now, projection, path],
        )
        .map_err(|e| {
            tracing::error!("mark_path_imported {} failed: {:?}", table, e);
            DatabaseError::ProcessingError
        })?;
    if rows == 0 {
        tracing::warn!(
            "mark_path_imported: no row matched {}/{} in {}",
            projection,
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
    projection: &str,
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
                 WHERE projection = ? AND path = ?",
                table
            ),
            params![
                ImportPathStatus::Failed,
                error_code,
                error_message,
                now,
                projection,
                path
            ],
        )
        .map_err(|e| {
            tracing::error!("mark_path_failed {} failed: {:?}", table, e);
            DatabaseError::ProcessingError
        })?;
    if rows == 0 {
        tracing::warn!(
            "mark_path_failed: no row matched {}/{} in {}",
            projection,
            path,
            table
        );
    }
    Ok(())
}

/// Mark every still-Pending row of a projection section Skipped with a
/// structured code (`"no_translator"` — the skip-unknown-sections contract).
/// Returns the number of rows marked.
pub fn mark_projection_skipped(
    tx: &Transaction,
    import_id: &CustomUUID,
    projection: &str,
    error_code: &str,
) -> Result<usize, DatabaseError> {
    let table = table_name(import_id);
    let now = Utc::now().to_rfc3339();
    let rows = tx
        .execute(
            &format!(
                "UPDATE {} SET status = ?, error_code = ?, processed_at = ?
                 WHERE projection = ? AND status = ?",
                table
            ),
            params![
                ImportPathStatus::Skipped,
                error_code,
                now,
                projection,
                ImportPathStatus::Pending
            ],
        )
        .map_err(|e| {
            tracing::error!("mark_projection_skipped {} failed: {:?}", table, e);
            DatabaseError::ProcessingError
        })?;
    Ok(rows)
}

/// Distinct projection sections present in the table, ordered by name —
/// the creation walk's section order (deterministic).
pub fn list_projections(
    conn: &Connection,
    import_id: &CustomUUID,
) -> Result<Vec<String>, DatabaseError> {
    let table = table_name(import_id);
    let mut stmt = conn
        .prepare(&format!(
            "SELECT DISTINCT projection FROM {} ORDER BY projection ASC",
            table
        ))
        .map_err(|_| DatabaseError::RecallError)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|_| DatabaseError::RecallError)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|_| DatabaseError::RecallError)?);
    }
    Ok(out)
}

/// One Pending row hydrated for the creation walk.
pub struct PendingPath {
    pub path: String,
    pub size_bytes: Option<u64>,
    pub source_data_block_id: Option<CustomUUID>,
    pub metadata: Option<String>,
}

/// Read Pending rows of one projection filtered by type. Folders ordered by
/// depth (slash count) then path so parents commit before children; files
/// ordered by path.
pub fn read_pending_paths(
    conn: &Connection,
    import_id: &CustomUUID,
    projection: &str,
    path_type: InodeType,
) -> Result<Vec<PendingPath>, DatabaseError> {
    let table = table_name(import_id);
    let type_value = match path_type {
        InodeType::File => 0,
        InodeType::Folder => 1,
    };
    let query = match path_type {
        InodeType::Folder => format!(
            "SELECT path, size_bytes, source_data_block_id, metadata FROM {} WHERE projection = ? AND type = ? AND status = 0
             ORDER BY (length(path) - length(replace(path, '/', ''))) ASC, path ASC",
            table
        ),
        InodeType::File => format!(
            "SELECT path, size_bytes, source_data_block_id, metadata FROM {} WHERE projection = ? AND type = ? AND status = 0 ORDER BY path ASC",
            table
        ),
    };
    let mut stmt = conn
        .prepare(&query)
        .map_err(|_| DatabaseError::RecallError)?;
    let rows = stmt
        .query_map(params![projection, type_value], |row| {
            let size: Option<i64> = row.get(1)?;
            Ok(PendingPath {
                path: row.get(0)?,
                size_bytes: size.map(|v| v as u64),
                source_data_block_id: row.get(2)?,
                metadata: row.get(3)?,
            })
        })
        .map_err(|_| DatabaseError::RecallError)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|_| DatabaseError::RecallError)?);
    }
    Ok(out)
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

/// Read all rows. Caller decides ordering at the API boundary. Row shape
/// (`ImportPathRow`) is unchanged from v1 — `projection`/`metadata` are
/// internal columns not exposed on the debug route.
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
