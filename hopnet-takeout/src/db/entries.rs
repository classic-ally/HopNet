//! Per-takeout work table — `takeout_entries_{takeout_id_simple}` (RFC-015
//! D5 decision 7; replaces the drive-shaped `takeout_inodes_{id}`).
//!
//! Populated by the materialization task from each registered exporter's
//! `enumerate()` stream (decision 3 moved enumeration OUT of consensus
//! apply), then driven row-by-row through staging, manifest, and archive.
//! Core-owned; the `projection` column namespaces one takeout across every
//! registered projection. `export_handle`/`metadata` persist the exporter's
//! private ref + sidecar payload so `open()` and manifest emission need no
//! re-enumeration.
//!
//! Naming uses `CustomUUID::simple()` (hex only) → SQL-injection-safe.

use hopnet_common::{Blake3Hash, CustomUUID};
use hopnet_projection::{DatabaseError, ExportEntry};
use rusqlite::{
    params,
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
    Connection, ToSql, Transaction,
};

use crate::manifest::EntryKind;

/// Status of entry materialization in the takeout process
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

/// One row of the work table, hydrated for the materialization/manifest
/// passes.
pub struct EntryRow {
    pub projection: String,
    pub path: String,
    pub kind: EntryKind,
    pub blob_id: Option<CustomUUID>,
    pub size: u64,
    pub metadata: serde_json::Value,
    pub export_handle: Option<String>,
    pub manifest_hash: Option<Blake3Hash>,
}

impl EntryRow {
    /// Rebuild the exporter-facing entry for `open()`.
    pub fn to_export_entry(&self) -> ExportEntry {
        ExportEntry {
            logical_path: self.path.clone(),
            blob_id: self.blob_id.clone(),
            size: self.size,
            metadata: self.metadata.clone(),
            export_handle: self.export_handle.clone(),
        }
    }
}

fn kind_to_i32(kind: EntryKind) -> i32 {
    match kind {
        EntryKind::File => 0,
        EntryKind::Folder => 1,
    }
}

fn kind_from_i32(v: i64) -> Result<EntryKind, rusqlite::Error> {
    match v {
        0 => Ok(EntryKind::File),
        1 => Ok(EntryKind::Folder),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

/// Build the per-takeout table name. Hex-only output keeps formatted SQL safe.
pub fn table_name(takeout_id: &CustomUUID) -> String {
    format!("takeout_entries_{}", takeout_id.simple())
}

/// Create the work table if it does not exist (idempotent so a re-scheduled
/// materialization doesn't fail on the DDL).
pub fn create_entries_table(
    conn: &Connection,
    takeout_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    let table = table_name(takeout_id);
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {} (
            projection      TEXT NOT NULL,
            path            TEXT NOT NULL,
            kind            INTEGER NOT NULL CHECK(kind IN (0, 1)),
            blob_id         TEXT,
            size            INTEGER NOT NULL DEFAULT 0,
            metadata        TEXT NOT NULL DEFAULT '{{}}',
            export_handle   TEXT,
            status          INTEGER NOT NULL DEFAULT 0 CHECK(status IN (0, 1, 2)),
            error           TEXT,
            manifest_hash   BLOB,
            PRIMARY KEY (projection, path)
        )",
        table
    ))
    .map_err(|e| {
        tracing::error!("Failed to create table {}: {:?}", table, e);
        DatabaseError::classified(&e, DatabaseError::InsertError)
    })?;
    Ok(())
}

/// Insert one enumerated entry at status Pending. `INSERT OR IGNORE` so a
/// re-run of a partially-populated enumeration is idempotent.
pub fn insert_entry(
    tx: &Transaction,
    takeout_id: &CustomUUID,
    projection: &str,
    entry: &ExportEntry,
    kind: EntryKind,
) -> Result<(), DatabaseError> {
    let table = table_name(takeout_id);
    let metadata = serde_json::to_string(&entry.metadata).unwrap_or_else(|_| "null".to_string());
    tx.execute(
        &format!(
            "INSERT OR IGNORE INTO {} (projection, path, kind, blob_id, size, metadata, export_handle, status)
             VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
            table
        ),
        params![
            projection,
            entry.logical_path,
            kind_to_i32(kind),
            entry.blob_id,
            entry.size as i64,
            metadata,
            entry.export_handle,
        ],
    )
    .map_err(|e| {
        tracing::error!("insert_entry {} failed: {:?}", table, e);
        DatabaseError::classified(&e, DatabaseError::InsertError)
    })?;
    Ok(())
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> Result<EntryRow, rusqlite::Error> {
    let metadata_str: String = row.get(5)?;
    Ok(EntryRow {
        projection: row.get(0)?,
        path: row.get(1)?,
        kind: kind_from_i32(row.get::<_, i64>(2)?)?,
        blob_id: row.get(3)?,
        size: row.get::<_, i64>(4)? as u64,
        metadata: serde_json::from_str(&metadata_str).unwrap_or(serde_json::Value::Null),
        export_handle: row.get(6)?,
        manifest_hash: row.get(7)?,
    })
}

const ENTRY_COLUMNS: &str =
    "projection, path, kind, blob_id, size, metadata, export_handle, manifest_hash";

/// All Pending folder entries, ordered depth-ascending then path (parents
/// before children within and across projections).
pub fn list_pending_folders(
    conn: &Connection,
    takeout_id: &CustomUUID,
) -> Result<Vec<EntryRow>, DatabaseError> {
    let table = table_name(takeout_id);
    let query = format!(
        "SELECT {} FROM {} WHERE kind = 1 AND status = 0
         ORDER BY projection ASC,
                  (LENGTH(path) - LENGTH(REPLACE(path, '/', ''))) ASC, path ASC",
        ENTRY_COLUMNS, table
    );
    collect_rows(conn, &query, &table)
}

/// Warn above this count: in-memory list grows large, consider paginating.
const PENDING_FILES_WARN_THRESHOLD: usize = 50_000;

/// All Pending file entries with content (blob_id present), ordered by
/// projection then path. (Entries without a blob never materialize —
/// matches the pre-split `data_id IS NOT NULL` filter.)
pub fn list_pending_files(
    conn: &Connection,
    takeout_id: &CustomUUID,
) -> Result<Vec<EntryRow>, DatabaseError> {
    let table = table_name(takeout_id);
    let query = format!(
        "SELECT {} FROM {} WHERE kind = 0 AND status = 0 AND blob_id IS NOT NULL
         ORDER BY projection ASC, path ASC",
        ENTRY_COLUMNS, table
    );
    let files = collect_rows(conn, &query, &table)?;

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

/// All Success entries (folders + files), ordered folders-first
/// depth-ascending then files by path — the manifest/archive emission order.
pub fn list_success_entries(
    conn: &Connection,
    takeout_id: &CustomUUID,
) -> Result<Vec<EntryRow>, DatabaseError> {
    let table = table_name(takeout_id);
    let query = format!(
        "SELECT {} FROM {} WHERE status = 1
         ORDER BY projection ASC, kind DESC,
                  (LENGTH(path) - LENGTH(REPLACE(path, '/', ''))) ASC, path ASC",
        ENTRY_COLUMNS, table
    );
    collect_rows(conn, &query, &table)
}

fn collect_rows(
    conn: &Connection,
    query: &str,
    table: &str,
) -> Result<Vec<EntryRow>, DatabaseError> {
    let mut stmt = conn.prepare(query).map_err(|e| {
        tracing::error!("Failed to prepare entries query on {}: {:?}", table, e);
        DatabaseError::classified(&e, DatabaseError::RecallError)
    })?;
    let rows = stmt.query_map([], row_to_entry).map_err(|e| {
        tracing::error!("Failed to execute entries query on {}: {:?}", table, e);
        DatabaseError::classified(&e, DatabaseError::RecallError)
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))?);
    }
    Ok(out)
}

/// Update one entry's materialization status using an existing transaction.
/// This prevents spawning additional database connections from the pool.
pub fn update_entry_status(
    tx: &Transaction,
    takeout_id: &CustomUUID,
    projection: &str,
    path: &str,
    status: MaterializationStatus,
    error_message: Option<&str>,
    manifest_hash: Option<&Blake3Hash>,
) -> Result<(), DatabaseError> {
    let table = table_name(takeout_id);
    if let Some(error_msg) = error_message {
        let update_query = format!(
            "UPDATE {} SET status = ?, error = ? WHERE projection = ? AND path = ?",
            table
        );
        tx.execute(&update_query, params![status, error_msg, projection, path])
    } else {
        let update_query = format!(
            "UPDATE {} SET status = ?, manifest_hash = ? WHERE projection = ? AND path = ?",
            table
        );
        tx.execute(
            &update_query,
            params![status, manifest_hash, projection, path],
        )
    }
    .map_err(|e| {
        tracing::error!("Failed to update entry status: {:?}", e);
        DatabaseError::classified(&e, DatabaseError::ProcessingError)
    })?;

    tracing::debug!("Updated entry {}/{} status to {}", projection, path, status);
    Ok(())
}
