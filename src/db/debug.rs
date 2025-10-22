use super::*;
use std::collections::HashMap;
use blake3::Hasher;
use serde::{Deserialize, Serialize};
use duckdb::OptionalExt;

/// Tables to skip entirely (local-only state)
const LOCAL_ONLY_TABLES: &[&str] = &[
    "this_node",
    "modification_log",
    "pending_fragment_requests",
];

/// Columns to exclude from consensus-tracked tables
const EXCLUDED_COLUMNS: &[(&str, &[&str])] = &[
    ("fragment_hashes", &["stored_locally"]),
    ("fragment_inventory", &["self_verified_height"]),
];

/// All consensus-tracked tables (order matters for deterministic hashing)
const CONSENSUS_TABLES: &[&str] = &[
    "sequences",
    "users",
    "nodes",
    "validators",
    "blocks",
    "quorum_certificates",
    "timeout_certificates",
    "data_blocks",
    "file_access",
    "fragment_hashes",
    "inodes",
    "takeouts",
    "fragment_request_metrics",
    "metrics",
    "fragment_inventory",
];

/// Internal state snapshot with rich types (Blake3Hash)
#[derive(Debug)]
pub struct StateSnapshot {
    pub consensus_height: i32,
    pub committed_view: i32,
    pub table_hashes: HashMap<String, TableHashInfo>,
}

/// Internal table hash info with rich types (Blake3Hash)
#[derive(Debug)]
pub struct TableHashInfo {
    pub hash: Blake3Hash,
    pub row_count: usize,
    pub excluded_columns: Vec<String>,
}

/// Convert internal snapshot to wire format (String hashes)
impl From<StateSnapshot> for hopnet_common::StateSnapshot {
    fn from(internal: StateSnapshot) -> Self {
        hopnet_common::StateSnapshot {
            consensus_height: internal.consensus_height,
            committed_view: internal.committed_view,
            table_hashes: internal
                .table_hashes
                .into_iter()
                .map(|(table_name, info)| {
                    (table_name, hopnet_common::TableHashInfo {
                        hash: info.hash.to_hex(),
                        row_count: info.row_count,
                        excluded_columns: info.excluded_columns,
                    })
                })
                .collect(),
        }
    }
}

/// Get excluded columns for a table
fn get_excluded_columns(table_name: &str) -> Vec<&'static str> {
    EXCLUDED_COLUMNS
        .iter()
        .find(|(name, _)| *name == table_name)
        .map(|(_, cols)| cols.to_vec())
        .unwrap_or_default()
}

/// Get primary key columns dynamically from schema
fn get_primary_key_columns(
    tx: &duckdb::Transaction,
    table_name: &str
) -> Result<Vec<String>, DatabaseError> {
    let mut stmt = tx.prepare(&format!("PRAGMA table_info({})", table_name))
        .map_err(|_| DatabaseError::RecallError)?;

    let mut pk_columns: Vec<String> = Vec::new();

    let rows = stmt.query_map([], |row| {
        let name: String = row.get(1)?;      // Column name
        let is_pk: bool = row.get(5)?;       // Part of primary key?
        Ok((name, is_pk))
    }).map_err(|_| DatabaseError::RecallError)?;

    for row in rows {
        let (col_name, is_pk) = row.map_err(|_| DatabaseError::RecallError)?;
        if is_pk {
            pk_columns.push(col_name);
        }
    }

    if pk_columns.is_empty() {
        tracing::error!("No primary key found for table: {}", table_name);
        return Err(DatabaseError::ProcessingError);
    }

    // Sort alphabetically for deterministic order across all nodes
    pk_columns.sort();

    Ok(pk_columns)
}

/// Build SQL query for a table with appropriate exclusions
fn build_table_query(
    tx: &duckdb::Transaction,
    table_name: &str,
    excluded_cols: &[&str]
) -> Result<String, DatabaseError> {
    let pk_cols = get_primary_key_columns(tx, table_name)?;

    // Build EXCLUDE clause (empty if no exclusions)
    let exclude_clause = if !excluded_cols.is_empty() {
        format!("EXCLUDE ({})", excluded_cols.join(", "))
    } else {
        String::new()
    };

    let query = format!(
        "SELECT COALESCE(json_group_array(json_object(*)), '[]') FROM (SELECT * {} FROM {} ORDER BY {})",
        exclude_clause,
        table_name,
        pk_cols.join(", ")
    );

    Ok(query)
}

/// Compute hash for a single table within a transaction
fn compute_table_hash_tx(
    tx: &duckdb::Transaction,
    table_name: &str,
) -> Result<TableHashInfo, DatabaseError> {
    // Build query with EXCLUDE clause if needed
    let excluded_cols = get_excluded_columns(table_name);
    let query = build_table_query(tx, table_name, &excluded_cols)?;

    // Get row count
    let row_count: usize = tx.query_row(
        &format!("SELECT COUNT(*) FROM {}", table_name),
        [],
        |row| row.get::<_, i64>(0)
    ).map_err(|_| DatabaseError::RecallError)? as usize;

    // Execute query and hash the JSON result
    let rows_json: String = tx.query_row(&query, [], |row| row.get(0))
        .map_err(|e| {
            tracing::error!("Failed to query table {}: {:?}", table_name, e);
            DatabaseError::RecallError
        })?;

    let mut hasher = Hasher::new();
    hasher.update(rows_json.as_bytes());
    let hash = Blake3Hash::new(hasher.finalize());

    Ok(TableHashInfo {
        hash,
        row_count,
        excluded_columns: excluded_cols.iter().map(|s| s.to_string()).collect(),
    })
}

/// Compute hash-based snapshot within a transaction for atomicity
/// This ensures consensus_height, committed_view, and all table data
/// are read from the same database snapshot
pub fn compute_state_snapshot_tx(
    tx: &duckdb::Transaction,
) -> Result<StateSnapshot, DatabaseError> {
    // Get consensus metadata from same transaction snapshot
    let consensus_height = crate::db::consensus::get_current_consensus_height(tx)?;
    let committed_view = crate::db::consensus::get_current_view_tx(tx)?;

    let mut table_hashes = HashMap::new();

    // Compute hash for each consensus-tracked table
    for table_name in CONSENSUS_TABLES {
        let hash_info = compute_table_hash_tx(tx, table_name)?;
        table_hashes.insert(table_name.to_string(), hash_info);
    }

    Ok(StateSnapshot {
        consensus_height,
        committed_view,
        table_hashes,
    })
}

/// Convenience wrapper that manages transaction creation
pub fn compute_state_snapshot(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<StateSnapshot, DatabaseError> {
    match db_connection {
        Ok(mut conn) => {
            let tx = conn.transaction().map_err(|_| DatabaseError::LockError)?;
            compute_state_snapshot_tx(&tx)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

/// Fragment information for distribution diagnostic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentInfo {
    pub fragment_index: u32,
    pub fragment_id: CustomUUID,
    pub fragment_hash: Blake3Hash,
    pub chunk_type: ChunkType,
    pub nodes_with_fragment: Vec<i32>,
}

/// Complete fragment distribution data for a file diagnostic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileFragmentDistribution {
    pub inode_id: CustomUUID,
    pub data_block_id: CustomUUID,
    pub file_size: u64,
    pub placement_height: Option<i64>,
    pub fragment_count: u32,
    pub original_count: u32,
    pub recovery_count: u32,
    pub fragments: Vec<FragmentInfo>,
}

/// Get fragment distribution data for a specific file
/// This is a diagnostic function that queries fragment_hashes and fragment_inventory
/// to show which nodes store which fragments for a given file
pub fn get_file_fragment_distribution(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    encrypted_path: String,
    user_id: i32,
) -> Result<FileFragmentDistribution, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // First, get the file's inode_id, data_block_id, and basic metadata
            let file_metadata: Option<(CustomUUID, CustomUUID, u64, Option<i32>, i32)> = db_lock.query_row(
                "SELECT i.id, db.id, db.file_size, db.placement_height, db.fragment_count
                 FROM inodes i
                 JOIN data_blocks db ON i.data_id = db.id
                 WHERE i.path = ? AND i.owner_id = ? AND i.type = 'file'",
                params![encrypted_path, user_id],
                |row| Ok((
                    row.get(0)?,  // inode_id
                    row.get(1)?,  // data_block_id
                    row.get::<_, i64>(2)? as u64,  // file_size
                    row.get(3)?,  // placement_height
                    row.get(4)?,  // fragment_count
                ))
            ).optional().map_err(|_| DatabaseError::RecallError)?;

            let (inode_id, data_block_id, file_size, placement_height, fragment_count) = match file_metadata {
                Some(metadata) => metadata,
                None => return Err(DatabaseError::NotFound),
            };

            // Get all fragments for this file with their metadata
            let mut stmt = db_lock.prepare(
                "SELECT fh.fragment_index, fh.fragment_id, fh.fragment_hash, fh.chunk_type
                 FROM fragment_hashes fh
                 WHERE fh.data_block_id = ?
                 ORDER BY fh.fragment_index"
            ).map_err(|_| DatabaseError::RecallError)?;

            let fragment_rows = stmt.query_map(params![data_block_id], |row| {
                Ok((
                    row.get::<_, i32>(0)? as u32,  // fragment_index
                    row.get::<_, CustomUUID>(1)?,  // fragment_id
                    row.get::<_, Blake3Hash>(2)?,  // fragment_hash
                    row.get::<_, ChunkType>(3)?,   // chunk_type
                ))
            }).map_err(|_| DatabaseError::ProcessingError)?;

            // Collect fragment data
            let mut fragments = Vec::new();
            let mut original_count = 0u32;
            let mut recovery_count = 0u32;

            for row in fragment_rows {
                let (fragment_index, fragment_id, fragment_hash, chunk_type) =
                    row.map_err(|_| DatabaseError::ProcessingError)?;

                // Count original vs recovery fragments
                match chunk_type {
                    ChunkType::Original => original_count += 1,
                    ChunkType::Recovery => recovery_count += 1,
                }

                // Query fragment_inventory for node IDs that have this fragment
                let mut inv_stmt = db_lock.prepare(
                    "SELECT node_id
                     FROM fragment_inventory
                     WHERE fragment_hash = ?
                     ORDER BY self_verified_height DESC"
                ).map_err(|_| DatabaseError::RecallError)?;

                let nodes: Result<Vec<i32>, _> = inv_stmt.query_map(
                    params![fragment_hash],
                    |row| row.get(0)
                ).map_err(|_| DatabaseError::RecallError)?
                .collect();

                fragments.push(FragmentInfo {
                    fragment_index,
                    fragment_id,
                    fragment_hash,
                    chunk_type,
                    nodes_with_fragment: nodes.map_err(|_| DatabaseError::ProcessingError)?,
                });
            }

            Ok(FileFragmentDistribution {
                inode_id,
                data_block_id,
                file_size,
                placement_height: placement_height.map(|h| h as i64),
                fragment_count: fragment_count as u32,
                original_count,
                recovery_count,
                fragments,
            })
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}
