use super::*;
use blake3::Hasher;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tables to skip entirely (local-only state)
const LOCAL_ONLY_TABLES: &[&str] = &[
    "this_node",
    "modification_log",
    "pending_fragment_requests",
    "hopnet_storage_pins",
];

/// Columns to exclude from consensus-tracked tables
const EXCLUDED_COLUMNS: &[(&str, &[&str])] = &[
    ("fragment_hashes", &["stored_locally"]),
    ("fragment_inventory", &["self_verified_height"]),
    ("quorum_certificates", &["voter_signatures"]),
    ("timeout_certificates", &["signatures"]),
];

/// All consensus-tracked tables (order matters for deterministic hashing)
const CONSENSUS_TABLES: &[&str] = &[
    "sequences",
    "users",
    "nodes",
    "validators",
    // Malachite decided chain. Deliberately EXCLUDED: consensus_wal
    // (per-node ephemeral), consensus_meta (per-node cursor), and
    // decided_certificates (a certificate is a node-local quorum proof —
    // different vote subsets are legitimate). decided_blocks IS the
    // agreement invariant.
    "decided_blocks",
    "data_blocks",
    "blob_access",
    "fragment_hashes",
    "inodes",
    "takeouts",
    "fragment_request_metrics",
    "metrics",
    "fragment_inventory",
    "device_tokens",
    "hopnet_storage_policy",
    "hopnet_consensus_policy",
];

/// Internal state snapshot with rich types (Blake3Hash)
#[derive(Debug)]
pub struct StateSnapshot {
    pub consensus_height: u64,
    pub committed_view: u64,
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
                    (
                        table_name,
                        hopnet_common::TableHashInfo {
                            hash: info.hash.to_hex(),
                            row_count: info.row_count,
                            excluded_columns: info.excluded_columns,
                        },
                    )
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
    tx: &rusqlite::Transaction,
    table_name: &str,
) -> Result<Vec<String>, DatabaseError> {
    let mut stmt = tx
        .prepare(&format!("PRAGMA table_info({})", table_name))
        .map_err(|_| DatabaseError::RecallError)?;

    let mut pk_columns: Vec<String> = Vec::new();

    let rows = stmt
        .query_map([], |row| {
            let name: String = row.get(1)?; // Column name
            let is_pk: bool = row.get(5)?; // Part of primary key?
            Ok((name, is_pk))
        })
        .map_err(|_| DatabaseError::RecallError)?;

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
/// Uses PRAGMA table_info for dynamic column listing (SQLite has no json_object(*) or EXCLUDE)
fn build_table_query(
    tx: &rusqlite::Transaction,
    table_name: &str,
    excluded_cols: &[&str],
) -> Result<String, DatabaseError> {
    let pk_cols = get_primary_key_columns(tx, table_name)?;

    // Get all column names and types from PRAGMA table_info
    let mut stmt = tx
        .prepare(&format!("PRAGMA table_info({})", table_name))
        .map_err(|_| DatabaseError::RecallError)?;

    let all_columns: Vec<(String, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })
        .map_err(|_| DatabaseError::RecallError)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| DatabaseError::RecallError)?;

    // Filter out excluded columns
    let columns: Vec<&(String, String)> = all_columns
        .iter()
        .filter(|(col, _)| !excluded_cols.contains(&col.as_str()))
        .collect();

    // Build json_object arguments: 'col1', col1, 'col2', col2, ...
    // BLOB columns must be hex-encoded (SQLite json_object cannot hold BLOBs)
    let json_args: Vec<String> = columns
        .iter()
        .map(|(col, col_type)| {
            if col_type.eq_ignore_ascii_case("BLOB") {
                format!("'{}', hex({})", col, col)
            } else {
                format!("'{}', {}", col, col)
            }
        })
        .collect();
    let json_object_expr = format!("json_object({})", json_args.join(", "));

    // Build explicit column list for SELECT
    let column_list = columns
        .iter()
        .map(|(c, _)| c.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "SELECT COALESCE(json_group_array({}), '[]') FROM (SELECT {} FROM {} ORDER BY {})",
        json_object_expr,
        column_list,
        table_name,
        pk_cols.join(", ")
    );

    Ok(query)
}

/// Compute hash for a single table within a transaction
fn compute_table_hash_tx(
    tx: &rusqlite::Transaction,
    table_name: &str,
) -> Result<TableHashInfo, DatabaseError> {
    // Build query with EXCLUDE clause if needed
    let excluded_cols = get_excluded_columns(table_name);
    let query = build_table_query(tx, table_name, &excluded_cols)?;

    // Get row count
    let row_count: usize = tx
        .query_row(&format!("SELECT COUNT(*) FROM {}", table_name), [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|_| DatabaseError::RecallError)? as usize;

    // Execute query and hash the JSON result
    let rows_json: String = tx.query_row(&query, [], |row| row.get(0)).map_err(|e| {
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
    tx: &rusqlite::Transaction,
) -> Result<StateSnapshot, DatabaseError> {
    // Get consensus metadata from same transaction snapshot. Views died with
    // the bespoke engine — the decided height is the only progress marker.
    let consensus_height = crate::db::consensus::get_current_consensus_height(tx)?;
    let committed_view = consensus_height;

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
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<StateSnapshot, DatabaseError> {
    match db_connection {
        Ok(mut conn) => {
            let tx = conn.transaction().map_err(|_| DatabaseError::LockError)?;
            compute_state_snapshot_tx(&tx)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Fragment information for distribution diagnostic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentInfo {
    pub chunk_number: u32,
    pub local_index: u32,
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
    pub placement_height: Option<u64>,
    pub fragment_count: u32,
    pub original_count: u32,
    pub recovery_count: u32,
    pub fragments: Vec<FragmentInfo>,
}

/// Always-on connection-pool / transaction counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterSnapshot {
    pub txn_commits: u64,
    pub txn_rollbacks: u64,
    pub conn_acquires: u64,
}

/// HDR histogram percentile snapshot of commit latency in microseconds.
/// Populated only by `commit_timed()` call sites (consensus + hot writes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySnapshot {
    pub count: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

/// SQLite sizing + active pragma snapshot + telemetry counters.
/// Read-only: no checkpoints, no writes. WAL size comes from filesystem stat
/// to avoid the checkpoint side-effect of `PRAGMA wal_checkpoint`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbStats {
    pub page_count: i64,
    pub page_size: i64,
    pub db_bytes: i64,
    pub freelist_count: i64,
    pub wal_bytes: u64,
    pub journal_mode: String,
    pub synchronous: String,
    pub cache_size_raw: i64,
    pub cache_bytes: i64,
    pub mmap_size: i64,
    pub temp_store: String,
    pub busy_timeout_ms: i64,
    pub counters: CounterSnapshot,
    pub commit_latency_us: LatencySnapshot,
}

pub fn get_db_stats(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<DbStats, DatabaseError> {
    let conn = db_connection.map_err(|_| DatabaseError::LockError)?;

    let q_i64 = |sql: &str| -> Result<i64, DatabaseError> {
        conn.query_row(sql, [], |r| r.get(0))
            .map_err(|_| DatabaseError::RecallError)
    };
    let q_str = |sql: &str| -> Result<String, DatabaseError> {
        conn.query_row(sql, [], |r| r.get(0))
            .map_err(|_| DatabaseError::RecallError)
    };

    let page_count = q_i64("PRAGMA page_count")?;
    let page_size = q_i64("PRAGMA page_size")?;
    let freelist_count = q_i64("PRAGMA freelist_count")?;
    let journal_mode = q_str("PRAGMA journal_mode")?;
    let sync_int = q_i64("PRAGMA synchronous")?;
    let cache_size_raw = q_i64("PRAGMA cache_size")?;
    let mmap_size = q_i64("PRAGMA mmap_size")?;
    let temp_store_int = q_i64("PRAGMA temp_store")?;
    let busy_timeout_ms = q_i64("PRAGMA busy_timeout")?;

    let db_path: Option<String> = conn
        .query_row(
            "SELECT file FROM pragma_database_list WHERE name = 'main'",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    let wal_bytes = db_path
        .as_deref()
        .filter(|p| !p.is_empty())
        .and_then(|p| std::fs::metadata(format!("{}-wal", p)).ok())
        .map(|m| m.len())
        .unwrap_or(0);

    let synchronous = match sync_int {
        0 => "OFF",
        1 => "NORMAL",
        2 => "FULL",
        3 => "EXTRA",
        _ => "UNKNOWN",
    }
    .to_string();

    let temp_store = match temp_store_int {
        0 => "DEFAULT",
        1 => "FILE",
        2 => "MEMORY",
        _ => "UNKNOWN",
    }
    .to_string();

    let cache_bytes = if cache_size_raw < 0 {
        -cache_size_raw * 1024
    } else {
        cache_size_raw * page_size
    };

    let db_bytes = page_count * page_size;

    let counters = CounterSnapshot {
        txn_commits: crate::db::shared::DB_COUNTERS
            .txn_commits
            .load(std::sync::atomic::Ordering::Relaxed),
        txn_rollbacks: crate::db::shared::DB_COUNTERS
            .txn_rollbacks
            .load(std::sync::atomic::Ordering::Relaxed),
        conn_acquires: crate::db::shared::DB_COUNTERS
            .conn_acquires
            .load(std::sync::atomic::Ordering::Relaxed),
    };

    let commit_latency_us = {
        let h = crate::db::shared::COMMIT_LATENCY_US.lock();
        LatencySnapshot {
            count: h.len(),
            p50_us: h.value_at_quantile(0.50),
            p90_us: h.value_at_quantile(0.90),
            p99_us: h.value_at_quantile(0.99),
            p999_us: h.value_at_quantile(0.999),
            max_us: h.max(),
        }
    };

    Ok(DbStats {
        page_count,
        page_size,
        db_bytes,
        freelist_count,
        wal_bytes,
        journal_mode,
        synchronous,
        cache_size_raw,
        cache_bytes,
        mmap_size,
        temp_store,
        busy_timeout_ms,
        counters,
        commit_latency_us,
    })
}

/// Get fragment distribution data for a specific file
/// This is a diagnostic function that queries fragment_hashes and fragment_inventory
/// to show which nodes store which fragments for a given file
pub fn get_file_fragment_distribution(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    encrypted_path: String,
    user_id: i32,
) -> Result<FileFragmentDistribution, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // First, get the file's inode_id, data_block_id, and basic metadata
            let file_metadata: Option<(CustomUUID, CustomUUID, u64, Option<u64>, i32)> = db_lock
                .query_row(
                    "SELECT i.id, db.id, db.file_size, db.placement_height, db.fragment_count
                 FROM inodes i
                 JOIN data_blocks db ON i.data_id = db.id
                 WHERE i.path = ? AND i.owner_id = ? AND i.type = 0",
                    params![encrypted_path, user_id],
                    |row| {
                        Ok((
                            row.get(0)?,                  // inode_id
                            row.get(1)?,                  // data_block_id
                            row.get::<_, i64>(2)? as u64, // file_size
                            row.get::<_, Option<i64>>(3)?.map(hopnet_common::height::height_from_db), // placement_height
                            row.get(4)?,                  // fragment_count
                        ))
                    },
                )
                .optional()
                .map_err(|_| DatabaseError::RecallError)?;

            let (inode_id, data_block_id, file_size, placement_height, fragment_count) =
                match file_metadata {
                    Some(metadata) => metadata,
                    None => return Err(DatabaseError::NotFound),
                };

            // Get all fragments for this file with their metadata
            let mut stmt = db_lock.prepare(
                "SELECT fh.chunk_number, fh.local_index, fh.fragment_id, fh.fragment_hash, fh.chunk_type
                 FROM fragment_hashes fh
                 WHERE fh.data_block_id = ?
                 ORDER BY fh.chunk_number, fh.local_index"
            ).map_err(|_| DatabaseError::RecallError)?;

            let fragment_rows = stmt
                .query_map(params![data_block_id], |row| {
                    Ok((
                        row.get::<_, u32>(0)?,        // chunk_number
                        row.get::<_, u32>(1)?,        // local_index
                        row.get::<_, CustomUUID>(2)?, // fragment_id
                        row.get::<_, Blake3Hash>(3)?, // fragment_hash
                        row.get::<_, ChunkType>(4)?,  // chunk_type
                    ))
                })
                .map_err(|_| DatabaseError::ProcessingError)?;

            // Collect fragment data
            let mut fragments = Vec::new();
            let mut original_count = 0u32;
            let mut recovery_count = 0u32;

            for row in fragment_rows {
                let (chunk_number, local_index, fragment_id, fragment_hash, chunk_type) =
                    row.map_err(|_| DatabaseError::ProcessingError)?;

                // Count original vs recovery fragments
                match chunk_type {
                    ChunkType::Original => original_count += 1,
                    ChunkType::Recovery => recovery_count += 1,
                }

                // Query fragment_inventory for node IDs that have this fragment
                let mut inv_stmt = db_lock
                    .prepare(
                        "SELECT node_id
                     FROM fragment_inventory
                     WHERE fragment_hash = ?
                     ORDER BY self_verified_height DESC",
                    )
                    .map_err(|_| DatabaseError::RecallError)?;

                let nodes: Result<Vec<i32>, _> = inv_stmt
                    .query_map(params![fragment_hash], |row| row.get(0))
                    .map_err(|_| DatabaseError::RecallError)?
                    .collect();

                fragments.push(FragmentInfo {
                    chunk_number,
                    local_index,
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
                placement_height,
                fragment_count: fragment_count as u32,
                original_count,
                recovery_count,
                fragments,
            })
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}
