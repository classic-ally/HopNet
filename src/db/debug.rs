use super::*;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};


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
