//! Orphaned-fragment garbage collection: filesystem fragments with no
//! `fragment_hashes` row (RFC-017 Stage 5 — descended from the host's
//! maintenance jobs).
//!
//! Two-phase by design: `scan_orphaned_fragments` diffs disk against the
//! replicated fragment table (grace-period filtered to avoid racing
//! in-flight stores); `cleanup_orphaned_fragments` deletes a previously
//! captured scan, refusing stale ones. The HOST owns the scan cache, the
//! clock reads, and the conn checkout — both functions are pure over their
//! inputs.

use crate::error::StorageError;
use crate::fragstore;
use crate::store::db_err;
use hopnet_common::Blake3Hash;
use serde::{Deserialize, Serialize};

/// Result of scanning filesystem for orphaned fragments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrphanedFragmentScan {
    pub scanned_at: i64, // Unix timestamp
    pub total_fragments: usize,
    pub orphaned_fragments: Vec<Blake3Hash>,
    pub total_bytes: u64,
}

/// Result of cleaning up orphaned fragments
#[derive(Debug, Serialize)]
pub struct OrphanedFragmentCleanupResult {
    pub deleted_count: usize,
    pub failed_count: usize,
    pub bytes_freed: u64,
}

/// Cleanup refused: the scan is older than the staleness bound.
#[derive(Debug)]
pub struct StaleScan {
    pub age_seconds: i64,
}

/// Staleness bound on a scan consumed by cleanup.
const SCAN_MAX_AGE_SECONDS: i64 = 3600;

/// Scan the fragment store for fragments that don't exist in the database.
/// Only considers fragments older than `grace_period_hours` (against the
/// caller-supplied `now_unix`) to avoid race conditions with in-flight
/// stores.
pub fn scan_orphaned_fragments(
    conn: &rusqlite::Connection,
    fragments_dir: &str,
    grace_period_hours: i64,
    now_unix: u64,
) -> Result<OrphanedFragmentScan, StorageError> {
    let cutoff_time = now_unix - (grace_period_hours * 3600) as u64;
    let disk_fragments = fragstore::scan_fragments(fragments_dir, cutoff_time)?;

    tracing::info!(
        "Found {} fragments on disk (older than {} hours)",
        disk_fragments.len(),
        grace_period_hours
    );

    if disk_fragments.is_empty() {
        return Ok(OrphanedFragmentScan {
            scanned_at: now_unix as i64,
            total_fragments: 0,
            orphaned_fragments: Vec::new(),
            total_bytes: 0,
        });
    }

    // Check which fragments exist in database (batch query for efficiency)
    let batch_size = 1000;
    let mut orphaned_fragments = Vec::new();
    let mut total_bytes = 0u64;

    for chunk in disk_fragments.chunks(batch_size) {
        // Build parameterized query to check which hashes exist in fragment_hashes table
        let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "SELECT fragment_hash FROM fragment_hashes WHERE fragment_hash IN ({})",
            placeholders
        );
        let mut stmt = conn
            .prepare(&query)
            .map_err(db_err("prepare orphan diff"))?;

        let params: Vec<&dyn rusqlite::ToSql> = chunk
            .iter()
            .map(|(hash, _)| hash as &dyn rusqlite::ToSql)
            .collect();
        let mut rows = stmt
            .query(params.as_slice())
            .map_err(db_err("query orphan diff"))?;

        // Collect hashes that exist in database
        let mut db_hashes = std::collections::HashSet::new();
        while let Some(row) = rows.next().map_err(db_err("read orphan diff row"))? {
            let hash: Blake3Hash = row.get(0).map_err(db_err("parse orphan diff hash"))?;
            db_hashes.insert(hash);
        }

        // Find orphaned fragments (on disk but not in database)
        for (hash, size) in chunk {
            if !db_hashes.contains(hash) {
                orphaned_fragments.push(*hash);
                total_bytes += size;
            }
        }
    }

    tracing::info!(
        "Scan complete: {} orphaned fragments found ({} bytes)",
        orphaned_fragments.len(),
        total_bytes
    );

    Ok(OrphanedFragmentScan {
        scanned_at: now_unix as i64,
        total_fragments: disk_fragments.len(),
        orphaned_fragments,
        total_bytes,
    })
}

/// Delete the fragments captured by a previous scan. Refuses a scan older
/// than the staleness bound (the disk/DB state it diffed has drifted).
pub fn cleanup_orphaned_fragments(
    fragments_dir: &str,
    scan: OrphanedFragmentScan,
    now_unix: i64,
) -> Result<OrphanedFragmentCleanupResult, StaleScan> {
    let age_seconds = now_unix - scan.scanned_at;
    if age_seconds > SCAN_MAX_AGE_SECONDS {
        return Err(StaleScan { age_seconds });
    }

    tracing::info!(
        "Deleting {} orphaned fragments",
        scan.orphaned_fragments.len()
    );

    // Approximate per-fragment size (individual sizes aren't stored)
    let avg_size = if scan.orphaned_fragments.is_empty() {
        0
    } else {
        scan.total_bytes / scan.orphaned_fragments.len() as u64
    };

    let mut deleted_count = 0;
    let mut failed_count = 0;
    let mut bytes_freed = 0u64;

    for fragment_hash in &scan.orphaned_fragments {
        match fragstore::delete_fragment(fragments_dir, fragment_hash) {
            Ok(()) => {
                deleted_count += 1;
                bytes_freed += avg_size;
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to delete fragment {}: {:?}",
                    fragment_hash.to_hex(),
                    e
                );
                failed_count += 1;
            }
        }
    }

    Ok(OrphanedFragmentCleanupResult {
        deleted_count,
        failed_count,
        bytes_freed,
    })
}
