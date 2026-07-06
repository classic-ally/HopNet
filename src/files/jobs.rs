use crate::{
    AppState,
    db::{
        CustomUUID, DatabaseError,
        fragments::{
            AvailabilityClass, find_orphaned_data_blocks, get_node_availability_classification,
        },
    },
};
use apalis::prelude::*;
use chrono::Utc;
use std::sync::Arc;
use uuid::{Timestamp, timestamp::context::NoContext};

#[derive(Debug)]
pub enum MaintenanceError {
    Database(DatabaseError),
    Storage(String),
    Configuration(String),
}

impl From<DatabaseError> for MaintenanceError {
    fn from(e: DatabaseError) -> Self {
        MaintenanceError::Database(e)
    }
}

/// Manual trigger for orphaned data block cleanup  
/// Initially only supports manual trigger - threshold checking and scheduling to be added later
pub async fn handle_orphaned_data_block_cleanup(
    job: TaskId,
    ctx: Data<AppState>,
) -> Result<(), Error> {
    // Use default values for scheduled jobs
    run_orphaned_data_block_cleanup(&ctx, 50, 30)
        .await
        .map(|_| ())
}

/// Core cleanup logic that can be called from job handler or manual trigger
pub async fn run_orphaned_data_block_cleanup(
    app_state: &AppState,
    batch_size: i32,
    retention_days: i64,
) -> Result<usize, Error> {
    tracing::info!("Starting orphaned data block cleanup");

    // Pre-flight check: Ensure no active takeouts are in progress
    match crate::db::takeout::has_active_takeout(app_state.db_pool.get(), None) {
        Ok(true) => {
            let error_msg = "Cannot run orphaned data cleanup: active takeout(s) in progress. Wait for takeouts to expire or complete before running cleanup.";
            tracing::warn!("{}", error_msg);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                error_msg,
            )))));
        }
        Ok(false) => {
            tracing::info!("Pre-flight check passed: no active takeouts found");
        }
        Err(e) => {
            tracing::error!(
                "Failed to check for active takeouts before cleanup: {:?}",
                e
            );
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                "Failed to check active takeouts",
            )))));
        }
    }

    // Get node ID for availability classification
    let node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("Node ID not initialized, cannot run cleanup");
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                "Node ID not initialized",
            )))));
        }
    };

    // Get database connection
    let db_connection = app_state.db_pool.get();

    // Determine cleanup strategy based on availability
    let (node_availability, availability_class) = match get_node_availability_classification(
        db_connection,
        node_id,
        30, // 30-day rolling average
    ) {
        Ok((avail, class)) => (avail, class),
        Err(e) => {
            tracing::error!("Failed to determine node availability: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                format!("Failed to determine node availability: {:?}", e),
            )))));
        }
    };

    tracing::info!(
        "Node availability: {:.1}%, classification: {:?}",
        node_availability * 100.0,
        availability_class
    );

    // For now, implement only historical data cleanup (Stage 1 for below-average nodes)
    // Redundant copy cleanup to be implemented later
    match availability_class {
        AvailabilityClass::BelowAverage => {
            tracing::info!("Below-average availability node: cleaning historical data first");
            cleanup_orphaned_data_blocks(app_state, node_id, batch_size, retention_days).await
        }
        AvailabilityClass::AboveAverage => {
            tracing::info!(
                "Above-average availability node: would clean redundant copies first (not implemented yet)"
            );
            // TODO: Implement redundant copy cleanup
            // For now, also clean historical data
            cleanup_orphaned_data_blocks(app_state, node_id, batch_size, retention_days).await
        }
    }
}

async fn cleanup_orphaned_data_blocks(
    app_state: &AppState,
    _node_id: i32,
    batch_size: i32,
    retention_days: i64,
) -> Result<usize, Error> {
    let mut total_cleaned = 0;

    // Generate cutoff UUID for retention policy
    let cutoff_uuid = generate_cutoff_uuid(retention_days).map_err(|e| {
        tracing::error!("Failed to generate cutoff UUID: {:?}", e);
        Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
            "Failed to generate cutoff UUID: {:?}",
            e
        )))))
    })?;

    tracing::info!(
        "Using {}-day retention policy, batch size: {}, cutoff UUID: {}",
        retention_days,
        batch_size,
        cutoff_uuid
    );

    loop {
        // Get database connection for this batch
        let db_connection = app_state.db_pool.get();

        // Find batch of orphaned data blocks
        let data_block_ids =
            match find_orphaned_data_blocks(db_connection, &cutoff_uuid, batch_size) {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::error!("Failed to find orphaned data blocks: {:?}", e);
                    return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                        format!("Failed to find orphaned data blocks: {:?}", e),
                    )))));
                }
            };

        if data_block_ids.is_empty() {
            tracing::info!("No more orphaned data blocks to clean");
            break;
        }

        tracing::info!(
            "Found {} orphaned data blocks in this batch",
            data_block_ids.len()
        );

        // Submit consensus transaction to delete these data blocks
        let payload = crate::files::handlers::DeleteOrphanedDataBlocksPayload {
            data_block_ids: data_block_ids.clone(),
        };

        let serialized_payload =
            match bincode::serde::encode_to_vec(&payload, bincode::config::standard()) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("Failed to serialize deletion payload: {:?}", e);
                    return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                        format!("Failed to serialize deletion payload: {:?}", e),
                    )))));
                }
            };

        let transaction = crate::consensus::dispatch::create_signed_transaction(
            app_state,
            "delete_orphaned_data_blocks".to_string(),
            serialized_payload,
        )
        .map_err(|_| {
            Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to sign transaction",
            ))))
        })?;

        // Get user ID for consensus submission
        let user_id = match app_state.get_user_id() {
            Ok(id) => id,
            Err(_) => {
                tracing::error!("User ID not initialized, cannot submit consensus transaction");
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                    "User ID not initialized",
                )))));
            }
        };

        // Submit to consensus
        match app_state.consensus_queue.submit(transaction).await {
            Ok(_) => {
                tracing::info!(
                    "Successfully submitted consensus transaction to delete {} data blocks",
                    data_block_ids.len()
                );
                total_cleaned += data_block_ids.len();
            }
            Err(e) => {
                tracing::error!("Failed to submit consensus transaction: {:?}", e);
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                    format!("Failed to submit consensus transaction: {:?}", e),
                )))));
            }
        }
    }

    Ok(total_cleaned)
}

fn generate_cutoff_uuid(retention_days: i64) -> Result<CustomUUID, MaintenanceError> {
    let cutoff_time = Utc::now() - chrono::Duration::days(retention_days);

    // Preserve sub-second precision: UUIDv7 ordering is millisecond-granular,
    // so a seconds-truncated cutoff makes anything created in the current
    // second invisible to retention_days=0 scans.
    let timestamp = Timestamp::from_unix(
        NoContext,
        cutoff_time.timestamp() as u64,
        cutoff_time.timestamp_subsec_nanos(),
    );

    Ok(CustomUUID::new(Some(&timestamp)))
}

/// Network rebalancing job (tier-1 repair, RFC-014): for blobs whose
/// placement commit has aged past min_age_heights, ask the storage engine
/// to recompute placement at the current height and pull/re-commit if the
/// selection moved.
pub async fn run_network_rebalancing(
    app_state: &AppState,
    max_data_blocks: i32,
    min_age_heights: i32,
) -> Result<NetworkRebalancingResult, Error> {
    tracing::info!(
        "Starting network rebalancing (max {} data blocks, min age {} heights)",
        max_data_blocks,
        min_age_heights
    );

    // Get current consensus height
    let consensus_height = match app_state
        .db_pool
        .get()
        .map_err(|_| crate::db::DatabaseError::LockError)
        .and_then(|conn| crate::db::consensus::get_current_consensus_height(&conn))
    {
        Ok(height) => height,
        Err(e) => {
            tracing::error!("Failed to get consensus height for rebalancing: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                format!("Failed to get consensus height: {:?}", e),
            )))));
        }
    };

    let max_placement_height = consensus_height - min_age_heights;
    tracing::info!(
        "Rebalancing at height {}, looking for data blocks placed before height {}",
        consensus_height,
        max_placement_height
    );

    // Get data blocks that need rebalancing
    let data_blocks_to_rebalance = match crate::db::fragments::get_data_blocks_for_rebalancing(
        app_state.db_pool.get(),
        max_placement_height,
        max_data_blocks,
    ) {
        Ok(blocks) => blocks,
        Err(e) => {
            tracing::error!("Failed to get data blocks for rebalancing: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                format!("Failed to get data blocks: {:?}", e),
            )))));
        }
    };

    let total_data_blocks = data_blocks_to_rebalance.len();
    tracing::info!("Found {} data blocks to check for repair", total_data_blocks);

    // Tier-1 repair (RFC-014): the storage engine recomputes the seeded
    // placement at the current height (blob_id seed — computable again since
    // Stage B killed the file_hash seed) and pulls what this node should now
    // hold; the new primary re-commits placement. Serial on the engine's
    // repair worker.
    let Some(storage) = app_state.storage.get() else {
        return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
            "storage engine not running",
        )))));
    };

    let mut rebalanced = 0usize;
    let mut failed = 0usize;
    let mut migrated = 0usize;
    for block in &data_blocks_to_rebalance {
        use hopnet_storage::engine::RepairOutcome;
        match storage.repair_blob(block.data_block_id.clone()).await {
            Some(RepairOutcome::Unchanged) => {}
            Some(RepairOutcome::Repaired {
                fragments_pulled,
                recommitted,
            }) => {
                rebalanced += 1;
                migrated += fragments_pulled;
                tracing::info!(
                    "repair: blob {} pulled {} fragments (recommitted: {})",
                    block.data_block_id,
                    fragments_pulled,
                    recommitted
                );
            }
            Some(RepairOutcome::Failed { fragments_pulled }) => {
                failed += 1;
                migrated += fragments_pulled;
            }
            None => {
                failed += 1;
                tracing::error!("repair: engine gone while repairing {}", block.data_block_id);
            }
        }
    }

    let result = NetworkRebalancingResult {
        consensus_height,
        data_blocks_checked: total_data_blocks,
        data_blocks_rebalanced: rebalanced,
        data_blocks_failed: failed,
        total_fragments_migrated: migrated,
    };

    tracing::info!("Network rebalancing completed: {:?}", result);
    Ok(result)
}

#[derive(Debug, Default, serde::Serialize)]
pub struct NetworkRebalancingResult {
    pub consensus_height: i32,
    pub data_blocks_checked: usize,
    pub data_blocks_rebalanced: usize,
    pub data_blocks_failed: usize,
    pub total_fragments_migrated: usize,
}

/// Scheduled job handler for fragment inventory self-check
/// Runs every 20-30 minutes to ensure node's inventory matches local fragment storage
pub async fn handle_fragment_inventory_self_check(
    job: TaskId,
    ctx: Data<AppState>,
) -> Result<(), Error> {
    run_fragment_inventory_self_check(&ctx).await
}

/// Core self-check logic that can be called from job handler or manual trigger
pub async fn run_fragment_inventory_self_check(app_state: &AppState) -> Result<(), Error> {
    // Get node ID
    let node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("Node ID not initialized, cannot run self-check");
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                "Node ID not initialized",
            )))));
        }
    };

    // Compute differential between consensus inventory and local fragments
    let differential = match crate::db::inventory::compute_inventory_differential(
        app_state.db_pool.get(),
        node_id,
    ) {
        Ok(diff) => diff,
        Err(e) => {
            tracing::error!("Failed to compute inventory differential: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                format!("Failed to compute inventory differential: {:?}", e),
            )))));
        }
    };

    // Create payload for consensus submission
    let serialized_payload =
        match bincode::serde::encode_to_vec(&differential, bincode::config::standard()) {
            Ok(data) => data,
            Err(e) => {
                tracing::error!("Failed to serialize self-check differential: {:?}", e);
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                    format!("Failed to serialize differential: {:?}", e),
                )))));
            }
        };

    // Create signed transaction for consensus
    let transaction = crate::consensus::dispatch::create_signed_transaction(
        app_state,
        "self_check_fragments".to_string(),
        serialized_payload,
    )
    .map_err(|_| {
        Error::Failed(Arc::new(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to sign transaction",
        ))))
    })?;

    // Submit to consensus
    match app_state.consensus_queue.submit(transaction).await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!("Failed to submit self-check to consensus: {:?}", e);
            Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                format!("Failed to submit to consensus: {:?}", e),
            )))))
        }
    }
}

// =============================================================================
// ORPHANED FRAGMENT CLEANUP
// Filesystem garbage collection for fragments not in database
// =============================================================================

/// Result of scanning filesystem for orphaned fragments
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OrphanedFragmentScan {
    pub scanned_at: i64, // Unix timestamp
    pub total_fragments: usize,
    pub orphaned_fragments: Vec<crate::db::Blake3Hash>,
    pub total_bytes: u64,
}

/// Result of cleaning up orphaned fragments
#[derive(Debug, serde::Serialize)]
pub struct OrphanedFragmentCleanupResult {
    pub deleted_count: usize,
    pub failed_count: usize,
    pub bytes_freed: u64,
}

/// Scan filesystem for fragments that don't exist in database
/// Only considers fragments older than grace_period_hours to avoid race conditions
pub async fn run_orphaned_fragments_scan(
    app_state: &AppState,
    grace_period_hours: i64,
) -> Result<OrphanedFragmentScan, Error> {
    use std::fs;
    use std::path::Path;
    use std::time::SystemTime;

    tracing::info!(
        "Starting orphaned fragments scan (grace period: {} hours)",
        grace_period_hours
    );

    let fragments_dir = &app_state.fragments_dir;
    let grace_period_secs = grace_period_hours * 3600;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(
                "Failed to get current time",
            ))))
        })?
        .as_secs();

    let cutoff_time = now - grace_period_secs as u64;

    // Walk directory structure to collect all fragment hashes on disk
    let mut disk_fragments: Vec<(crate::db::Blake3Hash, u64, u64)> = Vec::new(); // (hash, size, mtime)

    // Fragments are stored in 2-level directory structure: fragments_dir/AB/CD/ABCD...hash
    let fragments_path = Path::new(fragments_dir);

    if !fragments_path.exists() {
        tracing::warn!("Fragments directory does not exist: {}", fragments_dir);
        return Ok(OrphanedFragmentScan {
            scanned_at: now as i64,
            total_fragments: 0,
            orphaned_fragments: Vec::new(),
            total_bytes: 0,
        });
    }

    // Iterate through first-level directories (00-ff)
    for first_level_entry in
        fs::read_dir(fragments_path).map_err(|e| Error::Failed(Arc::new(Box::new(e))))?
    {
        let first_level_entry =
            first_level_entry.map_err(|e| Error::Failed(Arc::new(Box::new(e))))?;

        if !first_level_entry
            .file_type()
            .map_err(|e| Error::Failed(Arc::new(Box::new(e))))?
            .is_dir()
        {
            continue;
        }

        // Iterate through second-level directories (00-ff)
        for second_level_entry in fs::read_dir(first_level_entry.path())
            .map_err(|e| Error::Failed(Arc::new(Box::new(e))))?
        {
            let second_level_entry =
                second_level_entry.map_err(|e| Error::Failed(Arc::new(Box::new(e))))?;

            if !second_level_entry
                .file_type()
                .map_err(|e| Error::Failed(Arc::new(Box::new(e))))?
                .is_dir()
            {
                continue;
            }

            // Iterate through fragment files
            for file_entry in fs::read_dir(second_level_entry.path())
                .map_err(|e| Error::Failed(Arc::new(Box::new(e))))?
            {
                let file_entry = file_entry.map_err(|e| Error::Failed(Arc::new(Box::new(e))))?;

                let metadata = file_entry
                    .metadata()
                    .map_err(|e| Error::Failed(Arc::new(Box::new(e))))?;

                if !metadata.is_file() {
                    continue;
                }

                // Get modification time
                let mtime = metadata
                    .modified()
                    .map_err(|e| Error::Failed(Arc::new(Box::new(e))))?
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|_| {
                        Error::Failed(Arc::new(Box::new(std::io::Error::other(
                            "Invalid file modification time",
                        ))))
                    })?
                    .as_secs();

                // Only consider files older than grace period
                if mtime >= cutoff_time {
                    continue;
                }

                // Parse filename as Blake3 hash (64 hex characters)
                let filename = file_entry.file_name();
                let filename_str = filename.to_string_lossy();

                if filename_str.len() != 64 {
                    tracing::warn!("Unexpected fragment filename: {}", filename_str);
                    continue;
                }

                // Parse hex to Blake3Hash
                match hex::decode(&*filename_str) {
                    Ok(bytes) if bytes.len() == 32 => {
                        let mut array = [0u8; 32];
                        array.copy_from_slice(&bytes);
                        let hash = crate::db::Blake3Hash::from_bytes(array);
                        disk_fragments.push((hash, metadata.len(), mtime));
                    }
                    _ => {
                        tracing::warn!("Invalid fragment hash filename: {}", filename_str);
                        continue;
                    }
                }
            }
        }
    }

    tracing::info!(
        "Found {} fragments on disk (older than {} hours)",
        disk_fragments.len(),
        grace_period_hours
    );

    if disk_fragments.is_empty() {
        return Ok(OrphanedFragmentScan {
            scanned_at: now as i64,
            total_fragments: 0,
            orphaned_fragments: Vec::new(),
            total_bytes: 0,
        });
    }

    // Check which fragments exist in database (batch query for efficiency)
    let db_conn = app_state.db_pool.get().map_err(|e| {
        Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
            "Failed to get database connection: {:?}",
            e
        )))))
    })?;
    let batch_size = 1000;
    let mut orphaned_fragments = Vec::new();
    let mut total_bytes = 0u64;

    for chunk in disk_fragments.chunks(batch_size) {
        let hashes: Vec<crate::db::Blake3Hash> = chunk.iter().map(|(h, _, _)| *h).collect();

        // Build parameterized query to check which hashes exist in fragment_hashes table
        let placeholders = hashes.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "SELECT fragment_hash FROM fragment_hashes WHERE fragment_hash IN ({})",
            placeholders
        );

        let mut stmt = db_conn.prepare(&query).map_err(|e| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Database query failed: {:?}",
                e
            )))))
        })?;

        // Execute query with hash parameters
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        for hash in &hashes {
            params.push(Box::new(*hash));
        }
        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let mut rows = stmt.query(param_refs.as_slice()).map_err(|e| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Database query execution failed: {:?}",
                e
            )))))
        })?;

        // Collect hashes that exist in database
        let mut db_hashes = std::collections::HashSet::new();
        while let Some(row) = rows.next().map_err(|e| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Failed to read query results: {:?}",
                e
            )))))
        })? {
            let hash: crate::db::Blake3Hash = row.get(0).map_err(|e| {
                Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                    "Failed to parse hash from row: {:?}",
                    e
                )))))
            })?;
            db_hashes.insert(hash);
        }

        // Find orphaned fragments (on disk but not in database)
        for (hash, size, _mtime) in chunk {
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

    let scan_result = OrphanedFragmentScan {
        scanned_at: now as i64,
        total_fragments: disk_fragments.len(),
        orphaned_fragments,
        total_bytes,
    };

    // Store scan result in app state
    *app_state.orphaned_fragment_scan.lock().unwrap() = Some(scan_result.clone());

    Ok(scan_result)
}

/// Delete orphaned fragments based on previous scan
/// Validates scan exists and isn't stale (> 1 hour old)
pub async fn run_orphaned_fragments_cleanup(
    app_state: &AppState,
) -> Result<OrphanedFragmentCleanupResult, Error> {
    use std::time::SystemTime;

    tracing::info!("Starting orphaned fragments cleanup");

    // Get and clear scan from state (take ownership)
    let scan = {
        let mut scan_lock = app_state.orphaned_fragment_scan.lock().unwrap();
        scan_lock.take()
    };

    let scan = scan.ok_or_else(|| {
        Error::Failed(Arc::new(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No scan results available. Run GET /maintenance/orphaned-fragments first",
        ))))
    })?;

    // Validate scan isn't stale (> 1 hour old)
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(
                "Failed to get current time",
            ))))
        })?
        .as_secs() as i64;

    let scan_age_seconds = now - scan.scanned_at;
    if scan_age_seconds > 3600 {
        return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
            format!(
                "Scan is stale ({} seconds old). Run a new scan first",
                scan_age_seconds
            ),
        )))));
    }

    tracing::info!(
        "Deleting {} orphaned fragments",
        scan.orphaned_fragments.len()
    );

    let mut deleted_count = 0;
    let mut failed_count = 0;
    let mut bytes_freed = 0u64;

    // Delete each orphaned fragment
    for fragment_hash in &scan.orphaned_fragments {
        match crate::files::functions::delete_fragment(&app_state.fragments_dir, fragment_hash) {
            Ok(_) => {
                deleted_count += 1;
                // Calculate approximate size (we don't store individual sizes, use average)
                let avg_size = if !scan.orphaned_fragments.is_empty() {
                    scan.total_bytes / scan.orphaned_fragments.len() as u64
                } else {
                    0
                };
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

    tracing::info!(
        "Cleanup complete: {} deleted, {} failed, {} bytes freed",
        deleted_count,
        failed_count,
        bytes_freed
    );

    Ok(OrphanedFragmentCleanupResult {
        deleted_count,
        failed_count,
        bytes_freed,
    })
}
