use crate::{
    AppState,
    db::{
        CustomUUID,
        fragments::{
            AvailabilityClass, find_orphaned_data_blocks, get_node_availability_classification,
        },
    },
    storage_host::substrate_host::SubstrateHost,
};
use apalis::prelude::*;
use hopnet_storage::maintenance::{OrphanedFragmentCleanupResult, OrphanedFragmentScan};
use hopnet_storage::traits::TxSubmitter;
use std::sync::Arc;

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
    let cutoff_uuid = CustomUUID::retention_cutoff(retention_days);

    tracing::info!(
        "Using {}-day retention policy, batch size: {}, cutoff UUID: {}",
        retention_days,
        batch_size,
        cutoff_uuid
    );

    // Storage-owned tx submission rides the TxSubmitter seam (sign + queue).
    let submitter = SubstrateHost::new(app_state.clone());

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
        let batch_len = data_block_ids.len();
        let payload = hopnet_storage::DeleteOrphanedDataBlocksPayload { data_block_ids };

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

        // Submit to consensus
        match submitter
            .submit("delete_orphaned_data_blocks", serialized_payload)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    "Successfully submitted consensus transaction to delete {} data blocks",
                    batch_len
                );
                total_cleaned += batch_len;
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

    // Get data blocks that need rebalancing — scoped checkout, dropped
    // before the engine's data plane runs.
    let data_blocks_to_rebalance = {
        let conn = app_state.db_pool.get().map_err(|e| {
            tracing::error!("Failed to get database connection for rebalancing: {:?}", e);
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Failed to get database connection: {:?}",
                e
            )))))
        })?;
        hopnet_storage::store::get_data_blocks_for_rebalancing(
            &conn,
            max_placement_height,
            max_data_blocks,
        )
        .map_err(|e| {
            tracing::error!("Failed to get data blocks for rebalancing: {:?}", e);
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Failed to get data blocks: {:?}",
                e
            )))))
        })?
    };

    tracing::info!(
        "Found {} data blocks to check for repair",
        data_blocks_to_rebalance.len()
    );

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

    let stats = storage
        .repair_blobs(
            data_blocks_to_rebalance
                .into_iter()
                .map(|block| block.data_block_id),
        )
        .await;

    let result = NetworkRebalancingResult {
        consensus_height,
        data_blocks_checked: stats.checked,
        data_blocks_rebalanced: stats.repaired,
        data_blocks_failed: stats.failed,
        total_fragments_migrated: stats.fragments_pulled,
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

    // Submit through the TxSubmitter seam (sign + consensus queue)
    match SubstrateHost::new(app_state.clone())
        .submit("self_check_fragments", serialized_payload)
        .await
    {
        Ok(()) => Ok(()),
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
// (scan/cleanup logic lives in hopnet_storage::maintenance — RFC-017 Stage 5;
// the host keeps the conn checkout, clock reads, and the scan cache)
// =============================================================================

/// Scan filesystem for fragments that don't exist in database
/// Only considers fragments older than grace_period_hours to avoid race conditions
pub async fn run_orphaned_fragments_scan(
    app_state: &AppState,
    grace_period_hours: i64,
) -> Result<OrphanedFragmentScan, Error> {
    use std::time::SystemTime;

    tracing::info!(
        "Starting orphaned fragments scan (grace period: {} hours)",
        grace_period_hours
    );

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(
                "Failed to get current time",
            ))))
        })?
        .as_secs();

    let db_conn = app_state.db_pool.get().map_err(|e| {
        Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
            "Failed to get database connection: {:?}",
            e
        )))))
    })?;

    let scan_result = hopnet_storage::maintenance::scan_orphaned_fragments(
        &db_conn,
        &app_state.fragments_dir,
        grace_period_hours,
        now,
    )
    .map_err(|e| {
        Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
            "Scan failed: {}",
            e
        )))))
    })?;

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

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(
                "Failed to get current time",
            ))))
        })?
        .as_secs() as i64;

    hopnet_storage::maintenance::cleanup_orphaned_fragments(&app_state.fragments_dir, scan, now)
        .map_err(|stale| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Scan is stale ({} seconds old). Run a new scan first",
                stale.age_seconds
            )))))
        })
}
