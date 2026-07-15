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

/// Watermark eviction (RFC-STORAGE-001 GC, RFC-STORAGE-002 S5): under
/// disk pressure, evict SURPLUS fragments oldest-blob-first from the high
/// watermark down to the low. The guard — never responsible, never
/// pinned, another member must attest a copy — carries the safety
/// invariant; watermarks only decide when pressure acts.
///
/// `override_watermarks` is a test hook replacing the this_node settings
/// for one run (e.g. (0, 0) forces maximal eviction of evictable surplus).
pub async fn run_watermark_eviction(
    app_state: &AppState,
    override_watermarks: Option<(u8, u8)>,
    grace_secs: Option<u64>,
) -> Result<serde_json::Value, Error> {
    use hopnet_storage::eviction::{DiskPressure, EvictionCandidate, plan_evictions};
    use hopnet_storage::traits::{LocalStateSink, StateReader};

    let fragments_dir = crate::storage_host::functions::get_fragments_dir()
        .map_err(|e| Error::Failed(Arc::new(format!("fragments dir: {e:?}").into())))?;

    // Disk pressure from the filesystem itself (statvfs).
    let dir = fragments_dir.clone();
    let (total_bytes, used_bytes) = tokio::task::spawn_blocking(move || {
        let stats = fs4::statvfs(&dir)?;
        Ok::<_, std::io::Error>((stats.total_space(), stats.total_space() - stats.available_space()))
    })
    .await
    .map_err(|e| Error::Failed(Arc::new(format!("statvfs join: {e}").into())))?
    .map_err(|e| Error::Failed(Arc::new(format!("statvfs: {e}").into())))?;

    let my_node_id = app_state
        .get_node_id()
        .map_err(|_| Error::Failed(Arc::new("node id not set".to_string().into())))?;

    let (high_pct, low_pct) = match override_watermarks {
        Some(marks) => marks,
        None => {
            let conn = app_state
                .db_pool
                .get()
                .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
            let settings = crate::db::shared::read_storage_node_settings(&conn)
                .map_err(|e| Error::Failed(Arc::new(format!("settings: {e:?}").into())))?;
            (settings.gc_high_pct, settings.gc_low_pct)
        }
    };

    let pressure = DiskPressure {
        used_bytes,
        total_bytes,
        high_pct,
        low_pct,
    };
    let high_bytes = total_bytes / 100 * high_pct as u64;
    if used_bytes <= high_bytes {
        return Ok(serde_json::json!({
            "evicted": 0, "bytes_freed": 0,
            "used_bytes": used_bytes, "total_bytes": total_bytes,
            "high_pct": high_pct, "low_pct": low_pct,
            "reason": "below high watermark",
        }));
    }

    // Member view + on-disk fragments (grace period avoids racing
    // in-flight stores, mirroring the orphan scan).
    let host = SubstrateHost::new(app_state.clone());
    let view = tokio::task::spawn_blocking(move || host.storage_view())
        .await
        .map_err(|e| Error::Failed(Arc::new(format!("view join: {e}").into())))?
        .map_err(|e| Error::Failed(Arc::new(format!("storage view: {e}").into())))?;
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let disk = hopnet_storage::fragstore::scan_fragments(
        &fragments_dir,
        now_unix - grace_secs.unwrap_or(3600),
    )
        .map_err(|e| Error::Failed(Arc::new(format!("disk scan: {e}").into())))?;
    if disk.is_empty() {
        return Ok(serde_json::json!({
            "evicted": 0, "bytes_freed": 0,
            "used_bytes": used_bytes, "total_bytes": total_bytes,
            "reason": "no eligible on-disk fragments",
        }));
    }

    let member_ids: std::collections::HashSet<i32> =
        view.members.iter().map(|p| p.node_id).collect();
    let hashes: Vec<crate::types::Blake3Hash> = disk.iter().map(|(h, _)| *h).collect();

    let (info, holder_counts, pinned) = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
        let info = crate::db::fragments::lookup_disk_fragments(&conn, &hashes)
            .map_err(|e| Error::Failed(Arc::new(format!("fragment lookup: {e:?}").into())))?;
        let holder_counts =
            crate::db::fragments::member_holder_counts(&conn, &hashes, &member_ids, my_node_id)
                .map_err(|e| Error::Failed(Arc::new(format!("holder counts: {e:?}").into())))?;
        let pinned = hopnet_storage::pins::pinned_blob_ids(&conn)
            .map_err(|e| Error::Failed(Arc::new(format!("pins: {e}").into())))?;
        (info, holder_counts, pinned)
    };

    // Per-blob class assignment under the current view — responsibility is
    // computed, never stored.
    let mut assignments: std::collections::HashMap<String, Vec<i32>> = Default::default();
    let mut candidates = Vec::new();
    for (hash, size) in &disk {
        // Not in fragment_hashes = orphan; the orphan GC flow owns it.
        let Some(frag) = info.get(hash) else { continue };
        let assignment = assignments
            .entry(frag.blob_id.clone())
            .or_insert_with(|| {
                use std::str::FromStr;
                let seed = hopnet_storage::BlobId::from_str(&frag.blob_id)
                    .map(|id| hopnet_storage::placement::placement_seed(&id))
                    .unwrap_or([0u8; 32]);
                hopnet_storage::engine::assign_for_blob(
                    &seed,
                    view.members.clone(),
                    view.metrics.clone(),
                    &view.weights,
                )
                .1
            });
        candidates.push(EvictionCandidate {
            fragment_hash: *hash,
            blob_id: frag.blob_id.clone(),
            size_bytes: *size,
            responsible: assignment.get(frag.local_index as usize).copied()
                == Some(my_node_id),
            pinned: pinned.contains(&frag.blob_id),
            other_member_holders: holder_counts.get(hash).copied().unwrap_or(0),
        });
    }

    let planned = plan_evictions(candidates, &pressure);
    let mut bytes_freed = 0u64;
    let sizes: std::collections::HashMap<_, _> = disk.into_iter().collect();
    let mut deleted = Vec::new();
    for hash in &planned {
        match hopnet_storage::fragstore::delete_fragment(&fragments_dir, hash) {
            Ok(()) => {
                bytes_freed += sizes.get(hash).copied().unwrap_or(0);
                deleted.push(*hash);
            }
            Err(e) => tracing::warn!("eviction: delete {} failed: {e}", hash.to_hex()),
        }
    }
    if !deleted.is_empty() {
        let host = SubstrateHost::new(app_state.clone());
        host.mark_remote_batch(deleted.clone());
    }

    tracing::info!(
        "watermark eviction: {} fragments evicted, {} bytes freed ({}% used, high {}%, low {}%)",
        deleted.len(),
        bytes_freed,
        used_bytes * 100 / total_bytes.max(1),
        high_pct,
        low_pct
    );
    Ok(serde_json::json!({
        "evicted": deleted.len(), "bytes_freed": bytes_freed,
        "used_bytes": used_bytes, "total_bytes": total_bytes,
        "high_pct": high_pct, "low_pct": low_pct,
    }))
}

/// Last-seen storage view summary — INFO logging only on change.
static LAST_VIEW_SUMMARY: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
/// Day stamp of the last scrub slice (one slice per day, full walk weekly).
static LAST_SCRUB_DAY: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(-1);

pub async fn handle_storage_policy_tick(_job: TaskId, ctx: Data<AppState>) -> Result<(), Error> {
    run_storage_policy_tick(&ctx).await.map(|_| ())
}

/// The engine policy tick (RFC-STORAGE-001 Repair; RFC-STORAGE-002 S6):
/// view sync → repair scan (urgent below-watermark re-encodes + one lazy
/// hopeless re-encode, elected by lowest-missing-class) → one migration
/// pull → eviction check → daily scrub slice. Inventory attestation stays
/// with its own self-check cron.
pub async fn run_storage_policy_tick(app_state: &AppState) -> Result<serde_json::Value, Error> {
    use hopnet_storage::engine::{ReencodeCmd, assign_for_blob, reencode::repairer_for_chunk};
    use hopnet_storage::traits::StateReader;

    let my_node_id = app_state
        .get_node_id()
        .map_err(|_| Error::Failed(Arc::new("node id not set".to_string().into())))?;

    // (1) View sync — derived fresh each tick; INFO only on change.
    let host = SubstrateHost::new(app_state.clone());
    let view = tokio::task::spawn_blocking(move || host.storage_view())
        .await
        .map_err(|e| Error::Failed(Arc::new(format!("view join: {e}").into())))?
        .map_err(|e| Error::Failed(Arc::new(format!("storage view: {e}").into())))?;
    let mut member_ids: Vec<i32> = view.members.iter().map(|p| p.node_id).collect();
    member_ids.sort_unstable();
    let tiers_sorted: std::collections::BTreeMap<i32, i64> =
        view.tiers.iter().map(|(k, v)| (*k, *v)).collect();
    let summary = format!(
        "members={member_ids:?} online={} W={} tiers={tiers_sorted:?}",
        view.online.len(),
        view.watermark
    );
    {
        let mut last = LAST_VIEW_SUMMARY.lock().unwrap();
        if last.as_deref() != Some(summary.as_str()) {
            tracing::info!("storage view: {summary}");
            *last = Some(summary);
        }
    }

    // (2) Repair scan: this node re-encodes the chunks it was elected for.
    let settings = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
        crate::db::shared::read_storage_node_settings(&conn)
            .map_err(|e| Error::Failed(Arc::new(format!("settings: {e:?}").into())))?
    };
    let mut urgent_enqueued = 0usize;
    let mut lazy_enqueued = 0usize;
    if settings.reencode_enabled {
        let online: std::collections::HashSet<i32> = view.online.iter().copied().collect();
        let members: std::collections::HashSet<i32> = member_ids.iter().copied().collect();
        let candidates = {
            let conn = app_state
                .db_pool
                .get()
                .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
            crate::db::inventory::find_chunks_with_missing_classes(&conn, &online, &members)
                .map_err(|e| Error::Failed(Arc::new(format!("repair scan: {e:?}").into())))?
        };
        if let Some(engine) = app_state.storage.get() {
            let mut lazy_pick: Option<ReencodeCmd> = None;
            for cand in candidates {
                let missing: Vec<u32> = cand.missing.iter().map(|(c, _)| *c).collect();
                let seed = hopnet_storage::placement::placement_seed(&cand.blob_id);
                let (_, assignment) = assign_for_blob(
                    &seed,
                    view.members.clone(),
                    view.metrics.clone(),
                    &view.weights,
                );
                if repairer_for_chunk(&assignment, &missing) != Some(my_node_id) {
                    continue;
                }
                let urgent = cand.live_classes < view.watermark;
                let hopeless = cand
                    .missing
                    .iter()
                    .any(|(_, s)| *s == crate::db::inventory::MissingHolderState::Hopeless);
                let cmd = ReencodeCmd {
                    blob_id: cand.blob_id,
                    chunk_number: cand.chunk_number,
                    missing_classes: missing,
                };
                if urgent {
                    engine.enqueue_reencode(cmd, true);
                    urgent_enqueued += 1;
                } else if hopeless && lazy_pick.is_none() {
                    lazy_pick = Some(cmd);
                }
            }
            if let Some(cmd) = lazy_pick {
                engine.enqueue_reencode(cmd, false);
                lazy_enqueued = 1;
            }
        }
    }

    // (3) One migration pull per tick (per-class placement diff no-ops on
    // unmoved blobs).
    let migration_repaired = run_network_rebalancing(app_state, 1, 0)
        .await
        .map(|r| r.data_blocks_rebalanced)
        .unwrap_or(0);

    // (4) Eviction check (statvfs no-op below the high watermark).
    let eviction = run_watermark_eviction(app_state, None, None).await?;

    // (5) Daily scrub slice (full walk weekly): corrupt bytes are deleted
    // and un-attested; the next repair scan regenerates them.
    let day = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 86400) as i64;
    let mut scrubbed_corrupt = 0usize;
    if LAST_SCRUB_DAY.swap(day, std::sync::atomic::Ordering::SeqCst) != day {
        let fragments_dir = crate::storage_host::functions::get_fragments_dir()
            .map_err(|e| Error::Failed(Arc::new(format!("fragments dir: {e:?}").into())))?;
        let slice = (day % 7) as u8;
        let dir = fragments_dir.clone();
        let corrupted =
            tokio::task::spawn_blocking(move || {
                hopnet_storage::fragstore::verify_slice(&dir, slice, 7)
            })
            .await
            .map_err(|e| Error::Failed(Arc::new(format!("scrub join: {e}").into())))?
            .map_err(|e| Error::Failed(Arc::new(format!("scrub: {e}").into())))?;
        if !corrupted.is_empty() {
            tracing::warn!("scrub: {} corrupt fragments on slice {slice}", corrupted.len());
            for hash in &corrupted {
                let _ = hopnet_storage::fragstore::delete_fragment(&fragments_dir, hash);
            }
            use hopnet_storage::traits::LocalStateSink;
            SubstrateHost::new(app_state.clone()).mark_remote_batch(corrupted.clone());
            scrubbed_corrupt = corrupted.len();
        }
    }

    Ok(serde_json::json!({
        "members": member_ids,
        "online": view.online.len(),
        "watermark": view.watermark,
        "urgent_reencodes": urgent_enqueued,
        "lazy_reencodes": lazy_enqueued,
        "migration_repaired": migration_repaired,
        "eviction": eviction,
        "scrub_corrupt": scrubbed_corrupt,
    }))
}
