use crate::{
    AppState,
    db::{CustomUUID, fragments::find_orphaned_data_blocks},
    storage_host::substrate_host::SubstrateHost,
};
use apalis::prelude::*;
use hopnet_storage::traits::TxSubmitter;
use std::sync::Arc;

/// Orphaned data-block cleanup: the scheduled fire (RFC-STORAGE-003 S6 —
/// registered daily with per-node jitter in main.rs) and the manual
/// route's defaults. Blobs no reference provider claims, older than the
/// retention window, oldest first; at most `MAX_BATCHES` consensus
/// transactions per fire — the rest waits for the next one. Deletion
/// policy, not convergence: the retention window is the recovery window
/// for anything a projection dropped.
pub const ORPHAN_CLEANUP_BATCH_SIZE: i32 = 50;
pub const ORPHAN_CLEANUP_RETENTION_DAYS: i64 = 30;
pub const ORPHAN_CLEANUP_MAX_BATCHES: usize = 10;

/// The daily cron entry.
pub async fn handle_orphaned_data_block_cleanup(
    _job: TaskId,
    ctx: Data<AppState>,
) -> Result<(), Error> {
    run_orphaned_data_block_cleanup(
        &ctx,
        ORPHAN_CLEANUP_BATCH_SIZE,
        ORPHAN_CLEANUP_RETENTION_DAYS,
    )
    .await
    .map(|_| ())
}

/// Core cleanup logic shared by the cron and `POST /maintenance/cleanup-orphaned`.
/// The takeout gate here is the pre-flight; the apply re-checks it inside
/// the transaction (`db_apply::delete_orphaned_data_blocks_consensus`).
pub async fn run_orphaned_data_block_cleanup(
    app_state: &AppState,
    batch_size: i32,
    retention_days: i64,
) -> Result<usize, Error> {
    tracing::debug!("Starting orphaned data block cleanup");

    match crate::db::takeout::has_active_takeout(app_state.db_pool.get(), None) {
        Ok(true) => {
            let error_msg = "Cannot run orphaned data cleanup: active takeout(s) in progress. Wait for takeouts to expire or complete before running cleanup.";
            tracing::warn!("{}", error_msg);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
                error_msg,
            )))));
        }
        Ok(false) => {}
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

    cleanup_orphaned_data_blocks(app_state, batch_size, retention_days).await
}

async fn cleanup_orphaned_data_blocks(
    app_state: &AppState,
    batch_size: i32,
    retention_days: i64,
) -> Result<usize, Error> {
    let mut total_cleaned = 0;

    // Generate cutoff UUID for retention policy
    let cutoff_uuid = CustomUUID::retention_cutoff(retention_days);

    tracing::debug!(
        "Using {}-day retention policy, batch size: {}, cutoff UUID: {}",
        retention_days,
        batch_size,
        cutoff_uuid
    );

    // Storage-owned tx submission rides the TxSubmitter seam (sign + queue).
    let submitter = SubstrateHost::new(app_state.clone());

    for batch in 0..ORPHAN_CLEANUP_MAX_BATCHES {
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
            tracing::debug!("No more orphaned data blocks to clean");
            break;
        }
        if batch + 1 == ORPHAN_CLEANUP_MAX_BATCHES && data_block_ids.len() as i32 == batch_size {
            tracing::debug!("orphan cleanup batch cap reached; more may remain for the next fire");
        }

        tracing::debug!(
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
                tracing::debug!(
                    "Submitted consensus transaction to delete {} data blocks",
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

    if total_cleaned > 0 {
        tracing::info!("orphan cleanup: {total_cleaned} data blocks submitted for deletion");
    }
    Ok(total_cleaned)
}

/// The level-triggered obligation check (RFC-STORAGE-003 S3): re-kick up
/// to `max_data_blocks` in-flight blobs — goal not yet confirmed, oldest
/// goal first — through this node's reconciler, which pulls what it owes
/// under each goal, attests, and proposes confirmation when the evidence
/// is complete. Every kick is idempotent; the in-flight set is the
/// work-list and a blob leaves it at confirm, so no cursor exists to
/// starve. `min_age_heights` is accepted for the route's compatibility
/// and ignored: need is need.
pub async fn run_network_rebalancing(
    app_state: &AppState,
    max_data_blocks: i32,
    _min_age_heights: u64,
) -> Result<NetworkRebalancingResult, Error> {
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

    // Scoped checkout, dropped before the engine's data plane runs.
    let in_flight = {
        let conn = app_state.db_pool.get().map_err(|e| {
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Failed to get database connection: {:?}",
                e
            )))))
        })?;
        hopnet_storage::lifecycle::in_flight_blobs(&conn, max_data_blocks.max(0) as usize).map_err(
            |e| {
                Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                    "Failed to select in-flight blobs: {e}"
                )))))
            },
        )?
    };

    let Some(storage) = app_state.storage.get() else {
        return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
            "storage engine not running",
        )))));
    };

    let stats = storage.pull_blobs(in_flight).await;
    let result = NetworkRebalancingResult {
        consensus_height,
        data_blocks_checked: stats.checked,
        data_blocks_rebalanced: stats.confirms_proposed,
        data_blocks_failed: stats.failed,
        total_fragments_migrated: stats.pulled + stats.rebuilt,
    };
    if stats.checked > 0 {
        tracing::info!("In-flight pull check completed: {:?}", result);
    }
    Ok(result)
}

/// The fulfillment floor (RFC-STORAGE-003 S3): one batched
/// ConfirmPlacement for the in-flight blobs whose evidence is complete
/// right now. Apply validation re-checks on every node, so a stale read
/// here costs a skipped entry, never a wrong confirmation. Returns how
/// many confirmations were proposed.
pub async fn propose_ready_confirmations(
    app_state: &AppState,
    base_sample: usize,
) -> Result<usize, Error> {
    use std::sync::atomic::Ordering;
    // Adaptive sample (RFC-STORAGE-003 S4): doubles while at least half of
    // the sample was ready — the post-transition rubber-stamp balloon —
    // and resets to the base otherwise. Recurrence over a draining set is
    // what makes a random sample comprehensive.
    let sample_n = FULFILL_SAMPLE.load(Ordering::Relaxed).max(base_sample);
    let (ready, sampled): (Vec<hopnet_storage::PlacementConfirmation>, usize) = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
        let sample = hopnet_storage::lifecycle::in_flight_sample(&conn, sample_n)
            .map_err(|e| Error::Failed(Arc::new(format!("in-flight sample: {e}").into())))?;
        let sampled = sample.len();
        let tip = crate::db::consensus::get_current_consensus_height(&conn)
            .map_err(|e| Error::Failed(Arc::new(format!("height: {e:?}").into())))?;
        let mut out = Vec::new();
        for blob_id in sample {
            if let Some(height) = hopnet_storage::lifecycle::confirm_ready(&conn, &blob_id, tip)
                .map_err(|e| Error::Failed(Arc::new(format!("confirm read: {e}").into())))?
            {
                out.push(hopnet_storage::PlacementConfirmation { blob_id, height });
            }
        }
        (out, sampled)
    };
    let next = if sampled > 0 && ready.len() * 2 >= sampled {
        (sample_n * 2).min(hopnet_storage::engine::policy::CONFIRM_SAMPLE_MAX)
    } else {
        base_sample
    };
    FULFILL_SAMPLE.store(next, Ordering::Relaxed);
    if ready.is_empty() {
        return Ok(0);
    }
    let count = ready.len();
    let payload = hopnet_storage::ConfirmPlacement {
        confirmations: ready,
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|e| Error::Failed(Arc::new(format!("confirm encode: {e}").into())))?;
    SubstrateHost::new(app_state.clone())
        .submit(hopnet_storage::lifecycle::CONFIRM_TX_FN, encoded)
        .await
        .map_err(|e| Error::Failed(Arc::new(format!("confirm submit: {e:?}").into())))?;
    tracing::info!("fulfillment: proposed {count} confirmations");
    Ok(count)
}

#[derive(Debug, Default, serde::Serialize)]
pub struct NetworkRebalancingResult {
    pub consensus_height: u64,
    pub data_blocks_checked: usize,
    pub data_blocks_rebalanced: usize,
    pub data_blocks_failed: usize,
    pub total_fragments_migrated: usize,
}

/// Operator re-kick (RFC-STORAGE-003 S3): wake this node's reconciler for
/// up to `limit` in-flight blobs, oldest goal first — the same check the
/// policy tick runs, on demand. Survives as a manual re-kick during the
/// cutover drain and retires once the reconciler owns the full lifecycle
/// (S4); its selection query IS the reconciler's own.
///
/// FIRE-AND-FORGET. `notify_blob_committed` is a non-blocking send, so
/// this returns once the ids are enqueued — NOT once they are pulled or
/// confirmed. Confirm by re-reading `unplaced_total` on a later call.
/// Re-running is safe: every kick is idempotent.
pub async fn run_unplaced_drain(
    app_state: &AppState,
    limit: i32,
) -> Result<UnplacedDrainResult, Error> {
    tracing::info!("Starting in-flight re-kick (limit {})", limit);

    // Scoped checkout, dropped before the engine is touched — the data plane
    // must never run while this task holds a pool connection.
    let (blob_ids, unplaced_total) = {
        let conn = app_state.db_pool.get().map_err(|e| {
            tracing::error!("Failed to get database connection for unplaced drain: {e:?}");
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Failed to get database connection: {e:?}"
            )))))
        })?;
        let total = hopnet_storage::store::count_unplaced_blobs(&conn).map_err(|e| {
            tracing::error!("Failed to count unplaced blobs: {e:?}");
            Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                "Failed to count unplaced blobs: {e:?}"
            )))))
        })?;
        let ids = hopnet_storage::lifecycle::in_flight_blobs(&conn, limit.max(0) as usize)
            .map_err(|e| {
                tracing::error!("Failed to select in-flight blobs: {e}");
                Error::Failed(Arc::new(Box::new(std::io::Error::other(format!(
                    "Failed to select in-flight blobs: {e}"
                )))))
            })?;
        (ids, total)
    };

    let Some(storage) = app_state.storage.get() else {
        return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
            "storage engine not running",
        )))));
    };

    let enqueued = blob_ids.len();
    for blob_id in blob_ids {
        storage.notify_blob_committed(blob_id);
    }

    let result = UnplacedDrainResult {
        unplaced_total,
        enqueued,
        limit,
    };
    tracing::info!(
        "Unplaced drain enqueued {} of {} stranded blobs (placement is asynchronous; \
         re-read unplaced_total to confirm progress)",
        result.enqueued,
        result.unplaced_total
    );
    Ok(result)
}

#[derive(Debug, Default, serde::Serialize)]
pub struct UnplacedDrainResult {
    /// Every blob stuck unplaced, ignoring `limit` — the remaining backlog.
    pub unplaced_total: i64,
    /// How many were kicked onto the distribution channel this pass.
    pub enqueued: usize,
    /// The bound applied to this pass.
    pub limit: i32,
}

/// Scheduled job handler for the disk-truth sweep + self-check (the
/// 20–30 minute cron): every self-check is disk-backed (RFC-STORAGE-003 S5).
pub async fn handle_fragment_inventory_self_check(
    job: TaskId,
    ctx: Data<AppState>,
) -> Result<(), Error> {
    run_disk_truth_sweep(&ctx, SWEEP_ORPHAN_GRACE_SECS)
        .await
        .map(|_| ())
}

/// Orphan grace: a rowless file younger than this is an in-flight store,
/// not an orphan.
pub const SWEEP_ORPHAN_GRACE_SECS: u64 = 3600;
/// Day stamp of the last scrub slice (one slice per day, full walk weekly),
/// now ridden by the sweep's walk.
static LAST_SCRUB_DAY: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(-1);

/// The disk-truth sweep (RFC-STORAGE-003 S5) — discharges `invView' =
/// copies`, repairing the record, never the data. One readdir walk:
///   1. diff disk against `fragment_hashes` and repair `stored_locally`
///      both ways (awaited marks);
///   2. delete orphan files older than the grace period (the former
///      two-call scan/delete API, folded in);
///   3. on the day's turn, verify one weekly scrub slice's content on the
///      same listing — corrupt bytes are deleted and un-marked;
///   4. run the self-check differential (now reading repaired flags) and
///      submit it — belief for what we hold;
///   5. submit `attest_fragments` for every file with a row — disk truth,
///      stamped with the current height.
///
/// The report is kept for the operator route. Belief is dishonest for at
/// most one cycle of this job.
pub async fn run_disk_truth_sweep(
    app_state: &AppState,
    orphan_grace_secs: u64,
) -> Result<hopnet_storage::sweep::SweepReport, Error> {
    use hopnet_storage::traits::LocalStateSink;

    let node_id = app_state
        .get_node_id()
        .map_err(|_| Error::Failed(Arc::new("node id not set".to_string().into())))?;
    let fragments_dir = app_state.fragments_dir.clone();
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // (1) The walk (blocking IO off the async thread) and the table.
    let dir = fragments_dir.clone();
    let listing = tokio::task::spawn_blocking(move || {
        hopnet_storage::fragstore::scan_fragments_detailed(&dir)
    })
    .await
    .map_err(|e| Error::Failed(Arc::new(format!("sweep join: {e}").into())))?
    .map_err(|e| Error::Failed(Arc::new(format!("sweep walk: {e}").into())))?;
    let rows = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
        crate::db::fragments::all_fragment_flags(&conn)
            .map_err(|e| Error::Failed(Arc::new(format!("fragment flags: {e:?}").into())))?
    };
    let diff =
        hopnet_storage::sweep::diff(&listing, &rows, now_unix.saturating_sub(orphan_grace_secs));

    // Repair the flag both ways.
    let host = SubstrateHost::new(app_state.clone());
    if !diff.present_unflagged.is_empty() {
        tracing::info!(
            "sweep: {} fragments on disk but unflagged — re-flagging",
            diff.present_unflagged.len()
        );
        for hash in &diff.present_unflagged {
            host.mark_local(*hash).await;
        }
    }
    if !diff.flagged_missing.is_empty() {
        tracing::warn!(
            "sweep: {} fragments flagged but gone from disk — un-flagging",
            diff.flagged_missing.len()
        );
        host.mark_remote_batch(diff.flagged_missing.clone()).await;
    }

    // (2) Orphans past grace.
    let mut orphans_deleted = 0usize;
    let mut orphan_bytes_freed = 0u64;
    for (hash, size) in &diff.orphans {
        match hopnet_storage::fragstore::delete_fragment(&fragments_dir, hash) {
            Ok(()) => {
                orphans_deleted += 1;
                orphan_bytes_freed += size;
            }
            Err(e) => tracing::warn!("sweep: delete orphan {} failed: {e}", hash.to_hex()),
        }
    }

    // (3) The weekly scrub slice shares the walk.
    let day = (now_unix / 86400) as i64;
    let mut corrupt_deleted = 0usize;
    let mut present = diff.present.clone();
    if LAST_SCRUB_DAY.swap(day, std::sync::atomic::Ordering::SeqCst) != day {
        let slice = (day % 7) as u8;
        let dir = fragments_dir.clone();
        let listing_for_scrub = listing.clone();
        let corrupted = tokio::task::spawn_blocking(move || {
            hopnet_storage::fragstore::verify_listing(&dir, &listing_for_scrub, slice, 7)
        })
        .await
        .map_err(|e| Error::Failed(Arc::new(format!("scrub join: {e}").into())))?;
        if !corrupted.is_empty() {
            tracing::warn!(
                "scrub: {} corrupt fragments on slice {slice}",
                corrupted.len()
            );
            for hash in &corrupted {
                let _ = hopnet_storage::fragstore::delete_fragment(&fragments_dir, hash);
            }
            host.mark_remote_batch(corrupted.clone()).await;
            let gone: std::collections::HashSet<_> = corrupted.iter().collect();
            present.retain(|h| !gone.contains(h));
            corrupt_deleted = corrupted.len();
        }
    }

    // (4) Belief: the differential over repaired flags.
    let differential =
        crate::db::inventory::compute_inventory_differential(app_state.db_pool.get(), node_id)
            .map_err(|e| {
                Error::Failed(Arc::new(format!("inventory differential: {e:?}").into()))
            })?;
    if !differential.is_empty() {
        let payload = bincode::serde::encode_to_vec(&differential, bincode::config::standard())
            .map_err(|e| Error::Failed(Arc::new(format!("self-check encode: {e}").into())))?;
        host.submit(hopnet_storage::engine::policy::SELF_CHECK_FN, payload)
            .await
            .map_err(|e| Error::Failed(Arc::new(format!("self-check submit: {e:?}").into())))?;
    }

    // (5) Truth: attest everything seen on disk this cycle.
    let mut attested = false;
    if !present.is_empty() {
        let height = {
            let conn = app_state
                .db_pool
                .get()
                .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
            crate::db::consensus::get_current_consensus_height(&conn)
                .map_err(|e| Error::Failed(Arc::new(format!("height: {e:?}").into())))?
        };
        let attestation = hopnet_storage::FragmentAttestation {
            node_id,
            height,
            present: present.clone(),
            suspect: Vec::new(),
        };
        let payload = bincode::serde::encode_to_vec(&attestation, bincode::config::standard())
            .map_err(|e| Error::Failed(Arc::new(format!("attestation encode: {e}").into())))?;
        host.submit(hopnet_storage::engine::policy::ATTEST_FN, payload)
            .await
            .map_err(|e| Error::Failed(Arc::new(format!("attestation submit: {e:?}").into())))?;
        attested = true;
    }

    let report = hopnet_storage::sweep::SweepReport {
        swept_at: now_unix as i64,
        files_on_disk: listing.len(),
        present: present.len(),
        reflagged: diff.present_unflagged.len(),
        unflagged: diff.flagged_missing.len(),
        orphans_deleted,
        orphan_bytes_freed,
        young_orphans: diff.young_orphans,
        corrupt_deleted,
        attested,
    };
    tracing::info!(
        "sweep: {} files, {} present, {} re-flagged, {} un-flagged, {} orphans deleted, {} corrupt deleted",
        report.files_on_disk,
        report.present,
        report.reflagged,
        report.unflagged,
        report.orphans_deleted,
        report.corrupt_deleted
    );
    *app_state.last_sweep.lock().unwrap() = Some(report.clone());
    Ok(report)
}

/// Kept for callers that only want belief refreshed (tests, routes): the
/// full sweep is the self-check now.
pub async fn run_fragment_inventory_self_check(app_state: &AppState) -> Result<(), Error> {
    run_disk_truth_sweep(app_state, SWEEP_ORPHAN_GRACE_SECS)
        .await
        .map(|_| ())
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
        Ok::<_, std::io::Error>((
            stats.total_space(),
            stats.total_space() - stats.available_space(),
        ))
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

    let (info, holder_counts, pinned, protection) = {
        use std::str::FromStr;
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

        // The protection predicate per blob (RFC-STORAGE-003 S2): the
        // confirmed epoch plus every in-flight epoch, from agreed state
        // only — never the current view. An unanswerable record protects.
        let blob_ids: Vec<hopnet_storage::BlobId> = {
            let mut ids: Vec<String> = info.values().map(|f| f.blob_id.clone()).collect();
            ids.sort_unstable();
            ids.dedup();
            ids.iter()
                .filter_map(|s| hopnet_storage::BlobId::from_str(s).ok())
                .collect()
        };
        let pairs = hopnet_storage::lifecycle::placement_pairs(&conn, &blob_ids)
            .map_err(|e| Error::Failed(Arc::new(format!("placement pairs: {e}").into())))?;
        let mut protection: std::collections::HashMap<
            String,
            hopnet_storage::protection::Protection,
        > = Default::default();
        for blob_id in &blob_ids {
            let verdict =
                match pairs.get(blob_id) {
                    Some((placed, desired)) => hopnet_storage::lifecycle::epochs_for_blob(
                        &conn, blob_id, *placed, *desired,
                    )
                    .map_err(|e| Error::Failed(Arc::new(format!("protection epochs: {e}").into())))?
                    .map(|epochs| hopnet_storage::protection::Protection::from_epochs(&epochs))
                    .unwrap_or_else(hopnet_storage::protection::Protection::unknown),
                    None => hopnet_storage::protection::Protection::unknown(),
                };
            protection.insert(blob_id.to_string(), verdict);
        }
        (info, holder_counts, pinned, protection)
    };

    let mut candidates = Vec::new();
    for (hash, size) in &disk {
        // Not in fragment_hashes = orphan; the orphan GC flow owns it.
        let Some(frag) = info.get(hash) else { continue };
        let protected = protection
            .get(&frag.blob_id)
            .map(|p| p.protects(frag.local_index, my_node_id, pinned.contains(&frag.blob_id)))
            .unwrap_or(true);
        candidates.push(EvictionCandidate {
            fragment_hash: *hash,
            blob_id: frag.blob_id.clone(),
            size_bytes: *size,
            protected,
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
        host.mark_remote_batch(deleted.clone()).await;
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
/// The fulfillment pass's current sample size (adaptive; see
/// `propose_ready_confirmations`).
static FULFILL_SAMPLE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub async fn handle_storage_policy_tick(_job: TaskId, ctx: Data<AppState>) -> Result<(), Error> {
    run_storage_policy_tick(&ctx).await.map(|_| ())
}

/// One policy tick's tally (RFC-STORAGE-003 S7): what the rungs found and
/// did. Kept in `AppState.last_tick` so the pane can show the re-encode
/// backlog the tick's own scan measured, instead of rescanning.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PolicyTickReport {
    /// Unix seconds when the tick ran.
    pub at: i64,
    pub members: Vec<i32>,
    pub online: usize,
    pub watermark: usize,
    /// Chunks below the watermark with a class this node owes a rebuild
    /// of (all enqueued, urgently).
    pub urgent_chunks_owed: usize,
    /// Chunks at or above the watermark with a class this node owes a
    /// rebuild of — one is picked per tick.
    pub lazy_chunks_owed: usize,
    pub urgent_reencodes: usize,
    pub lazy_reencodes: usize,
    pub migration_repaired: usize,
    pub confirms_proposed: usize,
    pub grace_declared: usize,
    pub eviction: serde_json::Value,
}

/// The engine policy tick (RFC-STORAGE-001 Repair; RFC-STORAGE-002 S6):
/// view sync → the obligation check's re-encode half (ladder + deputy
/// under the goal assignment) → grace rung → in-flight re-kick →
/// fulfillment → eviction check. Disk truth (the sweep, the scrub slice,
/// belief and attestation) rides the self-check cron.
pub async fn run_storage_policy_tick(app_state: &AppState) -> Result<PolicyTickReport, Error> {
    use hopnet_storage::engine::ReencodeCmd;
    use hopnet_storage::reconcile::{self, ChunkState, ClassState, Duty};
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

    // (2) The obligation check's re-encode half (RFC-STORAGE-003 S4): for
    // every chunk with a dead class, the reconciler's ladder under the
    // blob's GOAL assignment says what THIS node owes — the responsible of
    // a dead class rebuilds it once ready (below the watermark, or no
    // holder is merely asleep inside its tier); below the watermark the
    // deputy rule has the lowest live class's responsible cover a down
    // responsible. Urgent items preempt pulls; one lazy pick per tick.
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
    let mut lazy_owed = 0usize;
    if settings.reencode_enabled {
        let online: std::collections::HashSet<i32> = view.online.iter().copied().collect();
        let members: std::collections::HashSet<i32> = member_ids.iter().copied().collect();
        let (candidates, goals) = {
            let conn = app_state
                .db_pool
                .get()
                .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
            let candidates =
                crate::db::inventory::find_chunks_with_missing_classes(&conn, &online, &members)
                    .map_err(|e| Error::Failed(Arc::new(format!("repair scan: {e:?}").into())))?;
            // Goal assignments, memoized per blob (many chunks share one).
            let mut goals: std::collections::HashMap<hopnet_storage::BlobId, Option<Vec<i32>>> =
                Default::default();
            for cand in &candidates {
                if !goals.contains_key(&cand.blob_id) {
                    let assignment = hopnet_storage::lifecycle::pull_target(&conn, &cand.blob_id)
                        .map_err(|e| Error::Failed(Arc::new(format!("pull target: {e}").into())))?
                        .map(|t| t.assignment);
                    goals.insert(cand.blob_id.clone(), assignment);
                }
            }
            (candidates, goals)
        };
        if let Some(engine) = app_state.storage.get() {
            let up: std::collections::BTreeSet<i32> = online.iter().copied().collect();
            let mut lazy_pick: Option<ReencodeCmd> = None;
            for cand in candidates {
                // No goal on record: nothing is owed until the record
                // reaches it (the staleness pass will re-goal it).
                let Some(Some(assignment)) = goals.get(&cand.blob_id) else {
                    continue;
                };
                let hopeful_down: std::collections::BTreeSet<i32> = cand
                    .classes
                    .iter()
                    .flat_map(|(_, holders)| holders.iter().copied())
                    .filter(|n| !online.contains(n) && members.contains(n))
                    .collect();
                let chunk = ChunkState {
                    classes: cand
                        .classes
                        .iter()
                        .map(|(class, holders)| ClassState {
                            holders: holders.iter().copied().collect(),
                            responsible: assignment.get(*class as usize).copied().unwrap_or(-1),
                        })
                        .collect(),
                    up: up.clone(),
                    hopeful_down,
                    k: hopnet_storage::rs::ORIGINAL_FRAGMENTS_PER_CHUNK,
                    watermark: view.watermark,
                };
                let mut owed: Vec<u32> = reconcile::plan(&chunk, my_node_id)
                    .into_iter()
                    .filter_map(|d| match d {
                        Duty::Reencode { classes } => Some(classes),
                        Duty::Pull { .. } => None,
                    })
                    .flatten()
                    .collect();
                if let Some(deputy) = reconcile::deputy(&chunk, my_node_id) {
                    owed.extend(deputy);
                }
                owed.sort_unstable();
                owed.dedup();
                if owed.is_empty() {
                    continue;
                }
                let urgent = cand.live_classes < view.watermark;
                let cmd = ReencodeCmd {
                    blob_id: cand.blob_id,
                    chunk_number: cand.chunk_number,
                    missing_classes: owed,
                };
                if urgent {
                    engine.enqueue_reencode(cmd, true);
                    urgent_enqueued += 1;
                } else {
                    lazy_owed += 1;
                    if lazy_pick.is_none() {
                        lazy_pick = Some(cmd);
                    }
                }
            }
            if let Some(cmd) = lazy_pick {
                engine.enqueue_reencode(cmd, false);
                lazy_enqueued = 1;
            }
        }
    }

    // (2b) The staleness pass's grace rung (S4): if no proposer has run
    // the `desired < T` check within the grace window, declare a page
    // directly so convergence never rests on a sibling's heartbeat.
    let grace_declared = crate::storage_host::staleness::grace_rung(app_state)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!("staleness grace rung failed: {e}");
            0
        });

    // (3) The obligation check (RFC-STORAGE-003 S3): re-kick a bounded page
    // of in-flight blobs through the reconciler — pulls owed under each
    // goal, prompt attestation, confirm proposals where the evidence is
    // complete. Level-triggered: any blob a kick missed is found here.
    let migration_repaired = run_network_rebalancing(
        app_state,
        hopnet_storage::engine::policy::PULL_KICKS_PER_TICK as i32,
        0,
    )
    .await
    .map(|r| r.data_blocks_rebalanced)
    .unwrap_or(0);

    // (3b) Fulfillment floor: propose confirmation for in-flight blobs
    // whose evidence is already complete (other nodes' pulls finished
    // after our own check). One batched ConfirmPlacement per tick.
    let confirms_proposed = propose_ready_confirmations(
        app_state,
        hopnet_storage::engine::policy::CONFIRM_CHECKS_PER_TICK,
    )
    .await
    .unwrap_or(0);

    // (4) Eviction check (statvfs no-op below the high watermark).
    let eviction = run_watermark_eviction(app_state, None, None).await?;

    let report = PolicyTickReport {
        at: chrono::Utc::now().timestamp(),
        members: member_ids,
        online: view.online.len(),
        watermark: view.watermark,
        urgent_chunks_owed: urgent_enqueued,
        lazy_chunks_owed: lazy_owed,
        urgent_reencodes: urgent_enqueued,
        lazy_reencodes: lazy_enqueued,
        migration_repaired,
        confirms_proposed,
        grace_declared,
        eviction,
    };
    *app_state.last_tick.lock().unwrap() = Some(report.clone());
    Ok(report)
}
