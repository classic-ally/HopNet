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

/// The operator's AWAITED obligation check (`POST /maintenance/rebalance-network`,
/// RFC-STORAGE-003 S3): run up to `max_data_blocks` in-flight blobs
/// through this node's reconciler, in the pull planner's order (blobs
/// this node owes, at-risk first), and report what it pulled. The planner
/// itself feeds the worker continuously; this is the operator's awaited
/// drain over the same plan. `min_age_heights` is accepted for the
/// route's compatibility and ignored: need is need.
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

    // One planning pass on the blocking pool (a fresh connection per
    // page), dropped before the engine's data plane runs.
    let in_flight: Vec<hopnet_storage::BlobId> = {
        let app_state = app_state.clone();
        tokio::task::spawn_blocking(move || crate::storage_host::pull_planner::plan(&app_state))
            .await
            .map_err(|e| Error::Failed(Arc::new(format!("plan join: {e}").into())))?
            .map_err(|e| Error::Failed(Arc::new(format!("plan: {e}").into())))?
            .0
            .into_iter()
            .take(max_data_blocks.max(0) as usize)
            .map(|item| item.blob_id)
            .collect()
    };

    let Some(storage) = app_state.storage.get() else {
        return Err(Error::Failed(Arc::new(Box::new(std::io::Error::other(
            "storage engine not running",
        )))));
    };

    let stats = storage.pull_blobs(in_flight, REBALANCE_DEADLINE).await;
    let result = rebalancing_result(consensus_height, &stats);
    if stats.held_back {
        tracing::info!("In-flight pull check skipped: this node is below its pull floor");
    }
    if stats.checked > 0 {
        tracing::info!("In-flight pull check completed: {:?}", result);
    }
    Ok(result)
}

/// The fulfillment pass (RFC-STORAGE-003 S4): the bulk confirmation path.
/// Up to `CONFIRM_ROUNDS_PER_TICK` rounds per tick; each samples the
/// in-flight set, proposes one batched ConfirmPlacement for the entries
/// whose evidence is complete, and awaits its commit. The sample doubles
/// between rounds while at least half of it was ready (the
/// post-transition rubber-stamp balloon) and the pass rests as soon as a
/// round comes back sparse — a sparse sample means the remaining work is
/// pulls, which the worker owns. Apply validation re-checks on every node,
/// so a stale read here costs a skipped entry, never a wrong confirmation.
/// Returns how many confirmations were proposed across the rounds.
pub async fn propose_ready_confirmations(
    app_state: &AppState,
    base_sample: usize,
) -> Result<usize, Error> {
    use hopnet_storage::engine::policy::{CONFIRM_ROUNDS_PER_TICK, next_fulfillment_sample};
    let mut sample_n = base_sample;
    let mut proposed = 0usize;
    for round in 0..CONFIRM_ROUNDS_PER_TICK {
        let (ready, sampled) = {
            // One evidence probe per sampled blob — blocking-pool work.
            let pool = app_state.db_pool.clone();
            tokio::task::spawn_blocking(move || {
                let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
                let tip = crate::db::consensus::get_current_consensus_height(&conn)
                    .map_err(|e| format!("height: {e:?}"))?;
                hopnet_storage::lifecycle::ready_confirmations(&conn, sample_n, tip)
                    .map_err(|e| format!("fulfillment read: {e}"))
            })
            .await
            .map_err(|e| Error::Failed(Arc::new(format!("fulfillment join: {e}").into())))?
            .map_err(|e| Error::Failed(Arc::new(e.into())))?
        };
        let count = ready.len();
        let next = next_fulfillment_sample(sample_n, sampled, count);
        if count > 0 {
            let payload = hopnet_storage::ConfirmPlacement {
                confirmations: ready,
            };
            let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
                .map_err(|e| Error::Failed(Arc::new(format!("confirm encode: {e}").into())))?;
            SubstrateHost::new(app_state.clone())
                .submit(hopnet_storage::lifecycle::CONFIRM_TX_FN, encoded)
                .await
                .map_err(|e| Error::Failed(Arc::new(format!("confirm submit: {e:?}").into())))?;
            proposed += count;
            tracing::info!(
                "fulfillment: round {round} proposed {count} of {sampled} sampled confirmations"
            );
        }
        match next {
            Some(n) => sample_n = n,
            None => break,
        }
    }
    Ok(proposed)
}

#[derive(Debug, Default, serde::Serialize)]
pub struct NetworkRebalancingResult {
    pub consensus_height: u64,
    pub data_blocks_checked: usize,
    pub data_blocks_rebalanced: usize,
    pub data_blocks_failed: usize,
    pub total_fragments_migrated: usize,
    /// Space held this batch back: the node was below its pull floor at
    /// entry (nothing was checked), or classes were held for space during
    /// it (`fragments_held_for_space`).
    pub held_for_space: bool,
    /// Owed classes held back by the pull floor during the batch.
    pub fragments_held_for_space: usize,
    /// Blobs still unanswered at `REBALANCE_DEADLINE` (they stay queued).
    pub data_blocks_timed_out: usize,
}

/// The re-kick's answer from the engine's batch stats.
fn rebalancing_result(
    consensus_height: u64,
    stats: &hopnet_storage::engine::PullStats,
) -> NetworkRebalancingResult {
    NetworkRebalancingResult {
        consensus_height,
        data_blocks_checked: stats.checked,
        data_blocks_rebalanced: stats.evidence_queued,
        data_blocks_failed: stats.failed,
        total_fragments_migrated: stats.pulled + stats.rebuilt,
        held_for_space: stats.held_back || stats.held_for_space > 0,
        fragments_held_for_space: stats.held_for_space,
        data_blocks_timed_out: stats.timed_out,
    }
}

/// How long the operator re-kick waits on the engine before answering.
pub const REBALANCE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(600);

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

/// Orphan grace: a rowless file younger than this is an in-flight store,
/// not an orphan.
pub const SWEEP_ORPHAN_GRACE_SECS: u64 = 3600;

/// How long the sweep holds this node's own upload whose `fragment_hashes`
/// row has not landed (`hopnet_storage_local_uploads`). Past it the upload
/// is given up on and its files are ordinary orphans. Long enough for any
/// transaction that is going to land — a straggler's rows arrive with its
/// join, within hours — and bounded so retried uploads under fresh blob
/// ids cannot grow the ledger forever. 14 days.
pub const LOCAL_UPLOAD_RETENTION_SECS: u64 = 14 * 86_400;

/// `LOCAL_UPLOAD_RETENTION_SECS`, overridden by
/// `HOPNET_STORAGE_LOCAL_UPLOAD_RETENTION_SECS`, read once.
fn local_upload_retention() -> u64 {
    static RETENTION: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *RETENTION.get_or_init(|| {
        std::env::var("HOPNET_STORAGE_LOCAL_UPLOAD_RETENTION_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(LOCAL_UPLOAD_RETENTION_SECS)
    })
}

/// One rotation of the rolling sweep visits every shard in about this
/// long, paced per shard; a slow disk stretches the rotation, it never
/// bursts. At drain traffic (~45 heights a minute) an hour is ~2,700
/// heights, well inside `ATTESTATION_RECENCY_HEIGHTS` (8192).
pub const ROTATION_TARGET_SECS: u64 = 3600;

/// A buffered belief or attestation page is submitted part-full once its
/// oldest hash has waited this long: the bound on how long a fragment the
/// walk saw waits for its stamp. One clock with the pull path's evidence
/// lane, so sweep and pull evidence age out of their buffers alike.
pub const MAX_BUFFER_AGE_SECS: u64 = hopnet_storage::engine::policy::EVIDENCE_MAX_AGE_SECS;

/// Shards whose content was scrubbed today: `(day, flags)`. Each shard is
/// read once on its day of seven (`sweep::scrub_due`); in memory, as the
/// single scrub-day flag was before the rolling sweep.
static SCRUBBED: std::sync::Mutex<(i64, [bool; 256])> = std::sync::Mutex::new((-1, [false; 256]));

/// Claim `shard`'s scrub for `day`: true exactly once per shard per day.
fn claim_scrub(day: i64, shard: u8) -> bool {
    let mut scrubbed = SCRUBBED.lock().unwrap_or_else(|p| p.into_inner());
    if scrubbed.0 != day {
        *scrubbed = (day, [false; 256]);
    }
    !std::mem::replace(&mut scrubbed.1[usize::from(shard)], true)
}

/// The walker and the operator's full rotation never sweep a shard at the
/// same time. Taken per shard, so a requested rotation interleaves with
/// the walker instead of waiting out its hour.
static SWEEP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn current_height(app_state: &AppState) -> Result<u64, Error> {
    let conn = app_state
        .db_pool
        .get()
        .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
    crate::db::consensus::get_current_consensus_height(&conn)
        .map_err(|e| Error::Failed(Arc::new(format!("height: {e:?}").into())))
}

/// One rotation's working state: the running report and the page buffers.
pub(crate) struct Rotation {
    node_id: i32,
    report: hopnet_storage::sweep::SweepReport,
    belief: hopnet_storage::sweep::BeliefBuffer,
    truth: hopnet_storage::sweep::PageBuffer,
    /// The rotation's prompt surplus release budget.
    surplus_left: usize,
    /// The current shard step's timings, filled step by step so a failed
    /// shard still reports the steps it got through.
    shard: hopnet_storage::sweep::ShardTimings,
    /// Shards since the last progress line, and when the last INFO one was
    /// logged.
    progress_shards: usize,
    progress_at: std::time::Instant,
}

impl Rotation {
    pub(crate) fn new(node_id: i32, started_unix: u64) -> Self {
        Rotation {
            node_id,
            report: hopnet_storage::sweep::SweepReport {
                swept_at: started_unix as i64,
                ..Default::default()
            },
            belief: Default::default(),
            truth: Default::default(),
            surplus_left: SURPLUS_RELEASE_MAX_PER_SWEEP,
            shard: Default::default(),
            progress_shards: 0,
            progress_at: std::time::Instant::now(),
        }
    }
}

/// Milliseconds since `since`.
fn ms_since(since: std::time::Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// The rolling disk-truth sweep (RFC-STORAGE-003 S5) — discharges
/// `invView' = copies`, repairing the record, never the data. Runs for
/// the node's lifetime, one shard (the hash's first byte, one first-level
/// directory of the store) per step, paced so a rotation over all 256
/// takes `ROTATION_TARGET_SECS`. The cursor is persisted after every
/// step that leaves the buffers drained, so a restart resumes at the
/// first shard whose belief and truth had not yet gone out (at most a
/// flush age of shards repeats). Belief is dishonest for at most one
/// rotation.
pub async fn run_rolling_sweep(app_state: AppState) {
    let per_shard = std::time::Duration::from_secs(ROTATION_TARGET_SECS)
        / hopnet_storage::sweep::SHARD_COUNT as u32;
    let host = SubstrateHost::new(app_state.clone());
    let mut state: Option<(hopnet_storage::sweep::SweepCursor, Rotation)> = None;
    loop {
        let step_started = std::time::Instant::now();
        let (cursor, rotation) = match &mut state {
            Some(s) => s,
            None => match start_walker(&app_state) {
                Ok(s) => state.insert(s),
                Err(e) => {
                    tracing::debug!("sweep: not ready: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }
            },
        };

        {
            let _guard = SWEEP_LOCK.lock().await;
            if let Err(e) = sweep_shard(
                &app_state,
                &host,
                rotation,
                cursor.next_shard,
                SWEEP_ORPHAN_GRACE_SECS,
            )
            .await
            {
                tracing::warn!("sweep: shard {:02x} failed: {e}", cursor.next_shard);
                rotation.report.failed_shards += 1;
            }
        }
        let flushed = flush_buffers(&host, rotation, false).await;
        let work_ms = ms_since(step_started);
        rotation.report.ms_work += work_ms;
        log_shard_timings(cursor.next_shard, &rotation.shard, &flushed, work_ms);

        let height = current_height(&app_state).unwrap_or(cursor.started_height);
        let (next, completed) = cursor.advance(unix_now(), height);
        rotation.progress_shards += 1;
        let due = hopnet_storage::sweep::progress_due(
            rotation.progress_shards,
            rotation.progress_at.elapsed().as_secs(),
        );
        if let (false, Some(level)) = (completed, due) {
            let (rot, shard) = (cursor.rotation, cursor.next_shard);
            let summary = rotation.report.timing_summary();
            if level == hopnet_storage::sweep::ProgressLine::Info {
                tracing::info!("sweep: rotation {rot} progress at shard {shard:02x}: {summary}");
                rotation.progress_at = std::time::Instant::now();
            } else {
                tracing::debug!("sweep: rotation {rot} progress at shard {shard:02x}: {summary}");
            }
            rotation.progress_shards = 0;
        }
        if completed {
            flush_buffers(&host, rotation, true).await;
            let fresh = Rotation::new(rotation.node_id, next.started_unix);
            let done = std::mem::replace(rotation, fresh);
            finish_rotation(&app_state, done, cursor.started_height);
        }
        *cursor = next;
        if let Some(durable) = cursor_to_persist(rotation, next)
            && let Err(e) = app_state
                .db_pool
                .get()
                .map_err(|e| e.to_string())
                .and_then(|conn| {
                    hopnet_storage::store::write_sweep_cursor(&conn, &durable)
                        .map_err(|e| e.to_string())
                })
        {
            tracing::warn!("sweep: cursor not saved: {e}");
        }
        *app_state.sweep_progress.lock().unwrap() = Some(hopnet_storage::sweep::SweepProgress {
            rotation: cursor.rotation,
            next_shard: cursor.next_shard,
            started_unix: cursor.started_unix,
            started_height: cursor.started_height,
            avg_ms_per_shard: rotation.report.avg_ms_per_shard(),
            report: rotation.report.clone(),
        });

        let slept = std::time::Instant::now();
        tokio::time::sleep(per_shard.saturating_sub(step_started.elapsed())).await;
        rotation.report.ms_sleep += ms_since(slept);
    }
}

/// The per-shard DEBUG line: each step's milliseconds, the walk's file
/// count, whether the shard was scrubbed, and the flush that followed.
fn log_shard_timings(
    shard: u8,
    t: &hopnet_storage::sweep::ShardTimings,
    flushed: &hopnet_storage::sweep::FlushTimings,
    work_ms: u64,
) {
    tracing::debug!(
        "sweep: shard {shard:02x} timings: {} files, {work_ms} ms work; ms: height={} walk={} db={} diff={} mark={} temps={} orphans={} scrub={} (scrubbed={}, {} files, {} bytes) release={} buffer={} flush={} ({} pages, {} ok, {} failed, slowest {})",
        t.files,
        t.ms_height,
        t.ms_walk,
        t.ms_db,
        t.ms_diff,
        t.ms_mark,
        t.ms_temps,
        t.ms_orphans,
        t.ms_scrub,
        t.scrubbed,
        t.scrub_files,
        t.scrub_bytes,
        t.ms_release,
        t.ms_buffer,
        flushed.ms,
        flushed.pages,
        flushed.ok,
        flushed.failed,
        flushed.slowest_ms,
    );
}

/// The cursor a step may persist: only once both buffers are empty, i.e.
/// every shard the cursor has passed has had its belief and truth handed
/// to consensus. Saving past buffered work would let a restart drop it
/// while the cursor claims the shards done — a node restarting faster than
/// the flush age would "complete" rotations having sent nothing. Holding
/// the save back costs a restart at most the shards since the last drain,
/// swept again (idempotent).
fn cursor_to_persist(
    rotation: &Rotation,
    next: hopnet_storage::sweep::SweepCursor,
) -> Option<hopnet_storage::sweep::SweepCursor> {
    (rotation.belief.is_empty() && rotation.truth.is_empty()).then_some(next)
}

/// The walker's starting point: the saved cursor, or a fresh rotation.
fn start_walker(
    app_state: &AppState,
) -> Result<(hopnet_storage::sweep::SweepCursor, Rotation), Error> {
    let node_id = app_state
        .get_node_id()
        .map_err(|_| Error::Failed(Arc::new("node id not set".to_string().into())))?;
    let saved = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| Error::Failed(Arc::new(format!("pool: {e}").into())))?;
        hopnet_storage::store::read_sweep_cursor(&conn)
            .map_err(|e| Error::Failed(Arc::new(format!("cursor: {e}").into())))?
    };
    let cursor = match saved {
        Some(c) => c,
        None => hopnet_storage::sweep::SweepCursor::fresh(unix_now(), current_height(app_state)?),
    };
    tracing::info!(
        "sweep: rolling walker resuming rotation {} at shard {:02x}",
        cursor.rotation,
        cursor.next_shard
    );
    Ok((cursor, Rotation::new(node_id, cursor.started_unix)))
}

/// Close a rotation: stamp its duration and height span, log it once, and
/// keep it as the operator's last report.
fn finish_rotation(
    app_state: &AppState,
    rotation: Rotation,
    started_height: u64,
) -> hopnet_storage::sweep::SweepReport {
    let mut report = rotation.report;
    report.rotation_secs = unix_now().saturating_sub(report.swept_at as u64);
    report.rotation_heights = current_height(app_state)
        .map(|tip| tip.saturating_sub(started_height))
        .unwrap_or(0);
    tracing::info!(
        "sweep: rotation of {} shards in {}s ({} heights): {} files, {} present, {} re-flagged, {} un-flagged, {} orphans deleted, {} orphans held (own uploads), {} held uploads expired, {} orphan batches skipped (busy), {} corrupt deleted, {} surplus released, {} temp files deleted, {} belief pages ({} failed), {} attestation pages ({} failed), {} shards failed; timings: {}",
        report.shards,
        report.rotation_secs,
        report.rotation_heights,
        report.files_on_disk,
        report.present,
        report.reflagged,
        report.unflagged,
        report.orphans_deleted,
        report.orphans_held,
        report.uploads_expired,
        report.orphan_batches_skipped,
        report.corrupt_deleted,
        report.surplus_released,
        report.temps_deleted,
        report.belief_pages,
        report.belief_failed_pages,
        report.attested_pages,
        report.attest_failed_pages,
        report.failed_shards,
        report.timing_summary(),
    );
    if report.rotation_heights > hopnet_storage::lifecycle::ATTESTATION_RECENCY_HEIGHTS / 2 {
        tracing::warn!(
            "sweep: a rotation spanned {} heights, more than half the {}-height attestation window",
            report.rotation_heights,
            hopnet_storage::lifecycle::ATTESTATION_RECENCY_HEIGHTS
        );
    }
    if report.unexpected_names > 0 {
        tracing::warn!(
            "sweep: {} files with names that are neither fragments nor temp files were left alone",
            report.unexpected_names
        );
    }
    *app_state.last_sweep.lock().unwrap() = Some(report.clone());
    report
}

/// One shard of the sweep, in the order the whole-store sweep used:
///   1. read the consensus height BEFORE listing — every stamp from this
///      step is no later than the moment its file was seen;
///   2. list the shard, diff it against its `fragment_hashes` rows and
///      repair `stored_locally` both ways (awaited marks);
///   3. delete orphan and temp files older than the grace period, holding
///      the orphans this node's upload ledger names (`reap_orphans`);
///   4. on the shard's day of seven, verify its content — corrupt bytes
///      are deleted and un-marked;
///   5. release surplus copies from the rotation's budget;
///   6. buffer the shard's belief differential (over repaired flags) and
///      its present hashes; `flush_buffers` pages them out.
///
/// Each step is timed into `rotation.shard` and folded into the report's
/// totals, a failed shard's up to the step it failed at.
pub(crate) async fn sweep_shard(
    app_state: &AppState,
    host: &SubstrateHost,
    rotation: &mut Rotation,
    shard: u8,
    orphan_grace_secs: u64,
) -> Result<(), Error> {
    rotation.shard = Default::default();
    let result = sweep_shard_steps(app_state, host, rotation, shard, orphan_grace_secs).await;
    let timings = rotation.shard;
    rotation.report.add_shard_timings(&timings);
    result
}

async fn sweep_shard_steps(
    app_state: &AppState,
    host: &SubstrateHost,
    rotation: &mut Rotation,
    shard: u8,
    orphan_grace_secs: u64,
) -> Result<(), Error> {
    use hopnet_storage::traits::LocalStateSink;
    use std::time::Instant;

    let fail = |what: &str, e: String| Error::Failed(Arc::new(format!("{what}: {e}").into()));
    let fragments_dir = app_state.fragments_dir.clone();
    let now = unix_now();

    // (1)
    let t = Instant::now();
    let height = current_height(app_state);
    rotation.shard.ms_height = ms_since(t);
    let height = height?;

    // (2)
    let t = Instant::now();
    let dir = fragments_dir.clone();
    let walk =
        tokio::task::spawn_blocking(move || hopnet_storage::fragstore::scan_shard(&dir, shard))
            .await;
    rotation.shard.ms_walk = ms_since(t);
    let walk = walk
        .map_err(|e| fail("walk join", e.to_string()))?
        .map_err(|e| fail("walk", e.to_string()))?;
    let listing = walk.fragments;
    rotation.shard.files = listing.len();
    let t = Instant::now();
    let rows = app_state
        .db_pool
        .get()
        .map_err(|e| fail("pool", e.to_string()))
        .and_then(|conn| {
            crate::db::fragments::shard_fragment_flags(&conn, shard)
                .map_err(|e| fail("fragment flags", format!("{e:?}")))
        });
    rotation.shard.ms_db = ms_since(t);
    let rows = rows?;
    let t = Instant::now();
    let diff = hopnet_storage::sweep::diff(&listing, &rows, now.saturating_sub(orphan_grace_secs));
    rotation.shard.ms_diff = ms_since(t);
    let t = Instant::now();
    for hash in &diff.present_unflagged {
        host.mark_local(*hash).await;
    }
    if !diff.flagged_missing.is_empty() {
        host.mark_remote_batch(diff.flagged_missing.clone()).await;
    }
    rotation.shard.ms_mark = ms_since(t);

    // (3)
    let t = Instant::now();
    let report = &mut rotation.report;
    for path in
        hopnet_storage::sweep::stale_temps(&walk.temps, now.saturating_sub(orphan_grace_secs))
    {
        match std::fs::remove_file(&path) {
            Ok(()) => report.temps_deleted += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => report.temps_deleted += 1,
            Err(e) => tracing::warn!("sweep: delete temp file {} failed: {e}", path.display()),
        }
    }
    rotation.shard.ms_temps = ms_since(t);
    let t = Instant::now();
    let deleted = reap_orphans(
        &app_state.db_pool,
        &fragments_dir,
        shard,
        &diff,
        local_upload_retention(),
        report,
    )
    .await;
    rotation.shard.ms_orphans = ms_since(t);

    // (4) Only a content hash mismatch is corruption; a file that vanished
    // or a read the OS refused is counted, never deleted or un-flagged.
    let day = (now / 86400) as i64;
    let mut present = diff.present.clone();
    if hopnet_storage::sweep::scrub_due(shard, day) && claim_scrub(day, shard) {
        let t = Instant::now();
        rotation.shard.scrubbed = true;
        let dir = fragments_dir.clone();
        let deleted: std::collections::HashSet<_> = deleted.into_iter().collect();
        let to_scrub: Vec<_> = listing
            .iter()
            .filter(|d| !deleted.contains(&d.hash))
            .copied()
            .collect();
        let outcome = tokio::task::spawn_blocking(move || {
            hopnet_storage::fragstore::verify_listing(
                &dir,
                &to_scrub,
                shard % hopnet_storage::sweep::SCRUB_SLICES,
                hopnet_storage::sweep::SCRUB_SLICES,
            )
        })
        .await;
        rotation.shard.ms_scrub = ms_since(t);
        let outcome = outcome.map_err(|e| fail("scrub join", e.to_string()))?;
        rotation.shard.scrub_files = outcome.files_read;
        rotation.shard.scrub_bytes = outcome.bytes_read;
        report.scrub_unreadable += outcome.unreadable;
        if !outcome.corrupt.is_empty() {
            tracing::warn!(
                "scrub: {} corrupt fragments in shard {shard:02x}",
                outcome.corrupt.len()
            );
            for hash in &outcome.corrupt {
                let _ = hopnet_storage::fragstore::delete_fragment(&fragments_dir, hash);
            }
            let gone: std::collections::HashSet<_> = outcome.corrupt.iter().collect();
            present.retain(|h| !gone.contains(h));
            report.corrupt_deleted += outcome.corrupt.len();
            host.mark_remote_batch(outcome.corrupt).await;
        }
        rotation.shard.ms_scrub = ms_since(t);
    }

    // (5) Before the belief: the shard's differential below carries the
    // removals, and the attestation never stamps a file this step deleted.
    let t = Instant::now();
    if rotation.surplus_left > 0 {
        let input = hopnet_storage::sweep::release_listing(
            &listing,
            &present,
            now.saturating_sub(SURPLUS_RELEASE_GRACE_SECS),
        );
        match release_surplus(app_state, &fragments_dir, input, rotation.surplus_left).await {
            Ok(outcome) => {
                if !outcome.released.is_empty() {
                    let gone: std::collections::HashSet<_> = outcome.released.iter().collect();
                    present.retain(|h| !gone.contains(h));
                }
                rotation.surplus_left =
                    rotation.surplus_left.saturating_sub(outcome.released.len());
                rotation.report.surplus_released += outcome.released.len();
                rotation.report.surplus_bytes_freed += outcome.bytes_freed;
            }
            Err(e) => tracing::warn!("sweep: surplus release failed: {e}"),
        }
    }
    rotation.shard.ms_release = ms_since(t);

    // (6) Belief and truth, both at the height read before the listing.
    let t = Instant::now();
    let pool = app_state.db_pool.clone();
    let node_id = rotation.node_id;
    let differential = tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        crate::db::inventory::compute_shard_inventory_differential(
            &mut conn, node_id, shard, height,
        )
        .map_err(|e| format!("{e:?}"))
    })
    .await;
    rotation.shard.ms_buffer = ms_since(t);
    let differential = differential
        .map_err(|e| fail("differential join", e.to_string()))?
        .map_err(|e| fail("inventory differential", e))?;
    rotation
        .belief
        .added
        .push(height, now, differential.fragments_added);
    rotation
        .belief
        .removed
        .push(height, now, differential.fragments_removed);
    rotation.truth.push(height, now, present.iter().copied());
    rotation.shard.ms_buffer = ms_since(t);

    let report = &mut rotation.report;
    report.shards += 1;
    report.files_on_disk += listing.len();
    report.present += present.len();
    report.reflagged += diff.present_unflagged.len();
    report.unflagged += diff.flagged_missing.len();
    report.young_orphans += diff.young_orphans;
    report.unexpected_names += walk.unexpected;
    Ok(())
}

/// Rowless files unlinked per write transaction, by the sweep's orphan
/// step, the operator's purge and an abandoned put alike: each batch holds
/// the database's write lock for its re-checks and unlinks only, `stat`s
/// done beforehand.
pub use hopnet_projection::host::UNLINK_BATCH;

/// Step 3's orphan half, the only place the sweep deletes a rowless file.
/// The shard's ledger entries older than the retention are expired first
/// (the upload is given up on; its files are orphans from here). Then
/// `diff.orphans` (rowless, past the grace) are split by the ledger: a
/// hash it names is this node's own upload whose `fragment_hashes` row has
/// not reached it — the transaction may still be in flight, or a staged
/// join may have imported an inventory without it (consensus-bugs 20) —
/// so the file is held and counted, never deleted. The rest are unlinked
/// in batches, each inside a write transaction that re-checks the hash has
/// no row and no ledger entry right before the unlink: `diff` is a snapshot
/// from earlier in the step (awaited marks and temp deletes sit between),
/// and a row that landed since makes the file an ordinary fragment, not an
/// orphan. Holds are never retired by a row landing; they last until the
/// retention. Returns the hashes deleted, which the scrub skips.
///
/// Every database and disk touch runs on the blocking pool, and nothing
/// here fails the shard: a step the database refused (busy, locked, a pool
/// checkout missed) is skipped and counted (`orphan_batches_skipped`), its
/// files waiting for the next rotation, so the scrub, surplus release and
/// the belief and attestation pages always run.
pub(crate) async fn reap_orphans(
    pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
    fragments_dir: &str,
    shard: u8,
    diff: &hopnet_storage::sweep::SweepDiff,
    retention_secs: u64,
    report: &mut hopnet_storage::sweep::SweepReport,
) -> Vec<hopnet_storage::Blake3Hash> {
    let settle_pool = pool.clone();
    let cutoff = unix_now().saturating_sub(retention_secs);
    let settled = tokio::task::spawn_blocking(move || settle_ledger(&settle_pool, shard, cutoff))
        .await
        .unwrap_or_else(|e| Err(format!("ledger task: {e}")));
    let ledger = match settled {
        Ok((expired, ledger)) => {
            report.uploads_expired += expired;
            ledger
        }
        Err(e) => {
            // No ledger, no deletions: without knowing the holds, every
            // orphan is treated as held this visit.
            tracing::warn!("sweep: shard {shard:02x} orphan step skipped: {e}");
            report.orphan_batches_skipped += 1;
            return Vec::new();
        }
    };
    let (delete, held) = hopnet_storage::sweep::split_orphans(&diff.orphans, &ledger);
    for (_, size) in &held {
        report.orphans_held += 1;
        report.orphan_bytes_held = report.orphan_bytes_held.saturating_add(*size);
    }
    let mut deleted = Vec::with_capacity(delete.len());
    for batch in delete.chunks(UNLINK_BATCH) {
        let candidates: Vec<_> = batch.iter().map(|(hash, _)| *hash).collect();
        let (batch_pool, dir) = (pool.clone(), fragments_dir.to_owned());
        let unlinked =
            tokio::task::spawn_blocking(move || unlink_batch(&batch_pool, &dir, &candidates))
                .await
                .unwrap_or_else(|e| Err(format!("unlink task: {e}")));
        match unlinked {
            Ok(gone) => {
                report.orphans_deleted += gone.hashes.len();
                report.orphan_bytes_freed = report.orphan_bytes_freed.saturating_add(gone.bytes);
                deleted.extend(gone.hashes);
            }
            Err(e) => {
                tracing::debug!("sweep: shard {shard:02x} orphan batch skipped: {e}");
                report.orphan_batches_skipped += 1;
            }
        }
    }
    deleted
}

/// Expire the shard's holds past `cutoff` and read what remains, in one
/// transaction: `(expired, ledger)`.
fn settle_ledger(
    pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
    shard: u8,
    cutoff: u64,
) -> Result<(usize, std::collections::HashSet<hopnet_storage::Blake3Hash>), String> {
    let mut conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    let tx = conn.transaction().map_err(|e| format!("tx: {e}"))?;
    let expired = hopnet_storage::store::expire_local_uploads(&tx, shard, cutoff)
        .map_err(|e| format!("expire ledger: {e}"))?;
    let ledger = hopnet_storage::store::local_uploads_in(&tx, shard)
        .map_err(|e| format!("upload ledger: {e}"))?;
    crate::db::shared::commit_timed(tx).map_err(|e| format!("ledger commit: {e}"))?;
    Ok((expired, ledger))
}

/// `stat` outside the lock, then re-check and unlink inside one write
/// transaction, for one batch of rowless candidates.
fn unlink_batch(
    pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
    fragments_dir: &str,
    batch: &[hopnet_storage::Blake3Hash],
) -> Result<hopnet_storage::store::DeletedFragments, String> {
    let sized = hopnet_storage::store::stat_fragments(fragments_dir, batch);
    let mut conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(|e| format!("tx: {e}"))?;
    let gone = hopnet_storage::store::delete_unclaimed_fragments(&tx, fragments_dir, &sized)
        .map_err(|e| format!("unlink re-check: {e}"))?;
    crate::db::shared::commit_timed(tx).map_err(|e| format!("unlink commit: {e}"))?;
    Ok(gone)
}

/// The most blobs the `held` listing shows; `total_blobs` says how many
/// there are.
pub const HELD_LISTING_LIMIT: usize = 1000;

/// What `purge_held_uploads` did.
#[derive(Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct PurgedUploads {
    /// Blobs whose holds were released.
    pub blobs: usize,
    /// Blobs refused as possibly still landing: a put for them is in flight
    /// in this process (`hopnet_projection::host::upload_is_live`), or
    /// their newest hold is younger than `PURGE_MIN_AGE_SECS`.
    pub skipped_recent: Vec<CustomUUID>,
    pub fragments_deleted: usize,
    pub bytes_freed: u64,
}

/// The `held` report: the own uploads the sweep is holding (ledger entries
/// still without a `fragment_hashes` row, computed at read time), grouped
/// by blob in SQL, oldest blob first, at most `HELD_LISTING_LIMIT` blobs
/// with the totals beside them — the operator's handle for re-uploading,
/// re-attesting or purging. Counts and stamps, not bytes: a byte total
/// would mean a `stat` per held file. On the blocking pool.
pub async fn held_uploads(
    app_state: &AppState,
) -> Result<hopnet_storage::store::HeldSummary, String> {
    let pool = app_state.db_pool.clone();
    tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
        hopnet_storage::store::held_local_upload_summary(&conn, HELD_LISTING_LIMIT)
            .map_err(|e| format!("ledger: {e}"))
    })
    .await
    .map_err(|e| format!("held task: {e}"))?
}

/// Give up on held uploads early: release the ledger entries of `blob_ids`
/// and delete the files only they held and that still have no row.
/// Explicit and operator-driven by design — a stuck upload is
/// indistinguishable from a slow one by anything but the operator (the
/// retention reclaims it eventually), and the blob ids are also the handle
/// for re-uploading. A blob with a put in flight in this process is
/// skipped and reported (`skipped_recent`), and so is a blob whose newest
/// hold is younger than `PURGE_MIN_AGE_SECS`. The registry alone is not
/// enough: it covers only the put itself, while the `fragment_hashes` rows
/// land with a later transaction — the photos publisher uploads every
/// resource of a photo before its one `photo_add`, a stalled mesh delays
/// commits, and a transaction already proposed before a restart can still
/// commit after it — so a just-finished upload is not yet a stuck one.
/// The age alone is not enough either: a slow client can take longer than
/// any fixed age per chunk. A hold stamped in the future (a clock step)
/// is purgeable, or it could neither expire nor be purged. Disk and
/// database work runs
/// on the blocking pool, in `UNLINK_BATCH` write transactions, each
/// re-checking every file right before its unlink
/// (`store::delete_unclaimed_fragments`).
pub async fn purge_held_uploads(
    app_state: &AppState,
    blob_ids: Vec<CustomUUID>,
) -> Result<PurgedUploads, String> {
    let pool = app_state.db_pool.clone();
    let fragments_dir = app_state.fragments_dir.clone();
    let report = tokio::task::spawn_blocking(move || {
        purge_held_uploads_blocking(&pool, &fragments_dir, &blob_ids, unix_now())
    })
    .await
    .map_err(|e| format!("purge task: {e}"))??;
    tracing::info!(
        blobs = report.blobs,
        skipped_recent = report.skipped_recent.len(),
        fragments_deleted = report.fragments_deleted,
        bytes_freed = report.bytes_freed,
        "purged held uploads at the operator's request"
    );
    Ok(report)
}

/// The youngest a blob's newest hold may be for the operator's purge to
/// take it: a day, comfortably past any transaction still on its way to
/// landing after its put finished (see `purge_held_uploads`).
pub const PURGE_MIN_AGE_SECS: i64 = 86_400;

/// Whether a blob whose newest hold was stamped `newest_unix` is too
/// recent to purge at `now_unix`. A future stamp (a clock step) is not.
fn hold_is_recent(newest_unix: i64, now_unix: u64) -> bool {
    let now = i64::try_from(now_unix).unwrap_or(i64::MAX);
    (0..PURGE_MIN_AGE_SECS).contains(&now.saturating_sub(newest_unix))
}

/// `purge_held_uploads` proper, synchronous, at `now_unix`: one blob at a
/// time, its age check and release in one write transaction and its
/// unlinks in batches of `UNLINK_BATCH`, each its own write transaction.
pub(crate) fn purge_held_uploads_blocking(
    pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
    fragments_dir: &str,
    blob_ids: &[CustomUUID],
    now_unix: u64,
) -> Result<PurgedUploads, String> {
    let mut report = PurgedUploads::default();
    for blob_id in blob_ids {
        if hopnet_projection::host::upload_is_live(blob_id) {
            report.skipped_recent.push(blob_id.clone());
            continue;
        }
        let candidates = {
            let mut conn = pool.get().map_err(|e| format!("pool: {e}"))?;
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|e| format!("tx: {e}"))?;
            let newest = hopnet_storage::store::newest_local_upload_unix(&tx, blob_id)
                .map_err(|e| format!("newest hold: {e}"))?;
            if newest.is_some_and(|newest| hold_is_recent(newest, now_unix)) {
                // Dropping the transaction rolls it back; it wrote nothing.
                report.skipped_recent.push(blob_id.clone());
                continue;
            }
            let candidates = hopnet_storage::store::release_local_uploads(&tx, blob_id)
                .map_err(|e| format!("release: {e}"))?;
            crate::db::shared::commit_timed(tx).map_err(|e| format!("release commit: {e}"))?;
            candidates
        };
        report.blobs += 1;
        for batch in candidates.chunks(UNLINK_BATCH) {
            let gone = unlink_batch(pool, fragments_dir, batch)?;
            report.fragments_deleted += gone.hashes.len();
            report.bytes_freed = report.bytes_freed.saturating_add(gone.bytes);
        }
    }
    Ok(report)
}

/// Page out the buffers once either is due (a full page, or an oldest
/// hash older than `MAX_BUFFER_AGE_SECS`), or unconditionally when
/// `force`. Belief first: attestation stamps only rows that already exist.
/// A failed belief page never blocks the truth pages, and a failed
/// attestation page never blocks the next. Returns the call's timings,
/// also folded into the rotation's report (zero when nothing was due).
pub(crate) async fn flush_buffers<S: hopnet_storage::traits::TxSubmitter>(
    submitter: &S,
    rotation: &mut Rotation,
    force: bool,
) -> hopnet_storage::sweep::FlushTimings {
    let page = hopnet_storage::engine::policy::ATTEST_PAGE_SIZE;
    let now = unix_now();
    let mut timings = hopnet_storage::sweep::FlushTimings::default();
    if !force
        && !rotation.truth.due(now, page, MAX_BUFFER_AGE_SECS)
        && !rotation.belief.due(now, page, MAX_BUFFER_AGE_SECS)
    {
        return timings;
    }
    let started = std::time::Instant::now();
    while let Some(report) = rotation.belief.take_report(rotation.node_id, page) {
        let submit_started = std::time::Instant::now();
        let submitted = match bincode::serde::encode_to_vec(&report, bincode::config::standard()) {
            Ok(payload) => submitter
                .submit(hopnet_storage::engine::policy::SELF_CHECK_FN, payload)
                .await
                .map_err(|e| format!("{e:?}")),
            Err(e) => Err(e.to_string()),
        };
        timings.slowest_ms = timings.slowest_ms.max(ms_since(submit_started));
        timings.pages += 1;
        match submitted {
            Ok(()) => {
                rotation.report.belief_pages += 1;
                timings.ok += 1;
            }
            Err(e) => {
                tracing::warn!("sweep: self-check page failed, attesting anyway: {e}");
                rotation.report.belief_failed_pages += 1;
                timings.failed += 1;
            }
        }
    }
    let mut pages = Vec::new();
    while let Some((height, present)) = rotation.truth.take_page(page) {
        pages.push(hopnet_storage::FragmentAttestation {
            node_id: rotation.node_id,
            height,
            present,
            suspect: Vec::new(),
        });
    }
    timings.pages += pages.len();
    let (committed, failed, slowest_ms) = submit_attestation_pages(submitter, pages).await;
    rotation.report.attested_pages += committed;
    rotation.report.attest_failed_pages += failed;
    timings.ok += committed;
    timings.failed += failed;
    timings.slowest_ms = timings.slowest_ms.max(slowest_ms);
    timings.ms = ms_since(started);
    rotation.report.add_flush_timings(&timings);
    if timings.pages > 0 {
        tracing::debug!(
            "sweep: flushed {} pages ({} ok, {} failed) in {} ms, slowest submit {} ms",
            timings.pages,
            timings.ok,
            timings.failed,
            timings.ms,
            timings.slowest_ms
        );
    }
    timings
}

/// One full rotation now, unpaced — the operator routes and orchestrator
/// tests (`POST /maintenance/fragment-inventory-self-check`, `GET
/// /maintenance/orphaned-fragments?run=true`). Interleaves with the
/// walker shard by shard and leaves its cursor alone.
pub async fn run_disk_truth_sweep(
    app_state: &AppState,
    orphan_grace_secs: u64,
) -> Result<hopnet_storage::sweep::SweepReport, Error> {
    let node_id = app_state
        .get_node_id()
        .map_err(|_| Error::Failed(Arc::new("node id not set".to_string().into())))?;
    let host = SubstrateHost::new(app_state.clone());
    let started_height = current_height(app_state)?;
    let mut rotation = Rotation::new(node_id, unix_now());
    for shard in 0..=u8::MAX {
        {
            let _guard = SWEEP_LOCK.lock().await;
            if let Err(e) =
                sweep_shard(app_state, &host, &mut rotation, shard, orphan_grace_secs).await
            {
                tracing::warn!("sweep: shard {shard:02x} failed: {e}");
                rotation.report.failed_shards += 1;
            }
        }
        flush_buffers(&host, &mut rotation, false).await;
    }
    flush_buffers(&host, &mut rotation, true).await;
    let report = finish_rotation(app_state, rotation, started_height);
    if report.attested_pages == 0 && report.attest_failed_pages > 0 {
        return Err(Error::Failed(Arc::new(
            format!(
                "attestation: all {} pages failed",
                report.attest_failed_pages
            )
            .into(),
        )));
    }
    Ok(report)
}

/// Submit the sweep's attestation pages one at a time, carrying on past a
/// failed page: each page stands alone (`apply_attestation` is
/// idempotent), so one timed-out page must not cost the others their
/// stamps. Returns `(committed, failed, slowest submit in ms)`.
pub(crate) async fn submit_attestation_pages<S: hopnet_storage::traits::TxSubmitter>(
    submitter: &S,
    pages: Vec<hopnet_storage::FragmentAttestation>,
) -> (usize, usize, u64) {
    let total = pages.len();
    let (mut committed, mut failed, mut slowest_ms) = (0usize, 0usize, 0u64);
    for (i, attestation) in pages.into_iter().enumerate() {
        let payload = match bincode::serde::encode_to_vec(&attestation, bincode::config::standard())
        {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("sweep: attestation encode (page {} of {total}): {e}", i + 1);
                failed += 1;
                continue;
            }
        };
        let submit_started = std::time::Instant::now();
        let submitted = submitter
            .submit(hopnet_storage::engine::policy::ATTEST_FN, payload)
            .await;
        slowest_ms = slowest_ms.max(ms_since(submit_started));
        match submitted {
            Ok(()) => committed += 1,
            Err(e) => {
                tracing::warn!(
                    "sweep: attestation submit (page {} of {total}): {e:?}",
                    i + 1
                );
                failed += 1;
            }
        }
    }
    (committed, failed, slowest_ms)
}

/// Kept for callers that only want belief refreshed (tests, routes): the
/// full sweep is the self-check now.
pub async fn run_fragment_inventory_self_check(app_state: &AppState) -> Result<(), Error> {
    run_disk_truth_sweep(app_state, SWEEP_ORPHAN_GRACE_SECS)
        .await
        .map(|_| ())
}

/// Grace before a fragment file is considered by the prompt surplus
/// release. Shorter than the watermark path's hour: the release only ever
/// touches copies the guard calls surplus (placement confirmed, not this
/// node's obligation, a recent attestation elsewhere), so the in-flight-store
/// race the grace exists for cannot apply to them.
pub const SURPLUS_RELEASE_GRACE_SECS: u64 = 600;

/// Most fragments the prompt surplus release deletes per rotation of the
/// rolling sweep (and per manual release): an origin that ingested a
/// library holds ~100k surplus files, and a few thousand per rotation
/// would take a day to drain. 10k unlinks is seconds, and the belief
/// pages that carry their removals stay under the payload limit.
pub const SURPLUS_RELEASE_MAX_PER_SWEEP: usize = 10_000;

/// The guard's facts for the given on-disk fragments (`(hash, size)`,
/// already filtered by the caller's grace): one candidate per fragment
/// with a known blob. `min_verified_height` tightens the other-holder
/// count to attestations disk-verified at or after that height.
async fn gather_eviction_candidates(
    app_state: &AppState,
    disk: &[(crate::types::Blake3Hash, u64)],
    min_verified_height: Option<u64>,
) -> Result<Vec<hopnet_storage::eviction::EvictionCandidate>, Error> {
    use hopnet_storage::eviction::EvictionCandidate;
    use hopnet_storage::traits::StateReader;

    if disk.is_empty() {
        return Ok(Vec::new());
    }
    let my_node_id = app_state
        .get_node_id()
        .map_err(|_| Error::Failed(Arc::new("node id not set".to_string().into())))?;

    let host = SubstrateHost::new(app_state.clone());
    let view = tokio::task::spawn_blocking(move || host.storage_view())
        .await
        .map_err(|e| Error::Failed(Arc::new(format!("view join: {e}").into())))?
        .map_err(|e| Error::Failed(Arc::new(format!("storage view: {e}").into())))?;

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
        let holder_counts = match min_verified_height {
            Some(floor) => crate::db::fragments::recent_member_holder_counts(
                &conn,
                &hashes,
                &member_ids,
                my_node_id,
                floor,
            ),
            None => {
                crate::db::fragments::member_holder_counts(&conn, &hashes, &member_ids, my_node_id)
            }
        }
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
    for (hash, size) in disk {
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
    Ok(candidates)
}

/// Delete planned fragments and flip them to not-stored-locally. Consensus
/// learns of the removals from a disk-truth sweep's self-check (the same
/// sweep's, for the surplus release). Returns (fragments deleted, bytes
/// freed).
async fn delete_and_mark(
    app_state: &AppState,
    fragments_dir: &str,
    planned: &[crate::types::Blake3Hash],
    sizes: &std::collections::HashMap<crate::types::Blake3Hash, u64>,
) -> (Vec<crate::types::Blake3Hash>, u64) {
    use hopnet_storage::traits::LocalStateSink;

    let mut bytes_freed = 0u64;
    let mut deleted = Vec::new();
    for hash in planned {
        match hopnet_storage::fragstore::delete_fragment(fragments_dir, hash) {
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
    (deleted, bytes_freed)
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
    use hopnet_storage::eviction::{DiskPressure, plan_evictions};

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

    // On-disk fragments past the grace (avoids racing in-flight stores,
    // mirroring the orphan scan). Walked only here, above the watermark.
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

    let candidates = gather_eviction_candidates(app_state, &disk, None).await?;
    let sizes: std::collections::HashMap<_, _> = disk.into_iter().collect();
    let planned = plan_evictions(candidates, &pressure);
    let (evicted, bytes_freed) = delete_and_mark(app_state, &fragments_dir, &planned, &sizes).await;
    let evicted = evicted.len();

    tracing::info!(
        "watermark eviction: {} fragments evicted, {} bytes freed ({}% used, high {}%, low {}%)",
        evicted,
        bytes_freed,
        used_bytes * 100 / total_bytes.max(1),
        high_pct,
        low_pct
    );
    Ok(serde_json::json!({
        "evicted": evicted, "bytes_freed": bytes_freed,
        "used_bytes": used_bytes, "total_bytes": total_bytes,
        "high_pct": high_pct, "low_pct": low_pct,
    }))
}

/// What one prompt surplus release did.
struct SurplusRelease {
    /// The copies deleted (and flipped to not-stored-locally).
    released: Vec<crate::types::Blake3Hash>,
    bytes_freed: u64,
    /// The per-sweep cap bit; the rest waits for the next walk.
    capped: bool,
}

/// Prompt surplus release over `disk` (`(hash, size)` of on-disk files past
/// the grace): delete every copy the eviction guard calls surplus, without
/// waiting for disk pressure. Placement confirmation is what turns an
/// origin's (or a departed holder's) copies into surplus, so this frees an
/// ingesting node's local fragments a sweep or so after its blobs are
/// confirmed elsewhere — a non-member origin ends up holding nothing.
/// Stricter than the watermark path on evidence: another member's copy
/// must have been disk-verified within the confirmation recency window
/// (hours since 2026.10.5, deliberately the same window as confirmation).
/// Bounded per call. Rides the disk-truth sweep's walk; the operator route
/// pays for its own.
async fn release_surplus(
    app_state: &AppState,
    fragments_dir: &str,
    disk: Vec<(crate::types::Blake3Hash, u64)>,
    max: usize,
) -> Result<SurplusRelease, Error> {
    use hopnet_storage::eviction::plan_surplus_release;
    use hopnet_storage::traits::StateReader;

    if disk.is_empty() || max == 0 {
        return Ok(SurplusRelease {
            released: Vec::new(),
            bytes_freed: 0,
            capped: false,
        });
    }
    let host = SubstrateHost::new(app_state.clone());
    let tip = tokio::task::spawn_blocking(move || host.current_height())
        .await
        .map_err(|e| Error::Failed(Arc::new(format!("height join: {e}").into())))?
        .map_err(|e| Error::Failed(Arc::new(format!("current height: {e}").into())))?;
    let recent = tip.saturating_sub(hopnet_storage::lifecycle::ATTESTATION_RECENCY_HEIGHTS);

    let candidates = gather_eviction_candidates(app_state, &disk, Some(recent)).await?;
    let sizes: std::collections::HashMap<_, _> = disk.into_iter().collect();
    let planned = plan_surplus_release(candidates, max);
    let capped = planned.len() >= max;
    let (released, bytes_freed) = delete_and_mark(app_state, fragments_dir, &planned, &sizes).await;
    if !released.is_empty() {
        tracing::info!(
            "surplus release: {} fragments released, {bytes_freed} bytes freed",
            released.len()
        );
    }
    Ok(SurplusRelease {
        released,
        bytes_freed,
        capped,
    })
}

/// Manual trigger for the prompt surplus release (`POST
/// /maintenance/surplus-release`): walks the store itself, which the
/// scheduled release never does — it rides the disk-truth sweep's walk.
pub async fn run_surplus_release(
    app_state: &AppState,
    grace_secs: Option<u64>,
) -> Result<serde_json::Value, Error> {
    let fragments_dir = crate::storage_host::functions::get_fragments_dir()
        .map_err(|e| Error::Failed(Arc::new(format!("fragments dir: {e:?}").into())))?;
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let disk = hopnet_storage::fragstore::scan_fragments(
        &fragments_dir,
        now_unix - grace_secs.unwrap_or(SURPLUS_RELEASE_GRACE_SECS),
    )
    .map_err(|e| Error::Failed(Arc::new(format!("disk scan: {e}").into())))?;
    let outcome = release_surplus(
        app_state,
        &fragments_dir,
        disk,
        SURPLUS_RELEASE_MAX_PER_SWEEP,
    )
    .await?;
    Ok(serde_json::json!({
        "released": outcome.released.len(), "bytes_freed": outcome.bytes_freed,
        "capped": outcome.capped,
    }))
}

/// Last-seen storage view summary — INFO logging only on change.
static LAST_VIEW_SUMMARY: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
/// Whether a policy tick is running on this node. The cron fires every
/// five minutes and the maintenance route on demand; a tick that overruns
/// (a long fulfillment pass, a slow eviction scan) must not stack a second
/// one on top — two passes over the same in-flight set would propose the
/// same confirmations twice and double the consensus traffic for nothing.
static TICK_RUNNING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// How long, after this node boots or after a member's contact resumes, a
/// member in liveness contact stays up for repair although the
/// availability grid calls it offline (`evidence::repair_grace_peers`). A
/// rebooted node is grid-offline until its own next metrics sample — up
/// to ten minutes after boot — so the grace must outlast that; after an
/// epoch crossing
/// every node reboots at once, and without it two members "down" put
/// every chunk below the watermark (2026.10.8: hours of urgent re-encodes
/// of copies that were only rebooting).
pub const REPAIR_GRACE: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// `REPAIR_GRACE`, overridden by `HOPNET_REPAIR_GRACE_SECS` (0 turns the
/// grace off).
fn repair_grace() -> std::time::Duration {
    static GRACE: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *GRACE.get_or_init(|| {
        std::env::var("HOPNET_REPAIR_GRACE_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(std::time::Duration::from_secs)
            .unwrap_or(REPAIR_GRACE)
    })
}

/// The members the repair grace may cover: grid-offline, and offline on
/// the grid for no longer than the grace plus one grid bucket (the grid's
/// own lag in seeing a node come back). The cap bounds the total grace per
/// grid-offline episode: a member whose contact keeps dropping and
/// resuming would otherwise restart its grace forever while the grid
/// calls it offline for good (a stuck metrics sampler, a full disk).
fn grace_candidates(
    members: &std::collections::HashSet<i32>,
    view: &hopnet_storage::traits::StorageView,
    grace: std::time::Duration,
) -> Vec<i32> {
    let cap = grace.as_secs() as i64 + view.grid_step_secs;
    members
        .iter()
        .copied()
        .filter(|m| !view.online.contains(m))
        .filter(|m| view.absence.get(m).copied().unwrap_or(0) <= cap)
        .collect()
}

/// Who repair counts as up: the grid's online set, this node, and the
/// members in liveness contact within the grace (`in_grace`). Grace only adds
/// members back — it never removes a grid-online node, and never revives
/// a node that has left the storage view.
fn repair_online(
    grid_online: &[i32],
    members: &std::collections::HashSet<i32>,
    me: i32,
    in_grace: &std::collections::HashSet<i32>,
) -> std::collections::HashSet<i32> {
    let mut up: std::collections::HashSet<i32> = grid_online.iter().copied().collect();
    up.extend(
        members
            .iter()
            .copied()
            .filter(|m| *m == me || in_grace.contains(m)),
    );
    up
}

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
    /// Members the availability grid calls online.
    pub online: usize,
    /// Members (and this node) repair counted as up although the grid
    /// calls them offline: in liveness contact within the repair grace.
    pub repair_grace_online: usize,
    pub watermark: usize,
    /// Chunks below the watermark with a class this node owes a rebuild
    /// of (all enqueued, urgently, unless already queued).
    pub urgent_chunks_owed: usize,
    /// Chunks at or above the watermark with a class this node owes a
    /// rebuild of — one is picked per tick.
    pub lazy_chunks_owed: usize,
    /// Urgent chunks newly queued by this tick (not already waiting).
    pub urgent_reencodes: usize,
    /// Urgent re-encodes queued or running on the engine after this tick.
    pub urgent_reencodes_pending: usize,
    pub lazy_reencodes: usize,
    /// Blobs the pull planner has handed the reconciler since its current
    /// pass started (a wake-up, not a result — the worker pulls on its
    /// own time).
    pub pull_kicks: usize,
    /// Confirmations proposed by the fulfillment pass, across its rounds.
    pub confirms_proposed: usize,
    pub grace_declared: usize,
    pub eviction: serde_json::Value,
    /// Wall time of the repair scan (plus its goal lookups), so a tick that
    /// overruns its 5-minute cron is visible without a profiler.
    pub scan_ms: u64,
    /// The pull planner's last pass and its feed (RFC-STORAGE-003 S3).
    pub planner: crate::storage_host::pull_planner::PlannerReport,
}

/// The engine policy tick (RFC-STORAGE-001 Repair; RFC-STORAGE-002 S6):
/// view sync → the obligation check's re-encode half (ladder + deputy
/// under the goal assignment) → grace rung → fulfillment pass → in-flight
/// re-kick → eviction check. Disk truth (the sweep, the scrub slice,
/// belief and attestation) rides the self-check cron. One tick at a time
/// per node: a second caller while one runs gets an error, never a
/// concurrent pass.
pub async fn run_storage_policy_tick(app_state: &AppState) -> Result<PolicyTickReport, Error> {
    use std::sync::atomic::Ordering;
    if TICK_RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(Error::Failed(Arc::new(
            "policy tick already running".to_string().into(),
        )));
    }
    // Released on every exit — an error, or the future being dropped when
    // the maintenance route's client gives up mid-tick.
    struct Running;
    impl Drop for Running {
        fn drop(&mut self) {
            TICK_RUNNING.store(false, Ordering::Release);
        }
    }
    let _running = Running;
    policy_tick_rungs(app_state).await
}

/// The tick's body; `run_storage_policy_tick` holds the one-at-a-time
/// guard around it.
async fn policy_tick_rungs(app_state: &AppState) -> Result<PolicyTickReport, Error> {
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
    // Every DB read in this tick runs on the blocking pool: the scan below
    // walks the whole fragment table, and the tick shares its runtime with
    // the consensus host and the HTTP surface.
    let settings = {
        let pool = app_state.db_pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
            crate::db::shared::read_storage_node_settings(&conn)
                .map_err(|e| format!("settings: {e:?}"))
        })
        .await
        .map_err(|e| Error::Failed(Arc::new(format!("settings join: {e}").into())))?
        .map_err(|e| Error::Failed(Arc::new(e.into())))?
    };
    let mut urgent_owed = 0usize;
    let mut urgent_enqueued = 0usize;
    let mut lazy_enqueued = 0usize;
    let mut lazy_owed = 0usize;
    let mut scan_ms = 0u64;
    let mut repair_grace_online = 0usize;
    if settings.reencode_enabled {
        let members: std::collections::HashSet<i32> = member_ids.iter().copied().collect();
        // Repair's "up": the grid plus members in liveness contact within the
        // grace, so a reboot (every node's, at a crossing) is not taken
        // for a departure.
        let in_grace = crate::consensus::evidence::repair_grace_peers(
            &app_state.evidence.snapshot(),
            app_state.evidence.origin(),
            std::time::Instant::now(),
            repair_grace(),
            grace_candidates(&members, &view, repair_grace()),
        );
        let online = repair_online(&view.online, &members, my_node_id, &in_grace);
        repair_grace_online = online.iter().filter(|n| !view.online.contains(n)).count();
        if repair_grace_online > 0 {
            tracing::debug!(
                grid_online = view.online.len(),
                repair_online = online.len(),
                "policy tick: members within the repair grace count as up"
            );
        }
        let (candidates, goals, elapsed) = {
            let pool = app_state.db_pool.clone();
            let (online, members) = (online.clone(), members.clone());
            tokio::task::spawn_blocking(move || {
                let started = std::time::Instant::now();
                let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
                let candidates = crate::db::inventory::find_chunks_with_missing_classes(
                    &conn, &online, &members,
                )
                .map_err(|e| format!("repair scan: {e:?}"))?;
                // Goal assignments, memoized per blob (many chunks share one).
                let mut goals: std::collections::HashMap<hopnet_storage::BlobId, Option<Vec<i32>>> =
                    Default::default();
                for cand in &candidates {
                    if !goals.contains_key(&cand.blob_id) {
                        let assignment =
                            hopnet_storage::lifecycle::pull_target(&conn, &cand.blob_id)
                                .map_err(|e| format!("pull target: {e}"))?
                                .map(|t| t.assignment);
                        goals.insert(cand.blob_id.clone(), assignment);
                    }
                }
                Ok::<_, String>((candidates, goals, started.elapsed()))
            })
            .await
            .map_err(|e| Error::Failed(Arc::new(format!("repair scan join: {e}").into())))?
            .map_err(|e| Error::Failed(Arc::new(e.into())))?
        };
        scan_ms = elapsed.as_millis() as u64;
        if elapsed > std::time::Duration::from_secs(60) {
            tracing::warn!(
                scan_ms,
                candidates = candidates.len(),
                "policy tick: repair scan is slow"
            );
        }
        if let Some(engine) = app_state.storage.get() {
            let up: std::collections::BTreeSet<i32> = online.iter().copied().collect();
            let mut lazy_pick: Option<ReencodeCmd> = None;
            let mut urgent_cmds: Vec<ReencodeCmd> = Vec::new();
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
                    urgent_cmds.push(cmd);
                } else {
                    lazy_owed += 1;
                    if lazy_pick.is_none() {
                        lazy_pick = Some(cmd);
                    }
                }
            }
            // Publish this tick's urgent set before queueing it: the
            // engine drops a queued urgent chunk this tick no longer owes
            // (its holders came back), so a wrong tick costs at most one
            // tick's work.
            urgent_owed = urgent_cmds.len();
            engine.set_urgent_reencodes(
                urgent_cmds
                    .iter()
                    .map(|c| {
                        (
                            (c.blob_id.clone(), c.chunk_number),
                            c.missing_classes.clone(),
                        )
                    })
                    .collect(),
            );
            for cmd in urgent_cmds {
                // A chunk still queued from an earlier tick is not
                // queued twice.
                if engine.enqueue_reencode(cmd, true) {
                    urgent_enqueued += 1;
                }
            }
            if let Some(cmd) = lazy_pick {
                lazy_enqueued = usize::from(engine.enqueue_reencode(cmd, false));
            }
        }
    } else if let Some(engine) = app_state.storage.get() {
        // Re-encode turned off: nothing queued is owed any more.
        engine.set_urgent_reencodes(Default::default());
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

    // (3) The fulfillment pass (RFC-STORAGE-003 S4): confirm, in batches,
    // every in-flight blob whose evidence is already complete — after a
    // transition that is most of them, and nothing else can retire them.
    // Runs BEFORE the re-kick so the tick's consensus budget goes to what
    // is provable now; the worker owns the rest.
    let confirms_proposed = propose_ready_confirmations(
        app_state,
        hopnet_storage::engine::policy::CONFIRM_CHECKS_PER_TICK,
    )
    .await
    .unwrap_or_else(|e| {
        tracing::warn!("fulfillment pass failed: {e}");
        0
    });

    // (3b) The obligation check (RFC-STORAGE-003 S3) is the pull planner's
    // (`pull_planner`): it walks the whole in-flight set, keeps what this
    // node owes, orders it at-risk first and feeds the worker as it
    // drains. The tick only keeps it alive and reports on it.
    if crate::storage_host::pull_planner::ensure_running(app_state) {
        tracing::info!("policy tick: pull planner started");
    }
    let planner = crate::storage_host::pull_planner::report(app_state);
    let pull_kicks = planner.offered;

    // (4) Eviction check (statvfs no-op below the high watermark). The
    //     prompt surplus release is not here: it rides the rolling
    //     sweep's walk (`sweep_shard`, step 5).
    let eviction = run_watermark_eviction(app_state, None, None).await?;

    let report = PolicyTickReport {
        at: chrono::Utc::now().timestamp(),
        members: member_ids,
        online: view.online.len(),
        repair_grace_online,
        watermark: view.watermark,
        urgent_chunks_owed: urgent_owed,
        lazy_chunks_owed: lazy_owed,
        urgent_reencodes: urgent_enqueued,
        urgent_reencodes_pending: app_state
            .storage
            .get()
            .map_or(0, |engine| engine.urgent_reencodes_pending()),
        lazy_reencodes: lazy_enqueued,
        pull_kicks,
        confirms_proposed,
        grace_declared,
        eviction,
        scan_ms,
        planner,
    };
    *app_state.last_tick.lock().unwrap() = Some(report.clone());
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hopnet_storage::types::Blake3Hash;
    use std::collections::HashSet;

    fn test_pool() -> r2d2::Pool<crate::db::SqliteConnectionManager> {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::chains::install(&pool.get().unwrap()).unwrap();
        pool
    }

    /// A fragment store and a head-shape database for the sweep's orphan
    /// step: `(dir, fragments_dir, pool)`.
    fn orphan_fixture() -> (
        tempfile::TempDir,
        String,
        r2d2::Pool<crate::db::SqliteConnectionManager>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let frags = dir.path().join("fragments").to_string_lossy().into_owned();
        (dir, frags, test_pool())
    }

    /// Store `bytes` as a fragment whose file is `age_secs` old.
    fn store_aged(frags: &str, bytes: &[u8], age_secs: u64) -> Blake3Hash {
        let hash = Blake3Hash::from_bytes(*blake3::hash(bytes).as_bytes());
        hopnet_storage::fragstore::store_fragment(frags, &hash, bytes.to_vec()).unwrap();
        let path = format!(
            "{}/{}",
            hopnet_storage::fragstore::create_fragment_path(frags, &hash).unwrap(),
            hash.to_hex()
        );
        let mtime = std::time::SystemTime::now() - std::time::Duration::from_secs(age_secs);
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
        hash
    }

    /// Step 2's diff for `shard` with the production grace.
    fn shard_diff(
        conn: &rusqlite::Connection,
        frags: &str,
        shard: u8,
    ) -> hopnet_storage::sweep::SweepDiff {
        let walk = hopnet_storage::fragstore::scan_shard(frags, shard).unwrap();
        let rows = crate::db::fragments::shard_fragment_flags(conn, shard).unwrap();
        hopnet_storage::sweep::diff(
            &walk.fragments,
            &rows,
            unix_now().saturating_sub(SWEEP_ORPHAN_GRACE_SECS),
        )
    }

    /// Steps 2 and 3 of `sweep_shard` for `shard`: the diff over the real
    /// walk and rows, then `reap_orphans`.
    async fn reap_shard(
        pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
        frags: &str,
        shard: u8,
    ) -> (
        Vec<Blake3Hash>,
        hopnet_storage::sweep::SweepDiff,
        hopnet_storage::sweep::SweepReport,
    ) {
        let diff = shard_diff(&pool.get().unwrap(), frags, shard);
        let mut report = hopnet_storage::sweep::SweepReport::default();
        let deleted = reap_orphans(
            pool,
            frags,
            shard,
            &diff,
            LOCAL_UPLOAD_RETENTION_SECS,
            &mut report,
        )
        .await;
        (deleted, diff, report)
    }

    /// The upload's transaction lands: `data_blocks` and `fragment_hashes`
    /// rows for `hash` under `blob`.
    fn land_row(conn: &rusqlite::Connection, blob: &CustomUUID, hash: &Blake3Hash, size: usize) {
        conn.execute_batch(&format!(
            "INSERT OR IGNORE INTO data_blocks (id, file_hash, fragment_count, added_bytes, file_size)
             VALUES ('{blob}', X'00', 1, 0, {size});
             INSERT INTO fragment_hashes
             (data_block_id, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally)
             VALUES ('{blob}', 0, 0, '{}', X'{}', 0, 0);",
            hash.to_hex(),
            hash.to_hex()
        ))
        .unwrap();
    }

    // Impact: regression guard for the 2026-10-01 loss of two uploaded
    // videos — the node's own upload wrote its fragments, their rows never
    // reached it before the sweep's grace ran out, and the only copies
    // were deleted as orphans (consensus-bugs 20).
    // Should: hold a ledgered rowless file past the grace, counting it as
    // held, and keep holding it on every later visit.
    // Should: once its row lands, see it as present, with the hold kept
    // until the retention rather than retired.
    // Should not: delete the file at any point.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_own_upload_older_than_the_grace_survives_the_sweep_until_its_rows_land() {
        let (_dir, frags, pool) = orphan_fixture();
        let bytes = b"a fragment whose transaction is still in flight";
        let hash = store_aged(&frags, bytes, 3 * 3600);
        let shard = hopnet_storage::sweep::shard_of(&hash);
        let blob = CustomUUID::new(None);
        hopnet_storage::store::record_local_uploads(
            &pool.get().unwrap(),
            &blob,
            [&hash],
            unix_now(),
        )
        .unwrap();

        for _ in 0..2 {
            let (deleted, diff, report) = reap_shard(&pool, &frags, shard).await;
            assert_eq!(diff.orphans.len(), 1, "rowless and past the grace");
            assert!(deleted.is_empty());
            assert_eq!(report.orphans_deleted, 0);
            assert_eq!(report.orphans_held, 1);
            assert_eq!(report.orphan_bytes_held, bytes.len() as u64);
            assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
                &frags, &hash
            ));
        }

        land_row(&pool.get().unwrap(), &blob, &hash, bytes.len());
        let (deleted, diff, report) = reap_shard(&pool, &frags, shard).await;
        assert!(deleted.is_empty());
        assert_eq!(diff.present, vec![hash]);
        assert_eq!(report.orphans_held, 0);
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &hash
        ));
        assert_eq!(
            hopnet_storage::store::local_uploads_in(&pool.get().unwrap(), shard).unwrap(),
            HashSet::from([hash]),
            "the hold is not retired by the row; it lasts until the retention"
        );
    }

    // Impact: the ledger must not turn the sweep into a disk leak — a
    // rowless file this node did not upload (a pull or re-encode whose row
    // was later removed, a leftover) is still reaped.
    // Should: delete a rowless file past the grace that the ledger does
    // not name, counting it and its bytes as freed and returning its hash
    // for the scrub to skip.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rowless_file_that_is_not_an_own_upload_is_still_deleted_after_the_grace() {
        let (_dir, frags, pool) = orphan_fixture();
        let bytes = b"a fragment nothing claims";
        let hash = store_aged(&frags, bytes, 3 * 3600);
        let shard = hopnet_storage::sweep::shard_of(&hash);

        let (deleted, _, report) = reap_shard(&pool, &frags, shard).await;
        assert_eq!(deleted, vec![hash]);
        assert_eq!(report.orphans_deleted, 1);
        assert_eq!(report.orphan_bytes_freed, bytes.len() as u64);
        assert_eq!(report.orphans_held, 0);
        assert_eq!(report.orphan_batches_skipped, 0);
        assert!(!hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &hash
        ));
    }

    // Impact: `diff.orphans` is a snapshot taken before awaited work in
    // `sweep_shard`; an upload's row can land in that window. Deleting
    // from the stale snapshot would take the only copy — the orphan unlink
    // must re-check at the last moment.
    // Should: leave a file alone whose fragment_hashes row landed between
    // the orphan snapshot and the unlink, whether or not it was ledgered.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_row_landing_after_the_orphan_snapshot_still_protects_the_file() {
        let (_dir, frags, pool) = orphan_fixture();
        let bytes = b"rowless when listed, rowed when reaped";
        let hash = store_aged(&frags, bytes, 3 * 3600);
        let shard = hopnet_storage::sweep::shard_of(&hash);
        let blob = CustomUUID::new(None);

        // Step 2's snapshot: no row, no ledger — an orphan to delete.
        let diff = shard_diff(&pool.get().unwrap(), &frags, shard);
        assert_eq!(diff.orphans.len(), 1);

        // The row lands in the window before step 3.
        land_row(&pool.get().unwrap(), &blob, &hash, bytes.len());
        let mut report = hopnet_storage::sweep::SweepReport::default();
        let deleted = reap_orphans(
            &pool,
            &frags,
            shard,
            &diff,
            LOCAL_UPLOAD_RETENTION_SECS,
            &mut report,
        )
        .await;
        assert!(deleted.is_empty());
        assert_eq!(report.orphans_deleted, 0);
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &hash
        ));
    }

    // Impact: a straggler's later staged join can import an inventory
    // without a row that had landed; a hold retired on the row would then
    // leave the file an ordinary orphan, and the sweep would delete the
    // only copy within the hour. Holds last until the retention instead.
    // Should: keep holding a file whose row landed and then disappeared.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_hold_whose_row_landed_and_vanished_is_still_held() {
        let (_dir, frags, pool) = orphan_fixture();
        let bytes = b"landed, then re-imported without its row";
        let hash = store_aged(&frags, bytes, 3 * 3600);
        let shard = hopnet_storage::sweep::shard_of(&hash);
        let blob = CustomUUID::new(None);
        {
            let conn = pool.get().unwrap();
            hopnet_storage::store::record_local_uploads(&conn, &blob, [&hash], unix_now()).unwrap();
            land_row(&conn, &blob, &hash, bytes.len());
        }

        let (deleted, diff, _) = reap_shard(&pool, &frags, shard).await;
        assert!(deleted.is_empty());
        assert_eq!(diff.present, vec![hash]);

        // The inventory is replaced without the row.
        pool.get()
            .unwrap()
            .execute("DELETE FROM fragment_hashes", [])
            .unwrap();
        let (deleted, diff, report) = reap_shard(&pool, &frags, shard).await;
        assert_eq!(diff.orphans.len(), 1, "rowless again, past the grace");
        assert!(deleted.is_empty());
        assert_eq!(report.orphans_held, 1);
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &hash
        ));
    }

    // Impact: an upload whose transaction never lands (rejected, given up,
    // retried under a fresh blob id) must not hold its files forever and
    // grow the ledger without bound; the retention is the durable bound a
    // restart cannot reset.
    // Should: expire a held upload written longer ago than the retention,
    // report it, and delete its file as an ordinary orphan on the same
    // visit.
    // Should not: expire an entry within the retention, nor one stamped in
    // the future by a clock step.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_held_upload_past_the_retention_becomes_an_ordinary_orphan() {
        let (_dir, frags, pool) = orphan_fixture();
        let now = unix_now();
        let stale = store_aged(&frags, b"given up on two weeks ago", 3 * 3600);
        let fresh = store_aged(&frags, b"uploaded yesterday", 3 * 3600);
        let future = store_aged(&frags, b"stamped after a clock step", 3 * 3600);
        let blob = CustomUUID::new(None);
        {
            let conn = pool.get().unwrap();
            hopnet_storage::store::record_local_uploads(
                &conn,
                &blob,
                [&stale],
                now - LOCAL_UPLOAD_RETENTION_SECS - 1,
            )
            .unwrap();
            hopnet_storage::store::record_local_uploads(&conn, &blob, [&fresh], now - 86_400)
                .unwrap();
            hopnet_storage::store::record_local_uploads(&conn, &blob, [&future], now + 86_400)
                .unwrap();
        }

        let mut total = hopnet_storage::sweep::SweepReport::default();
        for hash in [&stale, &fresh, &future] {
            let (_, _, report) =
                reap_shard(&pool, &frags, hopnet_storage::sweep::shard_of(hash)).await;
            total.uploads_expired += report.uploads_expired;
            total.orphans_deleted += report.orphans_deleted;
            total.orphans_held += report.orphans_held;
        }
        assert_eq!(total.uploads_expired, 1);
        assert_eq!(total.orphans_deleted, 1);
        assert_eq!(total.orphans_held, 2);
        assert!(!hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &stale
        ));
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &fresh
        ));
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &future
        ));
    }

    // Impact: the orphan step takes the write lock that consensus apply
    // also needs; a SQLITE_BUSY there used to fail the whole shard, losing
    // its scrub, surplus release and belief/attestation pages for the
    // rotation (the class of error that wedged desktop).
    // Should: skip the orphan step while another writer holds the
    // database, count the skip, keep the files, and reap them on the next
    // visit once the lock is gone.
    // Should not: fail the shard.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_busy_database_skips_the_orphan_step_without_failing_the_shard() {
        let dir = tempfile::tempdir().unwrap();
        let frags = dir.path().join("fragments").to_string_lossy().into_owned();
        let db_path = dir.path().join("sweep.db");
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .build(crate::db::SqliteConnectionManager::file(&db_path))
            .unwrap();
        {
            let conn = pool.get().unwrap();
            crate::db::chains::install(&conn).unwrap();
            // The one pooled connection gives up on a lock fast, so the
            // test does not wait out the default busy_timeout.
            conn.execute_batch("PRAGMA busy_timeout = 50;").unwrap();
        }
        let hash = store_aged(&frags, b"an orphan behind a busy database", 3 * 3600);
        let shard = hopnet_storage::sweep::shard_of(&hash);

        // Another writer holds the database for the whole first visit.
        let locker = rusqlite::Connection::open(&db_path).unwrap();
        locker.execute_batch("BEGIN IMMEDIATE;").unwrap();
        let (deleted, diff, report) = reap_shard(&pool, &frags, shard).await;
        assert_eq!(diff.orphans.len(), 1);
        assert!(deleted.is_empty());
        assert_eq!(report.orphan_batches_skipped, 1);
        assert_eq!(report.orphans_deleted, 0);
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &hash
        ));

        locker.execute_batch("ROLLBACK;").unwrap();
        let (deleted, _, report) = reap_shard(&pool, &frags, shard).await;
        assert_eq!(deleted, vec![hash]);
        assert_eq!(report.orphan_batches_skipped, 0);
        assert!(!hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &hash
        ));
    }

    // Impact: a purged video is thousands of fragments; one transaction
    // across every unlink would hold the write lock for the whole pass, so
    // the purge works in small batches and must not stop after the first.
    // Should: release and delete every eligible file of a blob larger than
    // one batch, reporting the full count and bytes.
    #[test]
    fn a_purge_larger_than_one_batch_deletes_everything_eligible() {
        let (_dir, frags, pool) = orphan_fixture();
        let blob = CustomUUID::new(None);
        let count = UNLINK_BATCH + 7;
        let mut hashes = Vec::with_capacity(count);
        let mut bytes = 0u64;
        for i in 0..count {
            let data = format!("fragment {i} of a stuck upload").into_bytes();
            bytes += data.len() as u64;
            let hash = Blake3Hash::from_bytes(*blake3::hash(&data).as_bytes());
            hopnet_storage::fragstore::store_fragment(&frags, &hash, data).unwrap();
            hashes.push(hash);
        }
        hopnet_storage::store::record_local_uploads(
            &pool.get().unwrap(),
            &blob,
            &hashes,
            unix_now() - 7 * 86_400,
        )
        .unwrap();

        let report =
            purge_held_uploads_blocking(&pool, &frags, std::slice::from_ref(&blob), unix_now())
                .unwrap();
        assert_eq!(
            report,
            PurgedUploads {
                blobs: 1,
                skipped_recent: vec![],
                fragments_deleted: count,
                bytes_freed: bytes,
            }
        );
        assert!(
            hopnet_storage::fragstore::scan_fragments_detailed(&frags)
                .unwrap()
                .is_empty()
        );
        assert!(
            hopnet_storage::store::held_local_uploads(&pool.get().unwrap())
                .unwrap()
                .is_empty()
        );
    }

    // Impact: a purge issued while the blob's put is still streaming would
    // unlink chunks the put has written and ledgered, under a running
    // upload. A ledger-stamp age cannot tell: a slow client can take longer
    // than any fixed age per chunk. The live-put registry can.
    // Should: skip a blob with a put in flight, report it, purge the
    // others, and purge it once its put is over.
    // Should not: release or unlink anything of the skipped blob.
    #[test]
    fn a_purge_skips_a_blob_with_a_live_put() {
        let (_dir, frags, pool) = orphan_fixture();
        let streaming = CustomUUID::new(None);
        let stuck = CustomUUID::new(None);
        let store = |bytes: &[u8]| {
            let hash = Blake3Hash::from_bytes(*blake3::hash(bytes).as_bytes());
            hopnet_storage::fragstore::store_fragment(&frags, &hash, bytes.to_vec()).unwrap();
            hash
        };
        let first_chunk = store(b"streaming upload, first chunk");
        let given_up = store(b"stuck upload from last week");
        let now = unix_now();
        {
            let conn = pool.get().unwrap();
            // A slow client: the only batch so far is hours old.
            hopnet_storage::store::record_local_uploads(
                &conn,
                &streaming,
                [&first_chunk],
                now - 2 * SWEEP_ORPHAN_GRACE_SECS,
            )
            .unwrap();
            hopnet_storage::store::record_local_uploads(
                &conn,
                &stuck,
                [&given_up],
                now - 7 * 86_400,
            )
            .unwrap();
        }
        let live = hopnet_projection::host::LiveUpload::register(streaming.clone());
        // Two days on, past the purge's age guard: only the registry
        // protects the streaming blob.
        let later = now + 2 * 86_400;

        let report =
            purge_held_uploads_blocking(&pool, &frags, &[streaming.clone(), stuck.clone()], later)
                .unwrap();
        assert_eq!(
            report,
            PurgedUploads {
                blobs: 1,
                skipped_recent: vec![streaming.clone()],
                fragments_deleted: 1,
                bytes_freed: b"stuck upload from last week".len() as u64,
            }
        );
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags,
            &first_chunk
        ));
        assert!(!hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &given_up
        ));
        assert_eq!(
            hopnet_storage::store::held_local_uploads(&pool.get().unwrap())
                .unwrap()
                .iter()
                .filter(|u| u.blob_id == streaming)
                .count(),
            1,
            "the streaming blob's hold is untouched"
        );

        drop(live);
        let report =
            purge_held_uploads_blocking(&pool, &frags, std::slice::from_ref(&streaming), later)
                .unwrap();
        assert_eq!(report.blobs, 1);
        assert!(report.skipped_recent.is_empty());
        assert_eq!(report.fragments_deleted, 1);
        assert!(!hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags,
            &first_chunk
        ));
    }

    // Impact: a forward clock step stamps holds in the future; expiry
    // rightly treats them as fresh, so the purge must be able to take them
    // or they could neither expire nor be purged.
    // Should: purge a blob whose holds are stamped in the future.
    #[test]
    fn a_future_stamped_hold_is_purgeable() {
        let (_dir, frags, pool) = orphan_fixture();
        let blob = CustomUUID::new(None);
        let hash = store_aged(&frags, b"stamped after a clock step", 3 * 3600);
        hopnet_storage::store::record_local_uploads(
            &pool.get().unwrap(),
            &blob,
            [&hash],
            unix_now() + 30 * 86_400,
        )
        .unwrap();

        let report =
            purge_held_uploads_blocking(&pool, &frags, std::slice::from_ref(&blob), unix_now())
                .unwrap();
        assert_eq!(report.blobs, 1);
        assert!(report.skipped_recent.is_empty());
        assert_eq!(report.fragments_deleted, 1);
        assert!(!hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags, &hash
        ));
    }

    // Impact: the live-put registry covers only the put; its rows land
    // with a later transaction (a photo's resources all upload before its
    // one photo_add, a stalled mesh delays commits, a transaction proposed
    // before a restart can still commit after it). Purging in that window
    // unlinks the only copy of an upload that is about to be confirmed.
    // Should: skip and report a blob with no put in flight whose newest
    // hold is an hour old, leaving its hold and file in place.
    // Should: purge a blob whose newest hold is past a day old.
    #[test]
    fn a_purge_skips_a_recently_finished_upload() {
        let (_dir, frags, pool) = orphan_fixture();
        let landing = CustomUUID::new(None);
        let stuck = CustomUUID::new(None);
        let landing_hash = store_aged(&frags, b"finished an hour ago", 3600);
        let stuck_hash = store_aged(&frags, b"finished yesterday", 3600);
        let now = unix_now();
        {
            let conn = pool.get().unwrap();
            hopnet_storage::store::record_local_uploads(
                &conn,
                &landing,
                [&landing_hash],
                now - 3600,
            )
            .unwrap();
            hopnet_storage::store::record_local_uploads(
                &conn,
                &stuck,
                [&stuck_hash],
                now - 25 * 3600,
            )
            .unwrap();
        }

        let report =
            purge_held_uploads_blocking(&pool, &frags, &[landing.clone(), stuck.clone()], now)
                .unwrap();
        assert_eq!(
            report,
            PurgedUploads {
                blobs: 1,
                skipped_recent: vec![landing.clone()],
                fragments_deleted: 1,
                bytes_freed: b"finished yesterday".len() as u64,
            }
        );
        assert!(hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags,
            &landing_hash
        ));
        assert!(!hopnet_storage::fragstore::fragment_exists_and_valid(
            &frags,
            &stuck_hash
        ));
        assert_eq!(
            hopnet_storage::store::held_local_uploads(&pool.get().unwrap())
                .unwrap()
                .into_iter()
                .map(|u| u.blob_id)
                .collect::<Vec<_>>(),
            vec![landing],
            "the recent blob's hold is untouched"
        );
    }

    // Impact: at a crossing every node reboots; repair taking the grid's
    // word for "offline" re-encoded every chunk (2026.10.8).
    // Should: count a grid-offline member seen within the grace, and this
    // node itself, as up for repair.
    // Should not: count a node that has left the storage view, however
    // recently it was seen.
    #[test]
    fn repair_counts_members_within_the_grace_and_itself_as_up() {
        let members = HashSet::from([0, 1, 2, 3]);
        let in_grace = HashSet::from([1, 9]);
        let up = repair_online(&[2], &members, 3, &in_grace);
        assert_eq!(up, HashSet::from([1, 2, 3]));
    }

    fn grid_view(online: Vec<i32>, absence: &[(i32, i64)]) -> hopnet_storage::traits::StorageView {
        hopnet_storage::traits::StorageView {
            height: 1,
            members: vec![],
            tiers: Default::default(),
            weights: Default::default(),
            watermark: 18,
            online,
            absence: absence.iter().copied().collect(),
            grid_step_secs: 600,
        }
    }

    // Impact: review of #99 — every silence over ~2 min restarts the
    // contact span, so a member the grid calls offline for good (a stuck
    // sampler, a full disk) whose link also drops every few minutes (a
    // flaky link, a sleeping laptop) would restart its grace forever and
    // never be rebuilt around.
    // Should not: keep a flapping member live for repair once the grid has
    // called it offline for longer than the grace plus one grid bucket.
    // Should: keep the same member live while its grid absence is within
    // that bound (a reboot whose metrics have not landed yet).
    #[test]
    fn a_flapping_member_offline_in_the_grid_for_longer_than_the_grace_is_not_kept_live() {
        let evidence = crate::consensus::evidence::EvidenceMap::new();
        let origin = evidence.origin();
        let grace = std::time::Duration::from_secs(900);
        // Long after boot; contact resumed 2 minutes ago after a 5-minute
        // drop-out: a fresh contact span.
        evidence.record_at(2, None, origin + std::time::Duration::from_secs(3000));
        evidence.record_at(2, None, origin + std::time::Duration::from_secs(3300));
        let now = origin + std::time::Duration::from_secs(3420);
        let members = HashSet::from([1, 2]);
        let in_grace = |view: &hopnet_storage::traits::StorageView| {
            crate::consensus::evidence::repair_grace_peers(
                &evidence.snapshot(),
                origin,
                now,
                grace,
                grace_candidates(&members, view, grace),
            )
        };

        let hours_offline = grid_view(vec![1], &[(1, 0), (2, 3 * 3600)]);
        assert!(in_grace(&hours_offline).is_empty());

        let just_rebooted = grid_view(vec![1], &[(1, 0), (2, 600)]);
        assert_eq!(in_grace(&just_rebooted), HashSet::from([2]));
    }

    // Should not: drop a node the grid calls online because it is outside
    // the grace — grace only ever adds holders back.
    #[test]
    fn grace_never_removes_a_grid_online_holder() {
        let members = HashSet::from([0, 1, 2]);
        let up = repair_online(&[0, 1, 2], &members, 0, &HashSet::new());
        assert_eq!(up, HashSet::from([0, 1, 2]));
    }

    /// Records every submit as (function, payload); fails the listed
    /// 1-based call numbers.
    #[derive(Default)]
    struct Recorder {
        calls: std::sync::Mutex<Vec<(&'static str, Vec<u8>)>>,
        fail_on: Vec<usize>,
    }

    impl TxSubmitter for Recorder {
        async fn submit(
            &self,
            function: &'static str,
            payload: Vec<u8>,
        ) -> Result<(), hopnet_storage::traits::SubmitError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((function, payload));
            if self.fail_on.contains(&calls.len()) {
                Err(hopnet_storage::traits::SubmitError::Transient(
                    "timeout".into(),
                ))
            } else {
                Ok(())
            }
        }
    }

    fn h(b: u8) -> Blake3Hash {
        Blake3Hash::from_bytes([b; 32])
    }

    // Impact: attestation stamps only rows that exist, so belief must go
    // out before truth; and a page stamped later than its fragments were
    // seen would overstate freshness (rolling-sweep decision 2).
    // Should: submit belief pages before attestation pages, each stamped
    // with the lowest height its hashes were observed at.
    // Should: carry on to the attestation when a belief page fails.
    // Should not: submit anything while neither buffer is due.
    #[tokio::test]
    async fn flush_sends_belief_then_truth_at_the_heights_seen() {
        let rec = Recorder {
            fail_on: vec![1],
            ..Default::default()
        };
        let mut rot = Rotation::new(7, 0);
        let now = unix_now();
        rot.belief.added.push(40, now, [h(1)]);
        rot.truth.push(42, now, [h(1)]);
        rot.truth.push(41, now, [h(2)]);
        flush_buffers(&rec, &mut rot, false).await;
        assert!(rec.calls.lock().unwrap().is_empty(), "nothing due yet");

        flush_buffers(&rec, &mut rot, true).await;
        let calls = rec.calls.lock().unwrap();
        let functions: Vec<_> = calls.iter().map(|(f, _)| *f).collect();
        assert_eq!(
            functions,
            vec![
                hopnet_storage::engine::policy::SELF_CHECK_FN,
                hopnet_storage::engine::policy::ATTEST_FN
            ]
        );
        let (attestation, _): (hopnet_storage::FragmentAttestation, _) =
            bincode::serde::decode_from_slice(&calls[1].1, bincode::config::standard()).unwrap();
        assert_eq!(attestation.height, 41);
        assert_eq!(attestation.present, vec![h(1), h(2)]);
        assert_eq!(
            (rot.report.belief_failed_pages, rot.report.attested_pages),
            (1, 1)
        );
    }

    // Impact: the cursor exists so a restart-looping node still completes
    // rotations; saving it past shards whose belief and truth are only
    // buffered in memory let a restart drop them while the cursor claimed
    // them done (code review of PR #95).
    // Should: persist the cursor once a flush has drained both buffers.
    // Should not: persist it while either buffer still holds a passed
    // shard's hashes.
    #[tokio::test]
    async fn cursor_is_persisted_only_after_the_buffers_drain() {
        let rec = Recorder::default();
        let mut rot = Rotation::new(7, 0);
        let next = hopnet_storage::sweep::SweepCursor::fresh(0, 10);
        assert_eq!(cursor_to_persist(&rot, next), Some(next), "empty buffers");

        rot.truth.push(42, unix_now(), [h(1)]);
        assert_eq!(cursor_to_persist(&rot, next), None, "truth buffered");
        flush_buffers(&rec, &mut rot, true).await;
        assert_eq!(cursor_to_persist(&rot, next), Some(next), "drained");

        rot.belief.removed.push(42, unix_now(), [h(2)]);
        assert_eq!(cursor_to_persist(&rot, next), None, "belief buffered");
    }

    // Should: hand each shard's scrub out once per day, and again on a
    // new day.
    #[test]
    fn scrub_is_claimed_once_per_shard_per_day() {
        let day = 1_000_000;
        assert!(claim_scrub(day, 3));
        assert!(!claim_scrub(day, 3));
        assert!(claim_scrub(day, 4));
        assert!(claim_scrub(day + 1, 3));
    }

    // Impact: review of #100 at bbf1cf58 — the re-kick reported
    // `held_for_space` only for a pause at entry and dropped the classes
    // held during the batch, so a batch the floor cut short read as fine.
    // Should: report held for space, with the class count, whenever classes
    // were held during the batch.
    // Should not: report held for a batch that held nothing.
    #[test]
    fn the_rekick_reports_classes_held_for_space() {
        let held = hopnet_storage::engine::PullStats {
            checked: 4,
            pulled: 6,
            held_for_space: 9,
            ..Default::default()
        };
        let result = rebalancing_result(7, &held);
        assert!(result.held_for_space);
        assert_eq!(result.fragments_held_for_space, 9);

        let paused = hopnet_storage::engine::PullStats {
            held_back: true,
            ..Default::default()
        };
        assert!(rebalancing_result(7, &paused).held_for_space);

        let clean = hopnet_storage::engine::PullStats {
            checked: 4,
            pulled: 6,
            ..Default::default()
        };
        assert!(!rebalancing_result(7, &clean).held_for_space);
    }
}
