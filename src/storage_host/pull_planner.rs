//! The pull planner (RFC-STORAGE-003 S3): what wakes the reconciler.
//!
//! Replaces the policy tick's 64-blob kick, which re-sent the same lowest
//! ids every tick once one transition gave every in-flight blob the same
//! goal (production, 2026-10-03: ~68.8k blobs never pulled). A pass walks
//! the whole in-flight set page by page on the blocking pool, keeps the
//! blobs this node owes a class of, and orders them at-risk first by the
//! resilience pane's rule (`hopnet_storage::planner`). The plan feeds the
//! engine as its queue drains, not on a cron, and a new pass starts as
//! soon as the last one is used up. State is in memory only: a restart
//! replans from scratch, which costs one pass (seconds on a fast node).

use crate::AppState;
use hopnet_storage::planner::{self, PlanItem};
use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

/// Pull checks the planner keeps queued on the engine. Enough to keep the
/// worker busy between refills without building a long stale backlog.
pub const PULL_QUEUE_TARGET: usize = 128;

/// Rest after a pass that walked to the end of the set and found nothing
/// owed.
pub const PLANNER_IDLE_SECS: u64 = 60;

/// Passes start at most this often: with most owed blobs parked a pass
/// drains its plan at once, and back-to-back passes would re-read the
/// same manifests continuously.
pub const PLANNER_MIN_PASS_SECS: u64 = 30;

/// In-flight blobs one pass reads (manifest plus inventory probes each);
/// the next pass resumes where it stopped. At 68.8k in-flight blobs a full
/// walk takes ~5 passes, ~2.5 min at the minimum interval.
pub const PLAN_BLOBS_PER_PASS: usize = 16_384;

/// How often the feeder looks at the engine's queue.
const FEED_POLL: Duration = Duration::from_millis(500);

/// What the last pass found and fed — the policy tick's report field.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct PlannerReport {
    /// Unix seconds the last pass started.
    pub pass_started_at: i64,
    pub pass_ms: u64,
    /// In-flight blobs the pass read.
    pub scanned: usize,
    /// Owed blobs waiting in the priority book (every slice read so far).
    pub owed_blobs: usize,
    /// Owed blobs at member fault tolerance 0 or below.
    pub at_risk_blobs: usize,
    /// Blobs the feeder has handed the engine since the planner started.
    pub offered: usize,
    /// Pull checks queued on the engine when the report was taken.
    pub queued: usize,
    /// The fetch scheduler at report time: window and fetches in use,
    /// per-peer load, parked peers and blobs.
    pub scheduler: hopnet_storage::engine::fetch::SchedulerStats,
}

static REPORT: Mutex<Option<PlannerReport>> = Mutex::new(None);
static TASK: Mutex<Option<tokio::task::JoinHandle<()>>> = Mutex::new(None);

/// The latest planner report, with the engine's current queue depth.
pub fn report(app_state: &AppState) -> PlannerReport {
    let mut report = REPORT.lock().unwrap().clone().unwrap_or_default();
    if let Some(engine) = app_state.storage.get() {
        report.queued = engine.queued_len();
        report.scheduler = engine.scheduler_stats();
        if report.scheduler.parked_blobs > 0 {
            tracing::info!(
                parked_blobs = report.scheduler.parked_blobs,
                parked_peers = ?report.scheduler.parked_peers.iter().map(|p| p.node_id).collect::<Vec<_>>(),
                longest_secs = report.scheduler.parked_longest_secs,
                "pull: blobs parked on unreachable sources"
            );
        }
    }
    report
}

/// Start the planner if it is not running (first engine spawn, or the
/// policy tick's heartbeat after the task died). Returns whether it was
/// (re)started.
pub fn ensure_running(app_state: &AppState) -> bool {
    let mut task = TASK.lock().unwrap();
    if task.as_ref().is_some_and(|t| !t.is_finished()) {
        return false;
    }
    if task.is_some() {
        tracing::warn!("pull planner: task had stopped; restarting");
    }
    *task = Some(tokio::spawn(run(app_state.clone())));
    true
}

/// The owed blobs known so far, in global at-risk-first order; passes
/// absorb slices into it and the feeder offers from its head.
type Book = std::sync::Arc<Mutex<planner::PriorityBook>>;

/// Offer the book's most at-risk blob whenever the engine's queue has
/// room, independently of the planning passes, so an at-risk blob found
/// in any slice is fed without waiting for the current slice to drain.
async fn feed(app_state: AppState, book: Book) {
    loop {
        let Some(engine) = app_state.storage.get().cloned() else {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        };
        if engine.queued_len() >= PULL_QUEUE_TARGET {
            tokio::time::sleep(FEED_POLL).await;
            continue;
        }
        let next = book.lock().unwrap().pop();
        let Some(blob_id) = next else {
            tokio::time::sleep(FEED_POLL).await;
            continue;
        };
        // Parked since it was planned: its next slice read brings it back
        // once the park ends.
        if engine.blob_parked(&blob_id) {
            continue;
        }
        if engine.offer(blob_id)
            && let Some(r) = REPORT.lock().unwrap().as_mut()
        {
            r.offered += 1;
        }
    }
}

async fn run(app_state: AppState) {
    let book: Book = Default::default();
    let feeder = tokio::spawn(feed(app_state.clone(), book.clone()));
    // The feeder dies with the planner (the tick restarts both).
    struct AbortOnDrop(tokio::task::JoinHandle<()>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _feeder = AbortOnDrop(feeder);
    let mut resume: Option<hopnet_storage::BlobId> = None;
    loop {
        let Some(engine) = app_state.storage.get().cloned() else {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        };
        let started = std::time::Instant::now();
        let started_at = chrono::Utc::now().timestamp();
        let start = resume.take();
        let pass = {
            let (app_state, engine, start) = (app_state.clone(), engine.clone(), start.clone());
            tokio::task::spawn_blocking(move || {
                plan_from(&app_state, start, PLAN_BLOBS_PER_PASS, &|id| {
                    engine.blob_parked(id)
                })
            })
            .await
        };
        let (found_work, scanned) = match pass {
            Ok(Ok(p)) => {
                resume = p.resume;
                let found = !p.plan.is_empty();
                book.lock()
                    .unwrap()
                    .absorb(start.as_ref(), resume.as_ref(), p.plan);
                (found, p.scanned)
            }
            Ok(Err(e)) => {
                tracing::warn!("pull planner: pass failed: {e}");
                tokio::time::sleep(Duration::from_secs(PLANNER_IDLE_SECS)).await;
                continue;
            }
            Err(e) => {
                tracing::warn!("pull planner: pass join failed: {e}");
                tokio::time::sleep(Duration::from_secs(PLANNER_IDLE_SECS)).await;
                continue;
            }
        };
        let (owed_blobs, at_risk_blobs) = {
            let book = book.lock().unwrap();
            (book.len(), book.at_risk())
        };
        let offered = REPORT.lock().unwrap().as_ref().map_or(0, |r| r.offered);
        *REPORT.lock().unwrap() = Some(PlannerReport {
            pass_started_at: started_at,
            pass_ms: started.elapsed().as_millis() as u64,
            scanned,
            owed_blobs,
            at_risk_blobs,
            offered,
            queued: engine.queued_len(),
            scheduler: engine.scheduler_stats(),
        });
        if found_work {
            tracing::info!(
                scanned,
                owed = owed_blobs,
                at_risk = at_risk_blobs,
                pass_ms = started.elapsed().as_millis() as u64,
                "pull planner: slice planned"
            );
        }
        if resume.is_none() {
            // A full walk ended: forget parks nothing has renewed.
            let pruned = engine.prune_parks();
            if pruned > 0 {
                tracing::debug!(pruned, "pull planner: stale park entries dropped");
            }
        }
        tokio::time::sleep(pass_rest(found_work, resume.is_none(), started.elapsed())).await;
    }
}

/// How long to rest before the next pass, which may start no sooner than
/// the minimum interval after this one started: one that reached the end
/// of the set with nothing owed rests for the idle interval instead.
pub fn pass_rest(found_work: bool, reached_end: bool, elapsed: Duration) -> Duration {
    let interval = if !found_work && reached_end {
        PLANNER_IDLE_SECS
    } else {
        PLANNER_MIN_PASS_SECS
    };
    Duration::from_secs(interval).saturating_sub(elapsed)
}

/// One full, unbounded planning pass over the in-flight set (the operator
/// route's order).
pub fn plan(app_state: &AppState) -> Result<(Vec<PlanItem>, usize), String> {
    plan_from(app_state, None, usize::MAX, &|_| false).map(|p| (p.plan, p.scanned))
}

/// A planning pass from `start`, reading at most `budget` in-flight blobs,
/// with a fresh pool connection per page so no read is held across the
/// walk. Blobs `skip` names cost no lookups.
pub fn plan_from(
    app_state: &AppState,
    start: Option<hopnet_storage::BlobId>,
    budget: usize,
    skip: &dyn Fn(&hopnet_storage::BlobId) -> bool,
) -> Result<planner::PlannedPass, String> {
    let me = app_state
        .get_node_id()
        .map_err(|_| "node id not set".to_string())?;
    let members: BTreeSet<i32> = {
        let conn = app_state.db_pool.get().map_err(|e| format!("pool: {e}"))?;
        crate::storage_host::substrate_host::storage_view_with_conn(&conn)
            .map_err(|e| format!("storage view: {e}"))?
            .members
            .iter()
            .map(|p| p.node_id)
            .collect()
    };
    let mut snapshots = HashMap::new();
    planner::plan_pass(start, budget, |after| {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| hopnet_storage::StorageError::Host(format!("pool: {e}")))?;
        planner::plan_page(&conn, after.as_ref(), me, &members, &mut snapshots, skip)
    })
    .map_err(|e| e.to_string())
}
