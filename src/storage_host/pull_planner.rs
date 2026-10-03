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

/// Rest after a pass that found nothing owed.
pub const PLANNER_IDLE_SECS: u64 = 60;

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
    /// Of those, blobs this node owes at least one class of.
    pub owed_blobs: usize,
    /// Owed blobs at member fault tolerance 0 or below.
    pub at_risk_blobs: usize,
    /// Blobs handed to the engine since the pass started.
    pub offered: usize,
    /// Pull checks queued on the engine when the report was taken.
    pub queued: usize,
}

static REPORT: Mutex<Option<PlannerReport>> = Mutex::new(None);
static TASK: Mutex<Option<tokio::task::JoinHandle<()>>> = Mutex::new(None);

/// The latest planner report, with the engine's current queue depth.
pub fn report(app_state: &AppState) -> PlannerReport {
    let mut report = REPORT.lock().unwrap().clone().unwrap_or_default();
    report.queued = app_state.storage.get().map_or(0, |e| e.queued_len());
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

async fn run(app_state: AppState) {
    loop {
        let Some(engine) = app_state.storage.get().cloned() else {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        };
        let started = std::time::Instant::now();
        let started_at = chrono::Utc::now().timestamp();
        let pass = {
            let app_state = app_state.clone();
            tokio::task::spawn_blocking(move || plan(&app_state)).await
        };
        let (plan, scanned) = match pass {
            Ok(Ok(p)) => p,
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
        let at_risk_blobs = plan.iter().filter(|i| i.tolerance <= 0).count();
        *REPORT.lock().unwrap() = Some(PlannerReport {
            pass_started_at: started_at,
            pass_ms: started.elapsed().as_millis() as u64,
            scanned,
            owed_blobs: plan.len(),
            at_risk_blobs,
            offered: 0,
            queued: engine.queued_len(),
        });
        if !plan.is_empty() {
            tracing::info!(
                scanned,
                owed = plan.len(),
                at_risk = at_risk_blobs,
                pass_ms = started.elapsed().as_millis() as u64,
                "pull planner: pass planned"
            );
        }

        let found_work = !plan.is_empty();
        for item in plan {
            while engine.queued_len() >= PULL_QUEUE_TARGET {
                tokio::time::sleep(FEED_POLL).await;
            }
            if engine.offer(item.blob_id)
                && let Some(r) = REPORT.lock().unwrap().as_mut()
            {
                r.offered += 1;
            }
        }
        if !found_work {
            tokio::time::sleep(Duration::from_secs(PLANNER_IDLE_SECS)).await;
        }
    }
}

/// One planning pass over the in-flight set: a fresh pool connection per
/// page, so no read is held across the whole walk.
pub fn plan(app_state: &AppState) -> Result<(Vec<PlanItem>, usize), String> {
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
    planner::plan_pass(|after| {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| hopnet_storage::StorageError::Host(format!("pool: {e}")))?;
        planner::plan_page(
            &conn,
            after.as_ref().map(|(h, id)| (*h, id)),
            me,
            &members,
            &mut snapshots,
        )
    })
    .map_err(|e| e.to_string())
}
