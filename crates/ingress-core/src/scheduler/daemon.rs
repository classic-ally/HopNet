//! The daemon loop: `drain` that never exits. Observer events and scan
//! re-deliveries arrive over a channel (the platform side pushes — spec
//! §Process model), get classified and applied between admission rounds, and
//! the loop sleeps on the earliest retry deadline when idle.
//!
//! ## Race story
//!
//! A `photo_task` loads its `library_id`/blob paths once at task start, so a
//! change applied mid-task (hard move, revert) could relocate state under it.
//! Prevention is structural, not locked:
//!
//! - Events for photos in the inflight set are **deferred** into a per-photo
//!   FIFO (order preserved — a deferred edit must not reorder after a later
//!   revert) and applied once the task finishes.
//! - Photos with deferred events are **not admitted** (a queued hard move
//!   must not start fetching into the old root).
//! - Unknown photos can't be inflight — their events always apply directly.
//!
//! Holding a per-photo lock instead would stall the single classification
//! path behind multi-gigabyte streams.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::classify::{RemovalOutcome, apply_change, apply_removal};
use crate::descriptor::AssetDescriptor;
use crate::error::Result;
use crate::ids::PhotoId;
use crate::scan::ScanState;
use crate::store::StateStore;

use super::{DrainReport, ResourceFetcher, Scheduler};

/// One pushed discovery event.
#[derive(Debug)]
pub enum ChangeEvent {
    /// Observer insert/change, or a scan `NeedsFull` re-delivery. Boxed —
    /// descriptors dwarf the removal variant.
    Descriptor(Box<AssetDescriptor>),
    /// Observer removal — `local_id` is the only handle a removed asset
    /// still exposes.
    Removed { local_id: String },
}

/// The daemon's inbox, shared with the FFI session: event sender, an idle
/// wake, and the at-most-one active scan.
pub struct DaemonHandle {
    events: mpsc::UnboundedSender<ChangeEvent>,
    wake: tokio::sync::Notify,
    /// The active reconciliation scan, if any (`begin_scan` errors on a
    /// second). Shared so `scan::probe` marks seen photos while the loop
    /// belt-and-braces marks the ones it classifies.
    pub scan: Mutex<Option<Arc<ScanState>>>,
}

impl DaemonHandle {
    /// Build the handle + the receiver `run_daemon` consumes. Events pushed
    /// before the loop starts buffer in the channel.
    pub fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<ChangeEvent>) {
        let (events, rx) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                events,
                wake: tokio::sync::Notify::new(),
                scan: Mutex::new(None),
            }),
            rx,
        )
    }

    /// Queue an event and wake the loop. Never blocks — safe from the
    /// platform observer's callback queue. Errors only after the daemon
    /// stopped (receiver dropped), which callers may ignore.
    pub fn push(&self, event: ChangeEvent) -> bool {
        let ok = self.events.send(event).is_ok();
        self.wake.notify_one();
        ok
    }

    /// Nudge an idle loop (e.g. after `finish_scan` re-enqueued gave-up rows).
    pub fn wake(&self) {
        self.wake.notify_one();
    }
}

/// Daemon outcome counters: everything `drain` reports, plus the event side
/// and the lifecycle-tick aggregates.
#[derive(Debug, Default, Clone)]
pub struct DaemonReport {
    pub drain: DrainReport,
    pub events_applied: u64,
    pub events_deferred: u64,
    pub deletions: u64,
    pub restores: u64,
    pub transitions: u64,
    pub resources_reopened: u64,
    pub cleanup: crate::cleanup::CleanupReport,
    pub publish: crate::publish::PublishReport,
}

#[derive(Default)]
struct EventCounters {
    applied: u64,
    deferred: u64,
    deletions: u64,
    restores: u64,
    transitions: u64,
    reopened: u64,
}

impl<F: ResourceFetcher> Scheduler<F> {
    /// Run until cancellation: apply queued events, flush deferrals, admit
    /// pending work, and idle on (event | task-exit | wake | earliest retry).
    pub async fn run_daemon(
        &self,
        mut rx: mpsc::UnboundedReceiver<ChangeEvent>,
        handle: Arc<DaemonHandle>,
    ) -> Result<DaemonReport> {
        let (_lock, swept) = self.prepare().await?;
        let mut tasks: JoinSet<()> = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.shared.config.fetch_concurrency,
        ));
        let mut deferred: HashMap<PhotoId, VecDeque<ChangeEvent>> = HashMap::new();
        let mut counters = EventCounters::default();
        // Lifecycle timers: None = due immediately, so a boot runs the
        // startup cleanup.
        let mut last_cleanup: Option<std::time::Instant> = None;
        let mut last_publish: Option<std::time::Instant> = None;
        let mut cleanup_totals = crate::cleanup::CleanupReport::default();
        // Publish pass state, shared with the spawned pass task (one alive at
        // a time — the tick is gated on the previous task having finished).
        let publish_totals = Arc::new(Mutex::new(crate::publish::PublishReport::default()));
        let publish_state = Arc::new(Mutex::new(crate::publish::PublishState::default()));
        let mut publish_task: Option<tokio::task::JoinHandle<()>> = None;

        fn due(last: Option<std::time::Instant>, interval: std::time::Duration) -> bool {
            last.map(|t| t.elapsed() >= interval).unwrap_or(true)
        }

        loop {
            if self.shared.cancel.is_cancelled() {
                break;
            }
            // Placed after the pause check: a storage pause implies the
            // mount is suspect — don't hammer it with lifecycle fs work.
            if self.paused_wait().await {
                continue;
            }

            // 0. Lifecycle ticks (serialized with event application — no
            //    race with restores; photo_tasks are unaffected). A failed
            //    tick logs and retries next interval, never exits the loop.
            if due(last_cleanup, self.shared.config.cleanup_interval) {
                match crate::cleanup::run_cleanup(
                    &self.shared.store,
                    &self.shared.data_dir,
                    &self.shared.config.cleanup,
                    Utc::now(),
                )
                .await
                {
                    Ok(r) => cleanup_totals.absorb(&r),
                    Err(e) => {
                        let _ = self
                            .shared
                            .store
                            .append_log(
                                "cleanup_error",
                                None,
                                Some(serde_json::json!({ "error": e.to_string() })),
                            )
                            .await;
                    }
                }
                last_cleanup = Some(std::time::Instant::now());
            }
            // Publish tick: claim in-loop (fast indexed query), register the
            // claimed photos INFLIGHT (their PhotoKit events defer, which
            // also excludes supersede/hard-move races on the blob reads),
            // then run the pass in ONE spawned task — unlike replication,
            // publishing streams multi-GB originals, and inline it would
            // stall event routing for the duration.
            if let Some(publisher) = &self.publisher {
                let alive = publish_task.as_ref().is_some_and(|t| !t.is_finished());
                if !alive && due(last_publish, self.shared.config.publish.interval) {
                    let skip = self.shared.inflight.lock().expect("inflight mutex").clone();
                    // All three work lists are claimed together so one pass
                    // covers uploads, tombstone propagation and edits under
                    // a single resolve per scope.
                    let propagatable = match crate::publish::claim_tombstone_propagatable(
                        &self.shared.store,
                        &self.shared.config.publish,
                        &skip,
                    )
                    .await
                    {
                        Ok(rows) => rows,
                        Err(e) => {
                            let _ = self
                                .shared
                                .store
                                .append_log(
                                    "publish_error",
                                    None,
                                    Some(serde_json::json!({
                                        "error": format!("claim propagatable: {e}")
                                    })),
                                )
                                .await;
                            Vec::new()
                        }
                    };
                    let editable = match crate::publish::claim_editable(
                        &self.shared.store,
                        &self.shared.config.publish,
                        &skip,
                    )
                    .await
                    {
                        Ok(rows) => rows,
                        Err(e) => {
                            let _ = self
                                .shared
                                .store
                                .append_log(
                                    "publish_error",
                                    None,
                                    Some(serde_json::json!({
                                        "error": format!("claim editable: {e}")
                                    })),
                                )
                                .await;
                            Vec::new()
                        }
                    };
                    match crate::publish::claim_publishable(
                        &self.shared.store,
                        &self.shared.config.publish,
                        &skip,
                    )
                    .await
                    {
                        Ok(claimed)
                            if !claimed.is_empty()
                                || !propagatable.is_empty()
                                || !editable.is_empty() =>
                        {
                            // Inflight covers ALL THREE sets: a restore
                            // event landing under an in-flight delete
                            // submission would clear deleted_at just as the
                            // pass stamps the marker, leaving a live photo
                            // recorded as converged-deleted — and a PhotoKit
                            // re-edit landing mid-submission would swap the
                            // content hash under a marker about to record
                            // the bytes that were actually sent.
                            let ids: Vec<PhotoId> = claimed
                                .iter()
                                .chain(propagatable.iter())
                                .chain(editable.iter())
                                .map(|p| p.photo_id.clone())
                                .collect();
                            self.shared
                                .inflight
                                .lock()
                                .expect("inflight mutex")
                                .extend(ids.iter().cloned());
                            let shared = self.shared.clone();
                            let publisher = publisher.clone();
                            let totals = publish_totals.clone();
                            let state_slot = publish_state.clone();
                            publish_task = Some(tokio::spawn(async move {
                                let mut state =
                                    state_slot.lock().expect("publish state").clone();
                                let result = crate::publish::run_publish_pass(
                                    &shared.store,
                                    &shared.data_dir.spool(),
                                    &publisher,
                                    &shared.config.publish,
                                    crate::publish::PassWork {
                                        claimed,
                                        propagatable,
                                        editable,
                                    },
                                    &mut state,
                                )
                                .await;
                                *state_slot.lock().expect("publish state") = state;
                                match result {
                                    Ok(r) => {
                                        totals.lock().expect("publish totals").absorb(&r)
                                    }
                                    Err(e) => {
                                        let _ = shared
                                            .store
                                            .append_log(
                                                "publish_error",
                                                None,
                                                Some(serde_json::json!({
                                                    "error": e.to_string()
                                                })),
                                            )
                                            .await;
                                    }
                                }
                                let mut inflight =
                                    shared.inflight.lock().expect("inflight mutex");
                                for id in &ids {
                                    inflight.remove(id);
                                }
                            }));
                        }
                        Ok(_) => {}
                        Err(e) => {
                            let _ = self
                                .shared
                                .store
                                .append_log(
                                    "publish_error",
                                    None,
                                    Some(serde_json::json!({
                                        "op": "claim",
                                        "error": e.to_string()
                                    })),
                                )
                                .await;
                        }
                    }
                    last_publish = Some(std::time::Instant::now());
                }
            }

            // 1. Route everything currently queued.
            while let Ok(event) = rx.try_recv() {
                self.route_event(event, &handle, &mut deferred, &mut counters)
                    .await;
            }

            // 2. Flush deferrals whose photo is no longer inflight. Safe
            //    without a lock: admission happens only below in this same
            //    loop, and tasks only ever REMOVE from the inflight set.
            let flushable: Vec<PhotoId> = {
                let inflight = self.shared.inflight.lock().expect("inflight mutex");
                deferred
                    .keys()
                    .filter(|id| !inflight.contains(*id))
                    .cloned()
                    .collect()
            };
            for id in flushable {
                if let Some(queue) = deferred.remove(&id) {
                    for event in queue {
                        self.apply_event(event, &handle, &mut counters).await;
                    }
                }
            }

            // 3. Admit pending work, skipping photos with deferred events.
            let skip: HashSet<PhotoId> = deferred.keys().cloned().collect();
            let claimable = self.claim_batch(&skip).await?;
            if !claimable.is_empty() {
                self.spawn_all(&mut tasks, &semaphore, claimable).await;
                continue;
            }
            if !deferred.is_empty() && !tasks.is_empty() {
                // Deferred events unblock on the next task exit.
                let _ = tasks.join_next().await;
                continue;
            }

            // 4. Idle: wake on a new event, a task exit (may unblock
            //    deferrals or free capacity), an external nudge, the
            //    earliest retry deadline, or the next lifecycle tick (an
            //    idle daemon must not skew lifecycle cadence to the retry
            //    default).
            let summary = self
                .shared
                .store
                .retry_summary(self.shared.config.retry_cap)
                .await?;
            let next_retry = summary
                .earliest_next_retry_at
                .and_then(|t| (t - Utc::now()).to_std().ok())
                .unwrap_or(std::time::Duration::from_secs(3600));
            let time_to = |last: Option<std::time::Instant>, interval: std::time::Duration| {
                last.map(|t| interval.saturating_sub(t.elapsed()))
                    .unwrap_or_default()
            };
            let mut next_retry =
                next_retry.min(time_to(last_cleanup, self.shared.config.cleanup_interval));
            if self.publisher.is_some() {
                next_retry =
                    next_retry.min(time_to(last_publish, self.shared.config.publish.interval));
            }
            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Some(event) => {
                            self.route_event(event, &handle, &mut deferred, &mut counters).await
                        }
                        None => break, // all senders dropped: session gone
                    }
                }
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
                // A finished publish pass frees its inflight photos — wake so
                // their deferred events flush promptly. The handler clears
                // the slot: this await consumed the handle's output, and the
                // shutdown join would panic re-polling it.
                _ = async { let _ = publish_task.as_mut().expect("guarded").await; },
                    if publish_task.as_ref().is_some_and(|t| !t.is_finished()) => {
                    publish_task = None;
                }
                _ = handle.wake.notified() => {}
                _ = self.shared.cancel.cancelled() => {}
                _ = tokio::time::sleep(next_retry) => {}
            }
        }

        while tasks.join_next().await.is_some() {}
        if let Some(task) = publish_task.take() {
            let _ = task.await;
        }
        // Deferred events still queued at shutdown are dropped — the next
        // startup scan re-derives them from PhotoKit state (events are
        // hints, not authoritative deltas).

        Ok(DaemonReport {
            drain: self.report(swept).await?,
            events_applied: counters.applied,
            events_deferred: counters.deferred,
            deletions: counters.deletions,
            restores: counters.restores,
            transitions: counters.transitions,
            resources_reopened: counters.reopened,
            cleanup: cleanup_totals,
            publish: publish_totals.lock().expect("publish totals").clone(),
        })
    }

    /// Apply now, or defer when the photo has a live task (or already-queued
    /// deferrals — FIFO per photo).
    async fn route_event(
        &self,
        event: ChangeEvent,
        handle: &DaemonHandle,
        deferred: &mut HashMap<PhotoId, VecDeque<ChangeEvent>>,
        counters: &mut EventCounters,
    ) {
        let photo_id = match self.resolve_event_photo(&event).await {
            Ok(id) => id,
            Err(e) => {
                self.log_event_error(&event, &e).await;
                return;
            }
        };
        if let Some(id) = photo_id {
            let inflight = self
                .shared
                .inflight
                .lock()
                .expect("inflight mutex")
                .contains(&id);
            if inflight || deferred.contains_key(&id) {
                deferred.entry(id).or_default().push_back(event);
                counters.deferred += 1;
                return;
            }
        }
        self.apply_event(event, handle, counters).await;
    }

    /// Cheap identity peek for deferral routing (no plan, no writes).
    async fn resolve_event_photo(&self, event: &ChangeEvent) -> Result<Option<PhotoId>> {
        let store: &StateStore = &self.shared.store;
        match event {
            ChangeEvent::Descriptor(desc) => match desc.cloud_id.as_deref() {
                Some(cloud_id) => Ok(store.photo_by_cloud_id(cloud_id).await?.map(|p| p.photo_id)),
                None => Ok(crate::store::photos::photo_by_local_id_no_cloud(
                    store.pool(),
                    &desc.local_id,
                )
                .await?
                .map(|p| p.photo_id)),
            },
            ChangeEvent::Removed { local_id } => Ok(
                crate::store::photos::photo_by_local_id_active(store.pool(), local_id)
                    .await?
                    .map(|p| p.photo_id),
            ),
        }
    }

    async fn apply_event(
        &self,
        event: ChangeEvent,
        handle: &DaemonHandle,
        counters: &mut EventCounters,
    ) {
        let scan = handle.scan.lock().expect("scan mutex").clone();
        match &event {
            ChangeEvent::Descriptor(desc) => {
                match apply_change(&self.shared.store, &self.shared.data_dir.spool(), desc).await {
                    Ok((classification, outcome)) => {
                        counters.applied += 1;
                        counters.restores += outcome.restored as u64;
                        counters.transitions += outcome.transitioned as u64;
                        counters.reopened += outcome.resources_reopened;
                        // Belt-and-braces seen-marking (probe-time marking is
                        // primary): an event applied during a scan proves the
                        // photo is alive.
                        if let Some(scan) = &scan
                            && let Some(id) = classification_photo_id(&classification)
                        {
                            crate::scan::mark_seen(scan, id);
                        }
                    }
                    Err(e) => self.log_event_error(&event, &e).await,
                }
            }
            ChangeEvent::Removed { local_id } => {
                match apply_removal(&self.shared.store, local_id).await {
                    Ok(outcome) => {
                        counters.applied += 1;
                        if matches!(outcome, RemovalOutcome::Tombstoned { .. }) {
                            counters.deletions += 1;
                        }
                    }
                    Err(e) => self.log_event_error(&event, &e).await,
                }
            }
        }
    }

    /// One failed event must never take the loop down; log and move on (the
    /// scan re-derives anything dropped).
    async fn log_event_error(&self, event: &ChangeEvent, e: &crate::IngressError) {
        let what = match event {
            ChangeEvent::Descriptor(d) => format!("descriptor {}", d.local_id),
            ChangeEvent::Removed { local_id } => format!("removed {local_id}"),
        };
        let _ = self
            .shared
            .store
            .append_log(
                "daemon_event_error",
                None,
                Some(serde_json::json!({ "event": what, "error": e.to_string() })),
            )
            .await;
    }
}

fn classification_photo_id(c: &crate::classify::Classification) -> Option<&PhotoId> {
    use crate::classify::Classification;
    use crate::resolve::SeedOutcome;
    match c {
        Classification::Known(plan) => Some(&plan.photo_id),
        Classification::NoOp { photo_id } => Some(photo_id),
        Classification::Seeded(
            SeedOutcome::AlreadyKnown { photo_id }
            | SeedOutcome::Adopted { photo_id }
            | SeedOutcome::MintedPending { photo_id, .. }
            | SeedOutcome::Unmapped { photo_id },
        ) => Some(photo_id),
    }
}
