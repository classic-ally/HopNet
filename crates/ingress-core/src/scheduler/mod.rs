//! The pipeline scheduler (spec §Ingest Pipeline): drains pending rows from
//! `state.db` through a [`ResourceFetcher`] under bounded concurrency, with
//! retry/backoff, storage-aware admission, pause handling, and cooperative
//! cancellation. `state.db` is the work queue; this module only orchestrates.

pub mod admission;
pub mod backoff;
pub mod daemon;
pub mod fetcher;
pub mod locks;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::task::JoinSet;

use crate::descriptor::AssetDescriptor;
use crate::error::{IngressError, Result};
use crate::ext::ext_for_uti;
use crate::ids::PhotoId;
use crate::model::{LibraryConfig, PhotoRecord, ResourceType};
use crate::paths::{BlobPaths, DataDir, TempKey};
use crate::resolve::late_binding_merge;
use crate::sidecar_io::write_photo_sidecar;
use crate::store::{StateStore, photos};
use crate::writer::{ResourceWrite, finalize_resource, sweep_partials};

pub use admission::{FreeSpaceProbe, StatvfsProbe};
pub use backoff::BackoffConfig;
pub use fetcher::{CancelToken, FetchFailure, FetchRequest, ResourceFetcher, StreamSink};

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub fetch_concurrency: usize,
    pub retry_cap: i64,
    pub backoff: BackoffConfig,
    pub reserve_floor_bytes: u64,
    pub pressure_pause: Duration,
    pub storage_poll: Duration,
    /// Fallback pessimistic size when neither descriptor nor blob history
    /// offers an estimate (fresh library).
    pub default_size_estimate: u64,
    /// Daemon-loop cadence of the hourly lifecycle job (hard deletes, log
    /// pruning, snapshots).
    pub cleanup_interval: Duration,
    /// Daemon-loop cadence of the dirty-sidecar replication drain — faster
    /// than cleanup so the remote backup tracks changes closely, batch-capped
    /// so one tick never stalls the loop.
    pub replication_interval: Duration,
    pub cleanup: crate::cleanup::CleanupConfig,
    /// HopNet publish tick (active only when a publisher is attached via
    /// `Scheduler::with_publisher`).
    pub publish: crate::publish::PublishConfig,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            fetch_concurrency: 4,
            retry_cap: 5,
            backoff: BackoffConfig::default(),
            reserve_floor_bytes: 10 * 1024 * 1024 * 1024,
            pressure_pause: Duration::from_secs(60),
            storage_poll: Duration::from_secs(15),
            default_size_estimate: 64 * 1024 * 1024,
            cleanup_interval: Duration::from_secs(3600),
            replication_interval: Duration::from_secs(60),
            cleanup: crate::cleanup::CleanupConfig::default(),
            publish: crate::publish::PublishConfig::default(),
        }
    }
}

/// Drain outcome counters.
#[derive(Debug, Default, Clone)]
pub struct DrainReport {
    pub photos_completed: u64,
    pub resources_written: u64,
    pub resources_deduped: u64,
    pub bytes_written: u64,
    pub late_binding_merges: u64,
    pub swept_partials: u64,
    pub pauses: u64,
    pub awaiting_retry: i64,
    pub gave_up: i64,
    pub earliest_next_retry_at: Option<DateTime<Utc>>,
}

#[derive(Default)]
struct Counters {
    photos_completed: u64,
    resources_written: u64,
    resources_deduped: u64,
    bytes_written: u64,
    late_binding_merges: u64,
    pauses: u64,
}

#[derive(Default)]
struct PauseState {
    /// 1005 local-disk pressure: pause everything until re-probe time.
    local_pressure: bool,
    /// Blob-root free space below floor (or sink I/O errors, mount-flavored).
    storage_low: bool,
}

struct Shared {
    store: StateStore,
    data_dir: DataDir,
    config: SchedulerConfig,
    probe: Arc<dyn FreeSpaceProbe>,
    cancel: CancelToken,
    inflight_bytes: admission::InflightBytes,
    locks: locks::KeyedLocks,
    counters: Mutex<Counters>,
    pause: Mutex<PauseState>,
    /// Photos with a live `photo_task`. Hoisted here (not loop-local) so the
    /// daemon's event classification can defer changes to inflight photos.
    inflight: Mutex<HashSet<PhotoId>>,
}

/// One drain run over the state store.
pub struct Scheduler<F: ResourceFetcher> {
    fetcher: Arc<F>,
    shared: Arc<Shared>,
    /// Attached via `with_publisher`; None = the daemon loop skips the
    /// publish tick entirely (drain-only tools, tests, pre-integration).
    publisher: Option<Arc<dyn crate::publish::Publisher>>,
}

impl<F: ResourceFetcher> Scheduler<F> {
    pub fn new(
        store: StateStore,
        data_dir: DataDir,
        fetcher: Arc<F>,
        probe: Arc<dyn FreeSpaceProbe>,
        config: SchedulerConfig,
        cancel: CancelToken,
    ) -> Self {
        Self {
            fetcher,
            shared: Arc::new(Shared {
                store,
                data_dir,
                config,
                probe,
                cancel,
                inflight_bytes: admission::InflightBytes::default(),
                locks: locks::KeyedLocks::new(),
                counters: Mutex::new(Counters::default()),
                pause: Mutex::new(PauseState::default()),
                inflight: Mutex::new(HashSet::new()),
            }),
            publisher: None,
        }
    }

    /// Attach a HopNet publisher; the daemon loop then runs the publish tick
    /// (`config.publish`). Drain runs never publish.
    pub fn with_publisher(mut self, publisher: Arc<dyn crate::publish::Publisher>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Drain until the queue is empty/terminal or cancellation. Blocking
    /// from the caller's perspective only in the async sense — run inside a
    /// multi-thread runtime (fetcher calls use `spawn_blocking`).
    pub async fn drain(&self) -> Result<DrainReport> {
        let (_lock, swept) = self.prepare().await?;
        let mut tasks: JoinSet<()> = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.shared.config.fetch_concurrency,
        ));
        let no_skip = HashSet::new();

        loop {
            if self.shared.cancel.is_cancelled() {
                break;
            }
            // Pause handling: wait out pressure/storage pauses, re-probing.
            if self.paused_wait().await {
                continue;
            }

            let claimable = self.claim_batch(&no_skip).await?;
            if claimable.is_empty() {
                if tasks.is_empty() {
                    break; // queue drained (or only future retries remain)
                }
                let _ = tasks.join_next().await; // wait for capacity/progress
                continue;
            }
            self.spawn_all(&mut tasks, &semaphore, claimable).await;
        }

        while tasks.join_next().await.is_some() {}
        self.report(swept).await
    }

    /// One-time run setup shared by `drain` and `run_daemon`: the exclusive
    /// pid-stamped lock (Tier-1 refcount repair on an unclean reclaim,
    /// BEFORE any work is admitted — repaired counts gate irreversible
    /// deletes), blob-root creation, and the startup `.partial` sweep.
    async fn prepare(&self) -> Result<(crate::runlock::DrainLock, u64)> {
        let acquired = crate::runlock::DrainLock::acquire(&self.shared.data_dir)?;
        if acquired.unclean {
            // Outcome lands in the ingest log (`refcount_repaired`, drift only).
            crate::recovery::repair_refcounts(&self.shared.store).await?;
        }
        let libraries = self.shared.store.libraries().await?;
        let mut swept = 0u64;
        for lib in &libraries {
            // First-run: the configured blob root may not exist yet, and the
            // admission statvfs needs a real path to probe.
            std::fs::create_dir_all(&lib.blob_root).map_err(|e| {
                IngressError::Invariant(format!("blob_root {}: {e}", lib.blob_root))
            })?;
            swept += sweep_partials(&BlobPaths::new(&lib.blob_root))?;
        }
        Ok((acquired.lock, swept))
    }

    /// Pull the next work batch, filtered against inflight photos and the
    /// caller's skip set (the daemon's deferred photos — a photo with a
    /// queued hard move must not start fetching into the old root).
    async fn claim_batch(&self, skip: &HashSet<PhotoId>) -> Result<Vec<PhotoRecord>> {
        let batch = photos::pending_photos(
            self.shared.store.pool(),
            self.shared.config.retry_cap,
            Utc::now(),
            (self.shared.config.fetch_concurrency * 2) as i64,
        )
        .await?;
        let inflight = self.shared.inflight.lock().expect("inflight mutex");
        Ok(batch
            .into_iter()
            .filter(|p| !inflight.contains(&p.photo_id) && !skip.contains(&p.photo_id))
            .collect())
    }

    /// Spawn one `photo_task` per claimed photo, bounded by the semaphore.
    async fn spawn_all(
        &self,
        tasks: &mut JoinSet<()>,
        semaphore: &Arc<tokio::sync::Semaphore>,
        claimable: Vec<PhotoRecord>,
    ) {
        for photo in claimable {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore open");
            if self.shared.cancel.is_cancelled() {
                break;
            }
            self.shared
                .inflight
                .lock()
                .expect("inflight mutex")
                .insert(photo.photo_id.clone());
            let shared = self.shared.clone();
            let fetcher = self.fetcher.clone();
            tasks.spawn(async move {
                let photo_id = photo.photo_id.clone();
                if let Err(e) = photo_task(shared.clone(), fetcher, photo.clone()).await {
                    // Task-level invariant failures are logged AND bump
                    // retries like a transient failure — a persistent
                    // error must converge to give-up, never livelock the
                    // work queue on an unbumped pending photo.
                    let _ = shared
                        .store
                        .append_log(
                            "drain_task_error",
                            Some(&photo_id),
                            Some(serde_json::json!({ "error": e.to_string() })),
                        )
                        .await;
                    let _ = apply_failure_to_photo(
                        &shared,
                        &photo,
                        &FetchFailure::Transient(format!("task error: {e}")),
                    )
                    .await;
                }
                shared
                    .inflight
                    .lock()
                    .expect("inflight mutex")
                    .remove(&photo_id);
                drop(permit);
            });
        }
    }

    async fn report(&self, swept: u64) -> Result<DrainReport> {
        let summary = self
            .shared
            .store
            .retry_summary(self.shared.config.retry_cap)
            .await?;
        let c = self.shared.counters.lock().expect("counters mutex");
        Ok(DrainReport {
            photos_completed: c.photos_completed,
            resources_written: c.resources_written,
            resources_deduped: c.resources_deduped,
            bytes_written: c.bytes_written,
            late_binding_merges: c.late_binding_merges,
            swept_partials: swept,
            pauses: c.pauses,
            awaiting_retry: summary.awaiting_retry,
            gave_up: summary.gave_up,
            earliest_next_retry_at: summary.earliest_next_retry_at,
        })
    }

    /// If paused, sleep one poll interval and clear recoverable pauses.
    /// Returns true when the loop should re-check instead of admitting.
    async fn paused_wait(&self) -> bool {
        let (pressure, storage) = {
            let p = self.shared.pause.lock().expect("pause mutex");
            (p.local_pressure, p.storage_low)
        };
        if !pressure && !storage {
            return false;
        }
        let wait = if pressure {
            self.shared.config.pressure_pause
        } else {
            self.shared.config.storage_poll
        };
        // Cancellation must not wait the pause out (the caller re-checks the
        // token immediately).
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = self.shared.cancel.cancelled() => return true,
        }
        {
            // Recovery is probe-by-retry: clear the pause and let the next
            // admission/fetch re-detect if the condition persists.
            let mut p = self.shared.pause.lock().expect("pause mutex");
            p.local_pressure = false;
            p.storage_low = false;
        }
        let _ = self
            .shared
            .store
            .append_log(
                "storage_recovered",
                None,
                Some(serde_json::json!({ "probe": "resume" })),
            )
            .await;
        true
    }
}

/// Enter a pause state (idempotent) and log `storage_low` once per entry.
async fn enter_pause(shared: &Shared, local_disk: bool, detail: serde_json::Value) {
    let newly = {
        let mut p = shared.pause.lock().expect("pause mutex");
        let flag = if local_disk {
            &mut p.local_pressure
        } else {
            &mut p.storage_low
        };
        let newly = !*flag;
        *flag = true;
        newly
    };
    if newly {
        shared.counters.lock().expect("counters mutex").pauses += 1;
        let _ = shared
            .store
            .append_log("storage_low", None, Some(detail))
            .await;
    }
}

/// One photo's drain: fresh descriptor, then its pending resources in order
/// (original first), each streamed through the fetcher and finalized.
async fn photo_task<F: ResourceFetcher>(
    shared: Arc<Shared>,
    fetcher: Arc<F>,
    photo: PhotoRecord,
) -> Result<()> {
    let library_id = photo
        .library_id
        .clone()
        .expect("work query filters NULL libraries");
    let library = shared
        .store
        .library(&library_id)
        .await?
        .ok_or_else(|| IngressError::Invariant(format!("no library row {library_id}")))?;
    let paths = BlobPaths::new(&library.blob_root);
    let local_id = photo.local_id.clone().ok_or_else(|| {
        IngressError::Invariant(format!("photo {} has no local_id", photo.photo_id))
    })?;

    // Fresh descriptor (blocking platform call).
    let desc = {
        let fetcher = fetcher.clone();
        let lid = local_id.clone();
        tokio::task::spawn_blocking(move || fetcher.descriptor_for(&lid))
            .await
            .map_err(|e| IngressError::Invariant(format!("descriptor join: {e}")))?
    };
    let desc = match desc {
        Ok(d) => d,
        Err(failure) => {
            // Apply the classified failure to every currently-eligible
            // pending resource of the photo (pause classes touch nothing).
            return apply_failure_to_photo(&shared, &photo, &failure).await;
        }
    };

    // Pending rows, original first (identity merge depends on it).
    let mut pending: Vec<ResourceType> = shared
        .store
        .resources_for_photo(&photo.photo_id)
        .await?
        .into_iter()
        .filter(|r| r.written_at.is_none() && r.retry_count < shared.config.retry_cap)
        .filter(|r| r.next_retry_at.map(|t| t <= Utc::now()).unwrap_or(true))
        .map(|r| r.resource_type)
        .collect();
    // Original first (identity merge depends on it), thumbnails last —
    // renditions are the cheapest and least critical rows.
    pending.sort_by_key(|rt| (rt.is_thumbnail(), *rt));

    let mut current_photo_id = photo.photo_id.clone();
    for resource_type in pending {
        if shared.cancel.is_cancelled() {
            return Ok(());
        }
        if shared.pause.lock().expect("pause mutex").local_pressure {
            return Ok(()); // photo re-queues after the pause clears
        }

        // Locate this resource on the fresh descriptor.
        let Some(res_desc) = desc
            .resources
            .iter()
            .find(|r| ResourceType::from_ph_type(r.ph_resource_type) == Some(resource_type))
        else {
            let n = bump_failure(
                &shared,
                &current_photo_id,
                resource_type,
                "resource no longer enumerated on asset",
            )
            .await?;
            let _ = n;
            continue;
        };

        // Storage-aware admission.
        let expected = match res_desc.expected_size {
            Some(s) => s,
            None => shared
                .store
                .max_blob_size(&library_id)
                .await?
                .map(|s| s as u64)
                .unwrap_or(shared.config.default_size_estimate),
        };
        let admitted = match admission::admit(
            shared.probe.as_ref(),
            std::path::Path::new(&library.blob_root),
            &shared.inflight_bytes,
            expected,
            shared.config.reserve_floor_bytes,
        ) {
            // A vanished blob root fails instantly, so treating it as a
            // per-resource failure burns the whole retry budget in minutes
            // and strands the queue until the next scan's gave-up reset.
            // Pause-and-poll instead; the photo re-queues untouched.
            Err(IngressError::StorageUnavailable(e)) => {
                enter_pause(
                    &shared,
                    false,
                    serde_json::json!({ "disk": "blob_root", "error": e }),
                )
                .await;
                return Ok(());
            }
            other => other?,
        };
        if !admitted {
            enter_pause(
                &shared,
                false,
                serde_json::json!({
                    "disk": "blob_root", "library": library_id.as_str(),
                    "reserve_floor": shared.config.reserve_floor_bytes,
                }),
            )
            .await;
            return Ok(()); // photo re-queues once storage recovers
        }

        shared.inflight_bytes.register(expected);
        let outcome = stream_one_resource(
            &shared,
            &fetcher,
            &library,
            &paths,
            &desc,
            &photo,
            &mut current_photo_id,
            resource_type,
            res_desc.ph_resource_type,
            &res_desc.uti,
            res_desc.original_filename.as_deref(),
            &local_id,
        )
        .await;
        shared.inflight_bytes.unregister(expected);
        match outcome? {
            TaskFlow::Continue => {}
            TaskFlow::Stop => return Ok(()),
        }
    }
    Ok(())
}

enum TaskFlow {
    Continue,
    Stop,
}

#[allow(clippy::too_many_arguments)]
async fn stream_one_resource<F: ResourceFetcher>(
    shared: &Arc<Shared>,
    fetcher: &Arc<F>,
    library: &LibraryConfig,
    paths: &BlobPaths,
    desc: &AssetDescriptor,
    photo: &PhotoRecord,
    current_photo_id: &mut PhotoId,
    resource_type: ResourceType,
    ph_type: i32,
    uti: &str,
    filename: Option<&str>,
    local_id: &str,
) -> Result<TaskFlow> {
    let derivation = ext_for_uti(uti, filename);
    if matches!(derivation, crate::ext::ExtDerivation::Fallback) {
        shared
            .store
            .append_log(
                "unknown_uti",
                Some(current_photo_id),
                Some(serde_json::json!({ "uti": uti, "filename": filename })),
            )
            .await?;
    }
    let ext = derivation.ext().to_string();

    let key = TempKey::Resource {
        photo_id: current_photo_id.clone(),
        resource_type,
    };
    let write = match ResourceWrite::begin(paths, &key) {
        Ok(w) => w,
        Err(e) => {
            // Blob-root I/O failure before any bytes: mount-flavored pause.
            enter_pause(
                shared,
                false,
                serde_json::json!({ "disk": "blob_root", "error": e.to_string() }),
            )
            .await;
            return Ok(TaskFlow::Stop);
        }
    };
    let sink = Arc::new(StreamSink::new(write, shared.cancel.clone()));

    let request = FetchRequest {
        photo_id: current_photo_id.clone(),
        local_id: local_id.to_string(),
        ph_resource_type: ph_type,
    };
    let fetch_result = {
        let fetcher = fetcher.clone();
        let sink = sink.clone();
        tokio::task::spawn_blocking(move || fetcher.fetch_resource(request, sink))
            .await
            .map_err(|e| IngressError::Invariant(format!("fetch join: {e}")))?
    };

    match fetch_result {
        Ok(()) => {}
        Err(failure) => {
            sink.abort();
            return handle_fetch_failure(shared, current_photo_id, resource_type, failure).await;
        }
    }

    let finished = match sink.take_finished() {
        Ok(f) => f,
        Err(_) => {
            enter_pause(
                shared,
                false,
                serde_json::json!({ "disk": "blob_root", "op": "finish" }),
            )
            .await;
            return Ok(TaskFlow::Stop);
        }
    };

    // Drain-time rule 2a: only for the original of a seed-minted photo.
    if resource_type == ResourceType::Original
        && photo.cloud_id.is_some()
        && let Some(survivor) = late_binding_merge(
            &shared.store,
            &library.library_id,
            &finished.hash,
            desc,
            current_photo_id,
        )
        .await?
    {
        std::fs::remove_file(&finished.temp_path).ok();
        shared
            .counters
            .lock()
            .expect("counters mutex")
            .late_binding_merges += 1;
        *current_photo_id = survivor;
        // The surviving photo's remaining pending resources (if any) drain
        // via the queue under its own admission; this provisional task is done.
        return Ok(TaskFlow::Stop);
    }

    // Finalize under the per-(library, hash) lock.
    let lock = shared.locks.lock_for(&library.library_id, &finished.hash);
    let _guard = lock.lock().await;
    let size = finished.size_bytes;
    let outcome = finalize_resource(
        &shared.store,
        paths,
        &library.library_id,
        current_photo_id,
        resource_type,
        finished,
        &ext,
    )
    .await?;
    drop(_guard);

    {
        let mut c = shared.counters.lock().expect("counters mutex");
        if outcome.deduped() {
            c.resources_deduped += 1;
        } else {
            c.resources_written += 1;
            c.bytes_written += size;
        }
        if outcome.photo_completed() {
            c.photos_completed += 1;
        }
    }

    if outcome.photo_completed() {
        write_photo_sidecar(&shared.store, &shared.data_dir, desc, current_photo_id).await?;
    }
    Ok(TaskFlow::Continue)
}

/// Disposition table (spec §Failure Handling).
async fn handle_fetch_failure(
    shared: &Arc<Shared>,
    photo_id: &PhotoId,
    resource_type: ResourceType,
    failure: FetchFailure,
) -> Result<TaskFlow> {
    match failure {
        FetchFailure::LocalDiskPressure => {
            enter_pause(
                shared,
                true,
                serde_json::json!({ "disk": "local", "code": 1005 }),
            )
            .await;
            Ok(TaskFlow::Stop)
        }
        FetchFailure::Cancelled => Ok(TaskFlow::Stop),
        FetchFailure::Sink(msg) => {
            enter_pause(
                shared,
                false,
                serde_json::json!({ "disk": "blob_root", "error": msg }),
            )
            .await;
            Ok(TaskFlow::Stop)
        }
        FetchFailure::AssetUnavailable(msg) | FetchFailure::Transient(msg) => {
            bump_failure(shared, photo_id, resource_type, &msg).await?;
            Ok(TaskFlow::Continue)
        }
    }
}

async fn bump_failure(
    shared: &Arc<Shared>,
    photo_id: &PhotoId,
    resource_type: ResourceType,
    error: &str,
) -> Result<i64> {
    // Peek current count to compute the post-bump backoff delay.
    let current: i64 = shared
        .store
        .resources_for_photo(photo_id)
        .await?
        .into_iter()
        .find(|r| r.resource_type == resource_type)
        .map(|r| r.retry_count)
        .unwrap_or(0);
    let delay = backoff::delay(&shared.config.backoff, current + 1);
    shared
        .store
        .record_resource_failure(
            photo_id,
            resource_type,
            error,
            Utc::now() + chrono::Duration::from_std(delay).expect("bounded delay"),
            shared.config.retry_cap,
        )
        .await
}

async fn apply_failure_to_photo(
    shared: &Arc<Shared>,
    photo: &PhotoRecord,
    failure: &FetchFailure,
) -> Result<()> {
    match failure {
        FetchFailure::LocalDiskPressure => {
            enter_pause(
                shared,
                true,
                serde_json::json!({ "disk": "local", "code": 1005 }),
            )
            .await;
            Ok(())
        }
        FetchFailure::Cancelled => Ok(()),
        FetchFailure::Sink(msg) => {
            enter_pause(
                shared,
                false,
                serde_json::json!({ "disk": "blob_root", "error": msg }),
            )
            .await;
            Ok(())
        }
        FetchFailure::AssetUnavailable(msg) | FetchFailure::Transient(msg) => {
            let eligible: Vec<ResourceType> = shared
                .store
                .resources_for_photo(&photo.photo_id)
                .await?
                .into_iter()
                .filter(|r| r.written_at.is_none() && r.retry_count < shared.config.retry_cap)
                .map(|r| r.resource_type)
                .collect();
            for rt in eligible {
                bump_failure(shared, &photo.photo_id, rt, msg).await?;
            }
            Ok(())
        }
    }
}
