//! Scheduler behavior (spec §Ingest Pipeline, §Failure Handling) — pure
//! Rust via a programmable FakeFetcher; no PhotoKit, no Mac.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use ingress_core::descriptor::AssetDescriptor;
use ingress_core::fixtures::{AssetDescriptorBuilder, add_shared};
use ingress_core::paths::DataDir;
use ingress_core::scheduler::{
    BackoffConfig, CancelToken, FetchFailure, FetchRequest, FreeSpaceProbe, ResourceFetcher,
    Scheduler, SchedulerConfig, StreamSink,
};
use ingress_core::{LibraryScope, SeedOutcome, StateStore, seed_descriptor};

// ---------------------------------------------------------------- fixtures

struct FakeProbe(AtomicU64);

impl FreeSpaceProbe for FakeProbe {
    fn free_bytes(&self, _: &std::path::Path) -> ingress_core::Result<u64> {
        Ok(self.0.load(Ordering::Relaxed))
    }
}

#[derive(Default)]
struct FakeFetcher {
    descriptors: Mutex<HashMap<String, AssetDescriptor>>,
    bytes: Mutex<HashMap<(String, i32), Vec<u8>>>,
    failures: Mutex<HashMap<(String, i32), VecDeque<FetchFailure>>>,
    barrier: Option<Arc<Barrier>>,
    cancel_on_fetch: Option<CancelToken>,
    fetch_calls: AtomicU64,
    /// ph_resource_type of every fetch, in call order (ordering assertions).
    fetch_order: Mutex<Vec<i32>>,
}

impl FakeFetcher {
    fn add_asset(&self, desc: &AssetDescriptor, original_bytes: &[u8], paired: Option<&[u8]>) {
        self.descriptors
            .lock()
            .unwrap()
            .insert(desc.local_id.clone(), desc.clone());
        let mut bytes = self.bytes.lock().unwrap();
        bytes.insert((desc.local_id.clone(), 1), original_bytes.to_vec());
        if let Some(v) = paired {
            bytes.insert((desc.local_id.clone(), 9), v.to_vec());
        }
    }

    fn fail_next(&self, local_id: &str, ph_type: i32, failure: FetchFailure) {
        self.failures
            .lock()
            .unwrap()
            .entry((local_id.to_string(), ph_type))
            .or_default()
            .push_back(failure);
    }
}

impl ResourceFetcher for FakeFetcher {
    fn descriptor_for(&self, local_id: &str) -> Result<AssetDescriptor, FetchFailure> {
        self.descriptors
            .lock()
            .unwrap()
            .get(local_id)
            .cloned()
            .ok_or_else(|| FetchFailure::AssetUnavailable(format!("no asset {local_id}")))
    }

    fn fetch_resource(
        &self,
        request: FetchRequest,
        sink: Arc<StreamSink>,
    ) -> Result<(), FetchFailure> {
        self.fetch_calls.fetch_add(1, Ordering::Relaxed);
        self.fetch_order
            .lock()
            .unwrap()
            .push(request.ph_resource_type);
        if let Some(b) = &self.barrier {
            b.wait();
        }
        if let Some(token) = &self.cancel_on_fetch {
            token.cancel();
        }
        if let Some(f) = self
            .failures
            .lock()
            .unwrap()
            .get_mut(&(request.local_id.clone(), request.ph_resource_type))
            .and_then(|q| q.pop_front())
        {
            return Err(f);
        }
        let bytes = self
            .bytes
            .lock()
            .unwrap()
            .get(&(request.local_id.clone(), request.ph_resource_type))
            .cloned()
            .ok_or_else(|| FetchFailure::AssetUnavailable("no bytes registered".into()))?;
        // Two chunks to exercise streaming.
        let mid = bytes.len() / 2;
        sink.write(&bytes[..mid])?;
        sink.write(&bytes[mid..])?;
        Ok(())
    }
}

struct Rig {
    store: StateStore,
    data_dir: DataDir,
    fetcher: Arc<FakeFetcher>,
    probe: Arc<FakeProbe>,
    cancel: CancelToken,
    config: SchedulerConfig,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

async fn rig() -> Rig {
    let blob_dir = tempfile::tempdir().unwrap();
    let data_tmp = tempfile::tempdir().unwrap();
    // File-backed (not in-memory): the lifecycle ticks snapshot via
    // `VACUUM INTO`, which silently no-ops on an in-memory store.
    let store = StateStore::open(&data_tmp.path().join("state.db"))
        .await
        .unwrap();
    let library = ingress_core::LibraryId::new("personal");
    store
        .insert_library(&ingress_core::LibraryConfig {
            library_id: library.clone(),
            display_name: "Personal".into(),
            blob_root: blob_dir.path().to_string_lossy().into_owned(),
            sidecar_root_remote: None,
            scope_binding: None,
            retention_days: 30,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
    Rig {
        store,
        data_dir: DataDir::new(data_tmp.path()),
        fetcher: Arc::new(FakeFetcher::default()),
        probe: Arc::new(FakeProbe(AtomicU64::new(u64::MAX))),
        cancel: CancelToken::default(),
        config: SchedulerConfig {
            fetch_concurrency: 2,
            retry_cap: 5,
            backoff: BackoffConfig {
                base: Duration::ZERO,
                max: Duration::ZERO,
            },
            reserve_floor_bytes: 0,
            pressure_pause: Duration::from_millis(10),
            storage_poll: Duration::from_millis(10),
            default_size_estimate: 1024,
            // Long intervals: lifecycle ticks stay out of the way unless a
            // test opts in by zeroing them.
            cleanup_interval: Duration::from_secs(3600),
            cleanup: ingress_core::cleanup::CleanupConfig::default(),
            publish: ingress_core::publish::PublishConfig::default(),
        },
        _dirs: (blob_dir, data_tmp),
    }
}

fn scheduler(rig: &Rig) -> Scheduler<FakeFetcher> {
    Scheduler::new(
        rig.store.clone(),
        rig.data_dir.clone(),
        rig.fetcher.clone(),
        rig.probe.clone(),
        rig.config.clone(),
        rig.cancel.clone(),
    )
}

async fn seed_asset(rig: &Rig, desc: &AssetDescriptor, bytes: &[u8], paired: Option<&[u8]>) {
    rig.fetcher.add_asset(desc, bytes, paired);
    match seed_descriptor(&rig.store, desc).await.unwrap() {
        SeedOutcome::MintedPending { .. } => {}
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

// ------------------------------------------------------------------- tests

// Impact: the drain loop is the daemon's engine; this is the canonical
// seed → drain → materialized flow every later phase builds on.
// Should: materialize every seeded photo, write sidecars, and report
// counters that match the database state.
#[tokio::test(flavor = "multi_thread")]
async fn drain_happy_path() {
    let rig = rig().await;
    for i in 0..3 {
        let desc = AssetDescriptorBuilder::live_photo().build();
        seed_asset(&rig, &desc, format!("still-{i}").as_bytes(), Some(b"video")).await;
    }

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 3);
    // 3 originals + 3 paired videos; identical "video" bytes dedup to 1 file
    // after the first, so written + deduped = 6.
    assert_eq!(report.resources_written + report.resources_deduped, 6);
    assert_eq!(report.gave_up, 0);
    assert_eq!(rig.store.count_photos().await.unwrap(), 3);

    let pending = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM photo_resources WHERE written_at IS NULL",
    )
    .fetch_one(rig.store.raw_pool())
    .await
    .unwrap();
    assert_eq!(pending, 0);
}

// Should: retry a transient failure within the same drain (zero backoff)
// and reset retry_count on eventual success.
#[tokio::test(flavor = "multi_thread")]
async fn transient_failure_retries_and_recovers() {
    let rig = rig().await;
    let desc = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &desc, b"eventually-fine", None).await;
    rig.fetcher
        .fail_next(&desc.local_id, 1, FetchFailure::Transient("blip".into()));

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 1);
    let rt: (i64,) =
        sqlx::query_as("SELECT retry_count FROM photo_resources WHERE resource_type = 0")
            .fetch_one(rig.store.raw_pool())
            .await
            .unwrap();
    assert_eq!(rt.0, 0); // reset on success
}

// Impact: give-up bookkeeping is the operator's only signal that a resource
// is stuck; double-logging or never-logging both mislead.
// Should: stop retrying at the cap and log resource_gave_up exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn gives_up_at_retry_cap() {
    let mut r = rig().await;
    r.config.retry_cap = 2;
    let rig = r;
    let desc = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &desc, b"never-arrives", None).await;
    for _ in 0..10 {
        rig.fetcher
            .fail_next(&desc.local_id, 1, FetchFailure::Transient("down".into()));
    }

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 0);
    assert_eq!(report.gave_up, 1);
    assert_eq!(rig.fetcher.fetch_calls.load(Ordering::Relaxed), 2); // cap, not 10
    let events = rig.store.log_events("resource_gave_up").await.unwrap();
    assert_eq!(events.len(), 1);
}

// Impact: 1005 is local disk pressure, not a resource failure — burning the
// retry budget on it would give up on perfectly fetchable photos.
// Should: pause on LocalDiskPressure without consuming retries, then resume
// and complete.
// Should not: log storage_low more than once for one pause episode.
#[tokio::test(flavor = "multi_thread")]
async fn disk_pressure_pauses_without_retry_burn() {
    let rig = rig().await;
    let desc = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &desc, b"pressured", None).await;
    rig.fetcher
        .fail_next(&desc.local_id, 1, FetchFailure::LocalDiskPressure);

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 1);
    assert_eq!(report.pauses, 1);
    let rt: (i64,) =
        sqlx::query_as("SELECT retry_count FROM photo_resources WHERE resource_type = 0")
            .fetch_one(rig.store.raw_pool())
            .await
            .unwrap();
    assert_eq!(rt.0, 0);
    assert_eq!(rig.store.log_events("storage_low").await.unwrap().len(), 1);
    assert_eq!(
        rig.store
            .log_events("storage_recovered")
            .await
            .unwrap()
            .len(),
        1
    );
}

// Should: admit nothing while free space is under the reserve floor, and
// resume when it recovers.
#[tokio::test(flavor = "multi_thread")]
async fn admission_floor_blocks_then_recovers() {
    let mut r = rig().await;
    r.config.reserve_floor_bytes = 1_000_000;
    let rig = r;
    rig.probe.0.store(10, Ordering::Relaxed); // way below floor
    let desc = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &desc, b"waiting-room", None).await;

    let probe = rig.probe.clone();
    let raise = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        probe.0.store(u64::MAX, Ordering::Relaxed);
    });

    let report = scheduler(&rig).drain().await.unwrap();
    raise.await.unwrap();
    assert_eq!(report.photos_completed, 1);
    assert!(report.pauses >= 1);
    assert!(
        !rig.store
            .log_events("storage_low")
            .await
            .unwrap()
            .is_empty()
    );
}

// Impact: an unmounted SMB blob root fails statvfs instantly, so charging
// the outage to resources burns the whole retry budget in minutes and
// strands the queue until the next scan's gave-up reset (observed live:
// 5,961 gave-ups from one overnight unmount).
// Should: pause on a vanished blob root without consuming retries, then
// resume and complete once the root returns.
// Should not: log drain_task_error for the outage.
#[tokio::test(flavor = "multi_thread")]
async fn vanished_blob_root_pauses_without_retry_burn() {
    struct VanishingProbe(AtomicU64);
    impl FreeSpaceProbe for VanishingProbe {
        fn free_bytes(&self, path: &std::path::Path) -> ingress_core::Result<u64> {
            if self.0.load(Ordering::Relaxed) > 0 {
                self.0.fetch_sub(1, Ordering::Relaxed);
                return Err(ingress_core::IngressError::StorageUnavailable(format!(
                    "statvfs({}): No such file or directory",
                    path.display()
                )));
            }
            Ok(u64::MAX)
        }
    }

    let rig = rig().await;
    let desc = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &desc, b"mount-flaps", None).await;

    let scheduler = Scheduler::new(
        rig.store.clone(),
        rig.data_dir.clone(),
        rig.fetcher.clone(),
        Arc::new(VanishingProbe(AtomicU64::new(3))),
        rig.config.clone(),
        rig.cancel.clone(),
    );
    let report = scheduler.drain().await.unwrap();
    assert_eq!(report.photos_completed, 1);
    assert!(report.pauses >= 1);
    let rt: (i64,) =
        sqlx::query_as("SELECT retry_count FROM photo_resources WHERE resource_type = 0")
            .fetch_one(rig.store.raw_pool())
            .await
            .unwrap();
    assert_eq!(rt.0, 0);
    assert!(
        rig.store
            .log_events("drain_task_error")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !rig.store
            .log_events("storage_low")
            .await
            .unwrap()
            .is_empty()
    );
}

// Impact: SIGTERM-clean is a spec invariant — cancellation must never
// consume retries or commit partial state.
// Should: stop draining on cancellation with rows untouched.
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_leaves_rows_pending() {
    let mut r = rig().await;
    r.fetcher = Arc::new(FakeFetcher {
        cancel_on_fetch: Some(r.cancel.clone()),
        ..FakeFetcher::default()
    });
    let rig = r;
    let desc = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &desc, b"interrupted", None).await;

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 0);
    let row: (i64, Option<String>) = sqlx::query_as(
        "SELECT retry_count, written_at FROM photo_resources WHERE resource_type = 0",
    )
    .fetch_one(rig.store.raw_pool())
    .await
    .unwrap();
    assert_eq!(row.0, 0);
    assert!(row.1.is_none());
}

// Impact: the keyed finalize lock enforces the spec's one-inflight-per-blob
// invariant; a race here corrupts refcounts or races the rename target.
// Should: converge two simultaneous identical streams to one blob file with
// ref_count 2.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_identical_bytes_share_one_blob() {
    let mut r = rig().await;
    r.fetcher = Arc::new(FakeFetcher {
        barrier: Some(Arc::new(Barrier::new(2))),
        ..FakeFetcher::default()
    });
    let rig = r;
    let a = AssetDescriptorBuilder::simple_image().build();
    let b = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &a, b"identical-bytes", None).await;
    seed_asset(&rig, &b, b"identical-bytes", None).await;

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 2);
    assert_eq!(report.resources_written, 1);
    assert_eq!(report.resources_deduped, 1);
    let (count,): (i64,) = sqlx::query_as("SELECT ref_count FROM blobs")
        .fetch_one(rig.store.raw_pool())
        .await
        .unwrap();
    assert_eq!(count, 2);
}

// Impact: drain-time rule 2a — the day-1-local/day-2-cloud asset must stay
// ONE logical photo across the seed/drain split.
// Should: merge the seed-minted provisional into the existing local-only
// photo and log late_binding_merge.
#[tokio::test(flavor = "multi_thread")]
async fn late_binding_merge_on_drain() {
    let rig = rig().await;

    // Day 1: local-only photo, fully materialized (Phase-2 style).
    let day1 = AssetDescriptorBuilder::simple_image().local_only().build();
    seed_asset(&rig, &day1, b"same-photo-bytes", None).await;
    scheduler(&rig).drain().await.unwrap();
    assert_eq!(rig.store.count_photos().await.unwrap(), 1);

    // Day 2: same bytes re-discovered with a cloud_id (fresh local_id).
    let day2 = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("CLOUD-MERGE:001")
        .build();
    seed_asset(&rig, &day2, b"same-photo-bytes", None).await;
    assert_eq!(rig.store.count_photos().await.unwrap(), 2); // provisional exists

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.late_binding_merges, 1);
    assert_eq!(rig.store.count_photos().await.unwrap(), 1);
    let survivor = rig
        .store
        .photo_by_cloud_id("CLOUD-MERGE:001")
        .await
        .unwrap()
        .unwrap();
    assert!(survivor.materialized_at.is_some());
    assert_eq!(
        rig.store
            .log_events("late_binding_merge")
            .await
            .unwrap()
            .len(),
        1
    );
}

// Should: adopt an unmapped photo once its scope is bound, making its
// pending rows drain-eligible.
#[tokio::test(flavor = "multi_thread")]
async fn unmapped_then_adopted_then_drained() {
    let rig = rig().await;
    let desc = AssetDescriptorBuilder::simple_image()
        .scope(LibraryScope::Shared)
        .build();
    rig.fetcher.add_asset(&desc, b"shared-bytes", None);

    match seed_descriptor(&rig.store, &desc).await.unwrap() {
        SeedOutcome::Unmapped { .. } => {}
        other => panic!("expected Unmapped, got {other:?}"),
    }
    // Drain does nothing — NULL-library rows are not eligible.
    assert_eq!(scheduler(&rig).drain().await.unwrap().photos_completed, 0);

    let shared_lib = add_shared(&rig.store).await;
    sqlx::query("UPDATE libraries SET blob_root = ? WHERE library_id = ?")
        .bind(
            rig._dirs
                .0
                .path()
                .join("shared")
                .to_string_lossy()
                .into_owned(),
        )
        .bind(shared_lib.as_str())
        .execute(rig.store.raw_pool())
        .await
        .unwrap();

    match seed_descriptor(&rig.store, &desc).await.unwrap() {
        SeedOutcome::Adopted { .. } => {}
        other => panic!("expected Adopted, got {other:?}"),
    }
    assert_eq!(scheduler(&rig).drain().await.unwrap().photos_completed, 1);
}

// Should: record a bump for a pending row missing from the fresh descriptor
// while draining the resources that do exist.
#[tokio::test(flavor = "multi_thread")]
async fn descriptor_drift_bumps_missing_resource() {
    let rig = rig().await;
    // Seed as Live Photo (original + paired video pending)…
    let desc = AssetDescriptorBuilder::live_photo().build();
    seed_asset(&rig, &desc, b"still", Some(b"motion")).await;
    // …but the fresh drain-time descriptor no longer has the video.
    let drifted = AssetDescriptor {
        resources: desc
            .resources
            .iter()
            .filter(|r| r.ph_resource_type == 1)
            .cloned()
            .collect(),
        ..desc.clone()
    };
    rig.fetcher
        .descriptors
        .lock()
        .unwrap()
        .insert(desc.local_id.clone(), drifted);

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 0); // paired video still pending
    assert_eq!(report.resources_written, 1); // original archived
    let (err,): (Option<String>,) =
        sqlx::query_as("SELECT last_error FROM photo_resources WHERE resource_type = 2")
            .fetch_one(rig.store.raw_pool())
            .await
            .unwrap();
    assert!(err.unwrap().contains("no longer enumerated"));
}

// ------------------------------------------------------------ daemon tests

use ingress_core::scheduler::daemon::{ChangeEvent, DaemonHandle};

/// Poll `check` until true or panic after ~5s.
async fn wait_until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..500 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

// Impact: the finalize-vs-move race is this phase's scariest corruption
// window — a hard move applied mid-task would relocate state under a
// photo_task that already resolved its blob paths.
// Should: defer the scope-flip event while the photo is inflight (its bytes
// land under the pre-move root), then apply it — bytes end under the
// destination exactly once.
// Should not: admit or re-fetch anything into the old root after the move.
#[tokio::test(flavor = "multi_thread")]
async fn events_for_inflight_photo_are_deferred() {
    let mut r = rig().await;
    let barrier = Arc::new(Barrier::new(2));
    r.fetcher = Arc::new(FakeFetcher {
        barrier: Some(barrier.clone()),
        ..FakeFetcher::default()
    });
    let rig = r;

    // Bind the shared library with its own temp root.
    let shared_lib = add_shared(&rig.store).await;
    let shared_root = rig._dirs.0.path().join("shared");
    sqlx::query("UPDATE libraries SET blob_root = ? WHERE library_id = ?")
        .bind(shared_root.to_string_lossy().into_owned())
        .bind(shared_lib.as_str())
        .execute(rig.store.raw_pool())
        .await
        .unwrap();

    let desc = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &desc, b"moving-bytes", None).await;

    let (handle, rx) = DaemonHandle::new();
    let sched = scheduler(&rig);
    let daemon = {
        let handle = handle.clone();
        tokio::spawn(async move { sched.run_daemon(rx, handle).await })
    };

    // Wait for the fetch to pin at the barrier (photo inflight), THEN push.
    let fetcher = rig.fetcher.clone();
    wait_until("fetch pinned inflight", || {
        let fetcher = fetcher.clone();
        async move { fetcher.fetch_calls.load(Ordering::Relaxed) >= 1 }
    })
    .await;
    let mut moved = desc.clone();
    moved.scope = LibraryScope::Shared;
    handle.push(ChangeEvent::Descriptor(Box::new(moved)));
    tokio::time::sleep(Duration::from_millis(150)).await; // let it route+defer
    barrier.wait(); // release the pinned fetch

    let store = rig.store.clone();
    let cloud = desc.cloud_id.clone().unwrap();
    wait_until("photo transitioned to shared", || {
        let store = store.clone();
        let shared_lib = shared_lib.clone();
        let cloud = cloud.clone();
        async move {
            store
                .photo_by_cloud_id(&cloud)
                .await
                .unwrap()
                .map(|p| p.library_id == Some(shared_lib.clone()))
                .unwrap_or(false)
        }
    })
    .await;

    rig.cancel.cancel();
    handle.wake();
    let report = daemon.await.unwrap().unwrap();
    assert_eq!(
        report.events_deferred, 1,
        "the move waited out the inflight task"
    );
    assert_eq!(report.transitions, 1);
    assert_eq!(
        rig.store
            .log_events("library_transition")
            .await
            .unwrap()
            .len(),
        1
    );

    // Bytes live under the destination — and only there.
    let hash = ingress_core::ContentHash::of_bytes(b"moving-bytes");
    let src_paths = ingress_core::paths::BlobPaths::new(rig._dirs.0.path());
    let dst_paths = ingress_core::paths::BlobPaths::new(&shared_root);
    assert!(dst_paths.blob_path(&hash, "heic").is_file());
    assert!(!src_paths.blob_path(&hash, "heic").is_file());
}

// Impact: reordering deferred events resurrects deleted renders — an edit
// applied after its own revert re-mints rows the user discarded.
// Should: apply a photo's deferred events in arrival order (edit, then
// revert), converging on the reverted shape with the photo materialized.
#[tokio::test(flavor = "multi_thread")]
async fn deferred_events_apply_in_fifo_order() {
    let mut r = rig().await;
    let barrier = Arc::new(Barrier::new(2));
    r.fetcher = Arc::new(FakeFetcher {
        barrier: Some(barrier.clone()),
        ..FakeFetcher::default()
    });
    let rig = r;

    let t1 = chrono::Utc::now();
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    seed_asset(&rig, &desc, b"fifo-bytes", None).await;

    let (handle, rx) = DaemonHandle::new();
    let sched = scheduler(&rig);
    let daemon = {
        let handle = handle.clone();
        tokio::spawn(async move { sched.run_daemon(rx, handle).await })
    };

    // Wait for the fetch to pin at the barrier (photo inflight)…
    let fetcher = rig.fetcher.clone();
    wait_until("fetch pinned inflight", || {
        let fetcher = fetcher.clone();
        async move { fetcher.fetch_calls.load(Ordering::Relaxed) >= 1 }
    })
    .await;
    // …then, while pinned: an edit event followed by its revert.
    let mut edited = desc.clone();
    edited.asset_modified_at = Some(t1 + chrono::Duration::seconds(5));
    edited.resources.push(ingress_core::ResourceDescriptor {
        ph_resource_type: 5,
        uti: "public.heic".into(),
        original_filename: None,
        expected_size: Some(2_000_000),
        locally_available: Some(true),
    });
    let mut reverted = desc.clone();
    reverted.asset_modified_at = Some(t1 + chrono::Duration::seconds(10));
    handle.push(ChangeEvent::Descriptor(Box::new(edited)));
    handle.push(ChangeEvent::Descriptor(Box::new(reverted)));
    tokio::time::sleep(Duration::from_millis(150)).await;
    barrier.wait();

    let store = rig.store.clone();
    let cloud = desc.cloud_id.clone().unwrap();
    wait_until("photo settles reverted + materialized", || {
        let store = store.clone();
        let cloud = cloud.clone();
        async move {
            store
                .photo_by_cloud_id(&cloud)
                .await
                .unwrap()
                .map(|p| p.materialized_at.is_some() && p.asset_modified_at > Some(t1))
                .unwrap_or(false)
        }
    })
    .await;

    rig.cancel.cancel();
    handle.wake();
    let report = daemon.await.unwrap().unwrap();
    assert_eq!(report.events_deferred, 2);

    let photo = rig
        .store
        .photo_by_cloud_id(desc.cloud_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let rows = rig
        .store
        .resources_for_photo(&photo.photo_id)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "edit row minted then reverted away — FIFO");
    assert_eq!(rows[0].resource_type, ingress_core::ResourceType::Original);
    assert!(
        photo.materialized_at.is_some(),
        "revert re-stamped completion"
    );
}

// Impact: the core daemon promise — an observer insert becomes a materialized
// photo without restarting anything, and the loop stays alive afterward.
// Should: seed from a pushed descriptor, drain it, and keep running until
// cancelled.
#[tokio::test(flavor = "multi_thread")]
async fn daemon_processes_event_then_drains_new_pending() {
    let rig = rig().await;
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(chrono::Utc::now())
        .build();
    rig.fetcher.add_asset(&desc, b"observer-insert", None);

    let (handle, rx) = DaemonHandle::new();
    let sched = scheduler(&rig);
    let daemon = {
        let handle = handle.clone();
        tokio::spawn(async move { sched.run_daemon(rx, handle).await })
    };

    handle.push(ChangeEvent::Descriptor(Box::new(desc.clone())));
    let store = rig.store.clone();
    let cloud = desc.cloud_id.clone().unwrap();
    wait_until("photo materialized", || {
        let store = store.clone();
        let cloud = cloud.clone();
        async move {
            store
                .photo_by_cloud_id(&cloud)
                .await
                .unwrap()
                .map(|p| p.materialized_at.is_some())
                .unwrap_or(false)
        }
    })
    .await;
    assert!(
        !daemon.is_finished(),
        "loop is a daemon — queue-empty must not exit"
    );

    // A removal now flows through the same loop.
    handle.push(ChangeEvent::Removed {
        local_id: desc.local_id.clone(),
    });
    let store = rig.store.clone();
    wait_until("photo tombstoned", || {
        let store = store.clone();
        let cloud = cloud.clone();
        async move {
            store
                .photo_by_cloud_id(&cloud)
                .await
                .unwrap()
                .map(|p| p.deleted_at.is_some())
                .unwrap_or(false)
        }
    })
    .await;

    rig.cancel.cancel();
    handle.wake();
    let report = daemon.await.unwrap().unwrap();
    assert_eq!(report.events_applied, 2);
    assert_eq!(report.deletions, 1);
    assert_eq!(report.drain.photos_completed, 1);
}

// Impact: SIGTERM-clean is a spec process-model requirement.
// Should: exit promptly on cancellation with a report, even when idle.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_exits_daemon_with_report() {
    let rig = rig().await;
    let (handle, rx) = DaemonHandle::new();
    let sched = scheduler(&rig);
    let daemon = {
        let handle = handle.clone();
        tokio::spawn(async move { sched.run_daemon(rx, handle).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    rig.cancel.cancel();
    handle.wake();
    let report = tokio::time::timeout(Duration::from_secs(5), daemon)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.events_applied, 0);
    assert!(
        !rig.data_dir.root().join("drain.lock").exists(),
        "lock released on exit"
    );
}

// Impact: the lifecycle job must run INSIDE the daemon without an operator —
// hard deletes, log pruning, and replication all ride the loop's timers.
// Should: with a zero interval, an expired tombstone hard-deletes during
// run_daemon; the report carries the cleanup counters.
#[tokio::test(flavor = "multi_thread")]
async fn daemon_tick_runs_cleanup() {
    let mut r = rig().await;
    r.config.cleanup_interval = Duration::ZERO;
    let rig = r;

    let dead = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &dead, b"dead-bytes", None).await;
    scheduler(&rig).drain().await.unwrap();
    let dead_id = rig
        .store
        .photo_by_cloud_id(dead.cloud_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap()
        .photo_id;
    ingress_core::classify::apply_removal(&rig.store, &dead.local_id)
        .await
        .unwrap();
    sqlx::query("UPDATE photos SET deleted_at = ? WHERE photo_id = ?")
        .bind(chrono::Utc::now() - chrono::Duration::days(31))
        .bind(dead_id.to_string())
        .execute(rig.store.raw_pool())
        .await
        .unwrap();

    let (handle, rx) = DaemonHandle::new();
    let sched = scheduler(&rig);
    let daemon = {
        let handle = handle.clone();
        tokio::spawn(async move { sched.run_daemon(rx, handle).await })
    };

    let store = rig.store.clone();
    let dead_check = dead_id.clone();
    wait_until("tombstone hard-deleted by daemon tick", || {
        let store = store.clone();
        let id = dead_check.clone();
        async move { store.photo(&id).await.unwrap().is_none() }
    })
    .await;

    rig.cancel.cancel();
    handle.wake();
    let report = daemon.await.unwrap().unwrap();
    assert!(report.cleanup.photos_hard_deleted >= 1);
    let events = rig.store.log_events("hard_delete").await.unwrap();
    assert_eq!(events.len(), 1);
}

// Impact: a crashed LaunchAgent daemon must reclaim its own stale lock and
// repair refcount drift BEFORE admitting work — drifted counts gate
// irreversible file deletes.
// Should: dead-pid lock reclaimed, seeded drift repaired, drain proceeds.
#[tokio::test(flavor = "multi_thread")]
async fn unclean_start_reclaims_lock_and_repairs_refcounts() {
    let rig = rig().await;
    let desc = AssetDescriptorBuilder::simple_image().build();
    seed_asset(&rig, &desc, b"unclean-bytes", None).await;
    scheduler(&rig).drain().await.unwrap();

    // Simulate a crash: stale lock from a dead pid + refcount drift.
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let dead_pid = child.id();
    child.wait().unwrap();
    std::fs::write(rig.data_dir.root().join("drain.lock"), dead_pid.to_string()).unwrap();
    sqlx::query("UPDATE blobs SET ref_count = 42")
        .execute(rig.store.raw_pool())
        .await
        .unwrap();

    scheduler(&rig).drain().await.unwrap(); // reclaims, repairs, proceeds
    let (count,): (i64,) = sqlx::query_as("SELECT ref_count FROM blobs")
        .fetch_one(rig.store.raw_pool())
        .await
        .unwrap();
    assert_eq!(count, 1, "drift repaired before work");
    assert_eq!(
        rig.store
            .log_events("refcount_repaired")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        !rig.data_dir.root().join("drain.lock").exists(),
        "reclaimed lock released"
    );
}

// Should: idempotently re-seed known assets without minting duplicates.
#[tokio::test(flavor = "multi_thread")]
async fn seed_is_idempotent() {
    let rig = rig().await;
    let cloud = AssetDescriptorBuilder::simple_image().build();
    let local = AssetDescriptorBuilder::simple_image().local_only().build();
    seed_asset(&rig, &cloud, b"c", None).await;
    seed_asset(&rig, &local, b"l", None).await;

    assert!(matches!(
        seed_descriptor(&rig.store, &cloud).await.unwrap(),
        SeedOutcome::AlreadyKnown { .. }
    ));
    assert!(matches!(
        seed_descriptor(&rig.store, &local).await.unwrap(),
        SeedOutcome::AlreadyKnown { .. }
    ));
    assert_eq!(rig.store.count_photos().await.unwrap(), 2);
}

// Impact: original-first ordering is what the late-binding identity merge
// depends on — renditions must never preempt it; and the thumbnail rows must
// flow the full write path (blob, ext derivation, sidecar) like any resource.
// Should: fetch thumbnails LAST (after every real resource), commit them
// with ext jpg, and list them in the completed photo's sidecar.
#[tokio::test(flavor = "multi_thread")]
async fn thumbnails_drain_last_and_flow_the_write_path() {
    let rig = rig().await;
    let desc = AssetDescriptorBuilder::live_photo()
        .with_thumbnails()
        .build();
    rig.fetcher.add_asset(&desc, b"original-bytes", Some(b"paired-bytes"));
    {
        let mut bytes = rig.fetcher.bytes.lock().unwrap();
        bytes.insert((desc.local_id.clone(), 1005), b"small-jpeg".to_vec());
        bytes.insert((desc.local_id.clone(), 1006), b"medium-jpeg".to_vec());
    }
    match seed_descriptor(&rig.store, &desc).await.unwrap() {
        SeedOutcome::MintedPending { .. } => {}
        other => panic!("expected MintedPending, got {other:?}"),
    }

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 1);
    assert_eq!(report.resources_written, 4);

    let order = rig.fetcher.fetch_order.lock().unwrap().clone();
    assert_eq!(order, vec![1, 9, 1005, 1006], "original first, thumbnails last");

    let photo_id = rig
        .store
        .photo_by_cloud_id(desc.cloud_id.as_ref().unwrap())
        .await
        .unwrap()
        .unwrap()
        .photo_id;
    let rows = rig.store.resources_for_photo(&photo_id).await.unwrap();
    for rt in [
        ingress_core::ResourceType::ThumbnailSmall,
        ingress_core::ResourceType::ThumbnailMedium,
    ] {
        let row = rows
            .iter()
            .find(|r| r.resource_type == rt)
            .unwrap_or_else(|| panic!("{rt:?} row"));
        assert!(row.written_at.is_some());
        assert_eq!(row.ext.as_deref(), Some("jpg"));
    }

    let photo = rig.store.photo(&photo_id).await.unwrap().unwrap();
    let capsule_json = photo.descriptor_json.expect("capsule persisted at completion");
    let capsule: ingress_core::descriptor::DescriptorCapsule =
        serde_json::from_str(&capsule_json).unwrap();
    let library = rig
        .store
        .library(&ingress_core::LibraryId::new("personal"))
        .await
        .unwrap()
        .unwrap();
    let photo = rig.store.photo(&photo_id).await.unwrap().unwrap();
    let doc = ingress_core::Sidecar::compose(
        &photo,
        &library,
        capsule.media_type,
        &capsule.media_subtypes,
        capsule.favorite,
        &capsule.capture,
        &rows,
    )
    .unwrap();
    let names: Vec<&str> = doc.resources.iter().map(|r| r.resource_type.as_str()).collect();
    assert!(names.contains(&"thumbnail_small") && names.contains(&"thumbnail_medium"));
}

// Impact: a stale Swift binary (Rust knows the sentinels, descriptors lack
// them) must degrade to thumbnail-only retry burn — never block the photo's
// real resources.
// Should: write the enumerated resources and burn retries only on the
// thumbnail rows ("resource no longer enumerated").
// Should not: materialize the photo while thumbnail rows stay pending.
#[tokio::test(flavor = "multi_thread")]
async fn missing_sentinels_burn_only_thumbnail_retries() {
    let rig = rig().await;
    // Seeded WITH thumbnails (rows exist), but the fetcher's descriptor —
    // what drain re-enumerates — lacks the sentinels.
    let seeded = AssetDescriptorBuilder::simple_image().with_thumbnails().build();
    let mut stale = seeded.clone();
    stale.resources.retain(|r| r.ph_resource_type < 1000);
    rig.fetcher.add_asset(&stale, b"original-bytes", None);
    match seed_descriptor(&rig.store, &seeded).await.unwrap() {
        SeedOutcome::MintedPending { .. } => {}
        other => panic!("expected MintedPending, got {other:?}"),
    }

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 0, "thumbnails gate materialization");

    let photo_id = rig
        .store
        .photo_by_cloud_id(seeded.cloud_id.as_ref().unwrap())
        .await
        .unwrap()
        .unwrap()
        .photo_id;
    let rows = rig.store.resources_for_photo(&photo_id).await.unwrap();
    let original = rows
        .iter()
        .find(|r| r.resource_type == ingress_core::ResourceType::Original)
        .unwrap();
    assert!(original.written_at.is_some(), "real resource unaffected");
    for rt in [
        ingress_core::ResourceType::ThumbnailSmall,
        ingress_core::ResourceType::ThumbnailMedium,
    ] {
        let row = rows.iter().find(|r| r.resource_type == rt).unwrap();
        assert!(row.written_at.is_none());
        assert_eq!(row.retry_count, rig.config.retry_cap, "burned to the cap");
        assert!(
            row.last_error.as_deref().unwrap_or_default().contains("no longer enumerated"),
            "error: {:?}",
            row.last_error
        );
    }
}
