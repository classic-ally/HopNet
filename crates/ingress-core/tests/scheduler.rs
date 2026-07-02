//! Scheduler behavior (spec §Ingest Pipeline, §Failure Handling) — pure
//! Rust via a programmable FakeFetcher; no PhotoKit, no Mac.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use ingress_core::descriptor::AssetDescriptor;
use ingress_core::fixtures::{add_shared, store_with_personal, AssetDescriptorBuilder};
use ingress_core::paths::DataDir;
use ingress_core::scheduler::{
    BackoffConfig, CancelToken, FetchFailure, FetchRequest, FreeSpaceProbe, ResourceFetcher,
    Scheduler, SchedulerConfig, StreamSink,
};
use ingress_core::{seed_descriptor, LibraryScope, SeedOutcome, StateStore};

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
    let (store, library) = store_with_personal().await;
    let blob_dir = tempfile::tempdir().unwrap();
    let data_tmp = tempfile::tempdir().unwrap();
    // Point the personal library's blob_root at the temp dir.
    sqlx::query("UPDATE libraries SET blob_root = ? WHERE library_id = ?")
        .bind(blob_dir.path().to_string_lossy().into_owned())
        .bind(library.as_str())
        .execute(store.raw_pool())
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
            backoff: BackoffConfig { base: Duration::ZERO, max: Duration::ZERO },
            reserve_floor_bytes: 0,
            pressure_pause: Duration::from_millis(10),
            storage_poll: Duration::from_millis(10),
            default_size_estimate: 1024,
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
    rig.fetcher.fail_next(&desc.local_id, 1, FetchFailure::Transient("blip".into()));

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 1);
    let rt: (i64,) = sqlx::query_as(
        "SELECT retry_count FROM photo_resources WHERE resource_type = 0",
    )
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
        rig.fetcher.fail_next(&desc.local_id, 1, FetchFailure::Transient("down".into()));
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
    rig.fetcher.fail_next(&desc.local_id, 1, FetchFailure::LocalDiskPressure);

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
    assert_eq!(rig.store.log_events("storage_recovered").await.unwrap().len(), 1);
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
    assert!(!rig.store.log_events("storage_low").await.unwrap().is_empty());
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
    let survivor = rig.store.photo_by_cloud_id("CLOUD-MERGE:001").await.unwrap().unwrap();
    assert!(survivor.materialized_at.is_some());
    assert_eq!(rig.store.log_events("late_binding_merge").await.unwrap().len(), 1);
}

// Should: adopt an unmapped photo once its scope is bound, making its
// pending rows drain-eligible.
#[tokio::test(flavor = "multi_thread")]
async fn unmapped_then_adopted_then_drained() {
    let rig = rig().await;
    let desc = AssetDescriptorBuilder::simple_image().scope(LibraryScope::Shared).build();
    rig.fetcher.add_asset(&desc, b"shared-bytes", None);

    match seed_descriptor(&rig.store, &desc).await.unwrap() {
        SeedOutcome::Unmapped { .. } => {}
        other => panic!("expected Unmapped, got {other:?}"),
    }
    // Drain does nothing — NULL-library rows are not eligible.
    assert_eq!(scheduler(&rig).drain().await.unwrap().photos_completed, 0);

    let shared_lib = add_shared(&rig.store).await;
    sqlx::query("UPDATE libraries SET blob_root = ? WHERE library_id = ?")
        .bind(rig._dirs.0.path().join("shared").to_string_lossy().into_owned())
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
    rig.fetcher.descriptors.lock().unwrap().insert(desc.local_id.clone(), drifted);

    let report = scheduler(&rig).drain().await.unwrap();
    assert_eq!(report.photos_completed, 0); // paired video still pending
    assert_eq!(report.resources_written, 1); // original archived
    let (err,): (Option<String>,) = sqlx::query_as(
        "SELECT last_error FROM photo_resources WHERE resource_type = 2",
    )
    .fetch_one(rig.store.raw_pool())
    .await
    .unwrap();
    assert!(err.unwrap().contains("no longer enumerated"));
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
