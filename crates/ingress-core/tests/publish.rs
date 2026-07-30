//! Publish queue behavior (spec §HopNet publish queue) — pure Rust via a
//! programmable FakePublisher; no PhotoKit, no node, no Mac. Photos are
//! materialized through the real seed → drain pipeline so sidecars and
//! blobs exist exactly as production leaves them.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use ingress_core::descriptor::AssetDescriptor;
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::paths::DataDir;
use ingress_core::publish::{
    PublishConfig, PublishError, PublishItem, PublishOutcome, PublishState, Publisher,
    claim_publishable, run_publish_pass,
};
use ingress_core::scheduler::{
    BackoffConfig, CancelToken, FetchFailure, FetchRequest, FreeSpaceProbe, ResourceFetcher,
    Scheduler, SchedulerConfig, StreamSink,
};
use ingress_core::{PhotoId, SeedOutcome, StateStore, seed_descriptor};

// ---------------------------------------------------------------- fixtures

struct FakeProbe;

impl FreeSpaceProbe for FakeProbe {
    fn free_bytes(&self, _: &std::path::Path) -> ingress_core::Result<u64> {
        Ok(u64::MAX)
    }
}

#[derive(Default)]
struct FakeFetcher {
    descriptors: Mutex<HashMap<String, AssetDescriptor>>,
    bytes: Mutex<HashMap<(String, i32), Vec<u8>>>,
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
        let bytes = self
            .bytes
            .lock()
            .unwrap()
            .get(&(request.local_id.clone(), request.ph_resource_type))
            .cloned()
            .ok_or_else(|| FetchFailure::AssetUnavailable("no bytes registered".into()))?;
        sink.write(&bytes)?;
        Ok(())
    }
}

/// Scripted publisher: per-call outcomes consumed in order, then the
/// fallback. Records every published photo id.
struct FakePublisher {
    scripted: Mutex<Vec<Result<PublishOutcome, PublishError>>>,
    fallback: fn() -> Result<PublishOutcome, PublishError>,
    calls: AtomicU64,
    seen: Mutex<Vec<PhotoId>>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
}

impl FakePublisher {
    fn ok() -> Self {
        Self::with_fallback(|| Ok(PublishOutcome::Published))
    }

    fn with_fallback(fallback: fn() -> Result<PublishOutcome, PublishError>) -> Self {
        Self {
            scripted: Mutex::new(Vec::new()),
            fallback,
            calls: AtomicU64::new(0),
            seen: Mutex::new(Vec::new()),
            gate: None,
        }
    }
}

#[async_trait::async_trait]
impl Publisher for FakePublisher {
    async fn publish(&self, item: PublishItem) -> Result<PublishOutcome, PublishError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.seen.lock().unwrap().push(item.photo.photo_id.clone());
        if let Some(gate) = &self.gate {
            let _permit = gate.acquire().await.unwrap();
        }
        let scripted = self.scripted.lock().unwrap().pop();
        scripted.unwrap_or_else(|| (self.fallback)())
    }
}

struct Rig {
    store: StateStore,
    data_dir: DataDir,
    fetcher: Arc<FakeFetcher>,
    cancel: CancelToken,
    config: SchedulerConfig,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

async fn rig() -> Rig {
    let blob_dir = tempfile::tempdir().unwrap();
    let data_tmp = tempfile::tempdir().unwrap();
    let store = StateStore::open(&data_tmp.path().join("state.db"))
        .await
        .unwrap();
    store
        .insert_library(&ingress_core::LibraryConfig {
            library_id: ingress_core::LibraryId::new("personal"),
            display_name: "Personal".into(),
            blob_root: blob_dir.path().to_string_lossy().into_owned(),
            sidecar_root_remote: None,
            scope_binding: None,
            retention_days: 30,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    Rig {
        store,
        data_dir: DataDir::new(data_tmp.path()),
        fetcher: Arc::new(FakeFetcher::default()),
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
            cleanup_interval: Duration::from_secs(3600),
            replication_interval: Duration::from_secs(3600),
            cleanup: ingress_core::cleanup::CleanupConfig::default(),
            publish: PublishConfig {
                interval: Duration::from_secs(3600),
                batch: 8,
                retry_cap: 5,
                backoff: BackoffConfig {
                    base: Duration::ZERO,
                    max: Duration::ZERO,
                },
            },
            ..SchedulerConfig::default()
        },
        _dirs: (blob_dir, data_tmp),
    }
}

/// Seed + drain one image asset to full materialization (blob + sidecar on
/// disk, exactly as production leaves them), returning its photo id.
async fn materialize(rig: &Rig, local_id: &str, bytes: &[u8]) -> PhotoId {
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id(&format!("cloud-{local_id}"))
        .with_local_id(local_id)
        .build();
    rig.fetcher
        .descriptors
        .lock()
        .unwrap()
        .insert(desc.local_id.clone(), desc.clone());
    rig.fetcher
        .bytes
        .lock()
        .unwrap()
        .insert((desc.local_id.clone(), 1), bytes.to_vec());
    let photo_id = match seed_descriptor(&rig.store, &desc).await.unwrap() {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    };
    let scheduler = Scheduler::new(
        rig.store.clone(),
        rig.data_dir.clone(),
        rig.fetcher.clone(),
        Arc::new(FakeProbe),
        rig.config.clone(),
        rig.cancel.clone(),
    );
    scheduler.drain().await.unwrap();
    photo_id
}

async fn claim(rig: &Rig) -> Vec<ingress_core::PhotoRecord> {
    claim_publishable(&rig.store, &rig.config.publish, &HashSet::new())
        .await
        .unwrap()
}

async fn photo(rig: &Rig, id: &PhotoId) -> ingress_core::PhotoRecord {
    sqlx::query_as("SELECT * FROM photos WHERE photo_id = ?")
        .bind(id)
        .fetch_one(rig.store.raw_pool())
        .await
        .unwrap()
}

async fn pass(rig: &Rig, publisher: &FakePublisher, state: &mut PublishState) -> ingress_core::publish::PublishReport {
    let claimed = claim(rig).await;
    run_publish_pass(
        &rig.store,
        &rig.data_dir,
        publisher,
        &rig.config.publish,
        claimed,
        state,
    )
    .await
    .unwrap()
}

// ------------------------------------------------------------------- tests

// Impact: the claim predicate is the entire publish/no-publish decision —
// over-claiming re-publishes duplicate photo ids that consensus rejects,
// under-claiming silently strands photos out of HopNet.
// Should: claim only materialized, active, personal-partition photos that
// have never been published.
// Should not: claim tombstoned, shared-partition, already-published, or
// mid-re-edit (unmaterialized) photos.
#[tokio::test(flavor = "multi_thread")]
async fn claims_only_publishable_photos() {
    let rig = rig().await;
    let eligible = materialize(&rig, "eligible", b"eligible-bytes").await;
    let tombstoned = materialize(&rig, "tombstoned", b"tombstoned-bytes").await;
    let published = materialize(&rig, "published", b"published-bytes").await;
    let reedit = materialize(&rig, "reedit", b"reedit-bytes").await;

    sqlx::query("UPDATE photos SET deleted_at = ? WHERE photo_id = ?")
        .bind(Utc::now())
        .bind(&tombstoned)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE photos SET published_at = ? WHERE photo_id = ?")
        .bind(Utc::now())
        .bind(&published)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE photos SET materialized_at = NULL WHERE photo_id = ?")
        .bind(&reedit)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();

    let claimed = claim(&rig).await;
    assert_eq!(
        claimed.iter().map(|p| p.photo_id.clone()).collect::<Vec<_>>(),
        vec![eligible]
    );
}

// Should not: claim photos in the caller's skip set (inflight elsewhere).
#[tokio::test(flavor = "multi_thread")]
async fn claim_honors_skip_set() {
    let rig = rig().await;
    let id = materialize(&rig, "asset", b"bytes").await;
    let skip: HashSet<PhotoId> = [id].into_iter().collect();
    let claimed = claim_publishable(&rig.store, &rig.config.publish, &skip)
        .await
        .unwrap();
    assert!(claimed.is_empty());
}

// Should: mark published photos terminally (stamp set, retry ledger clear)
// and drain the queue — a second pass finds nothing to claim.
#[tokio::test(flavor = "multi_thread")]
async fn pass_marks_published_and_drains_queue() {
    let rig = rig().await;
    let a = materialize(&rig, "a", b"a-bytes").await;
    let b = materialize(&rig, "b", b"b-bytes").await;

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.published, 2);
    assert!(!report.parked);
    for id in [&a, &b] {
        let row = photo(&rig, id).await;
        assert!(row.published_at.is_some());
        assert_eq!(row.publish_attempts, 0);
        assert!(row.publish_last_error.is_none());
    }
    assert!(claim(&rig).await.is_empty());
}

// Should: stamp a photo the confirm probe found already committed without
// re-counting it as newly published.
#[tokio::test(flavor = "multi_thread")]
async fn already_published_outcome_stamps_photo() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;

    let publisher = FakePublisher::with_fallback(|| Ok(PublishOutcome::AlreadyPublished));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.published, 0);
    assert_eq!(report.already_published, 1);
    assert!(photo(&rig, &id).await.published_at.is_some());
}

// Impact: lazy coupling — the daemon owns its lifecycle and must treat a
// down node as "try later", not as per-photo failures that burn the retry
// budget toward gave-up while the node reboots.
// Should: park the pass on an unreachable node, log the edge exactly once
// across consecutive parked passes, and log recovery once on success.
// Should not: consume retry attempts for photos in a parked batch.
#[tokio::test(flavor = "multi_thread")]
async fn unreachable_node_parks_without_consuming_attempts() {
    let rig = rig().await;
    let a = materialize(&rig, "a", b"a-bytes").await;
    let _b = materialize(&rig, "b", b"b-bytes").await;

    let publisher =
        FakePublisher::with_fallback(|| Err(PublishError::NodeUnreachable("refused".into())));
    let mut state = PublishState::default();

    let report = pass(&rig, &publisher, &mut state).await;
    assert!(report.parked);
    assert_eq!(report.published + report.failed + report.gave_up, 0);
    // First failure aborts the batch — the second photo is never attempted.
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 1);
    let row = photo(&rig, &a).await;
    assert_eq!(row.publish_attempts, 0);
    assert!(row.published_at.is_none());

    // Second parked pass: still unreachable, edge already logged.
    let report = pass(&rig, &publisher, &mut state).await;
    assert!(report.parked);
    assert_eq!(rig.store.log_events("node_unreachable").await.unwrap().len(), 1);

    // Node comes back: photos publish and the recovery edge logs once.
    let publisher = FakePublisher::ok();
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.published, 2);
    assert_eq!(rig.store.log_events("node_regained").await.unwrap().len(), 1);
}

// Should: back off transient failures per attempt and stop claiming a photo
// once its attempts reach the cap, logging the give-up.
#[tokio::test(flavor = "multi_thread")]
async fn transient_failures_back_off_to_the_cap() {
    let mut rig = rig().await;
    rig.config.publish.retry_cap = 2;
    let id = materialize(&rig, "a", b"a-bytes").await;

    let publisher = FakePublisher::with_fallback(|| Err(PublishError::Transient("flaky".into())));
    let mut state = PublishState::default();

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.failed, 1);
    assert_eq!(photo(&rig, &id).await.publish_attempts, 1);

    // Zero backoff: immediately claimable for the second (final) attempt.
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.gave_up, 1);
    let row = photo(&rig, &id).await;
    assert_eq!(row.publish_attempts, 2);
    assert_eq!(row.publish_last_error.as_deref(), Some("flaky"));

    assert!(claim(&rig).await.is_empty());
    assert_eq!(rig.store.log_events("publish_gave_up").await.unwrap().len(), 1);
}

// Should: treat a rejection as terminal immediately — attempts jump to the
// cap and the photo leaves the claimable set after one attempt.
#[tokio::test(flavor = "multi_thread")]
async fn rejection_gives_up_immediately() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;

    let publisher =
        FakePublisher::with_fallback(|| Err(PublishError::Rejected("bad mapping".into())));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.gave_up, 1);
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 1);
    let row = photo(&rig, &id).await;
    assert_eq!(row.publish_attempts, rig.config.publish.retry_cap);
    assert!(claim(&rig).await.is_empty());
    assert_eq!(rig.store.log_events("publish_rejected").await.unwrap().len(), 1);
}

// Should: skip a claimed photo whose sidecar is missing (crash window) and
// keep publishing the rest of the batch.
#[tokio::test(flavor = "multi_thread")]
async fn missing_sidecar_skips_photo_but_batch_continues() {
    let rig = rig().await;
    let broken = materialize(&rig, "broken", b"broken-bytes").await;
    let healthy = materialize(&rig, "healthy", b"healthy-bytes").await;

    let sidecar_root = rig
        .data_dir
        .sidecar_root(&ingress_core::LibraryId::new("personal"));
    let path = ingress_core::sidecar_io::find_sidecar(&sidecar_root, &broken)
        .unwrap()
        .expect("sidecar written at materialization");
    std::fs::remove_file(path).unwrap();

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.missing_sidecar, 1);
    assert_eq!(report.published, 1);
    assert!(photo(&rig, &healthy).await.published_at.is_some());
    assert!(photo(&rig, &broken).await.published_at.is_none());
    assert_eq!(
        rig.store
            .log_events("publish_sidecar_missing")
            .await
            .unwrap()
            .len(),
        1
    );
}

// Impact: consensus hard-rejects duplicate photo ids (proposer preflight),
// so a re-edited published photo must NOT re-enter the queue — content-update
// propagation is a separate future transaction, not a re-publish.
// Should not: re-claim a published photo after it re-materializes.
#[tokio::test(flavor = "multi_thread")]
async fn republished_reedit_is_not_reclaimed() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    assert_eq!(pass(&rig, &publisher, &mut state).await.published, 1);

    // Simulate a re-edit completing: re-materialized, sidecar re-dirtied.
    sqlx::query(
        "UPDATE photos SET materialized_at = ?, sidecar_replicated_at = NULL WHERE photo_id = ?",
    )
    .bind(Utc::now())
    .bind(&id)
    .execute(rig.store.raw_pool())
    .await
    .unwrap();

    assert!(claim(&rig).await.is_empty());
}

// Impact: the daemon tick claims photos INFLIGHT for the pass duration so
// PhotoKit events for a mid-publish photo defer instead of rewriting state
// under the streaming blob read.
// Should: run the publish tick from the daemon loop, defer events for
// mid-publish photos, and surface pass totals in the daemon report.
#[tokio::test(flavor = "multi_thread")]
async fn daemon_tick_publishes_and_defers_events() {
    use ingress_core::scheduler::daemon::{ChangeEvent, DaemonHandle};

    let mut rig = rig().await;
    rig.config.publish.interval = Duration::from_millis(20);
    let id = materialize(&rig, "a", b"a-bytes").await;
    let desc = rig
        .fetcher
        .descriptors
        .lock()
        .unwrap()
        .get("a")
        .cloned()
        .unwrap();

    // Publisher blocks until released — the photo stays inflight while we
    // push an event for it.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut publisher = FakePublisher::ok();
    publisher.gate = Some(gate.clone());
    let publisher = Arc::new(publisher);

    let (handle, rx) = DaemonHandle::new();
    let scheduler = Scheduler::new(
        rig.store.clone(),
        rig.data_dir.clone(),
        rig.fetcher.clone(),
        Arc::new(FakeProbe),
        rig.config.clone(),
        rig.cancel.clone(),
    )
    .with_publisher(publisher.clone());
    let daemon = {
        let handle = handle.clone();
        tokio::spawn(async move { scheduler.run_daemon(rx, handle).await })
    };

    // Wait until the publish task holds the photo (publisher entered).
    for _ in 0..500 {
        if publisher.calls.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 1);

    // An event for the mid-publish photo must defer, not apply.
    handle.push(ChangeEvent::Descriptor(Box::new(desc)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.add_permits(10);

    // Published state lands after release.
    for _ in 0..500 {
        if photo(&rig, &id).await.published_at.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(photo(&rig, &id).await.published_at.is_some());

    rig.cancel.cancel();
    handle.wake();
    let report = daemon.await.unwrap().unwrap();
    assert_eq!(report.publish.published, 1);
    assert!(report.events_deferred >= 1);
}
