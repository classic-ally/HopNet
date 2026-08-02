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
    EditItem, PassWork, PublishConfig, PublishError, PublishItem, PublishOutcome, PublishState,
    Publisher, ResolveEntry, ResolveOutcome, Responsibility, TombstoneOp, claim_editable,
    claim_publishable, claim_tombstone_propagatable, run_publish_pass,
};
use ingress_core::scheduler::{
    BackoffConfig, CancelToken, FetchFailure, FetchRequest, FreeSpaceProbe, ResourceFetcher,
    Scheduler, SchedulerConfig, StreamSink,
};
use ingress_core::ids::ContentHash;
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
    /// The `cloud_fingerprint` each publish item carried, in call order.
    seen_fingerprints: Mutex<Vec<Option<String>>>,
    /// The recomposed metadata document each publish item carried.
    seen_sidecars: Mutex<Vec<ingress_core::sidecar::Sidecar>>,
    /// None = use the trait's legacy default (Holder, no entries).
    resolve_result: Mutex<Option<Result<ResolveOutcome, PublishError>>>,
    /// Per-scope scripted resolves (key None = personal); falls back to
    /// `resolve_result`, then the trait default.
    resolve_results:
        Mutex<std::collections::HashMap<Option<String>, Result<ResolveOutcome, PublishError>>>,
    /// The (scope, cloud_id batch) pairs the pass sent to resolve.
    resolve_seen: Mutex<Vec<(Option<String>, Vec<String>)>>,
    /// The (consensus id, direction) pairs the pass propagated, in order.
    propagate_seen: Mutex<Vec<(String, TombstoneOp)>>,
    /// Scripted propagation results, popped per call; empty = Ok.
    propagate_scripted: Mutex<Vec<Result<(), PublishError>>>,
    /// Every edit the pass submitted, in order.
    edits_seen: Mutex<Vec<EditItem>>,
    /// Scripted edit results, popped per call; empty = Ok.
    edit_scripted: Mutex<Vec<Result<(), PublishError>>>,
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
            seen_fingerprints: Mutex::new(Vec::new()),
            seen_sidecars: Mutex::new(Vec::new()),
            resolve_result: Mutex::new(None),
            resolve_results: Mutex::new(std::collections::HashMap::new()),
            resolve_seen: Mutex::new(Vec::new()),
            propagate_seen: Mutex::new(Vec::new()),
            propagate_scripted: Mutex::new(Vec::new()),
            edits_seen: Mutex::new(Vec::new()),
            edit_scripted: Mutex::new(Vec::new()),
            gate: None,
        }
    }

    fn propagated(&self) -> Vec<(String, TombstoneOp)> {
        self.propagate_seen.lock().unwrap().clone()
    }

    fn script_propagate(&self, results: Vec<Result<(), PublishError>>) {
        *self.propagate_scripted.lock().unwrap() = results;
    }

    fn edits(&self) -> Vec<EditItem> {
        self.edits_seen.lock().unwrap().clone()
    }

    fn script_edit(&self, results: Vec<Result<(), PublishError>>) {
        *self.edit_scripted.lock().unwrap() = results;
    }

    fn set_resolve(&self, result: Result<ResolveOutcome, PublishError>) {
        *self.resolve_result.lock().unwrap() = Some(result);
    }

    fn set_resolve_for(&self, scope: Option<&str>, result: Result<ResolveOutcome, PublishError>) {
        self.resolve_results
            .lock()
            .unwrap()
            .insert(scope.map(str::to_string), result);
    }
}

fn entry(cloud_id: &str, fingerprint: &str, committed: Option<&str>) -> ResolveEntry {
    ResolveEntry {
        cloud_id: cloud_id.into(),
        fingerprint: fingerprint.into(),
        committed_photo_id: committed.map(Into::into),
    }
}

fn outcome(responsibility: Responsibility, entries: Vec<ResolveEntry>) -> ResolveOutcome {
    ResolveOutcome {
        responsibility,
        entries,
    }
}

#[async_trait::async_trait]
impl Publisher for FakePublisher {
    async fn publish(&self, item: PublishItem) -> Result<PublishOutcome, PublishError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.seen.lock().unwrap().push(item.photo.photo_id.clone());
        self.seen_fingerprints
            .lock()
            .unwrap()
            .push(item.cloud_fingerprint.clone());
        self.seen_sidecars.lock().unwrap().push(item.sidecar.clone());
        if let Some(gate) = &self.gate {
            let _permit = gate.acquire().await.unwrap();
        }
        let scripted = self.scripted.lock().unwrap().pop();
        scripted.unwrap_or_else(|| (self.fallback)())
    }

    async fn resolve(
        &self,
        library_id: Option<&str>,
        cloud_ids: &[String],
    ) -> Result<ResolveOutcome, PublishError> {
        self.resolve_seen
            .lock()
            .unwrap()
            .push((library_id.map(str::to_string), cloud_ids.to_vec()));
        match self.resolve_results.lock().unwrap().get(&library_id.map(str::to_string)) {
            Some(result) => result.clone(),
            None => match &*self.resolve_result.lock().unwrap() {
                Some(result) => result.clone(),
                None => Ok(outcome(Responsibility::Holder, Vec::new())),
            },
        }
    }

    async fn propagate_tombstone(
        &self,
        consensus_photo_id: &str,
        op: TombstoneOp,
    ) -> Result<(), PublishError> {
        self.propagate_seen
            .lock()
            .unwrap()
            .push((consensus_photo_id.to_string(), op));
        let scripted = self.propagate_scripted.lock().unwrap().pop();
        scripted.unwrap_or(Ok(()))
    }

    async fn publish_edit(&self, item: EditItem) -> Result<(), PublishError> {
        self.edits_seen.lock().unwrap().push(item);
        let scripted = self.edit_scripted.lock().unwrap().pop();
        scripted.unwrap_or(Ok(()))
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
            scope_binding: None,
            retention_days: 30,
            created_at: Utc::now(),
            mesh_library_id: None,
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
        },
        _dirs: (blob_dir, data_tmp),
    }
}

/// Insert the daemon's single shared (SPL-bound) library, optionally with
/// a mesh publish target. The marker is UNIQUE — one shared library per
/// daemon, so scope tests run personal + shared.
async fn add_shared_library(rig: &Rig, mesh_library_id: Option<&str>) {
    rig.store
        .insert_library(&ingress_core::LibraryConfig {
            library_id: ingress_core::LibraryId::new("shared"),
            display_name: "Shared".into(),
            scope_binding: Some(ingress_core::model::ICLOUD_SHARED_LIBRARY_BINDING.into()),
            retention_days: 30,
            created_at: Utc::now(),
            mesh_library_id: mesh_library_id.map(str::to_string),
        })
        .await
        .unwrap();
}

async fn set_mesh_binding(rig: &Rig, mesh_library_id: Option<&str>) {
    sqlx::query("UPDATE libraries SET mesh_library_id = ? WHERE library_id = 'shared'")
        .bind(mesh_library_id)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
}

/// Seed + drain one image asset to full materialization (blob + sidecar on
/// disk, exactly as production leaves them), returning its photo id.
async fn materialize(rig: &Rig, local_id: &str, bytes: &[u8]) -> PhotoId {
    materialize_scoped(rig, local_id, bytes, ingress_core::descriptor::LibraryScope::Personal)
        .await
}

async fn materialize_shared(rig: &Rig, local_id: &str, bytes: &[u8]) -> PhotoId {
    materialize_scoped(rig, local_id, bytes, ingress_core::descriptor::LibraryScope::Shared).await
}

async fn materialize_scoped(
    rig: &Rig,
    local_id: &str,
    bytes: &[u8],
    scope: ingress_core::descriptor::LibraryScope,
) -> PhotoId {
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id(&format!("cloud-{local_id}"))
        .with_local_id(local_id)
        .scope(scope)
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
    let propagatable = claim_tombstone_propagatable(&rig.store, &rig.config.publish, &HashSet::new())
        .await
        .unwrap();
    let editable = claim_editable(&rig.store, &rig.config.publish, &HashSet::new())
        .await
        .unwrap();
    run_publish_pass(
        &rig.store,
        &rig.data_dir.spool(),
        publisher,
        &rig.config.publish,
        PassWork {
            claimed,
            propagatable,
            editable,
        },
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

// Should: skip a claimed photo whose publish-metadata capsule is absent
// (pre-capsule row awaiting heal) and keep publishing the rest of the batch.
// Should not: burn a publish attempt on the skipped photo.
#[tokio::test(flavor = "multi_thread")]
async fn missing_descriptor_skips_photo_but_batch_continues() {
    let rig = rig().await;
    let broken = materialize(&rig, "broken", b"broken-bytes").await;
    let healthy = materialize(&rig, "healthy", b"healthy-bytes").await;

    sqlx::query("UPDATE photos SET descriptor_json = NULL WHERE photo_id = ?")
        .bind(&broken)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.missing_descriptor, 1);
    assert_eq!(report.published, 1);
    assert!(photo(&rig, &healthy).await.published_at.is_some());
    let broken_row = photo(&rig, &broken).await;
    assert!(broken_row.published_at.is_none());
    assert_eq!(broken_row.publish_attempts, 0);
    assert_eq!(
        rig.store
            .log_events("publish_descriptor_missing")
            .await
            .unwrap()
            .len(),
        1
    );
}

// Impact: state.db is the sole publish-metadata source — publish must
// recompose its metadata document from the persisted capsule + live DB
// rows, with no filesystem dependency at all.
#[tokio::test(flavor = "multi_thread")]
async fn publishes_from_capsule_and_db_rows() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.published, 1);
    assert_eq!(report.missing_descriptor, 0);
    let sidecars = publisher.seen_sidecars.lock().unwrap();
    assert_eq!(sidecars.len(), 1);
    assert_eq!(sidecars[0].photo_id, id);
    assert_eq!(
        sidecars[0].library_id,
        ingress_core::LibraryId::new("personal")
    );
    assert!(!sidecars[0].resources.is_empty());
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

    // Simulate a re-edit completing: re-materialized.
    sqlx::query(
        "UPDATE photos SET materialized_at = ? WHERE photo_id = ?",
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

// ---------------------------------------------- resolve / adoption / gate

// Impact: adoption is what makes a second device (or a fresh state.db after
// handoff) converge on the mesh's existing photos instead of duplicating
// the whole archive upload.
// Should: stamp a resolve-hit photo as adopted with the remote consensus id
// recorded, and drain it from the claimable queue.
// Should not: call publish (no bytes move) or consume retry attempts for an
// adopted photo.
#[tokio::test(flavor = "multi_thread")]
async fn resolve_hit_adopts_without_publishing() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;

    let publisher = FakePublisher::ok();
    let remote = "01890a5d-ac96-774b-0000-000000000042";
    publisher.set_resolve(Ok(outcome(
        Responsibility::Holder,
        vec![entry("cloud-a", &"ab".repeat(32), Some(remote))],
    )));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.adopted, 1);
    assert_eq!(report.published, 0);
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 0, "nothing uploads");
    let row = photo(&rig, &id).await;
    assert!(row.published_at.is_some());
    assert_eq!(row.consensus_photo_id.as_deref(), Some(remote));
    assert_eq!(row.publish_attempts, 0);
    assert!(claim(&rig).await.is_empty());
    assert_eq!(rig.store.log_events("publish_adopted").await.unwrap().len(), 1);
}

// Should: count a resolve hit on the photo's OWN id as already-published
// (an earlier ambiguous submit landed) rather than adopted, leaving
// consensus_photo_id NULL (self-published identity).
#[tokio::test(flavor = "multi_thread")]
async fn self_resolution_counts_already_published() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve(Ok(outcome(
        Responsibility::Holder,
        vec![entry("cloud-a", &"ab".repeat(32), Some(id.as_str()))],
    )));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.already_published, 1);
    assert_eq!(report.adopted, 0);
    let row = photo(&rig, &id).await;
    assert!(row.published_at.is_some());
    assert!(row.consensus_photo_id.is_none());
}

// Impact: explicit-claim contract — a daemon must never publish before a
// human designates it, and a stale designation must not burn retry budget
// while it waits for a transfer.
// Should: hold remaining photos with a distinct parked state when not the
// holder, log the edge once, and log recovery once after a claim/transfer.
// Should not: call publish or consume attempts while parked.
#[tokio::test(flavor = "multi_thread")]
async fn non_holder_parks_remaining_without_attempts() {
    let rig = rig().await;
    let a = materialize(&rig, "a", b"a-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve(Ok(outcome(Responsibility::Unclaimed, Vec::new())));
    let mut state = PublishState::default();

    let report = pass(&rig, &publisher, &mut state).await;
    assert!(report.parked_responsibility);
    assert!(!report.parked, "responsibility park is distinct from unreachable park");
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 0);
    let row = photo(&rig, &a).await;
    assert_eq!(row.publish_attempts, 0);
    assert!(row.published_at.is_none());

    // Second parked pass: edge already logged.
    let report = pass(&rig, &publisher, &mut state).await;
    assert!(report.parked_responsibility);
    assert_eq!(
        rig.store.log_events("publish_not_responsible").await.unwrap().len(),
        1
    );

    // Claim lands (holder now): photo publishes, recovery edge logs once.
    publisher.set_resolve(Ok(outcome(Responsibility::Holder, Vec::new())));
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.published, 1);
    assert!(!report.parked_responsibility);
    assert_eq!(
        rig.store.log_events("responsibility_regained").await.unwrap().len(),
        1
    );
}

// Impact: adoption during non-holder standing is what makes a
// responsibility handoff a cheap sweep — the incoming Mac converges on the
// mesh's photos before it is ever allowed to publish.
// Should: adopt resolve hits even when another device holds responsibility,
// while holding (not failing) the genuinely-new photos.
#[tokio::test(flavor = "multi_thread")]
async fn non_holder_still_adopts() {
    let rig = rig().await;
    let known = materialize(&rig, "a", b"a-bytes").await;
    let fresh = materialize(&rig, "b", b"b-bytes").await;

    let publisher = FakePublisher::ok();
    let remote = "01890a5d-ac96-774b-0000-000000000099";
    publisher.set_resolve(Ok(outcome(
        Responsibility::Other,
        vec![entry("cloud-a", &"cd".repeat(32), Some(remote))],
    )));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.adopted, 1);
    assert!(report.parked_responsibility);
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        photo(&rig, &known).await.consensus_photo_id.as_deref(),
        Some(remote)
    );
    let held = photo(&rig, &fresh).await;
    assert!(held.published_at.is_none());
    assert_eq!(held.publish_attempts, 0);
}

// Should: stamp the resolve entry's fingerprint into the publish item, and
// publish a NULL-cloud_id photo with no fingerprint (exempt from dedupe,
// absent from the resolve batch).
#[tokio::test(flavor = "multi_thread")]
async fn fingerprints_thread_into_publish_items() {
    let rig = rig().await;
    let _with_cloud = materialize(&rig, "a", b"a-bytes").await;
    let local_only = materialize(&rig, "b", b"b-bytes").await;
    sqlx::query("UPDATE photos SET cloud_id = NULL WHERE photo_id = ?")
        .bind(&local_only)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();

    let publisher = FakePublisher::ok();
    let fp = "ef".repeat(32);
    publisher.set_resolve(Ok(outcome(
        Responsibility::Holder,
        vec![entry("cloud-a", &fp, None)],
    )));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.published, 2);
    assert_eq!(
        *publisher.resolve_seen.lock().unwrap(),
        vec![(None, vec!["cloud-a".to_string()])],
        "only cloud-bearing photos enter the resolve batch, personal scope"
    );
    let mut fingerprints = publisher.seen_fingerprints.lock().unwrap().clone();
    fingerprints.sort();
    assert_eq!(fingerprints, vec![None, Some(fp)]);
}

// Should: park on an unreachable resolve exactly like an unreachable
// publish — batch aborted, edge logged once, no attempts consumed.
#[tokio::test(flavor = "multi_thread")]
async fn resolve_unreachable_parks_the_pass() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve(Err(PublishError::NodeUnreachable("refused".into())));
    let mut state = PublishState::default();

    let report = pass(&rig, &publisher, &mut state).await;
    assert!(report.parked);
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 0);
    assert_eq!(photo(&rig, &id).await.publish_attempts, 0);
    let report = pass(&rig, &publisher, &mut state).await;
    assert!(report.parked);
    assert_eq!(rig.store.log_events("node_unreachable").await.unwrap().len(), 1);
}

// Should: burn one attempt per claimed photo when resolve fails
// non-transport (a persistently broken resolve must back off toward
// gave_up, not spin silently forever).
#[tokio::test(flavor = "multi_thread")]
async fn resolve_failure_burns_one_attempt_per_photo() {
    let rig = rig().await;
    let a = materialize(&rig, "a", b"a-bytes").await;
    let b = materialize(&rig, "b", b"b-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve(Err(PublishError::Transient("resolve 500".into())));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.failed, 2);
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 0);
    for id in [&a, &b] {
        let row = photo(&rig, id).await;
        assert_eq!(row.publish_attempts, 1);
        assert!(row.publish_last_error.as_deref().unwrap().contains("resolve"));
    }
}

// ------------------------------------------------------ scoped publish pass

const MESH_LIB: &str = "0198f3a2-aaaa-bbbb-cccc-dddddddddddd";

// Impact: the claim predicate is the only thing standing between the
// iCloud shared partition and the personal consensus namespace — an
// unbound shared library published as personal photos is exactly the
// dedup debt the old personal-only gate existed to avoid.
// Should: claim shared-library photos once the library is mesh-bound.
// Should not: claim shared photos while the mesh binding is absent —
// the v1 exclusion is preserved verbatim for unbound rows.
#[tokio::test(flavor = "multi_thread")]
async fn claim_requires_publish_target() {
    let rig = rig().await;
    add_shared_library(&rig, None).await;
    let personal = materialize(&rig, "p", b"p-bytes").await;
    let shared = materialize_shared(&rig, "s", b"s-bytes").await;

    let claimed = claim(&rig).await;
    assert_eq!(
        claimed.iter().map(|p| p.photo_id.clone()).collect::<Vec<_>>(),
        vec![personal.clone()],
        "unbound shared photos must stay unclaimed"
    );

    set_mesh_binding(&rig, Some(MESH_LIB)).await;
    let claimed = claim(&rig).await;
    let ids: Vec<PhotoId> = claimed.iter().map(|p| p.photo_id.clone()).collect();
    assert!(ids.contains(&personal) && ids.contains(&shared));
}

// Should: issue exactly one resolve per scope — personal first, carrying
// only that scope's cloud ids, byte-identical to the v1 personal call.
#[tokio::test(flavor = "multi_thread")]
async fn pass_partitions_resolve_per_scope() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    materialize(&rig, "p", b"p-bytes").await;
    materialize_shared(&rig, "s1", b"s1-bytes").await;
    materialize_shared(&rig, "s2", b"s2-bytes").await;

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.published, 3);

    let seen = publisher.resolve_seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].0, None, "personal scope resolves first");
    assert_eq!(seen[0].1, vec!["cloud-p".to_string()]);
    assert_eq!(seen[1].0.as_deref(), Some(MESH_LIB));
    let mut shared_ids = seen[1].1.clone();
    shared_ids.sort();
    assert_eq!(shared_ids, vec!["cloud-s1".to_string(), "cloud-s2".to_string()]);
}

// Impact: v1 parked the WHOLE batch on lost standing; per-scope parking is
// what lets the personal queue keep draining while a shared library waits
// for a claim (or after a kick).
// Should: park only the not-responsible scope's photos, burning no
// attempts for them, while the other scope publishes in the same pass.
#[tokio::test(flavor = "multi_thread")]
async fn responsibility_parks_per_scope() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    let personal = materialize(&rig, "p", b"p-bytes").await;
    let shared = materialize_shared(&rig, "s", b"s-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve_for(Some(MESH_LIB), Ok(outcome(Responsibility::Other, Vec::new())));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.published, 1);
    assert!(report.parked_responsibility);
    assert_eq!(*publisher.seen.lock().unwrap(), vec![personal]);
    let row = photo(&rig, &shared).await;
    assert_eq!(row.publish_attempts, 0, "parking must not burn attempts");
    assert!(row.published_at.is_none());
}

// Should: run adoption for a non-holder scope before parking it — a
// responsibility handoff (or another member's publish) stays a cheap
// sweep per scope.
#[tokio::test(flavor = "multi_thread")]
async fn adoption_precedes_scope_park() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    let shared = materialize_shared(&rig, "s", b"s-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve_for(
        Some(MESH_LIB),
        Ok(outcome(
            Responsibility::Other,
            vec![entry("cloud-s", &"ab".repeat(32), Some("remote-id"))],
        )),
    );
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.adopted, 1);
    assert!(!report.parked_responsibility, "nothing left to park after adoption");
    assert_eq!(photo(&rig, &shared).await.consensus_photo_id.as_deref(), Some("remote-id"));
}

// Impact: a kicked member's shared-scope resolve 403s forever — that
// must back its own photos off toward gave_up without starving the
// personal queue.
// Should: burn attempts only for the failing scope's photos on a
// non-unreachable resolve error; the other scope proceeds untouched.
#[tokio::test(flavor = "multi_thread")]
async fn resolve_failure_isolated_to_scope() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    let personal = materialize(&rig, "p", b"p-bytes").await;
    let shared = materialize_shared(&rig, "s", b"s-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve_for(
        Some(MESH_LIB),
        Err(PublishError::Transient("http 403: library_not_member".into())),
    );
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.published, 1);
    assert_eq!(report.failed, 1);
    assert_eq!(photo(&rig, &personal).await.publish_attempts, 0);
    let row = photo(&rig, &shared).await;
    assert_eq!(row.publish_attempts, 1);
    assert!(row.publish_last_error.as_deref().unwrap().contains("403"));
}

// Should: park the entire pass on NodeUnreachable regardless of scope —
// no later scope resolves, no attempts burned anywhere (regression on the
// v1 whole-pass park).
#[tokio::test(flavor = "multi_thread")]
async fn unreachable_parks_whole_pass() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    let personal = materialize(&rig, "p", b"p-bytes").await;
    let shared = materialize_shared(&rig, "s", b"s-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve_for(None, Err(PublishError::NodeUnreachable("refused".into())));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert!(report.parked);
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        publisher.resolve_seen.lock().unwrap().len(),
        1,
        "the shared scope must not resolve after the park"
    );
    for id in [&personal, &shared] {
        assert_eq!(photo(&rig, id).await.publish_attempts, 0);
    }
}

// Should: edge-trigger publish_not_responsible / responsibility_regained
// per scope, tagging the log entry with the library.
#[tokio::test(flavor = "multi_thread")]
async fn responsibility_edges_are_scoped() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    materialize(&rig, "p", b"p-bytes").await;
    materialize_shared(&rig, "s", b"s-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve_for(Some(MESH_LIB), Ok(outcome(Responsibility::Other, Vec::new())));
    let mut state = PublishState::default();
    pass(&rig, &publisher, &mut state).await;
    pass(&rig, &publisher, &mut state).await;

    let parked = rig.store.log_events("publish_not_responsible").await.unwrap();
    assert_eq!(parked.len(), 1, "edge fires once while parked");
    assert!(
        parked[0].detail.as_deref().unwrap().contains(MESH_LIB),
        "park edge names the library"
    );

    publisher.set_resolve_for(Some(MESH_LIB), Ok(outcome(Responsibility::Holder, Vec::new())));
    pass(&rig, &publisher, &mut state).await;
    let regained = rig.store.log_events("responsibility_regained").await.unwrap();
    assert_eq!(regained.len(), 1);
    assert!(regained[0].detail.as_deref().unwrap().contains(MESH_LIB));
}

// Should: skip (transient, publisher untouched) a scope-bound photo whose
// library lost its mesh binding between claim and assemble — the
// defense-in-depth re-check behind the run lock.
#[tokio::test(flavor = "multi_thread")]
async fn assemble_skips_mesh_unbound() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    let shared = materialize_shared(&rig, "s", b"s-bytes").await;

    let claimed = claim(&rig).await;
    assert_eq!(claimed.len(), 1);
    set_mesh_binding(&rig, None).await;

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let report = run_publish_pass(
        &rig.store,
        &rig.data_dir.spool(),
        &publisher,
        &rig.config.publish,
        PassWork {
            claimed,
            ..Default::default()
        },
        &mut state,
    )
    .await
    .unwrap();

    assert_eq!(report.published, 0);
    assert_eq!(report.failed, 1);
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 0);
    let row = photo(&rig, &shared).await;
    assert!(row.publish_last_error.as_deref().unwrap().contains("not bound"));
}

// ------------------------------------------------------------ spool eviction

/// The on-disk blob file for a written resource of `id`.
async fn blob_file(rig: &Rig, id: &PhotoId) -> std::path::PathBuf {
    let row = &rig.store.resources_for_photo(id).await.unwrap()[0];
    rig.data_dir.spool().blob_path(
        row.content_hash.as_ref().unwrap(),
        row.ext.as_deref().unwrap(),
    )
}

async fn blob_row(rig: &Rig, id: &PhotoId) -> ingress_core::model::BlobRecord {
    let hash = rig.store.resources_for_photo(id).await.unwrap()[0]
        .content_hash
        .clone()
        .unwrap();
    rig.store
        .blob(&ingress_core::LibraryId::new("personal"), &hash)
        .await
        .unwrap()
        .unwrap()
}

// Impact: eviction is the transplant's point — after HopNet takes custody
// the local copy is spool pollution — but deleting bytes for an UNDECIDED
// photo is data loss, since nothing else holds them yet.
// Should: evict a published photo's blob at the end of the pass — file
// deleted, ledger row retained with its refcount and an eviction stamp,
// photo and resource rows untouched.
#[tokio::test(flavor = "multi_thread")]
async fn pass_evicts_blobs_of_published_photos() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;
    let file = blob_file(&rig, &id).await;
    assert!(file.is_file());

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.published, 1);
    assert_eq!(report.evicted_blobs, 1);
    assert!(!file.exists(), "spool bytes deleted after decided publish");
    let blob = blob_row(&rig, &id).await;
    assert!(blob.evicted_at.is_some());
    assert_eq!(blob.ref_count, 1, "refcount untouched by eviction");
    let row = &rig.store.resources_for_photo(&id).await.unwrap()[0];
    assert!(row.written_at.is_some(), "resource row untouched");
    assert!(photo(&rig, &id).await.published_at.is_some());
    assert_eq!(
        rig.store.log_events("spool_evicted").await.unwrap().len(),
        1
    );
}

// Impact: the never-delete-undecided invariant under dedup — one blob, two
// photos, only one decided: the bytes are still the sole copy for the other.
// Should: retain a shared blob until EVERY referencing photo is decided,
// then evict it.
#[tokio::test(flavor = "multi_thread")]
async fn shared_blob_evicts_only_when_all_referents_decided() {
    let rig = rig().await;
    let a = materialize(&rig, "a", b"same-bytes").await;
    let b = materialize(&rig, "b", b"same-bytes").await;
    let file = blob_file(&rig, &a).await;
    assert_eq!(blob_row(&rig, &a).await.ref_count, 2, "deduped share");

    // Keep b out of the first pass via a future retry deadline.
    sqlx::query(
        "UPDATE photos SET publish_attempts = 1, publish_next_retry_at = ? WHERE photo_id = ?",
    )
    .bind(Utc::now() + chrono::Duration::hours(1))
    .bind(&b)
    .execute(rig.store.raw_pool())
    .await
    .unwrap();

    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.published, 1);
    assert_eq!(report.evicted_blobs, 0, "undecided referent keeps the blob");
    assert!(file.is_file());
    assert!(blob_row(&rig, &a).await.evicted_at.is_none());

    // b becomes claimable and publishes: now every referent is decided.
    sqlx::query(
        "UPDATE photos SET publish_attempts = 0, publish_next_retry_at = NULL WHERE photo_id = ?",
    )
    .bind(&b)
    .execute(rig.store.raw_pool())
    .await
    .unwrap();
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.published, 1);
    assert_eq!(report.evicted_blobs, 1);
    assert!(!file.exists());
    assert!(blob_row(&rig, &a).await.evicted_at.is_some());
}

// Should: treat adoption (mesh already held the photo; zero uploads) as
// decided and evict its spool bytes the same pass.
#[tokio::test(flavor = "multi_thread")]
async fn adoption_evicts_spool_bytes() {
    let rig = rig().await;
    let id = materialize(&rig, "a", b"a-bytes").await;
    let file = blob_file(&rig, &id).await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve(Ok(outcome(
        Responsibility::Holder,
        vec![entry(
            "cloud-a",
            "aa".repeat(32).as_str(),
            Some("01912e5a-7b3c-7f21-a4d8-3e9f12ab34cd"),
        )],
    )));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.adopted, 1);
    assert_eq!(publisher.calls.load(Ordering::Relaxed), 0, "zero uploads");
    assert_eq!(report.evicted_blobs, 1);
    assert!(!file.exists());
}

// ---------------------------------------------------- tombstone propagation

/// Publish a photo, then tombstone it locally — the precondition for a
/// pending delete.
async fn publish_then_tombstone(
    rig: &Rig,
    publisher: &FakePublisher,
    state: &mut PublishState,
    local_id: &str,
) -> PhotoId {
    let id = materialize(rig, local_id, local_id.as_bytes()).await;
    pass(rig, publisher, state).await;
    sqlx::query("UPDATE photos SET deleted_at = ? WHERE photo_id = ?")
        .bind(Utc::now())
        .bind(&id)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
    id
}

// Impact: the mesh keeps serving a deleted photo until this fires, and the
// only record that it still needs telling is the marker this stamps.
// Should: submit photo_delete for a published photo tombstoned locally.
// Should: converge — a second pass finds nothing left to say.
#[tokio::test(flavor = "multi_thread")]
async fn tombstone_propagates_once_and_converges() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = publish_then_tombstone(&rig, &publisher, &mut state, "gone").await;

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.tombstones_propagated, 1);
    assert_eq!(
        publisher.propagated(),
        vec![(id.to_string(), TombstoneOp::Delete)]
    );
    assert!(photo(&rig, &id).await.tombstone_published_at.is_some());

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.tombstones_propagated, 0);
    assert_eq!(publisher.propagated().len(), 1, "no re-submission");
}

// Impact: the resettable marker is the only thing that makes a delete →
// restore → delete cycle converge; a set-once marker would strand the
// second delete forever.
// Should: submit photo_restore when the asset comes back from Recently
// Deleted, then photo_delete again when it is deleted a second time.
#[tokio::test(flavor = "multi_thread")]
async fn restore_propagates_and_re_arms_the_next_delete() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = publish_then_tombstone(&rig, &publisher, &mut state, "cycle").await;
    pass(&rig, &publisher, &mut state).await;

    sqlx::query("UPDATE photos SET deleted_at = NULL WHERE photo_id = ?")
        .bind(&id)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.restores_propagated, 1);
    assert!(photo(&rig, &id).await.tombstone_published_at.is_none());

    sqlx::query("UPDATE photos SET deleted_at = ? WHERE photo_id = ?")
        .bind(Utc::now())
        .bind(&id)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.tombstones_propagated, 1);

    assert_eq!(
        publisher.propagated(),
        vec![
            (id.to_string(), TombstoneOp::Delete),
            (id.to_string(), TombstoneOp::Restore),
            (id.to_string(), TombstoneOp::Delete),
        ]
    );
}

// Impact: an adopted photo lives in consensus under whichever device
// published it first — deleting under the local id would silently no-op
// against a photo that does not exist there.
// Should: propagate under consensus_photo_id when the photo was adopted.
#[tokio::test(flavor = "multi_thread")]
async fn adopted_photo_propagates_under_the_consensus_id() {
    const REMOTE: &str = "01912e5a-7b3c-7f21-a4d8-3e9f12ab34cd";
    let rig = rig().await;
    let id = materialize(&rig, "adopted", b"adopted-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve(Ok(outcome(
        Responsibility::Holder,
        vec![entry("cloud-adopted", "bb".repeat(32).as_str(), Some(REMOTE))],
    )));
    let mut state = PublishState::default();
    pass(&rig, &publisher, &mut state).await;
    assert_eq!(photo(&rig, &id).await.consensus_photo_id.as_deref(), Some(REMOTE));

    sqlx::query("UPDATE photos SET deleted_at = ? WHERE photo_id = ?")
        .bind(Utc::now())
        .bind(&id)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
    pass(&rig, &publisher, &mut state).await;

    assert_eq!(
        publisher.propagated(),
        vec![(REMOTE.to_string(), TombstoneOp::Delete)]
    );
}

// Impact: the node's device-tx gate rejects any transaction touching a
// scope this device does not hold, so propagation must park on the same
// standing as publishing rather than burn attempts discovering 403s.
// Should: hold the tombstone and consume no attempts when another device
// holds responsibility.
// Should not: submit anything to the publisher.
#[tokio::test(flavor = "multi_thread")]
async fn propagation_parks_when_another_device_is_responsible() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = publish_then_tombstone(&rig, &publisher, &mut state, "parked").await;

    publisher.set_resolve(Ok(outcome(Responsibility::Other, Vec::new())));
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.tombstones_propagated, 0);
    assert!(report.parked_responsibility);
    assert!(publisher.propagated().is_empty());
    let row = photo(&rig, &id).await;
    assert_eq!(row.tombstone_publish_attempts, 0, "no attempts burned");
    assert!(row.tombstone_published_at.is_none());
}

// Impact: a photo added and deleted between two passes must reach the mesh
// before it is tombstoned there — a delete of a photo consensus has never
// seen is a no-op, and the tombstone would be lost.
// Should: publish before propagating within one scope.
#[tokio::test(flavor = "multi_thread")]
async fn publish_precedes_propagation_within_a_scope() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let old = publish_then_tombstone(&rig, &publisher, &mut state, "old").await;
    let fresh = materialize(&rig, "fresh", b"fresh-bytes").await;

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.published, 1);
    assert_eq!(report.tombstones_propagated, 1);
    assert_eq!(publisher.seen.lock().unwrap().last(), Some(&fresh));
    assert_eq!(
        publisher.propagated(),
        vec![(old.to_string(), TombstoneOp::Delete)]
    );
}

// Should: back off with the tombstone ledger, leaving the publish ledger
// untouched, when propagation fails transiently.
// Should not: stamp the marker.
#[tokio::test(flavor = "multi_thread")]
async fn propagation_failure_uses_its_own_ledger() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = publish_then_tombstone(&rig, &publisher, &mut state, "flaky").await;

    publisher.script_propagate(vec![Err(PublishError::Transient("nope".into()))]);
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.failed, 1);
    assert_eq!(report.tombstones_propagated, 0);
    let row = photo(&rig, &id).await;
    assert_eq!(row.tombstone_publish_attempts, 1);
    assert_eq!(row.tombstone_publish_last_error.as_deref(), Some("nope"));
    assert_eq!(row.publish_attempts, 0, "publish ledger untouched");
    assert!(row.publish_last_error.is_none());
    assert!(row.tombstone_published_at.is_none());
}

// Impact: the shared half of the iCloud cutover depends on this path, and
// the node's device-tx gate resolves a shared photo's scope from its
// committed row — so a delete in a library this device does not hold is
// rejected wholesale. The pass must partition tombstones by scope for the
// same reason it partitions publishes.
// Should: propagate personal and shared tombstones under their own scopes.
// Should: park only the scope whose responsibility is held elsewhere,
// leaving the other's tombstone told and its attempts unburned.
#[tokio::test(flavor = "multi_thread")]
async fn propagation_partitions_by_scope() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();

    let personal = materialize(&rig, "p", b"p-bytes").await;
    let shared = materialize_shared(&rig, "s", b"s-bytes").await;
    pass(&rig, &publisher, &mut state).await;

    sqlx::query("UPDATE photos SET deleted_at = ?")
        .bind(Utc::now())
        .execute(rig.store.raw_pool())
        .await
        .unwrap();

    // Both scopes held: both tombstones travel, each under its own scope.
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.tombstones_propagated, 2);
    let mut told: Vec<String> = publisher.propagated().into_iter().map(|(id, _)| id).collect();
    told.sort();
    let mut expected = vec![personal.to_string(), shared.to_string()];
    expected.sort();
    assert_eq!(told, expected);

    // Re-arm both, then withhold responsibility for the shared scope only.
    for id in [&personal, &shared] {
        sqlx::query("UPDATE photos SET tombstone_published_at = NULL WHERE photo_id = ?")
            .bind(id)
            .execute(rig.store.raw_pool())
            .await
            .unwrap();
    }
    publisher.propagate_seen.lock().unwrap().clear();
    publisher.set_resolve_for(Some(MESH_LIB), Ok(outcome(Responsibility::Other, Vec::new())));

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.tombstones_propagated, 1, "personal scope still moves");
    assert!(report.parked_responsibility);
    assert_eq!(
        publisher.propagated(),
        vec![(personal.to_string(), TombstoneOp::Delete)]
    );
    let shared_row = photo(&rig, &shared).await;
    assert!(shared_row.tombstone_published_at.is_none());
    assert_eq!(
        shared_row.tombstone_publish_attempts, 0,
        "a parked scope burns no attempts"
    );
}

// Should: park the whole pass without burning attempts when the node goes
// unreachable mid-propagation.
#[tokio::test(flavor = "multi_thread")]
async fn propagation_unreachable_parks_without_burning_attempts() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = publish_then_tombstone(&rig, &publisher, &mut state, "offline").await;

    publisher.script_propagate(vec![Err(PublishError::NodeUnreachable("down".into()))]);
    let report = pass(&rig, &publisher, &mut state).await;

    assert!(report.parked);
    assert_eq!(report.tombstones_propagated, 0);
    let row = photo(&rig, &id).await;
    assert_eq!(row.tombstone_publish_attempts, 0);
    assert!(row.tombstone_published_at.is_none());
}

// Should: jump attempts straight to the cap on a permanent rejection, so a
// malformed tombstone gives up instead of retrying forever.
#[tokio::test(flavor = "multi_thread")]
async fn propagation_rejection_gives_up_immediately() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = publish_then_tombstone(&rig, &publisher, &mut state, "bad").await;

    publisher.script_propagate(vec![Err(PublishError::Rejected("malformed".into()))]);
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.gave_up, 1);
    assert_eq!(report.tombstones_propagated, 0);
    let row = photo(&rig, &id).await;
    assert_eq!(row.tombstone_publish_attempts, rig.config.publish.retry_cap);
    assert!(row.tombstone_published_at.is_none());
}

// --------------------------------------------------------- edit propagation

/// Simulate what the materialize tick leaves behind after PhotoKit reports a
/// re-edit: fresh bytes in the spool and a new hash on the row, with
/// `published_content_hash` still naming what the mesh holds.
async fn re_edit(rig: &Rig, id: &PhotoId, resource_type: i64, bytes: &[u8]) -> ContentHash {
    let hash = ContentHash::of_bytes(bytes);
    let path = rig.data_dir.spool().blob_path(&hash, "jpg");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    sqlx::query(
        "UPDATE photo_resources SET content_hash = ?, ext = 'jpg', size_bytes = ?, \
         written_at = ? WHERE photo_id = ? AND resource_type = ?",
    )
    .bind(&hash)
    .bind(bytes.len() as i64)
    .bind(Utc::now())
    .bind(id)
    .bind(resource_type)
    .execute(rig.store.raw_pool())
    .await
    .unwrap();
    hash
}

/// Bump the modification date PhotoKit reports, which is the metadata half
/// of the queue.
async fn touch_metadata(rig: &Rig, id: &PhotoId) {
    sqlx::query("UPDATE photos SET asset_modified_at = ? WHERE photo_id = ?")
        .bind(Utc::now())
        .bind(id)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
}

async fn resource_marker(rig: &Rig, id: &PhotoId, resource_type: i64) -> Option<ContentHash> {
    sqlx::query_scalar(
        "SELECT published_content_hash FROM photo_resources \
         WHERE photo_id = ? AND resource_type = ?",
    )
    .bind(id)
    .bind(resource_type)
    .fetch_one(rig.store.raw_pool())
    .await
    .unwrap()
}

// Impact: a freshly published photo and one whose bytes the mesh has never
// seen would be indistinguishable if publish left the markers NULL — every
// pass would re-upload the entire archive.
// Should: leave nothing to say immediately after a publish.
#[tokio::test(flavor = "multi_thread")]
async fn publish_stamps_the_edit_baseline() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = materialize(&rig, "fresh", b"fresh-bytes").await;
    // A real asset carries a modification date; the marker has to capture
    // whatever it was at publish time, not merely be non-NULL.
    touch_metadata(&rig, &id).await;

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.published, 1);
    assert_eq!(report.edits_propagated, 0);
    let row = photo(&rig, &id).await;
    assert!(row.asset_modified_at.is_some());
    assert_eq!(row.published_asset_modified_at, row.asset_modified_at);
    assert!(resource_marker(&rig, &id, 0).await.is_some());

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.edits_propagated, 0, "a published photo is converged");
    assert!(publisher.edits().is_empty());
}

// Impact: adoption uploads nothing — the bytes came from another device. A
// missing baseline here would make this daemon "correct" the mesh by
// re-uploading a resource set it was never asked for.
// Should not: queue an edit for a photo adopted by fingerprint.
#[tokio::test(flavor = "multi_thread")]
async fn adoption_stamps_the_edit_baseline() {
    const REMOTE: &str = "01912e5a-7b3c-7f21-a4d8-3e9f12ab34cd";
    let rig = rig().await;
    let id = materialize(&rig, "adopted", b"adopted-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve(Ok(outcome(
        Responsibility::Holder,
        vec![entry("cloud-adopted", "bb".repeat(32).as_str(), Some(REMOTE))],
    )));
    let mut state = PublishState::default();
    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.adopted, 1);

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.edits_propagated, 0);
    assert!(publisher.edits().is_empty(), "nothing to correct");
    assert!(resource_marker(&rig, &id, 0).await.is_some());
}

// Impact: without this the mesh serves the pre-edit render forever —
// published_at is set-once and nothing else re-enqueues the photo.
// Should: submit the changed resource with its new bytes, then converge.
#[tokio::test(flavor = "multi_thread")]
async fn re_edit_propagates_once_and_converges() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = materialize(&rig, "edited", b"v1-bytes").await;
    pass(&rig, &publisher, &mut state).await;

    let new_hash = re_edit(&rig, &id, 0, b"v2-bytes-are-longer").await;
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.edits_propagated, 1);
    assert_eq!(report.metadata_propagated, 0);
    let edits = publisher.edits();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].resources.len(), 1);
    assert_eq!(edits[0].resources[0].content_hash, new_hash);
    assert!(edits[0].removals.is_empty());
    assert!(!edits[0].metadata_changed, "only the bytes moved");
    assert_eq!(resource_marker(&rig, &id, 0).await.as_ref(), Some(&new_hash));

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.edits_propagated, 0);
    assert_eq!(publisher.edits().len(), 1, "no re-submission");
}

// Impact: after publish the photo's blobs are evicted, so a metadata-only
// refresh has no bytes to send — it must reach the mesh on its own.
// Should: route a bare modification-date bump to the metadata counter.
// Should not: carry any resource with it.
#[tokio::test(flavor = "multi_thread")]
async fn metadata_refresh_propagates_without_resources() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = materialize(&rig, "meta", b"meta-bytes").await;
    pass(&rig, &publisher, &mut state).await;

    touch_metadata(&rig, &id).await;
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.metadata_propagated, 1);
    assert_eq!(report.edits_propagated, 0);
    let edits = publisher.edits();
    assert_eq!(edits.len(), 1);
    assert!(edits[0].resources.is_empty());
    assert!(edits[0].metadata_changed);

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.metadata_propagated, 0, "converged");
}

// Impact: a crop changes the pixels AND the dimensions that describe them;
// splitting them across two transactions would leave a window where the
// mesh serves one with the other's metadata.
// Should: carry both in a single content edit.
#[tokio::test(flavor = "multi_thread")]
async fn a_crop_carries_its_metadata_with_the_bytes() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = materialize(&rig, "crop", b"crop-v1").await;
    pass(&rig, &publisher, &mut state).await;

    re_edit(&rig, &id, 0, b"crop-v2-different").await;
    touch_metadata(&rig, &id).await;
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.edits_propagated, 1);
    assert_eq!(report.metadata_propagated, 0, "one transaction, not two");
    let edits = publisher.edits();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].resources.len(), 1);
    assert!(edits[0].metadata_changed);
    assert!(
        photo(&rig, &id).await.published_asset_modified_at
            == photo(&rig, &id).await.asset_modified_at
    );
}

// Impact: a revert deletes the local row, and a hard delete would take the
// marker with it — the divergence would be an absence no predicate can see.
// Should: keep the row as a removal marker, send the removal, then reap it.
#[tokio::test(flavor = "multi_thread")]
async fn revert_propagates_as_a_removal_and_reaps_the_marker_row() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = materialize(&rig, "revert", b"revert-bytes").await;
    pass(&rig, &publisher, &mut state).await;

    // Stand in for classify's soft-remove of a published resource.
    sqlx::query(
        "UPDATE photo_resources SET removed_at = ?, content_hash = NULL, ext = NULL, \
         size_bytes = NULL WHERE photo_id = ? AND resource_type = 0",
    )
    .bind(Utc::now())
    .bind(&id)
    .execute(rig.store.raw_pool())
    .await
    .unwrap();

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.edits_propagated, 1);
    let edits = publisher.edits();
    assert_eq!(edits.len(), 1);
    assert!(edits[0].resources.is_empty(), "a revert uploads nothing");
    assert_eq!(edits[0].removals.len(), 1);

    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM photo_resources WHERE photo_id = ? AND resource_type = 0",
    )
    .bind(&id)
    .fetch_one(rig.store.raw_pool())
    .await
    .unwrap();
    assert_eq!(rows, 0, "the marker row has no job left");
}

// Impact: an adopted photo lives in consensus under another device's id;
// editing under the local id would target a photo that does not exist there.
// Should: submit the edit under consensus_photo_id.
#[tokio::test(flavor = "multi_thread")]
async fn adopted_photo_edits_under_the_consensus_id() {
    const REMOTE: &str = "01912e5a-7b3c-7f21-a4d8-3e9f12ab34cd";
    let rig = rig().await;
    let id = materialize(&rig, "adopted", b"adopted-bytes").await;

    let publisher = FakePublisher::ok();
    publisher.set_resolve(Ok(outcome(
        Responsibility::Holder,
        vec![entry("cloud-adopted", "bb".repeat(32).as_str(), Some(REMOTE))],
    )));
    let mut state = PublishState::default();
    pass(&rig, &publisher, &mut state).await;

    re_edit(&rig, &id, 0, b"adopted-then-edited").await;
    pass(&rig, &publisher, &mut state).await;

    let edits = publisher.edits();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].consensus_photo_id, REMOTE);
}

// Impact: the node's device-tx gate rejects any transaction touching a scope
// this device does not hold, so edits must park on the same standing as
// publishes rather than burn attempts discovering 403s.
// Should: hold the edit and consume no attempts when another device holds
// responsibility.
#[tokio::test(flavor = "multi_thread")]
async fn edit_parks_when_another_device_is_responsible() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = materialize(&rig, "parked", b"parked-v1").await;
    pass(&rig, &publisher, &mut state).await;

    let old_marker = resource_marker(&rig, &id, 0).await;
    re_edit(&rig, &id, 0, b"parked-v2-longer").await;
    publisher.set_resolve(Ok(outcome(Responsibility::Other, Vec::new())));
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.edits_propagated, 0);
    assert!(report.parked_responsibility);
    assert!(publisher.edits().is_empty());
    assert_eq!(photo(&rig, &id).await.edit_publish_attempts, 0);
    assert_eq!(resource_marker(&rig, &id, 0).await, old_marker);
}

// Should: back off with the edit ledger, leaving the publish and tombstone
// ledgers untouched, when an edit fails transiently.
// Should not: stamp the marker.
#[tokio::test(flavor = "multi_thread")]
async fn edit_failure_uses_its_own_ledger() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = materialize(&rig, "flaky", b"flaky-v1").await;
    pass(&rig, &publisher, &mut state).await;

    let old_marker = resource_marker(&rig, &id, 0).await;
    re_edit(&rig, &id, 0, b"flaky-v2-longer").await;
    publisher.script_edit(vec![Err(PublishError::Transient("nope".into()))]);
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.failed, 1);
    assert_eq!(report.edits_propagated, 0);
    let row = photo(&rig, &id).await;
    assert_eq!(row.edit_publish_attempts, 1);
    assert_eq!(row.edit_publish_last_error.as_deref(), Some("nope"));
    assert_eq!(row.publish_attempts, 0, "publish ledger untouched");
    assert_eq!(row.tombstone_publish_attempts, 0);
    assert_eq!(resource_marker(&rig, &id, 0).await, old_marker);
}

// Impact: a rejected edit is permanent — the same bytes will be refused
// again, and spinning would keep the blobs pinned out of eviction forever.
// Should: jump attempts to the cap on a rejection.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_edit_burns_attempts_to_the_cap() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = materialize(&rig, "bad", b"bad-v1").await;
    pass(&rig, &publisher, &mut state).await;

    re_edit(&rig, &id, 0, b"bad-v2-longer").await;
    publisher.script_edit(vec![Err(PublishError::Rejected("nope".into()))]);
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.gave_up, 1);
    assert_eq!(report.edits_propagated, 0);
    assert_eq!(
        photo(&rig, &id).await.edit_publish_attempts,
        rig.config.publish.retry_cap
    );
}

// Impact: both edit handlers reject a photo the mesh still believes is
// tombstoned, so a restore and an edit in one pass converge only if the
// restore goes first.
// Should: submit photo_restore before the edit.
#[tokio::test(flavor = "multi_thread")]
async fn restore_precedes_edit_within_a_scope() {
    let rig = rig().await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();
    let id = publish_then_tombstone(&rig, &publisher, &mut state, "back").await;
    pass(&rig, &publisher, &mut state).await;

    sqlx::query("UPDATE photos SET deleted_at = NULL WHERE photo_id = ?")
        .bind(&id)
        .execute(rig.store.raw_pool())
        .await
        .unwrap();
    re_edit(&rig, &id, 0, b"back-and-edited").await;

    let report = pass(&rig, &publisher, &mut state).await;
    assert_eq!(report.restores_propagated, 1);
    assert_eq!(report.edits_propagated, 1);
    assert_eq!(
        publisher.propagated().last(),
        Some(&(id.to_string(), TombstoneOp::Restore)),
        "the restore lands before the edit is attempted"
    );
    assert_eq!(publisher.edits().len(), 1);
}

// Impact: the shared half of the cutover depends on this — the node resolves
// a shared photo's scope from its committed row, so an edit in a library
// this device does not hold is rejected wholesale.
// Should: park only the scope held elsewhere, leaving the other's edit told.
#[tokio::test(flavor = "multi_thread")]
async fn edits_partition_by_scope() {
    let rig = rig().await;
    add_shared_library(&rig, Some(MESH_LIB)).await;
    let publisher = FakePublisher::ok();
    let mut state = PublishState::default();

    let personal = materialize(&rig, "p", b"p-bytes").await;
    let shared = materialize_shared(&rig, "s", b"s-bytes").await;
    pass(&rig, &publisher, &mut state).await;

    re_edit(&rig, &personal, 0, b"p-bytes-v2-longer").await;
    let shared_marker = resource_marker(&rig, &shared, 0).await;
    re_edit(&rig, &shared, 0, b"s-bytes-v2-longer").await;

    publisher.set_resolve_for(None, Ok(outcome(Responsibility::Holder, Vec::new())));
    publisher.set_resolve_for(Some(MESH_LIB), Ok(outcome(Responsibility::Other, Vec::new())));
    let report = pass(&rig, &publisher, &mut state).await;

    assert_eq!(report.edits_propagated, 1, "personal only");
    assert!(report.parked_responsibility);
    let edits = publisher.edits();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].photo.photo_id, personal);
    assert_eq!(photo(&rig, &shared).await.edit_publish_attempts, 0);
    assert_eq!(resource_marker(&rig, &shared, 0).await, shared_marker);
}
