//! Lifecycle scenarios (spec §Hard-delete cleanup): retention hard-deletes,
//! log pruning, and the spool-eviction sweep.

use chrono::{Duration, Utc};
use ingress_core::classify::{apply_change, apply_removal};
use ingress_core::cleanup::{CleanupConfig, run_cleanup};
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig};
use ingress_core::paths::DataDir;
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::{AssetDescriptor, ContentHash, LibraryId, LibraryScope, PhotoId, StateStore};

/// Personal + shared libraries over a file-backed store, plus the data dir
/// whose spool holds every blob.
async fn rig(tmp: &std::path::Path) -> (StateStore, DataDir, LibraryId, LibraryId) {
    let store = StateStore::open(&tmp.join("state.db")).await.unwrap();
    let personal = LibraryId::new("personal");
    let shared = LibraryId::new("shared_household");
    for (id, name, binding) in [
        (&personal, "Personal", None),
        (
            &shared,
            "Shared",
            Some(ICLOUD_SHARED_LIBRARY_BINDING.to_string()),
        ),
    ] {
        store
            .insert_library(&LibraryConfig {
                library_id: id.clone(),
                display_name: name.into(),
                scope_binding: binding,
                retention_days: 30,
                created_at: Utc::now(),
                mesh_library_id: None,
            })
            .await
            .unwrap();
    }
    let data_dir = DataDir::new(tmp.join("data"));
    std::fs::create_dir_all(data_dir.root()).unwrap();
    (store, data_dir, personal, shared)
}

async fn seed_one(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
    match seed_descriptor(store, desc).await.expect("seed") {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

/// Materialize every pending resource with real spool bytes + persist the
/// publish-metadata capsule.
async fn materialize_all(
    store: &StateStore,
    data_dir: &DataDir,
    desc: &AssetDescriptor,
    photo_id: &PhotoId,
) {
    let spool = data_dir.spool();
    let size = desc.resources[0].expected_size.unwrap() as i64;
    for row in store.resources_for_photo(photo_id).await.unwrap() {
        if row.written_at.is_some() {
            continue;
        }
        let bytes = format!("{photo_id}-{:?}", row.resource_type);
        let hash = ContentHash::of_bytes(bytes.as_bytes());
        let path = spool.blob_path(&hash, "bin");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        store
            .mark_resource_written(photo_id, row.resource_type, &hash, "bin", size)
            .await
            .unwrap();
    }
    store.persist_descriptor(photo_id, desc).await.unwrap();
}

// Impact: post-crash refcounts gate the daemon's irreversible file deletes
// (hard delete, re-edit supersede, hard move) — unrepaired drift deletes
// live bytes or strands dead ones.
// Should: fix all three drift classes (count mismatch, orphan row, missing
// row) in one run and log ONE refcount_repaired event.
// Should not: touch any blob file, or log when clean.
#[tokio::test]
async fn refcount_repair_covers_all_three_drift_classes() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, lib, _) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::live_photo()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;
    let rows = store.resources_for_photo(&id).await.unwrap();
    let (h1, h2) = (
        rows[0].content_hash.clone().unwrap(),
        rows[1].content_hash.clone().unwrap(),
    );
    let blob_file = data_dir.spool().blob_path(&h1, "bin");
    assert!(blob_file.is_file());

    // Drift class 1: count mismatch. Class 2: orphan row. Class 3: missing row.
    sqlx::query("UPDATE blobs SET ref_count = 7 WHERE content_hash = ?")
        .bind(h1.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO blobs (library_id, content_hash, ext, size_bytes, ref_count, written_at) \
         VALUES (?, 'feedfeed', 'bin', 1, 3, ?)",
    )
    .bind(lib.to_string())
    .bind(Utc::now())
    .execute(store.raw_pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM blobs WHERE content_hash = ?")
        .bind(h2.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();

    let report = ingress_core::recovery::repair_refcounts(&store)
        .await
        .unwrap();
    assert_eq!(report.updated, 1);
    assert_eq!(report.deleted, 1);
    assert_eq!(report.inserted, 1);
    assert_eq!(store.blob(&lib, &h1).await.unwrap().unwrap().ref_count, 1);
    assert_eq!(store.blob(&lib, &h2).await.unwrap().unwrap().ref_count, 1);
    assert!(blob_file.is_file(), "startup repair never deletes files");
    assert_eq!(
        store.log_events("refcount_repaired").await.unwrap().len(),
        1
    );

    // Clean run: silent no-op.
    let clean = ingress_core::recovery::repair_refcounts(&store)
        .await
        .unwrap();
    assert_eq!(clean.drift(), 0);
    assert_eq!(
        store.log_events("refcount_repaired").await.unwrap().len(),
        1
    );
}

/// Backdate a tombstone so it reads as expired.
async fn backdate_tombstone(store: &StateStore, id: &PhotoId, days: i64) {
    sqlx::query("UPDATE photos SET deleted_at = ? WHERE photo_id = ?")
        .bind(Utc::now() - Duration::days(days))
        .bind(id.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();
}

async fn set_retention(store: &StateStore, lib: &LibraryId, days: i64) {
    sqlx::query("UPDATE libraries SET retention_days = ? WHERE library_id = ?")
        .bind(days)
        .bind(lib.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();
}

fn cfg() -> CleanupConfig {
    CleanupConfig::default()
}

// Impact: this is the daemon's only irreversible byte destruction — every
// artifact (rows, spool files) must go, and the black-box hard_delete row
// must commit atomically with the row deletions.
// Should: remove everything and log hard_delete with the reaped hashes.
#[tokio::test]
async fn hard_delete_past_retention_removes_everything() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, lib, _) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::live_photo()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    apply_removal(&store, &desc.local_id).await.unwrap();
    backdate_tombstone(&store, &id, 31).await;

    let hashes: Vec<ContentHash> = store
        .resources_for_photo(&id)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|r| r.content_hash)
        .collect();

    let report = run_cleanup(&store, &data_dir, &cfg(), Utc::now())
        .await
        .unwrap();
    assert_eq!(report.photos_hard_deleted, 1);
    assert_eq!(report.blob_files_deleted, 2);

    assert!(store.photo(&id).await.unwrap().is_none());
    assert!(store.resources_for_photo(&id).await.unwrap().is_empty());
    for h in &hashes {
        assert!(
            store.blob(&lib, h).await.unwrap().is_none(),
            "blob row reaped"
        );
        assert!(
            !data_dir.spool().blob_path(h, "bin").is_file(),
            "spool file deleted"
        );
    }

    let events = store.log_events("hard_delete").await.unwrap();
    assert_eq!(events.len(), 1);
    let detail: serde_json::Value =
        serde_json::from_str(events[0].detail.as_ref().unwrap()).unwrap();
    assert_eq!(detail["reaped"].as_array().unwrap().len(), 2);
}

// Should: retention 0 reaps while retention 1000 survives, in the same run
// (per-library retention_days read fresh — spec edge-case table).
#[tokio::test]
async fn hard_delete_respects_per_library_retention() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, personal, shared) = rig(tmp.path()).await;

    let p_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let p_id = seed_one(&store, &p_desc).await;
    materialize_all(&store, &data_dir, &p_desc, &p_id).await;
    let s_desc = AssetDescriptorBuilder::simple_image()
        .scope(LibraryScope::Shared)
        .modified_at(Utc::now())
        .build();
    let s_id = seed_one(&store, &s_desc).await;
    materialize_all(&store, &data_dir, &s_desc, &s_id).await;

    apply_removal(&store, &p_desc.local_id).await.unwrap();
    apply_removal(&store, &s_desc.local_id).await.unwrap();
    backdate_tombstone(&store, &p_id, 2).await;
    backdate_tombstone(&store, &s_id, 2).await;
    set_retention(&store, &personal, 0).await;
    set_retention(&store, &shared, 1000).await;

    let report = run_cleanup(&store, &data_dir, &cfg(), Utc::now())
        .await
        .unwrap();
    assert_eq!(report.photos_hard_deleted, 1);
    assert!(
        store.photo(&p_id).await.unwrap().is_none(),
        "retention-0 photo reaped"
    );
    assert!(
        store.photo(&s_id).await.unwrap().is_some(),
        "retention-1000 photo survives"
    );
}

// Impact: spec edge-case row 1 — a blob shared with an active photo must
// survive its co-referent's hard delete.
// Should: refcount decrements to 1, file stays.
#[tokio::test]
async fn hard_delete_preserves_shared_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, lib, _) = rig(tmp.path()).await;

    // Two photos sharing identical original bytes (dedup at write time).
    let keep_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let keep = seed_one(&store, &keep_desc).await;
    let gone_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let gone = seed_one(&store, &gone_desc).await;
    let bytes = b"shared-bytes";
    let hash = ContentHash::of_bytes(bytes);
    let path = data_dir.spool().blob_path(&hash, "bin");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    for id in [&keep, &gone] {
        store
            .mark_resource_written(id, ingress_core::ResourceType::Original, &hash, "bin", 12)
            .await
            .unwrap();
    }
    assert_eq!(store.blob(&lib, &hash).await.unwrap().unwrap().ref_count, 2);

    apply_removal(&store, &gone_desc.local_id).await.unwrap();
    backdate_tombstone(&store, &gone, 31).await;
    let report = run_cleanup(&store, &data_dir, &cfg(), Utc::now())
        .await
        .unwrap();
    assert_eq!(report.photos_hard_deleted, 1);
    assert_eq!(report.blob_files_deleted, 0, "shared blob file preserved");
    assert_eq!(store.blob(&lib, &hash).await.unwrap().unwrap().ref_count, 1);
    assert!(path.is_file());
}

// Should: a tombstone with only hash-less pending rows deletes rows without
// touching blobs; a superseded-pending row's retained hash IS decremented.
#[tokio::test]
async fn hard_delete_pending_and_superseded_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, lib, _) = rig(tmp.path()).await;

    // Pending-only photo (seeded, never fetched).
    let pending_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let pending = seed_one(&store, &pending_desc).await;
    apply_removal(&store, &pending_desc.local_id).await.unwrap();
    backdate_tombstone(&store, &pending, 31).await;

    // Superseded-pending photo: materialized then reopened (hash retained).
    let sp_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let sp = seed_one(&store, &sp_desc).await;
    materialize_all(&store, &data_dir, &sp_desc, &sp).await;
    let hash = store.resources_for_photo(&sp).await.unwrap()[0]
        .content_hash
        .clone()
        .unwrap();
    sqlx::query("UPDATE photo_resources SET written_at = NULL WHERE photo_id = ?")
        .bind(sp.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();
    apply_removal(&store, &sp_desc.local_id).await.unwrap();
    backdate_tombstone(&store, &sp, 31).await;

    let report = run_cleanup(&store, &data_dir, &cfg(), Utc::now())
        .await
        .unwrap();
    assert_eq!(report.photos_hard_deleted, 2);
    assert!(
        store.blob(&lib, &hash).await.unwrap().is_none(),
        "superseded hash decremented"
    );
    assert!(store.photo(&pending).await.unwrap().is_none());
}

// Should: second run reports zero; batch=1 processes one per run.
#[tokio::test]
async fn hard_delete_is_idempotent_and_batch_capped() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, ..) = rig(tmp.path()).await;
    for _ in 0..2 {
        let desc = AssetDescriptorBuilder::simple_image()
            .modified_at(Utc::now())
            .build();
        let id = seed_one(&store, &desc).await;
        materialize_all(&store, &data_dir, &desc, &id).await;
        apply_removal(&store, &desc.local_id).await.unwrap();
        backdate_tombstone(&store, &id, 31).await;
    }

    let capped = CleanupConfig {
        hard_delete_batch: 1,
        ..cfg()
    };
    let r1 = run_cleanup(&store, &data_dir, &capped, Utc::now())
        .await
        .unwrap();
    assert_eq!(r1.photos_hard_deleted, 1, "batch cap respected");
    let r2 = run_cleanup(&store, &data_dir, &capped, Utc::now())
        .await
        .unwrap();
    assert_eq!(r2.photos_hard_deleted, 1);
    let r3 = run_cleanup(&store, &data_dir, &capped, Utc::now())
        .await
        .unwrap();
    assert_eq!(r3.photos_hard_deleted, 0, "idempotent once drained");
}

// Should: an unmapped (NULL-library) tombstone reaps after the 30-day
// default — rows only, nothing else exists for it.
#[tokio::test]
async fn unmapped_tombstone_hard_deletes_after_default_window() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, ..) = rig(tmp.path()).await;

    let id = PhotoId::mint();
    sqlx::query(
        "INSERT INTO photos (photo_id, library_id, local_id, discovered_at, deleted_at) \
         VALUES (?, NULL, 'UNMAPPED/L0/001', ?, ?)",
    )
    .bind(id.to_string())
    .bind(Utc::now() - Duration::days(60))
    .bind(Utc::now() - Duration::days(31))
    .execute(store.raw_pool())
    .await
    .unwrap();

    let report = run_cleanup(&store, &data_dir, &cfg(), Utc::now())
        .await
        .unwrap();
    assert_eq!(report.photos_hard_deleted, 1);
    assert!(store.photo(&id).await.unwrap().is_none());
}

// Impact: the log is the black box for a daemon that deletes irreplaceable
// data — pruning must never eat recent rows.
// Should: >180-day rows deleted, younger retained, count reported.
#[tokio::test]
async fn log_prune_removes_only_expired_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, ..) = rig(tmp.path()).await;

    store.append_log("scan_started", None, None).await.unwrap();
    sqlx::query("INSERT INTO ingest_log (at, event_type) VALUES (?, 'ancient_event')")
        .bind(Utc::now() - Duration::days(200))
        .execute(store.raw_pool())
        .await
        .unwrap();

    let report = run_cleanup(&store, &data_dir, &cfg(), Utc::now())
        .await
        .unwrap();
    assert_eq!(report.log_rows_pruned, 1);
    assert!(store.log_events("ancient_event").await.unwrap().is_empty());
    assert_eq!(store.log_events("scan_started").await.unwrap().len(), 1);
}

// Impact: the publish pass's own eviction can crash between mark_published
// and the eviction step — the cleanup tick is the sweep that guarantees
// decided bytes still leave local disk.
// Should: evict a published photo's blob on the cleanup tick (file gone,
// eviction stamped, refcount intact).
// Should not: touch an unpublished photo's blob.
#[tokio::test]
async fn cleanup_sweep_evicts_decided_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, lib, _) = rig(tmp.path()).await;

    let published_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let published = seed_one(&store, &published_desc).await;
    materialize_all(&store, &data_dir, &published_desc, &published).await;
    let pending_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let pending = seed_one(&store, &pending_desc).await;
    materialize_all(&store, &data_dir, &pending_desc, &pending).await;

    sqlx::query("UPDATE photos SET published_at = ? WHERE photo_id = ?")
        .bind(Utc::now())
        .bind(published.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();

    let hash_of = |rows: &[ingress_core::model::ResourceRecord]| {
        rows[0].content_hash.clone().unwrap()
    };
    let published_hash = hash_of(&store.resources_for_photo(&published).await.unwrap());
    let pending_hash = hash_of(&store.resources_for_photo(&pending).await.unwrap());
    let published_file = data_dir.spool().blob_path(&published_hash, "bin");
    let pending_file = data_dir.spool().blob_path(&pending_hash, "bin");

    let report = run_cleanup(&store, &data_dir, &cfg(), Utc::now())
        .await
        .unwrap();
    assert_eq!(report.spool_evicted, 1);
    assert!(!published_file.exists(), "decided bytes swept");
    assert!(pending_file.is_file(), "undecided bytes retained");
    let blob = store.blob(&lib, &published_hash).await.unwrap().unwrap();
    assert!(blob.evicted_at.is_some());
    assert_eq!(blob.ref_count, 1);
}

// Impact: the crash-window class (materialized, capsule never persisted)
// leaks forever — publish skips it as missing-descriptor and the light-probe
// scan sees no metadata drift, so nothing re-triggers the write. The scan
// must self-heal it: re-probe as NeedsFull -> apply backfills the capsule
// from the live descriptor.
// Should: probe verdict flips Done -> NeedsFull when the capsule is NULL;
// apply_change (even a no-op delivery) backfills it.
// Should not: verdict Done for a materialized photo with no capsule.
#[tokio::test]
async fn scan_self_heals_missing_descriptor_capsule() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, ..) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    let probe_of = ingress_core::scan::ScanProbe {
        local_id: desc.local_id.clone(),
        cloud_id: desc.cloud_id.clone(),
        scope: desc.scope,
        asset_modified_at: desc.asset_modified_at,
    };

    // Baseline: an unchanged photo WITH its capsule verdicts Done.
    let scan = ingress_core::scan::begin(&store).await.unwrap();
    assert_eq!(
        ingress_core::scan::probe(&store, &scan, &probe_of)
            .await
            .unwrap(),
        ingress_core::scan::ScanVerdict::Done,
        "unchanged + capsule present = Done"
    );

    // Simulate the crash window: the completion committed but the capsule
    // never landed (or the row predates the capsule migration).
    sqlx::query("UPDATE photos SET descriptor_json = NULL WHERE photo_id = ?")
        .bind(id.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();

    // The ONLY signal now is the NULL capsule — the probe must escalate.
    let scan2 = ingress_core::scan::begin(&store).await.unwrap();
    assert_eq!(
        ingress_core::scan::probe(&store, &scan2, &probe_of)
            .await
            .unwrap(),
        ingress_core::scan::ScanVerdict::NeedsFull,
        "missing capsule forces a full re-probe"
    );

    // The re-delivered descriptor classifies NoOp (nothing else changed),
    // yet apply_change backfills the capsule.
    apply_change(&store, &data_dir.spool(), &desc).await.unwrap();
    let capsule: Option<String> =
        sqlx::query_scalar("SELECT descriptor_json FROM photos WHERE photo_id = ?")
            .bind(id.to_string())
            .fetch_one(store.raw_pool())
            .await
            .unwrap();
    assert!(capsule.is_some(), "capsule backfilled");
}

// Impact: a cleanup run concurrent with a live daemon would race photo_tasks
// and the loop's own ticks — the shared lock is the exclusivity story.
// Should: error while a live-pid lock exists; run + release otherwise.
#[tokio::test]
async fn standalone_cleanup_respects_drain_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir, ..) = rig(tmp.path()).await;

    // A live holder (our own pid).
    let lock_path = data_dir.root().join("drain.lock");
    std::fs::write(&lock_path, std::process::id().to_string()).unwrap();
    let err = ingress_core::cleanup::run_standalone(&store, &data_dir, &cfg(), Utc::now()).await;
    assert!(err.is_err(), "live-pid lock refuses standalone cleanup");

    std::fs::remove_file(&lock_path).unwrap();
    let cleanup = ingress_core::cleanup::run_standalone(&store, &data_dir, &cfg(), Utc::now())
        .await
        .unwrap();
    assert_eq!(cleanup.photos_hard_deleted, 0);
    assert!(!lock_path.exists(), "lock released after the run");
}
