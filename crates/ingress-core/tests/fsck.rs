//! Tier-2 fsck audit (spec §Recovery Tier 2): refcount drift, blob
//! existence, orphan scan.

use chrono::Utc;
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::fsck::{FsckOptions, run_fsck};
use ingress_core::model::LibraryConfig;
use ingress_core::paths::{DataDir, SpoolPaths};
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::{AssetDescriptor, ContentHash, LibraryId, PhotoId, StateStore};

/// File-backed store: personal library with a tempdir blob root.
async fn rig(tmp: &std::path::Path) -> (StateStore, LibraryId, DataDir) {
    let store = StateStore::open(&tmp.join("state.db")).await.unwrap();
    let personal = LibraryId::new("personal");
    store
        .insert_library(&LibraryConfig {
            library_id: personal.clone(),
            display_name: "Personal".into(),
            scope_binding: None,
            retention_days: 30,
            created_at: Utc::now(),
            mesh_library_id: None,
        })
        .await
        .unwrap();
    let data_dir = DataDir::new(tmp.join("data"));
    std::fs::create_dir_all(data_dir.root()).unwrap();
    (store, personal, data_dir)
}

async fn seed_one(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
    match seed_descriptor(store, desc).await.expect("seed") {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

async fn materialize_all(
    data_dir: &DataDir,
    store: &StateStore,
    desc: &AssetDescriptor,
    photo_id: &PhotoId,
) {
    let paths = data_dir.spool();
    for row in store.resources_for_photo(photo_id).await.unwrap() {
        if row.written_at.is_some() {
            continue;
        }
        let bytes = format!("{photo_id}-{:?}", row.resource_type);
        let hash = ContentHash::of_bytes(bytes.as_bytes());
        let path = paths.blob_path(&hash, "bin");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        store
            .mark_resource_written(
                photo_id,
                row.resource_type,
                &hash,
                "bin",
                bytes.len() as i64,
            )
            .await
            .unwrap();
    }
    store.persist_descriptor(photo_id, desc).await.unwrap();
}

async fn fsck(
    store: &StateStore,
    data_dir: &DataDir,
    repair: bool,
) -> ingress_core::fsck::FsckReport {
    run_fsck(store, data_dir, &FsckOptions { repair })
        .await
        .unwrap()
}

fn plant_orphan(spool: &SpoolPaths) -> std::path::PathBuf {
    let hash = ContentHash::of_bytes(b"orphan-bytes");
    let path = spool.blob_path(&hash, "bin");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"orphan-bytes").unwrap();
    path
}

// Impact: a false positive on a healthy archive erodes trust in the one
// tool that audits irreversible-delete bookkeeping; a stray write from the
// "read-only" default would be worse.
// Should: report clean on a fully-consistent store and log nothing.
#[tokio::test]
async fn clean_tree_is_clean_and_logs_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, _lib, data_dir) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::live_photo()
        .with_cloud_id("c1")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&data_dir, &store, &desc, &id).await;


    let report = fsck(&store, &data_dir, false).await;
    assert!(report.is_clean(), "{report:?}");

    assert!(
        store
            .log_events("fsck_orphans_deleted")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .log_events("refcount_repaired")
            .await
            .unwrap()
            .is_empty()
    );
}

// Impact: refcounts gate irreversible file deletes; fsck must expose drift
// without --repair (operator wants a dry look first) and fix it under
// --repair.
// Should: report drift read-only unrepaired; repair + log under --repair.
// Should not: mutate blobs rows on the default run.
#[tokio::test]
async fn refcount_drift_reported_then_repaired() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, data_dir) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c1")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&data_dir, &store, &desc, &id).await;

    // Corrupt the stored count.
    sqlx::query("UPDATE blobs SET ref_count = 7 WHERE library_id = ?")
        .bind(lib.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();

    let report = fsck(&store, &data_dir, false).await;
    assert_eq!(report.refcount_drift.len(), 1);
    assert!(!report.refcount_repaired);
    assert!(!report.is_clean());
    let still: i64 = sqlx::query_scalar("SELECT ref_count FROM blobs")
        .fetch_one(store.raw_pool())
        .await
        .unwrap();
    assert_eq!(still, 7, "default run must not repair");

    let report = fsck(&store, &data_dir, true).await;
    assert_eq!(
        report.refcount_drift.len(),
        1,
        "report shows what was found"
    );
    assert!(report.refcount_repaired);
    assert!(report.is_clean());
    let fixed: i64 = sqlx::query_scalar("SELECT ref_count FROM blobs")
        .fetch_one(store.raw_pool())
        .await
        .unwrap();
    assert_eq!(fixed, 1);
    assert_eq!(
        store.log_events("refcount_repaired").await.unwrap().len(),
        1
    );
}

// Impact: a missing blob is byte loss — the one finding that must never be
// silently "repaired" away (deleting the row would erase the evidence).
// Should: report loudly and keep reporting after --repair.
#[tokio::test]
async fn missing_blob_is_loud_and_survives_repair() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, _lib, data_dir) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c1")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&data_dir, &store, &desc, &id).await;

    // Destroy the blob file behind the row's back.
    let row = &store.resources_for_photo(&id).await.unwrap()[0];
    let _config = store
        .library(&LibraryId::new("personal"))
        .await
        .unwrap()
        .unwrap();
    let path =
        data_dir.spool().blob_path(row.content_hash.as_ref().unwrap(), "bin");
    std::fs::remove_file(&path).unwrap();

    for repair in [false, true] {
        let report = fsck(&store, &data_dir, repair).await;
        assert_eq!(report.missing_blobs.len(), 1, "repair={repair}");
        assert_eq!(report.missing_blobs[0].expected_path, path);
        assert!(!report.is_clean());
    }
    // The row must still exist — no destructive "fix".
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blobs")
        .fetch_one(store.raw_pool())
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

// Impact: orphan deletion is fsck's ONE destructive repair; running it by
// default (or touching .partial temps) would delete a mid-write blob.
// Should: report + leave orphans by default; delete + log under --repair.
// Should not: flag .partial contents.
#[tokio::test]
async fn orphans_deleted_only_under_repair() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, data_dir) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c1")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&data_dir, &store, &desc, &id).await;

    let _config = store.library(&lib).await.unwrap().unwrap();
    let orphan = plant_orphan(&data_dir.spool());
    // An in-flight temp that must never be flagged or deleted.
    let partial = data_dir.spool()
        .partial_dir()
        .join("probe-test");
    std::fs::create_dir_all(partial.parent().unwrap()).unwrap();
    std::fs::write(&partial, b"inflight").unwrap();

    let report = fsck(&store, &data_dir, false).await;
    assert_eq!(report.orphan_blobs.len(), 1);
    assert_eq!(report.orphans_deleted, 0);
    assert!(!report.is_clean());
    assert!(orphan.is_file(), "default run leaves the file");
    assert!(report.foreign_files.is_empty());

    let report = fsck(&store, &data_dir, true).await;
    assert_eq!(report.orphan_blobs.len(), 1);
    assert_eq!(report.orphans_deleted, 1);
    assert!(report.is_clean());
    assert!(!orphan.exists(), "--repair deletes the orphan");
    assert!(partial.is_file(), ".partial temp untouched");
    assert_eq!(
        store
            .log_events("fsck_orphans_deleted")
            .await
            .unwrap()
            .len(),
        1
    );
}

// Impact: fsck's delete gate is exact-match-only — an ext mismatch or a
// foreign file under --repair must survive (deleting either could destroy
// the only copy of something fsck doesn't understand).
// Should: report both classes and delete neither.
#[tokio::test]
async fn ext_mismatch_and_foreign_files_never_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, data_dir) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c1")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&data_dir, &store, &desc, &id).await;

    let _config = store.library(&lib).await.unwrap().unwrap();
    let paths = data_dir.spool();
    // Same hash as the real row, different extension on disk.
    let row = &store.resources_for_photo(&id).await.unwrap()[0];
    let hash = row.content_hash.as_ref().unwrap();
    let mismatched = paths.blob_path(hash, "heic");
    std::fs::write(&mismatched, b"x").unwrap();
    // Junk at fan-out depth and at top level.
    let junk = paths.blobs_dir().join("README.txt");
    std::fs::write(&junk, b"hello").unwrap();
    // Finder droppings at every level: exempt, never findings (a browsed
    // share would otherwise pin fsck at exit 1 forever).
    let ds_top = paths.blobs_dir().join(".DS_Store");
    std::fs::write(&ds_top, b"finder").unwrap();
    let ds_leaf = mismatched.parent().unwrap().join(".DS_Store");
    std::fs::write(&ds_leaf, b"finder").unwrap();

    let report = fsck(&store, &data_dir, true).await;
    assert_eq!(report.ext_mismatches.len(), 1);
    assert_eq!(report.ext_mismatches[0].file_ext, "heic");
    assert_eq!(report.foreign_files, vec![junk.clone()]);
    assert!(!report.is_clean());
    assert!(mismatched.is_file(), "ext mismatch never deleted");
    assert!(junk.is_file(), "foreign file never deleted");
    assert!(ds_top.is_file() && ds_leaf.is_file(), ".DS_Store untouched");
}

// Impact: a dead-pid lock reclaim is THE unclean-shutdown signal; if fsck
// --repair consumes it without running Tier-1, the next daemon start looks
// clean and skips the recount that gates irreversible deletes.
// Should: reclaim, run the refcount repair, and log it.
#[tokio::test]
async fn repair_on_unclean_reclaim_runs_tier1() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, data_dir) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c1")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&data_dir, &store, &desc, &id).await;

    // Simulate the crash: stale empty lock + drifted refcount.
    std::fs::write(data_dir.root().join("drain.lock"), "").unwrap();
    sqlx::query("UPDATE blobs SET ref_count = 9 WHERE library_id = ?")
        .bind(lib.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();

    let report = fsck(&store, &data_dir, true).await;
    assert!(report.refcount_repaired);
    assert!(report.is_clean());
    let fixed: i64 = sqlx::query_scalar("SELECT ref_count FROM blobs")
        .fetch_one(store.raw_pool())
        .await
        .unwrap();
    assert_eq!(fixed, 1);
    assert_eq!(
        store.log_events("refcount_repaired").await.unwrap().len(),
        1
    );
    assert!(
        !data_dir.root().join("drain.lock").exists(),
        "lock released after the run"
    );
}

// Impact: after spool eviction, missing bytes are the CORRECT state — fsck
// reading them as byte loss would page the operator on every archive photo.
// Should: report clean when an evicted blob's file is absent.
// Should: classify a lingering file for an evicted row as a benign orphan
// (the stamp-then-unlink crash window), never byte loss.
#[tokio::test]
async fn evicted_blob_is_not_byte_loss() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, data_dir) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c1")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&data_dir, &store, &desc, &id).await;

    let row = &store.resources_for_photo(&id).await.unwrap()[0];
    let hash = row.content_hash.clone().unwrap();
    let path = data_dir.spool().blob_path(&hash, "bin");

    // Completed eviction: stamp + unlink.
    store.stamp_blob_evicted(&lib, &hash).await.unwrap();
    std::fs::remove_file(&path).unwrap();

    let report = fsck(&store, &data_dir, false).await;
    assert!(report.missing_blobs.is_empty(), "{report:?}");
    assert!(report.is_clean());

    // Crash window: stamped but the unlink never ran.
    std::fs::write(&path, b"lingering").unwrap();
    let report = fsck(&store, &data_dir, false).await;
    assert!(report.missing_blobs.is_empty());
    assert_eq!(report.orphan_blobs.len(), 1, "lingering file is orphan-class");
}
