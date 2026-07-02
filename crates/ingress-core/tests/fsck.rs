//! Tier-2 fsck audit (spec §Recovery Tier 2): refcount drift, blob
//! existence, orphan scan, sidecar consistency local + remote.

use chrono::Utc;
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::fsck::{FsckOptions, RemoteProblem, SidecarProblem, run_fsck};
use ingress_core::model::LibraryConfig;
use ingress_core::paths::{BlobPaths, DataDir};
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::{AssetDescriptor, ContentHash, LibraryId, PhotoId, StateStore};

/// File-backed store: personal library with tempdir blob root and remote
/// sidecar root.
async fn rig(tmp: &std::path::Path) -> (StateStore, LibraryId, DataDir) {
    let store = StateStore::open(&tmp.join("state.db")).await.unwrap();
    let personal = LibraryId::new("personal");
    store
        .insert_library(&LibraryConfig {
            library_id: personal.clone(),
            display_name: "Personal".into(),
            blob_root: tmp.join("blobs-personal").to_string_lossy().into_owned(),
            sidecar_root_remote: Some(tmp.join("remote-personal").to_string_lossy().into_owned()),
            scope_binding: None,
            retention_days: 30,
            created_at: Utc::now(),
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
    store: &StateStore,
    data_dir: &DataDir,
    desc: &AssetDescriptor,
    photo_id: &PhotoId,
) {
    let library = store
        .photo(photo_id)
        .await
        .unwrap()
        .unwrap()
        .library_id
        .unwrap();
    let config = store.library(&library).await.unwrap().unwrap();
    let paths = BlobPaths::new(&config.blob_root);
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
    ingress_core::sidecar_io::write_photo_sidecar(store, data_dir, desc, photo_id)
        .await
        .unwrap();
}

/// Materialize + replicate the sidecar to the remote root (stamped).
async fn replicate(store: &StateStore, data_dir: &DataDir) {
    let mut state = ingress_core::cleanup::ReplicationState::default();
    ingress_core::cleanup::replicate_dirty_sidecars(
        store,
        data_dir,
        100,
        &std::collections::HashSet::new(),
        &mut state,
    )
    .await
    .unwrap();
}

async fn fsck(
    store: &StateStore,
    data_dir: &DataDir,
    repair: bool,
    deep: bool,
) -> ingress_core::fsck::FsckReport {
    run_fsck(store, data_dir, &FsckOptions { repair, deep })
        .await
        .unwrap()
}

fn plant_orphan(blob_root: &str) -> std::path::PathBuf {
    let hash = ContentHash::of_bytes(b"orphan-bytes");
    let path = BlobPaths::new(blob_root).blob_path(&hash, "bin");
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
    materialize_all(&store, &data_dir, &desc, &id).await;
    replicate(&store, &data_dir).await;

    let report = fsck(&store, &data_dir, false, true).await;
    assert!(report.is_clean(), "{report:?}");
    assert!(report.skipped_roots.is_empty());
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
    materialize_all(&store, &data_dir, &desc, &id).await;

    // Corrupt the stored count.
    sqlx::query("UPDATE blobs SET ref_count = 7 WHERE library_id = ?")
        .bind(lib.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();

    let report = fsck(&store, &data_dir, false, false).await;
    assert_eq!(report.refcount_drift.len(), 1);
    assert!(!report.refcount_repaired);
    assert!(!report.is_clean());
    let still: i64 = sqlx::query_scalar("SELECT ref_count FROM blobs")
        .fetch_one(store.raw_pool())
        .await
        .unwrap();
    assert_eq!(still, 7, "default run must not repair");

    let report = fsck(&store, &data_dir, true, false).await;
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
    materialize_all(&store, &data_dir, &desc, &id).await;

    // Destroy the blob file behind the row's back.
    let row = &store.resources_for_photo(&id).await.unwrap()[0];
    let config = store
        .library(&LibraryId::new("personal"))
        .await
        .unwrap()
        .unwrap();
    let path =
        BlobPaths::new(&config.blob_root).blob_path(row.content_hash.as_ref().unwrap(), "bin");
    std::fs::remove_file(&path).unwrap();

    for repair in [false, true] {
        let report = fsck(&store, &data_dir, repair, false).await;
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
    materialize_all(&store, &data_dir, &desc, &id).await;

    let config = store.library(&lib).await.unwrap().unwrap();
    let orphan = plant_orphan(&config.blob_root);
    // An in-flight temp that must never be flagged or deleted.
    let partial = BlobPaths::new(&config.blob_root)
        .partial_dir()
        .join("probe-test");
    std::fs::create_dir_all(partial.parent().unwrap()).unwrap();
    std::fs::write(&partial, b"inflight").unwrap();

    let report = fsck(&store, &data_dir, false, false).await;
    assert_eq!(report.orphan_blobs.len(), 1);
    assert_eq!(report.orphans_deleted, 0);
    assert!(!report.is_clean());
    assert!(orphan.is_file(), "default run leaves the file");
    assert!(report.foreign_files.is_empty());

    let report = fsck(&store, &data_dir, true, false).await;
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
    materialize_all(&store, &data_dir, &desc, &id).await;

    let config = store.library(&lib).await.unwrap().unwrap();
    let paths = BlobPaths::new(&config.blob_root);
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

    let report = fsck(&store, &data_dir, true, false).await;
    assert_eq!(report.ext_mismatches.len(), 1);
    assert_eq!(report.ext_mismatches[0].file_ext, "heic");
    assert_eq!(report.foreign_files, vec![junk.clone()]);
    assert!(!report.is_clean());
    assert!(mismatched.is_file(), "ext mismatch never deleted");
    assert!(junk.is_file(), "foreign file never deleted");
    assert!(ds_top.is_file() && ds_leaf.is_file(), ".DS_Store untouched");
}

// Impact: a down mount looks exactly like mass byte loss to a naive check;
// fsck screaming BYTE LOSS during SMB flaps would train the operator to
// ignore the one banner that matters.
// Should: record the root as skipped, zero missing-blob findings.
#[tokio::test]
async fn absent_blob_root_is_skipped_not_byte_loss() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, data_dir) = rig(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c1")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    // "Unmount": the whole root vanishes.
    let config = store.library(&lib).await.unwrap().unwrap();
    std::fs::remove_dir_all(&config.blob_root).unwrap();

    let report = fsck(&store, &data_dir, false, false).await;
    assert!(report.missing_blobs.is_empty());
    assert_eq!(report.skipped_roots.len(), 1);
    assert!(report.skipped_roots[0].contains("blob root unavailable"));
}

// Impact: sidecars are the only off-device record of the photo-to-blob
// mapping; silent local drift becomes permanent damage the day the Mac dies.
// Should: flag a missing document, a corrupt document, and db-vs-doc field
// drift (including the resources array).
#[tokio::test]
async fn sidecar_findings_cover_missing_corrupt_and_drift() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, data_dir) = rig(tmp.path()).await;

    let missing_desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c-missing")
        .build();
    let corrupt_desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c-corrupt")
        .build();
    let drifted_desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c-drift")
        .build();
    let missing_id = seed_one(&store, &missing_desc).await;
    let corrupt_id = seed_one(&store, &corrupt_desc).await;
    let drifted_id = seed_one(&store, &drifted_desc).await;
    for (desc, id) in [
        (&missing_desc, &missing_id),
        (&corrupt_desc, &corrupt_id),
        (&drifted_desc, &drifted_id),
    ] {
        materialize_all(&store, &data_dir, desc, id).await;
    }

    let root = data_dir.sidecar_root(&lib);
    let find = |id: &PhotoId| {
        ingress_core::sidecar_io::find_sidecar(&root, id)
            .unwrap()
            .unwrap()
    };
    std::fs::remove_file(find(&missing_id)).unwrap();
    std::fs::write(find(&corrupt_id), b"{ not json").unwrap();
    // Drift: flip deleted_at in the doc only.
    let path = find(&drifted_id);
    let doc = std::fs::read_to_string(&path).unwrap();
    let mut v: serde_json::Value = serde_json::from_str(&doc).unwrap();
    v["deleted_at"] = serde_json::json!("2020-01-01T00:00:00Z");
    std::fs::write(&path, serde_json::to_string_pretty(&v).unwrap()).unwrap();

    let report = fsck(&store, &data_dir, false, false).await;
    assert_eq!(report.sidecar_findings.len(), 3, "{report:?}");
    let kind_for = |id: &PhotoId| {
        &report
            .sidecar_findings
            .iter()
            .find(|f| &f.photo_id == id)
            .unwrap()
            .kind
    };
    assert!(matches!(kind_for(&missing_id), SidecarProblem::Missing));
    assert!(matches!(
        kind_for(&corrupt_id),
        SidecarProblem::ParseError { .. }
    ));
    match kind_for(&drifted_id) {
        SidecarProblem::Mismatch { fields } => {
            assert_eq!(fields, &vec!["deleted_at".to_string()])
        }
        other => panic!("expected Mismatch, got {other:?}"),
    }
}

// Impact: "stamped ⇒ remote ≥ local" is the disaster-recovery invariant;
// a stamp with no (or stale) remote bytes means the backup lies.
// Should: existence miss by default; --deep catches byte-different remote
// content that passes the existence check.
#[tokio::test]
async fn remote_findings_existence_and_deep() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, data_dir) = rig(tmp.path()).await;

    let gone_desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c-gone")
        .build();
    let stale_desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c-stale")
        .build();
    let gone_id = seed_one(&store, &gone_desc).await;
    let stale_id = seed_one(&store, &stale_desc).await;
    materialize_all(&store, &data_dir, &gone_desc, &gone_id).await;
    materialize_all(&store, &data_dir, &stale_desc, &stale_id).await;
    replicate(&store, &data_dir).await;

    let remote_root = store
        .library(&lib)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    let remote_of = |id: &PhotoId| {
        ingress_core::sidecar_io::find_sidecar(std::path::Path::new(&remote_root), id)
            .unwrap()
            .unwrap()
    };
    std::fs::remove_file(remote_of(&gone_id)).unwrap();
    std::fs::write(remote_of(&stale_id), b"{\"stale\": true}").unwrap();

    let shallow = fsck(&store, &data_dir, false, false).await;
    assert_eq!(shallow.remote_findings.len(), 1, "existence only");
    assert!(matches!(
        shallow.remote_findings[0].kind,
        RemoteProblem::Missing { .. }
    ));

    let deep = fsck(&store, &data_dir, false, true).await;
    assert_eq!(deep.remote_findings.len(), 2, "{deep:?}");
    assert!(
        deep.remote_findings
            .iter()
            .any(|f| { f.photo_id == stale_id && matches!(f.kind, RemoteProblem::Differs { .. }) })
    );
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
    materialize_all(&store, &data_dir, &desc, &id).await;

    // Simulate the crash: stale empty lock + drifted refcount.
    std::fs::write(data_dir.root().join("drain.lock"), "").unwrap();
    sqlx::query("UPDATE blobs SET ref_count = 9 WHERE library_id = ?")
        .bind(lib.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();

    let report = fsck(&store, &data_dir, true, false).await;
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
