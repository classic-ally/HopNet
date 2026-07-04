//! Lifecycle scenarios (spec §Hard-delete cleanup, §State snapshots,
//! §photos notes on `sidecar_replicated_at`, §Failure Handling remote
//! replication) — Phase 5.

use std::collections::HashSet;

use chrono::{Duration, Utc};
use ingress_core::classify::{apply_change, apply_removal};
use ingress_core::cleanup::{
    CleanupConfig, ReplicationState, replicate_dirty_sidecars, run_cleanup,
};
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig};
use ingress_core::paths::{BlobPaths, DataDir};
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::{AssetDescriptor, ContentHash, LibraryId, LibraryScope, PhotoId, StateStore};

/// Personal + shared libraries with tempdir blob roots AND remote sidecar
/// roots (the replication target). File-backed (not in-memory): snapshots
/// go through `VACUUM INTO`, which silently no-ops on an in-memory store.
async fn store_with_remotes(tmp: &std::path::Path) -> (StateStore, LibraryId, LibraryId) {
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
                blob_root: tmp
                    .join(format!("blobs-{id}"))
                    .to_string_lossy()
                    .into_owned(),
                sidecar_root_remote: Some(
                    tmp.join(format!("remote-{id}"))
                        .to_string_lossy()
                        .into_owned(),
                ),
                scope_binding: binding,
                retention_days: 30,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
    }
    (store, personal, shared)
}

async fn seed_one(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
    match seed_descriptor(store, desc).await.expect("seed") {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

/// Materialize every pending resource with real blob bytes + write the sidecar.
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
    let size = desc.resources[0].expected_size.unwrap() as i64;
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
            .mark_resource_written(photo_id, row.resource_type, &hash, "bin", size)
            .await
            .unwrap();
    }
    ingress_core::sidecar_io::write_photo_sidecar(store, data_dir, desc, photo_id)
        .await
        .unwrap();
}

async fn stamp_replicated(store: &StateStore, id: &PhotoId) {
    sqlx::query("UPDATE photos SET sidecar_replicated_at = ? WHERE photo_id = ?")
        .bind(Utc::now())
        .bind(id.to_string())
        .execute(store.raw_pool())
        .await
        .unwrap();
}

async fn replicated_at(store: &StateStore, id: &PhotoId) -> Option<String> {
    sqlx::query_scalar("SELECT sidecar_replicated_at FROM photos WHERE photo_id = ?")
        .bind(id.to_string())
        .fetch_one(store.raw_pool())
        .await
        .unwrap()
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
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
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
    let blob_file = BlobPaths::new(&store.library(&lib).await.unwrap().unwrap().blob_root)
        .blob_path(&h1, "bin");
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

// Impact: a missed dirty trigger means a Mac dying with an unreplicated
// sidecar rewrite silently loses that state in disaster recovery — for the
// tombstone trigger, that RESURRECTS a deleted photo.
// Should: every sidecar-rewrite trigger (completion, metadata refresh,
// revert, tombstone, restore, hard move) NULLs sidecar_replicated_at in the
// same transaction as its state change.
#[tokio::test]
async fn sidecar_dirty_flag_nulls_on_every_rewrite_trigger() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, ..) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let t1 = Utc::now();
    let desc = AssetDescriptorBuilder::edited_live_photo()
        .modified_at(t1)
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    // T6 — metadata refresh.
    stamp_replicated(&store, &id).await;
    let mut newer = desc.clone();
    newer.asset_modified_at = Some(t1 + Duration::seconds(5));
    newer.favorite = true;
    apply_change(&store, &data_dir, &newer).await.unwrap();
    assert_eq!(
        replicated_at(&store, &id).await,
        None,
        "T6 metadata refresh must dirty"
    );

    // T2 + T1 — re-edit reopens (resource-plan tx), then completion re-stamps.
    stamp_replicated(&store, &id).await;
    let mut reedit = newer.clone();
    reedit.asset_modified_at = Some(t1 + Duration::seconds(10));
    for r in &mut reedit.resources {
        if r.ph_resource_type == 5 {
            r.expected_size = Some(9_999_999);
        }
    }
    apply_change(&store, &data_dir, &reedit).await.unwrap();
    assert_eq!(
        replicated_at(&store, &id).await,
        None,
        "T2 resource-plan tx must dirty"
    );

    stamp_replicated(&store, &id).await;
    let new_hash = ContentHash::of_bytes(b"render-v2");
    store
        .mark_resource_written(
            &id,
            ingress_core::ResourceType::Edited,
            &new_hash,
            "bin",
            9_999_999,
        )
        .await
        .unwrap();
    assert_eq!(
        replicated_at(&store, &id).await,
        None,
        "T1 completion must dirty"
    );

    // T2 (revert flavor) — rows removed, sidecar rewritten.
    stamp_replicated(&store, &id).await;
    let mut reverted = AssetDescriptorBuilder::live_photo()
        .modified_at(t1 + Duration::seconds(15))
        .build();
    reverted.cloud_id = desc.cloud_id.clone();
    reverted.local_id = desc.local_id.clone();
    apply_change(&store, &data_dir, &reverted).await.unwrap();
    assert_eq!(
        replicated_at(&store, &id).await,
        None,
        "T2 revert must dirty"
    );

    // T3 — tombstone.
    stamp_replicated(&store, &id).await;
    apply_removal(&store, &data_dir, &desc.local_id)
        .await
        .unwrap();
    assert_eq!(
        replicated_at(&store, &id).await,
        None,
        "T3 tombstone must dirty"
    );

    // T4 — restore.
    stamp_replicated(&store, &id).await;
    apply_change(&store, &data_dir, &reverted).await.unwrap();
    assert_eq!(
        replicated_at(&store, &id).await,
        None,
        "T4 restore must dirty"
    );

    // T5 — hard move.
    stamp_replicated(&store, &id).await;
    let mut moved = reverted.clone();
    moved.scope = LibraryScope::Shared;
    moved.asset_modified_at = Some(t1 + Duration::seconds(20));
    apply_change(&store, &data_dir, &moved).await.unwrap();
    assert_eq!(
        replicated_at(&store, &id).await,
        None,
        "T5 transition must dirty"
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
// artifact (rows, blob files, local AND remote sidecars) must go, and the
// black-box hard_delete row must commit atomically with the row deletions.
// Should: remove everything and log hard_delete with the reaped hashes.
#[tokio::test]
async fn hard_delete_past_retention_removes_everything() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::live_photo()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    // Replicate the sidecar so a remote copy exists to be deleted.
    let mut state = ReplicationState::default();
    replicate_dirty_sidecars(&store, &data_dir, 100, &HashSet::new(), &mut state)
        .await
        .unwrap();
    let remote_root = store
        .library(&lib)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    let remote_count = walkdir_count(std::path::Path::new(&remote_root));
    assert_eq!(remote_count, 1, "remote sidecar replicated as precondition");

    apply_removal(&store, &data_dir, &desc.local_id)
        .await
        .unwrap();
    backdate_tombstone(&store, &id, 31).await;

    let hashes: Vec<ContentHash> = store
        .resources_for_photo(&id)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|r| r.content_hash)
        .collect();
    let paths = BlobPaths::new(&store.library(&lib).await.unwrap().unwrap().blob_root);

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
        assert!(!paths.blob_path(h, "bin").is_file(), "blob file deleted");
    }
    assert!(
        ingress_core::sidecar_io::find_sidecar(&data_dir.sidecar_root(&lib), &id)
            .unwrap()
            .is_none(),
        "local sidecar deleted"
    );
    assert_eq!(
        walkdir_count(std::path::Path::new(&remote_root)),
        0,
        "remote sidecar deleted"
    );

    let events = store.log_events("hard_delete").await.unwrap();
    assert_eq!(events.len(), 1);
    let detail: serde_json::Value =
        serde_json::from_str(events[0].detail.as_ref().unwrap()).unwrap();
    assert_eq!(detail["reaped"].as_array().unwrap().len(), 2);
}

fn walkdir_count(root: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path.extension().map(|x| x == "json").unwrap_or(false) {
                    *acc += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(root, &mut n);
    n
}

/// Count leftover `.json.tmp` staging files under a remote root — the litter
/// a failed `copy_atomic` must clean up.
fn tmp_litter_count(root: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path.extension().map(|x| x == "tmp").unwrap_or(false) {
                    *acc += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(root, &mut n);
    n
}

/// The remote path a photo's local sidecar replicates to (mirrors the drain's
/// own `<remote_root>/<YYYY/MM>/<id>.json` derivation).
async fn remote_dest(
    store: &StateStore,
    data_dir: &DataDir,
    lib: &LibraryId,
    id: &PhotoId,
) -> std::path::PathBuf {
    let local = ingress_core::sidecar_io::find_sidecar(&data_dir.sidecar_root(lib), id)
        .unwrap()
        .unwrap();
    let rel = local
        .strip_prefix(data_dir.sidecar_root(lib))
        .unwrap()
        .to_path_buf();
    let remote_root = store
        .library(lib)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    std::path::Path::new(&remote_root).join(rel)
}

// Should: retention 0 reaps while retention 1000 survives, in the same run
// (per-library retention_days read fresh — spec edge-case table).
#[tokio::test]
async fn hard_delete_respects_per_library_retention() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

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

    apply_removal(&store, &data_dir, &p_desc.local_id)
        .await
        .unwrap();
    apply_removal(&store, &data_dir, &s_desc.local_id)
        .await
        .unwrap();
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
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

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
    let paths = BlobPaths::new(&store.library(&lib).await.unwrap().unwrap().blob_root);
    let path = paths.blob_path(&hash, "bin");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    for id in [&keep, &gone] {
        store
            .mark_resource_written(id, ingress_core::ResourceType::Original, &hash, "bin", 12)
            .await
            .unwrap();
    }
    assert_eq!(store.blob(&lib, &hash).await.unwrap().unwrap().ref_count, 2);

    apply_removal(&store, &data_dir, &gone_desc.local_id)
        .await
        .unwrap();
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
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    // Pending-only photo (seeded, never fetched).
    let pending_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let pending = seed_one(&store, &pending_desc).await;
    apply_removal(&store, &data_dir, &pending_desc.local_id)
        .await
        .unwrap();
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
    apply_removal(&store, &data_dir, &sp_desc.local_id)
        .await
        .unwrap();
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
    let (store, ..) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    for _ in 0..2 {
        let desc = AssetDescriptorBuilder::simple_image()
            .modified_at(Utc::now())
            .build();
        let id = seed_one(&store, &desc).await;
        materialize_all(&store, &data_dir, &desc, &id).await;
        apply_removal(&store, &data_dir, &desc.local_id)
            .await
            .unwrap();
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
    let (store, ..) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    // Unmapped mint: personal store has no binding for a Shared-scope asset?
    // Both scopes are bound here, so mint the row directly.
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
    let (store, ..) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

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

// Impact: snapshots are what Tier-3 recovery restores from on a dead Mac —
// they must be written daily, be openable, and prune to the newest 7.
// Should: one per UTC day per root; same-day rerun no-op; consistent copy;
// keep-7 pruning ignoring unparseable names.
#[tokio::test]
async fn snapshots_daily_consistent_and_pruned() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    let now = Utc::now();
    let r1 = run_cleanup(&store, &data_dir, &cfg(), now).await.unwrap();
    assert_eq!(r1.snapshots_written, 2, "one per library root");
    let snap_dir =
        BlobPaths::new(&store.library(&lib).await.unwrap().unwrap().blob_root).snapshot_dir();
    let first: Vec<_> = std::fs::read_dir(&snap_dir).unwrap().flatten().collect();
    assert_eq!(first.len(), 1);

    // Same day: no-op.
    let r2 = run_cleanup(&store, &data_dir, &cfg(), now + Duration::hours(1))
        .await
        .unwrap();
    assert_eq!(r2.snapshots_written, 0);

    // The snapshot is a consistent, openable database.
    let snap_path = first[0].path();
    let snap_store = StateStore::open(&snap_path).await.unwrap();
    assert_eq!(snap_store.count_photos().await.unwrap(), 1);

    // Next days: new snapshots; keep only the newest N (unparseable ignored).
    std::fs::write(snap_dir.join("not-a-snapshot.txt"), "x").unwrap();
    let keep2 = CleanupConfig {
        snapshot_keep: 2,
        ..cfg()
    };
    for d in 1..=3 {
        let r = run_cleanup(&store, &data_dir, &keep2, now + Duration::days(d))
            .await
            .unwrap();
        assert_eq!(r.snapshots_written, 2);
    }
    let names: Vec<String> = std::fs::read_dir(&snap_dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    // Note: opening the snapshot above leaves `-wal`/`-shm` companions in
    // the dir — count only real snapshot names.
    assert_eq!(
        names
            .iter()
            .filter(|n| n.starts_with("state.db.") && n.ends_with(".sqlite3"))
            .count(),
        2
    );
    assert!(
        names.iter().any(|n| n == "not-a-snapshot.txt"),
        "unparseable untouched"
    );
}

// Impact: remote sidecars are what saves recovery from blob-only rebuild —
// the drain must copy byte-identically, stamp, and never touch NULL-remote
// libraries or photos without sidecars.
// Should: dirty materialized photo lands at remote YYYY/MM byte-identical,
// flag stamped, second pass no-op; skip-set and missing-sidecar photos
// untouched/unstamped.
#[tokio::test]
async fn replication_copies_dirty_and_respects_skips() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;
    // A dirty photo whose sidecar is missing (crash-window class).
    let ghost_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let ghost = seed_one(&store, &ghost_desc).await;
    {
        let library = store
            .photo(&ghost)
            .await
            .unwrap()
            .unwrap()
            .library_id
            .unwrap();
        let _ = library;
        // Materialize WITHOUT writing the sidecar.
        let hash = ContentHash::of_bytes(b"ghost");
        store
            .mark_resource_written(
                &ghost,
                ingress_core::ResourceType::Original,
                &hash,
                "bin",
                5,
            )
            .await
            .unwrap();
    }

    // Skip-set photo: dirty but inflight.
    let inflight_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let inflight = seed_one(&store, &inflight_desc).await;
    materialize_all(&store, &data_dir, &inflight_desc, &inflight).await;
    let skip: HashSet<PhotoId> = [inflight.clone()].into_iter().collect();

    let mut state = ReplicationState::default();
    let report = replicate_dirty_sidecars(&store, &data_dir, 100, &skip, &mut state)
        .await
        .unwrap();
    assert_eq!(report.replicated, 1);
    assert_eq!(report.missing, 1);
    assert!(!report.stalled);

    let local = ingress_core::sidecar_io::find_sidecar(&data_dir.sidecar_root(&lib), &id)
        .unwrap()
        .unwrap();
    let remote_root = store
        .library(&lib)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    let rel = local
        .strip_prefix(data_dir.sidecar_root(&lib))
        .unwrap()
        .to_path_buf();
    let remote = std::path::Path::new(&remote_root).join(rel);
    assert_eq!(
        std::fs::read(&local).unwrap(),
        std::fs::read(&remote).unwrap(),
        "byte-identical copy"
    );
    assert!(replicated_at(&store, &id).await.is_some(), "stamped");
    assert_eq!(
        replicated_at(&store, &inflight).await,
        None,
        "skip-set unstamped"
    );
    assert_eq!(
        replicated_at(&store, &ghost).await,
        None,
        "missing-sidecar unstamped"
    );

    // Second pass: only the still-dirty candidates remain; nothing recopied.
    let again = replicate_dirty_sidecars(&store, &data_dir, 100, &skip, &mut state)
        .await
        .unwrap();
    assert_eq!(again.replicated, 0);
}

// Impact: a down mount must not flood the log or burn the pass budget —
// and recovery must be announced exactly once.
// Should: one mount_lost on first failure, one mount_regained on recovery.
// Should not: log per failing tick.
#[tokio::test]
async fn replication_stall_edge_logs_once() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    // Make the remote root unwritable: replace the dir with a FILE.
    let remote_root = store
        .library(&lib)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    let _ = std::fs::remove_dir_all(&remote_root);
    std::fs::write(&remote_root, "not a dir").unwrap();

    let mut state = ReplicationState::default();
    for _ in 0..3 {
        let r = replicate_dirty_sidecars(&store, &data_dir, 100, &HashSet::new(), &mut state)
            .await
            .unwrap();
        assert!(r.stalled);
        assert_eq!(r.replicated, 0);
    }
    assert_eq!(
        store.log_events("mount_lost").await.unwrap().len(),
        1,
        "edge, not level"
    );
    assert_eq!(replicated_at(&store, &id).await, None);

    // Mount comes back.
    std::fs::remove_file(&remote_root).unwrap();
    std::fs::create_dir_all(&remote_root).unwrap();
    let r = replicate_dirty_sidecars(&store, &data_dir, 100, &HashSet::new(), &mut state)
        .await
        .unwrap();
    assert_eq!(r.replicated, 1);
    assert!(!r.stalled);
    assert_eq!(store.log_events("mount_regained").await.unwrap().len(), 1);
}

// Impact: the drain visits photos in `photo_id` order; before this fix one
// un-copyable photo at the head aborted the whole pass, so a single poison
// file (a Photos-provenance xattr the mount rejected, a stale destination)
// froze replication for every following photo AND every other library — the
// production incident where a Personal-library head photo left 26k sidecars
// unbacked while the mount was healthy.
// Should: skip the failing photo, keep draining the rest, stamp the good one,
// leave the poison unstamped, and log ONE sidecar_copy_failed (not mount_lost)
// with no leftover .tmp litter.
// Should not: set stalled, or block the later photo.
#[tokio::test]
async fn replication_poison_file_does_not_freeze_backlog() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    // Two dirty photos in one library. Materialize both, then pick the one
    // that sorts FIRST by photo_id (SQLite BINARY order == Rust String order)
    // as the poison — reproducing the head-of-line freeze exactly.
    let mut ids = Vec::new();
    for _ in 0..2 {
        let desc = AssetDescriptorBuilder::simple_image()
            .modified_at(Utc::now())
            .build();
        let id = seed_one(&store, &desc).await;
        materialize_all(&store, &data_dir, &desc, &id).await;
        ids.push(id);
    }
    ids.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    let (poison, healthy) = (ids[0].clone(), ids[1].clone());

    // Sabotage the poison's destination: a NON-EMPTY directory where its
    // `.json` should land makes the rename fail (ENOTEMPTY) while the remote
    // ROOT stays a healthy directory — a per-file failure, not a lost mount.
    let dst = remote_dest(&store, &data_dir, &lib, &poison).await;
    std::fs::create_dir_all(&dst).unwrap();
    std::fs::write(dst.join("blocker"), b"x").unwrap();

    let mut state = ReplicationState::default();
    let report = replicate_dirty_sidecars(&store, &data_dir, 100, &HashSet::new(), &mut state)
        .await
        .unwrap();

    assert_eq!(report.replicated, 1, "the healthy photo still drains");
    assert_eq!(report.failed, 1, "the poison is counted, not fatal");
    assert!(!report.stalled, "a healthy mount never stalls");

    assert!(
        replicated_at(&store, &healthy).await.is_some(),
        "later photo stamped despite the poison at the head"
    );
    assert_eq!(
        replicated_at(&store, &poison).await,
        None,
        "poison stays dirty for a later retry"
    );

    assert_eq!(
        store.log_events("sidecar_copy_failed").await.unwrap().len(),
        1,
        "one row per pass, not one per poison tick"
    );
    assert_eq!(
        store.log_events("mount_lost").await.unwrap().len(),
        0,
        "a per-file failure is not a mount loss"
    );

    let remote_root = store
        .library(&lib)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    assert_eq!(
        tmp_litter_count(std::path::Path::new(&remote_root)),
        0,
        "failed copy left no .tmp behind"
    );
}

// Impact: the pre-fix copy left a 0-byte `.json.tmp` on every failed pass
// (fs::copy creates the destination before the metadata step that EPERM'd on
// the mount) — litter a later pass could mistake for a real sidecar, and the
// visible symptom that first exposed the freeze.
// Should: a failed copy removes its own staging file, leaving zero litter.
#[tokio::test]
async fn replication_removes_tmp_after_failed_copy() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    // Block just this file's destination; the mount root stays healthy.
    let dst = remote_dest(&store, &data_dir, &lib, &id).await;
    std::fs::create_dir_all(&dst).unwrap();
    std::fs::write(dst.join("blocker"), b"x").unwrap();

    let mut state = ReplicationState::default();
    let report = replicate_dirty_sidecars(&store, &data_dir, 100, &HashSet::new(), &mut state)
        .await
        .unwrap();
    assert_eq!(report.failed, 1);
    assert!(!report.stalled);
    assert_eq!(replicated_at(&store, &id).await, None, "unstamped on failure");

    let remote_root = store
        .library(&lib)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    assert_eq!(
        tmp_litter_count(std::path::Path::new(&remote_root)),
        0,
        "no 0-byte .json.tmp survives a failed copy"
    );
}

// Impact: the crash-window class (materialized, local sidecar never written)
// leaks forever — the drain skips it unstamped (report.missing) and the
// light-probe scan sees no metadata drift, so nothing re-triggers the write.
// The scan must self-heal it: re-probe as NeedsFull -> apply recomposes the
// sidecar from the live descriptor -> the drain finally backs it up.
// Should: probe verdict flips Done -> NeedsFull when the local sidecar vanishes;
// apply_change (even a no-op delivery) rewrites it; the drain then replicates
// and stamps it.
// Should not: verdict Done for a materialized photo with no local sidecar.
#[tokio::test]
async fn scan_self_heals_missing_local_sidecar() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
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

    // Baseline: an unchanged photo WITH its sidecar verdicts Done.
    let scan = ingress_core::scan::begin(&store).await.unwrap();
    assert_eq!(
        ingress_core::scan::probe(&store, &data_dir, &scan, &probe_of)
            .await
            .unwrap(),
        ingress_core::scan::ScanVerdict::Done,
        "unchanged + sidecar present = Done"
    );

    // Simulate the crash window: the completion committed but the sidecar is
    // gone (deleted / never landed).
    let local = ingress_core::sidecar_io::find_sidecar(&data_dir.sidecar_root(&lib), &id)
        .unwrap()
        .unwrap();
    std::fs::remove_file(&local).unwrap();

    // The ONLY signal now is the absent sidecar — the probe must escalate.
    let scan2 = ingress_core::scan::begin(&store).await.unwrap();
    assert_eq!(
        ingress_core::scan::probe(&store, &data_dir, &scan2, &probe_of)
            .await
            .unwrap(),
        ingress_core::scan::ScanVerdict::NeedsFull,
        "missing sidecar forces a full re-probe"
    );

    // The re-delivered descriptor classifies NoOp (nothing else changed), yet
    // apply_change recomposes the sidecar on disk.
    apply_change(&store, &data_dir, &desc).await.unwrap();
    assert!(
        ingress_core::sidecar_io::find_sidecar(&data_dir.sidecar_root(&lib), &id)
            .unwrap()
            .is_some(),
        "sidecar recomposed on disk"
    );

    // And the drain — which skipped it as `missing` before — now backs it up.
    let mut state = ReplicationState::default();
    let report = replicate_dirty_sidecars(&store, &data_dir, 100, &HashSet::new(), &mut state)
        .await
        .unwrap();
    assert_eq!(report.replicated, 1, "healed sidecar replicates");
    assert_eq!(report.missing, 0, "no longer the missing-sidecar class");
    assert!(replicated_at(&store, &id).await.is_some(), "stamped");
}

// Impact: a cleanup run concurrent with a live daemon would race photo_tasks
// and the loop's own ticks — the shared lock is the exclusivity story.
// Should: error while a live-pid lock exists; run + release otherwise.
#[tokio::test]
async fn standalone_cleanup_respects_drain_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, ..) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    std::fs::create_dir_all(data_dir.root()).unwrap();

    // A live holder (our own pid).
    let lock_path = data_dir.root().join("drain.lock");
    std::fs::write(&lock_path, std::process::id().to_string()).unwrap();
    let err = ingress_core::cleanup::run_standalone(&store, &data_dir, &cfg(), Utc::now()).await;
    assert!(err.is_err(), "live-pid lock refuses standalone cleanup");

    std::fs::remove_file(&lock_path).unwrap();
    let (cleanup, replication) =
        ingress_core::cleanup::run_standalone(&store, &data_dir, &cfg(), Utc::now())
            .await
            .unwrap();
    assert_eq!(cleanup.photos_hard_deleted, 0);
    assert_eq!(replication.replicated, 0);
    assert!(!lock_path.exists(), "lock released after the run");
}

// Impact: a lingering remote src document resurrects the photo in the WRONG
// library during a sidecar-tree recovery (spec gap fixed this phase).
// Should: the source library's remote copy is removed by a hard move; the
// destination copy appears on the next drain.
#[tokio::test]
async fn transition_removes_stale_remote_src_sidecar() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_remotes(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    let mut state = ReplicationState::default();
    replicate_dirty_sidecars(&store, &data_dir, 100, &HashSet::new(), &mut state)
        .await
        .unwrap();
    let p_remote = store
        .library(&personal)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    assert_eq!(walkdir_count(std::path::Path::new(&p_remote)), 1);

    let mut moved = desc.clone();
    moved.scope = LibraryScope::Shared;
    apply_change(&store, &data_dir, &moved).await.unwrap();
    assert_eq!(
        walkdir_count(std::path::Path::new(&p_remote)),
        0,
        "stale remote src copy removed"
    );

    replicate_dirty_sidecars(&store, &data_dir, 100, &HashSet::new(), &mut state)
        .await
        .unwrap();
    let s_remote = store
        .library(&shared)
        .await
        .unwrap()
        .unwrap()
        .sidecar_root_remote
        .unwrap();
    assert_eq!(
        walkdir_count(std::path::Path::new(&s_remote)),
        1,
        "dst copy replicated"
    );
    assert!(replicated_at(&store, &id).await.is_some());
}
