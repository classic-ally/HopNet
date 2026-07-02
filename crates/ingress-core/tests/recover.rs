//! Tier-3 recovery (spec §Recovery Tier 3): snapshot-first restore,
//! sidecar-tree rebuild fallback, inventory error when neither exists.

use std::collections::HashSet;

use chrono::Utc;
use ingress_core::classify::apply_removal;
use ingress_core::cleanup::{
    CleanupConfig, ReplicationState, replicate_dirty_sidecars, run_cleanup,
};
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::model::LibraryConfig;
use ingress_core::paths::{BlobPaths, DataDir};
use ingress_core::recover::{RecoverLibrarySpec, RecoverOptions, RecoverSource, recover};
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::{AssetDescriptor, ContentHash, LibraryId, LibraryScope, PhotoId, StateStore};

/// A "source Mac": file-backed store, one personal library with blob +
/// remote sidecar roots under `tmp`, its own data dir.
async fn source_env(tmp: &std::path::Path) -> (StateStore, LibraryId, DataDir) {
    let store = StateStore::open(&tmp.join("state.db")).await.unwrap();
    let personal = LibraryId::new("personal");
    store
        .insert_library(&LibraryConfig {
            library_id: personal.clone(),
            display_name: "Personal".into(),
            blob_root: tmp.join("blobs").to_string_lossy().into_owned(),
            sidecar_root_remote: Some(tmp.join("remote").to_string_lossy().into_owned()),
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

async fn replicate(store: &StateStore, data_dir: &DataDir) {
    let mut state = ReplicationState::default();
    replicate_dirty_sidecars(store, data_dir, 100, &HashSet::new(), &mut state)
        .await
        .unwrap();
}

/// Populate the source env: a live photo, a burst frame, and a tombstoned
/// photo — all materialized and replicated.
async fn populate(store: &StateStore, data_dir: &DataDir) -> Vec<PhotoId> {
    let live = AssetDescriptorBuilder::live_photo()
        .with_cloud_id("c-live")
        .build();
    let burst = AssetDescriptorBuilder::burst_frame("burst-1", true)
        .with_cloud_id("c-burst")
        .build();
    let doomed = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("c-doomed")
        .with_local_id("local-doomed")
        .build();
    let mut ids = Vec::new();
    for (desc, _) in [(&live, ""), (&burst, ""), (&doomed, "")] {
        let id = seed_one(store, desc).await;
        materialize_all(store, data_dir, desc, &id).await;
        ids.push(id);
    }
    apply_removal(store, data_dir, "local-doomed")
        .await
        .unwrap();
    replicate(store, data_dir).await;
    ids
}

// Impact: snapshot restore is the complete-recovery path (photo_ids,
// tombstones, pipeline state); picking a stale snapshot when a newer root
// has a fresher one silently loses days of state.
// Should: pick the globally newest snapshot across roots, restore a store
// that opens with rows intact, and hydrate the local sidecar tree from the
// remote copies.
#[tokio::test]
async fn snapshot_restore_picks_newest_and_hydrates() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, data_dir) = source_env(tmp.path()).await;
    let ids = populate(&store, &data_dir).await;

    // Produce a real snapshot on the library root.
    let report = run_cleanup(&store, &data_dir, &CleanupConfig::default(), Utc::now())
        .await
        .unwrap();
    assert_eq!(report.snapshots_written, 1);
    let snap_dir = BlobPaths::new(tmp.path().join("blobs")).snapshot_dir();
    let newest = std::fs::read_dir(&snap_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|e| e == "sqlite3"))
        .unwrap();
    // A decoy OLDER snapshot on a second root — must lose.
    let old_root = tmp.path().join("old-root");
    let old_dir = BlobPaths::new(&old_root).snapshot_dir();
    std::fs::create_dir_all(&old_dir).unwrap();
    std::fs::write(old_dir.join("state.db.1000000000.sqlite3"), b"decoy").unwrap();

    // "New Mac": fresh data dir.
    let new_data = DataDir::new(tmp.path().join("new-data"));
    let report = recover(
        &new_data,
        &RecoverOptions {
            roots: vec![old_root, tmp.path().join("blobs")],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    match &report.source {
        RecoverSource::Snapshot { path, .. } => assert_eq!(path, &newest),
        other => panic!("expected snapshot source, got {other:?}"),
    }
    assert_eq!(report.photos, 3);
    assert_eq!(report.sidecars_hydrated, 3);
    assert_eq!(report.libraries.len(), 1);

    let restored = StateStore::open(&new_data.state_db_path()).await.unwrap();
    for id in &ids {
        let photo = restored.photo(id).await.unwrap();
        assert!(photo.is_some(), "photo_id {id} survives");
    }
    // Hydrated local tree serves the tombstoned photo's document too.
    for id in &ids {
        assert!(
            ingress_core::sidecar_io::find_sidecar(&new_data.sidecar_root(&personal), id)
                .unwrap()
                .is_some(),
            "local sidecar hydrated for {id}"
        );
    }
    assert_eq!(
        restored.log_events("recovered").await.unwrap().len(),
        1,
        "recovery is logged in the restored db"
    );
}

// Impact: recover overwrites the authoritative state; an accidental run on
// a healthy Mac must not destroy it, and --force must preserve the old db
// for post-mortem.
// Should: refuse an existing state.db without --force; move it (and WAL
// companions) aside under --force; refuse while a live daemon holds the lock.
#[tokio::test]
async fn refuses_existing_db_and_live_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, _personal, data_dir) = source_env(tmp.path()).await;
    populate(&store, &data_dir).await;
    run_cleanup(&store, &data_dir, &CleanupConfig::default(), Utc::now())
        .await
        .unwrap();
    let opts = RecoverOptions {
        roots: vec![tmp.path().join("blobs")],
        ..Default::default()
    };

    // The source env's own data dir already has a state.db.
    let source_data = DataDir::new(tmp.path()); // state.db lives at tmp root here
    let err = recover(&source_data, &opts).await.unwrap_err();
    assert!(err.to_string().contains("--force"), "{err}");
    assert!(source_data.state_db_path().is_file(), "db untouched");

    // Live lock refusal on a fresh data dir.
    let locked_data = DataDir::new(tmp.path().join("locked-data"));
    std::fs::create_dir_all(locked_data.root()).unwrap();
    std::fs::write(
        locked_data.root().join("drain.lock"),
        std::process::id().to_string(),
    )
    .unwrap();
    assert!(recover(&locked_data, &opts).await.is_err());

    // --force moves the trio aside and recovers.
    let report = recover(
        &source_data,
        &RecoverOptions {
            force: true,
            ..opts
        },
    )
    .await
    .unwrap();
    assert_eq!(report.photos, 3);
    let aside: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("state.db.pre-recover."))
        .collect();
    assert!(!aside.is_empty(), "old db preserved: {aside:?}");
}

// Impact: the sidecar tree is the last full-fidelity source — the rebuild
// must round-trip identity (photo_ids!), tombstones, groups, and the
// photo-to-blob mapping, or disaster recovery quietly degrades.
// Should: reconstruct rows from documents, preserve photo_ids, recount
// blobs, populate the local tree, and verify blob files (reporting misses).
#[tokio::test]
async fn sidecar_rebuild_round_trips_state() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, data_dir) = source_env(tmp.path()).await;
    let ids = populate(&store, &data_dir).await;

    // Reference state before "the Mac dies".
    let mut expected_blobs: Vec<(String, i64)> = Vec::new();
    for row in store.blobs_for_library(&personal).await.unwrap() {
        expected_blobs.push((row.content_hash.to_string(), row.ref_count));
    }
    expected_blobs.sort();

    let new_data = DataDir::new(tmp.path().join("new-data"));
    let spec = RecoverLibrarySpec::parse(&format!(
        "id=personal,blob={},sidecars={},name=Recovered",
        tmp.path().join("blobs").display(),
        tmp.path().join("remote").display(),
    ))
    .unwrap();
    let report = recover(
        &new_data,
        &RecoverOptions {
            libraries: vec![spec],
            from_sidecars: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(matches!(report.source, RecoverSource::Sidecars));
    assert_eq!(report.photos, 3);
    assert_eq!(report.resources, 4); // live(2) + burst(1) + doomed(1)
    assert_eq!(report.missing_blob_files, 0);
    assert_eq!(report.sidecars_hydrated, 3);

    let restored = StateStore::open(&new_data.state_db_path()).await.unwrap();
    for id in &ids {
        assert!(restored.photo(id).await.unwrap().is_some(), "{id} survives");
    }
    // Tombstone survives via the document's deleted_at.
    let doomed = restored
        .photo_by_cloud_id("c-doomed")
        .await
        .unwrap()
        .unwrap();
    assert!(doomed.deleted_at.is_some());
    // Group fields survive.
    let burst = restored
        .photo_by_cloud_id("c-burst")
        .await
        .unwrap()
        .unwrap();
    assert!(burst.group_id.is_some());
    assert_eq!(burst.group_type, Some(0));
    assert!(burst.is_group_pick);
    // Blob recount matches the original refcounts exactly.
    let mut rebuilt: Vec<(String, i64)> = restored
        .blobs_for_library(&personal)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.content_hash.to_string(), r.ref_count))
        .collect();
    rebuilt.sort();
    assert_eq!(rebuilt, expected_blobs);
    // Local tree populated.
    for id in &ids {
        assert!(
            ingress_core::sidecar_io::find_sidecar(&new_data.sidecar_root(&personal), id)
                .unwrap()
                .is_some()
        );
    }

    // Second recovery with a destroyed blob file reports the miss.
    let victim = &expected_blobs[0].0;
    let path = BlobPaths::new(tmp.path().join("blobs"))
        .blob_path(&ContentHash::from_hex(victim.clone()), "bin");
    std::fs::remove_file(&path).unwrap();
    let new_data2 = DataDir::new(tmp.path().join("new-data-2"));
    let spec = RecoverLibrarySpec::parse(&format!(
        "id=personal,blob={},sidecars={}",
        tmp.path().join("blobs").display(),
        tmp.path().join("remote").display(),
    ))
    .unwrap();
    let report = recover(
        &new_data2,
        &RecoverOptions {
            libraries: vec![spec],
            from_sidecars: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(report.missing_blob_files, 1);
}

// Impact: a pre-rename straggler or misfiled document must not steer rows
// into a library the operator didn't ask for — the CLI spec is the
// explicit instruction.
// Should: recover into the spec's library with a warning.
#[tokio::test]
async fn spec_library_id_wins_over_document() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, _personal, data_dir) = source_env(tmp.path()).await;
    populate(&store, &data_dir).await;

    let new_data = DataDir::new(tmp.path().join("new-data"));
    let spec = RecoverLibrarySpec::parse(&format!(
        "id=brave_otter,blob={},sidecars={}",
        tmp.path().join("blobs").display(),
        tmp.path().join("remote").display(),
    ))
    .unwrap();
    let report = recover(
        &new_data,
        &RecoverOptions {
            libraries: vec![spec],
            from_sidecars: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(report.photos, 3);
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("document claims library")),
        "{:?}",
        report.warnings
    );
    let restored = StateStore::open(&new_data.state_db_path()).await.unwrap();
    let stats = restored.library_stats().await.unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].library_id, LibraryId::new("brave_otter"));
    assert_eq!(stats[0].photos_active + stats[0].tombstones, 3);
}

// Impact: the last-resort error is the operator's map of what survived;
// a bare "not found" in a disaster is cruel.
// Should: inventory every candidate root and explain the blob-only
// deferral; reject malformed specs at parse time.
#[tokio::test]
async fn inventory_error_and_spec_parsing() {
    let tmp = tempfile::tempdir().unwrap();
    let empty_root = tmp.path().join("nothing");
    std::fs::create_dir_all(&empty_root).unwrap();

    let new_data = DataDir::new(tmp.path().join("new-data"));
    let err = recover(
        &new_data,
        &RecoverOptions {
            roots: vec![empty_root],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("no usable recovery source"), "{msg}");
    assert!(msg.contains("no snapshots"), "{msg}");
    assert!(
        msg.contains("blob-only recovery is deliberately not implemented"),
        "{msg}"
    );

    // No roots at all.
    assert!(
        recover(&new_data, &RecoverOptions::default())
            .await
            .is_err()
    );

    // Spec parsing.
    assert!(RecoverLibrarySpec::parse("id=ok,blob=/abs").is_ok());
    assert!(
        RecoverLibrarySpec::parse("blob=/abs").is_err(),
        "id required"
    );
    assert!(RecoverLibrarySpec::parse("id=ok").is_err(), "blob required");
    assert!(
        RecoverLibrarySpec::parse("id=ok,blob=rel/path").is_err(),
        "absolute blob"
    );
    assert!(
        RecoverLibrarySpec::parse("id=BAD,blob=/abs").is_err(),
        "charset"
    );
    assert!(
        RecoverLibrarySpec::parse("id=ok,blob=/abs,bogus=1").is_err(),
        "unknown key"
    );
    let full = RecoverLibrarySpec::parse(
        "id=ok,blob=/abs,sidecars=/abs/sidecars,scope=shared,retention=60,name=Nice Name",
    )
    .unwrap();
    assert!(matches!(full.scope, LibraryScope::Shared));
    assert_eq!(full.retention_days, 60);
    assert_eq!(full.display_name.as_deref(), Some("Nice Name"));
}
