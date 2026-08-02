//! CLI `status` views (spec §Phase 6): library counters, pipeline posture,
//! and the per-photo lookup.

use chrono::{Duration, Utc};
use ingress_core::classify::apply_removal;
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig, ResourceType};
use ingress_core::paths::DataDir;
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::status::{photo_status, status};
use ingress_core::{AssetDescriptor, ContentHash, LibraryId, LibraryScope, PhotoId, StateStore};

/// File-backed store with a personal library only — Shared-scope seeds land
/// unmapped, exercising the pipeline's unmapped counter.
async fn store_personal_only(tmp: &std::path::Path) -> (StateStore, LibraryId) {
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
    (store, personal)
}

async fn add_shared_library(store: &StateStore, _tmp: &std::path::Path) -> LibraryId {
    let shared = LibraryId::new("shared_household");
    store
        .insert_library(&LibraryConfig {
            library_id: shared.clone(),
            display_name: "Shared".into(),
            scope_binding: Some(ICLOUD_SHARED_LIBRARY_BINDING.to_string()),
            retention_days: 30,
            created_at: Utc::now(),
            mesh_library_id: None,
        })
        .await
        .unwrap();
    shared
}

async fn seed_one(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
    match seed_descriptor(store, desc).await.expect("seed") {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

/// Materialize every pending resource with real spool bytes + persist the
/// capsule.
async fn materialize_all(
    store: &StateStore,
    data_dir: &DataDir,
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
    store.persist_descriptor(photo_id, desc)
        .await
        .unwrap();
}

// Impact: the library table is the operator's first answer to "is the
// archive healthy" — wrong counters hide stuck pipelines or unreplicated
// tombstones.
// Should: split active/tombstoned/pending/dirty per library and count blobs.
// Should not: fold unmapped photos into any library's counts.
#[tokio::test]
async fn library_stats_reflect_mixed_population() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal) = store_personal_only(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let shared = add_shared_library(&store, tmp.path()).await;

    // Personal: one materialized-active, one materialized-tombstoned, one pending.
    let done = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("cloud-done")
        .build();
    let done_id = seed_one(&store, &done).await;
    materialize_all(&store, &data_dir, &done, &done_id).await;

    let doomed = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("cloud-doomed")
        .with_local_id("local-doomed")
        .build();
    let doomed_id = seed_one(&store, &doomed).await;
    materialize_all(&store, &data_dir, &doomed, &doomed_id).await;
    apply_removal(&store, "local-doomed")
        .await
        .unwrap();

    let pending = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("cloud-pending")
        .build();
    seed_one(&store, &pending).await;

    // Shared: one materialized.
    let shared_desc = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("cloud-shared")
        .scope(LibraryScope::Shared)
        .build();
    let shared_id = seed_one(&store, &shared_desc).await;
    materialize_all(&store, &data_dir, &shared_desc, &shared_id).await;

    let report = status(&store, 5).await.unwrap();
    assert_eq!(report.libraries.len(), 2);
    let by_id = |id: &LibraryId| {
        report
            .libraries
            .iter()
            .find(|l| &l.config.library_id == id)
            .unwrap()
    };
    let p = by_id(&personal);
    assert_eq!(p.stats.photos_active, 2); // done + pending (doomed is tombstoned)
    assert_eq!(p.stats.tombstones, 1);
    assert_eq!(p.stats.photos_pending, 1);
    assert_eq!(p.stats.blob_count, 2); // done + doomed originals (pending unwritten)
    assert!(p.stats.blob_bytes > 0);
    let s = by_id(&shared);
    assert_eq!(s.stats.photos_active, 1);
    assert_eq!(s.stats.tombstones, 0);
    assert_eq!(s.stats.blob_count, 1);
}

// Impact: the pipeline view is how an operator distinguishes "working
// through backlog" from "stuck on retries" from "waiting on me to bind a
// scope" — misclassification points debugging at the wrong layer.
// Should: split fresh work vs awaiting-retry vs gave-up at the cap, count
// unmapped photos, and surface the earliest retry deadline.
#[tokio::test]
async fn pipeline_view_splits_work_states() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, _personal) = store_personal_only(tmp.path()).await;
    let _data_dir = DataDir::new(tmp.path().join("data"));

    // Fresh pending resource.
    let fresh = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("cloud-fresh")
        .build();
    seed_one(&store, &fresh).await;

    // Awaiting retry (1 failure < cap).
    let retrying = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("cloud-retry")
        .build();
    let retry_id = seed_one(&store, &retrying).await;
    let deadline = Utc::now() + Duration::minutes(5);
    store
        .record_resource_failure(&retry_id, ResourceType::Original, "boom", deadline, 5)
        .await
        .unwrap();

    // Gave up (failures reach the cap of 2).
    let dead = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("cloud-dead")
        .build();
    let dead_id = seed_one(&store, &dead).await;
    for _ in 0..2 {
        store
            .record_resource_failure(&dead_id, ResourceType::Original, "boom", deadline, 2)
            .await
            .unwrap();
    }

    // Unmapped: shared scope with no shared library configured.
    let unmapped = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("cloud-unmapped")
        .scope(LibraryScope::Shared)
        .build();
    match seed_descriptor(&store, &unmapped).await.unwrap() {
        SeedOutcome::Unmapped { .. } => {}
        other => panic!("expected Unmapped, got {other:?}"),
    }

    let report = status(&store, 2).await.unwrap();
    assert_eq!(report.pipeline.unmapped_photos, 1);
    assert_eq!(report.pipeline.resources_pending, 1); // fresh only
    assert_eq!(report.pipeline.retries.awaiting_retry, 1);
    assert_eq!(report.pipeline.retries.gave_up, 1);
    assert!(report.pipeline.retries.earliest_next_retry_at.is_some());
}

// Impact: the per-photo view is the operator's drill-down when one photo
// misbehaves; a broken lookup or wrong blob path turns triage into guesswork.
// Should: resolve by photo_id AND cloud_id, reconstruct existing blob paths,
// report the capsule as present, and tail this photo's log newest-first.
// Should not: match garbage keys.
#[tokio::test]
async fn photo_view_resolves_both_keys_with_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, _personal) = store_personal_only(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    let desc = AssetDescriptorBuilder::live_photo()
        .with_cloud_id("cloud-live")
        .with_local_id("local-live")
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;
    // Tombstone to generate a photo-scoped log event.
    apply_removal(&store, "local-live")
        .await
        .unwrap();

    for key in [id.as_str(), "cloud-live"] {
        let view = photo_status(&store, &data_dir.spool(), key)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("lookup by {key:?} missed"));
        assert_eq!(view.photo.photo_id, id);
        assert!(view.photo.deleted_at.is_some());
        assert_eq!(view.resources.len(), 2); // original + paired video
        for res in &view.resources {
            let path = res.blob_path.as_ref().expect("written resource has path");
            assert!(path.to_string_lossy().contains("spool"));
            assert_eq!(res.blob_exists, Some(true));
        }
        assert!(view.photo.descriptor_json.is_some(), "capsule present");
        assert!(
            view.events
                .iter()
                .any(|e| e.event_type == "deletion_observed"),
            "log tail carries the tombstone event"
        );
    }

    assert!(
        photo_status(&store, &data_dir.spool(), "no-such-key")
            .await
            .unwrap()
            .is_none()
    );
}
