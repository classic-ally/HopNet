//! Change-classification scenarios (spec §Change classification): the five
//! kinds as seen through `classify`/`apply_change`, against the spike's
//! live-verified resource-set shapes.

use chrono::{Duration, Utc};
use ingress_core::classify::{Classification, apply_change, apply_removal, classify};
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig, ResourceType};
use ingress_core::paths::{BlobPaths, DataDir};
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::sidecar_io::find_sidecar;
use ingress_core::{
    AssetDescriptor, ContentHash, LibraryId, LibraryScope, PhotoId, Sidecar, StateStore,
};

/// Store with personal + shared libraries whose blob roots live under a
/// per-test tempdir — required by flows that delete real blob files.
async fn store_with_roots(tmp: &std::path::Path) -> (StateStore, LibraryId, LibraryId) {
    let store = StateStore::open_in_memory().await.unwrap();
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
                sidecar_root_remote: None,
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

/// Materialize every pending resource with a real blob file on disk, distinct
/// bytes per resource type, then write the sidecar. Sizes are made to match
/// the descriptor's `expected_size` so materialization never reads as a
/// re-edit.
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

// Impact: PhotoKit fires 2–4 near-identical events per action plus spontaneous
// shared-library churn — NoOp is the hot path, and a spurious write per event
// is a churn bug that grinds a 50k-asset library.
// Should: classify an unchanged re-delivery as NoOp with no state change.
#[tokio::test]
async fn unchanged_descriptor_classifies_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, ..) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::live_photo()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;

    let (classification, outcome) = apply_change(&store, &data_dir, &desc).await.unwrap();
    assert_eq!(classification, Classification::NoOp { photo_id: id });
    assert_eq!(outcome, Default::default());
}

// Impact: metadata-only is the favorite/caption path — it must refresh the
// sidecar without moving a single byte, and stamp asset_modified_at so the
// next delivery is NoOp.
// Should: rewrite the sidecar (favorite reflected) then stamp the date.
// Should not: touch resource rows or blobs.
#[tokio::test]
async fn newer_modification_date_is_metadata_only() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let t1 = Utc::now();
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    let mut newer = desc.clone();
    let t2 = t1 + Duration::seconds(5);
    newer.asset_modified_at = Some(t2);
    newer.favorite = true;

    let (classification, outcome) = apply_change(&store, &data_dir, &newer).await.unwrap();
    assert!(matches!(classification, Classification::Known(_)));
    assert!(outcome.metadata_refreshed);
    assert_eq!(
        outcome.resources_added + outcome.resources_reopened + outcome.resources_removed,
        0
    );

    let photo = store.photo(&id).await.unwrap().unwrap();
    assert_eq!(photo.asset_modified_at, Some(t2));
    let path = find_sidecar(&data_dir.sidecar_root(&lib), &id)
        .unwrap()
        .unwrap();
    let doc = Sidecar::from_json(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert!(doc.favorite, "sidecar reflects the refreshed metadata");

    // Third delivery of the same state: back to NoOp.
    let (again, _) = apply_change(&store, &data_dir, &newer).await.unwrap();
    assert_eq!(again, Classification::NoOp { photo_id: id });
}

// Impact: edits are the steady-state byte work — the [1,9] → five-resource
// transition observed live in the spike must re-enter the work queue.
// Should: mint pending rows for the new types and clear materialized_at.
// Should not: touch the already-written original rows.
#[tokio::test]
async fn first_edit_adds_pending_rows_and_clears_materialized() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, ..) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let t1 = Utc::now();
    let desc = AssetDescriptorBuilder::live_photo().modified_at(t1).build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    let mut edited = AssetDescriptorBuilder::edited_live_photo()
        .modified_at(t1 + Duration::seconds(5))
        .build();
    edited.cloud_id = desc.cloud_id.clone();
    edited.local_id = desc.local_id.clone();

    let (_, outcome) = apply_change(&store, &data_dir, &edited).await.unwrap();
    assert_eq!(
        outcome.resources_added, 3,
        "edited, adjustment_data, edited_paired_video"
    );

    let photo = store.photo(&id).await.unwrap().unwrap();
    assert!(
        photo.materialized_at.is_none(),
        "photo re-entered the work queue"
    );
    let rows = store.resources_for_photo(&id).await.unwrap();
    assert_eq!(rows.len(), 5);
    for rt in [ResourceType::Original, ResourceType::PairedVideo] {
        assert!(
            rows.iter()
                .any(|r| r.resource_type == rt && r.written_at.is_some())
        );
    }
    for rt in [
        ResourceType::Edited,
        ResourceType::AdjustmentData,
        ResourceType::EditedPairedVideo,
    ] {
        assert!(
            rows.iter()
                .any(|r| r.resource_type == rt && r.written_at.is_none())
        );
    }
}

// Impact: this is user decision #3's exact contract — fileSize compare is the
// ONLY re-edit trigger; refetching on every metadata bump would re-download
// renders every time someone favorites a photo.
// Should: reopen exactly the size-mismatched written edit-mutable row.
// Should not: reopen on equal sizes, absent sizes, or non-edit-mutable types.
#[tokio::test]
async fn re_edit_reopens_on_filesize_mismatch_only() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, ..) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let t1 = Utc::now();
    let desc = AssetDescriptorBuilder::edited_live_photo()
        .modified_at(t1)
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    // Equal sizes → metadata-only, nothing reopened.
    let mut same = desc.clone();
    same.asset_modified_at = Some(t1 + Duration::seconds(5));
    let (_, outcome) = apply_change(&store, &data_dir, &same).await.unwrap();
    assert_eq!(
        outcome.resources_reopened, 0,
        "equal fileSize is not a re-edit"
    );

    // Absent size → assume unchanged (never reopen on a missing KVC value).
    let mut absent = same.clone();
    absent.asset_modified_at = Some(t1 + Duration::seconds(10));
    for r in &mut absent.resources {
        r.expected_size = None;
    }
    let (_, outcome) = apply_change(&store, &data_dir, &absent).await.unwrap();
    assert_eq!(outcome.resources_reopened, 0);

    // fullSizePhoto (Edited) size differs → that row alone reopens.
    let mut reedit = same.clone();
    reedit.asset_modified_at = Some(t1 + Duration::seconds(15));
    for r in &mut reedit.resources {
        if r.ph_resource_type == 5 {
            r.expected_size = Some(3_333_333);
        }
    }
    let (_, outcome) = apply_change(&store, &data_dir, &reedit).await.unwrap();
    assert_eq!(outcome.resources_reopened, 1);
    let rows = store.resources_for_photo(&id).await.unwrap();
    let edited = rows
        .iter()
        .find(|r| r.resource_type == ResourceType::Edited)
        .unwrap();
    assert!(edited.written_at.is_none(), "reopened for refetch");
    assert!(
        edited.content_hash.is_some(),
        "superseded pointer kept for the swap"
    );
    let original = rows
        .iter()
        .find(|r| r.resource_type == ResourceType::Original)
        .unwrap();
    assert!(
        original.written_at.is_some(),
        "originals are never reopened by size compare"
    );
    assert!(
        store
            .photo(&id)
            .await
            .unwrap()
            .unwrap()
            .materialized_at
            .is_none()
    );
}

// Impact: revert is an irreversible byte delete — decrementing the wrong rows
// or leaving files behind makes fsck's refcount recount diverge.
// Should: delete the edit rows, reap their blobs (rows AND files), rewrite
// the sidecar back to the two-resource shape.
#[tokio::test]
async fn revert_deletes_rows_and_reaps_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let t1 = Utc::now();
    let desc = AssetDescriptorBuilder::edited_live_photo()
        .modified_at(t1)
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    let config = store.library(&lib).await.unwrap().unwrap();
    let paths = BlobPaths::new(&config.blob_root);
    let edited_hash = store
        .resources_for_photo(&id)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.resource_type == ResourceType::Edited)
        .and_then(|r| r.content_hash)
        .unwrap();
    assert!(paths.blob_path(&edited_hash, "bin").is_file());

    let mut reverted = AssetDescriptorBuilder::live_photo()
        .modified_at(t1 + Duration::seconds(5))
        .build();
    reverted.cloud_id = desc.cloud_id.clone();
    reverted.local_id = desc.local_id.clone();

    let (_, outcome) = apply_change(&store, &data_dir, &reverted).await.unwrap();
    assert_eq!(outcome.resources_removed, 3);

    let rows = store.resources_for_photo(&id).await.unwrap();
    assert_eq!(rows.len(), 2, "back to [original, paired_video]");
    assert!(
        store.blob(&lib, &edited_hash).await.unwrap().is_none(),
        "blob row reaped"
    );
    assert!(
        !paths.blob_path(&edited_hash, "bin").is_file(),
        "blob file deleted"
    );

    let path = find_sidecar(&data_dir.sidecar_root(&lib), &id)
        .unwrap()
        .unwrap();
    let doc = Sidecar::from_json(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(
        doc.resources.len(),
        2,
        "sidecar reflects the reverted shape"
    );
}

// Impact: spec's "the original row is never overwritten" is load-bearing for
// recovery — a hostile/buggy diff must not delete original-class rows.
// Should: keep the rows, log original_disappeared.
#[tokio::test]
async fn original_never_removed_by_diff() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, ..) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let t1 = Utc::now();
    let desc = AssetDescriptorBuilder::live_photo().modified_at(t1).build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    // Hostile diff: the descriptor claims only adjustment data exists.
    let mut hostile = desc.clone();
    hostile.asset_modified_at = Some(t1 + Duration::seconds(5));
    hostile.resources = vec![ingress_core::ResourceDescriptor {
        ph_resource_type: 7,
        uti: "com.apple.property-list".into(),
        original_filename: None,
        expected_size: Some(1_000),
        locally_available: Some(true),
    }];

    let (_, outcome) = apply_change(&store, &data_dir, &hostile).await.unwrap();
    assert_eq!(outcome.resources_removed, 0, "original-class rows survive");
    assert_eq!(outcome.resources_added, 1, "the adjustment row still mints");

    let rows = store.resources_for_photo(&id).await.unwrap();
    assert!(
        rows.iter()
            .any(|r| r.resource_type == ResourceType::Original && r.written_at.is_some())
    );
    assert!(
        rows.iter()
            .any(|r| r.resource_type == ResourceType::PairedVideo && r.written_at.is_some())
    );
    assert_eq!(
        store
            .log_events("original_disappeared")
            .await
            .unwrap()
            .len(),
        1
    );
}

// Impact: the spike's trap — restore-from-Recently-Deleted arrives as an
// INSERT with the same identity; a naive insert handler would duplicate the
// photo instead of clearing the tombstone.
// Should: clear deleted_at, log restore_observed, clear the sidecar's
// deleted_at via recomposition.
#[tokio::test]
async fn tombstoned_photo_redelivered_classifies_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, lib, _) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    materialize_all(&store, &data_dir, &desc, &id).await;

    apply_removal(&store, &data_dir, &desc.local_id)
        .await
        .unwrap();
    assert!(
        store
            .photo(&id)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_some()
    );

    let (_, outcome) = apply_change(&store, &data_dir, &desc).await.unwrap();
    assert!(outcome.restored);
    assert!(
        store
            .photo(&id)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_none()
    );
    assert_eq!(store.log_events("restore_observed").await.unwrap().len(), 1);
    assert_eq!(
        store.count_photos().await.unwrap(),
        1,
        "no duplicate row minted"
    );

    let path = find_sidecar(&data_dir.sidecar_root(&lib), &id)
        .unwrap()
        .unwrap();
    let doc = Sidecar::from_json(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert!(
        doc.deleted_at.is_none(),
        "sidecar recomposed without the tombstone"
    );
}

// Impact: move-to-Shared-Library arrives as a `changed` event with a flipped
// scope (spike) — classification must plan the hard move, not a new photo.
// Should: plan transition (src, dst) for a known cloud_id under a new scope.
#[tokio::test]
async fn scope_flip_plans_transition() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_roots(tmp.path()).await;
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;

    let mut moved = desc.clone();
    moved.scope = LibraryScope::Shared;

    match classify(&store, &moved).await.unwrap() {
        Classification::Known(plan) => {
            assert_eq!(plan.photo_id, id);
            assert_eq!(plan.transition, Some((personal, shared)));
        }
        other => panic!("expected Known(plan), got {other:?}"),
    }
}
