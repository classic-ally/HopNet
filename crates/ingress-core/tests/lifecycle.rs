//! Tombstone / restore / sidecar-editing flows (spec §Deletion and Retention,
//! §Tombstone, §Restore inside the window) — the Phase 4 slice; hard-delete
//! cleanup is Phase 5.

use chrono::Utc;
use ingress_core::classify::{RemovalOutcome, apply_removal};
use ingress_core::fixtures::{AssetDescriptorBuilder, store_with_personal};
use ingress_core::model::ResourceType;
use ingress_core::paths::DataDir;
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::sidecar_io::{
    edit_sidecar_deleted_at, find_sidecar, move_sidecar, write_photo_sidecar,
};
use ingress_core::{AssetDescriptor, ContentHash, LibraryId, PhotoId, Sidecar, StateStore};

async fn seed_one(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
    match seed_descriptor(store, desc).await.expect("seed") {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

/// Seed + materialize a simple image and write its sidecar under `data_dir`.
async fn materialized_photo(
    store: &StateStore,
    data_dir: &DataDir,
    desc: &AssetDescriptor,
) -> PhotoId {
    let id = seed_one(store, desc).await;
    store
        .mark_resource_written(
            &id,
            ResourceType::Original,
            &ContentHash::of_bytes(b"orig"),
            "heic",
            10,
        )
        .await
        .unwrap();
    write_photo_sidecar(store, data_dir, desc, &id)
        .await
        .unwrap();
    id
}

// Impact: the sidecar path is keyed on captured_at, which state.db does not
// persist — without the walk, tombstone and hard-move flows cannot locate the
// document they must edit.
// Should: find a written sidecar by photo_id alone; None for unknown ids and
// missing roots.
#[tokio::test]
async fn find_sidecar_walks_year_month_dirs() {
    let (store, lib) = store_with_personal().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = DataDir::new(tmp.path());
    let desc = AssetDescriptorBuilder::simple_image().build();
    let id = materialized_photo(&store, &data_dir, &desc).await;

    let root = data_dir.sidecar_root(&lib);
    let found = find_sidecar(&root, &id).unwrap().expect("sidecar located");
    assert!(found.ends_with(format!("{id}.json")));

    let ghost: PhotoId = PhotoId::mint();
    assert!(find_sidecar(&root, &ghost).unwrap().is_none());
    assert!(
        find_sidecar(&root.join("nonexistent"), &id)
            .unwrap()
            .is_none()
    );
}

// Impact: disaster recovery reads deleted_at from sidecars; a dropped or
// mangled field on the rewrite is silent metadata loss.
// Should: set and clear deleted_at while round-tripping every other field.
#[tokio::test]
async fn edit_sidecar_deleted_at_round_trips() {
    let (store, lib) = store_with_personal().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = DataDir::new(tmp.path());
    let desc = AssetDescriptorBuilder::simple_image().build();
    let id = materialized_photo(&store, &data_dir, &desc).await;

    let path = find_sidecar(&data_dir.sidecar_root(&lib), &id)
        .unwrap()
        .unwrap();
    let before = Sidecar::from_json(&std::fs::read_to_string(&path).unwrap()).unwrap();

    let at = Utc::now();
    edit_sidecar_deleted_at(&data_dir, &lib, &id, Some(at))
        .unwrap()
        .expect("edited");
    let deleted = Sidecar::from_json(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(deleted.deleted_at, Some(at));

    edit_sidecar_deleted_at(&data_dir, &lib, &id, None)
        .unwrap()
        .expect("edited");
    let restored = Sidecar::from_json(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        restored, before,
        "every non-deleted_at field survives the rewrite cycle"
    );
}

// Impact: deleting a still-pending photo must not wedge the daemon's event
// loop on a missing file.
// Should not: error when the photo never materialized a sidecar.
#[tokio::test]
async fn tombstone_without_sidecar_is_silent() {
    let (store, lib) = store_with_personal().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = DataDir::new(tmp.path());
    let desc = AssetDescriptorBuilder::simple_image().build();
    let id = seed_one(&store, &desc).await; // pending — no sidecar written

    let outcome = apply_removal(&store, &data_dir, &desc.local_id)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RemovalOutcome::Tombstoned {
            photo_id: id.clone()
        }
    );
    assert!(
        store
            .photo(&id)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_some()
    );
    assert!(
        find_sidecar(&data_dir.sidecar_root(&lib), &id)
            .unwrap()
            .is_none()
    );
}

// Impact: PhotoKit delivers redundant removed events; double-logging
// deletion_observed would corrupt the forensic record and restart retention.
// Should: tombstone + log deletion_observed + set sidecar deleted_at once.
// Should not: log again on a redundant delivery, or resolve unknown local_ids.
#[tokio::test]
async fn apply_removal_tombstones_once_and_edits_sidecar() {
    let (store, lib) = store_with_personal().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = DataDir::new(tmp.path());
    let desc = AssetDescriptorBuilder::simple_image().build();
    let id = materialized_photo(&store, &data_dir, &desc).await;

    let first = apply_removal(&store, &data_dir, &desc.local_id)
        .await
        .unwrap();
    assert_eq!(
        first,
        RemovalOutcome::Tombstoned {
            photo_id: id.clone()
        }
    );
    let second = apply_removal(&store, &data_dir, &desc.local_id)
        .await
        .unwrap();
    assert_eq!(
        second,
        RemovalOutcome::Unknown,
        "tombstoned row no longer resolves"
    );
    assert_eq!(
        store.log_events("deletion_observed").await.unwrap().len(),
        1
    );

    let path = find_sidecar(&data_dir.sidecar_root(&lib), &id)
        .unwrap()
        .unwrap();
    let doc = Sidecar::from_json(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert!(doc.deleted_at.is_some());

    // Resource rows and blob refcounts untouched (bytes stay through the window).
    let resources = store.resources_for_photo(&id).await.unwrap();
    assert!(resources.iter().all(|r| r.written_at.is_some()));

    let unknown = apply_removal(&store, &data_dir, "NEVER-SEEN/L0/001")
        .await
        .unwrap();
    assert_eq!(unknown, RemovalOutcome::Unknown);
}

// Impact: recovery rebuilds per-library from sidecar trees — a stale source
// copy after a hard move resurrects the photo in the wrong library.
// Should: rewrite under the destination root with the destination library_id
// and remove the source document.
#[tokio::test]
async fn move_sidecar_relocates_between_library_roots() {
    let (store, lib) = store_with_personal().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = DataDir::new(tmp.path());
    let desc = AssetDescriptorBuilder::simple_image().build();
    let id = materialized_photo(&store, &data_dir, &desc).await;

    let dst = LibraryId::new("shared_household");
    let new_path = move_sidecar(&data_dir, &id, &lib, &dst)
        .unwrap()
        .expect("moved");
    assert!(
        find_sidecar(&data_dir.sidecar_root(&lib), &id)
            .unwrap()
            .is_none()
    );
    let doc = Sidecar::from_json(&std::fs::read_to_string(&new_path).unwrap()).unwrap();
    assert_eq!(doc.library_id, dst);

    // No sidecar (unmaterialized photo) → None, silently.
    let ghost = PhotoId::mint();
    assert!(
        move_sidecar(&data_dir, &ghost, &lib, &dst)
            .unwrap()
            .is_none()
    );
}
