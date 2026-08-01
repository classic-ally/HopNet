//! Tombstone / restore flows (spec §Deletion and Retention, §Tombstone,
//! §Restore inside the window) — the Phase 4 slice; hard-delete cleanup is
//! Phase 5. Tombstones are row-state only: the capsule needs no editing
//! (deleted_at is authoritative on the photo row).

use ingress_core::classify::{RemovalOutcome, apply_removal};
use ingress_core::fixtures::{AssetDescriptorBuilder, store_with_personal};
use ingress_core::model::ResourceType;
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::{AssetDescriptor, ContentHash, PhotoId, StateStore};

async fn seed_one(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
    match seed_descriptor(store, desc).await.expect("seed") {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

/// Seed + materialize a simple image and persist its capsule.
async fn materialized_photo(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
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
    store.persist_descriptor(&id, desc).await.unwrap();
    id
}

// Impact: deleting a still-pending photo must not wedge the daemon's event
// loop.
// Should: tombstone a pending (never-materialized) photo cleanly.
#[tokio::test]
async fn tombstone_of_pending_photo_is_clean() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::simple_image().build();
    let id = seed_one(&store, &desc).await; // pending — no capsule persisted

    let outcome = apply_removal(&store, &desc.local_id).await.unwrap();
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
}

// Impact: PhotoKit delivers redundant removed events; double-logging
// deletion_observed would corrupt the forensic record and restart retention.
// Should: tombstone + log deletion_observed once.
// Should not: log again on a redundant delivery, resolve unknown local_ids,
// or touch resource rows, blob refcounts, or the capsule (bytes and
// metadata stay through the retention window).
#[tokio::test]
async fn apply_removal_tombstones_once() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::simple_image().build();
    let id = materialized_photo(&store, &desc).await;

    let first = apply_removal(&store, &desc.local_id).await.unwrap();
    assert_eq!(
        first,
        RemovalOutcome::Tombstoned {
            photo_id: id.clone()
        }
    );
    let second = apply_removal(&store, &desc.local_id).await.unwrap();
    assert_eq!(
        second,
        RemovalOutcome::Unknown,
        "tombstoned row no longer resolves"
    );
    assert_eq!(
        store.log_events("deletion_observed").await.unwrap().len(),
        1
    );

    // Resource rows, refcounts, and the capsule untouched (state stays
    // through the window; a restore needs all of it).
    let photo = store.photo(&id).await.unwrap().unwrap();
    assert!(photo.descriptor_json.is_some());
    let resources = store.resources_for_photo(&id).await.unwrap();
    assert!(resources.iter().all(|r| r.written_at.is_some()));

    let unknown = apply_removal(&store, "NEVER-SEEN/L0/001").await.unwrap();
    assert_eq!(unknown, RemovalOutcome::Unknown);
}
