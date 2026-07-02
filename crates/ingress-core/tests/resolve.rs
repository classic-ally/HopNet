//! Match-precedence engine scenarios (spec §Asset Identity Model rules 1,
//! 2a–2c, plus the edge-case table).

use chrono::{Duration, Utc};
use ingress_core::fixtures::{add_shared, store_with_personal, AssetDescriptorBuilder};
use ingress_core::model::ResourceType;
use ingress_core::resolve::{diff_resources, resolve_descriptor, resolve_with_hash};
use ingress_core::{ContentHash, HashResolution, IngressError, LibraryScope, PhotoId, Resolution};

async fn ingest_new(
    store: &ingress_core::StateStore,
    desc: &ingress_core::AssetDescriptor,
    hash: &ContentHash,
) -> PhotoId {
    match resolve_descriptor(store, desc).await.expect("resolve") {
        Resolution::NeedsContentHash => {}
        other => panic!("expected NeedsContentHash, got {other:?}"),
    }
    match resolve_with_hash(store, desc, hash).await.expect("resolve with hash") {
        HashResolution::NewPhoto { photo_id } => photo_id,
        other => panic!("expected NewPhoto, got {other:?}"),
    }
}

/// Ingest AND materialize the original — hash-based matching (2a/2b) can only
/// see photos whose original bytes were durably archived (two-state rule).
async fn ingest_written(
    store: &ingress_core::StateStore,
    desc: &ingress_core::AssetDescriptor,
    hash: &ContentHash,
) -> PhotoId {
    let photo_id = ingest_new(store, desc, hash).await;
    store
        .mark_resource_written(&photo_id, ResourceType::Original, hash, "heic", 2_000_000)
        .await
        .expect("materialize original");
    photo_id
}

// Impact: idempotency here is what makes PhotoKit's redundant observer events
// (2–4 near-identical per action, per the spike) safe to feed straight in.
// Should: resolve a known cloud_id to the same photo without downloading.
// Should not: create additional rows on re-delivery of an identical descriptor.
#[tokio::test]
async fn rule1_steady_state_is_idempotent() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::simple_image().build();
    let photo_id = ingest_new(&store, &desc, &ContentHash::of_bytes(b"img-1")).await;

    let second = resolve_descriptor(&store, &desc).await.unwrap();
    match second {
        Resolution::KnownByCloudId { photo_id: found, scope_changed, .. } => {
            assert_eq!(found, photo_id);
            assert!(!scope_changed);
        }
        other => panic!("expected KnownByCloudId, got {other:?}"),
    }
    assert_eq!(store.count_photos().await.unwrap(), 1);
}

// Should: refresh a changed local identifier on the existing photo row.
#[tokio::test]
async fn rule1_refreshes_local_id() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::simple_image().with_local_id("OLD/L0/001").build();
    let photo_id = ingest_new(&store, &desc, &ContentHash::of_bytes(b"img-2")).await;

    let mut redelivered = desc.clone();
    redelivered.local_id = "NEW/L0/001".to_string();
    resolve_descriptor(&store, &redelivered).await.unwrap();

    let record = store.photo(&photo_id).await.unwrap().unwrap();
    assert_eq!(record.local_id.as_deref(), Some("NEW/L0/001"));
}

// Should: report metadata_changed when the incoming modification date is newer.
// Should: report metadata_changed when no modification date was ever stored.
#[tokio::test]
async fn rule1_detects_metadata_change() {
    let (store, _) = store_with_personal().await;
    let t1 = Utc::now();
    let desc = AssetDescriptorBuilder::simple_image().modified_at(t1).build();
    ingest_new(&store, &desc, &ContentHash::of_bytes(b"img-3")).await;

    let same = resolve_descriptor(&store, &desc).await.unwrap();
    assert!(matches!(same, Resolution::KnownByCloudId { metadata_changed: false, .. }));

    let mut newer = desc.clone();
    newer.asset_modified_at = Some(t1 + Duration::seconds(5));
    let changed = resolve_descriptor(&store, &newer).await.unwrap();
    assert!(matches!(changed, Resolution::KnownByCloudId { metadata_changed: true, .. }));

    // Never-synced photo (stored NULL) must always refresh.
    let no_date = AssetDescriptorBuilder::simple_image().build();
    ingest_new(&store, &no_date, &ContentHash::of_bytes(b"img-4")).await;
    let redelivered = resolve_descriptor(&store, &no_date).await.unwrap();
    assert!(matches!(redelivered, Resolution::KnownByCloudId { metadata_changed: true, .. }));
}

// Impact: late binding is what keeps a photo's identity stable across the
// local-only → iCloud-uploaded transition; failure here duplicates photos.
// Should: link a new cloud_id onto the existing photo when hashes match.
// Should not: mint a second photo row for the same bytes.
#[tokio::test]
async fn rule2a_late_binding() {
    let (store, _) = store_with_personal().await;
    let hash = ContentHash::of_bytes(b"local-then-cloud");

    // Day 1: local-only, fully materialized.
    let day1 = AssetDescriptorBuilder::simple_image().local_only().build();
    let photo_id = ingest_written(&store, &day1, &hash).await;

    // Day 2: iCloud upload completed; same bytes, new cloud_id.
    let day2 = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("CLOUD-LATE-1:001")
        .with_local_id(&day1.local_id)
        .build();
    assert!(matches!(
        resolve_descriptor(&store, &day2).await.unwrap(),
        Resolution::NeedsContentHash
    ));
    match resolve_with_hash(&store, &day2, &hash).await.unwrap() {
        HashResolution::LateBound { photo_id: bound } => assert_eq!(bound, photo_id),
        other => panic!("expected LateBound, got {other:?}"),
    }

    let record = store.photo(&photo_id).await.unwrap().unwrap();
    assert_eq!(record.cloud_id.as_deref(), Some("CLOUD-LATE-1:001"));
    assert_eq!(store.count_photos().await.unwrap(), 1);
}

// Impact: guards the delete-then-re-add edge case — PhotoKit issues a fresh
// cloud_id for re-imported bytes and the daemon must mirror that as a new
// logical photo, not a restore.
// Should: mint a distinct photo for byte-identical content under a different cloud_id.
// Should: report the shared hash so the write path can reuse the blob.
#[tokio::test]
async fn rule2b_new_photo_shared_blob() {
    let (store, _) = store_with_personal().await;
    let hash = ContentHash::of_bytes(b"same-bytes");

    let first = AssetDescriptorBuilder::simple_image().with_cloud_id("CLOUD-A:001").build();
    let first_id = ingest_written(&store, &first, &hash).await;

    let second = AssetDescriptorBuilder::simple_image().with_cloud_id("CLOUD-B:001").build();
    assert!(matches!(
        resolve_descriptor(&store, &second).await.unwrap(),
        Resolution::NeedsContentHash
    ));
    match resolve_with_hash(&store, &second, &hash).await.unwrap() {
        HashResolution::NewPhotoSharedBlob { photo_id, shared_hash } => {
            assert_ne!(photo_id, first_id);
            assert_eq!(shared_hash, hash);
        }
        other => panic!("expected NewPhotoSharedBlob, got {other:?}"),
    }
    assert_eq!(store.count_photos().await.unwrap(), 2);
}

// Should: mint a photo with all pipeline-state columns NULL (discovered, not materialized).
#[tokio::test]
async fn rule2c_new_photo_is_pending() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::live_photo().build();
    let photo_id = ingest_new(&store, &desc, &ContentHash::of_bytes(b"new")).await;

    let record = store.photo(&photo_id).await.unwrap().unwrap();
    assert!(record.materialized_at.is_none());
    for r in store.resources_for_photo(&photo_id).await.unwrap() {
        assert!(r.content_hash.is_none());
        assert!(r.written_at.is_none());
    }
}

// Impact: the per-library dedup namespace is an access-control boundary
// (spec §Dedup namespace per library) — cross-library hash hits must NOT
// merge records across ACL domains.
// Should: treat a hash known in another library as a brand-new photo.
#[tokio::test]
async fn hash_lookup_is_scoped_per_library() {
    let (store, _) = store_with_personal().await;
    add_shared(&store).await;
    let hash = ContentHash::of_bytes(b"cross-library-bytes");

    let personal = AssetDescriptorBuilder::simple_image().local_only().build();
    ingest_written(&store, &personal, &hash).await;

    let shared = AssetDescriptorBuilder::simple_image()
        .local_only()
        .scope(LibraryScope::Shared)
        .build();
    assert!(matches!(
        resolve_descriptor(&store, &shared).await.unwrap(),
        Resolution::NeedsContentHash
    ));
    // Same bytes, different library: 2c, not 2a late-binding.
    assert!(matches!(
        resolve_with_hash(&store, &shared, &hash).await.unwrap(),
        HashResolution::NewPhoto { .. }
    ));
}

// Should: record a NULL-library photo row for an unbound scope so discovery is never lost.
// Should: emit a scope_unmapped ingest-log event.
#[tokio::test]
async fn unmapped_scope_blocks_ingest() {
    let (store, _) = store_with_personal().await; // no shared library bound
    let desc = AssetDescriptorBuilder::simple_image().scope(LibraryScope::Shared).build();

    let photo_id = match resolve_descriptor(&store, &desc).await.unwrap() {
        Resolution::UnmappedScope { photo_id } => photo_id,
        other => panic!("expected UnmappedScope, got {other:?}"),
    };
    let record = store.photo(&photo_id).await.unwrap().unwrap();
    assert!(record.library_id.is_none());
    assert_eq!(store.log_events("scope_unmapped").await.unwrap().len(), 1);
}

// Impact: scope-change detection is the trigger for the hard-move procedure;
// missing it would strand bytes under the wrong ACL subtree.
// Should: flag a known cloud_id arriving with a different library scope.
// Should not: move anything (hard move is a later phase).
#[tokio::test]
async fn scope_change_detected_not_applied() {
    let (store, _) = store_with_personal().await;
    add_shared(&store).await;

    let desc = AssetDescriptorBuilder::simple_image().build(); // personal
    let photo_id = ingest_new(&store, &desc, &ContentHash::of_bytes(b"mover")).await;

    let mut moved = desc.clone();
    moved.scope = LibraryScope::Shared;
    match resolve_descriptor(&store, &moved).await.unwrap() {
        Resolution::KnownByCloudId { scope_changed, .. } => assert!(scope_changed),
        other => panic!("expected KnownByCloudId, got {other:?}"),
    }
    // Library unchanged until the hard move runs.
    let record = store.photo(&photo_id).await.unwrap().unwrap();
    assert_eq!(record.library_id.unwrap().as_str(), "personal");
}

// Should: report exactly the edited renders and adjustment data as added when
// a Live Photo gains edits, leaving original resources untouched.
#[tokio::test]
async fn edit_diff_adds_edited_renders() {
    let (store, _) = store_with_personal().await;
    let plain = AssetDescriptorBuilder::live_photo().build();
    let photo_id = ingest_new(&store, &plain, &ContentHash::of_bytes(b"lp")).await;

    let stored = store.resources_for_photo(&photo_id).await.unwrap();
    let incoming: Vec<ResourceType> = AssetDescriptorBuilder::edited_live_photo()
        .build()
        .resources
        .iter()
        .filter_map(|r| ResourceType::from_ph_type(r.ph_resource_type))
        .collect();

    let diff = diff_resources(&stored, &incoming);
    assert_eq!(
        diff.added.into_iter().collect::<Vec<_>>(),
        vec![ResourceType::Edited, ResourceType::AdjustmentData, ResourceType::EditedPairedVideo]
    );
    assert!(diff.removed.is_empty());
    assert_eq!(
        diff.unchanged.into_iter().collect::<Vec<_>>(),
        vec![ResourceType::Original, ResourceType::PairedVideo]
    );
}

// Impact: group_id convergence across devices without coordination depends on
// deterministic derivation from the shared burstIdentifier.
// Should: give burst frames a common derived group_id with exactly one pick.
#[tokio::test]
async fn burst_frames_share_group_id() {
    let (store, _) = store_with_personal().await;
    let burst_id = "E5A6CEB6-B839-45AD-A028-D625CD72470D";

    let pick = AssetDescriptorBuilder::burst_frame(burst_id, true).build();
    let other = AssetDescriptorBuilder::burst_frame(burst_id, false).build();
    let pick_id = ingest_new(&store, &pick, &ContentHash::of_bytes(b"frame-1")).await;
    let other_id = ingest_new(&store, &other, &ContentHash::of_bytes(b"frame-2")).await;

    let a = store.photo(&pick_id).await.unwrap().unwrap();
    let b = store.photo(&other_id).await.unwrap().unwrap();
    assert_eq!(a.group_id, b.group_id);
    assert!(a.group_id.is_some());
    assert_eq!(a.group_type, Some(0));
    assert!(a.is_group_pick);
    assert!(!b.is_group_pick);
}

// Should: mint one photo with original + paired_video rows for a Live Photo.
#[tokio::test]
async fn live_photo_resource_rows() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::live_photo().build();
    let photo_id = ingest_new(&store, &desc, &ContentHash::of_bytes(b"lp-rows")).await;

    let types: Vec<ResourceType> = store
        .resources_for_photo(&photo_id)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.resource_type)
        .collect();
    assert_eq!(types, vec![ResourceType::Original, ResourceType::PairedVideo]);
}

// Should: mint one photo with original + raw_alternate rows for a RAW+JPEG pair.
#[tokio::test]
async fn raw_jpeg_resource_rows() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::raw_jpeg().build();
    let photo_id = ingest_new(&store, &desc, &ContentHash::of_bytes(b"raw-rows")).await;

    let types: Vec<ResourceType> = store
        .resources_for_photo(&photo_id)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.resource_type)
        .collect();
    assert_eq!(types, vec![ResourceType::Original, ResourceType::RawAlternate]);
}

// Impact: guards the archive-known-and-log decision — an exotic resource must
// cost visibility, never the photo's archive.
// Should: archive recognized resources and log the unrecognized type.
// Should not: block the asset over an unknown PHAssetResourceType.
#[tokio::test]
async fn unknown_resource_type_logged_not_blocking() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::simple_image()
        .with_ph_resource(8, "public.heic") // adjustmentBasePhoto: unmapped
        .build();
    let photo_id = ingest_new(&store, &desc, &ContentHash::of_bytes(b"exotic")).await;

    let types: Vec<ResourceType> = store
        .resources_for_photo(&photo_id)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.resource_type)
        .collect();
    assert_eq!(types, vec![ResourceType::Original]);

    let events = store.log_events("unknown_resource_type").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].photo_id.as_ref(), Some(&photo_id));
}

// Impact: cloud_id uniqueness backs the whole identity model; a violation
// means Apple's invariant broke or local state diverged, and it must surface
// loudly instead of silently corrupting records.
// Should: surface a duplicate cloud_id insert as CloudIdConflict.
#[tokio::test]
async fn duplicate_cloud_id_fails_loud() {
    let (store, _) = store_with_personal().await;
    let desc = AssetDescriptorBuilder::simple_image().with_cloud_id("CLOUD-DUP:001").build();
    ingest_new(&store, &desc, &ContentHash::of_bytes(b"dup-1")).await;

    // Bypass rule 1 (as a diverged-state caller would) and try to mint the
    // same cloud_id with different bytes.
    let clash = AssetDescriptorBuilder::simple_image().with_cloud_id("CLOUD-DUP:001").build();
    let err = resolve_with_hash(&store, &clash, &ContentHash::of_bytes(b"dup-2"))
        .await
        .unwrap_err();
    assert!(matches!(err, IngressError::CloudIdConflict(id) if id == "CLOUD-DUP:001"));
}
