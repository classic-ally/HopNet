//! Hard-move scenarios (spec §Asset migrating between libraries): bytes
//! physically relocate so a photo's state lives entirely under its current
//! library's subtree — the invariant the server-side ACL story depends on.

use chrono::Utc;
use ingress_core::classify::apply_change;
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig, ResourceType};
use ingress_core::paths::{BlobPaths, DataDir};
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::sidecar_io::find_sidecar;
use ingress_core::transition::execute_transition;
use ingress_core::{
    AssetDescriptor, ContentHash, LibraryId, LibraryScope, PhotoId, Sidecar, StateStore,
};

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

async fn blob_paths(store: &StateStore, lib: &LibraryId) -> BlobPaths {
    BlobPaths::new(&store.library(lib).await.unwrap().unwrap().blob_root)
}

async fn seed_one(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
    match seed_descriptor(store, desc).await.expect("seed") {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

/// Write real bytes + commit one resource.
async fn write_resource(
    store: &StateStore,
    photo_id: &PhotoId,
    rt: ResourceType,
    bytes: &[u8],
) -> ContentHash {
    let library = store
        .photo(photo_id)
        .await
        .unwrap()
        .unwrap()
        .library_id
        .unwrap();
    let paths = blob_paths(store, &library).await;
    let hash = ContentHash::of_bytes(bytes);
    let path = paths.blob_path(&hash, "bin");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    store
        .mark_resource_written(photo_id, rt, &hash, "bin", bytes.len() as i64)
        .await
        .unwrap();
    hash
}

// Impact: "a photo's bytes are always under its current library_id's subtree"
// is the feature — a half-moved photo has an incoherent ACL state.
// Should: relocate bytes, swap refcounts, repoint the photo, move the
// sidecar, and log library_transition — end-to-end through apply_change.
#[tokio::test]
async fn hard_move_relocates_bytes_and_refcounts() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    let hash = write_resource(&store, &id, ResourceType::Original, b"original-bytes").await;
    ingress_core::sidecar_io::write_photo_sidecar(&store, &data_dir, &desc, &id)
        .await
        .unwrap();

    let src_paths = blob_paths(&store, &personal).await;
    let dst_paths = blob_paths(&store, &shared).await;

    let mut moved = desc.clone();
    moved.scope = LibraryScope::Shared;
    let (_, outcome) = apply_change(&store, &data_dir, &moved).await.unwrap();
    assert!(outcome.transitioned);

    assert_eq!(
        store.photo(&id).await.unwrap().unwrap().library_id,
        Some(shared.clone())
    );
    assert!(
        dst_paths.blob_path(&hash, "bin").is_file(),
        "bytes live under dst"
    );
    assert!(
        !src_paths.blob_path(&hash, "bin").is_file(),
        "src file reaped"
    );
    assert!(
        store.blob(&personal, &hash).await.unwrap().is_none(),
        "src blob row reaped"
    );
    assert_eq!(
        store.blob(&shared, &hash).await.unwrap().unwrap().ref_count,
        1
    );
    assert_eq!(
        store.log_events("library_transition").await.unwrap().len(),
        1
    );

    assert!(
        find_sidecar(&data_dir.sidecar_root(&personal), &id)
            .unwrap()
            .is_none()
    );
    let path = find_sidecar(&data_dir.sidecar_root(&shared), &id)
        .unwrap()
        .unwrap();
    let doc = Sidecar::from_json(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(doc.library_id, shared);
}

// Impact: spec step-3 refcount check — the destination already holding the
// bytes (independent import) must not be re-copied or double-counted.
// Should: refcount 2 at dst, no copy performed.
#[tokio::test]
async fn hard_move_shared_dst_blob_skips_copy() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    // A shared-library photo already holds the same bytes.
    let bytes = b"identical-content";
    let shared_desc = AssetDescriptorBuilder::simple_image()
        .scope(LibraryScope::Shared)
        .modified_at(Utc::now())
        .build();
    let shared_photo = seed_one(&store, &shared_desc).await;
    write_resource(&store, &shared_photo, ResourceType::Original, bytes).await;

    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    let hash = write_resource(&store, &id, ResourceType::Original, bytes).await;

    let report = execute_transition(&store, &data_dir, &id, &personal, &shared)
        .await
        .unwrap();
    assert_eq!(report.blobs_shared, 1);
    assert_eq!(report.blobs_copied, 0);
    assert_eq!(
        store.blob(&shared, &hash).await.unwrap().unwrap().ref_count,
        2
    );
    assert_eq!(report.src_files_deleted, 1, "src copy no longer referenced");
}

// Impact: spec step-4 symmetric check — deleting a src file still referenced
// by another photo is byte loss for that photo.
// Should: decrement src to 1 and KEEP the file.
#[tokio::test]
async fn hard_move_shared_src_blob_keeps_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    let bytes = b"shared-within-src";
    let stay_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let stay = seed_one(&store, &stay_desc).await;
    write_resource(&store, &stay, ResourceType::Original, bytes).await;

    let move_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &move_desc).await;
    let hash = write_resource(&store, &id, ResourceType::Original, bytes).await;
    assert_eq!(
        store
            .blob(&personal, &hash)
            .await
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );

    let src_paths = blob_paths(&store, &personal).await;
    let report = execute_transition(&store, &data_dir, &id, &personal, &shared)
        .await
        .unwrap();
    assert_eq!(report.blobs_copied, 1);
    assert_eq!(report.src_files_deleted, 0);
    assert_eq!(
        store
            .blob(&personal, &hash)
            .await
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );
    assert!(
        src_paths.blob_path(&hash, "bin").is_file(),
        "still referenced by `stay`"
    );
    assert!(
        blob_paths(&store, &shared)
            .await
            .blob_path(&hash, "bin")
            .is_file()
    );
}

// Impact: transitions mid-ingest are normal on first sync — pending rows must
// ride along logically and fetch into the destination root later.
// Should: move written blobs only; a later write commits under the dst.
#[tokio::test]
async fn hard_move_with_pending_resources_moves_written_only() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_roots(tmp.path()).await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::live_photo()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    let still = write_resource(&store, &id, ResourceType::Original, b"still-bytes").await;
    // PairedVideo stays pending.

    let report = execute_transition(&store, &data_dir, &id, &personal, &shared)
        .await
        .unwrap();
    assert_eq!(report.blobs_copied, 1);

    let rows = store.resources_for_photo(&id).await.unwrap();
    let paired = rows
        .iter()
        .find(|r| r.resource_type == ResourceType::PairedVideo)
        .unwrap();
    assert!(paired.written_at.is_none(), "pending row untouched");
    assert!(store.blob(&shared, &still).await.unwrap().is_some());

    // The later fetch commits under the destination (mark reads library_id
    // in its own transaction).
    let motion = write_resource(&store, &id, ResourceType::PairedVideo, b"motion-bytes").await;
    assert!(store.blob(&shared, &motion).await.unwrap().is_some());
    assert!(store.blob(&personal, &motion).await.unwrap().is_none());
    assert!(
        blob_paths(&store, &shared)
            .await
            .blob_path(&motion, "bin")
            .is_file()
    );
}
