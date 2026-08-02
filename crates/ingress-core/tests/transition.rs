//! Hard-move scenarios (spec §Asset migrating between libraries). With the
//! process-global spool a hard move is ledger-only: refcounts transfer
//! between the libraries' `blobs` rows and the photo row repoints — the
//! spool file never moves.

use chrono::Utc;
use ingress_core::classify::apply_change;
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig, ResourceType};
use ingress_core::paths::DataDir;
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::transition::execute_transition;
use ingress_core::{AssetDescriptor, ContentHash, LibraryId, LibraryScope, PhotoId, StateStore};

async fn store_with_libs() -> (StateStore, LibraryId, LibraryId) {
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
                scope_binding: binding,
                retention_days: 30,
                created_at: Utc::now(),
                mesh_library_id: None,
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

/// Write real spool bytes + commit one resource.
async fn write_resource(
    store: &StateStore,
    data_dir: &DataDir,
    photo_id: &PhotoId,
    rt: ResourceType,
    bytes: &[u8],
) -> ContentHash {
    let hash = ContentHash::of_bytes(bytes);
    let path = data_dir.spool().blob_path(&hash, "bin");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    store
        .mark_resource_written(photo_id, rt, &hash, "bin", bytes.len() as i64)
        .await
        .unwrap();
    hash
}

// Impact: a photo's ACL story is its library_id — a half-moved photo would
// answer personal queries with shared-scope content or vice versa.
// Should: transfer refcounts src -> dst, repoint the photo, and log
// library_transition — end-to-end through apply_change. The spool file
// never moves; the capsule rides the photo row untouched.
#[tokio::test]
async fn hard_move_transfers_refcounts_ledger_only() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_libs().await;
    let data_dir = DataDir::new(tmp.path().join("data"));
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    let hash = write_resource(&store, &data_dir, &id, ResourceType::Original, b"orig").await;
    store.persist_descriptor(&id, &desc).await.unwrap();
    let file = data_dir.spool().blob_path(&hash, "bin");

    let mut moved = desc.clone();
    moved.scope = LibraryScope::Shared;
    let (_, outcome) = apply_change(&store, &data_dir.spool(), &moved)
        .await
        .unwrap();
    assert!(outcome.transitioned);

    assert_eq!(
        store.photo(&id).await.unwrap().unwrap().library_id,
        Some(shared.clone())
    );
    assert!(file.is_file(), "spool file untouched by the move");
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
        store
            .photo(&id)
            .await
            .unwrap()
            .unwrap()
            .descriptor_json
            .is_some(),
        "capsule untouched by the move"
    );
}

// Impact: spec step-3 refcount check — the destination already holding the
// hash (independent import) must not be double-counted.
// Should: dst refcount 2 after the move; src row reaped.
#[tokio::test]
async fn hard_move_merges_into_existing_dst_row() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_libs().await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    let bytes = b"identical-content";
    let shared_desc = AssetDescriptorBuilder::simple_image()
        .scope(LibraryScope::Shared)
        .modified_at(Utc::now())
        .build();
    let shared_photo = seed_one(&store, &shared_desc).await;
    write_resource(&store, &data_dir, &shared_photo, ResourceType::Original, bytes).await;

    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    let hash = write_resource(&store, &data_dir, &id, ResourceType::Original, bytes).await;

    let report = execute_transition(&store, &id, &personal, &shared)
        .await
        .unwrap();
    assert_eq!(report.blobs_transferred, 1);
    assert_eq!(
        store.blob(&shared, &hash).await.unwrap().unwrap().ref_count,
        2
    );
    assert!(store.blob(&personal, &hash).await.unwrap().is_none());
}

// Impact: a src hash still referenced by another photo of the src library
// must keep its row (and the file backs both libraries).
// Should: decrement src to 1 and keep the file.
#[tokio::test]
async fn hard_move_shared_src_hash_keeps_row_and_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_libs().await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    let bytes = b"shared-within-src";
    let stay_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let stay = seed_one(&store, &stay_desc).await;
    write_resource(&store, &data_dir, &stay, ResourceType::Original, bytes).await;

    let move_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &move_desc).await;
    let hash = write_resource(&store, &data_dir, &id, ResourceType::Original, bytes).await;
    assert_eq!(
        store
            .blob(&personal, &hash)
            .await
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );

    let report = execute_transition(&store, &id, &personal, &shared)
        .await
        .unwrap();
    assert_eq!(report.blobs_transferred, 1);
    assert_eq!(
        store
            .blob(&personal, &hash)
            .await
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );
    assert_eq!(
        store.blob(&shared, &hash).await.unwrap().unwrap().ref_count,
        1
    );
    assert!(data_dir.spool().blob_path(&hash, "bin").is_file());
}

// Impact: an evicted src blob has no bytes on purpose — the dst row must
// inherit the eviction stamp or fsck would read the (correctly) absent
// file as byte loss.
// Should: stamp the fresh dst row evicted when the src row was evicted.
#[tokio::test]
async fn hard_move_inherits_eviction_stamp() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, personal, shared) = store_with_libs().await;
    let data_dir = DataDir::new(tmp.path().join("data"));

    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;
    let hash = write_resource(&store, &data_dir, &id, ResourceType::Original, b"evicted").await;
    store.stamp_blob_evicted(&personal, &hash).await.unwrap();
    std::fs::remove_file(data_dir.spool().blob_path(&hash, "bin")).unwrap();

    execute_transition(&store, &id, &personal, &shared)
        .await
        .unwrap();
    let dst = store.blob(&shared, &hash).await.unwrap().unwrap();
    assert!(dst.evicted_at.is_some(), "dst row inherits the stamp");
}
