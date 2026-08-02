//! Publish-metadata document composition tests (the `Sidecar` struct is the
//! recomposed metadata document `assemble_item` hands the publisher).

use ingress_core::sidecar::Sidecar;

// Should: compose the metadata document from committed state only (written
// resources).
#[tokio::test]
async fn compose_includes_only_written_resources() {
    use ingress_core::fixtures::AssetDescriptorBuilder;
    use ingress_core::model::ResourceType;
    use ingress_core::resolve::{resolve_descriptor, resolve_with_hash};
    use ingress_core::{ContentHash, HashResolution, Resolution};

    let (store, lib_id) = ingress_core::fixtures::store_with_personal().await;
    let desc = AssetDescriptorBuilder::live_photo().build();
    let hash = ContentHash::of_bytes(b"still-bytes");

    assert!(matches!(
        resolve_descriptor(&store, &desc).await.unwrap(),
        Resolution::NeedsContentHash
    ));
    let photo_id = match resolve_with_hash(&store, &desc, &hash).await.unwrap() {
        HashResolution::NewPhoto { photo_id } => photo_id,
        other => panic!("expected NewPhoto, got {other:?}"),
    };

    // Only the still is written; the paired video is inflight.
    store
        .mark_resource_written(&photo_id, ResourceType::Original, &hash, "heic", 2_000_000)
        .await
        .unwrap();

    let photo = store.photo(&photo_id).await.unwrap().unwrap();
    let library = store.library(&lib_id).await.unwrap().unwrap();
    let resources = store.resources_for_photo(&photo_id).await.unwrap();
    let sidecar = Sidecar::compose(
        &photo,
        &library,
        desc.media_type,
        &desc.media_subtypes,
        desc.favorite,
        &desc.capture,
        &resources,
    )
    .unwrap();

    assert_eq!(sidecar.resources.len(), 1);
    assert_eq!(sidecar.resources[0].resource_type, "original");
    // Photo not fully materialized: paired video still pending.
    assert!(photo.materialized_at.is_none());
}
