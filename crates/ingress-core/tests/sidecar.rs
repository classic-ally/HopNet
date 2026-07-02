//! Sidecar format tests (spec §Sidecar Format).

use chrono::{DateTime, FixedOffset, Utc};
use ingress_core::descriptor::{Camera, Location};
use ingress_core::sidecar::{Sidecar, SidecarGroup, SidecarResource, SIDECAR_SCHEMA_V1};
use ingress_core::{IngressError, LibraryId, PhotoId};

fn example_sidecar() -> Sidecar {
    // Mirrors the spec's example document.
    Sidecar {
        schema: SIDECAR_SCHEMA_V1.to_string(),
        photo_id: serde_json::from_str::<PhotoId>("\"01912e5a-7b3c-7f21-a4d8-3e9f12ab34cd\"")
            .unwrap(),
        library_id: LibraryId::new("personal"),
        cloud_id: Some("ABC123-EXAMPLE:001".to_string()),
        ingested_at: "2026-07-02T14:03:11Z".parse::<DateTime<Utc>>().unwrap(),
        deleted_at: None,
        captured_at: Some(
            "2019-08-14T16:22:03+02:00".parse::<DateTime<FixedOffset>>().unwrap(),
        ),
        media_type: ingress_core::descriptor::MediaType::Image,
        media_subtypes: vec!["hdr".to_string()],
        pixel_width: Some(4032),
        pixel_height: Some(3024),
        orientation: Some(6),
        duration_ms: None,
        camera: Some(Camera { make: Some("Apple".into()), model: Some("iPhone 15 Pro".into()) }),
        location: Some(Location { lat: 45.5017, lon: -73.5673 }),
        favorite: false,
        group: Some(SidecarGroup {
            id: "3f9a0000000000000000000000000000".to_string(),
            group_type: "burst".to_string(),
            index: Some(3),
            is_pick: true,
        }),
        resources: vec![
            SidecarResource {
                resource_type: "original".to_string(),
                content_hash: "ab34".to_string(),
                ext: "heic".to_string(),
                size_bytes: 2_841_923,
            },
            SidecarResource {
                resource_type: "paired_video".to_string(),
                content_hash: "9c1f".to_string(),
                ext: "mov".to_string(),
                size_bytes: 1_204_833,
            },
        ],
    }
}

// Impact: the sidecar is the recovery contract — a shape change breaks
// rebuild of state.db from surviving storage; this golden pins the format.
// Should: serialize with the spec's exact field names and null-vs-absent shape.
// Should not: emit camera/location/group/cloud_id keys when those are absent.
#[tokio::test]
async fn golden_shape_matches_spec_example() {
    let full = example_sidecar();
    let value: serde_json::Value = serde_json::from_str(&full.to_json().unwrap()).unwrap();

    assert_eq!(value["schema"], "hopnet-photo-ingress/v1");
    assert_eq!(value["media_type"], "image");
    assert_eq!(value["resources"][0]["type"], "original");
    assert_eq!(value["resources"][1]["type"], "paired_video");
    assert_eq!(value["group"]["type"], "burst");
    // Explicit nulls per the spec example:
    assert!(value.as_object().unwrap().contains_key("deleted_at"));
    assert!(value["deleted_at"].is_null());
    assert!(value.as_object().unwrap().contains_key("duration_ms"));
    assert!(value["duration_ms"].is_null());
    // Offset-preserving capture time:
    assert_eq!(value["captured_at"], "2019-08-14T16:22:03+02:00");

    // Minimal document: optional objects are ABSENT, not null.
    let mut minimal = example_sidecar();
    minimal.cloud_id = None;
    minimal.camera = None;
    minimal.location = None;
    minimal.group = None;
    let value: serde_json::Value = serde_json::from_str(&minimal.to_json().unwrap()).unwrap();
    let keys = value.as_object().unwrap();
    assert!(!keys.contains_key("cloud_id"));
    assert!(!keys.contains_key("camera"));
    assert!(!keys.contains_key("location"));
    assert!(!keys.contains_key("group"));
}

// Should: round-trip a full sidecar document without loss.
#[tokio::test]
async fn round_trip_preserves_document() {
    let original = example_sidecar();
    let parsed = Sidecar::from_json(&original.to_json().unwrap()).unwrap();
    assert_eq!(parsed, original);
}

// Impact: forward-only schema versioning — a v1 daemon must refuse documents
// it cannot faithfully interpret rather than silently munging them.
// Should: reject a sidecar with an unknown schema version.
#[tokio::test]
async fn rejects_unknown_schema_version() {
    let mut doc = example_sidecar();
    doc.schema = "hopnet-photo-ingress/v2".to_string();
    let json = serde_json::to_string(&doc).unwrap();
    let err = Sidecar::from_json(&json).unwrap_err();
    assert!(matches!(err, IngressError::UnsupportedSidecarSchema(s) if s.ends_with("/v2")));
}

// Should: derive the YYYY/MM path from the capture date.
// Should: fall back to the ingest date when capture date is unknown.
#[tokio::test]
async fn path_derivation() {
    let doc = example_sidecar();
    assert_eq!(
        doc.rel_path().to_string_lossy(),
        format!("2019/08/{}.json", doc.photo_id)
    );

    let mut undated = example_sidecar();
    undated.captured_at = None;
    assert_eq!(
        undated.rel_path().to_string_lossy(),
        format!("2026/07/{}.json", undated.photo_id)
    );
}

// Should: compose sidecars from committed state only (written resources).
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
