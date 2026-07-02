//! FFI surface smoke tests, Rust-side through the exported API — exercises
//! everything except uniffi codegen itself (the build script's bindgen stage
//! verifies that).

use ingress_ffi::{
    FfiAssetDescriptor, FfiCaptureMetadata, FfiError, FfiHashResolutionKind, FfiLibraryScope,
    FfiMediaType, FfiResolution, FfiResourceDescriptor, IngressSession,
};

fn slice_descriptor(cloud_id: &str, live_photo: bool) -> FfiAssetDescriptor {
    let mut resources = vec![FfiResourceDescriptor {
        ph_resource_type: 1,
        uti: "public.heic".into(),
        original_filename: Some("IMG_0001.HEIC".into()),
        expected_size: Some(1024),
        locally_available: Some(true),
    }];
    if live_photo {
        resources.push(FfiResourceDescriptor {
            ph_resource_type: 9,
            uti: "com.apple.quicktime-movie".into(),
            original_filename: Some("IMG_0001.MOV".into()),
            expected_size: Some(2048),
            locally_available: Some(true),
        });
    }
    FfiAssetDescriptor {
        local_id: format!("LOCAL-{cloud_id}/L0/001"),
        cloud_id: Some(cloud_id.to_string()),
        scope: FfiLibraryScope::Personal,
        media_type: if live_photo { FfiMediaType::LivePhoto } else { FfiMediaType::Image },
        media_subtypes: vec![],
        asset_modified_at: Some("2026-07-02T12:00:00Z".into()),
        favorite: false,
        burst: None,
        capture: FfiCaptureMetadata {
            captured_at: Some("2019-08-14T16:22:03+02:00".into()),
            pixel_width: Some(4032),
            pixel_height: Some(3024),
            orientation: Some(6),
            duration_ms: None,
            camera: None,
            location: None,
        },
        resources,
    }
}

struct Rig {
    session: std::sync::Arc<IngressSession>,
    data_dir: tempfile::TempDir,
    blob_dir: tempfile::TempDir,
}

fn rig() -> Rig {
    let data_dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let session = IngressSession::new(data_dir.path().to_string_lossy().into_owned()).unwrap();
    session
        .add_library(
            "personal".into(),
            "Personal".into(),
            blob_dir.path().to_string_lossy().into_owned(),
            FfiLibraryScope::Personal,
        )
        .unwrap();
    Rig { session, data_dir, blob_dir }
}

// Impact: this is the exact call sequence the Swift slice performs; if it
// works here, remaining slice risk is codegen + PhotoKit only.
// Should: carry a two-resource asset end-to-end — resolve, stream original,
// stream paired video, complete photo, write sidecar, land blobs on disk.
#[test]
fn end_to_end_live_photo() {
    let rig = rig();

    let desc = slice_descriptor("CLOUD-E2E:001", true);
    match rig.session.ingest_descriptor(desc.clone()).unwrap() {
        FfiResolution::NeedsOriginal => {}
        other => panic!("expected NeedsOriginal, got {other:?}"),
    }

    // Original: three odd-sized chunks.
    let still: Vec<u8> = (0..3000u16).flat_map(|i| i.to_le_bytes()).collect();
    let sink = rig.session.begin_original(desc.clone()).unwrap();
    let (a, rest) = still.split_at(7);
    let (b, c) = rest.split_at(1024);
    for chunk in [a, b, c] {
        sink.write(chunk.to_vec()).unwrap();
    }
    let outcome = sink.finish().unwrap();
    assert!(matches!(outcome.resolution_kind, FfiHashResolutionKind::NewPhoto));
    assert!(!outcome.deduped);
    assert!(!outcome.photo_completed); // paired video still pending
    assert!(std::path::Path::new(&outcome.blob_path).exists());
    assert_eq!(outcome.size_bytes, still.len() as u64);
    assert_eq!(outcome.ext, "heic");

    // Paired video.
    let video = b"fake-video-bytes".to_vec();
    let sink = rig
        .session
        .begin_resource(outcome.photo_id.clone(), 9, "com.apple.quicktime-movie".into(), None)
        .unwrap();
    sink.write(video.clone()).unwrap();
    let outcome2 = sink.finish().unwrap();
    assert!(matches!(outcome2.resolution_kind, FfiHashResolutionKind::ExistingPhoto));
    assert!(outcome2.photo_completed);
    assert_eq!(outcome2.ext, "mov");
    let sidecar_path = outcome2.sidecar_path.expect("sidecar written on completion");
    assert!(std::path::Path::new(&sidecar_path).exists());

    // Sidecar content sanity: both resources, capture offset preserved.
    let doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(doc["resources"].as_array().unwrap().len(), 2);
    assert_eq!(doc["captured_at"], "2019-08-14T16:22:03+02:00");
    assert_eq!(doc["media_type"], "live_photo");

    // Blobs live under the configured root; .partial is empty.
    assert!(std::path::Path::new(&outcome.blob_path).starts_with(rig.blob_dir.path()));
    let partial = rig.blob_dir.path().join("blobs").join(".partial");
    assert_eq!(std::fs::read_dir(&partial).unwrap().count(), 0);
    let _ = &rig.data_dir;
}

// Should: resolve a re-delivered descriptor to AlreadyKnown without streaming.
#[test]
fn re_ingest_is_already_known() {
    let rig = rig();
    let desc = slice_descriptor("CLOUD-DUPE:001", false);

    assert!(matches!(
        rig.session.ingest_descriptor(desc.clone()).unwrap(),
        FfiResolution::NeedsOriginal
    ));
    let sink = rig.session.begin_original(desc.clone()).unwrap();
    sink.write(b"bytes".to_vec()).unwrap();
    let outcome = sink.finish().unwrap();
    assert!(outcome.photo_completed);

    match rig.session.ingest_descriptor(desc).unwrap() {
        FfiResolution::AlreadyKnown { photo_id, .. } => assert_eq!(photo_id, outcome.photo_id),
        other => panic!("expected AlreadyKnown, got {other:?}"),
    }
}

// Should: surface an unbound scope as the UnmappedScope error on begin_original.
#[test]
fn unmapped_scope_error_mapping() {
    let rig = rig(); // personal only — no shared library bound
    let mut desc = slice_descriptor("CLOUD-SHARED:001", false);
    desc.scope = FfiLibraryScope::Shared;

    let err = rig.session.begin_original(desc).unwrap_err();
    assert!(matches!(err, FfiError::UnmappedScope { .. }));
}

// Should: reject sink use after finish, and reject descriptors without an original.
#[test]
fn sink_and_descriptor_misuse() {
    let rig = rig();
    let desc = slice_descriptor("CLOUD-MISUSE:001", false);
    rig.session.ingest_descriptor(desc.clone()).unwrap();

    let sink = rig.session.begin_original(desc.clone()).unwrap();
    sink.write(b"x".to_vec()).unwrap();
    sink.finish().unwrap();
    assert!(matches!(sink.write(b"y".to_vec()).unwrap_err(), FfiError::SinkState { .. }));
    assert!(matches!(sink.finish().unwrap_err(), FfiError::SinkState { .. }));

    let mut no_original = slice_descriptor("CLOUD-NOORIG:001", false);
    no_original.resources.clear();
    assert!(matches!(
        rig.session.begin_original(no_original).unwrap_err(),
        FfiError::InvalidDescriptor { .. }
    ));
}

// Should: reject malformed timestamps as InvalidDescriptor.
#[test]
fn bad_timestamp_rejected() {
    let rig = rig();
    let mut desc = slice_descriptor("CLOUD-BADTS:001", false);
    desc.asset_modified_at = Some("yesterday-ish".into());
    assert!(matches!(
        rig.session.ingest_descriptor(desc).unwrap_err(),
        FfiError::InvalidDescriptor { .. }
    ));
}
