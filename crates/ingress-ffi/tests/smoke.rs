//! FFI surface smoke tests, Rust-side through the exported API — exercises
//! everything except uniffi codegen itself (the build script's bindgen stage
//! verifies that).

use ingress_ffi::{
    FfiAssetDescriptor, FfiCaptureMetadata, FfiEnsureLibraryOutcome, FfiError,
    FfiHashResolutionKind, FfiLibraryScope, FfiMediaType, FfiResolution, FfiResourceDescriptor,
    IngressSession,
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
        media_type: if live_photo {
            FfiMediaType::LivePhoto
        } else {
            FfiMediaType::Image
        },
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
}

fn rig() -> Rig {
    let data_dir = tempfile::tempdir().unwrap();
    // Seed via ingress-core BEFORE the session opens its pool (the FFI's
    // ensure_personal_library generates ids; the rig wants the fixed
    // "personal" id), and drop the seeding store so exactly one writer pool
    // is live at a time.
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = ingress_core::StateStore::open(&data_dir.path().join("state.db"))
                .await
                .unwrap();
            store
                .insert_library(&ingress_core::LibraryConfig {
                    library_id: ingress_core::LibraryId::new("personal"),
                    display_name: "Personal".into(),
                    scope_binding: None,
                    retention_days: 30,
                    created_at: chrono::Utc::now(),
                })
                .await
                .unwrap();
        });
    }
    let session = IngressSession::new(data_dir.path().to_string_lossy().into_owned()).unwrap();
    Rig {
        session,
        data_dir,

    }
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
    assert!(matches!(
        outcome.resolution_kind,
        FfiHashResolutionKind::NewPhoto
    ));
    assert!(!outcome.deduped);
    assert!(!outcome.photo_completed); // paired video still pending
    assert!(std::path::Path::new(&outcome.blob_path).exists());
    assert_eq!(outcome.size_bytes, still.len() as u64);
    assert_eq!(outcome.ext, "heic");

    // Paired video.
    let video = b"fake-video-bytes".to_vec();
    let sink = rig
        .session
        .begin_resource(
            outcome.photo_id.clone(),
            9,
            "com.apple.quicktime-movie".into(),
            None,
        )
        .unwrap();
    sink.write(video.clone()).unwrap();
    let outcome2 = sink.finish().unwrap();
    assert!(matches!(
        outcome2.resolution_kind,
        FfiHashResolutionKind::ExistingPhoto
    ));
    assert!(outcome2.photo_completed);
    assert_eq!(outcome2.ext, "mov");
    assert!(
        outcome2.descriptor_persisted,
        "capsule persisted on completion"
    );

    // Capsule content sanity: capture offset preserved, media type carried.
    let json = sqlite_scalar(
        rig.data_dir.path(),
        "SELECT descriptor_json FROM photos LIMIT 1",
    );
    let doc: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(doc["capture"]["captured_at"], "2019-08-14T16:22:03+02:00");
    assert_eq!(doc["media_type"], "live_photo");

    // Blobs live under the data dir's spool; .partial is empty.
    let spool = rig.data_dir.path().join("spool");
    assert!(std::path::Path::new(&outcome.blob_path).starts_with(&spool));
    let partial = spool.join("blobs").join(".partial");
    assert_eq!(std::fs::read_dir(&partial).unwrap().count(), 0);
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
    assert!(matches!(
        sink.write(b"y".to_vec()).unwrap_err(),
        FfiError::SinkState { .. }
    ));
    assert!(matches!(
        sink.finish().unwrap_err(),
        FfiError::SinkState { .. }
    ));

    let mut no_original = slice_descriptor("CLOUD-NOORIG:001", false);
    no_original.resources.clear();
    assert!(matches!(
        rig.session.begin_original(no_original).unwrap_err(),
        FfiError::InvalidDescriptor { .. }
    ));
}

// Impact: this is the exact call sequence the Swift daemon performs for
// seed + drain; a native impl of the foreign trait exercises the whole
// adapter/scheduled-sink path without codegen.
// Should: seed then drain a live photo through the foreign-trait surface.
// Should not: allow Swift-side finish on a scheduled sink.
#[test]
fn seed_and_drain_through_fetcher_trait() {
    use ingress_ffi::{FfiDrainOptions, FfiFetchRequest, FfiSeedOutcome, PhotoResourceFetcher};

    struct NativeFetcher {
        desc: FfiAssetDescriptor,
    }
    impl PhotoResourceFetcher for NativeFetcher {
        fn descriptor_for(&self, _local_id: String) -> Result<FfiAssetDescriptor, FfiError> {
            Ok(self.desc.clone())
        }
        fn fetch_resource(
            &self,
            request: FfiFetchRequest,
            sink: std::sync::Arc<ingress_ffi::ChunkSink>,
        ) -> Result<(), FfiError> {
            // Scheduled sinks must reject finish — commit control is Rust's.
            assert!(matches!(sink.finish(), Err(FfiError::SinkState { .. })));
            let bytes: &[u8] = if request.ph_resource_type == 1 {
                b"still-bytes"
            } else {
                b"motion-bytes"
            };
            sink.write(bytes.to_vec())?;
            Ok(())
        }
    }

    let rig = rig();
    let desc = slice_descriptor("CLOUD-DRAIN:001", true);
    match rig.session.seed_descriptor(desc.clone()).unwrap() {
        FfiSeedOutcome::MintedPending { resources, .. } => assert_eq!(resources, 2),
        other => panic!("expected MintedPending, got {other:?}"),
    }

    let report = rig
        .session
        .drain(
            std::sync::Arc::new(NativeFetcher { desc }),
            FfiDrainOptions {
                fetch_concurrency: 2,
                retry_cap: 3,
                retry_base_secs: 0,
                retry_max_secs: 0,
                reserve_floor_gib: 0,
                pressure_pause_secs: 1,
                storage_poll_secs: 1,
            },
        )
        .unwrap();
    assert_eq!(report.photos_completed, 1);
    assert_eq!(report.resources_written, 2);
    assert_eq!(report.gave_up, 0);
}

// Impact: this is the exact call sequence the Swift `cleanup` subcommand
// performs — lock, hard-delete past retention, snapshot, report.
// Should: hard-delete an expired tombstone (rows + blob file) and write a
// state snapshot, reporting both.
#[test]
fn cleanup_round_trip() {
    use ingress_ffi::FfiCleanupOptions;

    let rig = rig();

    // One completed photo that survives the run…
    let keep = slice_descriptor("CLOUD-CLEAN-KEEP:001", false);
    rig.session.ingest_descriptor(keep.clone()).unwrap();
    let sink = rig.session.begin_original(keep.clone()).unwrap();
    sink.write(b"keep-bytes".to_vec()).unwrap();
    sink.finish().unwrap();

    // …and one completed, tombstoned, retention-expired photo.
    let gone = slice_descriptor("CLOUD-CLEAN-GONE:001", false);
    rig.session.ingest_descriptor(gone.clone()).unwrap();
    let sink = rig.session.begin_original(gone.clone()).unwrap();
    sink.write(b"gone-bytes".to_vec()).unwrap();
    let outcome = sink.finish().unwrap();
    // Tombstone + backdate directly (retention 30 in this rig).
    let db = rusqlite_free_update(
        rig.data_dir.path(),
        &format!(
            "UPDATE photos SET deleted_at = datetime('now', '-31 days') WHERE photo_id = '{}'",
            outcome.photo_id
        ),
    );
    assert!(db, "backdate tombstone");

    let report = rig
        .session
        .cleanup(FfiCleanupOptions {
            log_retention_days: 180,

            hard_delete_batch: 500,
        })
        .unwrap();
    assert_eq!(report.photos_hard_deleted, 1);
    assert_eq!(report.blob_files_deleted, 1);
}

/// The FFI crate has no direct sqlx dev-dep; shell out to sqlite3 for the
/// one test fixture mutation the surface doesn't (and shouldn't) expose.
fn rusqlite_free_update(data_dir: &std::path::Path, sql: &str) -> bool {
    std::process::Command::new("sqlite3")
        .arg(data_dir.join("state.db"))
        .arg(sql)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
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

// ============================================================================
// ensure_personal_library — the daemon-startup auto-bind surface
// ============================================================================

/// Read one scalar back out of state.db (same sqlite3-shell approach as
/// `rusqlite_free_update` — no sqlx dev-dep in this crate).
fn sqlite_scalar(data_dir: &std::path::Path, sql: &str) -> String {
    let out = std::process::Command::new("sqlite3")
        .arg(data_dir.join("state.db"))
        .arg(sql)
        .output()
        .unwrap();
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

// Impact: the enablement flow depends on this being the ONLY provisioning
// step a fresh install needs — without it every ingest silently parks as
// scope_unmapped.
// Should: create a personal library with a generated id when none exists.
// Should: record the creation in the ingest log.
#[test]
fn ensure_personal_library_creates_when_absent() {
    let data_dir = tempfile::tempdir().unwrap();
    let session = IngressSession::new(data_dir.path().to_string_lossy().into_owned()).unwrap();
    let outcome = session
        .ensure_personal_library()
        .unwrap();
    match outcome {
        FfiEnsureLibraryOutcome::Created { library_id, .. } => {
            assert!(library_id.contains('_'), "generated two-word id: {library_id}");
        }
        other => panic!("expected Created, got {other:?}"),
    }
    assert_eq!(
        sqlite_scalar(
            data_dir.path(),
            "SELECT COUNT(*) FROM ingest_log WHERE event_type = 'library_added'",
        ),
        "1"
    );
}

// Should: report the already-existing library instead of creating a second
// personal-routing candidate.
#[test]
fn ensure_personal_library_is_idempotent() {
    let rig = rig();
    let outcome = rig.session.ensure_personal_library().unwrap();
    match outcome {
        FfiEnsureLibraryOutcome::AlreadyExists { library_id } => {
            assert_eq!(library_id, "personal");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

// Impact: library writes and daemon/CLI runs share one exclusive run lock;
// the auto-bind must respect a live holder rather than corrupting refcounts.
// Should: refuse to bind while another live process holds the run lock.
#[test]
fn ensure_personal_library_refuses_live_run_lock() {
    let data_dir = tempfile::tempdir().unwrap();
    let session = IngressSession::new(data_dir.path().to_string_lossy().into_owned()).unwrap();
    // A lock file stamped with THIS (live) pid reads as another running
    // process to the acquire path.
    std::fs::write(
        data_dir.path().join("drain.lock"),
        format!("{}", std::process::id()),
    )
    .unwrap();
    assert!(matches!(
        session
            .ensure_personal_library()
            .unwrap_err(),
        FfiError::Invariant { .. }
    ));
}
