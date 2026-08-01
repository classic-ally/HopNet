//! Mapping, wire, and flow behavior — the HTTP side runs against an
//! in-process axum stub node; no real HopNet anywhere.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use chrono::Utc;
use hopnet_common::Blake3Hash;
use hopnet_photos_core::dispatch::{LibraryMember, LibraryMembership, UploadedDataBlock, UploadedFragment};
use ingress_core::descriptor::MediaType;
use ingress_core::publish::{PublishError, PublishItem, PublishOutcome, PublishResource, Publisher};
use ingress_core::sidecar::{Sidecar, SidecarGroup};
use ingress_publisher::NodePublisher;
use ingress_publisher::map;

// ---------------------------------------------------------------- fixtures

const PHOTO_ID: &str = "01890a5d-ac96-774b-bcce-b302099a8057";

fn hex_hash(byte: u8) -> String {
    hex::encode([byte; 32])
}

/// A fully-populated publishable item; blob files land in `dir`.
fn make_item(dir: &std::path::Path, resources: Vec<(&str, &str, Vec<u8>)>) -> PublishItem {
    let library_id = ingress_core::LibraryId::new("personal");
    let mut publish_resources = Vec::new();
    let mut sidecar_resources = Vec::new();
    for (index, (type_name, ext, bytes)) in resources.iter().enumerate() {
        let hash = hex_hash(index as u8 + 1);
        let blob_path = dir.join(format!("{hash}.{ext}"));
        std::fs::write(&blob_path, bytes).unwrap();
        let resource_type = ingress_core::ResourceType::from_name(type_name)
            .unwrap_or_else(|| panic!("unknown type {type_name}"));
        publish_resources.push(PublishResource {
            resource_type,
            content_hash: ingress_core::ContentHash::from_hex(hash.clone()),
            ext: ext.to_string(),
            size_bytes: bytes.len() as i64,
            blob_path,
        });
        sidecar_resources.push(ingress_core::sidecar::SidecarResource {
            resource_type: type_name.to_string(),
            content_hash: hash,
            ext: ext.to_string(),
            size_bytes: bytes.len() as i64,
        });
    }

    let now = Utc::now();
    PublishItem {
        photo: ingress_core::PhotoRecord {
            photo_id: ingress_core::PhotoId::from_string(PHOTO_ID),
            library_id: Some(library_id.clone()),
            cloud_id: Some("cloud-abc".into()),
            local_id: Some("local-abc".into()),
            group_id: None,
            group_type: None,
            group_index: None,
            is_group_pick: false,
            discovered_at: now,
            asset_modified_at: None,
            materialized_at: Some(now),
            descriptor_json: None,
            published_at: None,
            publish_attempts: 0,
            publish_next_retry_at: None,
            publish_last_error: None,
            consensus_photo_id: None,
            deleted_at: None,
        },
        library: ingress_core::LibraryConfig {
            library_id: library_id.clone(),
            display_name: "Personal".into(),
            scope_binding: None,
            retention_days: 30,
            created_at: now,
        },
        sidecar: Sidecar {
            schema: "hopnet-photo-ingress/v1".into(),
            photo_id: ingress_core::PhotoId::from_string(PHOTO_ID),
            library_id,
            cloud_id: Some("cloud-abc".into()),
            ingested_at: now,
            deleted_at: None,
            captured_at: Some(
                chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05+02:00").unwrap(),
            ),
            media_type: MediaType::Image,
            media_subtypes: vec![],
            pixel_width: Some(4032),
            pixel_height: Some(3024),
            orientation: Some(6),
            duration_ms: None,
            camera: Some(ingress_core::descriptor::Camera {
                make: Some("Apple".into()),
                model: Some("iPhone 16 Pro".into()),
            }),
            location: Some(ingress_core::descriptor::Location {
                lat: 45.5019,
                lon: -73.5674,
            }),
            favorite: false,
            group: None,
            resources: sidecar_resources,
        },
        resources: publish_resources,
        cloud_fingerprint: None,
    }
}

fn simple_item(dir: &std::path::Path) -> PublishItem {
    make_item(dir, vec![("original", "jpg", vec![7u8; 4096])])
}

// -------------------------------------------------------------- map tests

// Impact: mapping runs before any bytes move; an asset that failed
// server-side validation would burn a full upload first, so the map must
// produce validate()-clean assets or reject locally.
// Should: map every daemon-storable resource type onto its RFC-011 kind
// with sizes, hashes, and MIME hints carried through, passing validation.
#[test]
fn maps_all_daemon_resource_types() {
    let dir = tempfile::tempdir().unwrap();
    let item = make_item(
        dir.path(),
        vec![
            ("original", "heic", vec![1u8; 100]),
            ("edited", "jpg", vec![2u8; 200]),
            ("paired_video", "mov", vec![3u8; 300]),
            ("adjustment_data", "plist", vec![4u8; 50]),
            ("raw_alternate", "dng", vec![5u8; 400]),
            ("thumbnail_small", "jpg", vec![7u8; 60]),
            ("thumbnail_medium", "jpg", vec![8u8; 120]),
            ("edited_paired_video", "mov", vec![6u8; 500]),
        ],
    );
    let asset = map::to_photo_asset(&item).unwrap();

    let kinds: Vec<&str> = asset.resources.iter().map(|r| r.kind.as_str()).collect();
    assert_eq!(
        kinds,
        vec![
            "original",
            "edited",
            "paired_video",
            "adjustment_data",
            "raw_alternate",
            "thumbnail_small",
            "thumbnail_medium",
            "edited_paired_video"
        ]
    );
    assert_eq!(
        asset.resources[5].content.format_hint.as_deref(),
        Some("image/jpeg")
    );
    assert_eq!(asset.resources[0].content.byte_len, 100);
    assert_eq!(
        asset.resources[0].content.format_hint.as_deref(),
        Some("image/heic")
    );
    assert_eq!(
        asset.resources[3].content.format_hint.as_deref(),
        Some("application/octet-stream")
    );
    assert!(asset.resources[0].content.content_hash.is_some());
    assert!(asset.validate().is_ok());
}

// Should: encode media type per RFC-011 — video 1, live photo 2, raw
// original 3 (extension rule), plain image 0.
#[test]
fn media_type_codes_match_rfc011() {
    let dir = tempfile::tempdir().unwrap();

    let mut item = simple_item(dir.path());
    assert_eq!(map::media_type_code(&item), 0);

    item.sidecar.media_type = MediaType::Video;
    assert_eq!(map::media_type_code(&item), 1);

    item.sidecar.media_type = MediaType::LivePhoto;
    assert_eq!(map::media_type_code(&item), 2);

    let raw = make_item(dir.path(), vec![("original", "dng", vec![9u8; 64])]);
    assert_eq!(map::media_type_code(&raw), 3);
}

// Impact: RFC-011's date_taken is non-optional and drives gallery sort
// order; an empty date would corrupt keyset pagination for the photo.
// Should: prefer captured_at and fall back to ingested_at when PhotoKit
// exposed no capture time.
#[test]
fn date_taken_falls_back_to_ingested_at() {
    let dir = tempfile::tempdir().unwrap();
    let mut item = simple_item(dir.path());

    let asset = map::to_photo_asset(&item).unwrap();
    assert!(asset.metadata.date_taken.starts_with("2026-01-02T03:04:05"));

    item.sidecar.captured_at = None;
    let asset = map::to_photo_asset(&item).unwrap();
    assert_eq!(asset.metadata.date_taken, item.sidecar.ingested_at.to_rfc3339());
}

// Should: carry capture metadata (dims, orientation, camera, location)
// and group fields through when present.
#[test]
fn capture_and_group_metadata_carry_through() {
    let dir = tempfile::tempdir().unwrap();
    let mut item = simple_item(dir.path());
    item.sidecar.group = Some(SidecarGroup {
        id: "group-1".into(),
        group_type: "stack".into(),
        index: Some(2),
        is_pick: true,
    });

    let meta = map::to_photo_asset(&item).unwrap().metadata;
    assert_eq!(meta.width, Some(4032));
    assert_eq!(meta.height, Some(3024));
    assert_eq!(meta.orientation, Some(6));
    assert_eq!(meta.camera_make.as_deref(), Some("Apple"));
    assert_eq!(meta.camera_model.as_deref(), Some("iPhone 16 Pro"));
    assert_eq!(meta.latitude, Some(45.5019));
    assert_eq!(meta.group_id.as_deref(), Some("group-1"));
    assert_eq!(meta.group_type, Some(1));
    assert_eq!(meta.group_index, Some(2));
    assert_eq!(meta.is_group_pick, Some(1));
}

// Should not: accept a malformed content hash or an unknown group type —
// both are state corruption, surfaced as mapping errors (Rejected class).
#[test]
fn corrupt_state_is_rejected_by_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let mut item = simple_item(dir.path());
    item.resources[0].content_hash = ingress_core::ContentHash::from_hex("not-hex");
    assert!(map::to_photo_asset(&item).is_err());

    let mut item = simple_item(dir.path());
    item.sidecar.group = Some(SidecarGroup {
        id: "g".into(),
        group_type: "mystery".into(),
        index: None,
        is_pick: false,
    });
    assert!(map::to_photo_asset(&item).is_err());
    assert!(map::group_type_code("mystery").is_err());
}

// --------------------------------------------------------------- the stub

#[derive(Default)]
struct Stub {
    /// Ordered route names, with the Bearer token observed on each.
    calls: Mutex<Vec<(String, String)>>,
    committed: Mutex<HashSet<String>>,
    /// Scripted status for the next transaction posts (default 200).
    tx_statuses: Mutex<VecDeque<u16>>,
    /// Scripted status for the next data-block posts (default 201 + JSON).
    upload_statuses: Mutex<VecDeque<u16>>,
    uploads: Mutex<Vec<UploadRecord>>,
    /// Raw transaction bodies as received (tx_type + payload bytes).
    tx_bodies: Mutex<Vec<serde_json::Value>>,
    /// Scripted `/resolve` JSON response (default: holder, no entries).
    resolve_response: Mutex<Option<serde_json::Value>>,
    /// cloud_id batches received by `/resolve`.
    resolve_seen: Mutex<Vec<Vec<String>>>,
}

struct UploadRecord {
    blob_id: String,
    key_hex: String,
    declared: u64,
    body: Vec<u8>,
}

fn bearer(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default()
        .to_string()
}

async fn start_stub() -> (Arc<Stub>, String) {
    let stub = Arc::new(Stub::default());

    async fn membership(State(stub): State<Arc<Stub>>, headers: HeaderMap) -> impl IntoResponse {
        stub.calls
            .lock()
            .unwrap()
            .push(("membership".into(), bearer(&headers)));
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from([0x42u8; 32]);
        axum::Json(LibraryMembership {
            uploaded_by: 7,
            members: vec![LibraryMember { user_id: 7, pubkey }],
        })
    }

    async fn data_block(
        State(stub): State<Arc<Stub>>,
        Path(blob_id): Path<String>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> axum::response::Response {
        stub.calls
            .lock()
            .unwrap()
            .push(("data-block".into(), bearer(&headers)));
        stub.uploads.lock().unwrap().push(UploadRecord {
            blob_id,
            key_hex: headers
                .get("x-hopnet-blob-key")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string(),
            declared: headers
                .get("x-hopnet-file-size")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            body: body.to_vec(),
        });
        if let Some(status) = stub.upload_statuses.lock().unwrap().pop_front() {
            return (StatusCode::from_u16(status).unwrap(), "scripted").into_response();
        }
        (
            StatusCode::CREATED,
            axum::Json(UploadedDataBlock {
                integrity_hash: Blake3Hash::from_bytes([0xAB; 32]),
                fragments: vec![UploadedFragment {
                    chunk_number: 0,
                    local_index: 0,
                    fragment_id: hopnet_common::CustomUUID::new(None),
                    fragment_hash: Blake3Hash::from_bytes([0xCD; 32]),
                    recovery: false,
                }],
                added_bytes: 1,
            }),
        )
            .into_response()
    }

    async fn transaction(
        State(stub): State<Arc<Stub>>,
        headers: HeaderMap,
        axum::Json(body): axum::Json<serde_json::Value>,
    ) -> axum::response::Response {
        stub.calls
            .lock()
            .unwrap()
            .push(("transaction".into(), bearer(&headers)));
        stub.tx_bodies.lock().unwrap().push(body);
        let status = stub
            .tx_statuses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(200);
        (StatusCode::from_u16(status).unwrap(), "scripted").into_response()
    }

    async fn resolve(
        State(stub): State<Arc<Stub>>,
        headers: HeaderMap,
        axum::Json(body): axum::Json<serde_json::Value>,
    ) -> axum::response::Response {
        stub.calls
            .lock()
            .unwrap()
            .push(("resolve".into(), bearer(&headers)));
        let cloud_ids: Vec<String> = body["cloud_ids"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        stub.resolve_seen.lock().unwrap().push(cloud_ids);
        let response = stub
            .resolve_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| serde_json::json!({ "responsibility": "holder", "entries": [] }));
        axum::Json(response).into_response()
    }

    async fn committed(
        State(stub): State<Arc<Stub>>,
        Path(photo_id): Path<String>,
        headers: HeaderMap,
    ) -> axum::response::Response {
        stub.calls
            .lock()
            .unwrap()
            .push(("committed".into(), bearer(&headers)));
        if stub.committed.lock().unwrap().contains(&photo_id) {
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({ "photo_id": photo_id, "uploaded_by": 7 })),
            )
                .into_response()
        } else {
            StatusCode::NOT_FOUND.into_response()
        }
    }

    let app = axum::Router::new()
        .route("/api/photos/client/membership", get(membership))
        .route("/api/photos/client/data-block/{blob_id}", post(data_block))
        .route("/api/photos/client/transaction", post(transaction))
        .route("/api/photos/client/committed/{photo_id}", get(committed))
        .route("/api/photos/client/resolve", post(resolve))
        .with_state(stub.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (stub, base_url)
}

fn call_names(stub: &Stub) -> Vec<String> {
    stub.calls.lock().unwrap().iter().map(|(n, _)| n.clone()).collect()
}

// -------------------------------------------------------------- flow tests

// Impact: consensus hard-rejects duplicate photo ids, so an ambiguous prior
// attempt that actually committed MUST short-circuit here — re-submitting
// would loop on rejections forever.
// Should: return AlreadyPublished from the confirm probe alone.
// Should not: fetch membership, upload, or submit after a confirmed commit.
#[tokio::test(flavor = "multi_thread")]
async fn confirm_first_short_circuits_committed_photos() {
    let dir = tempfile::tempdir().unwrap();
    let (stub, base_url) = start_stub().await;
    stub.committed.lock().unwrap().insert(PHOTO_ID.into());

    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();
    let outcome = publisher.publish(simple_item(dir.path())).await.unwrap();

    assert_eq!(outcome, PublishOutcome::AlreadyPublished);
    assert_eq!(call_names(&stub), vec!["committed"]);
}

// Should: publish in order (confirm → membership → one streamed upload per
// resource → transaction), carrying the device token on every request and
// delivering blob bytes verbatim with the declared size and a 64-hex key.
#[tokio::test(flavor = "multi_thread")]
async fn happy_path_streams_bytes_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let (stub, base_url) = start_stub().await;

    let bytes = vec![0x5Au8; 8192];
    let item = make_item(
        dir.path(),
        vec![("original", "jpg", bytes.clone()), ("edited", "jpg", vec![0x66u8; 1024])],
    );
    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();
    let outcome = publisher.publish(item).await.unwrap();

    assert_eq!(outcome, PublishOutcome::Published);
    assert_eq!(
        call_names(&stub),
        vec!["committed", "membership", "data-block", "data-block", "transaction"]
    );
    for (_, token) in stub.calls.lock().unwrap().iter() {
        assert_eq!(token, "dev.secret");
    }
    let uploads = stub.uploads.lock().unwrap();
    assert_eq!(uploads[0].body, bytes);
    assert_eq!(uploads[0].declared, 8192);
    assert_eq!(uploads[0].key_hex.len(), 64);
    assert!(uploads[0].key_hex.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(!uploads[0].blob_id.is_empty());
    // Client-minted per-blob keys must differ per resource.
    assert_ne!(uploads[0].key_hex, uploads[1].key_hex);
}

// Impact: this is the ambiguous-outcome disambiguation loop of the
// idempotency contract — a submit that failed opaquely but actually
// committed must resolve to AlreadyPublished on the next attempt.
// Should: classify an opaque submit failure as Transient, then resolve to
// AlreadyPublished via the confirm probe without a second submit.
#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_submit_resolves_on_next_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let (stub, base_url) = start_stub().await;
    stub.tx_statuses.lock().unwrap().push_back(500);

    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();
    let err = publisher.publish(simple_item(dir.path())).await.unwrap_err();
    assert!(matches!(err, PublishError::Transient(_)), "got {err:?}");

    // The node actually committed it (the 500 was after the consensus wait).
    stub.committed.lock().unwrap().insert(PHOTO_ID.into());
    let outcome = publisher.publish(simple_item(dir.path())).await.unwrap();
    assert_eq!(outcome, PublishOutcome::AlreadyPublished);
    // Exactly one transaction post across both attempts.
    let transactions = call_names(&stub).iter().filter(|n| *n == "transaction").count();
    assert_eq!(transactions, 1);
}

// Should: classify a refused connection as NodeUnreachable (park class).
#[tokio::test(flavor = "multi_thread")]
async fn connection_refused_is_unreachable() {
    let dir = tempfile::tempdir().unwrap();
    // Bind then drop: the port is real but nothing listens.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);

    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();
    let err = publisher.publish(simple_item(dir.path())).await.unwrap_err();
    assert!(matches!(err, PublishError::NodeUnreachable(_)), "got {err:?}");
}

// Impact: a 503 is the node's shed gate (down-adjacent, client owns the
// retry) but a 422 is a length/validation bug — treating the latter as
// unreachable would park the whole queue forever on one bad photo.
// Should: classify upload 503 as NodeUnreachable.
// Should not: classify upload 422 as NodeUnreachable (Transient instead).
#[tokio::test(flavor = "multi_thread")]
async fn shedding_parks_but_client_errors_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let (stub, base_url) = start_stub().await;
    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();

    stub.upload_statuses.lock().unwrap().push_back(503);
    let err = publisher.publish(simple_item(dir.path())).await.unwrap_err();
    assert!(matches!(err, PublishError::NodeUnreachable(_)), "got {err:?}");

    stub.upload_statuses.lock().unwrap().push_back(422);
    let err = publisher.publish(simple_item(dir.path())).await.unwrap_err();
    assert!(matches!(err, PublishError::Transient(_)), "got {err:?}");
}

// Should: reject unmappable state before any network traffic beyond the
// confirm probe — no membership fetch, no uploads, no submit.
#[tokio::test(flavor = "multi_thread")]
async fn mapping_failure_rejects_without_uploading() {
    let dir = tempfile::tempdir().unwrap();
    let (stub, base_url) = start_stub().await;

    let mut item = simple_item(dir.path());
    item.resources[0].content_hash = ingress_core::ContentHash::from_hex("junk");
    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();
    let err = publisher.publish(item).await.unwrap_err();

    assert!(matches!(err, PublishError::Rejected(_)), "got {err:?}");
    assert_eq!(call_names(&stub), vec!["committed"]);
}

// ------------------------------------------------------- resolve/identity

// Should: map the resolve wire response onto the core's outcome — each
// standing string, and entries with and without a committed photo id.
#[tokio::test(flavor = "multi_thread")]
async fn resolve_maps_wire_to_outcome() {
    let (stub, base_url) = start_stub().await;
    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();

    *stub.resolve_response.lock().unwrap() = Some(serde_json::json!({
        "responsibility": "other",
        "entries": [
            { "cloud_id": "c1", "fingerprint": "ab12", "photo_id": PHOTO_ID },
            { "cloud_id": "c2", "fingerprint": "cd34", "photo_id": null },
        ],
    }));
    let outcome = publisher
        .resolve(&["c1".into(), "c2".into()])
        .await
        .unwrap();
    assert_eq!(outcome.responsibility, ingress_core::publish::Responsibility::Other);
    assert_eq!(outcome.entries.len(), 2);
    assert_eq!(outcome.entries[0].committed_photo_id.as_deref(), Some(PHOTO_ID));
    assert_eq!(outcome.entries[1].committed_photo_id, None);
    assert_eq!(outcome.entries[1].fingerprint, "cd34");
    assert_eq!(
        *stub.resolve_seen.lock().unwrap(),
        vec![vec!["c1".to_string(), "c2".to_string()]]
    );

    // Unknown standing string is Transient, not a silent Holder.
    *stub.resolve_response.lock().unwrap() = Some(serde_json::json!({
        "responsibility": "supreme-leader",
        "entries": [],
    }));
    let err = publisher.resolve(&[]).await.unwrap_err();
    assert!(matches!(err, PublishError::Transient(_)), "got {err:?}");
}

// Should: classify a refused connection on resolve as NodeUnreachable so
// the pass parks instead of burning attempts.
#[tokio::test(flavor = "multi_thread")]
async fn resolve_connection_refused_is_unreachable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);

    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();
    let err = publisher.resolve(&["c1".into()]).await.unwrap_err();
    assert!(matches!(err, PublishError::NodeUnreachable(_)), "got {err:?}");
}

// Impact: the fingerprint is the mesh's only cross-device dedupe key — if
// it silently drops between the resolve entry and the committed
// PhotoAddEntry, every second device duplicates the archive.
// Should: carry the item's hex fingerprint into the submitted photo_add
// payload as raw bytes; a fingerprint-less item submits None.
#[tokio::test(flavor = "multi_thread")]
async fn fingerprint_travels_into_photo_add_payload() {
    let dir = tempfile::tempdir().unwrap();
    let (stub, base_url) = start_stub().await;
    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();

    let mut item = simple_item(dir.path());
    item.cloud_fingerprint = Some("5a".repeat(32));
    publisher.publish(item).await.unwrap();

    let decode = |body: &serde_json::Value| -> hopnet_photos::envelopes::PhotoAddPayload {
        let bytes: Vec<u8> = body["payload"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as u8)
            .collect();
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
            .unwrap()
            .0
    };
    {
        let bodies = stub.tx_bodies.lock().unwrap();
        assert_eq!(bodies[0]["tx_type"], "photo_add");
        let payload = decode(&bodies[0]);
        assert_eq!(payload.entries[0].cloud_fingerprint, Some([0x5A; 32]));
    }

    let item = simple_item(dir.path()); // fingerprint None
    publisher.publish(item).await.unwrap();
    let bodies = stub.tx_bodies.lock().unwrap();
    let payload = decode(&bodies[1]);
    assert_eq!(payload.entries[0].cloud_fingerprint, None);
}

// Should: reject a malformed fingerprint locally (Rejected, nothing
// uploaded) rather than submitting a corrupt payload.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_fingerprint_rejects_before_upload() {
    let dir = tempfile::tempdir().unwrap();
    let (stub, base_url) = start_stub().await;
    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();

    let mut item = simple_item(dir.path());
    item.cloud_fingerprint = Some("not-hex".into());
    let err = publisher.publish(item).await.unwrap_err();
    assert!(matches!(err, PublishError::Rejected(_)), "got {err:?}");
    assert_eq!(call_names(&stub), vec!["committed"], "no bytes moved");
}

// Impact: the 403 gate is the admission backstop — a daemon that somehow
// publishes without resolving must land in the retry class that a
// responsibility transfer can heal, not park or give up permanently.
// Should: classify the node's ingress_not_responsible 403 as Transient.
#[tokio::test(flavor = "multi_thread")]
async fn not_responsible_403_classifies_transient() {
    let dir = tempfile::tempdir().unwrap();
    let (stub, base_url) = start_stub().await;
    stub.tx_statuses.lock().unwrap().push_back(403);

    let publisher = NodePublisher::new(&base_url, "dev.secret").unwrap();
    let err = publisher.publish(simple_item(dir.path())).await.unwrap_err();
    assert!(matches!(err, PublishError::Transient(_)), "got {err:?}");
}
