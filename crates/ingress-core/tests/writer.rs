//! Blob write path (spec §Write path): streaming, dedup, crash windows,
//! photo completion, sidecar composition on disk.

use ingress_core::fixtures::{AssetDescriptorBuilder, store_with_personal};
use ingress_core::model::ResourceType;
use ingress_core::paths::{BlobPaths, DataDir, TempKey};
use ingress_core::resolve::{resolve_descriptor, resolve_with_hash};
use ingress_core::sidecar_io::write_photo_sidecar;
use ingress_core::writer::{FinalizeOutcome, ResourceWrite, finalize_resource, place_blob};
use ingress_core::{ContentHash, HashResolution, LibraryId, PhotoId, Resolution, StateStore};

struct Rig {
    store: StateStore,
    library: LibraryId,
    paths: BlobPaths,
    _blob_dir: tempfile::TempDir,
}

async fn rig() -> Rig {
    let (store, library) = store_with_personal().await;
    let blob_dir = tempfile::tempdir().expect("temp blob root");
    let paths = BlobPaths::new(blob_dir.path());
    Rig {
        store,
        library,
        paths,
        _blob_dir: blob_dir,
    }
}

fn stream_chunks(
    paths: &BlobPaths,
    key: &TempKey,
    chunks: &[&[u8]],
) -> ingress_core::writer::FinishedStream {
    let mut w = ResourceWrite::begin(paths, key).expect("begin");
    for c in chunks {
        w.append(c).expect("append");
    }
    w.finish().expect("finish")
}

async fn minted_photo(rig: &Rig, bytes: &[u8]) -> PhotoId {
    let desc = AssetDescriptorBuilder::simple_image().build();
    assert!(matches!(
        resolve_descriptor(&rig.store, &desc).await.unwrap(),
        Resolution::NeedsContentHash
    ));
    match resolve_with_hash(&rig.store, &desc, &ContentHash::of_bytes(bytes))
        .await
        .unwrap()
    {
        HashResolution::NewPhoto { photo_id } => photo_id,
        other => panic!("expected NewPhoto, got {other:?}"),
    }
}

// Impact: the streamed hash IS the storage address; drift between streamed
// and whole-buffer hashing corrupts the entire dedup contract.
// Should: produce the same hash for chunked streaming as for the whole buffer.
// Should: write file contents byte-exactly with an accurate size count.
#[tokio::test]
async fn streamed_hash_matches_whole_buffer() {
    let rig = rig().await;
    let payload: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
    let (a, rest) = payload.split_at(7);
    let (b, c) = rest.split_at(1024);

    let key = TempKey::Probe {
        token: "hash-check".into(),
    };
    let finished = stream_chunks(&rig.paths, &key, &[a, b, c]);

    assert_eq!(finished.hash, ContentHash::of_bytes(&payload));
    assert_eq!(finished.size_bytes, payload.len() as u64);
    assert_eq!(std::fs::read(&finished.temp_path).unwrap(), payload);
}

// Should: place the blob at its fan-out path, remove the temp, create the
// blobs row at ref_count 1, and stamp materialization for a one-resource photo.
// Should not: leave anything under .partial/.
#[tokio::test]
async fn finalize_miss_places_blob() {
    let rig = rig().await;
    let photo_id = minted_photo(&rig, b"miss-bytes").await;
    let key = TempKey::Resource {
        photo_id: photo_id.clone(),
        resource_type: ResourceType::Original,
    };
    let finished = stream_chunks(&rig.paths, &key, &[b"miss-bytes"]);
    let hash = finished.hash.clone();

    let outcome = finalize_resource(
        &rig.store,
        &rig.paths,
        &rig.library,
        &photo_id,
        ResourceType::Original,
        finished,
        "heic",
    )
    .await
    .unwrap();

    assert!(matches!(outcome, FinalizeOutcome::Written { .. }));
    assert!(outcome.photo_completed());
    assert!(outcome.blob_path().exists());
    assert_eq!(outcome.blob_path(), &rig.paths.blob_path(&hash, "heic"));
    let leftovers: Vec<_> = std::fs::read_dir(rig.paths.partial_dir())
        .unwrap()
        .collect();
    assert!(leftovers.is_empty());

    let blob = rig.store.blob(&rig.library, &hash).await.unwrap().unwrap();
    assert_eq!(blob.ref_count, 1);
    let photo = rig.store.photo(&photo_id).await.unwrap().unwrap();
    assert!(photo.materialized_at.is_some());
}

// Impact: rule 2b's "reuse the existing blob via refcount increment" — the
// second byte-identical photo must share storage, not duplicate it.
// Should: discard the temp, increment ref_count to 2, report Deduped.
// Should not: rewrite the existing blob file.
#[tokio::test]
async fn finalize_hit_dedupes() {
    let rig = rig().await;
    let bytes = b"shared-bytes";

    // First photo: normal miss path.
    let first = minted_photo(&rig, bytes).await;
    let k1 = TempKey::Resource {
        photo_id: first.clone(),
        resource_type: ResourceType::Original,
    };
    let f1 = stream_chunks(&rig.paths, &k1, &[bytes]);
    let hash = f1.hash.clone();
    finalize_resource(
        &rig.store,
        &rig.paths,
        &rig.library,
        &first,
        ResourceType::Original,
        f1,
        "heic",
    )
    .await
    .unwrap();
    let mtime_before = std::fs::metadata(rig.paths.blob_path(&hash, "heic"))
        .unwrap()
        .modified()
        .unwrap();

    // Second photo, byte-identical original (2b shape: different cloud_id).
    let desc2 = AssetDescriptorBuilder::simple_image()
        .with_cloud_id("CLOUD-2B:001")
        .build();
    resolve_descriptor(&rig.store, &desc2).await.unwrap();
    let second = match resolve_with_hash(&rig.store, &desc2, &hash).await.unwrap() {
        HashResolution::NewPhotoSharedBlob { photo_id, .. } => photo_id,
        other => panic!("expected NewPhotoSharedBlob, got {other:?}"),
    };
    let k2 = TempKey::Resource {
        photo_id: second.clone(),
        resource_type: ResourceType::Original,
    };
    let f2 = stream_chunks(&rig.paths, &k2, &[bytes]);
    let temp2 = f2.temp_path.clone();

    let outcome = finalize_resource(
        &rig.store,
        &rig.paths,
        &rig.library,
        &second,
        ResourceType::Original,
        f2,
        "heic",
    )
    .await
    .unwrap();

    assert!(outcome.deduped());
    assert!(!temp2.exists());
    let blob = rig.store.blob(&rig.library, &hash).await.unwrap().unwrap();
    assert_eq!(blob.ref_count, 2);
    let mtime_after = std::fs::metadata(rig.paths.blob_path(&hash, "heic"))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(mtime_before, mtime_after);
}

// Impact: proves the spec's mid-stream crash window is benign — the exact
// artifact Phase 3's startup sweep deletes, with no state corruption.
// Should: leave an orphan .partial file and a fully pending resource row.
// Should not: touch the blobs table.
#[tokio::test]
async fn crash_mid_stream_leaves_only_orphan_temp() {
    let rig = rig().await;
    let photo_id = minted_photo(&rig, b"crash-bytes").await;
    let key = TempKey::Resource {
        photo_id: photo_id.clone(),
        resource_type: ResourceType::Original,
    };

    let mut w = ResourceWrite::begin(&rig.paths, &key).unwrap();
    w.append(b"partial-").unwrap();
    // Simulated crash: drop without finish/abort.
    drop(w);

    assert!(rig.paths.temp_path(&key).exists());
    for r in rig.store.resources_for_photo(&photo_id).await.unwrap() {
        assert!(r.content_hash.is_none());
        assert!(r.written_at.is_none());
    }
    assert!(
        rig.store
            .blob(&rig.library, &ContentHash::of_bytes(b"partial-"))
            .await
            .unwrap()
            .is_none()
    );
}

// Impact: proves the spec's rename-before-commit crash window row — a blob
// file with no row is benign and re-ingest converges.
// Should: converge to consistent state when the stream re-runs after a
// crash that placed the file but never committed the row.
#[tokio::test]
async fn crash_after_rename_before_commit_converges() {
    let rig = rig().await;
    let photo_id = minted_photo(&rig, b"window-bytes").await;
    let key = TempKey::Resource {
        photo_id: photo_id.clone(),
        resource_type: ResourceType::Original,
    };

    // Filesystem half only — simulates dying between rename and commit.
    let finished = stream_chunks(&rig.paths, &key, &[b"window-bytes"]);
    let hash = finished.hash.clone();
    let blob_path = place_blob(&rig.paths, &finished, "heic").unwrap();
    assert!(blob_path.exists());
    assert!(rig.store.blob(&rig.library, &hash).await.unwrap().is_none()); // file, no row

    // Recovery-by-retry: stream again, full finalize.
    let finished2 = stream_chunks(&rig.paths, &key, &[b"window-bytes"]);
    let outcome = finalize_resource(
        &rig.store,
        &rig.paths,
        &rig.library,
        &photo_id,
        ResourceType::Original,
        finished2,
        "heic",
    )
    .await
    .unwrap();

    // No blobs row existed, so this is a miss: rename-over-identical is
    // idempotent (content addressing), and the row commits.
    assert!(matches!(outcome, FinalizeOutcome::Written { .. }));
    assert!(blob_path.exists());
    assert_eq!(
        rig.store
            .blob(&rig.library, &hash)
            .await
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );
}

// Should: remove the temp on abort.
// Should: truncate a stale temp when a new stream re-begins the same key.
#[tokio::test]
async fn abort_and_reentry_semantics() {
    let rig = rig().await;
    let key = TempKey::Probe {
        token: "reentry".into(),
    };

    let mut w = ResourceWrite::begin(&rig.paths, &key).unwrap();
    w.append(b"doomed").unwrap();
    w.abort();
    assert!(!rig.paths.temp_path(&key).exists());

    // Stale temp from an abandoned stream…
    let mut w = ResourceWrite::begin(&rig.paths, &key).unwrap();
    w.append(b"stale-stale-stale").unwrap();
    drop(w);
    // …is truncated by re-begin, not appended to.
    let finished = stream_chunks(&rig.paths, &key, &[b"fresh"]);
    assert_eq!(finished.size_bytes, 5);
    assert_eq!(finished.hash, ContentHash::of_bytes(b"fresh"));
}

// Impact: photo-level completion gates sidecar writes and (later) the CLI's
// notion of "done" — stamping early would advertise unarchived photos.
// Should: stamp materialized_at only with the photo's final resource, and
// write a sidecar listing exactly the written resources.
#[tokio::test]
async fn completion_and_sidecar_for_multi_resource_photo() {
    let rig = rig().await;
    let data_dir_tmp = tempfile::tempdir().unwrap();
    let data_dir = DataDir::new(data_dir_tmp.path());

    let desc = AssetDescriptorBuilder::live_photo().build();
    assert!(matches!(
        resolve_descriptor(&rig.store, &desc).await.unwrap(),
        Resolution::NeedsContentHash
    ));
    let still = b"still-bytes".as_slice();
    let photo_id = match resolve_with_hash(&rig.store, &desc, &ContentHash::of_bytes(still))
        .await
        .unwrap()
    {
        HashResolution::NewPhoto { photo_id } => photo_id,
        other => panic!("expected NewPhoto, got {other:?}"),
    };

    // First resource (still): not complete yet.
    let k1 = TempKey::Resource {
        photo_id: photo_id.clone(),
        resource_type: ResourceType::Original,
    };
    let f1 = stream_chunks(&rig.paths, &k1, &[still]);
    let o1 = finalize_resource(
        &rig.store,
        &rig.paths,
        &rig.library,
        &photo_id,
        ResourceType::Original,
        f1,
        "heic",
    )
    .await
    .unwrap();
    assert!(!o1.photo_completed());

    // Second resource (paired video): completes the photo.
    let video = b"video-bytes".as_slice();
    let k2 = TempKey::Resource {
        photo_id: photo_id.clone(),
        resource_type: ResourceType::PairedVideo,
    };
    let f2 = stream_chunks(&rig.paths, &k2, &[video]);
    let o2 = finalize_resource(
        &rig.store,
        &rig.paths,
        &rig.library,
        &photo_id,
        ResourceType::PairedVideo,
        f2,
        "mov",
    )
    .await
    .unwrap();
    assert!(o2.photo_completed());

    let sidecar_path = write_photo_sidecar(&rig.store, &data_dir, &desc, &photo_id)
        .await
        .unwrap();
    assert!(sidecar_path.exists());
    let doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    let types: Vec<&str> = doc["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["type"].as_str().unwrap())
        .collect();
    assert_eq!(types, vec!["original", "paired_video"]);
    assert!(sidecar_path.starts_with(data_dir.sidecar_root(&rig.library)));
}
