//! File processing functions test module.
//! Gated via `#[cfg(test)] mod tests;` in src/files/mod.rs.

use crate::db::{Blake3Hash, ChunkType, CustomUUID, SqliteConnectionManager};
use crate::storage_host::functions::{
    CHUNK_SIZE, MAX_FRAGMENT_SIZE, ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK,
    calculate_chunked_fragments, calculate_padding_and_chunks,
};
use crate::storage_host::routes::process_uploaded_file;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng as ChaChaOsRng};
use hopnet_storage::store::BlobManifest;
use rand::prelude::*;
use rusqlite::params;
use std::collections::HashMap;
use std::io::Cursor;
use std::str::FromStr;
use tempfile::TempDir;
use tokio_stream::StreamExt;

/// Helper function to generate random test data
fn generate_random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random::<u8>()).collect()
}

/// Test calculate_chunked_fragments function for Phase 4 chunked Reed-Solomon
#[test]
fn test_calculate_chunked_fragments() {
    // Empty file: special case, no chunks
    let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(0);
    assert_eq!(
        num_chunks, 0,
        "Empty file should have 0 chunks (handled separately)"
    );
    assert_eq!(
        total_original, 0,
        "Empty file should have 0 original fragments"
    );
    assert_eq!(
        total_recovery, 0,
        "Empty file should have 0 recovery fragments"
    );

    // 1 byte file: 1 chunk (< 40MB)
    let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(1);
    assert_eq!(num_chunks, 1, "1-byte file should have 1 chunk");
    assert_eq!(total_original, 10, "Should have 10 original fragments");
    assert_eq!(total_recovery, 20, "Should have 20 recovery fragments");

    // 1KB file: 1 chunk (< 40MB)
    let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(1024);
    assert_eq!(num_chunks, 1, "1KB file should have 1 chunk");
    assert_eq!(total_original, 10, "Should have 10 original fragments");
    assert_eq!(total_recovery, 20, "Should have 20 recovery fragments");

    // 1MB file: 1 chunk (< 40MB)
    let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(1024 * 1024);
    assert_eq!(num_chunks, 1, "1MB file should have 1 chunk");
    assert_eq!(total_original, 10, "Should have 10 original fragments");
    assert_eq!(total_recovery, 20, "Should have 20 recovery fragments");

    // 40MB file: exactly 1 chunk
    let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(CHUNK_SIZE);
    assert_eq!(num_chunks, 1, "40MB file should have 1 chunk");
    assert_eq!(total_original, 10, "Should have 10 original fragments");
    assert_eq!(total_recovery, 20, "Should have 20 recovery fragments");

    // 45MB file: 2 chunks (chunk 0: 40MB, chunk 1: 5MB)
    let (num_chunks, total_original, total_recovery) =
        calculate_chunked_fragments(45 * 1024 * 1024);
    assert_eq!(num_chunks, 2, "45MB file should have 2 chunks");
    assert_eq!(
        total_original, 20,
        "Should have 20 original fragments (10 per chunk)"
    );
    assert_eq!(
        total_recovery, 40,
        "Should have 40 recovery fragments (20 per chunk)"
    );

    // 100MB file: 3 chunks (40MB + 40MB + 20MB)
    let (num_chunks, total_original, total_recovery) =
        calculate_chunked_fragments(100 * 1024 * 1024);
    assert_eq!(num_chunks, 3, "100MB file should have 3 chunks");
    assert_eq!(
        total_original, 30,
        "Should have 30 original fragments (10 per chunk)"
    );
    assert_eq!(
        total_recovery, 60,
        "Should have 60 recovery fragments (20 per chunk)"
    );

    // 1GB file: 26 chunks (25 × 40MB + 1 × 24MB)
    let (num_chunks, total_original, total_recovery) =
        calculate_chunked_fragments(1024 * 1024 * 1024);
    assert_eq!(num_chunks, 26, "1GB file should have 26 chunks");
    assert_eq!(
        total_original, 260,
        "Should have 260 original fragments (10 per chunk)"
    );
    assert_eq!(
        total_recovery, 520,
        "Should have 520 recovery fragments (20 per chunk)"
    );

    // Verify constants relationship: CHUNK_SIZE should equal MAX_FRAGMENT_SIZE * ORIGINAL_FRAGMENTS_PER_CHUNK
    assert_eq!(
        CHUNK_SIZE,
        MAX_FRAGMENT_SIZE * ORIGINAL_FRAGMENTS_PER_CHUNK,
        "CHUNK_SIZE must be derived from MAX_FRAGMENT_SIZE × ORIGINAL_FRAGMENTS_PER_CHUNK"
    );
}

/// Test calculate_padding_and_chunks function used for splitting chunks into fragments
#[test]
fn test_calculate_padding_and_chunks() {
    // Test with data that divides evenly into even-sized chunks (no padding needed)
    let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    let (chunks, padding) = calculate_padding_and_chunks(test_data.clone(), 2);

    assert_eq!(chunks.len(), 2, "Should create 2 chunks");
    assert_eq!(
        padding, 0,
        "No padding needed when chunk length is already even"
    );
    assert_eq!(chunks[0].len(), 6, "Each chunk should have 6 bytes");
    assert_eq!(chunks[1].len(), 6, "Each chunk should have 6 bytes");

    // Test with uneven division requiring padding for even chunk length
    // 10 bytes / 2 chunks = 5 bytes each (odd), needs padding to make 6 bytes each
    let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let (chunks, padding) = calculate_padding_and_chunks(test_data.clone(), 2);

    assert_eq!(chunks.len(), 2, "Should create 2 chunks");
    assert_eq!(
        padding, 2,
        "Should add 2 bytes of padding to make chunk length even"
    );
    assert_eq!(
        chunks[0].len(),
        6,
        "Each chunk should have 6 bytes (5 + padding)"
    );
    assert_eq!(
        chunks[1].len(),
        6,
        "Each chunk should have 6 bytes (5 + padding)"
    );

    // Test with data requiring padding for both uneven division AND even chunk length
    // 5 bytes / 2 chunks = 2.5, rounds up to 3 bytes each (odd)
    // Must add 3 bytes total padding to make 8 bytes → 4 bytes per chunk (even)
    let test_data = vec![1, 2, 3, 4, 5];
    let (chunks, padding) = calculate_padding_and_chunks(test_data.clone(), 2);

    assert_eq!(chunks.len(), 2, "Should create 2 chunks");
    assert_eq!(
        padding, 3,
        "Should add 3 bytes of padding to ensure even chunk length"
    );
    assert_eq!(
        chunks[0].len(),
        4,
        "Each chunk should have 4 bytes (even length)"
    );
    assert_eq!(
        chunks[1].len(),
        4,
        "Each chunk should have 4 bytes (even length)"
    );
}

/// Test that chunk content is preserved correctly
#[test]
fn test_chunk_content_preservation() {
    let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let (chunks, _padding) = calculate_padding_and_chunks(test_data.clone(), 2);

    // Reconstruct data from chunks to verify preservation
    let mut reconstructed = Vec::new();
    for chunk in chunks {
        reconstructed.extend_from_slice(&chunk);
    }

    // Should match original data (with possible padding at end)
    assert_eq!(
        &reconstructed[..test_data.len()],
        test_data.as_slice(),
        "Original data should be preserved in chunks"
    );
}

fn build_manifest(
    plaintext_size: usize,
    blob_op: &hopnet_storage::store::BlobInsertOp,
    dataid: &CustomUUID,
) -> BlobManifest {
    let mut chunks: HashMap<u32, hopnet_storage::store::ChunkFragmentMaps> = HashMap::new();

    for fragment in &blob_op.fragments {
        let entry = chunks.entry(fragment.chunk_number).or_default();
        let bucket = if fragment.recovery {
            &mut entry.1
        } else {
            &mut entry.0
        };
        bucket.insert(
            fragment.local_index as usize,
            (fragment.fragment_hash, fragment.fragment_id.clone(), true),
        );
    }

    BlobManifest {
        blob_id: dataid.clone(),
        integrity_hash: blob_op.integrity_hash,
        added_bytes: blob_op.added_bytes,
        file_size: plaintext_size as u64,
        placement_height: None,
        chunks,
    }
}

async fn run_round_trip(plaintext: Vec<u8>) {
    let temp_dir = TempDir::new().unwrap();
    let fragments_dir = temp_dir.path().to_str().unwrap().to_string();
    let dataid = CustomUUID::new(None);
    let per_file_key = ChaCha20Poly1305::generate_key(&mut ChaChaOsRng);

    let source = Cursor::new(plaintext.clone());
    let blob_op = process_uploaded_file(
        source,
        plaintext.len(),
        dataid.clone(),
        &per_file_key,
        &fragments_dir,
    )
    .await
    .expect("process_uploaded_file should succeed");

    assert_eq!(
        blob_op.blob_id, dataid,
        "BlobInsertOp id must equal input dataid"
    );
    assert_eq!(
        blob_op.file_size,
        plaintext.len() as u64,
        "file_size must match plaintext length"
    );

    let manifest = build_manifest(plaintext.len(), &blob_op, &dataid);

    let stream = hopnet_storage::api::get_local(fragments_dir, manifest, Some(per_file_key), None);
    tokio::pin!(stream);

    let mut reconstructed = Vec::with_capacity(plaintext.len());
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.expect("reconstruction chunk should succeed");
        reconstructed.extend_from_slice(&chunk);
    }

    assert_eq!(
        reconstructed.len(),
        plaintext.len(),
        "reconstructed size must match plaintext"
    );
    assert_eq!(
        reconstructed, plaintext,
        "reconstructed bytes must match plaintext"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_process_uploaded_file_round_trip_small() {
    let plaintext = generate_random_data(100 * 1024);
    run_round_trip(plaintext).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_process_uploaded_file_chunk_boundary() {
    let plaintext = generate_random_data(CHUNK_SIZE);
    run_round_trip(plaintext).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_process_uploaded_file_round_trip_multi_chunk() {
    let plaintext = generate_random_data(CHUNK_SIZE + 1024);
    run_round_trip(plaintext).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy: 3 chunks (~120 MB plaintext)"]
async fn test_process_uploaded_file_round_trip_three_chunks() {
    let plaintext = generate_random_data(3 * CHUNK_SIZE);
    run_round_trip(plaintext).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy: 10 chunks (~400 MB plaintext)"]
async fn test_process_uploaded_file_round_trip_ten_chunks() {
    let plaintext = generate_random_data(10 * CHUNK_SIZE);
    run_round_trip(plaintext).await;
}

fn setup_test_db() -> r2d2::Pool<SqliteConnectionManager> {
    let manager = SqliteConnectionManager::memory();
    let pool = r2d2::Pool::builder()
        .max_size(1)
        .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
        .build(manager)
        .unwrap();
    crate::db::shared::initialize(&pool.get().unwrap()).unwrap();
    pool
}

fn insert_dummy_user(conn: &rusqlite::Connection, user_id: i32) {
    conn.execute(
        "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt) \
         VALUES (?, ?, ?, ?, ?, ?)",
        params![
            user_id,
            format!("user{}", user_id),
            vec![0u8; 32],
            vec![0u8; 32],
            vec![0u8; 32],
            vec![0u8; 16],
        ],
    )
    .unwrap();
}

fn insert_folder_inode(conn: &rusqlite::Connection, user_id: i32, path: &str) {
    conn.execute(
        "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, 1, NULL)",
        params![CustomUUID::new(None), user_id, path],
    )
    .unwrap();
}

#[test]
fn test_find_missing_parents_lexicographic_order() {
    let pool = setup_test_db();
    let mut conn = pool.get().unwrap();
    insert_dummy_user(&conn, 1);
    let tx = conn.transaction().unwrap();

    let result = crate::db::files::find_missing_parents(&tx, &["x/y/z/file"])
        .expect("find_missing_parents must succeed");

    assert_eq!(
        result,
        vec!["/x".to_string(), "/x/y".to_string(), "/x/y/z".to_string()],
        "missing parents must be depth-ascending lexicographic"
    );
}

#[test]
fn test_find_missing_parents_partial_existing() {
    let pool = setup_test_db();
    let mut conn = pool.get().unwrap();
    insert_dummy_user(&conn, 1);
    insert_folder_inode(&conn, 1, "/x");
    let tx = conn.transaction().unwrap();

    let result = crate::db::files::find_missing_parents(&tx, &["x/y/z/file"])
        .expect("find_missing_parents must succeed");

    assert_eq!(
        result,
        vec!["/x/y".to_string(), "/x/y/z".to_string()],
        "existing /x must be excluded from missing parents"
    );
}

/// Test padding edge cases with odd numbers
#[test]
fn test_padding_edge_cases() {
    // Test files that don't divide evenly into chunks
    let odd_cases = vec![
        (1, 3),   // 1 byte into 3 chunks
        (5, 2),   // 5 bytes into 2 chunks
        (7, 3),   // 7 bytes into 3 chunks
        (100, 7), // 100 bytes into 7 chunks
    ];

    for (data_size, num_chunks) in odd_cases {
        let test_data = generate_random_data(data_size);
        let (chunks, _padding) = calculate_padding_and_chunks(test_data.clone(), num_chunks);

        assert_eq!(
            chunks.len(),
            num_chunks,
            "Should create exactly {} chunks",
            num_chunks
        );

        // All chunks should have equal size (with padding)
        let expected_chunk_size = chunks[0].len();
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(
                chunk.len(),
                expected_chunk_size,
                "Chunk {} should have even length for input size {}",
                i,
                data_size
            );
        }
    }
}

/// Should: the narrowed InodeOwner (single tag-0 variant) reproduce the
/// legacy Either<i32, User>::Left wire bytes EXACTLY (golden hex captured
/// from the pre-narrowing code on 2026-07-07), and round-trip decode.
/// Should not: change a single byte — inodes ride consensus envelopes.
/// Impact: any drift here is a wire break for insert_files/modify_item.
#[test]
fn golden_inode_wire_survives_owner_narrowing() {
    use crate::db::{Inode, InodeOwner};
    let inode = Inode {
        id: CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap(),
        owner: InodeOwner::Id(7),
        path: "/golden/path".to_string(),
        inode_type: hopnet_common::InodeType::File,
        data_id: Some(CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap()),
    };
    let bytes = bincode::serde::encode_to_vec(&inode, bincode::config::standard()).unwrap();
    assert_eq!(
        hex::encode(&bytes),
        "1001890a5dac96774bb9aa9f8b24f0c9a1000e0c2f676f6c64656e2f7061746800011001890a5dac96774bb9aa9f8b24f0c9a2"
    );
    let (decoded, _): (Inode, _) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
    assert_eq!(decoded.owner.id(), 7);

    let folder = Inode {
        id: CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a3").unwrap(),
        owner: InodeOwner::Id(0),
        path: "/g".to_string(),
        inode_type: hopnet_common::InodeType::Folder,
        data_id: None,
    };
    let bytes = bincode::serde::encode_to_vec(&folder, bincode::config::standard()).unwrap();
    assert_eq!(
        hex::encode(&bytes),
        "1001890a5dac96774bb9aa9f8b24f0c9a30000022f670100"
    );
}
