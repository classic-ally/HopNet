use crate::AppState;
use crate::db::CustomUUID;
use crate::types::Blake3Hash;
use std::fs;
use std::io;

/// Drive-owned (RFC-015): the file error taxonomy and deterministic path
/// crypto (AES-SIV) live in hopnet-drive; re-exported here so call sites
/// don't churn.
pub use hopnet_drive::error::FileError;
pub use hopnet_drive::paths::{
    build_encrypted_path, decrypt_part, decrypt_path, encrypt_part, encrypt_path, generate_siv_key,
    generate_siv_nonce,
};

// Chunk/padding math and format constants live in the substrate crate
// (hopnet-storage, RFC-014); re-exported here so call sites don't churn.
pub use hopnet_storage::rs::{
    CHUNK_SIZE, MAX_FRAGMENT_SIZE, ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK,
    TOTAL_FRAGMENTS_PER_CHUNK, calculate_chunk_padding, calculate_chunked_fragments,
    calculate_optimal_chunks, calculate_padding_and_chunks,
};

/// Fragment file I/O lives in the substrate crate (hopnet-storage::fragstore);
/// thin delegations here keep call sites and the FileError taxonomy stable.
pub fn get_fragments_dir() -> Result<String, FileError> {
    hopnet_storage::fragstore::get_fragments_dir().map_err(FileError::from)
}

pub fn create_fragment_path(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<String, FileError> {
    hopnet_storage::fragstore::create_fragment_path(fragments_dir, fragment_hash)
        .map_err(FileError::from)
}

pub fn store_fragment(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
    data: Vec<u8>,
) -> Result<(), FileError> {
    hopnet_storage::fragstore::store_fragment(fragments_dir, fragment_hash, data)
        .map_err(FileError::from)
}

pub fn delete_fragment(fragments_dir: &str, fragment_hash: &Blake3Hash) -> Result<(), FileError> {
    hopnet_storage::fragstore::delete_fragment(fragments_dir, fragment_hash)
        .map_err(FileError::from)
}

/// Fetch a fragment from local storage only
/// Returns the fragment data if found locally, otherwise returns an error
pub fn fetch_fragment_local(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<Vec<u8>, FileError> {
    // Yield the executor around the blocking read when possible.
    // block_in_place PANICS on a current_thread runtime — and this path runs
    // on the consensus shell's dedicated thread (apply_block → handlers), so
    // fall back to a plain blocking read there (that thread is ours to block).
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| {
                hopnet_storage::fragstore::read_fragment(fragments_dir, fragment_hash)
                    .map_err(FileError::from)
            })
        }
        _ => hopnet_storage::fragstore::read_fragment(fragments_dir, fragment_hash)
            .map_err(FileError::from),
    }
}

/// Fetch and verify a fragment from local storage
/// Returns the fragment data if found locally and hash matches, otherwise returns an error
pub fn fetch_and_verify_fragment(
    fragment_hash: &Blake3Hash,
    fragments_dir: &str,
) -> Result<Vec<u8>, FileError> {
    hopnet_storage::fragstore::fetch_and_verify_fragment(fragment_hash, fragments_dir)
        .map_err(FileError::from)
}

/// Drive-owned (RFC-015): FileAccessData lives in hopnet-drive's model;
/// re-exported here so call sites don't churn.
pub use hopnet_drive::model::FileAccessData;

/// Check if a fragment exists on disk and is valid (hash matches)
pub fn fragment_exists_and_valid(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    hopnet_storage::fragstore::fragment_exists_and_valid(fragments_dir, fragment_hash)
}

// Drive-owned (RFC-015, Stage D4): shared content-update preparation
// (`prepare_content_update`) lives in hopnet_drive::http::files alongside
// its only callers (PATCH /files and FileProvider modify_item).

// Fragment cipher primitives live in the substrate crate
// (hopnet-storage::crypto, format-frozen with golden-vector tests).
pub use hopnet_storage::crypto::{
    calculate_encrypted_chunk_length, derive_chunk_key, derive_chunk_nonce,
};

pub fn encrypt_chunk(
    chunk: Vec<u8>,
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &crate::db::CustomUUID,
) -> Result<Vec<u8>, FileError> {
    hopnet_storage::crypto::encrypt_chunk(chunk, per_file_key, fragment_id).map_err(FileError::from)
}

pub fn decrypt_chunk(
    encrypted_chunk: &[u8],
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &crate::db::CustomUUID,
) -> Result<Vec<u8>, FileError> {
    hopnet_storage::crypto::decrypt_chunk(encrypted_chunk, per_file_key, fragment_id)
        .map_err(FileError::from)
}
