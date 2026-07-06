//! Fragment file I/O: content-addressed on-disk fragment store
//! (2-level hex nesting, atomic writes, verify-on-read).
//!
//! Moved verbatim from the main crate's files/functions.rs. All functions are
//! synchronous/blocking; async callers wrap them (the main crate keeps a
//! runtime-flavor-aware wrapper for reads on tokio worker threads).

use crate::error::StorageError;
use hopnet_common::Blake3Hash;
use std::fs;
use std::io;

/// Get the XDG data directory for storing fragments
pub fn get_fragments_dir() -> Result<String, StorageError> {
    let data_dir = std::env::var("XDG_DATA_HOME").unwrap_or_else(|_| {
        format!(
            "{}/.local/share",
            std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
        )
    });

    let fragments_dir = format!("{}/hopnet/fragments", data_dir);
    println!("Using fragments directory: {}", fragments_dir);
    Ok(fragments_dir)
}

/// Create 2-level directory structure for a fragment hash
/// e.g., "abcdef123..." -> "fragments/ab/cd/"
pub fn create_fragment_path(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<String, StorageError> {
    let hash_str = fragment_hash.to_hex();

    // Take first 4 hex characters for 2-level nesting
    if hash_str.len() < 4 {
        return Err(StorageError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Fragment hash too short",
        )));
    }

    let first_level = &hash_str[0..2];
    let second_level = &hash_str[2..4];

    let full_path = format!("{}/{}/{}", fragments_dir, first_level, second_level);

    Ok(full_path)
}

/// Store a fragment to disk using 2-level directory structure
pub fn store_fragment(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
    data: Vec<u8>,
) -> Result<(), StorageError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());

    // Create directory structure if it doesn't exist
    fs::create_dir_all(&dir_path)?;

    // Write to temp file then atomic rename to prevent concurrent readers
    // from seeing partial data (POSIX rename is atomic on the same filesystem)
    let temp_path = format!("{}.tmp.{:x}", full_file_path, rand::random::<u64>());
    fs::write(&temp_path, &data)?;
    fs::rename(&temp_path, &full_file_path).map_err(|e| {
        // Clean up temp file on rename failure
        let _ = fs::remove_file(&temp_path);
        StorageError::Io(e)
    })?;

    Ok(())
}

/// Delete a fragment from local storage
/// Simple deletion without directory cleanup for performance
pub fn delete_fragment(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<(), StorageError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());

    // Remove the fragment file
    match fs::remove_file(&full_file_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Fragment file doesn't exist - consider it successfully "deleted"
            Ok(())
        }
        Err(e) => Err(StorageError::Io(e)),
    }
}

/// Read a fragment from local storage — plain blocking read.
/// Async callers must wrap this appropriately for their runtime flavor.
pub fn read_fragment(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<Vec<u8>, StorageError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());
    fs::read(&full_file_path).map_err(StorageError::Io)
}

/// Fetch and verify a fragment from local storage
/// Returns the fragment data if found locally and hash matches, otherwise returns an error
pub fn fetch_and_verify_fragment(
    fragment_hash: &Blake3Hash,
    fragments_dir: &str,
) -> Result<Vec<u8>, StorageError> {
    let chunk_data = read_fragment(fragments_dir, fragment_hash)?;

    // Verify chunk hash matches expected
    let actual_chunk_hash = Blake3Hash::new(blake3::hash(&chunk_data));
    if actual_chunk_hash != *fragment_hash {
        tracing::error!(
            "Fragment hash mismatch: expected {:?}, got {:?}",
            fragment_hash,
            actual_chunk_hash
        );
        return Err(StorageError::HashMismatch);
    }

    Ok(chunk_data)
}

/// Check if a fragment exists on disk and is valid (hash matches)
pub fn fragment_exists_and_valid(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    fetch_and_verify_fragment(fragment_hash, fragments_dir).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_read_verify_delete_cycle() {
        // Should: store → verify-read round-trips; corruption reads as HashMismatch;
        // delete is idempotent.
        // Impact: the fragment store is the data plane's disk truth.
        let dir =
            std::env::temp_dir().join(format!("hopnet-fragstore-test-{}", std::process::id()));
        let dir = dir.to_str().unwrap().to_string();

        let data = b"fragment bytes".to_vec();
        let hash = Blake3Hash::new(blake3::hash(&data));

        store_fragment(&dir, &hash, data.clone()).unwrap();
        assert!(fragment_exists_and_valid(&dir, &hash));
        assert_eq!(fetch_and_verify_fragment(&hash, &dir).unwrap(), data);

        // Corrupt in place → verify must fail
        let path = format!(
            "{}/{}",
            create_fragment_path(&dir, &hash).unwrap(),
            hash.to_hex()
        );
        fs::write(&path, b"corrupted").unwrap();
        assert!(matches!(
            fetch_and_verify_fragment(&hash, &dir),
            Err(StorageError::HashMismatch)
        ));

        delete_fragment(&dir, &hash).unwrap();
        delete_fragment(&dir, &hash).unwrap(); // idempotent
        assert!(!fragment_exists_and_valid(&dir, &hash));

        let _ = fs::remove_dir_all(&dir);
    }
}
