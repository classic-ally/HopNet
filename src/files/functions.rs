use crate::db::Data;
use crate::types::Blake3Hash;
use aes_siv::{
    aead::{Aead, OsRng}, siv::Aes256Siv, Aes256SivAead, Key, KeyInit, Nonce
};
use duckdb::arrow::datatypes::ToByteSlice;
use rayon::prelude::*;
use rand::Rng;
use hex;
use std::path::Path;
use std::fs;
use std::io;
use std::collections::HashMap;

#[derive(Debug)]
pub enum FileError {
    ShardingError,
    HashingError,
    InvalidChunkCount,
    TaskJoinError,
    EncryptionError,
    StorageError(io::Error)
}

// Maximum fragment size for consumer network performance
const MAX_FRAGMENT_SIZE: usize = 64 * 1024 * 1024; // 64MB

/// Calculate optimal number of original and recovery chunks based on file size
pub fn calculate_optimal_chunks(file_size: usize) -> (usize, usize) {
    if file_size == 0 {
        return (0, 0); // Empty files have no chunks
    }
    
    // Calculate minimum chunks needed to stay under fragment size limit
    let min_original_chunks = (file_size + MAX_FRAGMENT_SIZE - 1) / MAX_FRAGMENT_SIZE;
    
    // Ensure at least 10 original chunks for good Reed-Solomon efficiency
    let original_chunks = min_original_chunks.max(10);
    
    // Use 2:1 redundancy ratio (2 recovery for every 1 original)
    let recovery_chunks = original_chunks * 2;
    
    (original_chunks, recovery_chunks)
}

/// Calculate padding needed to ensure even chunk sizes
/// Returns (padded_file, added_bytes)
pub fn calculate_padding_and_chunks(mut file: Vec<u8>, num_chunks: usize) -> (Vec<Vec<u8>>, u8) {
    let original_len = file.len();
    
    // Calculate padding needed for the chosen number of chunks
    let mut remainder = if original_len == 0 {
        0
    } else {
        (num_chunks - (original_len % num_chunks)) % num_chunks
    };
    
    // Ensure chunk length is even
    let chunk_len_after_padding = if original_len + remainder == 0 {
        0
    } else {
        (original_len + remainder) / num_chunks
    };
    
    if chunk_len_after_padding % 2 != 0 {
        remainder += num_chunks;
    }
    
    // Apply padding in one go
    if remainder > 0 {
        file.resize(original_len + remainder, 0);
    }
    let added_bytes = remainder as u8;
    
    // Split into chunks
    let chunks = if file.is_empty() {
        vec![vec![]; num_chunks] // Empty chunks for empty file
    } else {
        let chunk_size = file.len() / num_chunks;
        let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(num_chunks);
        while !file.is_empty() {
            let current_len = file.len();
            let chunk = file.split_off(current_len - chunk_size);
            chunks.push(chunk);
        }
        chunks.reverse();
        chunks
    };
    
    (chunks, added_bytes)
}

impl From<FileError> for duckdb::Error {
    fn from(err: FileError) -> Self {
        duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(duckdb::ffi::DuckDBError),
            Some(format!("File operation failed: {:?}", err))
        )
    }
}

pub async fn shard_file(file: Vec<u8>, fragments_dir: &str, data_block_id: crate::db::CustomUUID) -> Result<Option<Data>, FileError> {
    let fragments_dir = fragments_dir.to_string(); // Clone for move into closure
    let data = tokio::task::spawn_blocking(move || -> Result<Option<_>, FileError> {
        // Handle empty files - no data record needed
        if file.is_empty() {
            return Ok(None);
        }
        
        // Hash the whole file first
        let whole_file_hash = Blake3Hash::new(blake3::hash(&file));
        
        // Calculate optimal chunk sizes based on file size
        let original_len = file.len();
        let (num_original_chunks, num_recovery_chunks) = calculate_optimal_chunks(original_len);
        
        // Calculate padding and split into chunks
        let (chunks, added_bytes) = calculate_padding_and_chunks(file, num_original_chunks);

        if chunks.len() != num_original_chunks {
            return Err(FileError::InvalidChunkCount);
        }

        // Hash original chunks in parallel using rayon (CPU-bound work)
        let original_hashes: Vec<Blake3Hash> = chunks
            .par_iter()
            .map(|chunk| Blake3Hash::new(blake3::hash(chunk)))
            .collect();
        
        // Reed-Solomon encoding (CPU-bound, keep synchronous)
        let recovery_chunks = reed_solomon_simd::encode(num_original_chunks, num_recovery_chunks, &chunks)
            .map_err(|_| FileError::ShardingError)?;
        
        // Hash recovery chunks in parallel
        let recovery_hashes: Vec<Blake3Hash> = recovery_chunks
            .par_iter()
            .map(|chunk| Blake3Hash::new(blake3::hash(chunk)))
            .collect();
        
        // Store all fragments locally in parallel
        // Store original chunks
        chunks.par_iter().zip(original_hashes.par_iter())
            .try_for_each(|(chunk, hash)| store_fragment(&fragments_dir, hash, chunk))?;
        
        // Store recovery chunks
        recovery_chunks.par_iter().zip(recovery_hashes.par_iter())
            .try_for_each(|(chunk, hash)| store_fragment(&fragments_dir, hash, chunk))?;
        
        // Build the result using array indexing with bounds checking
        if original_hashes.len() != num_original_chunks || recovery_hashes.len() != num_recovery_chunks {
            return Err(FileError::InvalidChunkCount);
        }

        Ok(Some((
            whole_file_hash,
            original_hashes,
            recovery_hashes,
            added_bytes
        )))

    })
    .await
    .map_err(|_| FileError::TaskJoinError)??;

    // Handle empty file case
    let data = match data {
        Some(data) => data,
        None => return Ok(None), // Empty file, no data record
    };
    
    // Destructure the results from the blocking task
    let (whole_file_hash, original_hashes, recovery_hashes, added_bytes) = data;

    // Combine original and recovery hashes into a single vector of FragmentHash
    let mut all_fragments = Vec::new();
    
    // Add original hashes (fragments 0, 1, 2...)
    for (index, hash) in original_hashes.into_iter().enumerate() {
        all_fragments.push(crate::db::FragmentHash {
            data_block_id: data_block_id.clone(),
            fragment_index: index as i32,
            fragment_hash: hash,
            chunk_type: crate::db::ChunkType::Original,
            stored_locally: false, // Will be verified by disk check later
        });
    }
    
    // Add recovery hashes (continue index from original chunks)
    let original_count = all_fragments.len();
    for (index, hash) in recovery_hashes.into_iter().enumerate() {
        all_fragments.push(crate::db::FragmentHash {
            data_block_id: data_block_id.clone(),
            fragment_index: (original_count + index) as i32,
            fragment_hash: hash,
            chunk_type: crate::db::ChunkType::Recovery,
            stored_locally: false, // Will be verified by disk check later
        });
    }
    
    let data = Data {
        hash: whole_file_hash,
        fragments: all_fragments,
        added_bytes: added_bytes,
    };
    
    Ok(Some(data))
}

pub fn generate_siv_nonce() -> Nonce {
    // we generate this SIV nonce once for each user
    // it's stored for the user forever + synced between nodes
    // probably not needed for security, but defence-in-depth?
    let mut rng = rand::rng();

    let random_value: u128 = rng.random();
    let random_bytes = random_value.to_be_bytes();
    let nonce = Nonce::from_slice(&random_bytes).clone();
    return nonce;
}

pub fn generate_siv_key() -> Key<Aes256Siv> {
    let key: Key<Aes256Siv> = Aes256SivAead::generate_key(&mut OsRng);
    return key;
}

pub async fn encrypt_path(
    path: String,
    key: &Key<Aes256Siv>,
    nonce: &Nonce
) -> Result<String, FileError> {
    let mut output_path: String = "".to_string();

    let split_path = path.split('/').collect::<Vec<&str>>();
    dbg!(split_path.len());
    if split_path.len() > 1 {
        for part in split_path {
            if part.len() != 0 {
                let encrypted_part = encrypt_part(part, &key, nonce).await?;
                output_path = output_path + &encrypted_part;
            }
        }
    } else {
        output_path = output_path + "/";
    }

    dbg!(&output_path);

    Ok(output_path)
}

pub async fn encrypt_part(
    part: &str,
    key: &Key<Aes256Siv>,
    nonce: &Nonce
) -> Result<String, FileError> {
    let cipher = Aes256SivAead::new(&key);
    let ciphertext = cipher.encrypt(nonce, part.as_bytes()).map_err(|_| FileError::EncryptionError)?;
    // we encode as hex to enable splitting by /
    // base64 more space efficient but collisions
    let base64_str = hex::encode(ciphertext);
    let this_part = "/".to_string() + &base64_str;
    Ok(this_part)
}

pub fn decrypt_path(
    enc_path: String,
    key: &Key<Aes256Siv>,
    nonce: &Nonce
) -> Result<String, FileError> {
    let mut output_path: String = "".to_string();

    let split_path = enc_path.split('/').collect::<Vec<&str>>();
    if split_path.len() > 1 {
        for part in split_path {
            if part.len() != 0 {
                let decrypted_part = decrypt_part(part, &key, nonce)?;
                output_path = output_path + "/" + &decrypted_part;
            }
        }
    } else {
        output_path = output_path + "/"
    }

    Ok(output_path)
}

pub fn decrypt_part(
    part: &str,
    key: &Key<Aes256Siv>,
    nonce: &Nonce
) -> Result<String, FileError> {
    let cipher = Aes256SivAead::new(key);
    match hex::decode(part) {
        Ok(binary) => {
            match cipher.decrypt(nonce, binary.to_byte_slice()) {
                Ok(bytes) => {
                    let string = String::from_utf8(bytes).map_err(|_| FileError::EncryptionError)?;
                    Ok(string)
                }
                Err(_) => Err(FileError::EncryptionError)
            }
        }
        Err(_) => Err(FileError::EncryptionError)
    }
}

/// Get the XDG data directory for storing fragments
pub fn get_fragments_dir() -> Result<String, FileError> {
    let data_dir = std::env::var("XDG_DATA_HOME")
        .unwrap_or_else(|_| format!("{}/.local/share", std::env::var("HOME").unwrap_or_else(|_| ".".to_string())));
    
    let fragments_dir = format!("{}/hopnet/fragments", data_dir);
    println!("Using fragments directory: {}", fragments_dir);
    Ok(fragments_dir)
}

/// Create 2-level directory structure for a fragment hash
/// e.g., "abcdef123..." -> "fragments/ab/cd/"
pub fn create_fragment_path(fragments_dir: &str, fragment_hash: &Blake3Hash) -> Result<String, FileError> {
    let hash_str = fragment_hash.to_hex();
    
    // Take first 4 hex characters for 2-level nesting
    if hash_str.len() < 4 {
        return Err(FileError::StorageError(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Fragment hash too short"
        )));
    }
    
    let first_level = &hash_str[0..2];
    let second_level = &hash_str[2..4];
    
    let full_path = format!("{}/{}/{}", fragments_dir, first_level, second_level);
    
    Ok(full_path)
}

/// Store a fragment to disk using 2-level directory structure
pub fn store_fragment(fragments_dir: &str, fragment_hash: &Blake3Hash, data: &[u8]) -> Result<(), FileError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());
    
    // Create directory structure if it doesn't exist
    fs::create_dir_all(&dir_path)
        .map_err(|e| FileError::StorageError(e))?;
    
    // Write the fragment data
    fs::write(&full_file_path, data)
        .map_err(|e| FileError::StorageError(e))?;
    
    Ok(())
}

/// Fetch a fragment from local storage only
/// Returns the fragment data if found locally, otherwise returns an error
pub fn fetch_fragment_local(fragments_dir: &str, fragment_hash: &Blake3Hash) -> Result<Vec<u8>, FileError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());
    
    // Read the fragment data
    fs::read(&full_file_path)
        .map_err(|e| FileError::StorageError(e))
}

/// Fetch and verify a fragment from local storage
/// Returns the fragment data if found locally and hash matches, otherwise returns an error
fn fetch_and_verify_fragment(fragments_dir: &str, fragment_hash: &Blake3Hash) -> Result<Vec<u8>, FileError> {
    let chunk_data = fetch_fragment_local(fragments_dir, fragment_hash)?;
    
    // Verify chunk hash matches expected
    let actual_chunk_hash = Blake3Hash::new(blake3::hash(&chunk_data));
    if actual_chunk_hash != *fragment_hash {
        return Err(FileError::HashingError);
    }
    
    Ok(chunk_data)
}

/// Finalize reconstructed file by removing padding and verifying hash
fn finalize_file(mut file: Vec<u8>, added_bytes: u8, expected_hash: Blake3Hash) -> Result<Vec<u8>, FileError> {
    // Remove padding
    if added_bytes > 0 {
        let final_length = file.len().saturating_sub(added_bytes as usize);
        file.truncate(final_length);
    }
    
    // Verify file hash
    let actual_hash = Blake3Hash::new(blake3::hash(&file));
    if actual_hash != expected_hash {
        return Err(FileError::HashingError);
    }
    
    Ok(file)
}

/// Data structure for file reassembly containing organized fragment information
pub struct FileReassemblyData {
    pub original_fragments: HashMap<usize, (Blake3Hash, bool)>,  // index -> (hash, exists_locally)
    pub recovery_fragments: HashMap<usize, (Blake3Hash, bool)>,  // index -> (hash, exists_locally)
    pub added_bytes: u8,
    pub expected_file_hash: Blake3Hash,
}

/// Reassemble a complete file from fragments using Reed-Solomon reconstruction
/// Uses streaming approach to minimize memory usage
pub fn reassemble_file(
    fragments_dir: &str,
    file_data: FileReassemblyData,
) -> Result<Vec<u8>, FileError> {
    let num_original_chunks = file_data.original_fragments.len();
    let num_recovery_chunks = file_data.recovery_fragments.len();
    
    // Handle empty file case
    if num_original_chunks == 0 {
        return Ok(Vec::new());
    }
    
    // Check if all original chunks are available locally (fast path)
    let all_original_available = file_data.original_fragments.values()
        .all(|(_, exists_locally)| *exists_locally);
    
    if all_original_available {
        // Fast path: reconstruct by concatenating original chunks in order
        let mut reconstructed_file = Vec::new();
        
        for i in 0..num_original_chunks {
            if let Some((hash, _)) = file_data.original_fragments.get(&i) {
                let chunk_data = fetch_and_verify_fragment(fragments_dir, hash)?;
                reconstructed_file.extend_from_slice(&chunk_data);
                // chunk_data is dropped here, minimizing memory usage
            } else {
                return Err(FileError::ShardingError);
            }
        }
        
        return finalize_file(reconstructed_file, file_data.added_bytes, file_data.expected_file_hash);
    }
    
    // Slow path: need Reed-Solomon reconstruction
    // Count available fragments
    let available_original = file_data.original_fragments.values()
        .filter(|(_, exists_locally)| *exists_locally)
        .count();
    let available_recovery = file_data.recovery_fragments.values()
        .filter(|(_, exists_locally)| *exists_locally)
        .count();
    
    // Check if we have enough fragments total
    let total_available = available_original + available_recovery;
    if total_available < num_original_chunks {
        return Err(FileError::ShardingError);
    }
    
    // Collect available fragments for Reed-Solomon (need indexed chunks)
    let mut available_original = Vec::new();
    let mut available_recovery = Vec::new();
    
    // Add available original chunks with their indices
    for i in 0..num_original_chunks {
        if let Some((hash, exists_locally)) = file_data.original_fragments.get(&i) {
            if *exists_locally {
                let chunk_data = fetch_and_verify_fragment(fragments_dir, hash)?;
                available_original.push((i, chunk_data));
            }
        }
    }
    
    // Add available recovery chunks with their indices
    for i in 0..num_recovery_chunks {
        if let Some((hash, exists_locally)) = file_data.recovery_fragments.get(&i) {
            if *exists_locally {
                let chunk_data = fetch_and_verify_fragment(fragments_dir, hash)?;
                available_recovery.push((i, chunk_data));
            }
        }
    }
    
    // Perform Reed-Solomon reconstruction
    let reconstructed_map = reed_solomon_simd::decode(num_original_chunks, num_recovery_chunks, available_original, available_recovery)
        .map_err(|_| FileError::ShardingError)?;
    
    // Convert HashMap to ordered vector and concatenate chunks
    let mut reconstructed_file = Vec::new();
    for i in 0..num_original_chunks {
        if let Some(chunk) = reconstructed_map.get(&i) {
            reconstructed_file.extend_from_slice(chunk);
        } else {
            return Err(FileError::ShardingError); // Missing chunk after reconstruction
        }
    }
    
    finalize_file(reconstructed_file, file_data.added_bytes, file_data.expected_file_hash)
}

/// Check if a fragment exists on disk and is valid (hash matches)
pub fn fragment_exists_and_valid(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    match fetch_and_verify_fragment(fragments_dir, fragment_hash) {
        Ok(_) => true,  // Exists and hash matches
        Err(_) => false // Missing, unreadable, or corrupted
    }
}
