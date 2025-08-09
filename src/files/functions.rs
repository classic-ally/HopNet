use crate::db::Data;
use crate::types::Blake3Hash;
use aes_siv::{
    aead::{Aead, OsRng}, siv::Aes256Siv, Aes256SivAead, Key, KeyInit, Nonce
};
use chacha20poly1305::{
    aead::stream::{EncryptorBE32, DecryptorBE32},
    ChaCha20Poly1305
};
use duckdb::arrow::datatypes::ToByteSlice;
use rayon::prelude::*;
use rand::Rng;
use hex;
use std::fs;
use std::io;
use std::collections::HashMap;
use crate::db::CustomUUID;
use reed_solomon_simd::ReedSolomonDecoder;
use crate::AppState;
use crate::files::discovery::find_fragment;
use crate::files::placement::FragmentType;

#[derive(Debug)]
pub enum FileError {
    ShardingError,
    HashingError,
    InvalidChunkCount,
    TaskJoinError,
    EncryptionError,
    StorageError(io::Error),
    DatabaseError,
    NetworkError,
}

// Maximum fragment size for consumer network performance
pub const MAX_FRAGMENT_SIZE: usize = 4 * 1024 * 1024; // 4MB

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

pub fn calculate_chunk_padding(file_size: usize, num_chunks: usize) -> usize {
    // Calculate padding needed for the chosen number of chunks
    let mut remainder = if file_size == 0 {
        0
    } else {
        (num_chunks - (file_size % num_chunks)) % num_chunks
    };
    
    // Ensure chunk length is even
    let chunk_len_after_padding = if file_size + remainder == 0 {
        0
    } else {
        (file_size + remainder) / num_chunks
    };
    
    if chunk_len_after_padding % 2 != 0 {
        remainder += num_chunks;
    }

    return remainder
}

/// Calculate padding needed to ensure even chunk sizes
/// Returns (padded_file, added_bytes)
pub fn calculate_padding_and_chunks(mut file: Vec<u8>, num_chunks: usize) -> (Vec<Vec<u8>>, u8) {
    let original_len = file.len();
    
    let remainder = calculate_chunk_padding(original_len, num_chunks);

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

pub async fn shard_file(file: Vec<u8>, fragments_dir: &str, data_block_id: crate::db::CustomUUID, per_file_key: &chacha20poly1305::Key) -> Result<Option<Data>, FileError> {
    let fragments_dir = fragments_dir.to_string(); // Clone for move into closure
    let per_file_key = per_file_key.clone(); // Clone for move into closure
    let data_block_id_for_closure = data_block_id.clone(); // Clone for move into closure
    let data = tokio::task::spawn_blocking(move || -> Result<Option<_>, FileError> {
        // Handle empty files - no data record needed
        if file.is_empty() {
            return Ok(None);
        }
        
        // Hash the whole file with data_block_id appended for privacy
        let whole_file_hash = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&file);
            hasher.update(data_block_id_for_closure.as_bytes());
            Blake3Hash::new(hasher.finalize())
        };
        
        // Calculate optimal chunk sizes based on file size
        let original_len = file.len();
        let (num_original_chunks, num_recovery_chunks) = calculate_optimal_chunks(original_len);
        
        // Calculate padding and split into chunks
        let (chunks, added_bytes) = calculate_padding_and_chunks(file, num_original_chunks);

        if chunks.len() != num_original_chunks {
            return Err(FileError::InvalidChunkCount);
        }

        // Encrypt chunks in parallel and collect results
        let encryption_results: Vec<(crate::db::CustomUUID, Vec<u8>, Blake3Hash)> = chunks
            .into_par_iter()
            .map(|chunk| {
                let fragment_id = crate::db::CustomUUID::new(None);
                let encrypted_chunk = encrypt_chunk(chunk, &per_file_key, &fragment_id)?;
                let chunk_hash = Blake3Hash::new(blake3::hash(&encrypted_chunk));
                Ok((fragment_id, encrypted_chunk, chunk_hash))
            })
            .collect::<Result<Vec<_>, FileError>>()?;
        
        // Separate metadata from encrypted chunks to avoid clones
        let (original_fragment_metadata, encrypted_original_chunks): (Vec<_>, Vec<_>) = encryption_results
            .into_iter()
            .map(|(fragment_id, encrypted_chunk, chunk_hash)| {
                ((fragment_id, chunk_hash), encrypted_chunk)
            })
            .unzip();
        
        // Reed-Solomon encoding on encrypted chunks (takes ownership, frees memory)
        let recovery_chunks = reed_solomon_simd::encode(num_original_chunks, num_recovery_chunks, &encrypted_original_chunks)
            .map_err(|_| FileError::ShardingError)?;
        
        // Generate metadata for recovery chunks in parallel
        let recovery_fragment_metadata: Vec<(crate::db::CustomUUID, Blake3Hash)> = recovery_chunks
            .par_iter()
            .map(|chunk| {
                let fragment_id = crate::db::CustomUUID::new(None);
                let chunk_hash = Blake3Hash::new(blake3::hash(chunk));
                (fragment_id, chunk_hash)
            })
            .collect();
        
        // Store all fragments locally in parallel
        // Store original encrypted chunks
        encrypted_original_chunks.par_iter()
            .zip(original_fragment_metadata.par_iter())
            .try_for_each(|(encrypted_chunk, (fragment_id, chunk_hash))| {
                store_fragment(&fragments_dir, chunk_hash, encrypted_chunk.to_vec())
            })?;
        
        // Store recovery chunks
        recovery_chunks.par_iter()
            .zip(recovery_fragment_metadata.par_iter())
            .try_for_each(|(chunk, (fragment_id, chunk_hash))| {
                store_fragment(&fragments_dir, chunk_hash, chunk.to_vec())
            })?;
        
        // Build the result using array indexing with bounds checking
        if original_fragment_metadata.len() != num_original_chunks || recovery_fragment_metadata.len() != num_recovery_chunks {
            return Err(FileError::InvalidChunkCount);
        }

        Ok(Some((
            whole_file_hash,
            original_fragment_metadata,
            recovery_fragment_metadata,
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
    let (whole_file_hash, original_fragment_metadata, recovery_fragment_metadata, added_bytes) = data;

    // Combine original and recovery fragments into a single vector of FragmentHash
    let mut all_fragments = Vec::new();
    
    // Add original fragments (fragments 0, 1, 2...)
    for (index, (fragment_id, chunk_hash)) in original_fragment_metadata.into_iter().enumerate() {
        all_fragments.push(crate::db::FragmentHash {
            data_block_id: data_block_id.clone(),
            fragment_index: index as i32,
            fragment_id,
            fragment_hash: chunk_hash,
            chunk_type: crate::db::ChunkType::Original,
            stored_locally: false, // Will be verified by disk check later
        });
    }
    
    // Add recovery fragments (continue index from original chunks)
    let original_count = all_fragments.len();
    for (index, (fragment_id, chunk_hash)) in recovery_fragment_metadata.into_iter().enumerate() {
        all_fragments.push(crate::db::FragmentHash {
            data_block_id: data_block_id.clone(),
            fragment_index: (original_count + index) as i32,
            fragment_id,
            fragment_hash: chunk_hash,
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

    tracing::debug!("Encrypted path: {}", output_path);

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
pub fn store_fragment(fragments_dir: &str, fragment_hash: &Blake3Hash, data: Vec<u8>) -> Result<(), FileError> {
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

/// Delete a fragment from local storage
/// Simple deletion without directory cleanup for performance
pub fn delete_fragment(fragments_dir: &str, fragment_hash: &Blake3Hash) -> Result<(), FileError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());
    
    // Remove the fragment file
    match fs::remove_file(&full_file_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Fragment file doesn't exist - consider it successfully "deleted"
            Ok(())
        }
        Err(e) => Err(FileError::StorageError(e))
    }
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
pub fn fetch_and_verify_fragment(fragment_hash: &Blake3Hash, fragments_dir: &str) -> Result<Vec<u8>, FileError> {
    let chunk_data = fetch_fragment_local(fragments_dir, fragment_hash)?;
    
    // Verify chunk hash matches expected
    let actual_chunk_hash = Blake3Hash::new(blake3::hash(&chunk_data));
    if actual_chunk_hash != *fragment_hash {
        tracing::error!("Fragment hash mismatch: expected {:?}, got {:?}", fragment_hash, actual_chunk_hash);
        return Err(FileError::HashingError);
    }
    
    Ok(chunk_data)
}

/// Finalize reconstructed file by removing padding and verifying hash
fn finalize_file(mut file: Vec<u8>, added_bytes: u8, expected_hash: Blake3Hash, data_block_id: &crate::db::CustomUUID) -> Result<Vec<u8>, FileError> {
    // Remove padding
    if added_bytes > 0 {
        let final_length = file.len().saturating_sub(added_bytes as usize);
        file.truncate(final_length);
    }
    
    // Verify file hash (with data_block_id appended for privacy)
    let actual_hash = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&file);
        hasher.update(data_block_id.as_bytes());
        Blake3Hash::new(hasher.finalize())
    };
    if actual_hash != expected_hash {
        tracing::error!("File hash mismatch after reconstruction: expected {:?}, got {:?}", expected_hash, actual_hash);
        return Err(FileError::HashingError);
    }
    
    Ok(file)
}

/// Data structure for file reassembly containing organized fragment information
pub struct FileReassemblyData {
    pub original_fragments: HashMap<usize, (Blake3Hash, CustomUUID, bool)>,  // index -> (hash, exists_locally)
    pub recovery_fragments: HashMap<usize, (Blake3Hash, CustomUUID, bool)>,  // index -> (hash, exists_locally)
    pub added_bytes: u8,
    pub expected_file_hash: Blake3Hash,
    pub data_block_id: crate::db::CustomUUID,  // Needed for hash verification
    pub per_file_key: Option<chacha20poly1305::Key>,  // Decrypted per-file key for chunk decryption
    pub placement_height: Option<i32>,  // Consensus height when fragments were distributed
}

/// Data structure containing file metadata and access control information from database
pub struct FileAccessData {
    pub file_reassembly_data: FileReassemblyData,
    pub file_access_entry: Option<crate::db::types::FileAccess>,
}

/// Perform concurrent fragment discovery using work queue pattern with thread reuse
async fn perform_concurrent_fragment_discovery(
    file_data: &mut FileReassemblyData,
    fragments_dir: &str,
    min_needed_fragments: usize,
    app_state: &crate::AppState,
    consensus_height: i32,
) -> Result<(), FileError> {
    use crate::files::discovery::find_fragment;
    use crate::files::placement::FragmentType;
    
    // Get node metrics at consensus height for placement calculations
    let conn = app_state.db_pool.get()
        .map_err(|_| FileError::StorageError(io::Error::new(io::ErrorKind::Other, "Database connection failed")))?;
    
    let node_metrics = crate::db::metrics::get_all_node_metrics(Ok(conn), consensus_height)
        .map_err(|_| FileError::StorageError(io::Error::new(io::ErrorKind::Other, "Failed to get node metrics")))?;
    
    // Create authentication info for inter-node requests
    let auth = crate::NodeAuthInfo::from_app_state(app_state)
        .map_err(|_| FileError::StorageError(io::Error::new(io::ErrorKind::Other, "Failed to get auth info")))?;
    
    // Build list of missing fragments (prioritize originals over recovery)
    let mut missing_fragments = Vec::new();
    
    // Add missing original fragments first (avoid Reed-Solomon if possible)
    for (index, (hash, fragment_id, exists_locally)) in &file_data.original_fragments {
        if !exists_locally {
            missing_fragments.push((*index, *hash, fragment_id.clone(), FragmentType::Original));
        }
    }
    
    // Add missing recovery fragments
    for (index, (hash, fragment_id, exists_locally)) in &file_data.recovery_fragments {
        if !exists_locally {
            missing_fragments.push((*index, *hash, fragment_id.clone(), FragmentType::Recovery));
        }
    }
    
    // Create work queue for fragments to try
    let work_queue = std::sync::Arc::new(tokio::sync::Mutex::new(missing_fragments));
    let (success_tx, mut success_rx) = tokio::sync::mpsc::unbounded_channel();
    let successful_downloads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    
    // Spawn exactly min_needed_fragments worker threads
    let mut worker_handles = Vec::new();
    
    for worker_id in 0..min_needed_fragments {
        let tx = success_tx.clone();
        let queue = work_queue.clone();
        let node_metrics_clone = node_metrics.clone();
        let auth_clone = auth.clone();
        let fragments_dir_clone = fragments_dir.to_string();
        let db_pool = app_state.db_pool.clone();
        let successful_downloads_clone = successful_downloads.clone();
        
        let worker_handle = tokio::spawn(async move {
            tracing::debug!("Worker {} starting fragment discovery", worker_id);
            
            // Keep working until we have enough successful downloads or run out of work
            loop {
                // Check if we already have enough successful downloads
                if successful_downloads_clone.load(std::sync::atomic::Ordering::Relaxed) >= min_needed_fragments {
                    tracing::debug!("Worker {} stopping - enough fragments downloaded", worker_id);
                    break;
                }
                
                // Get next fragment to try from work queue
                let next_work = {
                    let mut queue_lock = queue.lock().await;
                    queue_lock.pop()
                };
                
                let (index, fragment_hash, fragment_id, fragment_type) = match next_work {
                    Some(work) => work,
                    None => {
                        tracing::debug!("Worker {} stopping - no more fragments to try", worker_id);
                        break;
                    }
                };
                
                tracing::debug!("Worker {} trying fragment {} (type: {:?})", worker_id, fragment_hash.to_hex(), fragment_type);
                
                // Try to find and fetch the fragment from network
                match find_fragment(&fragment_hash, fragment_type, &node_metrics_clone, &auth_clone).await {
                Ok(encrypted_data) => {
                        // Store fragment locally
                        if let Err(e) = store_fragment(&fragments_dir_clone, &fragment_hash, encrypted_data) {
                            tracing::error!("Worker {} failed to store fragment {}: {:?}", worker_id, fragment_hash.to_hex(), e);
                            let _ = tx.send(Err((index, fragment_type)));
                            continue; // Try next fragment
                        }
                        
                        tracing::info!("Worker {} successfully cached fragment {} from network", worker_id, fragment_hash.to_hex());
                        
                        // Update database to mark fragment as stored locally
                        let conn_result = db_pool.get();
                        if let Err(e) = crate::db::files::mark_fragment_local_state(conn_result, &fragment_hash, true) {
                            tracing::error!("Worker {} failed to update database for fragment {}: {:?}", worker_id, fragment_hash.to_hex(), e);
                            // Continue anyway - fragment is cached on disk
                        }
                        
                        // Increment successful downloads and report success
                        successful_downloads_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = tx.send(Ok((index, fragment_type, fragment_hash)));
                    }
                    Err(e) => {
                        tracing::warn!("Worker {} failed to discover fragment {}: {:?}", worker_id, fragment_hash.to_hex(), e);
                        let _ = tx.send(Err((index, fragment_type)));
                        // Continue loop to try next fragment
                    }
                }
            }
        });
        
        worker_handles.push(worker_handle);
    }
    
    drop(success_tx); // Close sender so channel ends when all workers complete
    
    // Collect results and update file_data
    let mut completed_downloads = 0;
    while let Some(result) = success_rx.recv().await {
        match result {
            Ok((index, fragment_type, fragment_hash)) => {
                // Update the exists_locally flag in file_data
                match fragment_type {
                    FragmentType::Original => {
                        if let Some((_, _, exists_locally)) = file_data.original_fragments.get_mut(&index) {
                            *exists_locally = true;
                        }
                    }
                    FragmentType::Recovery => {
                        if let Some((_, _, exists_locally)) = file_data.recovery_fragments.get_mut(&index) {
                            *exists_locally = true;
                        }
                    }
                }
                
                completed_downloads += 1;
                let total_successful = successful_downloads.load(std::sync::atomic::Ordering::Relaxed);
                tracing::debug!("Fragment discovery progress: {}/{} min needed, {} total successful", 
                               completed_downloads, min_needed_fragments, total_successful);
            }
            Err((index, fragment_type)) => {
                tracing::debug!("Failed to download fragment at index {} (type: {:?})", index, fragment_type);
            }
        }
    }
    
    // Wait for all worker threads to complete
    for worker_handle in worker_handles {
        let _ = worker_handle.await;
    }
    
    // Re-count total available fragments after discovery
    let new_total_local = file_data.original_fragments.values()
        .filter(|(_, _, exists_locally)| *exists_locally)
        .count() +
        file_data.recovery_fragments.values()
        .filter(|(_, _, exists_locally)| *exists_locally)
        .count();
    
    if new_total_local < file_data.original_fragments.len() {
        tracing::error!(
            "Insufficient fragments after discovery: have {}/{} needed",
            new_total_local, file_data.original_fragments.len()
        );
        return Err(FileError::ShardingError);
    }
    
    let final_successful = successful_downloads.load(std::sync::atomic::Ordering::Relaxed);
    tracing::info!(
        "Fragment discovery complete: successfully fetched {} fragments, now have {}/{} needed",
        final_successful, new_total_local, file_data.original_fragments.len()
    );
    
    Ok(())
}

/// Fetch a single fragment from network and cache it locally
async fn fetch_and_cache_fragment(
    fragment_hash: &Blake3Hash,
    fragments_dir: &str,
    app_state: &AppState,
    placement_height: Option<i32>,
    auth: &crate::NodeAuthInfo,
) -> Result<(), FileError> {
    // Get node metrics for fragment discovery
    let node_metrics = match placement_height {
        Some(height) => crate::db::metrics::get_all_node_metrics(app_state.db_pool.get(), height)
            .map_err(|_| FileError::DatabaseError)?,
        None => {
            tracing::warn!("No placement height available for fragment discovery");
            return Err(FileError::DatabaseError);
        }
    };

    // Try to find and fetch the fragment
    match find_fragment(fragment_hash, FragmentType::Original, &node_metrics, auth).await {
        Ok(fragment_data) => {
            // Store fragment locally
            store_fragment(fragments_dir, fragment_hash, fragment_data)?;
            
            // Update database to mark as stored locally
            if let Err(e) = crate::db::files::mark_fragment_local_state(app_state.db_pool.get(), fragment_hash, true) {
                tracing::warn!("Failed to update database for fragment {}: {:?}", fragment_hash.to_hex(), e);
            }
            
            tracing::debug!("Successfully fetched and cached fragment {}", fragment_hash.to_hex());
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to fetch fragment {} from network: {:?}", fragment_hash.to_hex(), e);
            Err(FileError::NetworkError)
        }
    }
}

/// Reassemble a complete file from fragments using Reed-Solomon reconstruction
/// Uses streaming approach to minimize memory usage with distributed fragment discovery
pub async fn reassemble_file(
    fragments_dir: &str,
    mut file_data: FileReassemblyData,
    app_state: Option<&crate::AppState>,
    consensus_height: Option<i32>,
) -> Result<Vec<u8>, FileError> {
    let num_original_chunks = file_data.original_fragments.len();
    let num_recovery_chunks = file_data.recovery_fragments.len();
    
    // Create authentication info for inter-node requests (if needed)
    let auth = if let Some(app_state) = app_state {
        Some(crate::NodeAuthInfo::from_app_state(app_state)
            .map_err(|_| FileError::DatabaseError)?)
    } else {
        None
    };
    
    // Handle empty file case
    if num_original_chunks == 0 {
        return Ok(Vec::new());
    }
    
    // Count total available fragments (original + recovery)
    let total_local_fragments = file_data.original_fragments.values()
        .filter(|(_, _, exists_locally)| *exists_locally)
        .count() +
        file_data.recovery_fragments.values()
        .filter(|(_, _, exists_locally)| *exists_locally)
        .count();
    
    // If we need more fragments for reconstruction, fetch them from network
    if total_local_fragments < num_original_chunks && app_state.is_some() {
        // Use placement_height from file data if available, otherwise use provided consensus_height
        let effective_height = file_data.placement_height.or(consensus_height);
        
        if let Some(height) = effective_height {
            let needed_fragments = num_original_chunks - total_local_fragments;
            
            tracing::info!(
                "Need {} more fragments for file reconstruction (have {}/{} needed) at consensus height {}",
                needed_fragments, total_local_fragments, num_original_chunks, height
            );
            
            // Perform concurrent fragment discovery
            perform_concurrent_fragment_discovery(
                &mut file_data,
                fragments_dir,
                needed_fragments,
                app_state.unwrap(),
                height,
            ).await?;
        } else {
            tracing::warn!("No consensus height available for distributed fragment discovery");
        }
    }
    
    // Check if all original chunks are available locally (fast path)
    let all_original_available = file_data.original_fragments.values()
        .all(|(_, _, exists_locally)| *exists_locally);
    
    if all_original_available {
        // Fast path: reconstruct by concatenating original chunks in order
        let mut reconstructed_file = Vec::new();
        
        for i in 0..num_original_chunks {
            if let Some((hash, fragment_id, _)) = file_data.original_fragments.get(&i) {
                let chunk_data = fetch_and_verify_fragment(hash, fragments_dir)?;
                
                if let Some(ref per_file_key) = file_data.per_file_key {
                    // Decrypt the chunk using the per-file key
                    tracing::debug!("Fast path: Decrypting chunk {} with fragment_id {}", i, fragment_id);
                    match decrypt_chunk(&chunk_data, per_file_key, fragment_id) {
                        Ok(decrypted_chunk_data) => {
                            reconstructed_file.extend_from_slice(&decrypted_chunk_data);
                        }
                        Err(e) => {
                            tracing::error!("Fast path: Failed to decrypt chunk {} with fragment_id {}: {:?}", i, fragment_id, e);
                            return Err(e);
                        }
                    }
                } else {
                    // No decryption needed (for backward compatibility or empty files)
                    reconstructed_file.extend_from_slice(&chunk_data);
                }
                // chunk_data is dropped here, minimizing memory usage
            } else {
                return Err(FileError::ShardingError);
            }
        }
        
        return finalize_file(reconstructed_file, file_data.added_bytes, file_data.expected_file_hash, &file_data.data_block_id);
    }
    
    // Slow path: need Reed-Solomon reconstruction
    // Count available fragments
    let available_original = file_data.original_fragments.values()
        .filter(|(_, _, exists_locally)| *exists_locally)
        .count();
    let available_recovery = file_data.recovery_fragments.values()
        .filter(|(_, _, exists_locally)| *exists_locally)
        .count();
    
    // Check if we have enough fragments total
    let total_available = available_original + available_recovery;
    if total_available < num_original_chunks {
        tracing::error!(
            "Insufficient fragments for Reed-Solomon reconstruction: have {} ({}+{} original+recovery), need {}",
            total_available, available_original, available_recovery, num_original_chunks
        );
        return Err(FileError::ShardingError);
    }
    
    // Collect available fragments for Reed-Solomon (need indexed chunks)
    let mut available_original = Vec::new();
    let mut available_recovery = Vec::new();
    
    // Add available original chunks with their indices
    for i in 0..num_original_chunks {
        if let Some((hash, fragment_id, exists_locally)) = file_data.original_fragments.get(&i) {
            if *exists_locally {
                match fetch_and_verify_fragment(hash, fragments_dir) {
                    Ok(chunk_data) => {
                        // Use original chunks in encrypted form for Reed-Solomon reconstruction
                        // They will be decrypted after reconstruction
                        tracing::debug!("Using original chunk {} in encrypted form for Reed-Solomon", i);
                        available_original.push((i, chunk_data));
                    }
                    Err(e) => {
                        tracing::warn!("Fragment {} marked as stored locally but not found on disk: {:?}. Updating database and attempting immediate fetch.", hash.to_hex(), e);
                        
                        // Update database to reflect that fragment is not actually stored locally
                        if let Some(app_state) = app_state {
                            if let Err(db_err) = crate::db::files::mark_fragment_local_state(app_state.db_pool.get(), hash, false) {
                                tracing::warn!("Failed to update database for missing fragment {}: {:?}", hash.to_hex(), db_err);
                            }
                        }
                        
                        // Try to fetch and cache the fragment immediately
                        if let (Some(app_state), Some(auth)) = (app_state, &auth) {
                            match fetch_and_cache_fragment(hash, fragments_dir, app_state, consensus_height, auth).await {
                                Ok(()) => {
                                    // Retry loading the fragment after caching
                                    match fetch_and_verify_fragment(hash, fragments_dir) {
                                        Ok(chunk_data) => {
                                            tracing::debug!("Successfully fetched and loaded original chunk {} after caching", i);
                                            available_original.push((i, chunk_data));
                                        }
                                        Err(e2) => {
                                            tracing::error!("Fragment {} still not available after fetch and cache: {:?}", hash.to_hex(), e2);
                                            return Err(FileError::ShardingError);
                                        }
                                    }
                                }
                                Err(fetch_err) => {
                                    tracing::error!("Failed to fetch fragment {} from network: {:?}", hash.to_hex(), fetch_err);
                                    return Err(FileError::ShardingError);
                                }
                            }
                        } else {
                            tracing::error!("Cannot fetch fragment {} - no app_state provided for network access", hash.to_hex());
                            return Err(FileError::ShardingError);
                        }
                    }
                }
            }
        }
    }
    
    // Add available recovery chunks with their indices
    for i in 0..num_recovery_chunks {
        // Recovery chunks are stored with offset in database (after original chunks)
        let database_index = i + num_original_chunks;
        if let Some((hash, fragment_id, exists_locally)) = file_data.recovery_fragments.get(&database_index) {
            if *exists_locally {
                match fetch_and_verify_fragment(hash, fragments_dir) {
                    Ok(chunk_data) => {
                        // Recovery chunks are not encrypted with per-chunk keys
                        // They are the output of Reed-Solomon encoding on already-encrypted original chunks
                        tracing::debug!("Using recovery chunk {} (database index {}) directly (no decryption needed)", i, database_index);
                        available_recovery.push((i, chunk_data));
                    }
                    Err(e) => {
                        tracing::warn!("Fragment {} marked as stored locally but not found on disk: {:?}. Updating database and attempting immediate fetch.", hash.to_hex(), e);
                        
                        // Update database to reflect that fragment is not actually stored locally
                        if let Some(app_state) = app_state {
                            if let Err(db_err) = crate::db::files::mark_fragment_local_state(app_state.db_pool.get(), hash, false) {
                                tracing::warn!("Failed to update database for missing fragment {}: {:?}", hash.to_hex(), db_err);
                            }
                        }
                        
                        // Try to fetch and cache the fragment immediately
                        if let (Some(app_state), Some(auth)) = (app_state, &auth) {
                            match fetch_and_cache_fragment(hash, fragments_dir, app_state, consensus_height, auth).await {
                                Ok(()) => {
                                    // Retry loading the fragment after caching
                                    match fetch_and_verify_fragment(hash, fragments_dir) {
                                        Ok(chunk_data) => {
                                            tracing::debug!("Successfully fetched and loaded recovery chunk {} (database index {}) after caching", i, database_index);
                                            available_recovery.push((i, chunk_data));
                                        }
                                        Err(e2) => {
                                            tracing::error!("Fragment {} still not available after fetch and cache: {:?}", hash.to_hex(), e2);
                                            return Err(FileError::ShardingError);
                                        }
                                    }
                                }
                                Err(fetch_err) => {
                                    tracing::error!("Failed to fetch fragment {} from network: {:?}", hash.to_hex(), fetch_err);
                                    return Err(FileError::ShardingError);
                                }
                            }
                        } else {
                            tracing::error!("Cannot fetch fragment {} - no app_state provided for network access", hash.to_hex());
                            return Err(FileError::ShardingError);
                        }
                    }
                }
            }
        }
    }
    
    // Perform Reed-Solomon reconstruction on encrypted chunks using streaming decoder
    let chunk_len = if let Some((_, first_chunk)) = available_original.first() {
        first_chunk.len()
    } else if let Some((_, first_chunk)) = available_recovery.first() {
        first_chunk.len()
    } else {
        return Err(FileError::ShardingError); // No chunks available
    };
    
    let mut decoder = ReedSolomonDecoder::new(num_original_chunks, num_recovery_chunks, chunk_len)
        .map_err(|_| FileError::ShardingError)?;
    
    // Add available original chunks
    for (index, chunk_data) in &available_original {
        decoder.add_original_shard(*index, chunk_data)
            .map_err(|_| FileError::ShardingError)?;
    }
    
    // Add available recovery chunks  
    for (index, chunk_data) in available_recovery {
        decoder.add_recovery_shard(index, &chunk_data)
            .map_err(|_| FileError::ShardingError)?;
    }
    
    // Perform the reconstruction
    let decoder_result = decoder.decode()
        .map_err(|_| FileError::ShardingError)?;
    
    // Build a map of all original chunks (existing + reconstructed)
    let mut reconstructed_map = std::collections::HashMap::new();
    
    // First, add any restored/reconstructed chunks
    for (index, chunk_data) in decoder_result.restored_original_iter() {
        tracing::debug!("Reed-Solomon reconstructed chunk {} ({} bytes)", index, chunk_data.len());
        reconstructed_map.insert(index, chunk_data.to_vec());
    }
    
    // Then add any original chunks we already had (they might not be in restored_original_iter)
    for (index, chunk_data) in &available_original {
        if !reconstructed_map.contains_key(index) {
            tracing::debug!("Using locally available chunk {} ({} bytes)", index, chunk_data.len());
        }
        reconstructed_map.entry(*index).or_insert_with(|| chunk_data.clone());
    }
    
    // Decrypt the reconstructed chunks and concatenate them
    let mut reconstructed_file = Vec::new();
    for i in 0..num_original_chunks {
        if let Some(encrypted_chunk) = reconstructed_map.get(&i) {
            if let Some(ref per_file_key) = file_data.per_file_key {
                // Find the fragment_id for this chunk
                if let Some((chunk_hash, fragment_id, _)) = file_data.original_fragments.get(&i) {
                    tracing::debug!("Decrypting reconstructed chunk {} with fragment_id {} (hash: {}, len: {} bytes)", 
                                   i, fragment_id, chunk_hash.to_hex(), encrypted_chunk.len());
                    match decrypt_chunk(encrypted_chunk, per_file_key, fragment_id) {
                        Ok(decrypted_chunk_data) => {
                            reconstructed_file.extend_from_slice(&decrypted_chunk_data);
                        }
                        Err(e) => {
                            tracing::error!("Failed to decrypt reconstructed chunk {} with fragment_id {}: {:?}", i, fragment_id, e);
                            return Err(e);
                        }
                    }
                } else {
                    return Err(FileError::ShardingError); // Missing fragment metadata
                }
            } else {
                // No decryption needed (for backward compatibility or empty files)
                reconstructed_file.extend_from_slice(encrypted_chunk);
            }
        } else {
            return Err(FileError::ShardingError); // Missing chunk after reconstruction
        }
    }
    
    finalize_file(reconstructed_file, file_data.added_bytes, file_data.expected_file_hash, &file_data.data_block_id)
}

/// Check if a fragment exists on disk and is valid (hash matches)
pub fn fragment_exists_and_valid(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    match fetch_and_verify_fragment(fragment_hash, fragments_dir) {
        Ok(_) => true,  // Exists and hash matches
        Err(_) => false // Missing, unreadable, or corrupted
    }
}

/// Derive chunk encryption key from per-file key and fragment UUID
pub fn derive_chunk_key(per_file_key: &chacha20poly1305::Key, fragment_id: &crate::db::CustomUUID) -> chacha20poly1305::Key {
    let mut key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet chunk_key");
    hasher.update(per_file_key);
    hasher.update(fragment_id.as_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut key_bytes);
    key_bytes.into()
}

/// Derive nonce from fragment UUID using Blake3 for collision resistance
pub fn derive_chunk_nonce(fragment_id: &crate::db::CustomUUID) -> [u8; 7] {
    let mut nonce_bytes = [0u8; 7];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet chunk_nonce");
    hasher.update(fragment_id.as_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut nonce_bytes);
    nonce_bytes
}

const BUFFER_SIZE: usize = 4096;

pub fn calculate_encrypted_chunk_length(
    chunk_length: usize
) -> usize {
    chunk_length + (((chunk_length + BUFFER_SIZE - 1) / BUFFER_SIZE) * 16)
}

/// Encrypt a chunk using streaming ChaCha20-Poly1305 with true memory efficiency
pub fn encrypt_chunk(
    mut chunk: Vec<u8>,  // Take ownership so we can consume it
    per_file_key: &chacha20poly1305::Key, 
    fragment_id: &crate::db::CustomUUID
) -> Result<Vec<u8>, FileError> {
    let chunk_key = derive_chunk_key(per_file_key, fragment_id);
    let nonce = derive_chunk_nonce(fragment_id);
    let cipher = ChaCha20Poly1305::new(&chunk_key);
    
    let mut stream_encryptor = EncryptorBE32::from_aead(cipher, nonce.as_ref().into());
    let mut encrypted_output = Vec::with_capacity(chunk.len() + 16);
    
    // Process all segments except the last one
    while chunk.len() > BUFFER_SIZE {
        let segment: Vec<u8> = chunk.drain(0..BUFFER_SIZE).collect();
        
        let ciphertext = stream_encryptor
            .encrypt_next(segment.as_slice())
            .map_err(|_| FileError::EncryptionError)?;
        
        encrypted_output.extend_from_slice(&ciphertext);
        // segment is dropped here, freeing memory
    }
    
    // Process the last segment (or entire chunk if smaller than BUFFER_SIZE)
    if !chunk.is_empty() {
        let ciphertext = stream_encryptor
            .encrypt_last(chunk.as_slice())
            .map_err(|_| FileError::EncryptionError)?;
        
        encrypted_output.extend_from_slice(&ciphertext);
    }
    
    Ok(encrypted_output)
}

/// Decrypt a chunk using streaming ChaCha20-Poly1305 with memory efficiency
pub fn decrypt_chunk(
    encrypted_chunk: &[u8], 
    per_file_key: &chacha20poly1305::Key, 
    fragment_id: &crate::db::CustomUUID
) -> Result<Vec<u8>, FileError> {
    let chunk_key = derive_chunk_key(per_file_key, fragment_id);
    let nonce = derive_chunk_nonce(fragment_id);
    let cipher = ChaCha20Poly1305::new(&chunk_key);
    
    let mut stream_decryptor = DecryptorBE32::from_aead(cipher, nonce.as_ref().into());
    let mut decrypted_output = Vec::with_capacity(encrypted_chunk.len());
    
    const BUFFER_SIZE: usize = 4096;
    const ENCRYPTED_SEGMENT_SIZE: usize = BUFFER_SIZE + 16; // Each segment has 16-byte auth tag
    let mut chunk_offset = 0;
    
    // Process all segments except the last one  
    while chunk_offset + ENCRYPTED_SEGMENT_SIZE < encrypted_chunk.len() {
        let segment = &encrypted_chunk[chunk_offset..chunk_offset + ENCRYPTED_SEGMENT_SIZE];
        
        let plaintext = stream_decryptor
            .decrypt_next(segment)
            .map_err(|_| FileError::EncryptionError)?;
        
        decrypted_output.extend_from_slice(&plaintext);
        chunk_offset += ENCRYPTED_SEGMENT_SIZE;
    }
    
    // Process the last segment (or entire chunk if smaller than ENCRYPTED_SEGMENT_SIZE)
    if chunk_offset < encrypted_chunk.len() {
        let segment = &encrypted_chunk[chunk_offset..];
        
        let plaintext = stream_decryptor
            .decrypt_last(segment)
            .map_err(|_| FileError::EncryptionError)?;
        
        decrypted_output.extend_from_slice(&plaintext);
    }
    
    Ok(decrypted_output)
}
