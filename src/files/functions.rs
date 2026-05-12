use crate::AppState;
use crate::db::CustomUUID;
use crate::files::discovery::find_fragment;
use crate::files::placement::FragmentType;
use crate::types::Blake3Hash;
use aes_siv::{
    Aes256SivAead, Key, KeyInit, Nonce,
    aead::{Aead, OsRng},
    siv::Aes256Siv,
};
use chacha20poly1305::{
    ChaCha20Poly1305,
    aead::stream::{DecryptorBE32, EncryptorBE32},
};
use hex;
use rand::Rng;
use reed_solomon_simd::ReedSolomonDecoder;
use std::collections::HashMap;
use std::fs;
use std::io;

#[derive(Debug)]
pub enum FileError {
    ShardingError,
    HashingError,
    HashMismatch,
    InvalidChunkCount,
    TaskJoinError,
    EncryptionError,
    StorageError(io::Error),
    DatabaseError,
    NetworkError,
    ReconstructionTimeout,
}

impl std::fmt::Display for FileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileError::ShardingError => write!(f, "Sharding error"),
            FileError::HashingError => write!(f, "Hashing error"),
            FileError::HashMismatch => write!(f, "Hash mismatch"),
            FileError::InvalidChunkCount => write!(f, "Invalid chunk count"),
            FileError::TaskJoinError => write!(f, "Task join error"),
            FileError::EncryptionError => write!(f, "Encryption error"),
            FileError::StorageError(e) => write!(f, "Storage error: {}", e),
            FileError::DatabaseError => write!(f, "Database error"),
            FileError::NetworkError => write!(f, "Network error"),
            FileError::ReconstructionTimeout => write!(f, "Reconstruction timeout"),
        }
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileError::StorageError(e) => Some(e),
            _ => None,
        }
    }
}

// Fundamental constraint: individual fragment size limit for network performance
pub const MAX_FRAGMENT_SIZE: usize = 4 * 1024 * 1024; // 4MB

// Fixed fragment count per chunk for predictable modulo placement
pub const ORIGINAL_FRAGMENTS_PER_CHUNK: usize = 10;
pub const RECOVERY_FRAGMENTS_PER_CHUNK: usize = 20;
pub const TOTAL_FRAGMENTS_PER_CHUNK: usize =
    ORIGINAL_FRAGMENTS_PER_CHUNK + RECOVERY_FRAGMENTS_PER_CHUNK;

// Derived: logical chunk size is constrained by fragment size and count
// This ensures each fragment in a full chunk is exactly MAX_FRAGMENT_SIZE
pub const CHUNK_SIZE: usize = MAX_FRAGMENT_SIZE * ORIGINAL_FRAGMENTS_PER_CHUNK; // 40MB

/// Calculate chunked Reed-Solomon parameters based on file size
/// Returns (num_chunks, total_original_fragments, total_recovery_fragments)
///
/// Each chunk is encoded independently with 10 original + 20 recovery fragments
/// Chunk size is fixed at 40MB, so files >40MB are split into multiple chunks
///
/// Special case: Returns (0, 0, 0) for empty files, which should be handled
/// separately without Reed-Solomon encoding (no fragments created)
pub fn calculate_chunked_fragments(file_size: usize) -> (usize, usize, usize) {
    // Empty files: no chunks, no fragments
    // These are handled specially in the upload path (skipping process_uploaded_file entirely)
    if file_size == 0 {
        return (0, 0, 0);
    }

    // Calculate number of logical chunks
    let num_chunks = file_size.div_ceil(CHUNK_SIZE);

    // Each chunk has fixed 10 original + 20 recovery fragments
    let total_original = num_chunks * ORIGINAL_FRAGMENTS_PER_CHUNK;
    let total_recovery = num_chunks * RECOVERY_FRAGMENTS_PER_CHUNK;

    (num_chunks, total_original, total_recovery)
}

/// Calculate optimal number of original and recovery chunks based on file size
/// DEPRECATED: Use calculate_chunked_fragments() instead for Phase 4+
pub fn calculate_optimal_chunks(file_size: usize) -> (usize, usize) {
    // Calculate minimum chunks needed to stay under fragment size limit
    let min_original_chunks = if file_size == 0 {
        10 // Empty files still need minimum chunks for Reed-Solomon
    } else {
        file_size.div_ceil(MAX_FRAGMENT_SIZE)
    };

    // Ensure at least 10 original chunks for good Reed-Solomon efficiency
    let original_chunks = min_original_chunks.max(10);

    // Use 2:1 redundancy ratio (2 recovery for every 1 original)
    let recovery_chunks = original_chunks * 2;

    (original_chunks, recovery_chunks)
}

pub fn calculate_chunk_padding(file_size: usize, num_chunks: usize) -> usize {
    if num_chunks == 0 {
        return 0; // Defensive: avoid division by zero
    }

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

    remainder
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

impl From<FileError> for rusqlite::Error {
    fn from(err: FileError) -> Self {
        rusqlite::Error::ToSqlConversionFailure(Box::new(err))
    }
}

pub fn generate_siv_nonce() -> Nonce {
    // we generate this SIV nonce once for each user
    // it's stored for the user forever + synced between nodes
    // probably not needed for security, but defence-in-depth?
    let mut rng = rand::rng();

    let random_value: u128 = rng.random();
    let random_bytes = random_value.to_be_bytes();
    
    *Nonce::from_slice(&random_bytes)
}

pub fn generate_siv_key() -> Key<Aes256Siv> {
    let key: Key<Aes256Siv> = Aes256SivAead::generate_key(&mut OsRng);
    key
}

pub async fn encrypt_path(
    path: String,
    key: &Key<Aes256Siv>,
    nonce: &Nonce,
) -> Result<String, FileError> {
    let mut output_path: String = "".to_string();

    let split_path = path.split('/').collect::<Vec<&str>>();
    if split_path.len() > 1 {
        for part in split_path {
            if !part.is_empty() {
                let encrypted_part = encrypt_part(part, key, nonce).await?;
                output_path = output_path + &encrypted_part;
            }
        }
    } else {
        output_path += "/";
    }

    tracing::debug!("Encrypted path: {}", output_path);

    Ok(output_path)
}

pub async fn encrypt_part(
    part: &str,
    key: &Key<Aes256Siv>,
    nonce: &Nonce,
) -> Result<String, FileError> {
    let cipher = Aes256SivAead::new(key);
    let ciphertext = cipher
        .encrypt(nonce, part.as_bytes())
        .map_err(|_| FileError::EncryptionError)?;
    // we encode as hex to enable splitting by /
    // base64 more space efficient but collisions
    let base64_str = hex::encode(ciphertext);
    let this_part = "/".to_string() + &base64_str;
    Ok(this_part)
}

pub fn decrypt_path(
    enc_path: String,
    key: &Key<Aes256Siv>,
    nonce: &Nonce,
) -> Result<String, FileError> {
    let mut output_path: String = "".to_string();

    let split_path = enc_path.split('/').collect::<Vec<&str>>();
    if split_path.len() > 1 {
        for part in split_path {
            if !part.is_empty() {
                let decrypted_part = decrypt_part(part, key, nonce)?;
                output_path = output_path + "/" + &decrypted_part;
            }
        }
    } else {
        output_path += "/"
    }

    Ok(output_path)
}

pub fn decrypt_part(part: &str, key: &Key<Aes256Siv>, nonce: &Nonce) -> Result<String, FileError> {
    let cipher = Aes256SivAead::new(key);
    match hex::decode(part) {
        Ok(binary) => match cipher.decrypt(nonce, binary.as_slice()) {
            Ok(bytes) => {
                let string = String::from_utf8(bytes).map_err(|_| FileError::EncryptionError)?;
                Ok(string)
            }
            Err(_) => Err(FileError::EncryptionError),
        },
        Err(_) => Err(FileError::EncryptionError),
    }
}

/// Construct an encrypted path from parent path and filename segment.
///
/// Handles the edge cases around slash separators:
/// - encrypt_part() returns segments with leading "/" (e.g., "/abc123")
/// - Extracted segments from existing paths don't have leading "/" (e.g., "abc123")
/// - Root level paths need a leading "/"
///
/// # Arguments
/// * `parent_path` - The encrypted parent directory path (empty string for root)
/// * `filename_segment` - The encrypted filename (may or may not have leading "/")
pub fn build_encrypted_path(parent_path: &str, filename_segment: &str) -> String {
    if parent_path.is_empty() {
        // Root level - ensure leading slash
        if filename_segment.starts_with('/') {
            filename_segment.to_string()
        } else {
            format!("/{}", filename_segment)
        }
    } else if filename_segment.starts_with('/') {
        // Segment from encrypt_part already has slash - concatenate directly
        format!("{}{}", parent_path, filename_segment)
    } else {
        // Extracted segment without slash - add separator
        format!("{}/{}", parent_path, filename_segment)
    }
}

/// Get the XDG data directory for storing fragments
pub fn get_fragments_dir() -> Result<String, FileError> {
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
) -> Result<String, FileError> {
    let hash_str = fragment_hash.to_hex();

    // Take first 4 hex characters for 2-level nesting
    if hash_str.len() < 4 {
        return Err(FileError::StorageError(io::Error::new(
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
) -> Result<(), FileError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());

    // Create directory structure if it doesn't exist
    fs::create_dir_all(&dir_path).map_err(FileError::StorageError)?;

    // Write to temp file then atomic rename to prevent concurrent readers
    // from seeing partial data (POSIX rename is atomic on the same filesystem)
    let temp_path = format!("{}.tmp.{:x}", full_file_path, rand::random::<u64>());
    fs::write(&temp_path, &data).map_err(FileError::StorageError)?;
    fs::rename(&temp_path, &full_file_path).map_err(|e| {
        // Clean up temp file on rename failure
        let _ = fs::remove_file(&temp_path);
        FileError::StorageError(e)
    })?;

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
        Err(e) => Err(FileError::StorageError(e)),
    }
}

/// Fetch a fragment from local storage only
/// Returns the fragment data if found locally, otherwise returns an error
pub fn fetch_fragment_local(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<Vec<u8>, FileError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());

    // Read the fragment data using block_in_place to avoid blocking the async executor
    tokio::task::block_in_place(|| {
        fs::read(&full_file_path).map_err(FileError::StorageError)
    })
}

/// Fetch and verify a fragment from local storage
/// Returns the fragment data if found locally and hash matches, otherwise returns an error
pub fn fetch_and_verify_fragment(
    fragment_hash: &Blake3Hash,
    fragments_dir: &str,
) -> Result<Vec<u8>, FileError> {
    let chunk_data = fetch_fragment_local(fragments_dir, fragment_hash)?;

    // Verify chunk hash matches expected
    let actual_chunk_hash = Blake3Hash::new(blake3::hash(&chunk_data));
    if actual_chunk_hash != *fragment_hash {
        tracing::error!(
            "Fragment hash mismatch: expected {:?}, got {:?}",
            fragment_hash,
            actual_chunk_hash
        );
        return Err(FileError::HashingError);
    }

    Ok(chunk_data)
}

/// Finalize reconstructed file by removing padding and verifying hash
fn finalize_file(
    mut file: Vec<u8>,
    added_bytes: u8,
    expected_hash: Blake3Hash,
    data_block_id: &crate::db::CustomUUID,
) -> Result<Vec<u8>, FileError> {
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
        tracing::error!(
            "File hash mismatch after reconstruction: expected {:?}, got {:?}",
            expected_hash,
            actual_hash
        );
        return Err(FileError::HashingError);
    }

    Ok(file)
}

/// One fragment as observed at reassembly time: (hash, id, stored-locally flag).
pub type FragmentEntry = (Blake3Hash, CustomUUID, bool);

/// Reassembly chunk map: chunk_number → (originals_by_index, recovery_by_index).
/// Each inner map keys on local_index inside that chunk.
pub type ReassemblyChunks = HashMap<u32, (HashMap<usize, FragmentEntry>, HashMap<usize, FragmentEntry>)>;

/// Data structure for file reassembly containing organized fragment information
pub struct FileReassemblyData {
    pub chunks: ReassemblyChunks,
    pub added_bytes: u8, // Padding bytes added to last chunk
    pub expected_file_hash: Blake3Hash,
    pub data_block_id: crate::db::CustomUUID, // Needed for hash verification
    pub per_file_key: Option<chacha20poly1305::Key>, // Decrypted per-file key for chunk decryption
    pub placement_height: Option<i32>,        // Consensus height when fragments were distributed
}

/// Data structure containing file metadata and access control information from database
pub struct FileAccessData {
    pub file_reassembly_data: Option<FileReassemblyData>, // None for empty files (no fragments)
    pub file_access_entry: Option<crate::db::types::FileAccess>,
    pub file_size: u64,
}

/// Perform concurrent fragment discovery for a single chunk using work queue pattern with thread reuse
async fn perform_concurrent_fragment_discovery(
    file_data: &mut FileReassemblyData,
    fragments_dir: &str,
    chunk_number: u32,
    app_state: &crate::AppState,
    consensus_height: Option<i32>,
) -> Result<(), FileError> {
    use crate::files::discovery::find_fragment;
    use crate::files::placement::FragmentType;
    use either::Either;

    // Get the target chunk's fragments
    let (originals, recovery) = file_data
        .chunks
        .get(&chunk_number)
        .ok_or(FileError::ShardingError)?;

    // Count how many fragments we already have locally for this chunk
    let local_count = originals
        .values()
        .filter(|(_, _, exists_locally)| *exists_locally)
        .count()
        + recovery
            .values()
            .filter(|(_, _, exists_locally)| *exists_locally)
            .count();

    let fragments_needed = ORIGINAL_FRAGMENTS_PER_CHUNK.saturating_sub(local_count);

    // Early exit if we already have enough fragments locally (skip all database work)
    if fragments_needed == 0 {
        tracing::debug!(
            "Chunk {}: already have {} fragments locally, no discovery needed",
            chunk_number,
            local_count
        );
        return Ok(());
    }

    tracing::debug!(
        "Chunk {}: have {} fragments locally, need {} more",
        chunk_number,
        local_count,
        fragments_needed
    );

    // Determine discovery mode based on consensus_height
    let nodes = match consensus_height {
        Some(height) => {
            // Deterministic placement mode: get node metrics at consensus height
            let conn = app_state.db_pool.get().map_err(|_| {
                FileError::StorageError(io::Error::other(
                    "Database connection failed",
                ))
            })?;

            let node_metrics =
                crate::db::metrics::get_all_node_metrics(Ok(conn), height).map_err(|_| {
                    FileError::StorageError(io::Error::other(
                        "Failed to get node metrics",
                    ))
                })?;

            Either::Right(node_metrics)
        }
        None => {
            // Gossip-only mode: get all nodes from database
            tracing::warn!("No consensus height available - using gossip-only fragment discovery");

            let conn = app_state.db_pool.get().map_err(|_| {
                FileError::StorageError(io::Error::other(
                    "Database connection failed",
                ))
            })?;

            let my_node_id = app_state.get_node_id().map_err(|_| {
                FileError::StorageError(io::Error::other(
                    "Failed to get node ID",
                ))
            })?;
            let gossip_nodes =
                crate::db::nodes::get_all_nodes_as_connection_info(Ok(conn), my_node_id).map_err(
                    |_| {
                        FileError::StorageError(io::Error::other(
                            "Failed to get nodes for gossip",
                        ))
                    },
                )?;

            Either::Left(gossip_nodes)
        }
    };

    // Build list of missing fragments for the target chunk only
    let mut missing_fragments = Vec::new();

    // Add missing original fragments
    for (index, (hash, fragment_id, exists_locally)) in originals {
        if !exists_locally {
            missing_fragments.push((*index, *hash, fragment_id.clone(), FragmentType::Original));
        }
    }

    // Add missing recovery fragments
    for (index, (hash, fragment_id, exists_locally)) in recovery {
        if !exists_locally {
            missing_fragments.push((*index, *hash, fragment_id.clone(), FragmentType::Recovery));
        }
    }

    // Batch query fragment inventory for all missing fragments
    let missing_hashes: Vec<Blake3Hash> = missing_fragments
        .iter()
        .map(|(_, hash, _, _)| *hash)
        .collect();

    let mut inventory_map = crate::db::inventory::batch_query_fragment_inventory(
        app_state.db_pool.get(),
        &missing_hashes,
        None, // Use default
    )
    .map_err(|_| FileError::DatabaseError)?;

    // Pre-distribute inventory hints - remove from map (avoiding clones) when building queue
    let missing_fragments: Vec<_> = missing_fragments
        .into_iter()
        .map(|(index, hash, fragment_id, fragment_type)| {
            let inventory_hint = inventory_map.remove(&hash);
            (index, hash, fragment_id, fragment_type, inventory_hint)
        })
        .collect();

    // Calculate number of workers: at least 2 for redundancy (even if only need 1), cap at missing count
    let num_workers = if fragments_needed == 1 {
        2.min(missing_fragments.len()) // Redundancy: 2 workers race to fetch 1
    } else {
        fragments_needed.min(missing_fragments.len())
    };

    let missing_count = missing_fragments.len();
    tracing::debug!(
        "Chunk {}: spawning {} workers to fetch {} fragments from {} candidates",
        chunk_number,
        num_workers,
        fragments_needed,
        missing_count
    );

    // Create work queue for fragments to try (move missing_fragments, no clone)
    let work_queue = std::sync::Arc::new(tokio::sync::Mutex::new(missing_fragments));
    let (success_tx, mut success_rx) = tokio::sync::mpsc::unbounded_channel();
    let successful_downloads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Spawn worker threads to fetch missing fragments
    let mut worker_handles = Vec::new();
    let fragments_needed_shared = fragments_needed; // Capture for workers

    for worker_id in 0..num_workers {
        let tx = success_tx.clone();
        let queue = work_queue.clone();
        let nodes_clone = nodes.clone();
        let iroh_transport_clone = app_state.iroh_transport.clone();
        let fragments_dir_clone = fragments_dir.to_string();
        let successful_downloads_clone = successful_downloads.clone();

        let worker_handle = tokio::spawn(async move {
            tracing::debug!("Worker {} starting fragment discovery", worker_id);

            // Keep working until we have enough successful downloads or run out of work
            loop {
                // Check if we already have enough successful downloads
                if successful_downloads_clone.load(std::sync::atomic::Ordering::Relaxed)
                    >= fragments_needed_shared
                {
                    tracing::debug!(
                        "Worker {} stopping - enough fragments downloaded",
                        worker_id
                    );
                    break;
                }

                // Get next fragment to try from work queue
                let next_work = {
                    let mut queue_lock = queue.lock().await;
                    queue_lock.pop()
                };

                let (index, fragment_hash, fragment_id, fragment_type, inventory_hint) =
                    match next_work {
                        Some(work) => work,
                        None => {
                            tracing::debug!(
                                "Worker {} stopping - no more fragments to try",
                                worker_id
                            );
                            break;
                        }
                    };

                tracing::debug!(
                    "Worker {} trying fragment {} (type: {:?})",
                    worker_id,
                    fragment_hash.to_hex(),
                    fragment_type
                );

                // Try to find and fetch the fragment from network
                match find_fragment(
                    &fragment_hash,
                    fragment_type,
                    nodes_clone.clone(),
                    &iroh_transport_clone,
                    inventory_hint,
                )
                .await
                {
                    Ok(encrypted_data) => {
                        // Store fragment locally
                        if let Err(e) =
                            store_fragment(&fragments_dir_clone, &fragment_hash, encrypted_data)
                        {
                            tracing::error!(
                                "Worker {} failed to store fragment {}: {:?}",
                                worker_id,
                                fragment_hash.to_hex(),
                                e
                            );
                            let _ = tx.send(Err((index, fragment_type)));
                            continue; // Try next fragment
                        }

                        tracing::info!(
                            "Worker {} successfully cached fragment {} from network",
                            worker_id,
                            fragment_hash.to_hex()
                        );

                        // Increment successful downloads and report success
                        // Database update will be handled by the receiver to avoid contention
                        successful_downloads_clone
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = tx.send(Ok((index, fragment_type, fragment_hash)));
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Worker {} failed to discover fragment {}: {:?}",
                            worker_id,
                            fragment_hash.to_hex(),
                            e
                        );
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
                // Queue async DB update (drain task will batch-flush through write gate)
                if let Err(e) = app_state.local_state_tx.try_send(
                    crate::db::write_gate::LocalStateUpdate::MarkLocal {
                        fragment_hash,
                    },
                ) {
                    tracing::warn!(
                        "Local state queue full, dropping mark-local for {}: {}",
                        fragment_hash.to_hex(),
                        e
                    );
                }

                // Update the exists_locally flag in file_data for the target chunk only
                if let Some((originals, recovery)) = file_data.chunks.get_mut(&chunk_number) {
                    match fragment_type {
                        FragmentType::Original => {
                            if let Some((_, _, exists_locally)) = originals.get_mut(&index) {
                                *exists_locally = true;
                            }
                        }
                        FragmentType::Recovery => {
                            if let Some((_, _, exists_locally)) = recovery.get_mut(&index) {
                                *exists_locally = true;
                            }
                        }
                    }
                }

                completed_downloads += 1;
                let total_successful =
                    successful_downloads.load(std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(
                    "Chunk {}: fragment discovery progress: {}/{} needed, {} total successful",
                    chunk_number,
                    completed_downloads,
                    fragments_needed,
                    total_successful
                );

                // Exit early once we have enough fragments - no need to wait for stragglers
                if completed_downloads >= fragments_needed {
                    tracing::debug!(
                        "Chunk {}: collected {} fragments (needed {}), stopping collection early",
                        chunk_number,
                        completed_downloads,
                        fragments_needed
                    );
                    break;
                }
            }
            Err((index, fragment_type)) => {
                tracing::debug!(
                    "Failed to download fragment at index {} (type: {:?})",
                    index,
                    fragment_type
                );
            }
        }
    }

    // Workers will finish in background - no need to block waiting for stragglers
    // They check successful_downloads atomic counter and stop when enough fragments are collected

    // Check if we collected enough fragments
    if completed_downloads < fragments_needed {
        tracing::error!(
            "Chunk {}: discovery failed - collected {}/{} needed",
            chunk_number,
            completed_downloads,
            fragments_needed
        );
        return Err(FileError::ShardingError);
    }

    tracing::debug!(
        "Chunk {}: discovery complete - fetched {} fragments",
        chunk_number,
        completed_downloads
    );
    Ok(())
}

/// Fetch a single fragment from network and cache it locally
pub async fn fetch_and_cache_fragment(
    fragment_hash: &Blake3Hash,
    fragments_dir: &str,
    app_state: &AppState,
    placement_height: Option<i32>,
    inventory_hint: Option<Vec<crate::types::NodeConnectionInfo>>,
) -> Result<(), FileError> {
    use either::Either;

    // If no hint provided, query inventory for this fragment (best-effort optimization)
    let inventory_hint = match inventory_hint {
        Some(hint) => Some(hint),
        None => crate::db::inventory::batch_query_fragment_inventory(
            app_state.db_pool.get(),
            &[*fragment_hash],
            None,
        )
        .ok()
        .and_then(|mut map| map.remove(fragment_hash)),
    };

    // Determine discovery mode based on placement_height
    let nodes = match placement_height {
        Some(height) => {
            // Deterministic placement mode: get node metrics at consensus height
            let node_metrics =
                crate::db::metrics::get_all_node_metrics(app_state.db_pool.get(), height)
                    .map_err(|_| FileError::DatabaseError)?;
            Either::Right(node_metrics)
        }
        None => {
            // Gossip-only mode: get all nodes from database
            tracing::warn!("No placement height available - using gossip-only fragment discovery");

            let my_node_id = app_state
                .get_node_id()
                .map_err(|_| FileError::DatabaseError)?;
            let gossip_nodes = crate::db::nodes::get_all_nodes_as_connection_info(
                app_state.db_pool.get(),
                my_node_id,
            )
            .map_err(|_| FileError::DatabaseError)?;

            Either::Left(gossip_nodes)
        }
    };

    // Try to find and fetch the fragment
    match find_fragment(
        fragment_hash,
        FragmentType::Original,
        nodes,
        &app_state.iroh_transport,
        inventory_hint,
    )
    .await
    {
        Ok(fragment_data) => {
            // Store fragment locally
            store_fragment(fragments_dir, fragment_hash, fragment_data)?;

            // Queue async DB update (drain task will batch-flush through write gate)
            if let Err(e) = app_state.local_state_tx.try_send(
                crate::db::write_gate::LocalStateUpdate::MarkLocal {
                    fragment_hash: *fragment_hash,
                },
            ) {
                tracing::warn!(
                    "Local state queue full, dropping mark-local for {}: {}",
                    fragment_hash.to_hex(),
                    e
                );
            }

            tracing::debug!(
                "Successfully fetched and cached fragment {}",
                fragment_hash.to_hex()
            );
            Ok(())
        }
        Err(e) => {
            tracing::error!(
                "Failed to fetch fragment {} from network: {:?}",
                fragment_hash.to_hex(),
                e
            );
            Err(FileError::NetworkError)
        }
    }
}

/// Check if a fragment exists on disk and is valid (hash matches)
pub fn fragment_exists_and_valid(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    fetch_and_verify_fragment(fragment_hash, fragments_dir).is_ok()
}

/// Shared content-update preparation for both PATCH /files and FileProvider modify_item.
/// Handles key generation, file processing, and share propagation.
/// Returns (data_block_id, DataRecord with file_access + propagation entries, incoming_share_updates, per_file_key).
/// The caller is responsible for building the final ModifyItemPayload, validation, and consensus submission.
pub async fn prepare_content_update(
    app_state: &AppState,
    user_id: i32,
    inode_id: &crate::db::CustomUUID,
    field: axum::extract::multipart::Field<'_>,
    file_size: usize,
) -> Result<
    (
        crate::db::CustomUUID,
        crate::db::DataRecord,
        Option<Vec<crate::shares::types::IncomingShareUpdate>>,
        chacha20poly1305::Key,
    ),
    axum::http::StatusCode,
> {
    use axum::http::StatusCode;
    use chacha20poly1305::{ChaCha20Poly1305, aead::KeyInit, aead::OsRng as CryptoOsRng};

    // Generate new data block ID and per-file key
    let dataid = crate::db::CustomUUID::new(None);
    let per_file_key = ChaCha20Poly1305::generate_key(&mut CryptoOsRng);

    // Create file access entry for the modifier
    let file_access = crate::db::types::FileAccess::new_for_user(
        app_state.db_pool.get(),
        dataid.clone(),
        user_id,
        &per_file_key,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Process uploaded file
    let mut data_record = if file_size == 0 {
        crate::db::DataRecord {
            id: dataid.clone(),
            modified_at: None,
            data: crate::db::Data {
                hash: crate::db::Blake3Hash::new({
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(dataid.as_bytes());
                    hasher.finalize()
                }),
                fragments: Vec::new(),
                added_bytes: 0,
            },
            file_access_entries: None,
            file_size: 0,
        }
    } else {
        use tokio_stream::StreamExt;
        use tokio_util::io::StreamReader;
        let reader = StreamReader::new(field.map(|r| r.map_err(std::io::Error::other)));
        super::routes::process_uploaded_file(
            reader,
            file_size,
            dataid.clone(),
            &per_file_key,
            &app_state.fragments_dir,
        )
        .await?
    };
    data_record.file_access_entries = Some(vec![file_access]);
    data_record.file_size = file_size as u64;

    // Build share propagation
    let conn = app_state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let (extra_file_access_entries, incoming_share_updates) =
        super::routes::build_share_propagation(&conn, inode_id, user_id, &dataid, &per_file_key)?;
    drop(conn);

    if !extra_file_access_entries.is_empty() {
        if let Some(entries) = data_record.file_access_entries.as_mut() {
            entries.extend(extra_file_access_entries);
        } else {
            data_record.file_access_entries = Some(extra_file_access_entries);
        }
    }

    Ok((dataid, data_record, incoming_share_updates, per_file_key))
}

/// Derive chunk encryption key from per-file key and fragment UUID
pub fn derive_chunk_key(
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &crate::db::CustomUUID,
) -> chacha20poly1305::Key {
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

pub fn calculate_encrypted_chunk_length(chunk_length: usize) -> usize {
    chunk_length + (chunk_length.div_ceil(BUFFER_SIZE) * 16)
}

/// Encrypt a chunk using streaming ChaCha20-Poly1305 with true memory efficiency
pub fn encrypt_chunk(
    mut chunk: Vec<u8>, // Take ownership so we can consume it
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &crate::db::CustomUUID,
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
    fragment_id: &crate::db::CustomUUID,
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

/// Reconstructs a single 40MB chunk from its fragments
/// Uses fast path (concatenate originals) if all 10 originals are local,
/// otherwise uses RS decoding with any available 10+ fragments
fn reconstruct_single_chunk(
    originals: &HashMap<usize, (Blake3Hash, CustomUUID, bool)>,
    recovery: &HashMap<usize, (Blake3Hash, CustomUUID, bool)>,
    fragments_dir: &str,
    per_file_key: &Option<chacha20poly1305::Key>,
) -> Result<Vec<u8>, FileError> {
    // Check if all original fragments are available locally (fast path)
    let all_originals_local = originals
        .values()
        .all(|(_, _, exists_locally)| *exists_locally);

    if all_originals_local && originals.len() == ORIGINAL_FRAGMENTS_PER_CHUNK {
        // Fast path: concatenate original fragments in order
        tracing::debug!(
            "Using fast path: all {} originals available locally",
            ORIGINAL_FRAGMENTS_PER_CHUNK
        );

        let mut chunk_data = Vec::new();
        for i in 0..ORIGINAL_FRAGMENTS_PER_CHUNK {
            if let Some((hash, fragment_id, _)) = originals.get(&i) {
                let fragment_data = fetch_and_verify_fragment(hash, fragments_dir)?;

                // Decrypt if needed
                let decrypted = if let Some(key) = per_file_key {
                    decrypt_chunk(&fragment_data, key, fragment_id)?
                } else {
                    fragment_data
                };

                chunk_data.extend_from_slice(&decrypted);
            } else {
                return Err(FileError::ShardingError);
            }
        }

        return Ok(chunk_data);
    }

    // Slow path: Reed-Solomon reconstruction
    tracing::debug!("Using slow path: RS reconstruction");

    // Collect available encrypted fragments (originals) and recovery fragments
    let mut available_original = Vec::new();
    let mut available_recovery = Vec::new();

    tracing::debug!(
        "RS reconstruction: originals.len()={}, recovery.len()={}",
        originals.len(),
        recovery.len()
    );

    // Collect original fragments (keep encrypted for RS, decrypt after reconstruction)
    for (index, (hash, fragment_id, exists_locally)) in originals.iter() {
        if *exists_locally {
            let fragment_data = fetch_and_verify_fragment(hash, fragments_dir)?;
            available_original.push((*index, fragment_data));
        }
    }

    // Collect recovery fragments (already in RS parity form, no per-chunk encryption)
    for (index, (hash, fragment_id, exists_locally)) in recovery.iter() {
        if *exists_locally {
            let fragment_data = fetch_and_verify_fragment(hash, fragments_dir)?;
            available_recovery.push((*index, fragment_data));
        }
    }

    let total_available = available_original.len() + available_recovery.len();
    tracing::debug!(
        "RS reconstruction: collected {} originals + {} recovery = {} total (need {})",
        available_original.len(),
        available_recovery.len(),
        total_available,
        ORIGINAL_FRAGMENTS_PER_CHUNK
    );

    if total_available < ORIGINAL_FRAGMENTS_PER_CHUNK {
        tracing::error!(
            "Insufficient fragments for reconstruction: have {}, need {}",
            total_available,
            ORIGINAL_FRAGMENTS_PER_CHUNK
        );
        return Err(FileError::ShardingError);
    }

    // Get fragment size (all fragments in a chunk are the same size)
    let fragment_size = if let Some((_, data)) = available_original.first() {
        data.len()
    } else if let Some((_, data)) = available_recovery.first() {
        data.len()
    } else {
        tracing::error!("RS reconstruction: no fragments available to determine size");
        return Err(FileError::ShardingError);
    };

    tracing::debug!("RS reconstruction: fragment_size={}", fragment_size);

    // Create Reed-Solomon decoder
    let mut decoder = ReedSolomonDecoder::new(
        ORIGINAL_FRAGMENTS_PER_CHUNK,
        RECOVERY_FRAGMENTS_PER_CHUNK,
        fragment_size,
    )
    .map_err(|e| {
        tracing::error!("RS reconstruction: failed to create decoder: {:?}", e);
        FileError::ShardingError
    })?;

    tracing::debug!("RS reconstruction: decoder created");

    // Add available original shards
    tracing::debug!(
        "RS reconstruction: adding {} original shards",
        available_original.len()
    );
    for (index, chunk_data) in &available_original {
        decoder
            .add_original_shard(*index, chunk_data)
            .map_err(|e| {
                tracing::error!(
                    "RS reconstruction: failed to add original shard {}: {:?}",
                    index,
                    e
                );
                FileError::ShardingError
            })?;
    }

    // Add available recovery shards
    // Note: Recovery fragments are stored with indices 10-29 in database (local_index)
    // But RS decoder expects recovery indices 0-19, so we subtract ORIGINAL_FRAGMENTS_PER_CHUNK
    tracing::debug!(
        "RS reconstruction: adding {} recovery shards",
        available_recovery.len()
    );
    for (index, chunk_data) in &available_recovery {
        let rs_recovery_index = index - ORIGINAL_FRAGMENTS_PER_CHUNK;
        decoder
            .add_recovery_shard(rs_recovery_index, chunk_data)
            .map_err(|e| {
                tracing::error!(
                    "RS reconstruction: failed to add recovery shard {} (RS index {}): {:?}",
                    index,
                    rs_recovery_index,
                    e
                );
                FileError::ShardingError
            })?;
    }

    // Perform reconstruction
    tracing::debug!("RS reconstruction: starting decode");
    let decoder_result = decoder.decode().map_err(|e| {
        tracing::error!("RS decode failed: {:?}", e);
        FileError::ShardingError
    })?;
    tracing::debug!("RS reconstruction: decode complete");

    // Build index of reconstructed fragments (store references, not copies)
    let mut reconstructed_indices: std::collections::HashMap<usize, Vec<u8>> =
        std::collections::HashMap::new();
    for (index, chunk_data) in decoder_result.restored_original_iter() {
        reconstructed_indices.insert(index, chunk_data.to_vec());
    }

    // Decrypt and concatenate fragments in order (no intermediate HashMap of all fragments)
    let mut chunk_data = Vec::new();
    for i in 0..ORIGINAL_FRAGMENTS_PER_CHUNK {
        // Try to get from available_original first, then from reconstructed
        let encrypted_fragment = if let Some((_, encrypted_data)) =
            available_original.iter().find(|(idx, _)| *idx == i)
        {
            encrypted_data
        } else if let Some(encrypted_data) = reconstructed_indices.get(&i) {
            encrypted_data
        } else {
            return Err(FileError::ShardingError);
        };

        // Decrypt the fragment
        if let Some(key) = per_file_key {
            // Get fragment_id from originals map
            if let Some((_, fragment_id, _)) = originals.get(&i) {
                let decrypted = decrypt_chunk(encrypted_fragment, key, fragment_id)?;
                chunk_data.extend_from_slice(&decrypted);
            } else {
                return Err(FileError::ShardingError);
            }
        } else {
            chunk_data.extend_from_slice(encrypted_fragment);
        }
    }

    tracing::debug!(
        "RS reconstruction: complete, reconstructed {} bytes",
        chunk_data.len()
    );
    Ok(chunk_data)
}

/// Reconstruct a file using chunked Reed-Solomon with streaming support
/// Processes chunks sequentially, yielding each 40MB chunk as it's reconstructed
/// Uses incremental Blake3 hashing for verification without buffering entire file in memory
///
/// When `range` is `Some((start, end))` (inclusive byte range), only the chunks covering
/// that range are reconstructed and boundary chunks are sliced. Hash verification is skipped
/// for partial content since we can't verify a partial file hash.
pub fn reconstruct_file_chunked(
    fragments_dir: String,
    mut file_data: FileReassemblyData,
    app_state: Option<crate::AppState>,
    consensus_height: Option<i32>,
    range: Option<(u64, u64)>,
) -> impl tokio_stream::Stream<Item = Result<bytes::Bytes, FileError>> {
    async_stream::try_stream! {
        // Handle empty files
        if file_data.chunks.is_empty() {
            return;
        }

        let num_chunks = file_data.chunks.len() as u32;
        let chunk_size = CHUNK_SIZE as u64;

        // Determine which chunks to iterate
        let (start_chunk, end_chunk, range_start, range_end) = match range {
            Some((start, end)) => {
                let sc = (start / chunk_size) as u32;
                let ec = (end / chunk_size) as u32;
                (sc, ec.min(num_chunks - 1), start, end)
            }
            None => (0, num_chunks - 1, 0u64, u64::MAX),
        };

        let is_range = range.is_some();
        let mut hasher = if is_range { None } else { Some(blake3::Hasher::new()) };

        // Process chunks in order (streaming with per-chunk discovery for rate-matching)
        for chunk_number in start_chunk..=end_chunk {
            // Perform fragment discovery for this chunk if needed (rate-matched to client download speed)
            if let Some(ref state) = app_state {
                perform_concurrent_fragment_discovery(
                    &mut file_data,
                    &fragments_dir,
                    chunk_number,
                    state,
                    consensus_height,
                ).await?;
            }

            let chunk_data = file_data.chunks.get(&chunk_number)
                .ok_or(FileError::ShardingError)?;

            let (originals, recovery) = chunk_data;

            // Count local fragments for this chunk (after discovery)
            let local_count = originals.values().filter(|(_, _, local)| *local).count() +
                             recovery.values().filter(|(_, _, local)| *local).count();

            tracing::debug!("Processing chunk {}/{}: {} fragments available",
                           chunk_number + 1, num_chunks, local_count);

            // Verify we have enough fragments (discovery should have ensured this)
            if local_count < ORIGINAL_FRAGMENTS_PER_CHUNK {
                tracing::error!("Chunk {}: insufficient fragments after discovery ({}/{})",
                              chunk_number, local_count, ORIGINAL_FRAGMENTS_PER_CHUNK);
                Err(FileError::ShardingError)?;
            }

            // Reconstruct this chunk
            let mut chunk_bytes = reconstruct_single_chunk(
                originals,
                recovery,
                &fragments_dir,
                &file_data.per_file_key,
            )?;

            // Remove padding from last chunk before hashing/slicing
            if chunk_number == num_chunks - 1 && file_data.added_bytes > 0 {
                let final_length = chunk_bytes.len().saturating_sub(file_data.added_bytes as usize);
                chunk_bytes.truncate(final_length);
            }

            if is_range {
                // Slice boundary chunks for range requests
                let chunk_start_byte = chunk_number as u64 * chunk_size;
                let slice_start = if chunk_number == start_chunk {
                    (range_start - chunk_start_byte) as usize
                } else {
                    0
                };
                let slice_end = if chunk_number == end_chunk {
                    ((range_end - chunk_start_byte) as usize + 1).min(chunk_bytes.len())
                } else {
                    chunk_bytes.len()
                };

                if slice_start < chunk_bytes.len() && slice_start < slice_end {
                    yield bytes::Bytes::from(chunk_bytes[slice_start..slice_end].to_vec());
                }
            } else {
                // Full download: update incremental hash and yield
                if let Some(ref mut h) = hasher {
                    h.update(&chunk_bytes);
                }
                yield bytes::Bytes::from(chunk_bytes);
            }
        }

        // Verify final hash after all chunks processed (only for full downloads)
        if let Some(hasher) = hasher {
            // Note: Hash includes data_block_id as per upload flow
            let mut hasher = hasher;
            hasher.update(file_data.data_block_id.as_bytes());
            let computed_hash = Blake3Hash::from(hasher.finalize());
            if computed_hash != file_data.expected_file_hash {
                tracing::error!(
                    "Hash mismatch for data_block_id {}: expected {}, got {}",
                    file_data.data_block_id,
                    file_data.expected_file_hash.to_hex(),
                    computed_hash.to_hex()
                );
                Err(FileError::HashMismatch)?;
            }

            tracing::info!("File reconstruction complete and verified: {}", file_data.data_block_id);
        }
    }
}
