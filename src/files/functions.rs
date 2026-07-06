use crate::AppState;
use crate::db::CustomUUID;
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
use rand::RngExt;
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

// Chunk/padding math and format constants live in the substrate crate
// (hopnet-storage, RFC-014); re-exported here so call sites don't churn.
pub use hopnet_storage::rs::{
    CHUNK_SIZE, MAX_FRAGMENT_SIZE, ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK,
    TOTAL_FRAGMENTS_PER_CHUNK, calculate_chunk_padding, calculate_chunked_fragments,
    calculate_optimal_chunks, calculate_padding_and_chunks,
};

impl From<hopnet_storage::StorageError> for FileError {
    fn from(e: hopnet_storage::StorageError) -> Self {
        match e {
            hopnet_storage::StorageError::Encryption => FileError::EncryptionError,
            // Fragment-hash mismatch mapped to HashingError, matching the
            // pre-extraction behavior of fetch_and_verify_fragment.
            hopnet_storage::StorageError::HashMismatch => FileError::HashingError,
            hopnet_storage::StorageError::Io(io)
            | hopnet_storage::StorageError::Read(io) => FileError::StorageError(io),
            hopnet_storage::StorageError::Rs => FileError::ShardingError,
            // Host seam failures (engine-side DB/signing) never reach the
            // projection's put/get delegations; map defensively.
            hopnet_storage::StorageError::Host(msg) => {
                FileError::StorageError(std::io::Error::other(msg))
            }
        }
    }
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

/// File metadata and access control from the database: the substrate's
/// reassembly manifest plus the caller's wrap row. `manifest` is None for
/// empty files (data_id NULL — no fragments, no encryption).
pub struct FileAccessData {
    pub manifest: Option<hopnet_storage::store::BlobManifest>,
    pub file_access_entry: Option<crate::db::types::BlobAccess>,
    pub file_size: u64,
}

/// Check if a fragment exists on disk and is valid (hash matches)
pub fn fragment_exists_and_valid(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    hopnet_storage::fragstore::fragment_exists_and_valid(fragments_dir, fragment_hash)
}

/// Shared content-update preparation for both PATCH /files and FileProvider modify_item.
/// Handles key generation, file processing, and share propagation.
/// Returns (data_block_id, Option<BlobInsertOp> (None = content is now empty
/// → inode data_id becomes NULL; RFC-014 B5), incoming_share_updates).
/// The caller builds the ModifyItemPayload and submits.
pub async fn prepare_content_update(
    app_state: &AppState,
    user_id: i32,
    inode_id: &crate::db::CustomUUID,
    field: axum::extract::multipart::Field<'_>,
    file_size: usize,
) -> Result<
    (
        crate::db::CustomUUID,
        Option<hopnet_storage::store::BlobInsertOp>,
        Option<Vec<crate::shares::types::IncomingShareUpdate>>,
    ),
    axum::http::StatusCode,
> {
    use axum::http::StatusCode;
    use chacha20poly1305::{ChaCha20Poly1305, aead::KeyInit, aead::OsRng as CryptoOsRng};

    let dataid = crate::db::CustomUUID::new(None);

    // Empty content: no blob, no key, nothing to share — the inode's
    // data_id becomes NULL and pending shares have nothing to re-wrap.
    if file_size == 0 {
        return Ok((dataid, None, None));
    }

    // Generate new per-file key and the modifier's wrap
    let per_file_key = ChaCha20Poly1305::generate_key(&mut CryptoOsRng);
    let file_access = crate::db::types::blob_access_for_user(
        app_state.db_pool.get(),
        dataid.clone(),
        user_id,
        &per_file_key,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Process uploaded file through the substrate ingest
    let mut blob_op = {
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
    let mut access = vec![file_access];

    // Build share propagation
    let conn = app_state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let (extra_file_access_entries, incoming_share_updates) =
        super::routes::build_share_propagation(&conn, inode_id, user_id, &dataid, &per_file_key)?;
    drop(conn);

    access.extend(extra_file_access_entries);
    blob_op.access = access;

    Ok((dataid, Some(blob_op), incoming_share_updates))
}

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
    hopnet_storage::crypto::encrypt_chunk(chunk, per_file_key, fragment_id)
        .map_err(FileError::from)
}

pub fn decrypt_chunk(
    encrypted_chunk: &[u8],
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &crate::db::CustomUUID,
) -> Result<Vec<u8>, FileError> {
    hopnet_storage::crypto::decrypt_chunk(encrypted_chunk, per_file_key, fragment_id)
        .map_err(FileError::from)
}



