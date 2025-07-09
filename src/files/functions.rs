use crate::db::Data;
use crate::types::Blake3Hash;
use crate::db::DataBlockRepresentation;
use aes_siv::{
    aead::{Aead, OsRng}, siv::Aes256Siv, Aes256SivAead, Key, KeyInit, Nonce
};
use duckdb::arrow::datatypes::ToByteSlice;
use rayon::prelude::*;
use rand::Rng;
use hex;

#[derive(Debug)]
pub enum FileError {
    ShardingError,
    HashingError,
    InvalidChunkCount,
    TaskJoinError,
    EncryptionError
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

pub async fn shard_file(file: Vec<u8>) -> Result<Option<Data>, FileError> {
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

    // Combine original and recovery hashes into a single vector
    let mut all_fragments = Vec::new();
    
    // Add original hashes (fragments 1-10)
    for hash in original_hashes {
        all_fragments.push(DataBlockRepresentation::Hash(hash));
    }
    
    // Add recovery hashes (fragments 11-30)
    for hash in recovery_hashes {
        all_fragments.push(DataBlockRepresentation::Hash(hash));
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