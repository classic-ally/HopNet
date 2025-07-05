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

impl From<FileError> for duckdb::Error {
    fn from(err: FileError) -> Self {
        duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(duckdb::ffi::DuckDBError),
            Some(format!("File operation failed: {:?}", err))
        )
    }
}

pub async fn shard_file(mut file: Vec<u8>) -> Result<Data, FileError> {
    let data = tokio::task::spawn_blocking(move || -> Result<_, FileError> {
        // Hash the whole file first
        let whole_file_hash = Blake3Hash::new(blake3::hash(&file));
        
        // Calculate padding needed
        let original_len = file.len();
        let mut remainder = (10 - (original_len % 10)) % 10;
        
        // Ensure chunk length is even
        let chunk_len_after_padding = (original_len + remainder) / 10;
        if chunk_len_after_padding % 2 != 0 {
            remainder += 10;
        }
        
        // Apply padding in one go
        if remainder > 0 {
            file.resize(original_len + remainder, 0);
        }
        let added_bytes = remainder as u8;
        
        // Split into chunks more efficiently
        let chunk_size = file.len() / 10;
        let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(10);
        while !file.is_empty() {
            let current_len = file.len();
            let chunk = file.split_off(current_len - chunk_size);
            chunks.push(chunk);
        }
        chunks.reverse();

        if chunks.len() != 10 {
            return Err(FileError::InvalidChunkCount);
        }

        // Hash original chunks in parallel using rayon (CPU-bound work)
        let original_hashes: Vec<Blake3Hash> = chunks
            .par_iter()
            .map(|chunk| Blake3Hash::new(blake3::hash(chunk)))
            .collect();
        
        // Reed-Solomon encoding (CPU-bound, keep synchronous)
        let recovery_chunks = reed_solomon_simd::encode(10, 20, &chunks)
            .map_err(|_| FileError::ShardingError)?;
        
        // Hash recovery chunks in parallel
        let recovery_hashes: Vec<Blake3Hash> = recovery_chunks
            .par_iter()
            .map(|chunk| Blake3Hash::new(blake3::hash(chunk)))
            .collect();
        
        // Build the result using array indexing with bounds checking
        if original_hashes.len() != 10 || recovery_hashes.len() != 20 {
            return Err(FileError::InvalidChunkCount);
        }

        Ok((
            whole_file_hash,
            original_hashes,
            recovery_hashes,
            added_bytes
        ))

    })
    .await
    .map_err(|_| FileError::TaskJoinError)??;

    // Destructure the results from the blocking task
    let (whole_file_hash, original_hashes, recovery_hashes, added_bytes) = data;

    let data = Data {
        hash: whole_file_hash,
        fragment_01: DataBlockRepresentation::Hash(original_hashes[0]),
        fragment_02: DataBlockRepresentation::Hash(original_hashes[1]),
        fragment_03: DataBlockRepresentation::Hash(original_hashes[2]),
        fragment_04: DataBlockRepresentation::Hash(original_hashes[3]),
        fragment_05: DataBlockRepresentation::Hash(original_hashes[4]),
        fragment_06: DataBlockRepresentation::Hash(original_hashes[5]),
        fragment_07: DataBlockRepresentation::Hash(original_hashes[6]),
        fragment_08: DataBlockRepresentation::Hash(original_hashes[7]),
        fragment_09: DataBlockRepresentation::Hash(original_hashes[8]),
        fragment_10: DataBlockRepresentation::Hash(original_hashes[9]),
        fragment_11: DataBlockRepresentation::Hash(recovery_hashes[0]),
        fragment_12: DataBlockRepresentation::Hash(recovery_hashes[1]),
        fragment_13: DataBlockRepresentation::Hash(recovery_hashes[2]),
        fragment_14: DataBlockRepresentation::Hash(recovery_hashes[3]),
        fragment_15: DataBlockRepresentation::Hash(recovery_hashes[4]),
        fragment_16: DataBlockRepresentation::Hash(recovery_hashes[5]),
        fragment_17: DataBlockRepresentation::Hash(recovery_hashes[6]),
        fragment_18: DataBlockRepresentation::Hash(recovery_hashes[7]),
        fragment_19: DataBlockRepresentation::Hash(recovery_hashes[8]),
        fragment_20: DataBlockRepresentation::Hash(recovery_hashes[9]),
        fragment_21: DataBlockRepresentation::Hash(recovery_hashes[10]),
        fragment_22: DataBlockRepresentation::Hash(recovery_hashes[11]),
        fragment_23: DataBlockRepresentation::Hash(recovery_hashes[12]),
        fragment_24: DataBlockRepresentation::Hash(recovery_hashes[13]),
        fragment_25: DataBlockRepresentation::Hash(recovery_hashes[14]),
        fragment_26: DataBlockRepresentation::Hash(recovery_hashes[15]),
        fragment_27: DataBlockRepresentation::Hash(recovery_hashes[16]),
        fragment_28: DataBlockRepresentation::Hash(recovery_hashes[17]),
        fragment_29: DataBlockRepresentation::Hash(recovery_hashes[18]),
        fragment_30: DataBlockRepresentation::Hash(recovery_hashes[19]),
        added_bytes: added_bytes,
    };
    
    Ok(data)
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