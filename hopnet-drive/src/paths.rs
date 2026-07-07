//! Deterministic path encryption (RFC-015).
//!
//! AES-SIV per-segment path crypto — moved verbatim from the host's
//! `files::functions`; the host re-exports these at their old paths.

use crate::error::FileError;
use aes_siv::{
    Aes256SivAead, Key, KeyInit, Nonce,
    aead::{Aead, OsRng},
    siv::Aes256Siv,
};
use rand::RngExt;

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
