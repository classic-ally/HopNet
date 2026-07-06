//! Fragment cipher + key-wrap primitives.
//!
//! FORMAT-FROZEN (fragment cipher): `derive_chunk_key`, `derive_chunk_nonce`,
//! `encrypt_chunk`, `decrypt_chunk` moved verbatim from the main crate's
//! files/functions.rs — the on-disk/on-wire fragment ciphertext format must
//! survive the extraction byte-for-byte. The golden-vector tests at the bottom
//! of this file pin it; do not change derivation strings, segmenting, or the
//! stream construction without a deliberate format migration.
//!
//! The key-wrap here is the LEGACY (user_id-bound) format; Stage B replaces it
//! with the pubkey-bound "hopnet-storage … v1" wrap and the RecipientKey
//! capability seam.

use crate::error::StorageError;
use chacha20poly1305::aead::stream::{DecryptorBE32, EncryptorBE32};
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::ChaCha20Poly1305;
use hopnet_common::CustomUUID;
use x25519_dalek::{EphemeralSecret, PublicKey as X25519PublicKey};

/// Derive chunk encryption key from per-file key and fragment UUID
pub fn derive_chunk_key(
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &CustomUUID,
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
pub fn derive_chunk_nonce(fragment_id: &CustomUUID) -> [u8; 7] {
    let mut nonce_bytes = [0u8; 7];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet chunk_nonce");
    hasher.update(fragment_id.as_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut nonce_bytes);
    nonce_bytes
}

pub const BUFFER_SIZE: usize = 4096;

pub fn calculate_encrypted_chunk_length(chunk_length: usize) -> usize {
    chunk_length + (chunk_length.div_ceil(BUFFER_SIZE) * 16)
}

/// Encrypt a chunk using streaming ChaCha20-Poly1305 with true memory efficiency
pub fn encrypt_chunk(
    mut chunk: Vec<u8>, // Take ownership so we can consume it
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &CustomUUID,
) -> Result<Vec<u8>, StorageError> {
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
            .map_err(|_| StorageError::Encryption)?;

        encrypted_output.extend_from_slice(&ciphertext);
        // segment is dropped here, freeing memory
    }

    // Process the last segment (or entire chunk if smaller than BUFFER_SIZE)
    if !chunk.is_empty() {
        let ciphertext = stream_encryptor
            .encrypt_last(chunk.as_slice())
            .map_err(|_| StorageError::Encryption)?;

        encrypted_output.extend_from_slice(&ciphertext);
    }

    Ok(encrypted_output)
}

/// Decrypt a chunk using streaming ChaCha20-Poly1305 with memory efficiency
pub fn decrypt_chunk(
    encrypted_chunk: &[u8],
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &CustomUUID,
) -> Result<Vec<u8>, StorageError> {
    let chunk_key = derive_chunk_key(per_file_key, fragment_id);
    let nonce = derive_chunk_nonce(fragment_id);
    let cipher = ChaCha20Poly1305::new(&chunk_key);

    let mut stream_decryptor = DecryptorBE32::from_aead(cipher, nonce.as_ref().into());
    let mut decrypted_output = Vec::with_capacity(encrypted_chunk.len());

    const ENCRYPTED_SEGMENT_SIZE: usize = BUFFER_SIZE + 16; // Each segment has 16-byte auth tag
    let mut chunk_offset = 0;

    // Process all segments except the last one
    while chunk_offset + ENCRYPTED_SEGMENT_SIZE < encrypted_chunk.len() {
        let segment = &encrypted_chunk[chunk_offset..chunk_offset + ENCRYPTED_SEGMENT_SIZE];

        let plaintext = stream_decryptor
            .decrypt_next(segment)
            .map_err(|_| StorageError::Encryption)?;

        decrypted_output.extend_from_slice(&plaintext);
        chunk_offset += ENCRYPTED_SEGMENT_SIZE;
    }

    // Process the last segment (or entire chunk if smaller than ENCRYPTED_SEGMENT_SIZE)
    if chunk_offset < encrypted_chunk.len() {
        let segment = &encrypted_chunk[chunk_offset..];

        let plaintext = stream_decryptor
            .decrypt_last(segment)
            .map_err(|_| StorageError::Encryption)?;

        decrypted_output.extend_from_slice(&plaintext);
    }

    Ok(decrypted_output)
}

/// One wrap of a per-file key to a recipient X25519 pubkey (legacy format).
pub struct WrappedKey {
    pub ephemeral_pubkey: X25519PublicKey,
    pub wrapped_key: Vec<u8>, // 48 bytes (32 key + 16 auth tag)
}

/// Wrap a per-file key to a recipient's X25519 public key — LEGACY format
/// (nonce binds user_id; Stage B replaces this with the pubkey-bound v1 wrap).
/// Extracted verbatim from FileAccess::new_for_user_with_conn; the user lookup
/// stays with the caller.
pub fn wrap_file_key_legacy(
    recipient: &X25519PublicKey,
    data_block_id: &CustomUUID,
    user_id: i32,
    per_file_key: &chacha20poly1305::Key,
) -> Result<WrappedKey, StorageError> {
    // Generate ephemeral key pair for this file
    let ephemeral_secret = EphemeralSecret::random_from_rng(OsRng);
    let ephemeral_public = X25519PublicKey::from(&ephemeral_secret);

    // Perform ECDH with the recipient's X25519 public key
    let shared_secret = ephemeral_secret.diffie_hellman(recipient);

    // Derive ChaCha20Poly1305 key from shared secret using Blake3
    let mut wrap_key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet key_wrap");
    hasher.update(shared_secret.as_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut wrap_key_bytes);
    let wrap_key = chacha20poly1305::Key::from(wrap_key_bytes);

    // Derive deterministic nonce from data_block_id + user_id + ephemeral_pubkey
    let mut nonce_bytes = [0u8; 12];
    let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet wrap_nonce");
    nonce_hasher.update(data_block_id.as_bytes());
    nonce_hasher.update(&user_id.to_le_bytes());
    nonce_hasher.update(ephemeral_public.as_bytes());
    nonce_hasher.finalize_xof().fill(&mut nonce_bytes);
    let wrap_nonce = chacha20poly1305::Nonce::from(nonce_bytes);

    // Encrypt the per-file key
    let wrap_cipher = ChaCha20Poly1305::new(&wrap_key);
    let wrapped_key = wrap_cipher
        .encrypt(&wrap_nonce, per_file_key.as_slice())
        .map_err(|_| StorageError::Encryption)?;

    Ok(WrappedKey {
        ephemeral_pubkey: ephemeral_public,
        wrapped_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn hex_of(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn fixed_key() -> chacha20poly1305::Key {
        [0x42u8; 32].into()
    }

    fn fixed_fid() -> CustomUUID {
        CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap()
    }

    // FORMAT-SURVIVAL GOLDEN VECTORS — captured from the pre-extraction main
    // crate (src/files/functions.rs) on 2026-07-06, consensus-malachite branch.
    // If any of these fail, the fragment cipher format changed: every
    // already-stored fragment in every mesh becomes undecryptable. Do not
    // update the constants to make tests pass.

    #[test]
    fn golden_chunk_key_derivation() {
        let key = derive_chunk_key(&fixed_key(), &fixed_fid());
        assert_eq!(
            hex_of(key.as_slice()),
            "40538c786e75ba0c5912182cbade11c52a614a09c85d2d4f936de0d912a423fb"
        );
    }

    #[test]
    fn golden_chunk_nonce_derivation() {
        assert_eq!(hex_of(&derive_chunk_nonce(&fixed_fid())), "bc1f77d094b2f9");
    }

    #[test]
    fn golden_ciphertext_short_single_segment() {
        let pt = b"hopnet-storage format survival golden vector v1".to_vec();
        let ct = encrypt_chunk(pt, &fixed_key(), &fixed_fid()).unwrap();
        assert_eq!(
            hex_of(&ct),
            "7cac54cedb64fbf60831f4480f883e36a17880a808aacea2e8257be666df4ddb\
             12555c6d7458a6ee9224ca1aeef73fe96fa8cd1c741792f29c1b119a05d788"
        );
    }

    #[test]
    fn golden_ciphertext_exact_buffer_boundary() {
        let pt: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
        let ct = encrypt_chunk(pt, &fixed_key(), &fixed_fid()).unwrap();
        assert_eq!(ct.len(), 4112);
        assert_eq!(
            blake3::hash(&ct).to_hex().to_string(),
            "c83bea197db8fb9d59adca6cd61b161e3cb9dd09195ac1c259a6f4fb3851a5fe"
        );
    }

    #[test]
    fn golden_ciphertext_multi_segment() {
        let pt: Vec<u8> = (0..5000u32)
            .map(|i| (i.wrapping_mul(31) % 256) as u8)
            .collect();
        let ct = encrypt_chunk(pt, &fixed_key(), &fixed_fid()).unwrap();
        assert_eq!(ct.len(), 5032);
        assert_eq!(
            blake3::hash(&ct).to_hex().to_string(),
            "e251300c5f1bb46e01a93eac165b7d6138590b57c05edfedbbec2d391101d010"
        );
    }

    #[test]
    fn encrypt_decrypt_round_trip() {
        // Should: decrypt(encrypt(x)) == x across segment boundaries.
        // Should not: succeed with a different fragment_id (wrong key + nonce).
        // Impact: a format or derivation regression would corrupt every read.
        for len in [0usize, 1, 100, 4096, 4097, 10000] {
            let pt: Vec<u8> = (0..len as u32).map(|i| (i % 251) as u8).collect();
            let ct = encrypt_chunk(pt.clone(), &fixed_key(), &fixed_fid()).unwrap();
            let rt = decrypt_chunk(&ct, &fixed_key(), &fixed_fid()).unwrap();
            assert_eq!(pt, rt, "round trip failed at len {len}");
            assert_eq!(ct.len(), calculate_encrypted_chunk_length(pt.len()));
        }
        let ct = encrypt_chunk(b"abc".to_vec(), &fixed_key(), &fixed_fid()).unwrap();
        let other = CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap();
        assert!(decrypt_chunk(&ct, &fixed_key(), &other).is_err());
    }

    #[test]
    fn wrap_round_trip_legacy() {
        // Should: recipient can unwrap via ECDH with the ephemeral pubkey.
        // Should not: unwrap with a different recipient key or user_id nonce.
        // Impact: wrap regressions lock every user out of their files.
        let recipient_secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let recipient_public = X25519PublicKey::from(&recipient_secret);
        let per_file_key = fixed_key();
        let id = fixed_fid();

        let wrapped = wrap_file_key_legacy(&recipient_public, &id, 7, &per_file_key).unwrap();
        assert_eq!(wrapped.wrapped_key.len(), 48);

        // Recipient-side unwrap (mirrors auth.rs decrypt_wrapped_file_key)
        let shared = recipient_secret.diffie_hellman(&wrapped.ephemeral_pubkey);
        let mut wrap_key_bytes = [0u8; 32];
        let mut hasher = blake3::Hasher::new_derive_key("hopnet key_wrap");
        hasher.update(shared.as_bytes());
        hasher.finalize_xof().fill(&mut wrap_key_bytes);
        let wrap_key = chacha20poly1305::Key::from(wrap_key_bytes);

        let mut nonce_bytes = [0u8; 12];
        let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet wrap_nonce");
        nonce_hasher.update(id.as_bytes());
        nonce_hasher.update(&7i32.to_le_bytes());
        nonce_hasher.update(wrapped.ephemeral_pubkey.as_bytes());
        nonce_hasher.finalize_xof().fill(&mut nonce_bytes);

        let cipher = ChaCha20Poly1305::new(&wrap_key);
        let unwrapped = cipher
            .decrypt(&nonce_bytes.into(), wrapped.wrapped_key.as_slice())
            .unwrap();
        assert_eq!(unwrapped.as_slice(), per_file_key.as_slice());

        // Wrong user_id in the nonce must fail
        let mut wrong_nonce = [0u8; 12];
        let mut wrong_hasher = blake3::Hasher::new_derive_key("hopnet wrap_nonce");
        wrong_hasher.update(id.as_bytes());
        wrong_hasher.update(&8i32.to_le_bytes());
        wrong_hasher.update(wrapped.ephemeral_pubkey.as_bytes());
        wrong_hasher.finalize_xof().fill(&mut wrong_nonce);
        assert!(cipher
            .decrypt(&wrong_nonce.into(), wrapped.wrapped_key.as_slice())
            .is_err());
    }
}
