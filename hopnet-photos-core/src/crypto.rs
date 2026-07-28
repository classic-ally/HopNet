use crate::error::PhotosCoreError;
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::aead::rand_core::RngCore;
use chacha20poly1305::{ChaCha20Poly1305, Key};
use hopnet_common::CustomUUID;
use hopnet_storage::types::BlobAccess;
use hopnet_storage::RecipientKey;

pub fn generate_metadata_key() -> Key {
    ChaCha20Poly1305::generate_key(OsRng)
}

pub fn generate_blob_key() -> Key {
    ChaCha20Poly1305::generate_key(OsRng)
}

pub fn encrypt_metadata(
    key: &Key,
    plaintext: &[u8],
) -> Result<(Vec<u8>, [u8; 12]), PhotosCoreError> {
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let cipher = ChaCha20Poly1305::new(key);
    let ct = cipher
        .encrypt(&nonce_bytes.into(), plaintext)
        .map_err(|_| PhotosCoreError::Encryption)?;
    Ok((ct, nonce_bytes))
}

pub fn decrypt_metadata(
    key: &Key,
    nonce: &[u8; 12],
    ciphertext: &[u8],
) -> Result<Vec<u8>, PhotosCoreError> {
    let cipher = ChaCha20Poly1305::new(key);
    cipher
        .decrypt(&(*nonce).into(), ciphertext)
        .map_err(|_| PhotosCoreError::Encryption)
}

pub fn wrap_metadata_key(
    photo_id: &CustomUUID,
    recipient: &hopnet_storage::x25519_dalek::PublicKey,
    key: &Key,
) -> Result<([u8; 32], Vec<u8>), PhotosCoreError> {
    hopnet_storage::wrap_key_v1_in_domain(
        &crate::METADATA_KEY_WRAP_DOMAIN,
        photo_id.as_bytes(),
        recipient,
        key,
    )
    .map_err(Into::into)
}

pub fn unwrap_metadata_key(
    photo_id: &CustomUUID,
    ephemeral_pubkey: &[u8; 32],
    wrapped: &[u8],
    reader: &dyn RecipientKey,
) -> Result<Key, PhotosCoreError> {
    hopnet_storage::unwrap_key_v1_in_domain(
        &crate::METADATA_KEY_WRAP_DOMAIN,
        photo_id.as_bytes(),
        reader.pubkey().as_bytes(),
        ephemeral_pubkey,
        wrapped,
        reader,
    )
    .map_err(Into::into)
}

pub fn wrap_blob_key_for_recipient(
    blob_id: &hopnet_storage::BlobId,
    recipient: &hopnet_storage::x25519_dalek::PublicKey,
    per_blob_key: &Key,
) -> Result<BlobAccess, PhotosCoreError> {
    hopnet_storage::crypto::wrap_blob_key(blob_id, recipient, per_blob_key).map_err(Into::into)
}

pub fn wrap_blob_key_for_recipients(
    blob_id: &hopnet_storage::BlobId,
    recipients: &[hopnet_storage::x25519_dalek::PublicKey],
    per_blob_key: &Key,
) -> Result<Vec<BlobAccess>, PhotosCoreError> {
    recipients
        .iter()
        .map(|r| {
            hopnet_storage::crypto::wrap_blob_key(blob_id, r, per_blob_key).map_err(Into::into)
        })
        .collect()
}

pub fn compute_integrity_hash(
    per_blob_key: &Key,
    plaintext: &[u8],
) -> hopnet_common::Blake3Hash {
    hopnet_storage::crypto::integrity_hash(per_blob_key, plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hopnet_storage::StaticRecipient;

    #[test]
    fn metadata_encrypt_decrypt_round_trip() {
        let key = generate_metadata_key();
        let pt = b"hello world";
        let (ct, nonce) = encrypt_metadata(&key, pt).unwrap();
        let dec = decrypt_metadata(&key, &nonce, &ct).unwrap();
        assert_eq!(dec, pt);
    }

    #[test]
    fn metadata_decrypt_wrong_key_fails() {
        let k1 = generate_metadata_key();
        let k2 = generate_metadata_key();
        let (ct, nonce) = encrypt_metadata(&k1, b"data").unwrap();
        assert!(decrypt_metadata(&k2, &nonce, &ct).is_err());
    }

    #[test]
    fn metadata_key_wrap_unwrap_round_trip() {
        let reader = StaticRecipient(hopnet_storage::x25519_dalek::StaticSecret::random_from_rng(OsRng));
        let key = generate_metadata_key();
        let photo_id = CustomUUID::retention_cutoff(0);
        let (eph, wrapped) = wrap_metadata_key(&photo_id, &reader.pubkey(), &key).unwrap();
        let unwrapped = unwrap_metadata_key(&photo_id, &eph, &wrapped, &reader).unwrap();
        assert_eq!(unwrapped.as_slice(), key.as_slice());
    }

    #[test]
    fn metadata_key_wrap_wrong_reader_fails() {
        let r1 = StaticRecipient(hopnet_storage::x25519_dalek::StaticSecret::random_from_rng(OsRng));
        let r2 = StaticRecipient(hopnet_storage::x25519_dalek::StaticSecret::random_from_rng(OsRng));
        let key = generate_metadata_key();
        let photo_id = CustomUUID::retention_cutoff(1);
        let (eph, wrapped) = wrap_metadata_key(&photo_id, &r1.pubkey(), &key).unwrap();
        assert!(unwrap_metadata_key(&photo_id, &eph, &wrapped, &r2).is_err());
    }

    #[test]
    fn generate_metadata_key_is_random() {
        let k1 = generate_metadata_key();
        let k2 = generate_metadata_key();
        assert_ne!(k1.as_slice(), k2.as_slice());
    }

    #[test]
    fn wrap_blob_key_for_recipient_builds_valid_blob_access() {
        let key = generate_blob_key();
        let blob_id = CustomUUID::retention_cutoff(2);
        let reader = StaticRecipient(hopnet_storage::x25519_dalek::StaticSecret::random_from_rng(OsRng));
        let access = wrap_blob_key_for_recipient(&blob_id, &reader.pubkey(), &key).unwrap();
        assert_eq!(access.wrapped_key.len(), 48);
        assert_eq!(access.recipient_pubkey, *reader.pubkey().as_bytes());
    }
}
