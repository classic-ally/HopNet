use serde::{Serialize, Deserialize};
use crate::db::CustomUUID;
use crate::db::types::XPubKey;
use x25519_dalek::PublicKey as X25519PublicKey;
use chacha20poly1305::{ChaCha20Poly1305, aead::{Aead, OsRng, KeyInit}};

// --- Consensus payload structs ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShareFilePayload {
    pub id: CustomUUID,
    pub data_block_id: CustomUUID,
    pub sender_id: i32,
    pub recipient_id: i32,
    pub file_access: Vec<u8>,                // bincode-encoded FileAccess for recipient
    pub display_ephemeral_pubkey: Vec<u8>,   // 32 bytes X25519 ephemeral public key
    pub encrypted_display_name: Vec<u8>,     // ChaCha20-Poly1305 ciphertext
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AcceptSharePayload {
    pub incoming_share_id: CustomUUID,
    pub recipient_id: i32,
    pub encrypted_path: String,
    pub inode_id: CustomUUID,
    pub parent_folder_inodes: Vec<(CustomUUID, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeclineSharePayload {
    pub incoming_share_id: CustomUUID,
    pub user_id: i32,
}

/// Pre-computed file_access blob update for a pending incoming_share.
/// The route handler creates these because only it has the decrypted per-file key.
/// The consensus handler applies them during propagation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IncomingShareUpdate {
    pub incoming_share_id: CustomUUID,
    pub new_file_access_blob: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnsharePayload {
    pub inode_id: CustomUUID,
    pub user_id: i32,
}

// --- API request/response types (re-exported from common) ---

pub use hopnet_common::shares::{
    ShareRequest, AcceptShareRequest, IncomingShareResponse,
    ShareCountResponse, ShareDetailResponse, ShareParticipant,
};

// --- Display name crypto ---

/// Encrypt a display name for a specific recipient using ECDH + Blake3 KDF + ChaCha20-Poly1305.
/// Returns (ephemeral_pubkey_bytes, ciphertext).
pub fn encrypt_display_name(
    plaintext: &str,
    recipient_x25519_pubkey: &XPubKey,
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    let ephemeral_secret = x25519_dalek::EphemeralSecret::random_from_rng(&mut OsRng);
    let ephemeral_public = X25519PublicKey::from(&ephemeral_secret);

    let shared_secret = ephemeral_secret.diffie_hellman(recipient_x25519_pubkey.as_x25519());

    // Derive wrapping key
    let mut wrap_key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet display_name_wrap");
    hasher.update(shared_secret.as_bytes());
    hasher.finalize_xof().fill(&mut wrap_key_bytes);
    let key = chacha20poly1305::Key::from(wrap_key_bytes);

    // Derive nonce from ephemeral pubkey
    let mut nonce_bytes = [0u8; 12];
    let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet display_name_nonce");
    nonce_hasher.update(ephemeral_public.as_bytes());
    nonce_hasher.finalize_xof().fill(&mut nonce_bytes);
    let nonce = chacha20poly1305::Nonce::from(nonce_bytes);

    let cipher = ChaCha20Poly1305::new(&key);
    let ciphertext = cipher.encrypt(&nonce, plaintext.as_bytes())
        .map_err(|e| format!("Display name encryption failed: {:?}", e))?;

    Ok((ephemeral_public.as_bytes().to_vec(), ciphertext))
}

/// Decrypt a display name using the recipient's X25519 private key.
pub fn decrypt_display_name(
    ephemeral_pubkey_bytes: &[u8],
    ciphertext: &[u8],
    recipient_x25519_privkey: &x25519_dalek::StaticSecret,
) -> Result<String, Box<dyn std::error::Error>> {
    if ephemeral_pubkey_bytes.len() != 32 {
        return Err("Invalid ephemeral pubkey length".into());
    }

    let mut pubkey_arr = [0u8; 32];
    pubkey_arr.copy_from_slice(ephemeral_pubkey_bytes);
    let ephemeral_pubkey = X25519PublicKey::from(pubkey_arr);

    let shared_secret = recipient_x25519_privkey.diffie_hellman(&ephemeral_pubkey);

    let mut wrap_key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet display_name_wrap");
    hasher.update(shared_secret.as_bytes());
    hasher.finalize_xof().fill(&mut wrap_key_bytes);
    let key = chacha20poly1305::Key::from(wrap_key_bytes);

    let mut nonce_bytes = [0u8; 12];
    let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet display_name_nonce");
    nonce_hasher.update(ephemeral_pubkey.as_bytes());
    nonce_hasher.finalize_xof().fill(&mut nonce_bytes);
    let nonce = chacha20poly1305::Nonce::from(nonce_bytes);

    let cipher = ChaCha20Poly1305::new(&key);
    let plaintext = cipher.decrypt(&nonce, ciphertext)
        .map_err(|e| format!("Display name decryption failed: {:?}", e))?;

    String::from_utf8(plaintext).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chacha20poly1305::aead::OsRng;

    #[test]
    fn test_display_name_encrypt_decrypt_round_trip() {
        let secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let pubkey = X25519PublicKey::from(&secret);
        let xpubkey = XPubKey::from(pubkey);

        let plaintext = "document.pdf";
        let (eph_pubkey, ciphertext) = encrypt_display_name(plaintext, &xpubkey).unwrap();
        let decrypted = decrypt_display_name(&eph_pubkey, &ciphertext, &secret).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_display_name_wrong_key_fails() {
        let secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let pubkey = X25519PublicKey::from(&secret);
        let xpubkey = XPubKey::from(pubkey);

        let wrong_secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);

        let (eph_pubkey, ciphertext) = encrypt_display_name("secret.txt", &xpubkey).unwrap();
        let result = decrypt_display_name(&eph_pubkey, &ciphertext, &wrong_secret);
        assert!(result.is_err());
    }

    #[test]
    fn test_display_name_unicode() {
        let secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let pubkey = X25519PublicKey::from(&secret);
        let xpubkey = XPubKey::from(pubkey);

        let plaintext = "notes 2026.txt";
        let (eph_pubkey, ciphertext) = encrypt_display_name(plaintext, &xpubkey).unwrap();
        let decrypted = decrypt_display_name(&eph_pubkey, &ciphertext, &secret).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
