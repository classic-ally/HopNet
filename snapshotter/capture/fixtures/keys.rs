use aes_siv::aead::KeyInit;
use aes_siv::aead::generic_array::GenericArray;
use aes_siv::siv::Aes256Siv;
use ed25519_dalek::SigningKey;
use hopnet::db::types::XPubKey;
use hopnet::types::{PrivKey, PubKey};
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

/// Deterministic Ed25519 key pair from a const seed
pub fn ed25519_from_seed(seed: [u8; 32]) -> (PrivKey, PubKey) {
    let signing_key = SigningKey::from_bytes(&seed);
    let verifying_key = signing_key.verifying_key();
    (PrivKey(signing_key), PubKey(verifying_key))
}

/// Deterministic X25519 public key from a const seed
pub fn x25519_pubkey_from_seed(seed: [u8; 32]) -> XPubKey {
    let secret = StaticSecret::from(seed);
    let public = X25519PublicKey::from(&secret);
    XPubKey::from_x25519(public)
}

/// Deterministic AES-256-SIV key and nonce from a const seed
pub fn siv_from_seed(
    seed: [u8; 32],
) -> (
    aes_siv::Key<aes_siv::siv::Aes256Siv>,
    GenericArray<u8, aes_siv::aead::consts::U16>,
) {
    // Derive 64 bytes for the SIV key
    let mut siv_key_bytes = [0u8; 64];
    let mut hasher = blake3::Hasher::new_derive_key("snapshotter siv_key");
    hasher.update(&seed);
    hasher.finalize_xof().fill(&mut siv_key_bytes);

    // Derive 16 bytes for the nonce
    let mut siv_nonce_bytes = [0u8; 16];
    let mut hasher = blake3::Hasher::new_derive_key("snapshotter siv_nonce");
    hasher.update(&seed);
    hasher.finalize_xof().fill(&mut siv_nonce_bytes);

    let siv_key = aes_siv::Key::<Aes256Siv>::from(siv_key_bytes);
    let siv_nonce = GenericArray::from(siv_nonce_bytes);
    (siv_key, siv_nonce)
}

// Fixed seeds for deterministic key generation
pub const USER_0_SEED: [u8; 32] = [1u8; 32];
pub const USER_1_SEED: [u8; 32] = [2u8; 32];
pub const NODE_0_SEED: [u8; 32] = [10u8; 32];
pub const NODE_1_SEED: [u8; 32] = [11u8; 32];
pub const NODE_2_SEED: [u8; 32] = [12u8; 32];
pub const SIV_SEED: [u8; 32] = [20u8; 32];

// X25519 seeds for user file encryption
pub const USER_0_X25519_SEED: [u8; 32] = [30u8; 32];
pub const USER_1_X25519_SEED: [u8; 32] = [31u8; 32];
