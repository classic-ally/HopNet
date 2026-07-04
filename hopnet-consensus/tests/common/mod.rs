#![allow(dead_code)]
use ed25519_dalek::SigningKey;
use hopnet_consensus::context::Validator;
use hopnet_consensus::types::{Blake3Hash, PrivKey, PubKey};
use hopnet_consensus::HopNetValidatorSet;

/// Deterministic test key for a node id.
pub fn key(node_id: i32) -> PrivKey {
    let mut seed = [0u8; 32];
    seed[..4].copy_from_slice(&node_id.to_le_bytes());
    seed[31] = 0xA5;
    PrivKey(SigningKey::from_bytes(&seed))
}

pub fn pubkey(node_id: i32) -> PubKey {
    key(node_id).public()
}

/// Validator set over node ids 0..n with uniform power 1.
pub fn valset(n: i32) -> HopNetValidatorSet {
    HopNetValidatorSet::new((0..n).map(|i| Validator::new(i, pubkey(i))).collect())
}

pub fn chain_id() -> Blake3Hash {
    Blake3Hash::from_bytes([7u8; 32])
}
