//! Substrate-owned wire/state types.

use serde::{Deserialize, Serialize};

pub use hopnet_common::{Blake3Hash, CustomUUID};

/// A blob's stable identity (today's `data_block_id`). Random UUIDv7 —
/// public, plaintext-independent; seeds placement and never changes across
/// the blob's life (rekey mints a NEW blob id).
pub type BlobId = CustomUUID;

/// One grant of the mesh-wide X25519 private key to a member's pubkey
/// (v1 wrap with the mesh-key wrap id). Replicated: rides the genesis and
/// insert_user transactions into the `mesh_key_access` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeshKeyGrant {
    pub recipient_pubkey: [u8; 32],
    pub ephemeral_pubkey: [u8; 32],
    pub wrapped_privkey: Vec<u8>, // 48 bytes (32 + 16 auth tag)
}

/// One blob's placement commit: records the consensus height whose
/// validator/metrics snapshot the placement was computed against. Batched —
/// the engine submits `Vec<PlacementUpdate>` as ONE `update_placement_heights`
/// transaction per flush window. (Storage-owned tx payload, decision #0;
/// bincode-compatible with the legacy PlacementHeightUpdate shape.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementUpdate {
    pub blob_id: BlobId,
    pub placement_height: i32,
}

/// One wrap of a per-blob key to a recipient X25519 pubkey (v1 format).
/// Replicated state: rides consensus transactions and lands in the
/// `blob_access` table. Keyed by pubkey — the substrate is user-agnostic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlobAccess {
    pub blob_id: BlobId,
    /// Recipient's X25519 public key (a user's derived key or the mesh key).
    pub recipient_pubkey: [u8; 32],
    /// Fresh per-wrap ephemeral X25519 public key.
    pub ephemeral_pubkey: [u8; 32],
    /// ChaCha20-Poly1305(wrap_key, wrap_nonce, per_blob_key) — 48 bytes.
    pub wrapped_key: Vec<u8>,
}
