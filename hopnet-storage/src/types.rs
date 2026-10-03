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
/// transaction per flush window. (Storage-owned tx payload, decision #0.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementUpdate {
    pub blob_id: BlobId,
    pub placement_height: u64,
}

/// Batch of orphaned blob ids for consensus deletion (storage-owned tx
/// payload). Liveness gates (takeout in flight, reference providers) are
/// the HOST's responsibility before submitting.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteOrphanedDataBlocksPayload {
    pub data_block_ids: Vec<BlobId>,
}

/// Differential self-attestation report for fragment inventory synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfCheckFragments {
    /// Node performing the self-check
    pub node_id: i32,

    /// Consensus height when this check was performed
    pub self_verified_height: u64,

    /// The builder's inventory row count at build time. Informational
    /// since the apply became idempotent (appliers log a mismatch at DEBUG,
    /// never gate on it); kept for wire stability. New builders send 0.
    pub previous_count: u32,

    /// Fragments found locally but not in consensus inventory
    pub fragments_added: Vec<Blake3Hash>,

    /// Fragments in consensus inventory but not found locally
    pub fragments_removed: Vec<Blake3Hash>,
}

impl SelfCheckFragments {
    /// Check if this is an empty report (no changes)
    pub fn is_empty(&self) -> bool {
        self.fragments_added.is_empty() && self.fragments_removed.is_empty()
    }
}

/// Disk-truth attestation (RFC-STORAGE-003 S5): the fragments this node
/// saw on its own disk this cycle (`present` — existence- or content-
/// verified, stamped `verified_height = height`, provenance self-scan) and
/// the rows it marks suspect (never by self-scan; the proof-of-possession
/// successor's hook). Storage-owned tx payload, function `attest_fragments`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FragmentAttestation {
    pub node_id: i32,
    /// Consensus height the verification was performed against.
    pub height: u64,
    pub present: Vec<Blake3Hash>,
    pub suspect: Vec<Blake3Hash>,
}

impl FragmentAttestation {
    pub fn is_empty(&self) -> bool {
        self.present.is_empty() && self.suspect.is_empty()
    }
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
