//! Drive transaction envelopes (RFC-015).
//!
//! Wire types for the drive projection's consensus transactions — moved
//! verbatim from the host's `files::handlers` / `shares::types` (the
//! handlers themselves stay host-side until Stage D3). Serde field order
//! is bincode-frozen; do not reorder.

use crate::model::Inode;
use hopnet_common::CustomUUID;
use serde::{Deserialize, Serialize};

/// The DRIVE projection's insert envelope: substrate blob registrations
/// (crate sub-payloads) alongside the inodes that reference them by id.
/// Both halves apply in ONE handler transaction — blob + first reference
/// are atomic, so mark-and-sweep never observes a zero-ref blob.
/// (Envelope ownership: this type belongs to the drive projection and
/// extracts with it to hopnet-drive.)
#[derive(Serialize, Deserialize, Debug)]
pub struct DriveInsertPayload {
    pub blob_ops: Vec<hopnet_storage::store::BlobInsertOp>,
    pub inodes: Vec<Inode>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteFilesPayload {
    pub encrypted_path: String,
    pub user_id: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ModifyItemPayload {
    pub user_id: i32,
    pub inode_id: CustomUUID,               // Stable inode identifier
    pub new_encrypted_path: Option<String>, // New path if renaming/moving
    /// Content change, when present. `blob_op: None` means the content is
    /// now EMPTY — the inode's data_id becomes NULL (no blob exists for
    /// empty content; RFC-014).
    pub content_update: Option<DriveContentUpdate>,
    // Phase 2b: Share propagation — pre-computed updates for pending incoming_shares
    pub incoming_share_updates: Option<Vec<IncomingShareUpdate>>,
}

/// Drive-scoped content-update sub-payload (extracts with the projection).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DriveContentUpdate {
    pub blob_op: Option<hopnet_storage::store::BlobInsertOp>,
}

/// Pre-computed file_access blob update for a pending incoming_share.
/// The route handler creates these because only it has the decrypted per-file key.
/// The consensus handler applies them during propagation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IncomingShareUpdate {
    pub incoming_share_id: CustomUUID,
    pub new_file_access_blob: Vec<u8>,
}
