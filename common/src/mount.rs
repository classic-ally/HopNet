//! Wire types for the Linux mount surface (`/api/integrations/mount`,
//! RFC-018 S2).
//!
//! Deliberately NOT `#[typeshare]`: the only consumer is the Rust
//! hopnet-mount daemon, and typeshare sweeps this directory into the
//! Swift/TS/Kotlin generated files — untagged types stay out of those
//! artifacts. Dates are epoch milliseconds (documentprovider precedent).

use serde::{Deserialize, Serialize};

use crate::db::{CustomUUID, InodeType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountItem {
    /// None = the root folder itself.
    pub id: Option<CustomUUID>,
    /// None = the parent is the root.
    pub parent_id: Option<CustomUUID>,
    /// Decrypted name; "" for the root.
    pub name: String,
    pub item_type: InodeType,
    /// Files only (0 for empty files); None for folders.
    pub size: Option<u64>,
    /// The blob backing this file (data_id). None for folders AND for
    /// empty files — there is nothing to download when absent. Load-bearing
    /// for the daemon: downloads are blob-addressed (snapshot-at-open).
    pub blob_id: Option<CustomUUID>,
    pub created_ms: i64,
    pub modified_ms: Option<i64>,
    /// Consensus height of the last modification, when known.
    pub height: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountEnumerateResponse {
    pub items: Vec<MountItem>,
    /// Present when another page exists; opaque, thread it back verbatim.
    pub next_cursor: Option<String>,
    pub height: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountChangesResponse {
    pub items: Vec<MountItem>,
    pub deleted_ids: Vec<CustomUUID>,
    pub height: u64,
}

/// Response to every mount mutation (RFC-018 S6). Sent only after the
/// transaction is decided AND applied on this node; `item` is the fresh
/// post-apply state (None for deletes), `height` the read anchor the
/// daemon may fast-forward to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountMutationResponse {
    pub item: Option<MountItem>,
    pub height: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountModifyRequest {
    pub id: CustomUUID,
    /// New parent folder; None = unchanged, Some(None) is not expressible —
    /// moving to root is `new_parent_id: Some(None)`… kept simple instead:
    /// `new_parent_root: true` moves to root.
    pub new_parent_id: Option<CustomUUID>,
    /// Move to the root folder (new_parent_id must be None).
    #[serde(default)]
    pub new_parent_root: bool,
    /// New name; None = unchanged.
    pub new_name: Option<String>,
    /// POSIX rename(2) semantics: atomically replace an occupied
    /// destination (dir-over-empty-dir included). Default false =
    /// RENAME_NOREPLACE, which is also what pre-replace daemons get.
    #[serde(default)]
    pub replace: bool,
}

/// Machine-readable discriminator on mount `/modify` 409s. Optional:
/// pre-replace daemons ignore the body and keep mapping the bare status;
/// a 409 *without* this body is a consensus rejection, not an occupancy
/// verdict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountConflictBody {
    pub code: MountConflictCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountConflictCode {
    /// Destination occupied and replace was not requested (EEXIST).
    Occupied,
    /// Replace onto a non-empty folder (ENOTEMPTY).
    NotEmpty,
    /// File over a folder (EISDIR).
    IsDirectory,
    /// Folder over a file (ENOTDIR).
    NotDirectory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountDeleteRequest {
    pub id: CustomUUID,
    #[serde(default)]
    pub recursive: bool,
}

/// Node-side statfs numbers (RFC-018 S8): mesh capacity constrained
/// to >= 2-failure tolerance, and raw user bytes consumed at
/// tolerance >= 0 — the resilience pane's definitions, not local-disk
/// state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MountStatfsResponse {
    pub total_bytes: u64,
    pub used_bytes: u64,
}
