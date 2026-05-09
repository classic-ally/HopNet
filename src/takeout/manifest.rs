use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::db::CustomUUID;
use crate::types::Blake3Hash;

/// Current manifest schema version. Bumped on any breaking change; see
/// docs/specs/user-data-takeout.md § Manifest Format § Versioning.
pub const MANIFEST_VERSION: u32 = 1;

/// Filename of the manifest entry at the tar root.
pub const MANIFEST_FILENAME: &str = "manifest.json";

/// Prefix applied to all user-facing paths when written into the archive,
/// separating content from the top-level manifest.
pub const ARCHIVE_FILES_PREFIX: &str = "files/";

/// Top-level manifest written as the first entry of every takeout archive.
/// Consumed by import to validate version, compute quota, and drive per-file
/// integrity verification against the salted file_hash.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TakeoutManifest {
    pub version: u32,
    pub takeout_id: CustomUUID,
    pub created_at: DateTime<Utc>,
    pub source_username: String,
    pub total_files: u64,
    pub total_folders: u64,
    pub total_bytes: u64,
    pub folders: Vec<TakeoutManifestFolder>,
    pub files: Vec<TakeoutManifestFile>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TakeoutManifestFolder {
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TakeoutManifestFile {
    pub path: String,
    pub size: u64,
    pub source_data_block_id: CustomUUID,
    pub file_hash: Blake3Hash,
}

impl TakeoutManifest {
    /// Serialize as pretty-printed JSON — the canonical wire format for embedding
    /// in a takeout archive. Single source of truth for the archive format choice;
    /// format drift would otherwise be possible if call sites picked their own
    /// `serde_json::to_*` variant. Pretty-printing costs negligible bytes after
    /// gzip and aids diagnostic inspection.
    pub fn to_archive_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec_pretty(self)
    }
}
