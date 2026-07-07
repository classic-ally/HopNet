//! Takeout manifest v2 (RFC-015 Stage D5, decision 8): projection-namespaced
//! sections. v1's flat `folders`/`files` arrays are gone — nothing was
//! deployed, so v1 dies rather than being migrated.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use hopnet_common::{Blake3Hash, CustomUUID};

/// Current manifest schema version. Bumped on any breaking change; import
/// rejects anything else (no version negotiation — fresh format).
pub const MANIFEST_VERSION: u32 = 2;

/// Filename of the manifest entry at the tar root.
pub const MANIFEST_FILENAME: &str = "manifest.json";

/// Top-level manifest written as the first entry of every takeout archive.
/// Consumed by import to validate version, compute quota, and drive per-file
/// integrity verification against the export-computed content hash.
///
/// Archive layout: `manifest.json` + `{projection}/<logical_path>` for every
/// entry of every section.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TakeoutManifest {
    pub version: u32,
    pub takeout_id: CustomUUID,
    pub created_at: DateTime<Utc>,
    pub source_username: String,
    /// One section per projection that participated in the export, keyed by
    /// the projection's registered name ("drive", "photos", …). BTreeMap for
    /// deterministic serialization. `default` so a version-mismatched
    /// manifest still parses far enough to report the VERSION error.
    #[serde(default)]
    pub projections: BTreeMap<String, ProjectionSection>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ProjectionSection {
    pub total_files: u64,
    pub total_folders: u64,
    pub total_bytes: u64,
    pub entries: Vec<ManifestEntry>,
}

/// What kind of thing an entry is. Serialized as `"file"` / `"folder"`.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    File,
    Folder,
}

/// One exported unit within a projection section. The sidecar principle
/// (RFC-015): `metadata` must carry everything a fresh mesh needs to
/// reconstruct the projection's state from entries alone.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManifestEntry {
    /// Decrypted logical path within the projection's namespace (no leading
    /// slash, no projection prefix — the archive prepends `{projection}/`).
    pub logical_path: String,
    pub kind: EntryKind,
    pub size: u64,
    /// Content blob id on the SOURCE mesh (None for folders/containers).
    pub blob_id: Option<CustomUUID>,
    /// Export-computed `blake3(plaintext ‖ blob_id bytes)` — the import side
    /// recomputes it during extraction. None for folders. (The DB's
    /// integrity hash is keyed and unverifiable by an importer, so the
    /// manifest carries this instead — formula unchanged from v1.)
    pub content_hash: Option<Blake3Hash>,
    /// Projection-specific metadata (self-describing, versioned by the
    /// projection). `{}` when the projection needs nothing beyond the path.
    #[serde(default)]
    pub metadata: serde_json::Value,
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

    /// Sum of `total_bytes` across all sections (import quota input).
    /// Checked — `None` on overflow so callers can map to their own error.
    pub fn total_bytes(&self) -> Option<u64> {
        self.projections
            .values()
            .try_fold(0u64, |acc, s| acc.checked_add(s.total_bytes))
    }

    /// Sum of `total_files` across all sections (log output).
    pub fn total_files(&self) -> u64 {
        self.projections
            .values()
            .fold(0u64, |acc, s| acc.saturating_add(s.total_files))
    }
}
