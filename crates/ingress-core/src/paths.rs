//! On-disk path derivations (spec §Architecture, §Ingest Pipeline).
//!
//! Every path rule lives here so the writer, sidecar I/O, and future
//! recovery code share one source of truth. Pure path math — no I/O.

use std::path::{Path, PathBuf};

use crate::ids::{ContentHash, LibraryId, PhotoId};
use crate::model::ResourceType;

/// Key for an in-flight `.partial` temp file.
///
/// The spec names temps `<photo_id>.<resource_type>`, which also provides
/// structural inflight exclusivity per resource. One structural exception:
/// a brand-new photo's original streams *before* a `photo_id` exists
/// (minting happens inside `resolve_with_hash`, so rule 2a never merges a
/// provisional row) — those streams use a fresh probe token instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TempKey {
    /// Post-mint resource stream: spec naming.
    Resource {
        photo_id: PhotoId,
        resource_type: ResourceType,
    },
    /// Pre-mint original probe (rules 2a–2c): caller-supplied unique token.
    Probe { token: String },
}

impl TempKey {
    fn file_name(&self) -> String {
        match self {
            TempKey::Resource {
                photo_id,
                resource_type,
            } => {
                format!("{photo_id}.{}", *resource_type as i64)
            }
            TempKey::Probe { token } => format!("probe-{token}"),
        }
    }
}

/// Per-library blob-tree paths, rooted at `libraries.blob_root`.
#[derive(Debug, Clone)]
pub struct BlobPaths {
    blob_root: PathBuf,
}

impl BlobPaths {
    pub fn new(blob_root: impl Into<PathBuf>) -> Self {
        Self {
            blob_root: blob_root.into(),
        }
    }

    /// `<blob_root>/blobs/.partial/` — in-flight write temps. Same
    /// filesystem as final blob paths, by construction: atomic rename.
    pub fn partial_dir(&self) -> PathBuf {
        self.blob_root.join("blobs").join(".partial")
    }

    pub fn temp_path(&self, key: &TempKey) -> PathBuf {
        self.partial_dir().join(key.file_name())
    }

    /// `<blob_root>/blobs/` — the content-addressed tree root (fsck's
    /// orphan-scan walk starts here).
    pub fn blobs_dir(&self) -> PathBuf {
        self.blob_root.join("blobs")
    }

    /// `<blob_root>/blobs/<aa>/<bb>/<hash>.<ext>` — final content-addressed path.
    pub fn blob_path(&self, hash: &ContentHash, ext: &str) -> PathBuf {
        let (aa, bb) = hash.fanout();
        self.blob_root
            .join("blobs")
            .join(aa)
            .join(bb)
            .join(format!("{hash}.{ext}"))
    }

    /// `<blob_root>/state-snapshots/` — daily state.db snapshots on the
    /// storage side (what Tier-3 recovery restores from on a dead Mac).
    pub fn snapshot_dir(&self) -> PathBuf {
        self.blob_root.join("state-snapshots")
    }
}

/// The daemon's local data directory (`~/.local/share/hopnet-photo-ingress`
/// in production, a temp dir in tests). The core never hardcodes `$HOME` —
/// the caller supplies the root.
#[derive(Debug, Clone)]
pub struct DataDir {
    root: PathBuf,
}

impl DataDir {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `<root>/state.db`
    pub fn state_db_path(&self) -> PathBuf {
        self.root.join("state.db")
    }

    /// `<root>/sidecars/<library_id>/` — hot-path local sidecar tree.
    /// Always derived, never stored (spec: `libraries` notes).
    pub fn sidecar_root(&self, library: &LibraryId) -> PathBuf {
        self.root.join("sidecars").join(library.as_str())
    }

    /// `<root>/state-snapshots-tmp/` — staging dir for `VACUUM INTO` before
    /// the per-root copies (VACUUM refuses an existing target, so the dir is
    /// cleaned before each use).
    pub fn snapshot_tmp_dir(&self) -> PathBuf {
        self.root.join("state-snapshots-tmp")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: derive the spec's fan-out blob path from hash and extension.
    #[test]
    fn blob_path_fanout() {
        let paths = BlobPaths::new("/mnt/photos/personal");
        let hash = ContentHash::from_hex("ab34cdef00112233445566778899aabb");
        assert_eq!(
            paths.blob_path(&hash, "heic").to_string_lossy(),
            "/mnt/photos/personal/blobs/ab/34/ab34cdef00112233445566778899aabb.heic"
        );
    }

    // Impact: temps must live on the destination filesystem — atomic rename
    // cannot cross filesystems (spec §Write path).
    // Should: place both temp shapes under <blob_root>/blobs/.partial/.
    #[test]
    fn temp_paths_live_under_partial() {
        let paths = BlobPaths::new("/mnt/photos/personal");
        let probe = paths.temp_path(&TempKey::Probe {
            token: "abc123".into(),
        });
        assert_eq!(
            probe.to_string_lossy(),
            "/mnt/photos/personal/blobs/.partial/probe-abc123"
        );

        let photo_id = PhotoId::mint();
        let res = paths.temp_path(&TempKey::Resource {
            photo_id: photo_id.clone(),
            resource_type: ResourceType::PairedVideo,
        });
        assert_eq!(
            res.to_string_lossy(),
            format!("/mnt/photos/personal/blobs/.partial/{photo_id}.2")
        );
    }

    // Should: derive state.db and per-library sidecar paths from the data root.
    #[test]
    fn data_dir_derivations() {
        let dir = DataDir::new("/tmp/ingress");
        assert_eq!(
            dir.state_db_path().to_string_lossy(),
            "/tmp/ingress/state.db"
        );
        assert_eq!(
            dir.sidecar_root(&LibraryId::new("personal"))
                .to_string_lossy(),
            "/tmp/ingress/sidecars/personal"
        );
    }
}
