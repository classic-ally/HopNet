//! On-disk path derivations (spec §Architecture, §Ingest Pipeline).
//!
//! Every path rule lives here so the writer and the eviction/audit code
//! share one source of truth. Pure path math — no I/O.

use std::path::{Path, PathBuf};

use crate::ids::{ContentHash, PhotoId};
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

/// The transient spool's content-addressed tree, rooted at
/// `<data_dir>/spool` in production ([`DataDir::spool`]). Bytes live here
/// between the PhotoKit fetch and the consensus-decided publish that lets
/// eviction delete them — HopNet is the archive of record; this is staging.
#[derive(Debug, Clone)]
pub struct SpoolPaths {
    root: PathBuf,
}

impl SpoolPaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// `<spool>/blobs/.partial/` — in-flight write temps. Same filesystem
    /// as final blob paths, by construction: atomic rename.
    pub fn partial_dir(&self) -> PathBuf {
        self.root.join("blobs").join(".partial")
    }

    pub fn temp_path(&self, key: &TempKey) -> PathBuf {
        self.partial_dir().join(key.file_name())
    }

    /// `<spool>/blobs/` — the content-addressed tree root (fsck's
    /// orphan-scan walk starts here).
    pub fn blobs_dir(&self) -> PathBuf {
        self.root.join("blobs")
    }

    /// `<spool>/blobs/<aa>/<bb>/<hash>.<ext>` — final content-addressed path.
    pub fn blob_path(&self, hash: &ContentHash, ext: &str) -> PathBuf {
        let (aa, bb) = hash.fanout();
        self.root
            .join("blobs")
            .join(aa)
            .join(bb)
            .join(format!("{hash}.{ext}"))
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

    /// `<root>/spool/` — the transient blob spool (process-global; library
    /// partitioning lives in the `blobs` ledger, not on disk).
    pub fn spool(&self) -> SpoolPaths {
        SpoolPaths::new(self.root.join("spool"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: derive the spec's fan-out blob path from hash and extension.
    #[test]
    fn blob_path_fanout() {
        let paths = DataDir::new("/tmp/ingress").spool();
        let hash = ContentHash::from_hex("ab34cdef00112233445566778899aabb");
        assert_eq!(
            paths.blob_path(&hash, "heic").to_string_lossy(),
            "/tmp/ingress/spool/blobs/ab/34/ab34cdef00112233445566778899aabb.heic"
        );
    }

    // Impact: temps must live on the destination filesystem — atomic rename
    // cannot cross filesystems (spec §Write path).
    // Should: place both temp shapes under <spool>/blobs/.partial/.
    #[test]
    fn temp_paths_live_under_partial() {
        let paths = SpoolPaths::new("/tmp/ingress/spool");
        let probe = paths.temp_path(&TempKey::Probe {
            token: "abc123".into(),
        });
        assert_eq!(
            probe.to_string_lossy(),
            "/tmp/ingress/spool/blobs/.partial/probe-abc123"
        );

        let photo_id = PhotoId::mint();
        let res = paths.temp_path(&TempKey::Resource {
            photo_id: photo_id.clone(),
            resource_type: ResourceType::PairedVideo,
        });
        assert_eq!(
            res.to_string_lossy(),
            format!("/tmp/ingress/spool/blobs/.partial/{photo_id}.2")
        );
    }

    // Should: derive state.db and the spool root from the data root.
    #[test]
    fn data_dir_derivations() {
        let dir = DataDir::new("/tmp/ingress");
        assert_eq!(
            dir.state_db_path().to_string_lossy(),
            "/tmp/ingress/state.db"
        );
        assert_eq!(
            dir.spool().blobs_dir().to_string_lossy(),
            "/tmp/ingress/spool/blobs"
        );
    }
}
