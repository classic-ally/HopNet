//! Tier-2 on-demand invariant audit (spec §Recovery Tier 2): refcount
//! drift, missing blob files (byte loss — loud, never repairable), orphan
//! blob files (deleted only under `--repair` — the one destructive repair),
//! and sidecar consistency, local and remote.
//!
//! The default run is READ-ONLY: it works on a read-only store, takes no
//! lock, and logs nothing. `--repair` requires a writable store; it takes
//! the exclusive run lock, applies the refcount repair, and deletes
//! exact-match orphan files.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::ids::{ContentHash, LibraryId, PhotoId};
use crate::model::LibraryConfig;
use crate::paths::{BlobPaths, DataDir};
use crate::recovery::{RefcountDrift, recount_diff, repair_refcounts};
use crate::sidecar::Sidecar;
use crate::sidecar_io::find_sidecar;
use crate::store::StateStore;

#[derive(Debug, Clone, Copy, Default)]
pub struct FsckOptions {
    /// Apply repairs: refcount drift and orphan-file deletion. Requires a
    /// writable store; takes the exclusive run lock.
    pub repair: bool,
    /// Byte-compare remote sidecars against local (default: existence only).
    pub deep: bool,
}

/// A `blobs` row whose file is gone: byte loss (or manual tampering).
/// Never repairable from local state — the resource must be re-fetched
/// from PhotoKit if the asset still exists there.
#[derive(Debug, serde::Serialize)]
pub struct MissingBlob {
    pub library_id: LibraryId,
    pub content_hash: ContentHash,
    pub ext: String,
    pub expected_path: PathBuf,
}

/// A well-formed blob file with no `blobs` row (crash window between
/// rename and commit, or a crashed hard-delete's leftovers).
#[derive(Debug, serde::Serialize)]
pub struct OrphanBlob {
    pub library_id: LibraryId,
    pub path: PathBuf,
}

/// A file whose hash matches a row but whose extension differs.
/// Informational — never deleted (the row's own file is separately
/// reported missing by the blob-existence check).
#[derive(Debug, serde::Serialize)]
pub struct ExtMismatch {
    pub library_id: LibraryId,
    pub content_hash: ContentHash,
    pub row_ext: String,
    pub file_ext: String,
    pub path: PathBuf,
}

#[derive(Debug, serde::Serialize)]
pub struct SidecarFinding {
    pub photo_id: PhotoId,
    pub library_id: LibraryId,
    #[serde(flatten)]
    pub kind: SidecarProblem,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SidecarProblem {
    /// Materialized photo with no local sidecar (crash between the
    /// completion tx and the first sidecar write).
    Missing,
    ParseError {
        error: String,
    },
    /// Db-backed fields disagree with the document. Capture metadata lives
    /// only in the sidecar and is deliberately not checkable.
    Mismatch {
        fields: Vec<String>,
    },
}

#[derive(Debug, serde::Serialize)]
pub struct RemoteFinding {
    pub photo_id: PhotoId,
    pub library_id: LibraryId,
    #[serde(flatten)]
    pub kind: RemoteProblem,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteProblem {
    /// `sidecar_replicated_at` is stamped but the remote copy is absent.
    Missing { path: PathBuf },
    /// Remote bytes differ from local (`--deep` only).
    Differs { path: PathBuf },
}

#[derive(Debug, Default, serde::Serialize)]
pub struct FsckReport {
    pub refcount_drift: Vec<RefcountDrift>,
    /// A repair pass ran (drift applied and/or Tier-1 on unclean reclaim).
    pub refcount_repaired: bool,
    pub missing_blobs: Vec<MissingBlob>,
    pub orphan_blobs: Vec<OrphanBlob>,
    pub orphans_deleted: u64,
    pub ext_mismatches: Vec<ExtMismatch>,
    /// Unparseable names or wrong-depth entries in the blob tree — never
    /// deleted, listed for the operator.
    pub foreign_files: Vec<PathBuf>,
    pub sidecar_findings: Vec<SidecarFinding>,
    pub remote_findings: Vec<RemoteFinding>,
    /// Roots that could not be checked (mount down). Advisory: an absent
    /// mount must never read as byte loss.
    pub skipped_roots: Vec<String>,
}

impl FsckReport {
    /// No findings remain (repaired classes count as resolved). Skipped
    /// roots are advisory and deliberately excluded — rerun with the mount
    /// up for full coverage.
    pub fn is_clean(&self) -> bool {
        (self.refcount_drift.is_empty() || self.refcount_repaired)
            && self.missing_blobs.is_empty()
            && (self.orphan_blobs.len() as u64 == self.orphans_deleted)
            && self.ext_mismatches.is_empty()
            && self.foreign_files.is_empty()
            && self.sidecar_findings.is_empty()
            && self.remote_findings.is_empty()
    }
}

pub async fn run_fsck(
    store: &StateStore,
    data_dir: &DataDir,
    opts: &FsckOptions,
) -> Result<FsckReport> {
    let mut report = FsckReport::default();

    // Repair mode holds the exclusive lock for the whole run; an unclean
    // reclaim runs Tier-1 immediately — the signal must not be swallowed
    // (the next daemon start would look clean and skip it).
    let _lock = if opts.repair {
        let acquired = crate::runlock::DrainLock::acquire(data_dir)?;
        if acquired.unclean {
            repair_refcounts(store).await?;
            report.refcount_repaired = true;
        }
        Some(acquired.lock)
    } else {
        None
    };

    // (a) Refcount recount/diff — pure read; repaired afterwards under
    // --repair so the report still shows what was found.
    {
        let mut conn = store.pool().acquire().await?;
        report.refcount_drift = recount_diff(&mut conn).await?;
    }
    if opts.repair && !report.refcount_drift.is_empty() {
        repair_refcounts(store).await?;
        report.refcount_repaired = true;
    }

    let libraries = store.libraries().await?;
    for library in &libraries {
        check_blob_tree(store, library, opts, &mut report).await?;
        check_sidecars(store, data_dir, library, opts, &mut report).await?;
    }

    if report.orphans_deleted > 0 {
        let samples: Vec<String> = report
            .orphan_blobs
            .iter()
            .take(50)
            .map(|o| o.path.to_string_lossy().into_owned())
            .collect();
        store
            .append_log(
                "fsck_orphans_deleted",
                None,
                Some(serde_json::json!({
                    "deleted": report.orphans_deleted,
                    "samples": samples,
                })),
            )
            .await?;
    }
    Ok(report)
}

/// Blob-existence (b) and orphan-scan (c) checks for one library.
async fn check_blob_tree(
    store: &StateStore,
    library: &LibraryConfig,
    opts: &FsckOptions,
    report: &mut FsckReport,
) -> Result<()> {
    let paths = BlobPaths::new(&library.blob_root);
    let rows = store.blobs_for_library(&library.library_id).await?;

    // Mount-down guard: an entirely-absent root is advisory, not thousands
    // of byte-loss findings.
    if !Path::new(&library.blob_root).is_dir() {
        report.skipped_roots.push(format!(
            "{}: blob root unavailable ({})",
            library.library_id, library.blob_root
        ));
        return Ok(());
    }

    // (b) every row's file must exist. The root is reachable here, so a
    // miss is genuine.
    let mut row_exts: HashMap<ContentHash, String> = HashMap::new();
    for row in rows {
        let expected = paths.blob_path(&row.content_hash, &row.ext);
        if !expected.is_file() {
            report.missing_blobs.push(MissingBlob {
                library_id: library.library_id.clone(),
                content_hash: row.content_hash.clone(),
                ext: row.ext.clone(),
                expected_path: expected,
            });
        }
        row_exts.insert(row.content_hash, row.ext);
    }

    // (c) walk <blob_root>/blobs/ two fan-out levels, skipping .partial
    // and Finder's .DS_Store droppings (browsing the share plants them at
    // every level; flagging them would pin fsck at exit 1 forever).
    let blobs_dir = paths.blobs_dir();
    if !blobs_dir.is_dir() {
        return Ok(()); // nothing written yet; (b) already covered the rows
    }
    for aa in fs::read_dir(&blobs_dir).map_err(io_invariant)?.flatten() {
        let name = aa.file_name().to_string_lossy().into_owned();
        if name == ".partial" || name == ".DS_Store" {
            continue;
        }
        if !aa.path().is_dir() || !is_hex_pair(&name) {
            report.foreign_files.push(aa.path());
            continue;
        }
        for bb in fs::read_dir(aa.path()).map_err(io_invariant)?.flatten() {
            let bb_name = bb.file_name().to_string_lossy().into_owned();
            if bb_name == ".DS_Store" {
                continue;
            }
            if !bb.path().is_dir() || !is_hex_pair(&bb_name) {
                report.foreign_files.push(bb.path());
                continue;
            }
            for entry in fs::read_dir(bb.path()).map_err(io_invariant)?.flatten() {
                classify_blob_file(
                    &entry.path(),
                    (&name, &bb_name),
                    library,
                    &row_exts,
                    opts,
                    report,
                );
            }
        }
    }
    Ok(())
}

/// One file at fan-out depth: expected, ext-mismatch, orphan, or foreign.
fn classify_blob_file(
    path: &Path,
    fanout: (&str, &str),
    library: &LibraryConfig,
    row_exts: &HashMap<ContentHash, String>,
    opts: &FsckOptions,
    report: &mut FsckReport,
) {
    if !path.is_file() {
        report.foreign_files.push(path.to_path_buf());
        return;
    }
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    if name == ".DS_Store" {
        return;
    }
    let parsed = name.split_once('.').and_then(|(stem, ext)| {
        let hex = stem.len() == 64
            && stem
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase());
        // A misplaced file (fan-out dirs not matching its own hash) is
        // foreign: nothing will ever look it up at this path.
        (hex && !ext.is_empty() && (&stem[0..2], &stem[2..4]) == fanout)
            .then(|| (ContentHash::from_hex(stem), ext.to_string()))
    });
    let Some((hash, file_ext)) = parsed else {
        report.foreign_files.push(path.to_path_buf());
        return;
    };

    match row_exts.get(&hash) {
        Some(row_ext) if *row_ext == file_ext => {} // expected
        Some(row_ext) => report.ext_mismatches.push(ExtMismatch {
            library_id: library.library_id.clone(),
            content_hash: hash,
            row_ext: row_ext.clone(),
            file_ext,
            path: path.to_path_buf(),
        }),
        None => {
            if opts.repair && fs::remove_file(path).is_ok() {
                report.orphans_deleted += 1;
            }
            report.orphan_blobs.push(OrphanBlob {
                library_id: library.library_id.clone(),
                path: path.to_path_buf(),
            });
        }
    }
}

/// Local consistency (d) and remote existence/deep (e) checks for one
/// library's materialized photos.
async fn check_sidecars(
    store: &StateStore,
    data_dir: &DataDir,
    library: &LibraryConfig,
    opts: &FsckOptions,
    report: &mut FsckReport,
) -> Result<()> {
    let photos =
        crate::store::photos::materialized_photos(store.pool(), &library.library_id).await?;
    let local_root = data_dir.sidecar_root(&library.library_id);

    let remote_root = library.sidecar_root_remote.as_deref().map(Path::new);
    let remote_reachable = remote_root.is_some_and(Path::is_dir);
    // An unreachable remote is only worth reporting if there is something
    // to check there — a never-replicated library legitimately has no
    // remote tree yet. Pushed lazily on the first stamped photo.
    let mut remote_skip_noted = false;

    for photo in photos {
        let Some(local) = find_sidecar(&local_root, &photo.photo_id)? else {
            report.sidecar_findings.push(SidecarFinding {
                photo_id: photo.photo_id.clone(),
                library_id: library.library_id.clone(),
                kind: SidecarProblem::Missing,
            });
            continue;
        };

        let doc = match fs::read_to_string(&local)
            .map_err(|e| e.to_string())
            .and_then(|json| Sidecar::from_json(&json).map_err(|e| e.to_string()))
        {
            Ok(doc) => Some(doc),
            Err(error) => {
                report.sidecar_findings.push(SidecarFinding {
                    photo_id: photo.photo_id.clone(),
                    library_id: library.library_id.clone(),
                    kind: SidecarProblem::ParseError { error },
                });
                None
            }
        };
        if let Some(doc) = &doc {
            let fields = diff_sidecar_fields(store, &photo, library, doc).await?;
            if !fields.is_empty() {
                report.sidecar_findings.push(SidecarFinding {
                    photo_id: photo.photo_id.clone(),
                    library_id: library.library_id.clone(),
                    kind: SidecarProblem::Mismatch { fields },
                });
            }
        }

        // (e) remote: only for stamped photos with a reachable remote root.
        if photo.sidecar_replicated_at.is_none() {
            continue;
        }
        if !remote_reachable {
            if let Some(root) = remote_root
                && !remote_skip_noted
            {
                remote_skip_noted = true;
                report.skipped_roots.push(format!(
                    "{}: remote sidecar root unavailable ({})",
                    library.library_id,
                    root.display()
                ));
            }
            continue;
        }
        let rel = local
            .strip_prefix(&local_root)
            .map_err(|_| crate::IngressError::Invariant("sidecar outside its root".into()))?;
        let remote = remote_root.expect("reachable implies configured").join(rel);
        if !remote.is_file() {
            report.remote_findings.push(RemoteFinding {
                photo_id: photo.photo_id.clone(),
                library_id: library.library_id.clone(),
                kind: RemoteProblem::Missing { path: remote },
            });
        } else if opts.deep {
            let same = fs::read(&local)
                .and_then(|l| fs::read(&remote).map(|r| l == r))
                .unwrap_or(false);
            if !same {
                report.remote_findings.push(RemoteFinding {
                    photo_id: photo.photo_id.clone(),
                    library_id: library.library_id.clone(),
                    kind: RemoteProblem::Differs { path: remote },
                });
            }
        }
    }
    Ok(())
}

/// Db-backed fields only: identity, tombstone, timestamps, and the
/// written-resource set (order-insensitive). Capture metadata lives solely
/// in the sidecar and cannot be cross-checked.
async fn diff_sidecar_fields(
    store: &StateStore,
    photo: &crate::model::PhotoRecord,
    library: &LibraryConfig,
    doc: &Sidecar,
) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    if doc.photo_id != photo.photo_id {
        fields.push("photo_id".to_string());
    }
    if doc.library_id != library.library_id {
        fields.push("library_id".to_string());
    }
    if doc.cloud_id != photo.cloud_id {
        fields.push("cloud_id".to_string());
    }
    if doc.deleted_at != photo.deleted_at {
        fields.push("deleted_at".to_string());
    }
    if doc.ingested_at != photo.discovered_at {
        fields.push("ingested_at".to_string());
    }

    let mut expected: Vec<(String, String, String, i64)> = store
        .resources_for_photo(&photo.photo_id)
        .await?
        .into_iter()
        .filter(|r| r.written_at.is_some())
        .map(|r| {
            (
                r.resource_type.as_str().to_string(),
                r.content_hash.map(|h| h.to_string()).unwrap_or_default(),
                r.ext.unwrap_or_default(),
                r.size_bytes.unwrap_or_default(),
            )
        })
        .collect();
    let mut actual: Vec<(String, String, String, i64)> = doc
        .resources
        .iter()
        .map(|r| {
            (
                r.resource_type.clone(),
                r.content_hash.clone(),
                r.ext.clone(),
                r.size_bytes,
            )
        })
        .collect();
    expected.sort();
    actual.sort();
    if expected != actual {
        fields.push("resources".to_string());
    }
    Ok(fields)
}

fn is_hex_pair(s: &str) -> bool {
    s.len() == 2
        && s.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

fn io_invariant(e: std::io::Error) -> crate::IngressError {
    crate::IngressError::Invariant(format!("blob tree walk: {e}"))
}
