//! Tier-2 on-demand invariant audit (spec §Recovery Tier 2): refcount
//! drift, missing blob files (byte loss — loud, never repairable), and
//! orphan blob files (deleted only under `--repair` — the one destructive
//! repair).
//!
//! The default run is READ-ONLY: it works on a read-only store, takes no
//! lock, and logs nothing. `--repair` requires a writable store; it takes
//! the exclusive run lock, applies the refcount repair, and deletes
//! exact-match orphan files.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::ids::{ContentHash, LibraryId};

use crate::paths::DataDir;
use crate::recovery::{RefcountDrift, recount_diff, repair_refcounts};
use crate::store::StateStore;

#[derive(Debug, Clone, Copy, Default)]
pub struct FsckOptions {
    /// Apply repairs: refcount drift and orphan-file deletion. Requires a
    /// writable store; takes the exclusive run lock.
    pub repair: bool,
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

    check_blob_tree(store, &data_dir.spool(), opts, &mut report).await?;

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

/// Blob-existence (b) and orphan-scan (c) checks over the process-global
/// spool. The spool is hash-addressed: one file can back rows in several
/// libraries, so a hash is "live" (bytes expected on disk) iff ANY
/// library's row for it is unevicted.
async fn check_blob_tree(
    store: &StateStore,
    spool: &crate::paths::SpoolPaths,
    opts: &FsckOptions,
    report: &mut FsckReport,
) -> Result<()> {
    // Live hashes across every library. Spool-evicted rows are excluded —
    // their bytes are SUPPOSED to be gone (HopNet holds them) — so a
    // lingering file for an all-evicted hash classifies as a benign orphan
    // in the walk below.
    let mut row_exts: HashMap<ContentHash, (String, LibraryId)> = HashMap::new();
    for library in store.libraries().await? {
        for row in store.blobs_for_library(&library.library_id).await? {
            if row.evicted_at.is_none() {
                row_exts.insert(row.content_hash, (row.ext, library.library_id.clone()));
            }
        }
    }

    // (b) every live hash's file must exist. The spool is a local
    // directory — no mount-down class.
    for (hash, (ext, library_id)) in &row_exts {
        let expected = spool.blob_path(hash, ext);
        if !expected.is_file() {
            report.missing_blobs.push(MissingBlob {
                library_id: library_id.clone(),
                content_hash: hash.clone(),
                ext: ext.clone(),
                expected_path: expected,
            });
        }
    }

    // (c) walk <spool>/blobs/ two fan-out levels, skipping .partial and
    // Finder's .DS_Store droppings.
    let blobs_dir = spool.blobs_dir();
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
                classify_blob_file(&entry.path(), (&name, &bb_name), &row_exts, opts, report);
            }
        }
    }
    Ok(())
}

/// One file at fan-out depth: expected, ext-mismatch, orphan, or foreign.
fn classify_blob_file(
    path: &Path,
    fanout: (&str, &str),
    row_exts: &HashMap<ContentHash, (String, LibraryId)>,
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
        Some((row_ext, _)) if *row_ext == file_ext => {} // expected
        Some((row_ext, library_id)) => report.ext_mismatches.push(ExtMismatch {
            library_id: library_id.clone(),
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
                path: path.to_path_buf(),
            });
        }
    }
}


fn is_hex_pair(s: &str) -> bool {
    s.len() == 2
        && s.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

fn io_invariant(e: std::io::Error) -> crate::IngressError {
    crate::IngressError::Invariant(format!("blob tree walk: {e}"))
}
