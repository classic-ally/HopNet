//! Archive assembly (export) and manifest read-back (import). Moved from the
//! host's `takeout::archive` at RFC-015 Stage D5b; the only reshape is the
//! v2 layout — entries live under `{projection}/<logical_path>` (callers now
//! pass the fully projection-prefixed archive path) instead of a global
//! `files/` prefix, and the version gate accepts exactly v2.

use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tar::{Archive, Builder};
use tracing;

use crate::manifest::{TakeoutManifest, MANIFEST_FILENAME, MANIFEST_VERSION};

/// Errors raised while reading an import-side archive (read counterpart to
/// `create_archive`'s write side). Each variant maps cleanly to a route-level
/// HTTP status (400 Bad Request for content-shape errors, 500 for I/O).
#[derive(Debug, thiserror::Error)]
pub enum ImportArchiveError {
    #[error("I/O error reading staging archive: {0}")]
    Io(#[from] std::io::Error),
    #[error("archive contained no entries")]
    EmptyArchive,
    #[error("first entry was {found:?}, expected {expected:?}")]
    WrongFirstEntry { found: String, expected: String },
    #[error("manifest JSON parse failed: {0}")]
    JsonParse(#[from] serde_json::Error),
    #[error("manifest version {found} not supported (require {supported})")]
    UnknownVersion { found: u32, supported: u32 },
}

/// File entry for archive creation
#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    /// Path to the file on disk (staging location)
    pub staging_path: String,
    /// Path to use inside the archive: `{projection}/<logical_path>`.
    /// Callers build the projection prefix — `create_archive` writes it
    /// verbatim (v2 has no global prefix).
    pub archive_path: String,
    /// Whether this is a directory
    pub is_directory: bool,
}

/// Create a tar.gz archive from a list of file entries.
///
/// `manifest_bytes` is written as the first tar entry at `manifest.json`.
/// Every subsequent entry is written at its (projection-prefixed) archive
/// path. Streams files into archive and optionally deletes source files.
pub fn create_archive(
    manifest_bytes: &[u8],
    entries: Vec<ArchiveEntry>,
    archive_path: &str,
    delete_source_files: bool,
) -> Result<u64, std::io::Error> {
    tracing::info!(
        "Creating archive {} with manifest ({} bytes) and {} entries",
        archive_path,
        manifest_bytes.len(),
        entries.len()
    );

    // Create the archive file and tar.gz encoder
    let archive_file = File::create(archive_path)?;
    let gz_encoder = GzEncoder::new(archive_file, Compression::default());
    let mut tar_builder = Builder::new(gz_encoder);
    // Portability: never emit GNU sparse entries. The builder's default
    // hole detection turns an all-zero staged file (stored sparse by the
    // filesystem) into a typeflag-'S' entry, which naive tar readers
    // (busybox; anything filtering on EntryType::is_file) drop or choke
    // on. Takeout archives are the user-facing backup wire format — plain
    // Regular entries only.
    tar_builder.sparse(false);

    // Write manifest as the first tar entry. Must be first so import can
    // parse it from a streaming reader without buffering the full archive.
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut manifest_header = tar::Header::new_gnu();
    manifest_header.set_size(manifest_bytes.len() as u64);
    manifest_header.set_mode(0o644);
    manifest_header.set_mtime(now_unix);
    manifest_header.set_entry_type(tar::EntryType::Regular);
    manifest_header.set_cksum();
    tar_builder.append_data(&mut manifest_header, MANIFEST_FILENAME, manifest_bytes)?;

    let mut files_archived = 0u64;
    let mut total_size = 0u64;

    // Sort entries: directories first, then files, to ensure proper structure
    let mut sorted_entries = entries;
    sorted_entries.sort_by(|a, b| {
        match (a.is_directory, b.is_directory) {
            (true, false) => std::cmp::Ordering::Less, // Directories first
            (false, true) => std::cmp::Ordering::Greater, // Files second
            _ => a.archive_path.cmp(&b.archive_path),  // Alphabetical within type
        }
    });

    // Process each entry
    for entry in sorted_entries {
        // Check if source exists
        if !Path::new(&entry.staging_path).exists() {
            tracing::warn!("Source path not found: {}, skipping", entry.staging_path);
            continue;
        }

        let prefixed_archive_path = &entry.archive_path;

        if entry.is_directory {
            // Add directory to archive
            if let Err(e) = tar_builder.append_dir(prefixed_archive_path, &entry.staging_path) {
                tracing::error!(
                    "Failed to add directory {} to archive: {:?}",
                    entry.staging_path,
                    e
                );
                continue;
            }
            tracing::debug!("Added directory: {}", prefixed_archive_path);
        } else {
            // Get file size before adding to archive
            let file_size = match std::fs::metadata(&entry.staging_path) {
                Ok(metadata) => metadata.len(),
                Err(e) => {
                    tracing::error!("Failed to get metadata for {}: {:?}", entry.staging_path, e);
                    continue;
                }
            };

            // Add file to tar.gz
            if let Err(e) =
                tar_builder.append_path_with_name(&entry.staging_path, prefixed_archive_path)
            {
                tracing::error!(
                    "Failed to add file {} to archive: {:?}",
                    entry.staging_path,
                    e
                );
                continue;
            }

            files_archived += 1;
            total_size += file_size;

            tracing::debug!(
                "Added file: {} ({} bytes)",
                prefixed_archive_path,
                file_size
            );
        }

        // Delete source file/directory if requested
        if delete_source_files {
            let delete_result = if entry.is_directory {
                std::fs::remove_dir_all(&entry.staging_path)
            } else {
                // Delete the file
                std::fs::remove_file(&entry.staging_path).map(|_| {
                    // Try to clean up empty parent directories
                    if let Some(parent) = Path::new(&entry.staging_path).parent() {
                        cleanup_empty_directories(parent);
                    }
                })
            };

            if let Err(e) = delete_result {
                tracing::warn!("Failed to delete source {}: {:?}", entry.staging_path, e);
                // Continue anyway - content is in archive
            }
        }

        if files_archived > 0 && files_archived.is_multiple_of(100) {
            tracing::debug!("Archived {} files so far...", files_archived);
        }
    }

    // Finalize the tar.gz archive
    tar_builder.finish().map_err(|e| {
        tracing::error!("Failed to finalize archive: {:?}", e);
        e
    })?;

    tracing::info!(
        "Archive creation completed: {} files archived, {} total bytes",
        files_archived,
        total_size
    );
    Ok(total_size)
}

/// Open a staging tar.gz, pull the first entry expecting `manifest.json`,
/// parse it, and validate the schema version. Stops after the first entry —
/// remaining tar payload stays on disk for extraction.
///
/// v2 gate: `version != MANIFEST_VERSION` → `UnknownVersion` (route maps it
/// to the same 400 the v1 check gave). v1 manifests parse (the sections map
/// defaults empty) and are rejected here on version, not on JSON shape.
///
/// Sync I/O — callers in async context should wrap in
/// `tokio::task::spawn_blocking`.
pub fn read_manifest_from_archive(
    archive_path: &Path,
) -> Result<TakeoutManifest, ImportArchiveError> {
    let file = File::open(archive_path)?;
    let gz = GzDecoder::new(file);
    let mut archive = Archive::new(gz);

    let mut entries = archive.entries()?;
    let mut first_entry = entries.next().ok_or(ImportArchiveError::EmptyArchive)??;

    let path_buf = first_entry.path()?.to_path_buf();
    let path_str = path_buf.to_string_lossy();
    if path_str != MANIFEST_FILENAME {
        return Err(ImportArchiveError::WrongFirstEntry {
            found: path_str.into_owned(),
            expected: MANIFEST_FILENAME.to_string(),
        });
    }

    let mut buf = Vec::new();
    first_entry.read_to_end(&mut buf)?;

    let manifest: TakeoutManifest = serde_json::from_slice(&buf)?;
    if manifest.version != MANIFEST_VERSION {
        return Err(ImportArchiveError::UnknownVersion {
            found: manifest.version,
            supported: MANIFEST_VERSION,
        });
    }

    Ok(manifest)
}

/// Recursively remove empty directories walking up the path
/// Stops when it encounters a non-empty directory or reaches safety boundaries
fn cleanup_empty_directories(dir_path: &Path) {
    let path_str = dir_path.to_string_lossy();

    // Safety boundaries - don't delete these or anything above them
    if !path_str.contains("/takeouts/")
        || path_str.ends_with("/takeouts")
        || path_str.ends_with("/staging")
    {
        return;
    }

    // Additional safety: ensure we're within a specific takeout's staging area
    if !path_str.contains("/staging/") {
        return;
    }

    // Try to remove the directory (will fail if not empty)
    if let Ok(()) = std::fs::remove_dir(dir_path) {
        tracing::debug!("Cleaned up empty directory: {:?}", dir_path);

        // Recursively try to clean up parent directory
        if let Some(parent) = dir_path.parent() {
            cleanup_empty_directories(parent);
        }
    }
    // If removal fails (directory not empty or other error), stop recursing
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    /// Should: write the manifest first, place a file at its
    /// projection-prefixed archive path, and delete the source when asked.
    /// Should not: leave the staged source file behind.
    /// Impact: the archive layout IS the portability wire format — a drifted
    /// path breaks every importer.
    #[test]
    fn test_create_archive() {
        let temp_dir = TempDir::new().unwrap();
        let staging_dir = temp_dir.path().join("staging");
        fs::create_dir_all(&staging_dir).unwrap();

        // Create test file
        let test_file = staging_dir.join("test.txt");
        let mut file = File::create(&test_file).unwrap();
        file.write_all(b"hello world").unwrap();

        let entries = vec![ArchiveEntry {
            staging_path: test_file.to_string_lossy().to_string(),
            archive_path: "drive/test.txt".to_string(),
            is_directory: false,
        }];

        let archive_path = temp_dir.path().join("test.tar.gz");
        let manifest_bytes = br#"{"version":2}"#;
        let result = create_archive(
            manifest_bytes,
            entries,
            archive_path.to_str().unwrap(),
            true,
        );

        assert!(result.is_ok());
        assert!(archive_path.exists());
        assert!(!test_file.exists()); // Should be deleted

        // Round-trip: first entry is the manifest, second is drive/test.txt.
        let gz = GzDecoder::new(File::open(&archive_path).unwrap());
        let mut ar = Archive::new(gz);
        let paths: Vec<String> = ar
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(paths, vec!["manifest.json", "drive/test.txt"]);
    }

    /// Should: write an all-zero, filesystem-sparse staged file as a plain
    /// Regular tar entry with its full content.
    /// Should not: emit a GNU sparse (typeflag 'S') entry.
    /// Impact: regression guard — the builder's default hole detection made
    /// the first all-zero file a sparse entry, which busybox tar rejects
    /// and EntryType::is_file() filters silently drop, so the file vanished
    /// from downloaded takeout archives (issue #28).
    #[test]
    fn all_zero_sparse_file_archives_as_regular_entry() {
        let temp_dir = TempDir::new().unwrap();
        let staging_dir = temp_dir.path().join("staging");
        fs::create_dir_all(&staging_dir).unwrap();

        // set_len without writing guarantees a hole on any fs that
        // supports sparse files — the strongest form of the trigger.
        let zero_file = staging_dir.join("zeros.bin");
        let file = File::create(&zero_file).unwrap();
        file.set_len(4096).unwrap();
        drop(file);

        let entries = vec![ArchiveEntry {
            staging_path: zero_file.to_string_lossy().to_string(),
            archive_path: "drive/zeros.bin".to_string(),
            is_directory: false,
        }];

        let archive_path = temp_dir.path().join("zeros.tar.gz");
        create_archive(
            br#"{"version":2}"#,
            entries,
            archive_path.to_str().unwrap(),
            false,
        )
        .unwrap();

        let gz = GzDecoder::new(File::open(&archive_path).unwrap());
        let mut ar = Archive::new(gz);
        let mut saw_zero_file = false;
        for entry in ar.entries().unwrap() {
            let mut entry = entry.unwrap();
            assert!(
                entry.header().entry_type().is_file(),
                "non-Regular entry {:?} ({:?}) in takeout archive",
                entry.path().unwrap(),
                entry.header().entry_type()
            );
            if entry.path().unwrap().to_string_lossy() == "drive/zeros.bin" {
                let mut buf = Vec::new();
                entry.read_to_end(&mut buf).unwrap();
                assert_eq!(buf, vec![0u8; 4096]);
                saw_zero_file = true;
            }
        }
        assert!(saw_zero_file);
    }

    /// Should: reject a manifest whose version differs from v2 (both the
    /// dead v1 and any future v3) with `UnknownVersion`.
    /// Should not: choke on v1's missing `projections` field before the
    /// version gate fires.
    /// Impact: the version gate is the compat contract for imports.
    #[test]
    fn version_gate_rejects_non_v2() {
        let temp_dir = TempDir::new().unwrap();
        for version in [1u32, 3] {
            let manifest_bytes = format!(
                r#"{{"version":{},"takeout_id":"019807b4-1111-7111-8111-111111111111","created_at":"2026-01-01T00:00:00Z","source_username":"t"}}"#,
                version
            );
            let archive_path = temp_dir.path().join(format!("v{}.tar.gz", version));
            create_archive(
                manifest_bytes.as_bytes(),
                vec![],
                archive_path.to_str().unwrap(),
                false,
            )
            .unwrap();
            match read_manifest_from_archive(&archive_path) {
                Err(ImportArchiveError::UnknownVersion { found, supported }) => {
                    assert_eq!(found, version);
                    assert_eq!(supported, MANIFEST_VERSION);
                }
                other => panic!("expected UnknownVersion for v{version}, got {other:?}"),
            }
        }
    }

    /// Impact: this is the silent-total-data-loss bug. A real 592-file, 3.5GB
    /// takeout produced a 53KB archive holding 3 entries, reported Ready, and
    /// carried a manifest still promising 3,509,810,146 bytes. Every operator
    /// signal said success. Restoring it would have yielded almost nothing —
    /// and it is the mechanism by which decommissioning a source NAS on the
    /// strength of a "successful" takeout loses everything.
    /// Should: archive a file nested beneath a directory entry.
    /// Should not: let a directory's post-append cleanup remove descendants
    /// that have not been archived yet.
    #[test]
    fn nested_entries_survive_directory_source_deletion() {
        let temp_dir = TempDir::new().unwrap();
        let staging = temp_dir.path().join("staging");
        let nested = staging.join("drive/Documents/High School");
        fs::create_dir_all(&nested).unwrap();

        let nested_file = nested.join("essay.txt");
        File::create(&nested_file)
            .unwrap()
            .write_all(b"nested content")
            .unwrap();

        // Entry order as export.rs builds it: every folder row and every file
        // row, unsorted — create_archive does its own directories-first sort.
        let entries = vec![
            ArchiveEntry {
                staging_path: staging.join("drive/Documents").to_string_lossy().into(),
                archive_path: "drive/Documents".to_string(),
                is_directory: true,
            },
            ArchiveEntry {
                staging_path: nested.to_string_lossy().into(),
                archive_path: "drive/Documents/High School".to_string(),
                is_directory: true,
            },
            ArchiveEntry {
                staging_path: nested_file.to_string_lossy().into(),
                archive_path: "drive/Documents/High School/essay.txt".to_string(),
                is_directory: false,
            },
        ];

        let archive_path = temp_dir.path().join("out.tar.gz");
        create_archive(
            br#"{"version":2}"#,
            entries,
            archive_path.to_str().unwrap(),
            true, // delete_source_files — what export.rs passes in production
        )
        .unwrap();

        let gz = GzDecoder::new(File::open(&archive_path).unwrap());
        let mut ar = Archive::new(gz);
        let paths: Vec<String> = ar
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();

        assert!(
            paths.contains(&"drive/Documents/High School/essay.txt".to_string()),
            "nested file missing from archive; got {paths:?}"
        );
    }
}
