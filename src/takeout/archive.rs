use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tar::{Archive, Builder};
use tracing;

use crate::takeout::manifest::{
    ARCHIVE_FILES_PREFIX, MANIFEST_FILENAME, MANIFEST_VERSION, TakeoutManifest,
};

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
    #[error("manifest version {found} not supported (max {supported})")]
    UnknownVersion { found: u32, supported: u32 },
}

/// File entry for archive creation
#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    /// Path to the file on disk (staging location)
    pub staging_path: String,
    /// Path to use inside the archive (user-facing path).
    /// `create_archive` applies the `ARCHIVE_FILES_PREFIX` wrapper before
    /// writing to tar, so this should be the raw user path (no `files/` prefix).
    pub archive_path: String,
    /// Whether this is a directory
    pub is_directory: bool,
}

/// Create a tar.gz archive from a list of file entries.
///
/// `manifest_bytes` is written as the first tar entry at `manifest.json`.
/// Every subsequent entry is placed under the `files/` prefix so the manifest
/// and content are cleanly separated (see spec § Manifest Format).
/// Streams files into archive and optionally deletes source files.
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

        // Wrap user-facing archive paths under the `files/` prefix.
        let prefixed_archive_path = format!("{}{}", ARCHIVE_FILES_PREFIX, entry.archive_path);

        if entry.is_directory {
            // Add directory to archive
            if let Err(e) = tar_builder.append_dir(&prefixed_archive_path, &entry.staging_path) {
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
                tar_builder.append_path_with_name(&entry.staging_path, &prefixed_archive_path)
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

        if files_archived.is_multiple_of(100) && files_archived > 0 {
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
/// remaining tar payload stays on disk for Phase 3.4 extraction.
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
    if manifest.version > MANIFEST_VERSION {
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
        || path_str.ends_with("/staging/files")
        || path_str.ends_with("/staging/folders")
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
            archive_path: "test.txt".to_string(),
            is_directory: false,
        }];

        let archive_path = temp_dir.path().join("test.tar.gz");
        let manifest_bytes = br#"{"version":1}"#;
        let result = create_archive(
            manifest_bytes,
            entries,
            archive_path.to_str().unwrap(),
            true,
        );

        assert!(result.is_ok());
        assert!(archive_path.exists());
        assert!(!test_file.exists()); // Should be deleted
    }
}
