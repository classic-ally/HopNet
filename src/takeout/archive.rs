use flate2::{Compression, write::GzEncoder};
use std::fs::File;
use std::path::Path;
use tar::Builder;
use tracing;

/// File entry for archive creation
#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    /// Path to the file on disk (staging location)
    pub staging_path: String,
    /// Path to use inside the archive (user-facing path)
    pub archive_path: String,
    /// Whether this is a directory
    pub is_directory: bool,
}

/// Create a tar.gz archive from a list of file entries
/// Streams files into archive and optionally deletes source files
pub fn create_archive(
    entries: Vec<ArchiveEntry>,
    archive_path: &str,
    delete_source_files: bool,
) -> Result<u64, std::io::Error> {
    tracing::info!("Creating archive {} with {} entries", archive_path, entries.len());

    // Create the archive file and tar.gz encoder
    let archive_file = File::create(archive_path)?;
    let gz_encoder = GzEncoder::new(archive_file, Compression::default());
    let mut tar_builder = Builder::new(gz_encoder);

    let mut files_archived = 0u64;
    let mut total_size = 0u64;

    // Sort entries: directories first, then files, to ensure proper structure
    let mut sorted_entries = entries;
    sorted_entries.sort_by(|a, b| {
        match (a.is_directory, b.is_directory) {
            (true, false) => std::cmp::Ordering::Less,  // Directories first
            (false, true) => std::cmp::Ordering::Greater, // Files second
            _ => a.archive_path.cmp(&b.archive_path),    // Alphabetical within type
        }
    });

    // Process each entry
    for entry in sorted_entries {
        // Check if source exists
        if !Path::new(&entry.staging_path).exists() {
            tracing::warn!("Source path not found: {}, skipping", entry.staging_path);
            continue;
        }

        if entry.is_directory {
            // Add directory to archive
            if let Err(e) = tar_builder.append_dir(&entry.archive_path, &entry.staging_path) {
                tracing::error!("Failed to add directory {} to archive: {:?}", entry.staging_path, e);
                continue;
            }
            tracing::debug!("Added directory: {}", entry.archive_path);
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
            if let Err(e) = tar_builder.append_path_with_name(&entry.staging_path, &entry.archive_path) {
                tracing::error!("Failed to add file {} to archive: {:?}", entry.staging_path, e);
                continue;
            }

            files_archived += 1;
            total_size += file_size;

            tracing::debug!("Added file: {} ({} bytes)", entry.archive_path, file_size);
        }

        // Delete source file/directory if requested
        if delete_source_files {
            let delete_result = if entry.is_directory {
                std::fs::remove_dir_all(&entry.staging_path)
            } else {
                // Delete the file
                std::fs::remove_file(&entry.staging_path).and_then(|_| {
                    // Try to clean up empty parent directories
                    if let Some(parent) = Path::new(&entry.staging_path).parent() {
                        cleanup_empty_directories(parent);
                    }
                    Ok(())
                })
            };

            if let Err(e) = delete_result {
                tracing::warn!("Failed to delete source {}: {:?}", entry.staging_path, e);
                // Continue anyway - content is in archive
            }
        }

        if files_archived % 100 == 0 && files_archived > 0 {
            tracing::debug!("Archived {} files so far...", files_archived);
        }
    }

    // Finalize the tar.gz archive
    tar_builder.finish().map_err(|e| {
        tracing::error!("Failed to finalize archive: {:?}", e);
        e
    })?;

    tracing::info!("Archive creation completed: {} files archived, {} total bytes", files_archived, total_size);
    Ok(total_size)
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
        || path_str.ends_with("/staging") {
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

        let entries = vec![
            ArchiveEntry {
                staging_path: test_file.to_string_lossy().to_string(),
                archive_path: "test.txt".to_string(),
                is_directory: false,
            }
        ];

        let archive_path = temp_dir.path().join("test.tar.gz");
        let result = create_archive(entries, archive_path.to_str().unwrap(), true);

        assert!(result.is_ok());
        assert!(archive_path.exists());
        assert!(!test_file.exists()); // Should be deleted
    }
}