//! Fragment file I/O: content-addressed on-disk fragment store
//! (2-level hex nesting, atomic writes, verify-on-read).
//!
//! Moved verbatim from the main crate's files/functions.rs. All functions are
//! synchronous/blocking; async callers wrap them (the main crate keeps a
//! runtime-flavor-aware wrapper for reads on tokio worker threads).

use crate::error::StorageError;
use hopnet_common::Blake3Hash;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::OnceLock;

/// Set by the host at startup for a disposable node, so the fragment store
/// lands in the same throwaway tree as the rest of that node's state.
///
/// This lives here rather than in the host's path module because three
/// callers resolve the directory directly instead of reading it off
/// `AppState` (the storage job runner, twice, and the regenesis boot
/// reconcile). Routing them through one override is what stops an
/// "ephemeral" node writing blobs into a real user's data directory.
static DIR_OVERRIDE: OnceLock<PathBuf> = OnceLock::new();

/// Install the override. Idempotent-by-first-write; later calls are ignored.
pub fn set_dir_override(dir: PathBuf) {
    let _ = DIR_OVERRIDE.set(dir);
}

/// Where fragments live, in precedence order: the host's override, an
/// explicit `HOPNET_FRAGMENTS_DIR`, then `$XDG_DATA_HOME/hopnet/fragments`.
///
/// `HOPNET_FRAGMENTS_DIR` is deliberately separate from the database's
/// location: blobs are large and belong wherever the operator has room,
/// which is not necessarily where the metadata goes.
pub fn get_fragments_dir() -> Result<String, StorageError> {
    if let Some(dir) = DIR_OVERRIDE.get() {
        return Ok(dir.to_string_lossy().into_owned());
    }
    if let Some(dir) = std::env::var_os("HOPNET_FRAGMENTS_DIR") {
        return Ok(PathBuf::from(dir).to_string_lossy().into_owned());
    }

    let data_dir = std::env::var("XDG_DATA_HOME").unwrap_or_else(|_| {
        format!(
            "{}/.local/share",
            std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
        )
    });

    Ok(format!("{}/hopnet/fragments", data_dir))
}

/// Create 2-level directory structure for a fragment hash
/// e.g., "abcdef123..." -> "fragments/ab/cd/"
pub fn create_fragment_path(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<String, StorageError> {
    let hash_str = fragment_hash.to_hex();

    // Take first 4 hex characters for 2-level nesting
    if hash_str.len() < 4 {
        return Err(StorageError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Fragment hash too short",
        )));
    }

    let first_level = &hash_str[0..2];
    let second_level = &hash_str[2..4];

    let full_path = format!("{}/{}/{}", fragments_dir, first_level, second_level);

    Ok(full_path)
}

/// Store a fragment to disk using 2-level directory structure
pub fn store_fragment(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
    data: Vec<u8>,
) -> Result<(), StorageError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());

    // Create directory structure if it doesn't exist
    fs::create_dir_all(&dir_path)?;

    // Write to temp file then atomic rename to prevent concurrent readers
    // from seeing partial data (POSIX rename is atomic on the same filesystem)
    let temp_path = format!("{}.tmp.{:x}", full_file_path, rand::random::<u64>());
    fs::write(&temp_path, &data).map_err(|e| {
        // A partial write (ENOSPC, EIO) must not leave its temp file behind:
        // the sweep cannot attribute it to a row and would only reap it
        // after the orphan grace period.
        let _ = fs::remove_file(&temp_path);
        StorageError::Io(e)
    })?;
    fs::rename(&temp_path, &full_file_path).map_err(|e| {
        // Clean up temp file on rename failure
        let _ = fs::remove_file(&temp_path);
        StorageError::Io(e)
    })?;

    Ok(())
}

/// Delete a fragment from local storage
/// Simple deletion without directory cleanup for performance
pub fn delete_fragment(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<(), StorageError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());

    // Remove the fragment file
    match fs::remove_file(&full_file_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Fragment file doesn't exist - consider it successfully "deleted"
            Ok(())
        }
        Err(e) => Err(StorageError::Io(e)),
    }
}

/// Read a fragment from local storage — plain blocking read.
/// Async callers must wrap this appropriately for their runtime flavor.
pub fn read_fragment(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<Vec<u8>, StorageError> {
    let dir_path = create_fragment_path(fragments_dir, fragment_hash)?;
    let full_file_path = format!("{}/{}", dir_path, fragment_hash.to_hex());
    fs::read(&full_file_path).map_err(StorageError::Io)
}

/// Walk the 2-level fragment store and return every fragment file whose
/// mtime is older than `older_than_unix`, as (hash, size) pairs. The
/// hash-named flat files under `AB/CD/` are this store's own on-disk format
/// (see `create_fragment_path`). A missing root directory scans as empty.
pub fn scan_fragments(
    fragments_dir: &str,
    older_than_unix: u64,
) -> Result<Vec<(Blake3Hash, u64)>, StorageError> {
    Ok(scan_fragments_detailed(fragments_dir)?
        .into_iter()
        .filter(|d| d.mtime < older_than_unix)
        .map(|d| (d.hash, d.size))
        .collect())
}

/// The whole store, one file per entry with its mtime — the sweep's walk
/// (RFC-STORAGE-003 S5), shared by the flag diff (every file) and the
/// orphan grace (old files only). A missing root directory scans as empty.
pub fn scan_fragments_detailed(
    fragments_dir: &str,
) -> Result<Vec<crate::sweep::DiskFragment>, StorageError> {
    Ok(scan_fragments_with_temps(fragments_dir)?.fragments)
}

/// A `<hash>.tmp.<nonce>` file left by an interrupted `store_fragment`
/// (a crash between write and rename, or a partial write on an older
/// binary). Never a fragment: it is reaped once older than the orphan
/// grace period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TempFile {
    pub path: std::path::PathBuf,
    /// Modification time, unix seconds.
    pub mtime: u64,
}

/// The sweep's full view of the fragment directory: the fragments, the
/// temp files, and how many names were neither.
#[derive(Debug, Default)]
pub struct FragmentListing {
    pub fragments: Vec<crate::sweep::DiskFragment>,
    pub temps: Vec<TempFile>,
    /// Files whose name is neither a fragment hash nor a temp file — left
    /// alone, counted so the operator sees one line, not one per file.
    pub unexpected: usize,
}

/// Is this the name `store_fragment` gives its in-flight file?
fn is_temp_fragment_name(name: &str) -> bool {
    match name.split_once(".tmp.") {
        Some((hash, nonce)) => {
            hash.len() == 64
                && hash.bytes().all(|b| b.is_ascii_hexdigit())
                && !nonce.is_empty()
                && nonce.bytes().all(|b| b.is_ascii_hexdigit())
        }
        None => false,
    }
}

/// `scan_fragments_detailed` plus the temp files and the unexpected-name
/// count — a full walk of the store.
pub fn scan_fragments_with_temps(fragments_dir: &str) -> Result<FragmentListing, StorageError> {
    let fragments_path = std::path::Path::new(fragments_dir);
    if !fragments_path.exists() {
        tracing::warn!("Fragments directory does not exist: {}", fragments_dir);
        return Ok(FragmentListing::default());
    }

    let mut listing = FragmentListing::default();
    // Iterate through first-level directories (00-ff)
    for first_level_entry in fs::read_dir(fragments_path)? {
        let first_level_entry = first_level_entry?;
        if !first_level_entry.file_type()?.is_dir() {
            continue;
        }
        scan_first_level(&first_level_entry.path(), &mut listing)?;
    }
    if listing.unexpected > 0 {
        tracing::warn!(
            "fragment walk: {} files with names that are neither fragments nor temp files were left alone",
            listing.unexpected
        );
    }
    Ok(listing)
}

/// One shard of the store: the first-level directory named by the hash's
/// first byte (`get_fragment_dir`'s layout), the unit the rolling sweep
/// lists per step. A missing directory scans as empty; unexpected names
/// are counted, not logged, so the caller reports once per rotation.
pub fn scan_shard(fragments_dir: &str, shard: u8) -> Result<FragmentListing, StorageError> {
    let mut listing = FragmentListing::default();
    let path = std::path::Path::new(fragments_dir).join(format!("{shard:02x}"));
    if path.is_dir() {
        scan_first_level(&path, &mut listing)?;
    }
    Ok(listing)
}

/// The fragment hashes in one shard, from directory listings alone: no
/// per-file `stat` (the entry type comes from the listing on ext4, xfs,
/// btrfs and APFS), no content read. On a cold HDD the per-file stat of
/// [`scan_shard`] dominates; this costs one read per directory. Temp
/// files and unexpected names are skipped silently. A missing directory
/// lists as empty.
pub fn list_shard_hashes(fragments_dir: &str, shard: u8) -> Result<Vec<Blake3Hash>, StorageError> {
    let path = std::path::Path::new(fragments_dir).join(format!("{shard:02x}"));
    let mut hashes = Vec::new();
    let first_level = match fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(hashes),
        Err(e) => return Err(StorageError::Io(e)),
    };
    for second_level in first_level {
        let second_level = second_level?;
        if !second_level.file_type()?.is_dir() {
            continue;
        }
        for file in fs::read_dir(second_level.path())? {
            let file = file?;
            if !file.file_type()?.is_file() {
                continue;
            }
            let name = file.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.len() != 64 {
                continue;
            }
            let mut bytes = [0u8; 32];
            if hex::decode_to_slice(name, &mut bytes).is_ok() {
                hashes.push(Blake3Hash::from_bytes(bytes));
            }
        }
    }
    Ok(hashes)
}

/// Walk one first-level directory (its second-level directories and
/// their files) into `listing`.
fn scan_first_level(
    first_level: &std::path::Path,
    listing: &mut FragmentListing,
) -> Result<(), StorageError> {
    use std::time::SystemTime;

    // Iterate through second-level directories (00-ff)
    for second_level_entry in fs::read_dir(first_level)? {
        let second_level_entry = second_level_entry?;
        if !second_level_entry.file_type()?.is_dir() {
            continue;
        }

        // Iterate through fragment files
        for file_entry in fs::read_dir(second_level_entry.path())? {
            let file_entry = file_entry?;
            let metadata = file_entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }

            let mtime = metadata
                .modified()?
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| StorageError::Io(io::Error::other("Invalid file modification time")))?
                .as_secs();

            // Parse filename as Blake3 hash (64 hex characters)
            let filename = file_entry.file_name();
            let filename_str = filename.to_string_lossy();
            if is_temp_fragment_name(&filename_str) {
                listing.temps.push(TempFile {
                    path: file_entry.path(),
                    mtime,
                });
                continue;
            }
            if filename_str.len() != 64 {
                tracing::debug!("Unexpected fragment filename: {}", filename_str);
                listing.unexpected += 1;
                continue;
            }
            match hex::decode(&*filename_str) {
                Ok(bytes) if bytes.len() == 32 => {
                    let mut array = [0u8; 32];
                    array.copy_from_slice(&bytes);
                    listing.fragments.push(crate::sweep::DiskFragment {
                        hash: Blake3Hash::from_bytes(array),
                        size: metadata.len(),
                        mtime,
                    });
                }
                _ => {
                    tracing::debug!("Invalid fragment hash filename: {}", filename_str);
                    listing.unexpected += 1;
                }
            }
        }
    }
    Ok(())
}

/// Fetch and verify a fragment from local storage
/// Returns the fragment data if found locally and hash matches, otherwise returns an error
pub fn fetch_and_verify_fragment(
    fragment_hash: &Blake3Hash,
    fragments_dir: &str,
) -> Result<Vec<u8>, StorageError> {
    let chunk_data = read_fragment(fragments_dir, fragment_hash)?;

    // Verify chunk hash matches expected
    let actual_chunk_hash = Blake3Hash::new(blake3::hash(&chunk_data));
    if actual_chunk_hash != *fragment_hash {
        tracing::error!(
            "Fragment hash mismatch: expected {:?}, got {:?}",
            fragment_hash,
            actual_chunk_hash
        );
        return Err(StorageError::HashMismatch);
    }

    Ok(chunk_data)
}

/// Check if a fragment exists on disk and is valid (hash matches)
pub fn fragment_exists_and_valid(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    fetch_and_verify_fragment(fragment_hash, fragments_dir).is_ok()
}

/// What the scrub found over one slice. Only `corrupt` is a verdict on the
/// bytes: the file was read whole and its content does not hash to its
/// name. A file that vanished between the walk and the read (deleted by
/// the sweep's own orphan pass, eviction, or an orphaned-block apply) is
/// not corruption, and neither is a read the operating system refused —
/// EMFILE, EIO, a permission change — which says nothing about the bytes.
/// The live scrub of 2026-10-01 reported 5,525 "corrupt" fragments that
/// were all orphans the same sweep had just deleted.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ScrubOutcome {
    /// Content hash mismatches — the caller deletes these and the
    /// lifecycle re-pulls them from attested holders.
    pub corrupt: Vec<Blake3Hash>,
    /// Listed files that no longer existed when read.
    pub vanished: usize,
    /// Listed files the read failed on for any other reason (logged).
    pub unreadable: usize,
    /// Files whose content was read and hashed (verified or corrupt), and
    /// their listed bytes.
    pub files_read: usize,
    pub bytes_read: u64,
}

/// Deep-verify one slice of the local fragment store (rolling scrub,
/// RFC-STORAGE-001 scrub period): slices selected by the first hash byte,
/// so a full walk completes every `slices` calls.
pub fn verify_slice(
    fragments_dir: &str,
    slice: u8,
    slices: u8,
) -> Result<ScrubOutcome, StorageError> {
    let all = scan_fragments_detailed(fragments_dir)?;
    Ok(verify_listing(fragments_dir, &all, slice, slices))
}

/// The scrub over an existing walk (the sweep shares its listing): verify
/// the content of every file in `slice`.
pub fn verify_listing(
    fragments_dir: &str,
    listing: &[crate::sweep::DiskFragment],
    slice: u8,
    slices: u8,
) -> ScrubOutcome {
    let mut outcome = ScrubOutcome::default();
    for d in listing {
        if d.hash.as_bytes()[0] % slices.max(1) != slice {
            continue;
        }
        match fetch_and_verify_fragment(&d.hash, fragments_dir) {
            Ok(_) => {
                outcome.files_read += 1;
                outcome.bytes_read = outcome.bytes_read.saturating_add(d.size);
            }
            Err(StorageError::HashMismatch) => {
                outcome.files_read += 1;
                outcome.bytes_read = outcome.bytes_read.saturating_add(d.size);
                outcome.corrupt.push(d.hash);
            }
            Err(StorageError::Io(e)) | Err(StorageError::Read(e))
                if e.kind() == std::io::ErrorKind::NotFound =>
            {
                outcome.vanished += 1;
            }
            Err(e) => {
                tracing::warn!(
                    "scrub: could not read fragment {} ({e}); not a verdict on its bytes",
                    d.hash.to_hex()
                );
                outcome.unreadable += 1;
            }
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the full-disk node (2026-10-02) left a `.tmp.` file behind
    // every failed store, which the sweep warned about on every walk and
    // never deleted.
    // Should: remove the temp file when the write itself fails.
    // Should not: leave anything behind in the fragment's directory.
    #[test]
    fn store_failure_leaves_no_temp_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir =
            std::env::temp_dir().join(format!("hopnet-fragstore-nowrite-{}", std::process::id()));
        let dir = dir.to_str().unwrap().to_string();
        let _ = fs::remove_dir_all(&dir);
        let data = b"doomed".to_vec();
        let hash = Blake3Hash::new(blake3::hash(&data));
        let leaf = create_fragment_path(&dir, &hash).unwrap();
        fs::create_dir_all(&leaf).unwrap();
        fs::set_permissions(&leaf, fs::Permissions::from_mode(0o555)).unwrap();

        let result = store_fragment(&dir, &hash, data);
        fs::set_permissions(&leaf, fs::Permissions::from_mode(0o755)).unwrap();
        if result.is_ok() {
            // Running as a user the mode bits do not bind (root): nothing
            // to assert about a failure that did not happen.
            let _ = fs::remove_dir_all(&dir);
            return;
        }
        let leftovers: Vec<_> = fs::read_dir(&leaf).unwrap().collect();
        assert!(leftovers.is_empty(), "temp file left behind: {leftovers:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    // Should: list a stored fragment under fragments, a `<hash>.tmp.<nonce>`
    // file under temps with its mtime, and count any other name as
    // unexpected without listing it anywhere.
    #[test]
    fn listing_separates_temp_files_from_fragments() {
        let dir =
            std::env::temp_dir().join(format!("hopnet-fragstore-listing-{}", std::process::id()));
        let dir = dir.to_str().unwrap().to_string();
        let _ = fs::remove_dir_all(&dir);
        let data = b"kept".to_vec();
        let hash = Blake3Hash::new(blake3::hash(&data));
        store_fragment(&dir, &hash, data).unwrap();
        let leaf = create_fragment_path(&dir, &hash).unwrap();
        let temp = format!("{leaf}/{}.tmp.deadbeef", hash.to_hex());
        fs::write(&temp, b"partial").unwrap();
        fs::write(format!("{leaf}/notes.txt"), b"junk").unwrap();

        let listing = scan_fragments_with_temps(&dir).unwrap();
        assert_eq!(listing.fragments.len(), 1);
        assert_eq!(listing.fragments[0].hash, hash);
        assert_eq!(listing.temps.len(), 1);
        assert_eq!(listing.temps[0].path, std::path::PathBuf::from(&temp));
        assert!(listing.temps[0].mtime > 0);
        assert_eq!(listing.unexpected, 1);
        // The plain listing still sees only the fragment.
        assert_eq!(scan_fragments_detailed(&dir).unwrap().len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    // Should: list only the fragments and temp files under the shard's
    // first-level directory, and scan a shard with no directory as empty.
    #[test]
    fn shard_scan_lists_only_its_prefix() {
        let dir =
            std::env::temp_dir().join(format!("hopnet-fragstore-shard-{}", std::process::id()));
        let dir = dir.to_str().unwrap().to_string();
        let _ = fs::remove_dir_all(&dir);
        let mut hashes = Vec::new();
        for i in 0u32..64 {
            let data = i.to_le_bytes().to_vec();
            let hash = Blake3Hash::new(blake3::hash(&data));
            store_fragment(&dir, &hash, data).unwrap();
            hashes.push(hash);
        }
        let shard = hashes[0].as_bytes()[0];
        let leaf = create_fragment_path(&dir, &hashes[0]).unwrap();
        fs::write(format!("{leaf}/{}.tmp.ab", hashes[0].to_hex()), b"x").unwrap();

        let listing = scan_shard(&dir, shard).unwrap();
        let mut expected: Vec<_> = hashes
            .iter()
            .filter(|h| h.as_bytes()[0] == shard)
            .copied()
            .collect();
        let mut listed: Vec<_> = listing.fragments.iter().map(|d| d.hash).collect();
        expected.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        listed.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(listed, expected);
        assert_eq!(listing.temps.len(), 1);

        let empty = (0..=u8::MAX)
            .find(|b| hashes.iter().all(|h| h.as_bytes()[0] != *b))
            .unwrap();
        let none = scan_shard(&dir, empty).unwrap();
        assert!(none.fragments.is_empty() && none.temps.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    // Should: name exactly the fragments under the shard's prefix, the
    // same set the full shard scan lists.
    // Should not: name temp files or unexpected names, or fail on a
    // shard with no directory.
    #[test]
    fn shard_hash_listing_names_only_fragments() {
        let dir = std::env::temp_dir().join(format!(
            "hopnet-fragstore-shard-names-{}",
            std::process::id()
        ));
        let dir = dir.to_str().unwrap().to_string();
        let _ = fs::remove_dir_all(&dir);
        let mut hashes = Vec::new();
        for i in 0u32..64 {
            let data = i.to_le_bytes().to_vec();
            let hash = Blake3Hash::new(blake3::hash(&data));
            store_fragment(&dir, &hash, data).unwrap();
            hashes.push(hash);
        }
        let shard = hashes[0].as_bytes()[0];
        let leaf = create_fragment_path(&dir, &hashes[0]).unwrap();
        fs::write(format!("{leaf}/{}.tmp.ab", hashes[0].to_hex()), b"x").unwrap();
        fs::write(format!("{leaf}/notes.txt"), b"junk").unwrap();

        let mut listed = list_shard_hashes(&dir, shard).unwrap();
        let mut scanned: Vec<_> = scan_shard(&dir, shard)
            .unwrap()
            .fragments
            .iter()
            .map(|d| d.hash)
            .collect();
        listed.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        scanned.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert!(!listed.is_empty());
        assert_eq!(listed, scanned);

        let empty = (0..=u8::MAX)
            .find(|b| hashes.iter().all(|h| h.as_bytes()[0] != *b))
            .unwrap();
        assert!(list_shard_hashes(&dir, empty).unwrap().is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn store_read_verify_delete_cycle() {
        // Should: store → verify-read round-trips; corruption reads as HashMismatch;
        // delete is idempotent.
        // Impact: the fragment store is the data plane's disk truth.
        let dir =
            std::env::temp_dir().join(format!("hopnet-fragstore-test-{}", std::process::id()));
        let dir = dir.to_str().unwrap().to_string();

        let data = b"fragment bytes".to_vec();
        let hash = Blake3Hash::new(blake3::hash(&data));

        store_fragment(&dir, &hash, data.clone()).unwrap();
        assert!(fragment_exists_and_valid(&dir, &hash));
        assert_eq!(fetch_and_verify_fragment(&hash, &dir).unwrap(), data);

        // Corrupt in place → verify must fail
        let path = format!(
            "{}/{}",
            create_fragment_path(&dir, &hash).unwrap(),
            hash.to_hex()
        );
        fs::write(&path, b"corrupted").unwrap();
        assert!(matches!(
            fetch_and_verify_fragment(&hash, &dir),
            Err(StorageError::HashMismatch)
        ));

        delete_fragment(&dir, &hash).unwrap();
        delete_fragment(&dir, &hash).unwrap(); // idempotent
        assert!(!fragment_exists_and_valid(&dir, &hash));

        let _ = fs::remove_dir_all(&dir);
    }

    // Impact: the 2026-10-01 live scrub reported 5,525 corrupt fragments
    // that were orphans the same sweep had deleted moments earlier — a
    // vanished file read back as an error and the error read as corruption.
    // Should: report only content hash mismatches as corrupt, and count a
    // file that disappeared after the walk as vanished.
    // Should not: report an intact or a vanished file as corrupt.
    #[test]
    fn scrub_reports_only_hash_mismatches() {
        let dir = std::env::temp_dir().join(format!("hopnet-scrub-test-{}", std::process::id()));
        let dir = dir.to_str().unwrap().to_string();
        let _ = fs::remove_dir_all(&dir);

        let mut listing = Vec::new();
        let mut hashes = Vec::new();
        for i in 0u8..3 {
            let data = vec![i; 64];
            let hash = Blake3Hash::new(blake3::hash(&data));
            store_fragment(&dir, &hash, data).unwrap();
            listing.push(crate::sweep::DiskFragment {
                hash,
                size: 64,
                mtime: 0,
            });
            hashes.push(hash);
        }
        // One corrupted in place, one deleted after the walk, one intact.
        let corrupt_path = format!(
            "{}/{}",
            create_fragment_path(&dir, &hashes[1]).unwrap(),
            hashes[1].to_hex()
        );
        fs::write(&corrupt_path, b"corrupted").unwrap();
        delete_fragment(&dir, &hashes[2]).unwrap();

        // One slice covering every hash.
        let outcome = verify_listing(&dir, &listing, 0, 1);
        assert_eq!(outcome.corrupt, vec![hashes[1]]);
        assert_eq!(outcome.vanished, 1);
        assert_eq!(outcome.unreadable, 0);

        let _ = fs::remove_dir_all(&dir);
    }
}
