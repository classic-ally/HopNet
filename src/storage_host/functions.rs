use crate::AppState;
use crate::db::CustomUUID;
use crate::types::Blake3Hash;
use std::fs;
use std::io;

/// Drive-owned (RFC-015): the file error taxonomy and deterministic path
/// crypto (AES-SIV) live in hopnet-drive; re-exported here so call sites
/// don't churn.
pub use hopnet_drive::error::FileError;
pub use hopnet_drive::paths::{
    build_encrypted_path, decrypt_part, decrypt_path, encrypt_part, encrypt_path, generate_siv_key,
    generate_siv_nonce,
};

// Chunk/padding math and format constants live in the substrate crate
// (hopnet-storage, RFC-014); re-exported here so call sites don't churn.
pub use hopnet_storage::rs::{
    CHUNK_SIZE, MAX_FRAGMENT_SIZE, ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK,
    TOTAL_FRAGMENTS_PER_CHUNK, calculate_chunk_padding, calculate_chunked_fragments,
    calculate_optimal_chunks, calculate_padding_and_chunks,
};

/// Fragment file I/O lives in the substrate crate (hopnet-storage::fragstore);
/// thin delegations here keep call sites and the FileError taxonomy stable.
pub fn get_fragments_dir() -> Result<String, FileError> {
    hopnet_storage::fragstore::get_fragments_dir().map_err(FileError::from)
}

pub fn create_fragment_path(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<String, FileError> {
    hopnet_storage::fragstore::create_fragment_path(fragments_dir, fragment_hash)
        .map_err(FileError::from)
}

/// Boot guard: refuse to run when the database says this node holds fragments
/// but the fragment store is missing or empty.
///
/// The database and the fragment store can live on different filesystems
/// (`HOPNET_DATA_DIR` / `HOPNET_FRAGMENTS_DIR`), and the quiet failure is a
/// node starting before the bulk filesystem is mounted: it would create the
/// directory on whatever is underneath, write fragments to the wrong disk, and
/// have them vanish beneath the real mount later. Newly decided blobs would
/// also apply with `stored_locally = 0`, so the node under-reports from then on.
///
/// Deliberately a hard refusal rather than a warning: under the systemd unit's
/// `Restart = on-failure` it becomes a visible restart loop that heals itself
/// the moment the filesystem appears. A fresh node claims nothing and never
/// trips.
pub fn check_fragment_store_present(
    conn: &rusqlite::Connection,
    fragments_dir: &str,
    data_dir: &std::path::Path,
) -> Result<(), String> {
    let claimed: i64 = match conn.query_row(
        "SELECT COUNT(*) FROM fragment_hashes WHERE stored_locally = 1",
        [],
        |row| row.get(0),
    ) {
        Ok(n) => n,
        // No table yet (pre-schema) is not a lost store. Fail open: a real
        // database fault will surface loudly elsewhere.
        Err(e) => {
            tracing::debug!("fragment store guard: inventory unreadable ({e}); skipping");
            return Ok(());
        }
    };
    if claimed == 0 {
        return Ok(());
    }

    let populated = fs::read_dir(fragments_dir)
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false);
    if populated {
        return Ok(());
    }

    Err(format!(
        "database claims {claimed} locally-stored fragments but the fragment \
         store at {fragments_dir} is missing or empty (database is at {}). \
         Refusing to start: continuing would write fragments to the wrong \
         filesystem. Check that the fragment store is mounted, or that \
         HOPNET_FRAGMENTS_DIR points where the fragments actually are.",
        data_dir.display()
    ))
}

pub fn store_fragment(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
    data: Vec<u8>,
) -> Result<(), FileError> {
    hopnet_storage::fragstore::store_fragment(fragments_dir, fragment_hash, data)
        .map_err(FileError::from)
}

pub fn delete_fragment(fragments_dir: &str, fragment_hash: &Blake3Hash) -> Result<(), FileError> {
    hopnet_storage::fragstore::delete_fragment(fragments_dir, fragment_hash)
        .map_err(FileError::from)
}

/// Fetch a fragment from local storage only
/// Returns the fragment data if found locally, otherwise returns an error
pub fn fetch_fragment_local(
    fragments_dir: &str,
    fragment_hash: &Blake3Hash,
) -> Result<Vec<u8>, FileError> {
    // Yield the executor around the blocking read when possible.
    // block_in_place PANICS on a current_thread runtime — and this path runs
    // on the consensus shell's dedicated thread (apply_block → handlers), so
    // fall back to a plain blocking read there (that thread is ours to block).
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| {
                hopnet_storage::fragstore::read_fragment(fragments_dir, fragment_hash)
                    .map_err(FileError::from)
            })
        }
        _ => hopnet_storage::fragstore::read_fragment(fragments_dir, fragment_hash)
            .map_err(FileError::from),
    }
}

/// Fetch and verify a fragment from local storage
/// Returns the fragment data if found locally and hash matches, otherwise returns an error
pub fn fetch_and_verify_fragment(
    fragment_hash: &Blake3Hash,
    fragments_dir: &str,
) -> Result<Vec<u8>, FileError> {
    hopnet_storage::fragstore::fetch_and_verify_fragment(fragment_hash, fragments_dir)
        .map_err(FileError::from)
}

/// Drive-owned (RFC-015): FileAccessData lives in hopnet-drive's model;
/// re-exported here so call sites don't churn.
pub use hopnet_drive::model::FileAccessData;

/// Check if a fragment exists on disk and is valid (hash matches)
pub fn fragment_exists_and_valid(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    hopnet_storage::fragstore::fragment_exists_and_valid(fragments_dir, fragment_hash)
}

// Drive-owned (RFC-015, Stage D4): shared content-update preparation
// (`prepare_content_update`) lives in hopnet_drive::http::files alongside
// its only callers (PATCH /files and FileProvider modify_item).

// Fragment cipher primitives live in the substrate crate
// (hopnet-storage::crypto, format-frozen with golden-vector tests).
pub use hopnet_storage::crypto::{
    calculate_encrypted_chunk_length, derive_chunk_key, derive_chunk_nonce,
};

pub fn encrypt_chunk(
    chunk: Vec<u8>,
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &crate::db::CustomUUID,
) -> Result<Vec<u8>, FileError> {
    hopnet_storage::crypto::encrypt_chunk(chunk, per_file_key, fragment_id).map_err(FileError::from)
}

pub fn decrypt_chunk(
    encrypted_chunk: &[u8],
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &crate::db::CustomUUID,
) -> Result<Vec<u8>, FileError> {
    hopnet_storage::crypto::decrypt_chunk(encrypted_chunk, per_file_key, fragment_id)
        .map_err(FileError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal stand-in for the substrate's table: the guard only reads the
    /// `stored_locally` column.
    fn db_claiming(local: usize) -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE fragment_hashes (fragment_hash BLOB, stored_locally INTEGER);",
        )
        .unwrap();
        for i in 0..local {
            conn.execute(
                "INSERT INTO fragment_hashes (fragment_hash, stored_locally) VALUES (?, 1)",
                [i as i64],
            )
            .unwrap();
        }
        conn
    }

    // Impact: the store and the database can sit on different filesystems, so
    //         a node can boot before the bulk one is mounted. Starting anyway
    //         writes fragments to whatever is underneath the mountpoint, where
    //         they disappear once the real filesystem arrives.
    // Should: refuse to start when the database claims local fragments and the
    //         store is empty or absent.
    // Should: name both resolved paths, since the whole failure is that one of
    //         them is not where the operator thinks.
    #[test]
    fn refuses_when_claimed_fragments_have_no_store() {
        let empty = tempfile::tempdir().unwrap();
        let data = std::path::Path::new("/srv/fast/hopnet");

        let err =
            check_fragment_store_present(&db_claiming(7), empty.path().to_str().unwrap(), data)
                .expect_err("empty store with 7 claimed fragments must refuse");
        assert!(err.contains('7'), "message must state the claim: {err}");
        assert!(
            err.contains("/srv/fast/hopnet"),
            "message names the db: {err}"
        );
        assert!(
            err.contains(empty.path().to_str().unwrap()),
            "message names the store: {err}"
        );

        // A path that does not exist at all is the mount-race shape.
        assert!(
            check_fragment_store_present(&db_claiming(1), "/nonexistent/fragments", data).is_err()
        );
    }

    // Should: start a node that claims fragments and has them.
    // Should: start a fresh node, which claims nothing and has an empty store.
    // Should not: trip merely because the store is empty.
    #[test]
    fn allows_a_populated_store_and_a_fresh_node() {
        let dir = tempfile::tempdir().unwrap();
        let data = std::path::Path::new("/srv/fast/hopnet");

        // Fresh node: no claims, empty store.
        assert!(
            check_fragment_store_present(&db_claiming(0), dir.path().to_str().unwrap(), data)
                .is_ok()
        );

        // Claims backed by content — the two-level hex nesting means any
        // entry at all proves the filesystem is the right one.
        std::fs::create_dir(dir.path().join("ab")).unwrap();
        assert!(
            check_fragment_store_present(&db_claiming(3), dir.path().to_str().unwrap(), data)
                .is_ok()
        );
    }

    // Impact: the guard runs during boot, before the storage schema is
    //         guaranteed to exist. Failing closed there would brick a first
    //         boot rather than protect anything.
    // Should: pass when the inventory table cannot be read at all.
    #[test]
    fn fails_open_when_the_inventory_is_unreadable() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let empty = tempfile::tempdir().unwrap();
        assert!(
            check_fragment_store_present(
                &conn,
                empty.path().to_str().unwrap(),
                std::path::Path::new("/srv/fast/hopnet")
            )
            .is_ok()
        );
    }
}
