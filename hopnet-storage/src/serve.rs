//! Server half of the fragment data plane (RFC-014).
//!
//! The host's RPC layer (iroh request arms in the main crate) stays a thin
//! shell: decode the wire request, call the matching `serve_*` fn here, map
//! the outcome back onto wire responses. Peer authentication already
//! happened at the transport layer (PeerValidator hook) — these functions
//! only enforce substrate rules (size cap, content addressing).

use crate::admission::{self, SpaceGuard, WriteClass};
use crate::error::StorageError;
use crate::fragstore;
use crate::traits::LocalStateSink;
use hopnet_common::Blake3Hash;

/// Maximum accepted fragment wire size: one encrypted max-size fragment.
pub fn max_fragment_wire_size() -> usize {
    crate::crypto::calculate_encrypted_chunk_length(crate::rs::MAX_FRAGMENT_SIZE)
}

/// Health probe: does this node hold a valid copy of the fragment?
pub fn serve_fragment_health(fragments_dir: &str, fragment_hash: &Blake3Hash) -> bool {
    fragstore::fragment_exists_and_valid(fragments_dir, fragment_hash)
}

/// Fetch: return the fragment bytes if present AND content-verified.
pub fn serve_fragment_fetch(fragments_dir: &str, fragment_hash: &Blake3Hash) -> Option<Vec<u8>> {
    fragstore::fetch_and_verify_fragment(fragment_hash, fragments_dir).ok()
}

/// Outcome of a fragment store request, for the host to map onto its wire
/// error/response shapes.
#[derive(Debug)]
pub enum StoreOutcome {
    /// Stored to disk; local-state settlement queued via the sink.
    Stored,
    /// Valid copy already on disk — no write, no settlement (the original
    /// store already queued it).
    AlreadyExisted,
    /// Rejected: payload exceeds the max encrypted fragment size.
    TooLarge { got: usize, max: usize },
    /// Rejected: content does not hash to the claimed fragment hash.
    HashMismatch { expected: String, actual: String },
    /// Disk write failed.
    Io(StorageError),
    /// Refused: this node is below its pull floor (`admission::SpaceGuard`).
    NoSpace,
}

/// Store a fragment pushed by a peer: enforce the size cap and content
/// addressing, persist atomically, and settle stored_locally (awaited —
/// the wire arm survives for compatibility; the engine no longer pushes).
pub async fn serve_fragment_store<L: LocalStateSink + ?Sized>(
    fragments_dir: &str,
    sink: &L,
    fragment_hash: &Blake3Hash,
    data: Vec<u8>,
) -> StoreOutcome {
    serve_fragment_store_within(
        fragments_dir,
        sink,
        fragment_hash,
        data,
        SpaceGuard::global(),
    )
    .await
}

/// [`serve_fragment_store`] against a given replica-write floor: a pushed
/// copy is a replica like a pulled one, so it stops at the pull floor.
pub async fn serve_fragment_store_within<L: LocalStateSink + ?Sized>(
    fragments_dir: &str,
    sink: &L,
    fragment_hash: &Blake3Hash,
    data: Vec<u8>,
    space: &SpaceGuard,
) -> StoreOutcome {
    let max = max_fragment_wire_size();
    if data.len() > max {
        return StoreOutcome::TooLarge {
            got: data.len(),
            max,
        };
    }

    let actual = Blake3Hash::new(blake3::hash(&data));
    if actual != *fragment_hash {
        return StoreOutcome::HashMismatch {
            expected: fragment_hash.to_hex(),
            actual: actual.to_hex(),
        };
    }

    if fragstore::fragment_exists_and_valid(fragments_dir, fragment_hash) {
        return StoreOutcome::AlreadyExisted;
    }

    let Ok(_reserved) = space.reserve(
        fragments_dir,
        admission::fragment_file_bytes(data.len()),
        WriteClass::Pull,
    ) else {
        return StoreOutcome::NoSpace;
    };
    if let Err(e) = fragstore::store_fragment(fragments_dir, fragment_hash, data) {
        if admission::is_disk_full(&e) {
            space.note_disk_full();
            return StoreOutcome::NoSpace;
        }
        return StoreOutcome::Io(e);
    }

    sink.mark_local(*fragment_hash).await;
    StoreOutcome::Stored
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::PullFloor;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Marks(Mutex<Vec<Blake3Hash>>);

    impl LocalStateSink for Marks {
        async fn mark_local(&self, fragment_hash: Blake3Hash) {
            self.0.lock().unwrap().push(fragment_hash);
        }
        async fn mark_remote_batch(&self, _fragment_hashes: Vec<Blake3Hash>) {}
    }

    // Impact: the inbound store arm is a replica write like a pull; left
    // unguarded, a peer push could fill a node the pull floor protects.
    // Should: refuse a pushed fragment below the pull floor, writing and
    // marking nothing.
    // Should: store it once free space is back.
    #[tokio::test]
    async fn serve_store_refuses_below_the_floor() {
        let dir = std::env::temp_dir().join(format!("hopnet-serve-floor-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.to_str().unwrap().to_string();
        let free = Arc::new(AtomicU64::new(10));
        let read = free.clone();
        let space = SpaceGuard::new(
            PullFloor {
                min_free_bytes: 1 << 20,
                min_free_basis_points: 0,
                resume_gap_bytes: Some(0),
            },
            Some(1),
            Box::new(move |_| Ok((read.load(Ordering::Acquire), 1 << 40))),
            Box::leak(Box::new(AtomicU64::new(0))),
        );
        let data = vec![7u8; 1000];
        let hash = Blake3Hash::new(blake3::hash(&data));
        let marks = Marks::default();

        let refused = serve_fragment_store_within(&dir, &marks, &hash, data.clone(), &space).await;
        assert!(matches!(refused, StoreOutcome::NoSpace), "{refused:?}");
        assert!(!fragstore::fragment_exists_and_valid(&dir, &hash));
        assert!(marks.0.lock().unwrap().is_empty());

        free.store(1 << 30, Ordering::Release);
        assert!(space.reprobe(&dir));
        let stored = serve_fragment_store_within(&dir, &marks, &hash, data, &space).await;
        assert!(matches!(stored, StoreOutcome::Stored), "{stored:?}");
        assert_eq!(marks.0.lock().unwrap().as_slice(), &[hash]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
