//! Server half of the fragment data plane (RFC-014).
//!
//! The host's RPC layer (iroh request arms in the main crate) stays a thin
//! shell: decode the wire request, call the matching `serve_*` fn here, map
//! the outcome back onto wire responses. Peer authentication already
//! happened at the transport layer (PeerValidator hook) — these functions
//! only enforce substrate rules (size cap, content addressing).

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
}

/// Store a fragment pushed by a peer: enforce the size cap and content
/// addressing, persist atomically, and queue the stored_locally settlement.
pub fn serve_fragment_store<L: LocalStateSink + ?Sized>(
    fragments_dir: &str,
    sink: &L,
    fragment_hash: &Blake3Hash,
    data: Vec<u8>,
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

    if let Err(e) = fragstore::store_fragment(fragments_dir, fragment_hash, data) {
        return StoreOutcome::Io(e);
    }

    sink.mark_local(*fragment_hash);
    StoreOutcome::Stored
}
