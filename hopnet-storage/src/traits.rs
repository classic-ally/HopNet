//! Host seams for the distribution engine (RFC-014).
//!
//! The engine is generic over four capabilities the HOST provides: moving
//! fragment bytes between peers (`Transport`), reading placement inputs from
//! replicated state (`StateReader`), submitting storage-owned consensus
//! transactions (`TxSubmitter`), and settling the node-local `stored_locally`
//! flag (`LocalStateSink`). No iroh, r2d2, or consensus types cross this
//! boundary — peers are `PeerRef`, errors are opaque strings classified only
//! by retry semantics.
//!
//! Trait futures carry an explicit `Send` bound because the engine spawns
//! them onto host-provided runtimes.

use crate::error::StorageError;
use crate::placement::{MetricsRow, PlacementNode};
use crate::store::DistributableBlob;
use crate::types::BlobId;
use hopnet_common::Blake3Hash;
use std::future::Future;

/// A peer as the substrate sees it: consensus node id plus the 32-byte
/// ed25519 pubkey the host's transport dials by. No transport types leak in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerRef {
    pub node_id: i32,
    pub pubkey: [u8; 32],
}

impl PlacementNode for PeerRef {
    fn node_id(&self) -> i32 {
        self.node_id
    }
}

/// Transport failures, classified by retry semantics.
#[derive(Debug)]
pub enum TransportError {
    /// Server-side application error (hash mismatch, size limit) — the
    /// engine retries these at the domain level.
    Peer(String),
    /// Transport-level failure (connect, timeout). The host's transport
    /// already did its own retry — the engine moves to the next candidate.
    Transport(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Peer(m) => write!(f, "peer error: {}", m),
            TransportError::Transport(m) => write!(f, "transport error: {}", m),
        }
    }
}

/// Outcome of a successful remote fragment store.
#[derive(Debug, Clone, Copy)]
pub struct StoreResult {
    pub already_existed: bool,
}

/// Fragment data plane: move fragment bytes to/from peers.
pub trait Transport: Send + Sync {
    /// Store a fragment on a remote peer.
    fn store_fragment(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
        data: Vec<u8>,
    ) -> impl Future<Output = Result<StoreResult, TransportError>> + Send;

    /// Fetch a fragment from a remote peer (get path — Stage F).
    fn fetch_fragment(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
    ) -> impl Future<Output = Result<Vec<u8>, TransportError>> + Send;

    /// Ask a remote peer whether it holds a healthy copy of a fragment.
    fn fragment_health(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
    ) -> impl Future<Output = Result<bool, TransportError>> + Send;
}

/// One consistent snapshot of everything placement needs: height, the
/// validator set at that height, and node metrics at that height. The host
/// MUST read all three on one scoped DB checkout (conn-lifecycle rule:
/// never hold a pool conn across the data plane).
#[derive(Debug, Clone)]
pub struct PlacementInputs {
    pub height: i32,
    pub validators: Vec<PeerRef>,
    pub metrics: Vec<MetricsRow>,
}

/// Replicated-state reads the engine needs. Sync — implementations read from
/// the host's DB pool on the calling task.
pub trait StateReader: Send + Sync {
    /// Snapshot of placement inputs at the current committed height.
    fn placement_inputs(&self) -> Result<PlacementInputs, StorageError>;

    /// The blob's local fragment set IF this node should distribute it:
    /// unplaced and every fragment stored locally (origin filter). `None`
    /// is the common no-op on non-origin nodes.
    fn distributable_blob(&self, blob_id: &BlobId)
    -> Result<Option<DistributableBlob>, StorageError>;

    /// This node's consensus id, once initialized. The engine skips sends to
    /// itself; `None` fails the blob's distribution (retried on next kick).
    fn local_node_id(&self) -> Option<i32>;
}

/// Submit failures, classified by retry semantics.
#[derive(Debug)]
pub enum SubmitError {
    /// Permanent rejection (business validation, signing) — drop the tx.
    Rejected(String),
    /// Transient (timeout, backpressure, internal) — the engine re-stages
    /// with attempt caps.
    Transient(String),
}

/// Consensus control plane for storage-owned transactions. Signing is
/// host-side; the substrate hands over only (function, payload).
pub trait TxSubmitter: Send + Sync {
    fn submit(
        &self,
        function: &'static str,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), SubmitError>> + Send;
}

/// Node-local `stored_locally` settlement (RFC-014 invariant: exactly the
/// crate-owned writers). Fire-and-forget — the host batches through its
/// write gate; a dropped update self-heals via self-check attestation.
pub trait LocalStateSink: Send + Sync {
    /// Fragment now on local disk (server store path).
    fn mark_local(&self, fragment_hash: Blake3Hash);
    /// Fragments handed to remote peers (distribution path).
    fn mark_remote_batch(&self, fragment_hashes: Vec<Blake3Hash>);
}
