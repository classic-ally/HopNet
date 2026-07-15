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

/// A peer as the substrate sees it — the comms vocabulary type (RFC-017
/// Stage 2 unified the previously-identical shapes; no iroh leaks in, the
/// comms default face is dependency-free). Re-exported so existing
/// `hopnet_storage::PeerRef` paths keep compiling.
pub use hopnet_comms::PeerRef;

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

/// The decay-tiered storage membership view (RFC-STORAGE-001 Membership):
/// registered nodes minus those absent beyond their tier, with quantized
/// placement weights and the derived watermark for this view size. Every
/// node derives the same view from the same replicated rows.
#[derive(Debug, Clone)]
pub struct StorageView {
    pub height: i32,
    pub members: Vec<PeerRef>,
    /// node_id → decay tier (seconds).
    pub tiers: std::collections::HashMap<i32, i64>,
    /// node_id → quantized placement weight (1..=16).
    pub weights: std::collections::HashMap<i32, u64>,
    /// W(|members|) under the mesh policy.
    pub watermark: usize,
}

/// Replicated-state reads the engine needs. Sync — implementations read from
/// the host's DB pool on the calling task.
pub trait StateReader: Send + Sync {
    /// Snapshot of placement inputs at the current committed height.
    fn placement_inputs(&self) -> Result<PlacementInputs, StorageError>;

    /// The decay-tiered membership view. Default falls back to the raw
    /// validator set with no tier gating (most conservative membership —
    /// nobody decayed) and default-policy watermark; hosts override with
    /// the metrics-derived view.
    fn storage_view(&self) -> Result<StorageView, StorageError> {
        let inputs = self.placement_inputs()?;
        let weights = inputs
            .metrics
            .iter()
            .map(|m| (m.node_id, crate::placement::quantized_weight(m)))
            .collect();
        Ok(StorageView {
            height: inputs.height,
            watermark: crate::membership::watermark(inputs.validators.len()),
            tiers: std::collections::HashMap::new(),
            weights,
            members: inputs.validators,
        })
    }

    /// Snapshot of placement inputs at a HISTORICAL height — the get path's
    /// placement-directed discovery rung reads the validator set the blob's
    /// placement commit was computed against.
    fn placement_inputs_at(&self, height: i32) -> Result<PlacementInputs, StorageError>;

    /// Top verified holders per fragment, from the replicated inventory
    /// attestations (self-check txs). Discovery's primary rung.
    fn fragment_sources(
        &self,
        fragment_hashes: &[Blake3Hash],
    ) -> Result<std::collections::HashMap<Blake3Hash, Vec<PeerRef>>, StorageError>;

    /// Every known peer except this node — discovery's gossip fallback when
    /// a blob has no placement height.
    fn all_peers(&self) -> Result<Vec<PeerRef>, StorageError>;

    /// The blob's local fragment set IF this node should distribute it:
    /// unplaced and every fragment stored locally (origin filter). `None`
    /// is the common no-op on non-origin nodes.
    fn distributable_blob(&self, blob_id: &BlobId)
    -> Result<Option<DistributableBlob>, StorageError>;

    /// The blob's full reassembly manifest (tier-1 repair reads the
    /// fragment layout + this node's local availability). `None` for an
    /// unknown blob id.
    fn blob_manifest(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<crate::store::BlobManifest>, StorageError>;

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
