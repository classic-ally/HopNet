//! Generation-1 frozen inventory: the regenesis scope's wire vocabulary
//! (RFC-025 §Scope Classes, §The Generation Contract).
//!
//! FROZEN ONCE RELEASED. This file may contain nothing but vocabulary
//! types, protocol constants, the GENERATION label, and their byte
//! goldens — any change after the release tag fails
//! `scripts/check-compat-freeze.sh` unless `COMPAT_HEAD` was bumped (a
//! mint adds `compat_g2.rs`, never edits this file). The normative byte
//! contract is `hopnet-comms/docs/wire.md`. The reached-into
//! LineageRecord encoding (RFC-025 §Scope Classes) is generation
//! content too — its closure golden lives here even though the type
//! stays in `genesis.rs`.

use serde::{Deserialize, Serialize};

/// The generation this module's vocabulary belongs to. Pinned against
/// the served window by the cross-crate tie test in `net::scopes`.
pub const GENERATION: u32 = 1;

/// Lineage records per LineageFetch response. Each is small (one record,
/// one final block, one certificate), so the cap is about bounding a
/// single frame, not about pagination in practice.
pub const LINEAGE_FETCH_MAX: u64 = 32;

/// Snapshot chunk ceiling — the fragment precedent's size, comfortably
/// under the transport's 8MB receiver-enforced frame cap.
pub const SNAPSHOT_CHUNK_MAX: u64 = 4 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug)]
pub enum RegenesisNetRequest {
    /// Epoch identity probe: what epoch is this node on, what can it serve.
    EpochInfo,
    /// Lineage records for epochs `from_epoch..`, ascending and contiguous,
    /// capped at LINEAGE_FETCH_MAX per response.
    LineageFetch { from_epoch: u64 },
    /// Artifact identity for the server's CURRENT epoch (v1 serves the
    /// latest snapshot only). Also the PREPARE step: a server that must
    /// recompute materializes the artifact file here, so subsequent chunk
    /// reads are plain file reads.
    SnapshotInfo { epoch: u64 },
    /// One artifact byte range. `len` is clamped to SNAPSHOT_CHUNK_MAX;
    /// a read at or past EOF returns empty data.
    SnapshotChunk { epoch: u64, offset: u64, len: u64 },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RegenesisNetResponse {
    EpochInfo {
        epoch: u64,
        decided_height: u64,
        /// H for a database born from a boundary; None on epoch 1.
        epoch_genesis_height: Option<u64>,
        /// Lowest lineage record on disk (normally 2); None = none.
        lineage_from: Option<u64>,
    },
    /// Encoded LineageRecord bytes (the exact on-disk encoding), ascending.
    Lineage {
        records: Vec<Vec<u8>>,
    },
    SnapshotInfo {
        epoch: u64,
        total_len: u64,
        snapshot_hash: [u8; 32],
    },
    SnapshotChunk {
        data: Vec<u8>,
    },
    /// Honest refusal: the requested epoch is not served, or the artifact
    /// is unrecoverable here (lost file, state advanced past H, rollback
    /// window closed). The requester rotates peers.
    NotAvailable {
        reason: String,
    },
    Error {
        message: String,
    },
}
