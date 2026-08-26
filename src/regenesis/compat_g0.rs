//! Generation-0 frozen inventory: the regenesis vocabulary every
//! pre-enforcement binary speaks (RFC-025 §Evolution).
//!
//! FROZEN, and test-only: generation 0 and generation 1 are
//! byte-identical for this scope, so production serves generation 0
//! with the HEAD handler (the same Arc registered for both window
//! slots in `net::scopes`) — the equality tests below are that
//! decision's license, and the first divergence forces a real adapter
//! or a deliberate revert. Retirement (the first real mint) deletes
//! this file whole under a `RETIRES: compat_g0` commit trailer.

use serde::{Deserialize, Serialize};

/// The generation this module's vocabulary belongs to.
pub const GENERATION: u32 = 0;

#[derive(Serialize, Deserialize)]
pub enum RegenesisNetRequest {
    EpochInfo,
    LineageFetch { from_epoch: u64 },
    SnapshotInfo { epoch: u64 },
    SnapshotChunk { epoch: u64, offset: u64, len: u64 },
}

#[derive(Serialize, Deserialize)]
pub enum RegenesisNetResponse {
    EpochInfo {
        epoch: u64,
        decided_height: u64,
        epoch_genesis_height: Option<u64>,
        lineage_from: Option<u64>,
    },
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
    NotAvailable {
        reason: String,
    },
    Error {
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::super::compat_g1 as g1;
    use super::*;

    fn encode<T: serde::Serialize>(value: &T) -> Vec<u8> {
        bincode::serde::encode_to_vec(value, bincode::config::standard()).unwrap()
    }

    // Impact: the license for serving generation 0 with the head handler
    // — the first divergence between these encodings forces a real
    // adapter (or a deliberate revert), never a silent strand.
    // Should: encode every generation-0 verb byte-identically to its
    // generation-1 counterpart.
    #[test]
    fn g0_encodings_equal_head_encodings() {
        assert_eq!(
            encode(&RegenesisNetRequest::EpochInfo),
            encode(&g1::RegenesisNetRequest::EpochInfo)
        );
        assert_eq!(
            encode(&RegenesisNetRequest::LineageFetch { from_epoch: 2 }),
            encode(&g1::RegenesisNetRequest::LineageFetch { from_epoch: 2 })
        );
        assert_eq!(
            encode(&RegenesisNetRequest::SnapshotInfo { epoch: 2 }),
            encode(&g1::RegenesisNetRequest::SnapshotInfo { epoch: 2 })
        );
        assert_eq!(
            encode(&RegenesisNetRequest::SnapshotChunk {
                epoch: 2,
                offset: 4096,
                len: 4 * 1024 * 1024,
            }),
            encode(&g1::RegenesisNetRequest::SnapshotChunk {
                epoch: 2,
                offset: 4096,
                len: 4 * 1024 * 1024,
            })
        );
        assert_eq!(
            encode(&RegenesisNetResponse::EpochInfo {
                epoch: 2,
                decided_height: 7,
                epoch_genesis_height: Some(7),
                lineage_from: Some(2),
            }),
            encode(&g1::RegenesisNetResponse::EpochInfo {
                epoch: 2,
                decided_height: 7,
                epoch_genesis_height: Some(7),
                lineage_from: Some(2),
            })
        );
        assert_eq!(
            encode(&RegenesisNetResponse::Lineage {
                records: vec![vec![0xAA, 0xBB]],
            }),
            encode(&g1::RegenesisNetResponse::Lineage {
                records: vec![vec![0xAA, 0xBB]],
            })
        );
        assert_eq!(
            encode(&RegenesisNetResponse::SnapshotInfo {
                epoch: 2,
                total_len: 559,
                snapshot_hash: [0xAB; 32],
            }),
            encode(&g1::RegenesisNetResponse::SnapshotInfo {
                epoch: 2,
                total_len: 559,
                snapshot_hash: [0xAB; 32],
            })
        );
        assert_eq!(
            encode(&RegenesisNetResponse::SnapshotChunk {
                data: vec![0x01, 0x02, 0x03],
            }),
            encode(&g1::RegenesisNetResponse::SnapshotChunk {
                data: vec![0x01, 0x02, 0x03],
            })
        );
        assert_eq!(
            encode(&RegenesisNetResponse::NotAvailable {
                reason: "gone".into(),
            }),
            encode(&g1::RegenesisNetResponse::NotAvailable {
                reason: "gone".into(),
            })
        );
        assert_eq!(
            encode(&RegenesisNetResponse::Error {
                message: "bad".into(),
            }),
            encode(&g1::RegenesisNetResponse::Error {
                message: "bad".into(),
            })
        );
    }
}
