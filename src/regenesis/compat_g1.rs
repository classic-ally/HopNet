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

#[cfg(test)]
mod tests {
    use super::*;

    fn encode<T: serde::Serialize>(value: &T) -> Vec<u8> {
        bincode::serde::encode_to_vec(value, bincode::config::standard()).unwrap()
    }

    // Impact: these bytes are the generation-1 wire contract for epoch
    // rejoin — silent drift strands every straggler at the next boundary
    // (wire.md).
    // Should: encode every request verb exactly — variant varint, field
    // varints in declaration order, including the multi-byte thresholds.
    #[test]
    fn g1_request_goldens() {
        assert_eq!(encode(&RegenesisNetRequest::EpochInfo), [0x00]);
        assert_eq!(
            encode(&RegenesisNetRequest::LineageFetch { from_epoch: 2 }),
            [0x01, 0x02]
        );
        assert_eq!(
            encode(&RegenesisNetRequest::SnapshotInfo { epoch: 2 }),
            [0x02, 0x02]
        );
        assert_eq!(
            encode(&RegenesisNetRequest::SnapshotChunk {
                epoch: 2,
                offset: 4096,
                len: SNAPSHOT_CHUNK_MAX,
            }),
            [
                0x03, // variant
                0x02, // epoch
                0xFB, 0x00, 0x10, // offset 4096 (u16 varint)
                0xFC, 0x00, 0x00, 0x40, 0x00, // len 4 MiB (u32 varint)
            ]
        );
    }

    // Should: encode every response verb exactly — Option tags, length
    // prefixes on vectors and strings, and the UNPREFIXED fixed hash
    // array.
    #[test]
    fn g1_response_goldens() {
        assert_eq!(
            encode(&RegenesisNetResponse::EpochInfo {
                epoch: 2,
                decided_height: 7,
                epoch_genesis_height: Some(7),
                lineage_from: Some(2),
            }),
            [0x00, 0x02, 0x07, 0x01, 0x07, 0x01, 0x02]
        );
        assert_eq!(
            encode(&RegenesisNetResponse::Lineage {
                records: vec![vec![0xAA, 0xBB]],
            }),
            [0x01, 0x01, 0x02, 0xAA, 0xBB]
        );
        let mut snapshot_info = vec![0x02, 0x02, 0xFB, 0x2F, 0x02];
        snapshot_info.extend_from_slice(&[0xAB; 32]); // fixed array: raw, no prefix
        assert_eq!(
            encode(&RegenesisNetResponse::SnapshotInfo {
                epoch: 2,
                total_len: 559,
                snapshot_hash: [0xAB; 32],
            }),
            snapshot_info
        );
        assert_eq!(
            encode(&RegenesisNetResponse::SnapshotChunk {
                data: vec![0x01, 0x02, 0x03],
            }),
            [0x03, 0x03, 0x01, 0x02, 0x03]
        );
        assert_eq!(
            encode(&RegenesisNetResponse::NotAvailable {
                reason: "gone".into(),
            }),
            [0x04, 0x04, b'g', b'o', b'n', b'e']
        );
        assert_eq!(
            encode(&RegenesisNetResponse::Error {
                message: "bad".into(),
            }),
            [0x05, 0x03, b'b', b'a', b'd']
        );
    }

    /// A complete on-disk/wire LineageRecord, generated ONCE from the
    /// deterministic sealed-pool fixture (genesis.rs tests) and pinned
    /// here DECOUPLED from it — fixture drift must never edit a frozen
    /// file. 559 bytes.
    const LINEAGE_RECORD_HEX: &str = concat!(
        "0102fcc0273501030303030303030303030303030303030303030303030303",
        "030303030303030300bbbc526237afa6c5f4724a3703b2e1e3fb8a31bdc8e6",
        "dcca1628cf97916d4c07ababababababababababababababababababababab",
        "ababababababababababab086d616a6f7269747902028a88e3dd7409f195fd",
        "52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c048139770ea87d17",
        "5f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394d12000bbbc52",
        "6237afa6c5f4724a3703b2e1e3fb8a31bdc8e6dcca1628cf97916d4c070001",
        "20020202020202020202020202020202020202020202020202020202020202",
        "02020110726567656e657369735f636f6d6d697426abababababababababab",
        "abababababababababababababababababababababab07fcc027350102b89e",
        "0ba5e6343aeb765ed72c2dd4fd9527348028c9034385961c91cf1a2594aeae",
        "13db3d1b48309ebfc079606b5727a87f80ab1874931e56cc6fa274cce9c309",
        "001000000000000070008000000000000001a807002000bbbc526237afa6c5",
        "f4724a3703b2e1e3fb8a31bdc8e6dcca1628cf97916d4c0202406a04741288",
        "8fbf57cc4148c2f5c4d6d5904afe52eb9603efd8d5b0774dd0b946744c4bb0",
        "e77705af0e3db4e8d7d365860a7b21f6a0684e63a172546fb5906b0704409e",
        "f0231291c26ef803c2a47566cc89f31c7122db7c7b311b4140df1244ac2632",
        "228b9140c90d0f1f5d28fa98b4f622745efaf36f5c09a13d675cc2d3344265",
        "0d",
    );

    // Impact: the reached-into encoding (RFC-025 §Scope Classes) — the
    // straggler's OLD binary parses these bytes hop by hop, so this
    // golden transitively pins LineageRecord, EpochGenesisRecord, Block,
    // BlockData, Transactions, Transaction, RpcCall, SignedIdentity,
    // Blake3Hash, WireCommitCertificate, WireSig, and the uuid /
    // ed25519 / bincode serde behavior underneath. Reshaping ANY of them
    // strands every straggler while the wire-type goldens still pass.
    // Should: decode the pinned record, verify its content, and
    // re-encode every layer back to the identical bytes.
    #[test]
    fn lineage_record_closure_golden() {
        let bytes = hex::decode(LINEAGE_RECORD_HEX).unwrap();
        let record = crate::regenesis::genesis::decode_lineage(&bytes).unwrap();
        assert_eq!(record.record.epoch, 2);
        assert_eq!(record.record.seal_height, 7);
        assert_eq!(record.record.snapshot_hash, [0xAB; 32]);
        assert_eq!(record.record.quorum_profile, "majority");
        assert_eq!(record.record.seated.len(), 2);
        assert_eq!(record.record.format_version, 1);

        // Outer re-encode: LineageRecord + EpochGenesisRecord.
        let re = bincode::serde::encode_to_vec(&record, bincode::config::standard()).unwrap();
        assert_eq!(re, bytes, "lineage wire format drifted");

        // Inner blobs: the straggler decodes these with the consensus
        // codec (chain verification) — same freeze surface.
        let block: hopnet_consensus::types::Block =
            hopnet_consensus::codec::decode(&record.final_block).unwrap();
        assert_eq!(block.data.height, 7);
        assert_eq!(block.data.transactions.0.len(), 1);
        assert_eq!(block.data.transactions.0[0].rpc.function, "regenesis_commit");
        assert_eq!(
            hopnet_consensus::codec::encode(&block).unwrap(),
            record.final_block,
            "final_block encoding drifted"
        );
        let cert: hopnet_consensus::codec::WireCommitCertificate =
            hopnet_consensus::codec::decode(&record.final_cert).unwrap();
        assert_eq!(cert.signatures.len(), 2);
        assert_eq!(
            hopnet_consensus::codec::encode(&cert).unwrap(),
            record.final_cert,
            "final_cert encoding drifted"
        );
    }
}
