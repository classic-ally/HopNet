//! The decided-fetch server bounds each reply by bytes so a lagging node can
//! sync through a stretch of fat blocks. Regression guard for 2026-10-01:
//! fifty post-crossing blocks encoded to 10.9 MB, the transport refused the
//! reply at the receiver on every peer alike, and the macbook never advanced
//! past its join height.

use ed25519_dalek::SigningKey;

use crate::consensus::malachite::gossip::{ConsensusNetRequest, ConsensusNetResponse};
use crate::consensus::tests::MockNode;
use crate::consensus::tests::regenesis::register_node;
use hopnet_consensus::codec::{self, WireCommitCertificate};
use hopnet_consensus::context::Height;
use hopnet_consensus::store::SqliteStorage;
use hopnet_consensus::traits::Storage;
use hopnet_consensus::types::{Block, BlockData, PrivKey, Transaction, Transactions};

/// One decided block per height, each carrying a single transaction with a
/// payload of `payload_len` bytes — the shape of an attestation page.
fn seed_fat_chain(node: &MockNode, heights: u64, payload_len: usize) {
    let key = {
        let mut seed = [0u8; 32];
        seed[0] = node.node_id as u8;
        seed[31] = 0x5A;
        PrivKey(SigningKey::from_bytes(&seed))
    };
    let mut conn = node.app_state.db_pool.get().unwrap();
    hopnet_consensus::store::install_schema(&conn).unwrap();
    let mut parent = None;
    for h in 1..=heights {
        let block = Block::new(BlockData {
            height: h,
            round: 0,
            parent_hash: parent,
            transactions: Transactions(vec![
                Transaction::new(
                    "test.fat".into(),
                    vec![h as u8; payload_len],
                    node.node_id,
                    &key,
                )
                .unwrap(),
            ]),
        })
        .unwrap();
        let cert = WireCommitCertificate {
            height: h,
            round: 0,
            value_id: block.block_hash,
            signatures: Vec::new(),
        };
        let mut tx = conn.transaction().unwrap();
        <SqliteStorage>::store_decided_tx(&mut tx, &block, &cert).unwrap();
        <SqliteStorage>::set_last_decided_tx(&mut tx, Height(h)).unwrap();
        tx.commit().unwrap();
        parent = Some(block.block_hash);
    }
}

// Impact: the transport refuses frames over 8 MB only on receive, so an
// oversized reply leaves the server and strikes every peer at the lagging
// node — it never catches up and nothing at INFO says why (macbook,
// 2026-10-01, stuck at the join height for hours).
// Should: answer a fetch over fat blocks with a contiguous prefix from the
// requested height whose encoded size stays under the frame cap.
// Should not: serve the whole range when it would cross the cap, or answer
// with zero pairs.
#[test]
fn fetch_over_fat_blocks_is_a_short_contiguous_prefix_under_the_frame() {
    let _env = crate::test_env::lock_env();
    let node = MockNode::new(7);
    register_node(&node);
    // 20 blocks × ~600 KB ≈ 12 MB in range: well over the frame, like the
    // live post-crossing stretch.
    seed_fat_chain(&node, 20, 600 * 1024);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scope = crate::net::scopes::ConsensusScope {
        app_state: node.app_state.clone(),
    };
    let peer = hopnet_comms::PeerRef {
        node_id: 42,
        pubkey: [0u8; 32],
    };
    let resp = rt.block_on(scope.serve(
        peer,
        crate::net::encode_payload(&ConsensusNetRequest::DecidedFetch {
            from_height: 1,
            to_height: 20,
            epoch: 1,
        }),
    ));

    let ConsensusNetResponse::Decided { items } = &resp else {
        panic!("expected a Decided reply, got {resp:?}");
    };
    assert!(!items.is_empty(), "a bounded reply still serves something");
    assert!(
        items.len() < 20,
        "the whole range would cross the frame cap; got {}",
        items.len()
    );
    assert!(
        crate::net::encode_payload(&resp).len() <= 8 * 1024 * 1024,
        "the encoded reply must fit the transport frame"
    );
    for (expected, (block_bytes, _)) in (1u64..).zip(items) {
        let block: Block = codec::decode(block_bytes).unwrap();
        assert_eq!(
            block.data.height, expected,
            "the prefix is contiguous from 1"
        );
    }
}
