//! Byzantine-input tests at the APPLICATION seam: forged transaction
//! signatures and invalid transactions must make `HopNetApplication`'s
//! Rule-8 validation reject the whole block before any node votes for it.
//!
//! The certificate-level adversarial cases (duplicate signers, forged vote
//! signatures, non-validator votes, sub-quorum certificates) live in
//! hopnet-consensus's `verify` tests — the engine crate owns that layer.

use crate::consensus::malachite::app::{HopNetApplication, to_engine_transactions};
use crate::consensus::tests::{MockNetwork, MockUser};
use crate::consensus::types::Transactions;
use hopnet_consensus::Validity;
use hopnet_consensus::context::Height;
use hopnet_consensus::store::SqliteStorage;
use hopnet_consensus::traits::{Application, ValidationOrigin};
use hopnet_consensus::types as engine;

type PoolStorage = SqliteStorage<r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>>;

/// Build the engine-shape block at height 1 (parent = the installed genesis)
/// containing `txs`, then run the receiver-side Rule-8 validation the way the
/// host does: inside a rolled-back DB transaction. Shared with the regenesis
/// boundary tests (RFC-019 S5).
pub(super) fn validate_at_height_1(
    app_state: &crate::AppState,
    txs: Vec<crate::consensus::types::Transaction>,
) -> Validity {
    let engine_txs = to_engine_transactions(&Transactions(txs)).expect("transaction bridge");

    let mut conn = app_state.db_pool.get().expect("pool");
    let parent: Vec<u8> = conn
        .query_row(
            "SELECT block_hash FROM decided_blocks WHERE height = 0",
            [],
            |row| row.get(0),
        )
        .expect("genesis installed");
    let parent_hash =
        engine::Blake3Hash::from_bytes(parent.as_slice().try_into().expect("32 bytes"));

    let block = engine::Block::new(engine::BlockData {
        height: 1,
        round: 0,
        parent_hash: Some(parent_hash),
        transactions: engine_txs,
    })
    .expect("block build");

    let app_conn = app_state.db_pool.get().expect("app conn");
    let mut app = HopNetApplication::new(app_state.clone(), app_conn);
    let mut db_tx = conn.transaction().expect("tx");
    // Rolled back on drop — validation must not mutate state.
    <HopNetApplication as Application<PoolStorage>>::validate_block(
        &mut app,
        Height(1),
        &block,
        &mut db_tx,
        ValidationOrigin::Live,
    )
}

// Should: reject a block containing a transaction that claims node 0's
// identity but carries node 1's signature.
// Should not: let submitter identity and signature diverge.
// Impact: Byzantine proposers cannot impersonate other nodes in proposals.
#[test]
fn test_forged_node_signature() {
    let network = MockNetwork::setup_with_validators(3);
    let node0 = &network.nodes[0];
    let node1 = &network.nodes[1];

    let rpc = crate::consensus::types::RpcCall {
        function: "test_function".to_string(),
        payload: vec![1, 2, 3],
    };
    let forged_signature = rpc
        .sign(&node1.signing_key)
        .expect("Failed to sign with node1's key");
    let forged_tx = crate::consensus::types::Transaction {
        rpc,
        submitter: crate::consensus::types::SignedIdentity {
            id: node0.node_id,           // Claims to be node0
            signature: forged_signature, // But signed with node1's key
        },
        user: None,
        nonce: hopnet_common::CustomUUID::new(None),
    };

    assert_eq!(
        validate_at_height_1(&node0.app_state, vec![forged_tx]),
        Validity::Invalid,
        "Block with forged node signature must be rejected"
    );
}

// Should: reject a block containing a transaction that claims user 0's
// identity but carries user 1's signature.
// Should not: accept user identity theft even when the node signature is valid.
// Impact: a compromised node cannot forge user-authorized operations.
#[test]
fn test_forged_user_signature() {
    let network = MockNetwork::setup_with_validators(3);
    let node = &network.nodes[0];
    let user0 = &network.users[0];
    let user1 = MockUser::new(1);

    // Add user1 to the database so pubkey lookup works
    for net_node in &network.nodes {
        let db = net_node.app_state.db_pool.get().expect("Failed to get DB");
        let x25519_pubkey = crate::auth::derive_x25519_pubkey_from_user(&user1.signing_key);
        db.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt) VALUES (?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                user1.user_id,
                format!("user_{}", user1.user_id),
                user1.verifying_key,
                x25519_pubkey,
                vec![0u8; 44],
                vec![0u8; 16]
            ]
        ).expect("Failed to insert user1");
    }

    let rpc = crate::consensus::types::RpcCall {
        function: "test_function".to_string(),
        payload: vec![1, 2, 3],
    };
    let node_signature = rpc
        .sign(&node.signing_key)
        .expect("Failed to sign with node key");
    let forged_user_signature = rpc
        .sign(&user1.signing_key)
        .expect("Failed to sign with user1's key");

    let forged_tx = crate::consensus::types::Transaction {
        rpc,
        submitter: crate::consensus::types::SignedIdentity {
            id: node.node_id,
            signature: node_signature,
        },
        user: Some(crate::consensus::types::SignedIdentity {
            id: user0.user_id,                // Claims to be user0
            signature: forged_user_signature, // But signed with user1's key
        }),
        nonce: hopnet_common::CustomUUID::new(None),
    };

    assert_eq!(
        validate_at_height_1(&node.app_state, vec![forged_tx]),
        Validity::Invalid,
        "Block with forged user signature must be rejected"
    );
}

// Should: reject the ENTIRE block when one of its transactions carries a
// forged signature, even if the others are valid (all-or-nothing).
// Should not: partially accept a block.
// Impact: a Byzantine proposer cannot smuggle one bad transaction inside a
// batch of good ones.
#[test]
fn test_one_invalid_tx_rejects_block() {
    let network = MockNetwork::setup_with_validators(3);
    let node0 = &network.nodes[0];
    let node1 = &network.nodes[1];

    // 5 well-signed transactions
    let mut transactions = Vec::new();
    for i in 0..5 {
        let tx = crate::consensus::types::Transaction::new(
            format!("valid_function_{}", i),
            vec![i as u8; 10],
            node0.node_id,
            &node0.signing_key,
        )
        .expect("Failed to create valid transaction");
        transactions.push(tx);
    }

    // 1 transaction with a corrupted (wrong-key) signature
    let mut invalid_tx = crate::consensus::types::Transaction::new(
        "invalid_function".to_string(),
        vec![99; 10],
        node0.node_id,
        &node0.signing_key,
    )
    .expect("Failed to create transaction");
    invalid_tx.submitter.signature = invalid_tx
        .rpc
        .sign(&node1.signing_key)
        .expect("Failed to sign with wrong key");
    transactions.push(invalid_tx);

    assert_eq!(
        validate_at_height_1(&node0.app_state, transactions),
        Validity::Invalid,
        "Block containing one forged transaction must be rejected wholesale"
    );
}
