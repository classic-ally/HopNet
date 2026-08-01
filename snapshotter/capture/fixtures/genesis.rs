use hopnet::db;
use hopnet::types::{Node, User};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

use super::keys;

/// Set up genesis state: user 0, node 0, this_node identity, and the
/// malachite genesis (decided_blocks[0] + synthetic certificate + meta).
/// Bypasses `post_initial_setup()` to avoid needing AppState.
pub fn setup_genesis(pool: &Pool<SqliteConnectionManager>) {
    let (node0_priv, node0_pub) = keys::ed25519_from_seed(keys::NODE_0_SEED);
    let (_user0_priv, user0_pub) = keys::ed25519_from_seed(keys::USER_0_SEED);
    let user0_x25519 = keys::x25519_pubkey_from_seed(keys::USER_0_X25519_SEED);

    let mut conn = pool.get().expect("Failed to get connection");

    // === Transaction 1: Initialize sequences + insert user 0 + node 0 + activate validator 0 ===
    {
        let tx = conn.transaction().expect("Failed to begin transaction");

        db::setup::initialize_sequences_tx(&tx).expect("Failed to initialize sequences");

        let user0 = User::new(
            0,
            "alice".to_string(),
            user0_pub,
            user0_x25519,
            vec![0u8; 48], // dummy encrypted_privkey
            vec![0u8; 16], // dummy key_salt
        );
        db::users::insert_user_tx(&tx, user0).expect("Failed to insert user 0");

        let node0 = Node {
            node_id: 0,
            name: "node-0".to_string(),
            owner: 0,
            pubkey: node0_pub,
        };
        db::nodes::insert_node_tx(&tx, node0).expect("Failed to insert node 0");

        db::consensus::activate_validator(&tx, 0, 0).expect("Failed to activate validator 0");

        tx.commit()
            .expect("Failed to commit genesis user/node/validator");
    }

    // === Transaction 2: this_node identity + malachite genesis ===
    {
        let tx = conn.transaction().expect("Failed to begin transaction");

        tx.execute(
            "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (?, ?, ?)",
            params![1, 0, node0_priv],
        )
        .expect("Failed to insert this_node");

        // Engine-shape genesis block (empty transactions — fixture doesn't
        // exercise the dispatch table) with the synthetic trusted certificate.
        let engine_block =
            hopnet_consensus::types::Block::new(hopnet_consensus::types::BlockData {
                height: 0,
                round: 0,
                parent_hash: None,
                transactions: hopnet_consensus::types::Transactions(Vec::new()),
            })
            .expect("Failed to build engine genesis block");
        let cert = hopnet_consensus::codec::WireCommitCertificate {
            height: 0,
            round: 0,
            value_id: engine_block.block_hash,
            signatures: Vec::new(),
        };
        hopnet_consensus::store::install_genesis(&tx, &engine_block, &cert)
            .expect("Failed to install malachite genesis");
        hopnet_consensus::store::meta_put(
            &tx,
            hopnet_consensus::store::META_CHAIN_ID,
            engine_block.block_hash.as_bytes(),
        )
        .expect("Failed to store chain id");
        hopnet_consensus::store::meta_put(
            &tx,
            hopnet_consensus::store::META_QUORUM_PROFILE,
            b"bft",
        )
        .expect("Failed to store quorum profile");

        tx.commit()
            .expect("Failed to commit this_node + malachite genesis");
    }
}
