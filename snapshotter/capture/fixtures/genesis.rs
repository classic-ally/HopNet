use hopnet::consensus::ConsensusPhase;
use hopnet::consensus::types::{
    Block, BlockData, QuorumCertificate, VoteSignMessage, VoteSignMessages,
};
use hopnet::db;
use hopnet::types::{Node, PrivKey, PubKey, User};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

use super::keys;

/// Set up genesis state: user 0, node 0, genesis block, QCs, this_node.
/// Bypasses `post_initial_setup()` to avoid needing AppState.
pub fn setup_genesis(pool: &Pool<SqliteConnectionManager>) {
    let (node0_priv, node0_pub) = keys::ed25519_from_seed(keys::NODE_0_SEED);
    let (user0_priv, user0_pub) = keys::ed25519_from_seed(keys::USER_0_SEED);
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

    // === Transaction 2: Genesis block ===
    let genesis_block = Block::new(BlockData {
        height: 0,
        view_number: 0,
        parent_hash: None,
        transactions: None,
    })
    .expect("Failed to create genesis block");

    {
        let tx = conn.transaction().expect("Failed to begin transaction");
        tx.execute(
            "INSERT INTO blocks (block_hash, height, view_number, transactions) VALUES (?, ?, ?, ?)",
            params![
                genesis_block.block_hash,
                genesis_block.data.height,
                genesis_block.data.view_number,
                genesis_block.data.transactions
            ],
        )
        .expect("Failed to insert genesis block");
        tx.commit().expect("Failed to commit genesis block");
    }

    // === Transaction 3: this_node + genesis QCs ===
    {
        let tx = conn.transaction().expect("Failed to begin transaction");

        // Initialize this_node
        tx.execute(
            "INSERT INTO this_node (internal_id, node_id, privkey, current_view, current_phase, committed_block_hash, highest_qc_block_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![1, 0, node0_priv, 0, ConsensusPhase::Propose, genesis_block.block_hash, genesis_block.block_hash],
        )
        .expect("Failed to insert this_node");

        // Create genesis QCs (Propose and Lock)
        let empty_sigs: Vec<VoteSignMessage> = Vec::new();

        let propose_qc = QuorumCertificate::create_unverified(
            &genesis_block,
            ConsensusPhase::Propose,
            0,
            &node0_priv,
            empty_sigs.clone(),
        )
        .expect("Failed to create genesis propose QC");

        let lock_qc = QuorumCertificate::create_unverified(
            &genesis_block,
            ConsensusPhase::Lock,
            0,
            &node0_priv,
            empty_sigs,
        )
        .expect("Failed to create genesis lock QC");

        db::consensus::insert_qc_unsafe_tx(&tx, &propose_qc)
            .expect("Failed to insert genesis propose QC");
        db::consensus::insert_qc_unsafe_tx(&tx, &lock_qc)
            .expect("Failed to insert genesis lock QC");

        tx.commit().expect("Failed to commit this_node + QCs");
    }
}
