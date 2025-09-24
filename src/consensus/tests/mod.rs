use ed25519_dalek::{SigningKey, VerifyingKey};
use rand_core::OsRng;
use super::*;  // Import from parent consensus module

mod signatures;
mod quorum;
mod authorization;
mod byzantine;

#[derive(Clone)]
pub struct MockNode {
    pub node_id: i32,
    pub signing_key: crate::db::PrivKey,
    pub verifying_key: crate::db::PubKey,
    pub app_state: AppState,
}

impl MockNode {
    pub fn new(node_id: i32) -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        let priv_key = crate::db::PrivKey(signing_key);
        let pub_key = crate::db::PubKey(verifying_key);

        Self {
            node_id,
            signing_key: priv_key.clone(),
            verifying_key: pub_key,
            app_state: create_test_app_state_with_keys(priv_key, pub_key),
        }
    }
}

pub struct MockUser {
    pub user_id: i32,
    pub signing_key: crate::db::PrivKey,
    pub verifying_key: crate::db::PubKey,
}

impl MockUser {
    pub fn new(user_id: i32) -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        Self {
            user_id,
            signing_key: crate::db::PrivKey(signing_key),
            verifying_key: crate::db::PubKey(verifying_key),
        }
    }
}

pub struct MockNetwork {
    pub nodes: Vec<MockNode>,
    pub users: Vec<MockUser>,
}

impl MockNetwork {
    pub fn new(num_nodes: usize, num_users: usize) -> Self {
        let nodes = (0..num_nodes as i32)
            .map(|id| MockNode::new(id))
            .collect();

        let users = (0..num_users as i32)
            .map(|id| MockUser::new(id))
            .collect();

        Self { nodes, users }
    }

    pub fn setup_with_validators(num_validators: usize) -> Self {
        let mut nodes = Vec::new();
        let users = vec![MockUser::new(0)];

        let first_node = MockNode::new(0);
        let x25519_pubkey = crate::auth::derive_x25519_pubkey_from_user(&users[0].signing_key);

        let user = crate::types::User {
            user_id: 0,
            username: "test_user".to_string(),
            password: "password".to_string(),
            pubkey: users[0].verifying_key,
            x25519_pubkey,
        };

        let first_db_node = crate::db::Node {
            node_id: 0,
            name: "node_0".to_string(),
            ip_address: "127.0.0.0".to_string(),
            port: 3000,
            owner: 0,
            pubkey: first_node.verifying_key,
        };

        crate::db::setup::post_initial_setup(
            first_node.app_state.db_pool.get(),
            user,
            first_db_node,
            first_node.verifying_key,
            first_node.signing_key.clone(),
            users[0].signing_key.clone(),
        ).expect("Failed to run initial setup for first node");

        nodes.push(first_node);

        for i in 1..num_validators {
            let joining_node = MockNode::new(i as i32);

            let joining_node_info = crate::db::Node {
                node_id: i as i32,
                name: format!("node_{}", i),
                ip_address: format!("127.0.0.{}", i),
                port: 3000 + i as i32,
                owner: 0,
                pubkey: joining_node.verifying_key,
            };

            for j in 0..i {
                let existing_node_db = nodes[j as usize].app_state.db_pool.get().expect("Failed to get existing node DB");
                existing_node_db.execute(
                    "INSERT INTO nodes (node_id, name, ip_address, port, owner, pubkey) VALUES (?, ?, ?, ?, ?, ?)",
                    duckdb::params![i as i32, format!("node_{}", i), format!("127.0.0.{}", i), 3000 + i as i32, 0, joining_node.verifying_key]
                ).expect("Failed to insert joining node into existing node");

                existing_node_db.execute(
                    "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
                    duckdb::params![0, i as i32, true]
                ).expect("Failed to insert validator into existing node");

                existing_node_db.execute(
                    "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'nodes'",
                    []
                ).expect("Failed to update node sequence");
            }

            let sync_obj = Self::build_sync_object_for_joining_node(
                &nodes[0].app_state,
                joining_node_info.clone(),
                users[0].signing_key.clone(),
            ).unwrap_or_else(|e| panic!("Failed to build sync object: {:?}", e));

            crate::db::setup::put_join_setup(
                joining_node.app_state.db_pool.get(),
                sync_obj,
                joining_node.signing_key.clone(),
            ).unwrap_or_else(|e| panic!("Failed to setup node {}: {:?}", i, e));

            nodes.push(joining_node);
        }

        Self { nodes, users }
    }

    fn build_sync_object_for_joining_node(
        source_app_state: &AppState,
        joining_node: crate::db::Node,
        user_privkey: crate::db::PrivKey,
    ) -> Result<crate::setup::SyncSetupObject, crate::db::DatabaseError> {
        use crate::setup::{SyncSetupObject, ThisNode};
        use bincode::serde::decode_from_slice;

        let db = source_app_state.db_pool.get().map_err(|_| crate::db::DatabaseError::LockError)?;

        let users = crate::db::users::get_users(source_app_state.db_pool.get())
            .map_err(|e| { eprintln!("Failed to get users: {:?}", e); e })?;
        let nodes = crate::db::nodes::get_nodes(source_app_state.db_pool.get())
            .map_err(|e| { eprintln!("Failed to get nodes: {:?}", e); e })?;

        let sequences: Vec<crate::types::Sequence> = db.prepare("SELECT * FROM sequences")
            .and_then(|mut stmt| stmt.query_map([], |row| {
                Ok(crate::types::Sequence {
                    name: row.get(0)?,
                    next_id: row.get(1)?,
                })
            }).and_then(|rows| rows.collect()))
            .map_err(|e| { eprintln!("Failed to get sequences: {:?}", e); crate::db::DatabaseError::RecallError })?;

        let blocks: Vec<crate::consensus::types::Block> = db.prepare("SELECT block_hash, height, view_number, parent_hash, transactions FROM blocks")
            .and_then(|mut stmt| stmt.query_map([], |row| {
                let block_hash: crate::db::Blake3Hash = row.get(0)?;
                let height: i32 = row.get(1)?;
                let view_number: i32 = row.get(2)?;
                let parent_hash: Option<crate::db::Blake3Hash> = row.get(3)?;
                let transactions_blob: Option<Vec<u8>> = row.get(4)?;

                let transactions = match transactions_blob {
                    Some(blob) => {
                        match decode_from_slice(&blob, bincode::config::standard()) {
                            Ok((txs, _)) => Some(txs),
                            Err(_) => None,
                        }
                    }
                    None => None,
                };

                Ok(crate::consensus::types::Block {
                    block_hash,
                    data: crate::consensus::types::BlockData {
                        height,
                        view_number,
                        parent_hash,
                        transactions,
                    },
                })
            }).and_then(|rows| rows.collect()))
            .map_err(|e| { eprintln!("Failed to get blocks: {:?}", e); crate::db::DatabaseError::RecallError })?;

        let (current_phase, current_view, _prepared, committed_hash, highest_qc_hash): (ConsensusPhase, i32, Option<crate::db::Blake3Hash>, crate::db::Blake3Hash, crate::db::Blake3Hash) = db.query_row(
            "SELECT current_phase, current_view, prepared_block_hash, committed_block_hash, highest_qc_block_hash FROM this_node WHERE internal_id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
        ).map_err(|_| crate::db::DatabaseError::RecallError)?;

        let validators: Vec<crate::setup::Validator> = db.prepare("SELECT effective_height, node_id, is_active FROM validators")
            .and_then(|mut stmt| stmt.query_map([], |row| {
                Ok(crate::setup::Validator {
                    effective_height: row.get(0)?,
                    node_id: row.get(1)?,
                    is_active: row.get(2)?,
                })
            }).and_then(|rows| rows.collect()))
            .map_err(|e| { eprintln!("Failed to get validators: {:?}", e); crate::db::DatabaseError::RecallError })?;

        let qcs: Vec<crate::consensus::QuorumCertificate> = db.prepare("SELECT view_number, phase, block_hash, proposer_signature, voter_signatures FROM quorum_certificates")
            .and_then(|mut stmt| stmt.query_map([], |row| {
                Ok(crate::consensus::QuorumCertificate {
                    view_number: row.get(0)?,
                    phase: row.get(1)?,
                    block_hash: row.get(2)?,
                    proposer_signature: row.get(3)?,
                    voter_signatures: row.get(4)?,
                })
            }).and_then(|rows| rows.collect()))
            .map_err(|e| { eprintln!("Failed to get QCs: {:?}", e); crate::db::DatabaseError::ProcessingError })?;

        Ok(SyncSetupObject {
            users,
            nodes,
            sequences,
            blocks,
            validators,
            quorum_certificates: qcs,
            timeout_certificates: vec![],
            data_blocks: vec![],
            fragment_hashes: vec![],
            file_access_entries: vec![],
            inodes: vec![],
            takeouts: vec![],
            yournode: ThisNode {
                node_id: joining_node.node_id,
                current_phase,
                current_view,
                prepared_block_hash: None,
                committed_block_hash: committed_hash,
                highest_qc_block_hash: highest_qc_hash,
            },
            user_privkey,
        })
    }
}

pub fn create_test_app_state_with_keys(signing_key: crate::db::PrivKey, verifying_key: crate::db::PubKey) -> AppState {
    use r2d2::Pool;
    use duckdb::DuckdbConnectionManager;
    use std::sync::Arc;
    use once_cell::sync::OnceCell;
    use jsonwebtoken::{EncodingKey, DecodingKey};

    let manager = DuckdbConnectionManager::memory().unwrap();
    let pool = Pool::new(manager).unwrap();

    crate::db::shared::initialize(pool.get().unwrap()).unwrap();

    let jwt_secret = b"test_jwt_secret_key_for_testing_only";
    let encoding_key = EncodingKey::from_secret(jwt_secret);
    let decoding_key = DecodingKey::from_secret(jwt_secret);

    AppState {
        db_pool: pool,
        encoding_key,
        decoding_key,
        private_key: signing_key,
        public_key: verifying_key,
        node_id: Arc::new(OnceCell::new()),
        user_id: Arc::new(OnceCell::new()),
        user_keys: Arc::new(OnceCell::new()),
        siv_key: Arc::new(OnceCell::new()),
        siv_nonce: Arc::new(OnceCell::new()),
        fragments_dir: "/tmp/test_fragments".to_string(),
        timeout_vote_collector: Arc::new(crate::consensus::functions::TimeoutVoteCollector::new()),
        throughput_result_collector: Arc::new(crate::metrics::functions::ThroughputResultCollector::new()),
        last_observed_view: Arc::new(std::sync::atomic::AtomicI32::new(0)),
        fileprovider_api_key: "test_api_key".to_string(),
        port: 3000,
        test_mode: true,
    }
}

pub fn create_test_app_state() -> AppState {
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    create_test_app_state_with_keys(crate::db::PrivKey(signing_key), crate::db::PubKey(verifying_key))
}