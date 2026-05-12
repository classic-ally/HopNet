use super::*;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand_core::OsRng; // Import from parent consensus module

mod authorization;
mod byzantine;
mod quorum;
mod signatures;

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
        let nodes = (0..num_nodes as i32).map(|id| MockNode::new(id)).collect();

        let users = (0..num_users as i32).map(|id| MockUser::new(id)).collect();

        Self { nodes, users }
    }

    pub fn setup_with_validators(num_validators: usize) -> Self {
        let mut nodes = Vec::new();
        let users = vec![MockUser::new(0)];

        // Set up first node using genesis block
        let first_node = MockNode::new(0);
        let x25519_pubkey = crate::auth::derive_x25519_pubkey_from_user(&users[0].signing_key);

        let (encrypted_privkey, key_salt) =
            crate::auth::wrap_user_privkey(&users[0].signing_key, "password")
                .expect("Failed to wrap test user privkey");
        let user = crate::types::User::new(
            0,
            "test_user".to_string(),
            users[0].verifying_key,
            x25519_pubkey,
            encrypted_privkey,
            key_salt,
        );

        let first_db_node = crate::db::Node {
            node_id: 0,
            name: "node_0".to_string(),
            owner: 0,
            pubkey: first_node.verifying_key,
        };

        let (user_id, node_id) =
            crate::db::setup::post_initial_setup(&first_node.app_state, user, first_db_node)
                .expect("Failed to run initial setup for first node");

        // Set node_id and user_id in app_state
        first_node
            .app_state
            .node_id
            .set(node_id)
            .expect("Failed to set node_id");
        first_node
            .app_state
            .user_id
            .set(user_id)
            .expect("Failed to set user_id");

        nodes.push(first_node);

        // Add additional nodes
        for i in 1..num_validators {
            let joining_node = MockNode::new(i as i32);

            let joining_node_info = crate::db::Node {
                node_id: i as i32,
                name: format!("node_{}", i),
                owner: 0,
                pubkey: joining_node.verifying_key,
            };

            // Insert new node info into all existing nodes' databases
            for j in 0..i {
                let existing_node_db = nodes[j as usize]
                    .app_state
                    .db_pool
                    .get()
                    .expect("Failed to get existing node DB");

                existing_node_db
                    .execute(
                        "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, ?, ?)",
                        rusqlite::params![
                            i as i32,
                            format!("node_{}", i),
                            0,
                            joining_node.verifying_key
                        ],
                    )
                    .expect("Failed to insert joining node into existing node");

                existing_node_db.execute(
                    "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
                    rusqlite::params![0, i as i32, true]
                ).expect("Failed to insert validator into existing node");

                existing_node_db
                    .execute(
                        "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'nodes'",
                        [],
                    )
                    .expect("Failed to update node sequence");
            }

            // Copy database state from first node to joining node
            eprintln!("setup_with_validators: About to sync state for node {}", i);
            Self::sync_node_state(
                &nodes[0].app_state,
                &joining_node.app_state,
                &joining_node_info,
            )
            .expect(&format!("Failed to sync state to node {}", i));

            // Set the node_id and user_id in the app_state (required for Block::new_tip)
            joining_node
                .app_state
                .node_id
                .set(i as i32)
                .expect("Failed to set node_id");
            joining_node
                .app_state
                .user_id
                .set(0) // All nodes owned by user 0
                .expect("Failed to set user_id");

            nodes.push(joining_node);
        }

        Self { nodes, users }
    }

    /// Copy all database state from source to destination for test setup
    fn sync_node_state(
        source: &AppState,
        dest: &AppState,
        joining_node: &crate::db::Node,
    ) -> Result<(), crate::db::DatabaseError> {
        use bincode::serde::{decode_from_slice, encode_to_vec};

        eprintln!(
            "sync_node_state: Starting sync for node {}",
            joining_node.node_id
        );
        let source_db = source.db_pool.get().map_err(|e| {
            eprintln!("Failed to get source DB: {:?}", e);
            crate::db::DatabaseError::LockError
        })?;
        let dest_db = dest.db_pool.get().map_err(|e| {
            eprintln!("Failed to get dest DB: {:?}", e);
            crate::db::DatabaseError::LockError
        })?;

        // Copy users
        eprintln!("sync_node_state: Copying users...");
        source_db.prepare("SELECT user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt FROM users")
            .and_then(|mut stmt| {
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, crate::db::PubKey>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, Vec<u8>>(5)?
                    ))
                })?;
                for row in rows {
                    let (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt) = row?;
                    dest_db.execute(
                        "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt) VALUES (?, ?, ?, ?, ?, ?)",
                        rusqlite::params![user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt]
                    )?;
                }
                Ok(())
            })
            .map_err(|e| {
                eprintln!("Failed to copy users: {:?}", e);
                crate::db::DatabaseError::ProcessingError
            })?;
        eprintln!("sync_node_state: Users copied successfully");

        // Copy nodes
        eprintln!("sync_node_state: Copying nodes...");
        source_db
            .prepare("SELECT node_id, name, owner, pubkey FROM nodes")
            .and_then(|mut stmt| {
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i32>(2)?,
                        row.get::<_, crate::db::PubKey>(3)?,
                    ))
                })?;
                for row in rows {
                    let (node_id, name, owner, pubkey) = row?;
                    dest_db.execute(
                        "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, ?, ?)",
                        rusqlite::params![node_id, name, owner, pubkey],
                    )?;
                }
                Ok(())
            })
            .map_err(|e| {
                eprintln!("Failed to copy nodes: {:?}", e);
                crate::db::DatabaseError::ProcessingError
            })?;
        eprintln!("sync_node_state: Nodes copied successfully");

        // Copy sequences
        eprintln!("sync_node_state: Copying sequences...");
        source_db
            .prepare("SELECT name, next_id FROM sequences")
            .and_then(|mut stmt| {
                let rows = stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
                })?;
                for row in rows {
                    let (name, next_id) = row?;
                    dest_db.execute(
                        "UPDATE sequences SET next_id = ? WHERE name = ?",
                        rusqlite::params![next_id, name],
                    )?;
                }
                Ok(())
            })
            .map_err(|e| {
                eprintln!("Failed to copy sequences: {:?}", e);
                crate::db::DatabaseError::ProcessingError
            })?;
        eprintln!("sync_node_state: Sequences copied successfully");

        // Copy validators
        source_db.prepare("SELECT effective_height, node_id, is_active FROM validators")
            .and_then(|mut stmt| {
                let rows = stmt.query_map([], |row| {
                    Ok((row.get::<_, i32>(0)?, row.get::<_, i32>(1)?, row.get::<_, bool>(2)?))
                })?;
                for row in rows {
                    let (effective_height, node_id, is_active) = row?;
                    dest_db.execute(
                        "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
                        rusqlite::params![effective_height, node_id, is_active]
                    )?;
                }
                Ok(())
            })
            .map_err(|e| {
                eprintln!("Failed to copy validators: {:?}", e);
                crate::db::DatabaseError::ProcessingError
            })?;
        eprintln!("sync_node_state: Validators copied successfully");

        // Copy blocks
        eprintln!("sync_node_state: Copying blocks...");
        source_db.prepare("SELECT block_hash, height, view_number, parent_hash, transactions FROM blocks")
            .and_then(|mut stmt| {
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, crate::db::Blake3Hash>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, i32>(2)?,
                        row.get::<_, Option<crate::db::Blake3Hash>>(3)?,
                        row.get::<_, Option<Vec<u8>>>(4)?
                    ))
                })?;
                for row in rows {
                    let (block_hash, height, view_number, parent_hash, transactions) = row?;
                    dest_db.execute(
                        "INSERT INTO blocks (block_hash, height, view_number, parent_hash, transactions) VALUES (?, ?, ?, ?, ?)",
                        rusqlite::params![block_hash, height, view_number, parent_hash, transactions]
                    )?;
                }
                Ok(())
            })
            .map_err(|e| {
                eprintln!("Failed to copy blocks: {:?}", e);
                crate::db::DatabaseError::ProcessingError
            })?;
        eprintln!("sync_node_state: Blocks copied successfully");

        // Copy quorum certificates
        eprintln!("sync_node_state: Copying QCs...");
        source_db.prepare("SELECT view_number, phase, block_hash, proposer_signature, voter_signatures FROM quorum_certificates")
            .and_then(|mut stmt| {
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, ConsensusPhase>(1)?,
                        row.get::<_, crate::db::Blake3Hash>(2)?,
                        row.get::<_, Vec<u8>>(3)?,  // proposer_signature stored as blob
                        row.get::<_, Vec<u8>>(4)?   // voter_signatures stored as blob
                    ))
                })?;
                for row in rows {
                    let (view_number, phase, block_hash, proposer_signature, voter_signatures) = row?;
                    dest_db.execute(
                        "INSERT INTO quorum_certificates (view_number, phase, block_hash, proposer_signature, voter_signatures) VALUES (?, ?, ?, ?, ?)",
                        rusqlite::params![view_number, phase, block_hash, proposer_signature, voter_signatures]
                    )?;
                }
                Ok(())
            })
            .map_err(|e| {
                eprintln!("Failed to copy QCs: {:?}", e);
                crate::db::DatabaseError::ProcessingError
            })?;
        eprintln!("sync_node_state: QCs copied successfully");

        // Copy timeout certificates
        eprintln!("sync_node_state: Copying TCs...");
        source_db.prepare("SELECT view_number, signatures, highest_qc_phase, highest_qc_block_hash, highest_qc_view FROM timeout_certificates")
            .and_then(|mut stmt| {
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, ConsensusPhase>(2)?,
                        row.get::<_, crate::db::Blake3Hash>(3)?,
                        row.get::<_, i32>(4)?
                    ))
                })?;
                for row in rows {
                    let (view_number, signatures, highest_qc_phase, highest_qc_block_hash, highest_qc_view) = row?;
                    dest_db.execute(
                        "INSERT INTO timeout_certificates (view_number, signatures, highest_qc_phase, highest_qc_block_hash, highest_qc_view) VALUES (?, ?, ?, ?, ?)",
                        rusqlite::params![view_number, signatures, highest_qc_phase, highest_qc_block_hash, highest_qc_view]
                    )?;
                }
                Ok(())
            })
            .map_err(|e| {
                eprintln!("Failed to copy TCs: {:?}", e);
                crate::db::DatabaseError::ProcessingError
            })?;
        eprintln!("sync_node_state: TCs copied successfully");

        // Set up this_node table for the joining node
        eprintln!("sync_node_state: Copying this_node state...");
        let (
            current_phase,
            current_view,
            last_timeout_vote_view,
            last_propose_vote_block_hash,
            prepared,
            committed,
            highest_qc,
            highest_qc_phase
        ): (
            ConsensusPhase,
            i32,
            Option<i32>,
            Option<crate::db::Blake3Hash>,
            Option<crate::db::Blake3Hash>,
            Option<crate::db::Blake3Hash>,
            Option<crate::db::Blake3Hash>,
            Option<ConsensusPhase>
        ) = source_db.query_row(
            "SELECT current_phase, current_view, last_timeout_vote_view, last_propose_vote_block_hash, prepared_block_hash, committed_block_hash, highest_qc_block_hash, highest_qc_phase FROM this_node WHERE internal_id = 1",
            [],
            |row| Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?
            ))
        ).map_err(|e| {
            eprintln!("Failed to read this_node from source: {:?}", e);
            crate::db::DatabaseError::RecallError
        })?;

        // INSERT the this_node row (dest DB only has schema, no data yet)
        // Use joining_node's specific node_id and privkey, but copy consensus state from source
        dest_db.execute(
            "INSERT INTO this_node (internal_id, node_id, privkey, current_phase, current_view, last_timeout_vote_view, last_propose_vote_block_hash, prepared_block_hash, committed_block_hash, highest_qc_block_hash, highest_qc_phase) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                joining_node.node_id,
                dest.private_key,  // Use joining node's own private key
                current_phase,
                current_view,
                last_timeout_vote_view,
                last_propose_vote_block_hash,
                prepared,
                committed,
                highest_qc,
                highest_qc_phase
            ]
        ).map_err(|e| {
            eprintln!("Failed to insert this_node in dest: {:?}", e);
            crate::db::DatabaseError::ProcessingError
        })?;
        eprintln!("sync_node_state: this_node inserted successfully");
        eprintln!("sync_node_state: Sync complete!");

        Ok(())
    }
}

pub fn create_test_app_state_with_keys(
    signing_key: crate::db::PrivKey,
    verifying_key: crate::db::PubKey,
) -> AppState {
    use jsonwebtoken::{DecodingKey, EncodingKey};
    use once_cell::sync::OnceCell;
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;
    use std::sync::Arc;

    let manager = SqliteConnectionManager::memory();
    let pool = Pool::builder()
        .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
        .build(manager)
        .unwrap();

    crate::db::shared::initialize(pool.get().unwrap()).unwrap();

    let jwt_secret = b"test_jwt_secret_key_for_testing_only";
    let encoding_key = EncodingKey::from_secret(jwt_secret);
    let decoding_key = DecodingKey::from_secret(jwt_secret);

    let iroh_secret = signing_key.to_iroh_secret_key();
    let iroh_transport = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(crate::net::IrohTransport::new(
            iroh_secret,
            pool.clone(),
            true,
        ))
        .expect("test iroh transport");

    let (consensus_queue, _consensus_queue_rx) =
        crate::consensus::queue::ConsensusQueue::new(pool.clone(), 256);

    AppState {
        db_pool: pool,
        encoding_key,
        decoding_key,
        private_key: signing_key,
        public_key: verifying_key,
        node_id: Arc::new(OnceCell::new()),
        user_id: Arc::new(OnceCell::new()),
        fragments_dir: "/tmp/test_fragments".to_string(),
        timeout_vote_collector: Arc::new(crate::consensus::functions::TimeoutVoteCollector::new()),
        catch_up_state: Arc::new(crate::consensus::catch_up_state::CatchUpState::new()),
        consensus_lock: Arc::new(tokio::sync::Mutex::new(())),
        port: 3000,
        test_mode: true,
        orphaned_fragment_scan: Arc::new(std::sync::Mutex::new(None)),
        iroh_transport,
        consensus_barriers: Arc::new(crate::consensus::barriers::new()),
        dedup_cache: Arc::new(crate::net::DedupCache::default()),
        lock_vote_evidence: Arc::new(std::sync::Mutex::new(None)),
        session_store: Arc::new(crate::auth::SessionStore::default()),
        takeout_runtime: Arc::new(crate::takeout::TakeoutRuntime::default()),
        consensus_queue,
        view_changed: Arc::new(tokio::sync::Notify::new()),
        write_gate: Arc::new(crate::db::write_gate::WriteGate::new()),
        local_state_tx: tokio::sync::mpsc::channel(1).0,
    }
}

pub fn create_test_app_state() -> AppState {
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    create_test_app_state_with_keys(
        crate::db::PrivKey(signing_key),
        crate::db::PubKey(verifying_key),
    )
}
