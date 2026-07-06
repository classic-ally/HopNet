use super::*;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rand_core::UnwrapErr;
use rand::rngs::SysRng;

mod authorization;
mod byzantine;
mod malachite_integration;
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
        let signing_key = SigningKey::generate(&mut UnwrapErr(SysRng));
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
        let signing_key = SigningKey::generate(&mut UnwrapErr(SysRng));
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
        let nodes = (0..num_nodes as i32).map(MockNode::new).collect();

        let users = (0..num_users as i32).map(MockUser::new).collect();

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
            for existing_node in &nodes[..i] {
                let existing_node_db = existing_node
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
            .unwrap_or_else(|_| panic!("Failed to sync state to node {}", i));

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

        // this_node identity for the joining node (its own key)
        dest_db
            .execute(
                "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (1, ?, ?)",
                rusqlite::params![joining_node.node_id, dest.private_key],
            )
            .map_err(|e| {
                eprintln!("Failed to insert this_node in dest: {:?}", e);
                crate::db::DatabaseError::ProcessingError
            })?;
        eprintln!("sync_node_state: this_node inserted successfully");

        // Malachite engine tables: replicate the genesis pair + meta the way
        // a real joiner's height-0 bootstrap would (decided_blocks,
        // decided_certificates, consensus_meta). Without these the joining
        // node's decided tip is empty and parent-linkage validation rejects
        // every block the genesis node proposes.
        {
            let copy_table = |sql_select: &str, sql_insert: &str| -> Result<(), crate::db::DatabaseError> {
                let mut stmt = source_db.prepare(sql_select).map_err(|e| {
                    eprintln!("Failed to prepare {sql_select}: {e:?}");
                    crate::db::DatabaseError::RecallError
                })?;
                let rows: Vec<Vec<rusqlite::types::Value>> = stmt
                    .query_map([], |row| {
                        let n = row.as_ref().column_count();
                        (0..n).map(|i| row.get::<_, rusqlite::types::Value>(i)).collect()
                    })
                    .map_err(|_| crate::db::DatabaseError::RecallError)?
                    .flatten()
                    .collect();
                for values in rows {
                    dest_db
                        .execute(sql_insert, rusqlite::params_from_iter(values))
                        .map_err(|e| {
                            eprintln!("Failed to copy row via {sql_insert}: {e:?}");
                            crate::db::DatabaseError::ProcessingError
                        })?;
                }
                Ok(())
            };
            copy_table(
                "SELECT height, block_hash, round, block FROM decided_blocks",
                "INSERT OR REPLACE INTO decided_blocks (height, block_hash, round, block) VALUES (?, ?, ?, ?)",
            )?;
            copy_table(
                "SELECT height, block_hash, round, certificate FROM decided_certificates",
                "INSERT OR REPLACE INTO decided_certificates (height, block_hash, round, certificate) VALUES (?, ?, ?, ?)",
            )?;
            copy_table(
                "SELECT key, value FROM consensus_meta",
                "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES (?, ?)",
            )?;
            eprintln!("sync_node_state: malachite tables copied successfully");
        }

        eprintln!("sync_node_state: Sync complete!");

        Ok(())
    }
}

/// One shared runtime for every test transport: the endpoint's internal
/// actors are spawned on the runtime that creates it, so a throwaway runtime
/// would kill them the moment it drops (any later dial then fails with
/// RemoteStateActorStopped). Tests that DIAL these endpoints should drive
/// their futures on this same runtime.
pub fn test_iroh_rt() -> &'static tokio::runtime::Runtime {
    static TEST_IROH_RT: once_cell::sync::Lazy<tokio::runtime::Runtime> =
        once_cell::sync::Lazy::new(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("test iroh runtime")
        });
    &TEST_IROH_RT
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
    let iroh_transport = test_iroh_rt()
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
        port: 3000,
        test_mode: true,
        orphaned_fragment_scan: Arc::new(std::sync::Mutex::new(None)),
        iroh_transport,
        consensus_barriers: Arc::new(crate::consensus::barriers::new()),
        dedup_cache: Arc::new(crate::net::DedupCache::default()),
        session_store: Arc::new(crate::auth::SessionStore::default()),
        takeout_runtime: Arc::new(crate::takeout::TakeoutRuntime::default()),
        consensus_queue,
        write_gate: Arc::new(crate::db::write_gate::WriteGate::new()),
        local_state_tx: tokio::sync::mpsc::channel(1).0,
        malachite: Arc::new(once_cell::sync::OnceCell::new()),
        placement_batch_tx: Arc::new(once_cell::sync::OnceCell::new()),
    }
}

pub fn create_test_app_state() -> AppState {
    let signing_key = SigningKey::generate(&mut UnwrapErr(SysRng));
    let verifying_key = signing_key.verifying_key();
    create_test_app_state_with_keys(
        crate::db::PrivKey(signing_key),
        crate::db::PubKey(verifying_key),
    )
}
