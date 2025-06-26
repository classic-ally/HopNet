use argon2::PasswordVerifier;
use duckdb::{params, Connection, Error, types::ValueRef};
use ed25519_dalek::SigningKey;
use reqwest::StatusCode;
use tokio::sync::oneshot;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime,Duration};
use serde::{Serialize,Deserialize};
use chrono::{DateTime, Utc};

use argon2::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash, PasswordHasher, SaltString
    },
    Argon2
};

use crate::setup::{self, SyncSetupObject, ThisNode};
use crate::types::{Block, BlockData, Node};

pub enum DatabaseError {
    LockError,
    InsertError,
    RecordError,
    RecallError,
    ProcessingError
}
#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub struct Metric {
    pub from_node: i32,
    pub to_node: i32,
    pub start_time: SystemTime,
    pub duration: Duration,
    pub rtt_latency: Option<f64>,
    pub rtt_variance: Option<f64>,
    pub rtt_jitter: Option<f64>,
    pub throughput: Option<i64>,
    pub version: u8
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Sequence {
    pub name: String,
    pub next_id: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct User {
    pub user_id: i32,
    pub username: String,
    pub password: String,
}

impl User {
    pub fn password_hash(&mut self) -> Result<String, argon2::password_hash::Error> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let password_hash = argon2.hash_password(self.password.as_bytes(), &salt)?.to_string();
        Ok(password_hash)
    }
    pub fn verify_password(&mut self, check_password: &[u8]) -> Result<bool, argon2::password_hash::Error> {
        let parsed_hash = PasswordHash::new(&self.password)?;
        return Ok(Argon2::default().verify_password(check_password, &parsed_hash).is_ok());
    }
}

pub fn initialize() -> Result<Arc<Mutex<Connection>>, Error> {
    let db = Connection::open(":memory:")?;
    db.execute_batch(
        "
            CREATE TABLE sequences (
                name            TEXT PRIMARY KEY,
                next_id         INTEGER NOT NULL
            );

            CREATE TABLE users (
                user_id         INTEGER PRIMARY KEY,
                username        VARCHAR NOT NULL,
                password_hash   VARCHAR NOT NULL,

                CONSTRAINT unique_username UNIQUE (username)
            );

            CREATE TABLE nodes (
                node_id         INTEGER PRIMARY KEY,
                name            VARCHAR NOT NULL,
                ip_address      VARCHAR NOT NULL,
                port            INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
                owner           INTEGER NOT NULL,
                pubkey          BLOB NOT NULL,

                -- CONSTRAINT enables indexed lookup of these
                CONSTRAINT unique_endpoint UNIQUE (ip_address, port),

                FOREIGN KEY (owner) REFERENCES users(user_id)
            );

            -- Common query patterns: 
            -- 1. user owns what nodes?
            CREATE INDEX idx_nodes_owner ON nodes(owner);
            -- 2. what node is this IP? (enabled by CONSTRAINT)

            -- Consensus architecture
            CREATE TABLE blocks (
                block_hash      BLOB PRIMARY KEY,
                height          INTEGER NOT NULL,
                view_number     INTEGER NOT NULL,
                parent_hash     BLOB,
                transactions    BLOB,

                CONSTRAINT fk_parent_exists FOREIGN KEY (parent_hash) REFERENCES blocks(block_hash)
            );

            -- Common query patterns:
            -- 1. Give me latest blocks, most recent few
            -- 2. Give me blocks for a given view
            -- 3. Look up parent of a block
            CREATE INDEX idx_blocks_height ON blocks(height DESC);
            CREATE INDEX idx_blocks_view ON blocks(view_number);
            CREATE INDEX idx_blocks_parent ON blocks(parent_hash);

            CREATE TABLE quorum_certificates (
                view_number         INTEGER NOT NULL,
                phase               ENUM('propose', 'vote') NOT NULL,
                block_hash          BLOB NOT NULL,
                proposer_signature  BLOB NOT NULL,
                voter_signatures    BLOB,

                PRIMARY KEY (view_number, phase, block_hash),
                FOREIGN KEY (block_hash) REFERENCES blocks(block_hash)
            );

            CREATE INDEX idx_qc_block ON quorum_certificates(block_hash);
            CREATE INDEX idx_view_phase ON quorum_certificates(view_number, phase);

            -- Track validators that are acceptable at any given time
            -- Not using views (nodes can be in different views due to network partitions)
            -- Not using timestamps (time sync requirement)
            -- Using height (deterministic, directly tied to the block being committed)
            CREATE TABLE validators (
                effective_height    INTEGER NOT NULL,   -- Height when this validator changes state
                node_id             INTEGER NOT NULL,
                is_active           BOOLEAN NOT NULL,

                PRIMARY KEY (effective_height, node_id),
                FOREIGN KEY (node_id) REFERENCES nodes(node_id)
            );

            -- Common query patterns:
            -- 1. Give me current validators (e.g. latest effective height for leave/rejoin)
            -- 2. For consensus rebuild, give me nodes active at a given height
            CREATE INDEX idx_validator_height ON validators(effective_height DESC); 
            CREATE INDEX idx_validator_active ON validators(effective_height, is_active);

            CREATE TABLE this_node (
                internal_id             INTEGER PRIMARY KEY DEFAULT 1,
                node_id                 INTEGER NOT NULL UNIQUE,
                privkey                 BLOB NOT NULL,

                -- Consensus mechanics
                -- View stored in case of leader change without block written
                -- Block height not stored -> always computable
                current_phase           ENUM('propose', 'vote') NOT NULL DEFAULT 'propose',
                current_view            INTEGER NOT NULL DEFAULT 0,
                -- Block is prepared when it has a QC
                prepared_block_hash     BLOB,
                -- HotStuff-2 efficiency improvement:
                -- Block is committed when we're working on a later block
                -- (Working on block n+1 implies we commit block n)
                committed_block_hash    BLOB NOT NULL,
                -- Safety: track highest QC seen (highest view for ordered execution)
                highest_qc_block_hash   BLOB NOT NULL,

                FOREIGN KEY (prepared_block_hash) REFERENCES blocks(block_hash),
                FOREIGN KEY (committed_block_hash) REFERENCES blocks(block_hash),
                FOREIGN KEY (highest_qc_block_hash) REFERENCES blocks(block_hash),

                FOREIGN KEY (node_id) REFERENCES nodes(node_id)
            );

            CREATE TABLE metrics (
                from_node       INTEGER NOT NULL,
                to_node         INTEGER NOT NULL,
                start_time      TIMESTAMP NOT NULL,
                duration        SMALLINT NOT NULL,
                rtt_latency     REAL,
                rtt_variance    REAL,
                rtt_jitter      REAL,
                throughput      BIGINT,
                version         TINYINT NOT NULL DEFAULT 0,
                
                PRIMARY KEY     (from_node, to_node, start_time),
                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node)   REFERENCES nodes(node_id)
            );

            -- Create indexes for common query patterns
            CREATE INDEX idx_metrics_time_range ON metrics(start_time, from_node, to_node);
            CREATE INDEX idx_metrics_from_node ON metrics(from_node, start_time);
            CREATE INDEX idx_metrics_to_node ON metrics(to_node, start_time);

            -- Add comments for documentation
            COMMENT ON TABLE metrics IS 'Network performance metrics between distributed system nodes';
            COMMENT ON COLUMN metrics.duration IS 'Measurement duration in milliseconds (max ~32 seconds)';
            COMMENT ON COLUMN metrics.rtt_latency IS 'Round-trip time latency in milliseconds';
            COMMENT ON COLUMN metrics.rtt_variance IS 'RTT variance in milliseconds';
            COMMENT ON COLUMN metrics.rtt_jitter IS 'RTT jitter in milliseconds';
            COMMENT ON COLUMN metrics.throughput IS 'Network throughput in bytes per second';
            COMMENT ON COLUMN metrics.version IS 'Schema version for backwards compatibility';
        "
    )?;
    Ok(Arc::new(Mutex::new(db)))
}

pub fn get_initial_setup(
    db: &Arc<Mutex<Connection>>
) -> Result<StatusCode, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            // if there is entry in the this_node table, we're set up
            let count = db_lock.query_row(
                "SELECT COUNT(*) FROM this_node",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;

            if count > 0 {
                return Ok(StatusCode::OK);
            } else {
                return Ok(StatusCode::NOT_FOUND);
            }
        },
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn post_initial_setup(
    db: &Arc<Mutex<Connection>>,
    mut user: User,
    node: Node,
    pubkey: &[u8],
    privkey: &[u8]
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            // initialize counters
            tx.execute_batch("
                INSERT INTO sequences (name, next_id) VALUES ('users', 0);
                INSERT INTO sequences (name, next_id) VALUES ('nodes', 0);
            ").map_err(|_| DatabaseError::InsertError)?;

            // compute user password
            let password_hash = user.password_hash().map_err(|_| DatabaseError::ProcessingError)?;

            // insert the user first
            let next_user_id = tx.query_row(
                "SELECT next_id FROM sequences WHERE name = 'users'",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;
            tx.execute(
                "INSERT INTO users (user_id, username, password_hash) VALUES (?, ?, ?)",
                params![next_user_id, user.username, password_hash]
            ).map_err(|_| DatabaseError::InsertError)?;
            // Update the sequence for next user
            tx.execute(
                "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'users'",
                []
            ).map_err(|_| DatabaseError::InsertError)?;

            // insert the node
            let next_node_id = tx.query_row(
                "SELECT next_id FROM sequences WHERE name = 'nodes'",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;
            tx.execute(
                "INSERT INTO nodes (node_id, name, ip_address, port, owner, pubkey) VALUES (?, ?, ?, ?, ?, ?)",
                params![next_node_id, node.name, node.ip_address, node.port, next_user_id, pubkey]
            ).map_err(|_| DatabaseError::InsertError)?;

            // Update sequence
            tx.execute(
                "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'nodes'", 
                []
            ).map_err(|_| DatabaseError::InsertError)?;

            // create genesis block for database
            let genesis_block = Block::new(
                BlockData {
                    height: 0,
                    view_number: 0,
                    parent_hash: None,
                    transactions: None,
                }
            ).map_err(|_| DatabaseError::ProcessingError)?;

            tx.execute(
                "INSERT INTO blocks (block_hash, height, view_number) VALUES (?, ?, ?)",
                params![genesis_block.block_hash.as_bytes(), genesis_block.data.height, genesis_block.data.view_number]
            ).map_err(|_| DatabaseError::InsertError)?;

            // mark myself as a validator from view 0
            tx.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
                params![0, next_node_id, true]
            ).map_err(|_| DatabaseError::InsertError)?;

            // also write this node so we know setup is completed
            tx.execute(
                "INSERT INTO this_node (internal_id, node_id, privkey, committed_block_hash, highest_qc_block_hash) VALUES (?, ?, ?, ?, ?)",
                params![1, next_node_id, privkey, genesis_block.block_hash.as_bytes(), genesis_block.block_hash.as_bytes()]
            ).map_err(|_| DatabaseError::InsertError)?;

            // Commit the transaction
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            
            dbg!("Successfully inserted setup info");

            Ok(())

        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn put_join_setup(
    db: &Arc<Mutex<Connection>>,
    setupobj: crate::setup::SyncSetupObject,
    privkey: &[u8]
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            // in this case we need to write the list of nodes and users to the DB
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            dbg!("Inserting users");
            for user in setupobj.users {
                tx.execute(
                    "INSERT INTO users (user_id, username, password_hash) VALUES (?, ?, ?)",
                    params![user.user_id, user.username, user.password]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting nodes");
            for node in setupobj.nodes {
                tx.execute(
                    "INSERT INTO nodes (node_id, name, ip_address, port, owner, pubkey) VALUES (?, ?, ?, ?, ?, ?)",
                    params![node.node_id, node.name, node.ip_address, node.port, node.owner, node.pubkey.as_bytes()]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting sequences");
            for sequence in setupobj.sequences {
                tx.execute(
                    "INSERT INTO sequences (name, next_id) VALUES (?, ?)",
                    params![sequence.name, sequence.next_id]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting blocks");
            for block in setupobj.blocks {
                // Encode transactions to blob if present
                let transactions_blob = match &block.data.transactions {
                    Some(transactions) => {
                        match bincode::encode_to_vec(transactions, bincode::config::standard()) {
                            Ok(blob) => Some(blob),
                            Err(_) => return Err(DatabaseError::ProcessingError),
                        }
                    }
                    None => None,
                };

                // Convert parent_hash to bytes if present
                let parent_hash_bytes = block.data.parent_hash
                    .map(|hash| hash.as_bytes().to_vec());

                tx.execute(
                    "INSERT INTO blocks (block_hash, height, view_number, parent_hash, transactions) VALUES (?, ?, ?, ?, ?)",
                    params![
                        block.block_hash.as_bytes(),
                        block.data.height,
                        block.data.view_number,
                        parent_hash_bytes,
                        transactions_blob
                    ]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            // also write to this_node table so we know setup is completed
            dbg!("Preparing for this_node");
            
            // Convert consensus phase to string for database storage
            let phase_str = match setupobj.yournode.current_phase {
                crate::consensus::ConsensusPhase::Propose => "propose",
                crate::consensus::ConsensusPhase::Vote => "vote",
            };
            
            // Convert optional prepared block hash to bytes
            let prepared_block_bytes = setupobj.yournode.prepared_block_hash
                .map(|hash| hash.as_bytes().to_vec());
            
            dbg!("Inserting this_node");
            tx.execute(
                "INSERT INTO this_node (internal_id, node_id, privkey, current_phase, current_view, prepared_block_hash, committed_block_hash, highest_qc_block_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    1,
                    setupobj.yournode.node_id,
                    privkey,
                    phase_str,
                    setupobj.yournode.current_view,
                    prepared_block_bytes,
                    setupobj.yournode.committed_block_hash.as_bytes(),
                    setupobj.yournode.highest_qc_block_hash.as_bytes()
                ]
            ).map_err(|_| DatabaseError::InsertError)?;

            dbg!("TX Commit");
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn insert_metric(
    db: &Arc<Mutex<Connection>>,
    metric: Metric,
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            // Convert SystemTime to DateTime<Utc>
            let start_time_utc: DateTime<Utc> = match metric.start_time {
                SystemTime::UNIX_EPOCH => Utc::now(), // fallback
                _ => match metric.start_time.duration_since(SystemTime::UNIX_EPOCH) {
                    Ok(dur) => DateTime::<Utc>::from(
                        SystemTime::UNIX_EPOCH + dur
                    ),
                    Err(_) => return Err(DatabaseError::RecordError),
                }
            };

            // Convert to ISO string or Unix timestamp in seconds
            let start_time_str = start_time_utc.to_rfc3339(); // "2025-04-16T12:00:00Z"

            let duration_ms = metric.duration.as_millis() as i32;

            dbg!("Attempting to place metric into db");
            let result = db_lock.execute(
                "INSERT INTO metrics (from_node, to_node, start_time, duration, rtt_latency, rtt_variance, rtt_jitter, throughput, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    metric.from_node,
                    metric.to_node,
                    start_time_str,
                    duration_ms,
                    metric.rtt_latency,
                    metric.rtt_variance,
                    metric.rtt_jitter,
                    metric.throughput,
                    metric.version,
                ]
            );
            match result {
                Ok(_) => {
                    dbg!("Successfully placed metric into db");
                    Ok(())
                }
                Err(e) => {
                    dbg!(e);
                    Err(DatabaseError::InsertError)
                }
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_metric(
    db: &Arc<Mutex<Connection>>,
) -> Result<Vec<Metric>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            // Prepare the query
            let mut stmt = db_lock.prepare("SELECT * FROM metrics").map_err(|_| DatabaseError::RecallError)?;
            // Execute the query and map each row to a Metric
            let results = stmt.query_map([], |row| {
                let from_node: i32 = row.get(0)?;
                let to_node: i32 = row.get(1)?;
                
                // Convert start_time from nanoseconds to SystemTime
                let timestamp_nanos: i64 = row.get(2)?;
                
                // Convert duration from milliseconds to Duration
                let duration_ms: i32 = row.get(3)?;
                
                Ok(Metric {
                    from_node,
                    to_node,
                    start_time: SystemTime::UNIX_EPOCH + Duration::from_nanos(timestamp_nanos as u64),
                    duration: Duration::from_millis(duration_ms as u64),
                    rtt_latency: row.get(4)?,
                    rtt_variance: row.get(5)?,
                    rtt_jitter: row.get(6)?,
                    throughput: row.get(7)?,
                    version: row.get(8)?,
                })
            });

            match results {
                Ok(metrics) => Ok(metrics.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?), // collect into Vec
                Err(e) => {
                    dbg!(e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            dbg!(e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn insert_user(
    db: &Arc<Mutex<Connection>>,
    mut user: User,
) -> Result<(), DatabaseError> {

    match db.lock() {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;
            let next_id = tx.query_row(
                "SELECT next_id FROM sequences WHERE name = 'users'",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;

            let password_hash = user.password_hash().map_err(|_| DatabaseError::ProcessingError)?;

            tx.execute(
                "INSERT INTO users (user_id, username, password_hash) VALUES (?, ?, ?)",
                params![next_id, user.username, password_hash]
            ).map_err(|_| DatabaseError::InsertError)?;
            
            // Update the sequence for next user
            tx.execute(
                "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'users'",
                []
            ).map_err(|_| DatabaseError::InsertError)?;
            
            // Commit the transaction
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            
            dbg!("Successfully inserted user");
            Ok(())
        },
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn get_users(
    db: &Arc<Mutex<Connection>>,
) -> Result<Vec<User>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare("SELECT * FROM users").map_err(|_| DatabaseError::RecallError)?;
            let results = stmt.query_map([], |row| {
                Ok(User {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    password: row.get(2)?,
                })
            });

            match results {
                Ok(users) => Ok(users.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?),
                Err(e) => {
                    dbg!(e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            dbg!(e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn get_user_by_username(
    db: &Arc<Mutex<Connection>>,
    username: String,
) -> Result<Option<User>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT * FROM users WHERE username = ?"
            ).map_err(|_|DatabaseError::RecallError)?;

            let mut rows = stmt.query(&[&username]).map_err(|_|DatabaseError::RecallError)?;

            if let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
                let user = User {
                    user_id: row.get(0).map_err(|_| DatabaseError::RecallError)?,
                    username: row.get(1).map_err(|_| DatabaseError::RecallError)?,
                    password: row.get(2).map_err(|_| DatabaseError::RecallError)?
                };
                return Ok(Some(user))
            } else {
                return Ok(None)
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_user_by_userid(
    db: &Arc<Mutex<Connection>>,
    userid: i32,
) -> Result<Option<User>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT * FROM users WHERE user_id = ?"
            ).map_err(|_|DatabaseError::RecallError)?;

            let mut rows = stmt.query(&[&userid]).map_err(|_|DatabaseError::RecallError)?;

            if let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
                let user = User {
                    user_id: row.get(0).map_err(|_| DatabaseError::RecallError)?,
                    username: row.get(1).map_err(|_| DatabaseError::RecallError)?,
                    password: row.get(2).map_err(|_| DatabaseError::RecallError)?
                };
                return Ok(Some(user))
            } else {
                return Ok(None)
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_nodes(
    db: &Arc<Mutex<Connection>>
) -> Result<Vec<Node>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare("SELECT * FROM nodes").map_err(|_| DatabaseError::RecallError)?;
            let results = stmt.query_map([], |row| {
                let pubkey_bytes: Vec<u8> = row.get(5)?;
                Ok(Node {
                    node_id: row.get(0)?,
                    name: row.get(1)?,
                    ip_address: row.get(2)?,
                    port: row.get(3)?,
                    owner: row.get(4)?,
                    pubkey: crate::types::PubKey::from_bytes(pubkey_bytes)
                })
            });

            match results {
                Ok(users) => Ok(users.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?),
                Err(e) => {
                    dbg!(e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            dbg!(e);
            Err(DatabaseError::LockError)
        }
    }
}

pub async fn insert_node(
    db: &Arc<Mutex<Connection>>,
    node: Node,
    dump_tx: oneshot::Sender<setup::SyncSetupObject>,
    mut confirm_write_rx: oneshot::Receiver<Result<(), Error>>
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            ///////////////
            // 2. Get the current DB state, dump into vecs
            ///////////////
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            // user data extract
            let mut stmt_users = tx.prepare(
                "SELECT * FROM users",
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_users = stmt_users.query_map([], |row| {
                Ok(User {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    password: row.get(2)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let users: Vec<User> = rows_users.collect::<Result<Vec<User>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            // node data extract
            let mut stmt_nodes = tx.prepare(
                "SELECT * FROM nodes",
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_nodes = stmt_nodes.query_map([], |row| {
                let pubkey_bytes: Vec<u8> = row.get(5)?;
                Ok(Node {
                    node_id: row.get(0)?,
                    name: row.get(1)?,
                    ip_address: row.get(2)?,
                    port: row.get(3)?,
                    owner: row.get(4)?,
                    pubkey: crate::types::PubKey::from_bytes(pubkey_bytes)
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let mut nodes: Vec<Node> = rows_nodes.collect::<Result<Vec<Node>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            // sequence data extract
            let mut stmt_sequences = tx.prepare(
                "SELECT * FROM sequences",
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_sequences = stmt_sequences.query_map([], |row| {
                Ok(Sequence {
                    name: row.get(0)?,
                    next_id: row.get(1)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let sequences: Vec<Sequence> = rows_sequences.collect::<Result<Vec<Sequence>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            // blocks data extract
            dbg!("Fetching block state");
            let mut stmt_blocks = tx.prepare(
                "SELECT block_hash, height, view_number, parent_hash, transactions FROM blocks",
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_blocks = stmt_blocks.query_map([], |row| {
                let block_hash: crate::types::Blake3Hash = row.get(0)?;
                let height: i32 = row.get(1)?;
                let view_number: i32 = row.get(2)?;
                let parent_hash: Option<Vec<u8>> = row.get(3)?;
                let transactions_blob: Option<Vec<u8>> = row.get(4)?;
                
                // Convert parent_hash from Option<Vec<u8>> to Option<Blake3Hash>
                let parent_hash = match parent_hash {
                    Some(bytes) => {
                        if bytes.len() == 32 {
                            let mut array = [0u8; 32];
                            array.copy_from_slice(&bytes);
                            Some(crate::types::Blake3Hash::from_bytes(array))
                        } else {
                            None
                        }
                    }
                    None => None,
                };
                
                // Decode transactions from blob if present
                let transactions = match transactions_blob {
                    Some(blob) => {
                        match bincode::decode_from_slice(&blob, bincode::config::standard()) {
                            Ok((txs, _)) => Some(txs),
                            Err(_) => None,
                        }
                    }
                    None => None,
                };
                
                Ok(Block {
                    block_hash,
                    data: crate::types::BlockData {
                        height,
                        view_number,
                        parent_hash,
                        transactions,
                    },
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let blocks: Vec<Block> = rows_blocks.collect::<Result<Vec<Block>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            dbg!("Fetching consensus state");
            // Get the consensus state from this_node table
            let (current_phase, current_view, prepared_block_hash, committed_block_hash, highest_qc_block_hash) = tx.query_row(
                "SELECT current_phase, current_view, prepared_block_hash, committed_block_hash, highest_qc_block_hash FROM this_node WHERE internal_id = 1",
                [],
                |row| {
                    // Handle DuckDB enum - try getting as string first, fallback to enum handling
                    let phase = match row.get_ref_unwrap(0) {
                        ValueRef::Text(text) => {
                            let text_str = std::str::from_utf8(text).unwrap_or("propose");
                            match text_str {
                                "propose" => crate::consensus::ConsensusPhase::Propose,
                                "vote" => crate::consensus::ConsensusPhase::Vote,
                                _ => crate::consensus::ConsensusPhase::Propose, // default fallback
                            }
                        },
                        ValueRef::Enum(_enum_type, idx) => {
                            // For now, map index directly to enum values
                            // This assumes: 0 = "propose", 1 = "vote"
                            match idx {
                                0 => crate::consensus::ConsensusPhase::Propose,
                                1 => crate::consensus::ConsensusPhase::Vote,
                                _ => crate::consensus::ConsensusPhase::Propose, // default fallback
                            }
                        },
                        _ => {
                            // Fallback: try to get as string
                            let phase_str: String = row.get(0)?;
                            match phase_str.as_str() {
                                "propose" => crate::consensus::ConsensusPhase::Propose,
                                "vote" => crate::consensus::ConsensusPhase::Vote,
                                _ => crate::consensus::ConsensusPhase::Propose, // default fallback
                            }
                        }
                    };
                    let view: i32 = row.get(1)?;
                    let prepared_hash: Option<Vec<u8>> = row.get(2)?;
                    let committed_hash: Vec<u8> = row.get(3)?;
                    let highest_qc_hash: Vec<u8> = row.get(4)?;
                    Ok((phase, view, prepared_hash, committed_hash, highest_qc_hash))
                }
            ).map_err(|_| DatabaseError::RecallError)?;

            dbg!("Consensus phase fetched successfully");

            dbg!("Mapping prepared hash");
            // Convert byte arrays to Blake3Hash using standardized database pattern
            let prepared_block_hash_opt = match prepared_block_hash {
                Some(bytes) => {
                    let hash_array: [u8; 32] = bytes.as_slice().try_into()
                        .map_err(|_| DatabaseError::ProcessingError)?;
                    Some(crate::types::Blake3Hash::new(blake3::Hash::from_bytes(hash_array)))
                },
                None => None
            };

            dbg!("Mapping committed hash");
            let committed_hash_array: [u8; 32] = committed_block_hash.as_slice().try_into()
                .map_err(|_| DatabaseError::ProcessingError)?;
            let committed_block_hash_blake3 = crate::types::Blake3Hash::new(blake3::Hash::from_bytes(committed_hash_array));

            dbg!("Mapping QC hash");
            let highest_qc_hash_array: [u8; 32] = highest_qc_block_hash.as_slice().try_into()
                .map_err(|_| DatabaseError::ProcessingError)?;
            let highest_qc_block_hash_blake3 = crate::types::Blake3Hash::new(blake3::Hash::from_bytes(highest_qc_hash_array));

            ///////////////
            // 3. Compute next node, append to node vec
            ///////////////
            let next_id = tx.query_row(
                "SELECT next_id FROM sequences WHERE name = 'nodes'",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;
            tx.execute(
                "INSERT INTO nodes (node_id, name, ip_address, port, owner, pubkey) VALUES (?, ?, ?, ?, ?, ?)",
                params![next_id, node.name, node.ip_address, node.port, node.owner, node.pubkey.as_bytes()]
            ).map_err(|_| DatabaseError::InsertError)?;

            // Update the sequence for the next node
            tx.execute(
                "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'nodes'",
                []
            ).map_err(|_| DatabaseError::InsertError)?;

            // Get current block height from committed block to add validator
            let current_height = {
                let committed_block_height = tx.query_row(
                    "SELECT height FROM blocks WHERE block_hash = (SELECT committed_block_hash FROM this_node WHERE internal_id = 1)",
                    [],
                    |row| row.get::<_, i32>(0)
                ).map_err(|_| DatabaseError::RecallError)?;
                committed_block_height
            };

            // Add the new node as a validator starting from the current block height
            tx.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
                params![current_height, next_id, true]
            ).map_err(|_| DatabaseError::InsertError)?;

            // Construct and append
            let new_node = Node {
                node_id: next_id,
                name: node.name,
                ip_address: node.ip_address,
                port: node.port,
                owner: node.owner,
                pubkey: node.pubkey
            };
            nodes.push(new_node);
            
            ///////////////
            // 4. Send our sync message to main thread
            ///////////////
            
            // formulate syncsetupobject with complete ThisNode
            let sync_msg = SyncSetupObject {
                users: users,
                nodes: nodes,
                sequences: sequences,
                blocks: blocks,
                yournode: ThisNode {
                    node_id: next_id,
                    current_phase: current_phase,
                    current_view: current_view,
                    prepared_block_hash: prepared_block_hash_opt,
                    committed_block_hash: committed_block_hash_blake3,
                    highest_qc_block_hash: highest_qc_block_hash_blake3,
                }
            };
            // tx to main thread
            match dump_tx.send(sync_msg) {
                Ok(_) => {},
                Err(_) => return Err(DatabaseError::ProcessingError)
            }

            ///////////////
            // 6. If PUT succeeds, send OK message to DB thread
            ///////////////
            match confirm_write_rx.await {
                Ok(Ok(())) => {
                    // If confirmed, commit the transaction
                    tx.commit().map_err(|_| DatabaseError::LockError)?;
                }
                Ok(Err(e)) => {
                    // If error, log or handle it
                    return Err(DatabaseError::LockError);
                }
                Err(_) => {
                    // If channel was closed, return error
                    return Err(DatabaseError::LockError);
                }
            }

            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsensusState {
    pub leader: Node,
    pub view: i32,
    pub prepared_block: Option<crate::types::Block>,
    pub committed_block: crate::types::Block,
    pub highest_qc_block: crate::types::Block,
}

pub fn get_consensus(
    db: &Arc<Mutex<Connection>>
) -> Result<ConsensusState, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT
                    n.node_id, n.name, n.ip_address, n.port, n.owner, n.pubkey, t.current_view,
                    -- Prepared block data (excluding transactions for performance)
                    pb.block_hash AS prepared_hash, pb.height AS prepared_height,
                    pb.view_number AS prepared_view, pb.parent_hash AS prepared_parent,
                    -- Committed block data
                    cb.block_hash AS committed_hash, cb.height AS committed_height,
                    cb.view_number AS committed_view, cb.parent_hash AS committed_parent,
                    -- Highest QC block data
                    hb.block_hash AS highest_qc_hash, hb.height AS highest_qc_height,
                    hb.view_number AS highest_qc_view, hb.parent_hash AS highest_qc_parent
                FROM nodes n
                JOIN this_node t ON n.node_id = (t.current_view % (SELECT COUNT(*) FROM nodes))
                LEFT JOIN blocks pb ON t.prepared_block_hash = pb.block_hash
                LEFT JOIN blocks cb ON t.committed_block_hash = cb.block_hash
                LEFT JOIN blocks hb ON t.highest_qc_block_hash = hb.block_hash
                WHERE t.internal_id = 1"
            ).map_err(|_| DatabaseError::RecallError)?;
            
            let result = stmt.query_row([], |row| {
                // Leader node data
                let node_id: i32 = row.get(0)?;
                let name: String = row.get(1)?;
                let ip_address: String = row.get(2)?;
                let port: i32 = row.get(3)?;
                let owner: i32 = row.get(4)?;
                let pubkey_bytes: Vec<u8> = row.get(5)?;
                let current_view: i32 = row.get(6)?;
                
                // Helper function to build block from row data (without transactions)
                let build_block = |hash_col: usize, height_col: usize, view_col: usize, parent_col: usize| -> Result<Option<crate::types::Block>, duckdb::Error> {
                    let hash_bytes: Option<Vec<u8>> = row.get(hash_col)?;
                    if let Some(hash_bytes) = hash_bytes {
                        let height: i32 = row.get(height_col)?;
                        let view_number: i32 = row.get(view_col)?;
                        let parent_hash_bytes: Option<Vec<u8>> = row.get(parent_col)?;
                        
                        let block_hash_array: [u8; 32] = hash_bytes.as_slice().try_into()
                            .map_err(|_| duckdb::Error::InvalidColumnIndex(hash_col))?;
                        let block_hash = crate::types::Blake3Hash::new(blake3::Hash::from_bytes(block_hash_array));
                        
                        let parent_hash = if let Some(parent_bytes) = parent_hash_bytes {
                            let parent_hash_array: [u8; 32] = parent_bytes.as_slice().try_into()
                                .map_err(|_| duckdb::Error::InvalidColumnIndex(parent_col))?;
                            Some(crate::types::Blake3Hash::new(blake3::Hash::from_bytes(parent_hash_array)))
                        } else {
                            None
                        };
                        
                        Ok(Some(crate::types::Block {
                            block_hash,
                            data: BlockData {
                                height,
                                view_number,
                                parent_hash,
                                transactions: None, // Not loading transactions for performance
                            }
                        }))
                    } else {
                        Ok(None)
                    }
                };
                
                // Build blocks (column indices: prepared=7-10, committed=11-14, highest_qc=15-18)
                let prepared_block = build_block(7, 8, 9, 10)?;
                let committed_block = build_block(11, 12, 13, 14)?;
                let highest_qc_block = build_block(15, 16, 17, 18)?;
                
                Ok((node_id, name, ip_address, port, owner, pubkey_bytes, current_view,
                    prepared_block, committed_block, highest_qc_block))
            }).map_err(|_| DatabaseError::RecallError)?;
            
            let (node_id, name, ip_address, port, owner, pubkey_bytes, current_view,
                 prepared_block, committed_block, highest_qc_block) = result;
            
            let pubkey = crate::types::PubKey::from_bytes(pubkey_bytes);
            let leader = crate::types::Node {
                node_id,
                name,
                ip_address,
                port,
                owner,
                pubkey,
            };
            
            // Since committed_block and highest_qc_block are now always required,
            // we need to ensure they exist in the database
            let committed_block = committed_block.ok_or(DatabaseError::RecallError)?;
            let highest_qc_block = highest_qc_block.ok_or(DatabaseError::RecallError)?;
            
            let consensus_state = ConsensusState {
                leader,
                view: current_view,
                prepared_block,
                committed_block,
                highest_qc_block,
            };
            
            return Ok(consensus_state)
        }
        Err(_) => {Err(DatabaseError::LockError)}
    }
}

pub fn get_validators(
    db: &Arc<Mutex<Connection>>,
    height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "
                WITH latest_effective AS (
                    SELECT 
                        node_id,
                        MAX(effective_height) AS max_eff
                    FROM validators
                    WHERE effective_height <= ?
                    GROUP BY node_id
                ),
                active_validators AS (
                    SELECT 
                        v.node_id,
                        v.is_active
                    FROM validators v
                    JOIN latest_effective le 
                        ON v.node_id = le.node_id 
                        AND v.effective_height = le.max_eff
                    WHERE v.is_active = true
                )
                SELECT n.node_id, n.name, n.ip_address, n.port, n.owner, n.pubkey
                FROM active_validators av
                JOIN nodes n ON av.node_id = n.node_id;
                "
            ).map_err(|_| DatabaseError::RecallError)?;
            
            let results = stmt.query_map([height], |row| {
                let node_id: i32 = row.get(0)?;
                let name: String = row.get(1)?;
                let ip_address: String = row.get(2)?;
                let port: i32 = row.get(3)?;
                let owner: i32 = row.get(4)?;
                let pubkey_bytes: Vec<u8> = row.get(5)?;

                Ok(Node {
                    node_id,
                    name,
                    ip_address,
                    port,
                    owner,
                    pubkey: crate::types::PubKey::from_bytes(pubkey_bytes),
                })
            });

            match results {
                Ok(rows) => {
                    let nodes: Vec<Node> = rows.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?;
                    Ok(nodes)
                }
                Err(e) => {
                    dbg!(e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(_) => {Err(DatabaseError::LockError)}
    }
}

pub struct MyNode {
    pub node_id: i32,
    pub privkey: SigningKey,
}

pub fn get_me(
    db: &Arc<Mutex<Connection>>,
) -> Result<MyNode, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "
                SELECT node_id, privkey FROM this_node
                "
            ).map_err(|_| DatabaseError::RecallError)?;

            let result = stmt.query_row([], |row| {
                let node_id: i32 = row.get(0)?;
                let privkey_bytes: Vec<u8> = row.get(1)?;

                // Reconstruct SigningKey from bytes
                let privkey_array: [u8; 32] = privkey_bytes.as_slice()
                    .try_into()
                    .map_err(|_| duckdb::Error::InvalidColumnType(0, "privkey".to_string(), duckdb::types::Type::Blob))?;
                
                let signing_key = SigningKey::from_bytes(&privkey_array);

                Ok(MyNode {
                    node_id,
                    privkey: signing_key
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            
            Ok(result)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}