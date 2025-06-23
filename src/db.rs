use argon2::PasswordVerifier;
use duckdb::{params, Connection, Error};
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
use crate::types::Node;

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

            CREATE TABLE this_node (
                internal_id     INTEGER PRIMARY KEY DEFAULT 1,
                node_id         INTEGER NOT NULL UNIQUE,
                privkey         BLOB NOT NULL,

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
                INSERT INTO sequences (name, next_id) VALUES ('users', 1);
                INSERT INTO sequences (name, next_id) VALUES ('nodes', 1);
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
            

            // also write this node so we know setup is completed
            tx.execute(
                "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (?, ?, ?)",
                params![1, next_node_id, privkey]
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

            // also write to this_node table so we know setup is completed
            dbg!("Inserting this_node");
            tx.execute(
                "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (?, ?, ?)",
                params![1, setupobj.yournode.node_id, privkey]
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
            
            // formulate syncsetupobject
            let sync_msg = SyncSetupObject {
                users: users,
                nodes: nodes,
                sequences: sequences,
                yournode: ThisNode {
                    node_id: next_id
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