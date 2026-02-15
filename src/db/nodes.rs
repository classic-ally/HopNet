use super::*;
use tokio::sync::oneshot;
use crate::consensus::QuorumCertificate;
use tokio::io::Error;
use crate::consensus::types::{Block,BlockData,ConsensusPhase};
use bincode::serde::decode_from_slice;
use crate::db::{DataRecord, FragmentHash, Inode, Data};
use either::Either;

pub fn get_nodes(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<Vec<Node>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare("SELECT node_id, name, owner, pubkey FROM nodes").map_err(|_| DatabaseError::RecallError)?;
            let results = stmt.query_map([], |row| {
                Ok(Node {
                    node_id: row.get(0)?,
                    name: row.get(1)?,
                    owner: row.get(2)?,
                    pubkey: row.get(3)?,
                })
            });

            match results {
                Ok(users) => Ok(users.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?),
                Err(e) => {
                    tracing::error!("Failed to execute query in get_nodes: {:?}", e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            tracing::error!("Failed to execute query in get_nodes: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn get_next_node_id(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<i32, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let next_id = db_lock.query_row(
                "SELECT next_id FROM sequences WHERE name = 'nodes'",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;
            Ok(next_id)
        },
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn node_exists(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    node_id: i32,
) -> Result<bool, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let count: i32 = db_lock.query_row(
                "SELECT COUNT(*) FROM nodes WHERE node_id = ?",
                params![node_id],
                |row| row.get(0)
            ).map_err(|_| DatabaseError::RecallError)?;
            Ok(count > 0)
        },
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Core node insertion logic - operates within provided transaction for atomicity
/// Returns the assigned node_id
/// Node is registered but not yet active (no validators table entry)
pub fn insert_node_tx(
    tx: &duckdb::Transaction,
    node: Node,
) -> Result<i32, DatabaseError> {
    // Get next node ID from sequence
    let next_id = tx.query_row(
        "SELECT next_id FROM sequences WHERE name = 'nodes'",
        [],
        |row| row.get::<_, i32>(0)
    ).map_err(|_| DatabaseError::RecallError)?;

    // Insert the new node
    tx.execute(
        "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, ?, ?)",
        params![next_id, node.name, node.owner, node.pubkey]
    ).map_err(|_| DatabaseError::InsertError)?;

    // Update the sequence for the next node
    tx.execute(
        "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'nodes'",
        []
    ).map_err(|_| DatabaseError::InsertError)?;

    Ok(next_id)
}

/// Wrapper that manages connection and transaction - for backward compatibility
pub fn insert_node_consensus(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    node: Node,
    execute: bool,
) -> Result<(), DatabaseError> {
    // Consensus-safe node insertion - just adds node to DB
    // Node is registered but not yet active (no validators table entry)
    // Node will activate itself via activation transaction after catching up
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            let node_id = insert_node_tx(&tx, node)?;

            // Commit or rollback based on execute flag
            if execute {
                tx.commit().map_err(|_| DatabaseError::InsertError)?;
                tracing::info!("Node {} registered via consensus (inactive, will activate after catch-up)", node_id);
            } else {
                tx.rollback().map_err(|_| DatabaseError::LockError)?;
                tracing::debug!("Node {} insertion validated successfully (rolled back)", node_id);
            }

            Ok(())
        },
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get all registered nodes as NodeConnectionInfo, excluding specified node
/// Used for fragment discovery when broadcasting to all nodes
pub fn get_all_nodes_as_connection_info(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    exclude_node_id: i32,
) -> Result<Vec<crate::types::NodeConnectionInfo>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT node_id, pubkey FROM nodes WHERE node_id != ?"
            ).map_err(|_| DatabaseError::RecallError)?;

            let results = stmt.query_map([exclude_node_id], |row| {
                Ok(crate::types::NodeConnectionInfo {
                    node_id: row.get(0)?,
                    pubkey: row.get(1)?,
                })
            });

            match results {
                Ok(nodes) => Ok(nodes.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?),
                Err(e) => {
                    tracing::error!("Failed to get nodes as connection info: {:?}", e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            tracing::error!("Failed to get database connection in get_all_nodes_as_connection_info: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}
