use super::*;
use tokio::sync::oneshot;
use crate::setup::SyncSetupObject;
use tokio::io::Error;
use crate::consensus::types::{Block,BlockData,ConsensusPhase};
use crate::setup::ThisNode;
use bincode::serde::decode_from_slice;

pub fn get_nodes(
    db: &Arc<Mutex<Connection>>
) -> Result<Vec<Node>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare("SELECT * FROM nodes").map_err(|_| DatabaseError::RecallError)?;
            let results = stmt.query_map([], |row| {
                Ok(Node {
                    node_id: row.get(0)?,
                    name: row.get(1)?,
                    ip_address: row.get(2)?,
                    port: row.get(3)?,
                    owner: row.get(4)?,
                    pubkey: row.get(5)?,
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
    dump_tx: oneshot::Sender<SyncSetupObject>,
    confirm_write_rx: oneshot::Receiver<Result<(), Error>>
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
                Ok(Node {
                    node_id: row.get(0)?,
                    name: row.get(1)?,
                    ip_address: row.get(2)?,
                    port: row.get(3)?,
                    owner: row.get(4)?,
                    pubkey: row.get(5)?
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
                        match decode_from_slice(&blob, bincode::config::standard()) {
                            Ok((txs, _)) => Some(txs),
                            Err(_) => None,
                        }
                    }
                    None => None,
                };
                
                Ok(Block {
                    block_hash,
                    data: BlockData {
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
                    let phase: ConsensusPhase = row.get(0)?;
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