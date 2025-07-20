use super::*;
use tokio::sync::oneshot;
use crate::consensus::QuorumCertificate;
use crate::setup::{SyncSetupObject, Validator};
use tokio::io::Error;
use crate::consensus::types::{Block,BlockData,ConsensusPhase};
use crate::setup::ThisNode;
use bincode::serde::decode_from_slice;
use crate::db::{DataRecord, FragmentHash, Inode, Data};
use either::Either;

pub fn get_nodes(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<Vec<Node>, DatabaseError> {
    match db_connection {
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
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    node: Node,
    dump_tx: oneshot::Sender<SyncSetupObject>,
    confirm_write_rx: oneshot::Receiver<Result<(), Error>>,
    user_privkey: PrivKey
) -> Result<(), DatabaseError> {
    match db_connection {
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
                    pubkey: row.get(3)?,
                    x25519_pubkey: row.get(4)?,
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
                let parent_hash: Option<crate::types::Blake3Hash> = row.get(3)?;
                let transactions_blob: Option<Vec<u8>> = row.get(4)?;
                
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
            let (current_phase, current_view, prepared_block_hash_opt, committed_block_hash_blake3, highest_qc_block_hash_blake3) = tx.query_row(
                "SELECT current_phase, current_view, prepared_block_hash, committed_block_hash, highest_qc_block_hash FROM this_node WHERE internal_id = 1",
                [],
                |row| {
                    let phase: ConsensusPhase = row.get(0)?;
                    let view: i32 = row.get(1)?;
                    let prepared_hash: Option<crate::types::Blake3Hash> = row.get(2)?;
                    let committed_hash: crate::types::Blake3Hash = row.get(3)?;
                    let highest_qc_hash: crate::types::Blake3Hash = row.get(4)?;
                    Ok((phase, view, prepared_hash, committed_hash, highest_qc_hash))
                }
            ).map_err(|_| DatabaseError::RecallError)?;

            dbg!("Fetching validator state");
            let mut stmt_validators = tx.prepare(
                "SELECT effective_height, node_id, is_active FROM validators"
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_validators = stmt_validators.query_map([], |row| {
                Ok(Validator {
                    effective_height: row.get(0)?,
                    node_id: row.get(1)?,
                    is_active: row.get(2)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let mut validators: Vec<Validator> = rows_validators.collect::<Result<Vec<Validator>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            dbg!("Fetching quorum certificate state");
            let mut stmt_qcs = tx.prepare(
                "SELECT view_number, phase, block_hash, proposer_signature, voter_signatures FROM quorum_certificates"
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_qcs = stmt_qcs.query_map([], |row| {
                Ok(QuorumCertificate {
                    view_number: row.get(0)?,
                    phase: row.get(1)?,
                    block_hash: row.get(2)?,
                    proposer_signature: row.get(3)?,
                    voter_signatures: row.get(4)?
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let qcs: Vec<QuorumCertificate> = rows_qcs.collect::<Result<Vec<QuorumCertificate>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            dbg!("Fetching timeout certificates");
            let mut stmt_tcs = tx.prepare(
                "SELECT view_number, highest_qc_view, highest_qc_phase, highest_qc_block_hash, signatures FROM timeout_certificates"
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_tcs = stmt_tcs.query_map([], |row| {
                Ok(crate::setup::TimeoutSyncCertificate {
                    view_number: row.get(0)?,
                    highest_qc_view: row.get(1)?,
                    highest_qc_phase: row.get(2)?,
                    highest_qc_block_hash: row.get(3)?,
                    signatures: row.get(4)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let tcs: Vec<crate::setup::TimeoutSyncCertificate> = rows_tcs.collect::<Result<Vec<crate::setup::TimeoutSyncCertificate>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            // File system data extract
            dbg!("Fetching data_blocks");
            let mut stmt_data_blocks = tx.prepare(
                "SELECT id, modified_at, file_hash, fragment_count, added_bytes FROM data_blocks"
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_data_blocks = stmt_data_blocks.query_map([], |row| {
                Ok(DataRecord {
                    id: row.get(0)?,
                    modified_at: row.get(1)?,
                    data: Data {
                        hash: row.get(2)?,
                        fragments: vec![], // Will be populated from fragment_hashes
                        added_bytes: row.get(4)?,
                    },
                    file_access_entries: None, // Will be populated separately if needed
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let data_blocks: Vec<DataRecord> = rows_data_blocks.collect::<Result<Vec<DataRecord>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            dbg!("Fetching fragment_hashes");
            let mut stmt_fragment_hashes = tx.prepare(
                "SELECT data_block_id, fragment_index, fragment_id, fragment_hash, chunk_type, stored_locally FROM fragment_hashes"
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_fragment_hashes = stmt_fragment_hashes.query_map([], |row| {
                Ok(FragmentHash {
                    data_block_id: row.get(0)?,
                    fragment_index: row.get(1)?,
                    fragment_id: row.get(2)?,
                    fragment_hash: row.get(3)?,
                    chunk_type: row.get(4)?,
                    stored_locally: row.get(5)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let fragment_hashes: Vec<FragmentHash> = rows_fragment_hashes.collect::<Result<Vec<FragmentHash>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            dbg!("Fetching inodes");
            let mut stmt_inodes = tx.prepare(
                "SELECT owner_id, path, type, data_id FROM inodes"
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_inodes = stmt_inodes.query_map([], |row| {
                let data_id: Option<CustomUUID> = row.get(3)?;
                Ok(Inode {
                    owner: Either::Left(row.get(0)?),
                    path: row.get(1)?,
                    inode_type: row.get(2)?,
                    data_id: data_id.map(Either::Left),
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let inodes: Vec<Inode> = rows_inodes.collect::<Result<Vec<Inode>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            dbg!("Fetching file_access entries");
            let mut stmt_file_access = tx.prepare(
                "SELECT data_block_id, user_id, ephemeral_pubkey, encrypted_file_key FROM file_access"
            ).map_err(|_| DatabaseError::RecallError)?;
            let rows_file_access = stmt_file_access.query_map([], |row| {
                Ok(crate::db::types::FileAccess {
                    data_block_id: row.get(0)?,
                    user_id: row.get(1)?,
                    ephemeral_pubkey: row.get(2)?,
                    encrypted_file_key: row.get(3)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            let file_access_entries: Vec<crate::db::types::FileAccess> = rows_file_access.collect::<Result<Vec<crate::db::types::FileAccess>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;

            dbg!("Consensus phase fetched successfully");

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
                params![next_id, node.name, node.ip_address, node.port, node.owner, node.pubkey]
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

            // Add the new node as a validator starting from the next block height
            tx.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
                params![current_height + 1, next_id, true]
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
            let new_validator = Validator {
                effective_height: current_height + 1,
                node_id: next_id,
                is_active: true
            };
            validators.push(new_validator);
            
            ///////////////
            // 4. Send our sync message to main thread
            ///////////////
            
            // formulate syncsetupobject with complete ThisNode
            let sync_msg = SyncSetupObject {
                users: users,
                nodes: nodes,
                sequences: sequences,
                blocks: blocks,
                validators: validators,
                quorum_certificates: qcs,
                timeout_certificates: tcs,
                data_blocks: data_blocks,
                fragment_hashes: fragment_hashes,
                file_access_entries: file_access_entries,
                inodes: inodes,
                yournode: ThisNode {
                    node_id: next_id,
                    current_phase: current_phase,
                    current_view: current_view,
                    prepared_block_hash: prepared_block_hash_opt,
                    committed_block_hash: committed_block_hash_blake3,
                    highest_qc_block_hash: highest_qc_block_hash_blake3,
                },
                user_privkey: user_privkey
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
