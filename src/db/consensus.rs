use super::*;
use crate::consensus::types::*;

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
                let pubkey: PubKey = row.get(5)?;
                let current_view: i32 = row.get(6)?;
                
                // Helper function to build block from row data (without transactions)
                let build_block = |hash_col: usize, height_col: usize, view_col: usize, parent_col: usize| -> Result<Option<Block>, duckdb::Error> {
                    let block_hash: Option<Blake3Hash> = row.get(hash_col)?;
                    if let Some(block_hash) = block_hash {
                        let height: i32 = row.get(height_col)?;
                        let view_number: i32 = row.get(view_col)?;
                        let parent_hash: Option<Blake3Hash> = row.get(parent_col)?;
                        
                        Ok(Some(Block {
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
                
                Ok((node_id, name, ip_address, port, owner, pubkey, current_view,
                    prepared_block, committed_block, highest_qc_block))
            }).map_err(|_| DatabaseError::RecallError)?;
            
            let (node_id, name, ip_address, port, owner, pubkey, current_view,
                 prepared_block, committed_block, highest_qc_block) = result;
            
            let pubkey = pubkey;
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
                let pubkey: PubKey = row.get(5)?;

                Ok(Node {
                    node_id,
                    name,
                    ip_address,
                    port,
                    owner,
                    pubkey,
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

pub fn insert_block(
    db: &Arc<Mutex<Connection>>,
    block: &Block,
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;
            tx.execute(
                "INSERT INTO blocks (block_hash, height, view_number, parent_hash, transactions) VALUES (?, ?, ?, ?, ?)",
                params![
                    block.block_hash,
                    block.data.height,
                    block.data.view_number,
                    block.data.parent_hash,
                    block.data.transactions
                ]
            ).map_err(|_| DatabaseError::InsertError)?;
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_block(
    db: &Arc<Mutex<Connection>>,
    block_hash: Blake3Hash,
) -> Result<Block, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT block_hash, height, view_number, parent_hash, transactions FROM blocks WHERE block_hash = ?"
            ).map_err(|_| DatabaseError::RecallError)?;

            let result = stmt.query_row([block_hash], |row| {
                let block_hash: Blake3Hash = row.get(0)?;
                let height: i32 = row.get(1)?;
                let view_number: i32 = row.get(2)?;
                let parent_hash: Option<Blake3Hash> = row.get(3)?;
                let transactions: Option<Transactions> = row.get(4)?;

                Ok((block_hash, height, view_number, parent_hash, transactions))
            }).map_err(|_| DatabaseError::RecallError)?;

            let (block_hash, height, view_number, parent_hash, transactions) = result;

            Ok(Block {
                block_hash,
                data: BlockData {
                    height,
                    view_number,
                    parent_hash,
                    transactions,
                },
            })
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn insert_qc(
    db: &Arc<Mutex<Connection>>,
    qc: QuorumCertificate,
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;
            dbg!("Attempting transaction");
            tx.execute(
                "INSERT INTO quorum_certificates (view_number, phase, block_hash, proposer_signature, voter_signatures) VALUES (?, ?, ?, ?, ?)",
                params![
                    qc.view_number,
                    qc.phase,
                    qc.block_hash,
                    qc.proposer_signature,
                    qc.voter_signatures,
                ]
            ).map_err(|_| DatabaseError::InsertError)?;
            dbg!("QC inserted.");
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
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
                let privkey: PrivKey = row.get(1)?;

                Ok(MyNode {
                    node_id,
                    privkey
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            
            Ok(result)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}