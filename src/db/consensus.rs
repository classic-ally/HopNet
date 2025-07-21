use super::*;
use crate::consensus::types::*;

pub fn get_consensus(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<ConsensusState, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "WITH latest_effective AS (
                    SELECT
                        node_id,
                        MAX(effective_height) AS max_eff
                    FROM validators
                    WHERE effective_height <= (
                        SELECT COALESCE(
                            (SELECT height FROM blocks WHERE block_hash = t.committed_block_hash),
                            0
                        )
                        FROM this_node t WHERE t.internal_id = 1
                    )
                    GROUP BY node_id
                ),
                active_validators AS (
                    SELECT
                        v.node_id,
                        v.is_active,
                        ROW_NUMBER() OVER (ORDER BY v.node_id) as validator_index
                    FROM validators v
                    JOIN latest_effective le
                        ON v.node_id = le.node_id
                        AND v.effective_height = le.max_eff
                    WHERE v.is_active = true
                ),
                leader_selection AS (
                    SELECT node_id
                    FROM active_validators
                    WHERE validator_index = (
                        (SELECT current_view FROM this_node WHERE internal_id = 1) %
                        (SELECT COUNT(*) FROM active_validators)
                    ) + 1
                )
                SELECT
                    n.node_id, n.name, n.ip_address, n.port, n.owner, n.pubkey, t.current_view, t.current_phase, t.last_timeout_vote_view,
                    -- Prepared block data (excluding transactions for performance)
                    pb.block_hash AS prepared_hash, pb.height AS prepared_height,
                    pb.view_number AS prepared_view, pb.parent_hash AS prepared_parent,
                    -- Committed block data
                    cb.block_hash AS committed_hash, cb.height AS committed_height,
                    cb.view_number AS committed_view, cb.parent_hash AS committed_parent,
                    -- Highest QC block data
                    hb.block_hash AS highest_qc_hash, hb.height AS highest_qc_height,
                    hb.view_number AS highest_qc_view, hb.parent_hash AS highest_qc_parent
                FROM leader_selection ls
                JOIN nodes n ON ls.node_id = n.node_id
                JOIN this_node t ON t.internal_id = 1
                LEFT JOIN blocks pb ON t.prepared_block_hash = pb.block_hash
                LEFT JOIN blocks cb ON t.committed_block_hash = cb.block_hash
                LEFT JOIN blocks hb ON t.highest_qc_block_hash = hb.block_hash"
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
                let current_phase: ConsensusPhase = row.get(7)?;
                let last_timeout_vote_view: i32 = row.get(8)?;
                
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
                
                // Build blocks (column indices: prepared=9-12, committed=13-16, highest_qc=17-20)
                let prepared_block = build_block(9, 10, 11, 12)?;
                let committed_block = build_block(13, 14, 15, 16)?;
                let highest_qc_block = build_block(17, 18, 19, 20)?;
                
                Ok((node_id, name, ip_address, port, owner, pubkey, current_view, current_phase, last_timeout_vote_view,
                    prepared_block, committed_block, highest_qc_block))
            }).map_err(|_| DatabaseError::RecallError)?;
            
            let (node_id, name, ip_address, port, owner, pubkey, current_view, current_phase, last_timeout_vote_view,
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
                phase: current_phase,
                prepared_block,
                committed_block,
                highest_qc_block,
                last_timeout_vote_view,
            };
            
            return Ok(consensus_state)
        }
        Err(_) => {Err(DatabaseError::LockError)}
    }
}

pub fn get_validators(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    match db_connection {
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
                    tracing::error!("Failed to query validators: {:?}", e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(_) => {Err(DatabaseError::LockError)}
    }
}

pub fn insert_block(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    block: &Block,
) -> Result<(), DatabaseError> {
    match db_connection {
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
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    block_hash: Blake3Hash,
) -> Result<Block, DatabaseError> {
    match db_connection {
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

pub fn get_quorum_certificate_by_hash(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    view_number: &i32,
    block_hash: &Blake3Hash,
    phase: &ConsensusPhase
) -> Result<QuorumCertificate, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT view_number, phase, block_hash, proposer_signature, voter_signatures FROM quorum_certificates WHERE view_number = ? AND phase = ? AND block_hash = ?"
            ).map_err(|_| DatabaseError::RecallError)?;

            let result = stmt.query_row(params![view_number, phase, block_hash], |row| {
                Ok(QuorumCertificate {
                    view_number: row.get(0)?,
                    phase: row.get(1)?,
                    block_hash: row.get(2)?,
                    proposer_signature: row.get(3)?,
                    voter_signatures: row.get(4)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?;
            Ok(result)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn insert_tc(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    tc: TimeoutCertificate,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;
            
            // Insert the TC into timeout_certificates table
            tx.execute(
                "INSERT INTO timeout_certificates (view_number, highest_qc_view, highest_qc_phase, highest_qc_block_hash, signatures) VALUES (?, ?, ?, ?, ?)",
                params![
                    tc.view_number,
                    tc.highest_qc.view_number,
                    tc.highest_qc.phase,
                    tc.highest_qc.block_hash,
                    tc.signatures,
                ]
            ).map_err(|_| DatabaseError::InsertError)?;
            
            // Update consensus state to new view
            let new_view = tc.view_number + 1;
            tx.execute(
                "UPDATE this_node SET current_view = ?, current_phase = 'propose' WHERE internal_id = 1",
                params![new_view]
            ).map_err(|_| DatabaseError::InsertError)?;
            
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn insert_qc(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    qc: QuorumCertificate,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;
            tracing::debug!("Starting QC insertion transaction");
            
            // Insert the QC into quorum_certificates table
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
            
            // Update this_node table based on QC's phase and view
            match qc.phase {
                ConsensusPhase::Propose => {
                    // If QC phase is propose, change to lock
                    tx.execute(
                        "UPDATE this_node SET highest_qc_block_hash = ?, current_phase = 'lock' WHERE internal_id = 1",
                        params![qc.block_hash]
                    ).map_err(|_| DatabaseError::InsertError)?;
                    tracing::info!("Updated consensus state: propose -> lock phase for view {}", qc.view_number);
                }
                ConsensusPhase::Lock => {
                    // If QC phase is lock, change to propose, set current_view to QC view + 1,
                    // and commit the block (set committed_block_hash = highest_qc_block_hash)
                    tx.execute(
                        "UPDATE this_node SET highest_qc_block_hash = ?, committed_block_hash = ?, current_phase = 'propose', current_view = ? WHERE internal_id = 1",
                        params![qc.block_hash, qc.block_hash, qc.view_number + 1]
                    ).map_err(|_| DatabaseError::InsertError)?;
                    tracing::info!(
                        "Updated consensus state: lock -> propose, view {} -> {}, committed block {:?}",
                        qc.view_number, qc.view_number + 1, qc.block_hash
                    );
                }
            }
            
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            tracing::debug!(
                "QC successfully inserted for view {} phase {:?}",
                qc.view_number, qc.phase
            );
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

// Efficient function to get node and user pubkeys for RPC authentication
// Also returns whether the user owns the node
pub fn get_node_user_auth_info(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    node_id: i32,
    user_id: i32,
) -> Result<(PubKey, PubKey, bool), DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT n.pubkey as node_pubkey, u.pubkey as user_pubkey, (n.owner = u.user_id) as user_owns_node
                 FROM nodes n, users u 
                 WHERE n.node_id = ? AND u.user_id = ?"
            ).map_err(|_| DatabaseError::RecallError)?;

            let result = stmt.query_row([node_id, user_id], |row| {
                let node_pubkey: PubKey = row.get(0)?;
                let user_pubkey: PubKey = row.get(1)?;
                let user_owns_node: bool = row.get(2)?;
                Ok((node_pubkey, user_pubkey, user_owns_node))
            }).map_err(|_| DatabaseError::RecallError)?;
            
            Ok(result)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_me(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<MyNode, DatabaseError> {
    match db_connection {
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

pub fn mark_timeout_vote_issued(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    view: i32,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            db_lock.execute(
                "UPDATE this_node SET last_timeout_vote_view = ? WHERE internal_id = 1",
                params![view]
            ).map_err(|_| DatabaseError::InsertError)?;
            
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}