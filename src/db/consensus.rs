use super::*;
use crate::consensus::types::*;

pub fn get_consensus_with_conn(
    db_lock: &r2d2::PooledConnection<DuckdbConnectionManager>,
) -> Result<ConsensusState, DatabaseError> {
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

            Ok(consensus_state)
}

pub fn get_consensus(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<ConsensusState, DatabaseError> {
    match db_connection {
        Ok(db_lock) => get_consensus_with_conn(&db_lock),
        Err(_) => Err(DatabaseError::LockError)
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

pub fn get_validators_elect(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    current_height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "
                WITH validators_elect AS (
                    SELECT DISTINCT node_id
                    FROM validators
                    WHERE effective_height > ? AND is_active = true
                )
                SELECT n.node_id, n.name, n.ip_address, n.port, n.owner, n.pubkey
                FROM validators_elect ve
                JOIN nodes n ON ve.node_id = n.node_id;
                "
            ).map_err(|_| DatabaseError::RecallError)?;
            
            let results = stmt.query_map([current_height], |row| {
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
                    tracing::error!("Failed to query validators elect: {:?}", e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(_) => {Err(DatabaseError::LockError)}
    }
}

pub fn insert_block_with_conn(
    db_lock: &mut r2d2::PooledConnection<DuckdbConnectionManager>,
    block: &Block,
    set_prepared: bool,
) -> Result<(), DatabaseError> {
    let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

    // Insert the block
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

    // Optionally set prepared_block_hash to mark consensus in progress
    if set_prepared {
        tx.execute(
            "UPDATE this_node SET prepared_block_hash = ? WHERE internal_id = 1",
            params![block.block_hash]
        ).map_err(|_| DatabaseError::InsertError)?;

        tracing::debug!(
            "Set prepared_block_hash to {:?} (height: {}, view: {})",
            block.block_hash, block.data.height, block.data.view_number
        );
    }

    tx.commit().map_err(|_| DatabaseError::InsertError)?;
    Ok(())
}

pub fn insert_block(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    block: &Block,
    set_prepared: bool,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => insert_block_with_conn(&mut db_lock, block, set_prepared),
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

/// Insert QC within an existing transaction (for use in genesis setup and multi-op transactions)
pub fn insert_qc_tx(
    tx: &duckdb::Transaction,
    qc: &QuorumCertificate,
) -> Result<(), DatabaseError> {
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
            // If QC phase is propose, change to lock and set prepared_block_hash
            tx.execute(
                "UPDATE this_node SET highest_qc_block_hash = ?, current_phase = 'lock', prepared_block_hash = ? WHERE internal_id = 1",
                params![qc.block_hash, qc.block_hash]
            ).map_err(|_| DatabaseError::InsertError)?;
            tracing::info!("Updated consensus state: propose -> lock phase for view {}, set prepared_block_hash", qc.view_number);
        }
        ConsensusPhase::Lock => {
            // If QC phase is lock, change to propose, set current_view to QC view + 1,
            // commit the block, and clear prepared_block_hash (consensus completed)
            tx.execute(
                "UPDATE this_node SET highest_qc_block_hash = ?, committed_block_hash = ?, current_phase = 'propose', current_view = ?, prepared_block_hash = NULL WHERE internal_id = 1",
                params![qc.block_hash, qc.block_hash, qc.view_number + 1]
            ).map_err(|_| DatabaseError::InsertError)?;
            tracing::info!(
                "Updated consensus state: lock -> propose, view {} -> {}, committed block {:?}, cleared prepared_block_hash",
                qc.view_number, qc.view_number + 1, qc.block_hash
            );
        }
    }

    Ok(())
}

/// Insert QC with retry logic for write-write conflicts (wrapper that manages transactions)
pub fn insert_qc(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    qc: QuorumCertificate,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            // Retry logic for write-write conflicts
            const MAX_RETRIES: u32 = 3;
            let mut retry_count = 0;

            loop {
                let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

                insert_qc_tx(&tx, &qc)?;

                match tx.commit() {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        let error_msg = format!("{:?}", e);
                        if error_msg.contains("write-write conflict") && retry_count < MAX_RETRIES - 1 {
                            retry_count += 1;
                            let delay_ms = 10 * (2_u64.pow(retry_count - 1)); // Exponential backoff: 10ms, 20ms, 40ms
                            tracing::warn!(
                                "Write-write conflict on QC insertion (view: {}, phase: {:?}), retrying in {}ms (attempt {}/{})",
                                qc.view_number, qc.phase, delay_ms, retry_count + 1, MAX_RETRIES
                            );
                            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                            continue;
                        } else {
                            tracing::error!(
                                "Failed to commit QC insertion transaction (view: {}, phase: {:?}) after {} attempts: {:?}",
                                qc.view_number, qc.phase, retry_count + 1, e
                            );
                            return Err(DatabaseError::InsertError);
                        }
                    }
                }
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_node_pubkey(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    node_id: i32,
) -> Result<PubKey, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT pubkey FROM nodes WHERE node_id = ?"
            ).map_err(|_| DatabaseError::RecallError)?;

            let node_pubkey: PubKey = stmt.query_row([node_id], |row| {
                row.get(0)
            }).map_err(|_| DatabaseError::RecallError)?;

            Ok(node_pubkey)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_all_node_pubkeys(
    db_lock: &r2d2::PooledConnection<DuckdbConnectionManager>,
) -> Result<std::collections::HashMap<i32, PubKey>, DatabaseError> {
    let mut stmt = db_lock.prepare(
        "SELECT node_id, pubkey FROM nodes"
    ).map_err(|_| DatabaseError::RecallError)?;

    let rows = stmt.query_map([], |row| {
        let node_id: i32 = row.get(0)?;
        let pubkey: PubKey = row.get(1)?;
        Ok((node_id, pubkey))
    }).map_err(|_| DatabaseError::RecallError)?;

    let mut map = std::collections::HashMap::new();
    for row in rows {
        let (node_id, pubkey) = row.map_err(|_| DatabaseError::RecallError)?;
        map.insert(node_id, pubkey);
    }

    Ok(map)
}

pub fn get_all_user_pubkeys(
    db_lock: &r2d2::PooledConnection<DuckdbConnectionManager>,
) -> Result<std::collections::HashMap<i32, PubKey>, DatabaseError> {
    let mut stmt = db_lock.prepare(
        "SELECT user_id, pubkey FROM users"
    ).map_err(|_| DatabaseError::RecallError)?;

    let rows = stmt.query_map([], |row| {
        let user_id: i32 = row.get(0)?;
        let pubkey: PubKey = row.get(1)?;
        Ok((user_id, pubkey))
    }).map_err(|_| DatabaseError::RecallError)?;

    let mut map = std::collections::HashMap::new();
    for row in rows {
        let (user_id, pubkey) = row.map_err(|_| DatabaseError::RecallError)?;
        map.insert(user_id, pubkey);
    }

    Ok(map)
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

pub fn get_view_consensus_data(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    view: i32,
) -> Result<ViewConsensusData, DatabaseError> {
    use crate::consensus::types::*;
    use duckdb::OptionalExt;
    
    match db_connection {
        Ok(db_lock) => {
            // Get timeout certificate for this view (if exists)
            let timeout_certificate = db_lock.query_row(
                "SELECT tc.view_number, tc.signatures, qc.view_number, qc.phase, qc.block_hash, qc.proposer_signature, qc.voter_signatures
                 FROM timeout_certificates tc
                 JOIN quorum_certificates qc ON tc.highest_qc_view = qc.view_number 
                   AND tc.highest_qc_phase = qc.phase 
                   AND tc.highest_qc_block_hash = qc.block_hash
                 WHERE tc.view_number = ?",
                params![view],
                |row| {
                    Ok(TimeoutCertificate {
                        view_number: row.get(0)?,
                        highest_qc: QuorumCertificate {
                            view_number: row.get(2)?,
                            phase: row.get(3)?,
                            block_hash: row.get(4)?,
                            proposer_signature: row.get(5)?,
                            voter_signatures: row.get(6)?,
                        },
                        signatures: row.get(1)?,
                    })
                }
            ).optional().map_err(|_| DatabaseError::RecallError)?;

            // Get all QCs for this view (both propose and lock phases)
            let mut qc_stmt = db_lock.prepare(
                "SELECT view_number, phase, block_hash, proposer_signature, voter_signatures
                 FROM quorum_certificates WHERE view_number = ?"
            ).map_err(|_| DatabaseError::RecallError)?;
            
            let qc_rows = qc_stmt.query_map(params![view], |row| {
                Ok(QuorumCertificate {
                    view_number: row.get(0)?,
                    phase: row.get(1)?,
                    block_hash: row.get(2)?,
                    proposer_signature: row.get(3)?,
                    voter_signatures: row.get(4)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?;

            let mut propose_qc = None;
            let mut lock_qc = None;
            let mut block_hashes = Vec::new();

            for qc_result in qc_rows {
                let qc = qc_result.map_err(|_| DatabaseError::RecallError)?;
                block_hashes.push(qc.block_hash.clone());
                match qc.phase {
                    ConsensusPhase::Propose => propose_qc = Some(qc),
                    ConsensusPhase::Lock => lock_qc = Some(qc),
                }
            }

            // Add block from timeout certificate if present
            if let Some(ref tc) = timeout_certificate {
                block_hashes.push(tc.highest_qc.block_hash.clone());
            }

            // Get all referenced blocks (deduplicate hashes)
            let mut blocks = Vec::new();
            let mut seen_hashes = std::collections::HashSet::new();
            for block_hash in block_hashes {
                if seen_hashes.insert(block_hash.clone()) {
                    if let Some(block) = db_lock.query_row(
                        "SELECT block_hash, height, view_number, parent_hash, transactions
                         FROM blocks WHERE block_hash = ?",
                        params![block_hash],
                        |row| {
                            Ok(Block {
                                block_hash: row.get(0)?,
                                data: BlockData {
                                    height: row.get(1)?,
                                    view_number: row.get(2)?,
                                    parent_hash: row.get(3)?,
                                    transactions: row.get(4)?,
                                }
                            })
                        }
                    ).optional().map_err(|_| DatabaseError::RecallError)? {
                        blocks.push(block);
                    }
                }
            }

            Ok(ViewConsensusData {
                view,
                timeout_certificate,
                propose_qc,
                lock_qc,
                blocks,
            })
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

/// Get the current consensus height (height of the committed block)
/// This is used consistently across the system for modification tracking
pub fn get_current_consensus_height(tx: &duckdb::Transaction) -> Result<i32, DatabaseError> {
    use duckdb::OptionalExt;

    let current_height: Option<i32> = tx.query_row(
        "SELECT COALESCE(b.height, 0) as committed_height
         FROM this_node t
         LEFT JOIN blocks b ON t.committed_block_hash = b.block_hash
         WHERE t.internal_id = 1",
        [],
        |row| row.get(0)
    ).optional().map_err(|_| DatabaseError::RecallError)?;

    // Return 0 if this_node doesn't exist yet (genesis case)
    Ok(current_height.unwrap_or(0))
}

/// Check if a node is active at a given height
/// Returns true if the node has an active validator entry effective at or before the given height
pub fn is_node_active(
    tx: &duckdb::Transaction,
    node_id: i32,
    height: i32,
) -> Result<bool, DatabaseError> {
    use duckdb::OptionalExt;

    // Get the most recent validator record at or before this height
    let is_active: Option<bool> = tx.query_row(
        "SELECT is_active FROM validators
         WHERE node_id = ? AND effective_height <= ?
         ORDER BY effective_height DESC
         LIMIT 1",
        params![node_id, height],
        |row| row.get(0)
    ).optional().map_err(|_| DatabaseError::RecallError)?;

    // If no record found, node is not active
    Ok(is_active.unwrap_or(false))
}

/// Activate a validator at a specific effective height
/// If the node already has a future activation (after current height), it will be updated
/// This enables hot-swap operations where validator-elect activation can be moved forward
pub fn activate_validator(
    tx: &duckdb::Transaction,
    node_id: i32,
    effective_height: i32,
) -> Result<(), DatabaseError> {
    use duckdb::OptionalExt;

    let current_height = get_current_consensus_height(tx)?;

    // Check if node already has the NEXT future activation (earliest after current height)
    let existing_future_activation: Option<i32> = tx.query_row(
        "SELECT effective_height FROM validators
         WHERE node_id = ? AND effective_height > ? AND is_active = true
         ORDER BY effective_height ASC
         LIMIT 1",
        params![node_id, current_height],
        |row| row.get(0)
    ).optional().map_err(|_| DatabaseError::RecallError)?;

    if let Some(old_height) = existing_future_activation {
        // UPDATE the existing next activation to new height
        tx.execute(
            "UPDATE validators SET effective_height = ?
             WHERE node_id = ? AND effective_height = ?",
            params![effective_height, node_id, old_height]
        ).map_err(|_| DatabaseError::InsertError)?;

        tracing::info!(
            "Updated activation for node {} from height {} to height {}",
            node_id, old_height, effective_height
        );
    } else {
        // INSERT new activation record
        tx.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
            params![effective_height, node_id, true]
        ).map_err(|_| DatabaseError::InsertError)?;

        tracing::info!(
            "Scheduled activation for node {} at height {}",
            node_id, effective_height
        );
    }

    Ok(())
}