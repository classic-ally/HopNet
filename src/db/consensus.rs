use super::*;
use crate::consensus::types::*;
use rusqlite::TransactionBehavior;

pub fn get_consensus_with_conn(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
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
                    n.node_id, n.name, n.owner, n.pubkey, t.current_view, t.current_phase, t.last_timeout_vote_view,
                    t.last_propose_vote_block_hash, t.highest_qc_phase,
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

    let result = stmt
        .query_row([], |row| {
            // Leader node data (columns: node_id, name, owner, pubkey, current_view, ...)
            let node_id: i32 = row.get(0)?;
            let name: String = row.get(1)?;
            let owner: i32 = row.get(2)?;
            let pubkey: PubKey = row.get(3)?;
            let current_view: i32 = row.get(4)?;
            let current_phase: ConsensusPhase = row.get(5)?;
            let last_timeout_vote_view: i32 = row.get(6)?;
            let last_propose_vote_block_hash: Option<Blake3Hash> = row.get(7)?;
            let highest_qc_phase: Option<ConsensusPhase> = row.get(8)?;

            // Helper function to build block from row data (without transactions)
            let build_block = |hash_col: usize,
                               height_col: usize,
                               view_col: usize,
                               parent_col: usize|
             -> Result<Option<Block>, rusqlite::Error> {
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
                        },
                    }))
                } else {
                    Ok(None)
                }
            };

            // Build blocks (column indices: prepared=9-12, committed=13-16, highest_qc=17-20)
            let prepared_block = build_block(9, 10, 11, 12)?;
            let committed_block = build_block(13, 14, 15, 16)?;
            let highest_qc_block = build_block(17, 18, 19, 20)?;

            Ok((
                node_id,
                name,
                owner,
                pubkey,
                current_view,
                current_phase,
                last_timeout_vote_view,
                last_propose_vote_block_hash,
                highest_qc_phase,
                prepared_block,
                committed_block,
                highest_qc_block,
            ))
        })
        .map_err(|_| DatabaseError::RecallError)?;

    let (
        node_id,
        name,
        owner,
        pubkey,
        current_view,
        current_phase,
        last_timeout_vote_view,
        last_propose_vote_block_hash,
        highest_qc_phase,
        prepared_block,
        committed_block,
        highest_qc_block,
    ) = result;

    let leader = crate::types::Node {
        node_id,
        name,
        owner,
        pubkey,
    };

    // Since committed_block, highest_qc_block, and highest_qc_phase are now always required,
    // we need to ensure they exist in the database
    let committed_block = committed_block.ok_or(DatabaseError::RecallError)?;
    let highest_qc_block = highest_qc_block.ok_or(DatabaseError::RecallError)?;
    let highest_qc_phase = highest_qc_phase.ok_or(DatabaseError::RecallError)?;

    let consensus_state = ConsensusState {
        leader,
        view: current_view,
        phase: current_phase,
        prepared_block,
        committed_block,
        highest_qc_block,
        highest_qc_phase,
        last_timeout_vote_view,
        last_propose_vote_block_hash,
    };

    Ok(consensus_state)
}

pub fn get_consensus(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<ConsensusState, DatabaseError> {
    match db_connection {
        Ok(db_lock) => get_consensus_with_conn(&db_lock),
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get consensus history showing view progression for debugging
pub fn get_consensus_history(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<Vec<crate::consensus::routes::ViewHistoryEntry>, DatabaseError> {
    use crate::consensus::routes::ViewHistoryEntry;

    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "WITH RECURSIVE view_range(view) AS (
                    SELECT 0
                    UNION ALL
                    SELECT view + 1 FROM view_range WHERE view < (SELECT current_view FROM this_node WHERE internal_id = 1)
                ),
                propose_qcs AS (
                    SELECT view_number FROM quorum_certificates WHERE phase = 0
                ),
                lock_qcs AS (
                    SELECT view_number FROM quorum_certificates WHERE phase = 1
                ),
                tcs AS (
                    SELECT view_number FROM timeout_certificates
                ),
                committed_blocks AS (
                    -- Only blocks that have Lock QCs (committed blocks)
                    SELECT b.view_number, b.block_hash, b.height
                    FROM blocks b
                    JOIN lock_qcs lq ON b.view_number = lq.view_number
                ),
                all_blocks AS (
                    -- All blocks for display (includes uncommitted)
                    SELECT view_number, block_hash FROM blocks
                )
                SELECT
                    v.view,
                    COALESCE(
                        MAX(cb.height) OVER (ORDER BY v.view ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                        0
                    ) as height,
                    pq.view_number IS NOT NULL as has_propose_qc,
                    lq.view_number IS NOT NULL as has_lock_qc,
                    tc.view_number IS NOT NULL as has_tc,
                    ab.block_hash
                FROM view_range v
                LEFT JOIN propose_qcs pq ON v.view = pq.view_number
                LEFT JOIN lock_qcs lq ON v.view = lq.view_number
                LEFT JOIN tcs tc ON v.view = tc.view_number
                LEFT JOIN committed_blocks cb ON v.view = cb.view_number
                LEFT JOIN all_blocks ab ON v.view = ab.view_number
                ORDER BY v.view"
            ).map_err(|_| DatabaseError::RecallError)?;

            let mut rows = stmt.query([]).map_err(|_| DatabaseError::RecallError)?;
            let mut history = Vec::new();

            while let Ok(Some(row)) = rows.next() {
                let view: i32 = row.get(0).map_err(|_| DatabaseError::RecallError)?;
                let height: i32 = row.get(1).map_err(|_| DatabaseError::RecallError)?;
                let has_propose_qc: bool = row.get(2).map_err(|_| DatabaseError::RecallError)?;
                let has_lock_qc: bool = row.get(3).map_err(|_| DatabaseError::RecallError)?;
                let has_tc: bool = row.get(4).map_err(|_| DatabaseError::RecallError)?;
                let block_hash: Option<Blake3Hash> = row.get(5).ok();

                history.push(ViewHistoryEntry {
                    view,
                    height,
                    has_propose_qc,
                    has_lock_qc,
                    has_tc,
                    block_hash: block_hash.map(|h| format!("{:.8}", h)),
                });
            }

            Ok(history)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get current view within a transaction - core logic
/// Get the leader for a specific view at a given height
/// Uses same SQL logic as get_consensus_with_conn leader_selection CTE
pub fn get_leader_for_view_tx(
    tx: &rusqlite::Transaction,
    view: i32,
    height: i32,
) -> Result<Option<Node>, DatabaseError> {
    let mut stmt = tx
        .prepare(
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
                ? % (SELECT COUNT(*) FROM active_validators)
            ) + 1
        )
        SELECT n.node_id, n.name, n.owner, n.pubkey
        FROM leader_selection ls
        JOIN nodes n ON ls.node_id = n.node_id
        ",
        )
        .map_err(|_| DatabaseError::RecallError)?;

    let result = stmt.query_row([height, view], |row| {
        Ok(Node {
            node_id: row.get(0)?,
            name: row.get(1)?,
            owner: row.get(2)?,
            pubkey: row.get(3)?,
        })
    });

    match result {
        Ok(node) => Ok(Some(node)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(_) => Err(DatabaseError::RecallError),
    }
}

/// Get the highest committed block height at or before a given view
/// Only counts blocks that have Lock QCs (committed blocks)
/// This ensures consistency with leader calculation and validator set selection
pub fn get_height_at_view_tx(tx: &rusqlite::Transaction, view: i32) -> Result<i32, DatabaseError> {
    let height: i32 = tx
        .query_row(
            "SELECT COALESCE(MAX(b.height), 0)
         FROM blocks b
         JOIN quorum_certificates qc ON b.view_number = qc.view_number
         WHERE qc.phase = 1 AND b.view_number <= ?",
            [view],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;
    Ok(height)
}

pub fn get_current_view_tx(tx: &rusqlite::Transaction) -> Result<i32, DatabaseError> {
    let view: i32 = tx
        .query_row(
            "SELECT current_view FROM this_node WHERE internal_id = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;
    Ok(view)
}

/// Get last propose vote block hash within a transaction
/// Used for double-vote detection in Ballot::propose
pub fn get_last_propose_vote_tx(
    tx: &rusqlite::Transaction,
) -> Result<Option<Blake3Hash>, DatabaseError> {
    let hash: Option<Blake3Hash> = tx
        .query_row(
            "SELECT last_propose_vote_block_hash FROM this_node WHERE internal_id = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;
    Ok(hash)
}

/// Get just the current view from this_node - works even when there are no validators
/// Used during catch-up for joining nodes before they have validators
/// Wrapper for backwards compatibility
pub fn get_current_view(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<i32, DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock
                .transaction()
                .map_err(|_| DatabaseError::LockError)?;
            get_current_view_tx(&tx)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Lightweight consensus progress: current view + highest QC view.
/// Used for message-driven catch-up decisions in the iroh handler.
/// Much cheaper than get_consensus() (one join, two integers, no CTEs).
pub fn get_consensus_progress(
    conn: &r2d2::PooledConnection<SqliteConnectionManager>,
) -> Result<(i32, i32), DatabaseError> {
    conn.query_row(
        "SELECT t.current_view, hb.view_number
         FROM this_node t
         JOIN blocks hb ON hb.block_hash = t.highest_qc_block_hash
         WHERE t.internal_id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .map_err(|_| DatabaseError::RecallError)
}

pub fn get_validators_with_conn(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
    height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    let mut stmt = db_lock
        .prepare(
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
        SELECT n.node_id, n.name, n.owner, n.pubkey
        FROM active_validators av
        JOIN nodes n ON av.node_id = n.node_id;
        ",
        )
        .map_err(|_| DatabaseError::RecallError)?;

    let results = stmt.query_map([height], |row| {
        Ok(Node {
            node_id: row.get(0)?,
            name: row.get(1)?,
            owner: row.get(2)?,
            pubkey: row.get(3)?,
        })
    });

    match results {
        Ok(rows) => {
            let nodes: Vec<Node> = rows.collect::<Result<_, _>>().map_err(|e| {
                tracing::debug!("Error collecting validator rows: {:?}", e);
                DatabaseError::ProcessingError
            })?;
            Ok(nodes)
        }
        Err(e) => {
            tracing::error!("Failed to query validators: {:?}", e);
            Err(DatabaseError::RecordError)
        }
    }
}

pub fn get_validators(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => get_validators_with_conn(&db_lock, height),
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn get_validators_elect_with_conn(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
    current_height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    let mut stmt = db_lock
        .prepare(
            "
        WITH validators_elect AS (
            SELECT DISTINCT node_id
            FROM validators
            WHERE effective_height > ? AND is_active = true
        )
        SELECT n.node_id, n.name, n.owner, n.pubkey
        FROM validators_elect ve
        JOIN nodes n ON ve.node_id = n.node_id;
        ",
        )
        .map_err(|_| DatabaseError::RecallError)?;

    let results = stmt.query_map([current_height], |row| {
        Ok(Node {
            node_id: row.get(0)?,
            name: row.get(1)?,
            owner: row.get(2)?,
            pubkey: row.get(3)?,
        })
    });

    match results {
        Ok(rows) => {
            let nodes: Vec<Node> = rows
                .collect::<Result<_, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?;
            Ok(nodes)
        }
        Err(e) => {
            tracing::error!("Failed to query validators elect: {:?}", e);
            Err(DatabaseError::RecordError)
        }
    }
}

pub fn get_validators_elect(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    current_height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => get_validators_elect_with_conn(&db_lock, current_height),
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn insert_block_with_conn(
    db_lock: &mut r2d2::PooledConnection<SqliteConnectionManager>,
    block: &Block,
) -> Result<(), DatabaseError> {
    let tx = db_lock
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| DatabaseError::LockError)?;

    // Insert the block (prepared_block_hash set later by insert_qc_unsafe_tx when Propose QC arrives)
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

    crate::db::shared::commit_timed(tx).map_err(|_| DatabaseError::InsertError)?;
    Ok(())
}

pub fn insert_block(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    block: &Block,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => insert_block_with_conn(&mut db_lock, block),
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get block within an existing transaction (core implementation)
pub fn get_block_tx(
    tx: &rusqlite::Transaction,
    block_hash: Blake3Hash,
) -> Result<Block, DatabaseError> {
    let mut stmt = tx.prepare(
        "SELECT block_hash, height, view_number, parent_hash, transactions FROM blocks WHERE block_hash = ?"
    ).map_err(|_| DatabaseError::RecallError)?;

    let result = stmt
        .query_row([block_hash], |row| {
            let block_hash: Blake3Hash = row.get(0)?;
            let height: i32 = row.get(1)?;
            let view_number: i32 = row.get(2)?;
            let parent_hash: Option<Blake3Hash> = row.get(3)?;
            let transactions: Option<Transactions> = row.get(4)?;

            Ok((block_hash, height, view_number, parent_hash, transactions))
        })
        .map_err(|_| DatabaseError::RecallError)?;

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

/// Get block (wrapper for backward compatibility)
pub fn get_block(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    block_hash: Blake3Hash,
) -> Result<Block, DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock
                .transaction()
                .map_err(|_| DatabaseError::LockError)?;
            get_block_tx(&tx, block_hash)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Insert transaction nonces inside an existing DB transaction (atomic with block commit).
/// Called by process_transactions for every committed block on all nodes.
pub fn insert_tx_nonces_tx(
    db_tx: &rusqlite::Transaction,
    nonces: &[hopnet_common::CustomUUID],
) -> Result<(), DatabaseError> {
    if nonces.is_empty() {
        return Ok(());
    }
    // Build a single INSERT with multiple VALUES rows for O(1) round-trips.
    // ON CONFLICT DO NOTHING prevents duplicate nonces from crashing the consensus commit.
    let placeholders: Vec<&str> = vec!["(?)"; nonces.len()];
    let sql = format!(
        "INSERT INTO committed_tx_nonces (nonce) VALUES {} ON CONFLICT DO NOTHING",
        placeholders.join(", ")
    );
    let params: Vec<&dyn rusqlite::ToSql> =
        nonces.iter().map(|n| n as &dyn rusqlite::ToSql).collect();
    db_tx.execute(&sql, params.as_slice()).map_err(|e| {
        tracing::error!(
            "Failed to insert {} transaction nonces: {:?}",
            nonces.len(),
            e
        );
        DatabaseError::InsertError
    })?;
    Ok(())
}

/// Check which nonces from the given batch are already committed.
/// Returns the set of nonce strings that exist in committed_tx_nonces.
pub fn check_committed_nonces(
    conn: &r2d2::PooledConnection<SqliteConnectionManager>,
    nonces: &[hopnet_common::CustomUUID],
) -> Result<std::collections::HashSet<String>, DatabaseError> {
    let mut committed = std::collections::HashSet::new();
    if nonces.is_empty() {
        return Ok(committed);
    }
    // Single query with IN (...) for O(1) round-trips.
    let placeholders: Vec<&str> = vec!["?"; nonces.len()];
    let sql = format!(
        "SELECT nonce FROM committed_tx_nonces WHERE nonce IN ({})",
        placeholders.join(", ")
    );
    let params: Vec<&dyn rusqlite::ToSql> =
        nonces.iter().map(|n| n as &dyn rusqlite::ToSql).collect();
    let mut stmt = conn.prepare(&sql).map_err(|_| DatabaseError::RecallError)?;
    let mut rows = stmt
        .query(params.as_slice())
        .map_err(|_| DatabaseError::RecallError)?;
    while let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
        let nonce_str: String = row.get(0).map_err(|_| DatabaseError::RecallError)?;
        committed.insert(nonce_str);
    }
    Ok(committed)
}

/// Delete nonces older than the cutoff UUID (UUIDv7 ordering = chronological).
/// Called inside a consensus transaction for deterministic cleanup across all nodes.
pub fn cleanup_old_nonces(
    db_tx: &rusqlite::Transaction,
    cutoff: &hopnet_common::CustomUUID,
) -> Result<usize, DatabaseError> {
    let deleted = db_tx
        .execute(
            "DELETE FROM committed_tx_nonces WHERE nonce < ?",
            params![cutoff],
        )
        .map_err(|_| DatabaseError::InsertError)?;
    Ok(deleted)
}

/// Get QC by hash within an existing transaction (core implementation)
pub fn get_quorum_certificate_by_hash_tx(
    tx: &rusqlite::Transaction,
    view_number: &i32,
    block_hash: &Blake3Hash,
    phase: &ConsensusPhase,
) -> Result<QuorumCertificate, DatabaseError> {
    let mut stmt = tx.prepare(
        "SELECT view_number, phase, block_hash, proposer_signature, voter_signatures FROM quorum_certificates WHERE view_number = ? AND phase = ? AND block_hash = ?"
    ).map_err(|_| DatabaseError::RecallError)?;

    let result = stmt
        .query_row(params![view_number, phase, block_hash], |row| {
            Ok(QuorumCertificate {
                view_number: row.get(0)?,
                phase: row.get(1)?,
                block_hash: row.get(2)?,
                proposer_signature: row.get(3)?,
                voter_signatures: row.get(4)?,
            })
        })
        .map_err(|_| DatabaseError::RecallError)?;
    Ok(result)
}

/// Get QC by hash (wrapper for backward compatibility)
pub fn get_quorum_certificate_by_hash(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    view_number: &i32,
    block_hash: &Blake3Hash,
    phase: &ConsensusPhase,
) -> Result<QuorumCertificate, DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock
                .transaction()
                .map_err(|_| DatabaseError::LockError)?;
            get_quorum_certificate_by_hash_tx(&tx, view_number, block_hash, phase)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Insert TC within an existing transaction (UNSAFE - skips QC validation)
/// Caller MUST ensure tc.highest_qc has been validated or doesn't need validation
/// Use cases: insert_tc_safe (after validation), legacy insert_tc (deprecated)
fn insert_tc_unsafe_tx(
    tx: &rusqlite::Transaction,
    tc: &TimeoutCertificate,
) -> Result<(), DatabaseError> {
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

    // Update consensus state to new view and clear prepared_block_hash + last_propose_vote_block_hash (Bug #6 fix)
    let new_view = tc.view_number + 1;
    tracing::debug!(
        "[DB WRITE this_node] About to UPDATE for TC in view {} -> {}",
        tc.view_number,
        new_view
    );
    tx.execute(
        "UPDATE this_node SET current_view = ?, current_phase = 0, prepared_block_hash = NULL, last_propose_vote_block_hash = NULL WHERE internal_id = 1",
        params![new_view]
    ).map_err(|e| {
        tracing::error!("[DB WRITE this_node] UPDATE failed for TC: {:?}", e);
        DatabaseError::InsertError
    })?;
    tracing::debug!(
        "[DB WRITE this_node] Updated consensus state for TC view {} -> {}",
        tc.view_number,
        new_view
    );

    Ok(())
}

/// Safe TC insertion with QC extraction and validation (Bug #6 and #7 fixes)
pub fn insert_tc_safe(
    app_state: &crate::AppState,
    tc: TimeoutCertificate,
) -> Result<(), DatabaseError> {
    match app_state.db_pool.get() {
        Ok(mut db_lock) => {
            let _wg = app_state.write_gate.guard();
            let tx = db_lock
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|_| DatabaseError::LockError)?;

            // Step 1: Check if we already have the QC
            match get_quorum_certificate_by_hash_tx(
                &tx,
                &tc.highest_qc.view_number,
                &tc.highest_qc.block_hash,
                &tc.highest_qc.phase,
            ) {
                Ok(_existing_qc) => {
                    // QC exists → block must exist (QC insertion guarantees this)
                    // Safe to proceed to TC insertion
                    tracing::debug!(
                        "TC's QC for view {} already exists, proceeding with TC insertion",
                        tc.highest_qc.view_number
                    );
                }
                Err(_) => {
                    // Step 2: QC missing - check if we have block
                    match get_block_tx(&tx, tc.highest_qc.block_hash) {
                        Ok(block) => {
                            // Have block but not QC (received ballot, missed QC)
                            // Verify QC before inserting
                            tc.highest_qc.verify(app_state, &block).map_err(|e| {
                                tracing::warn!(
                                    "TC's QC verification failed for view {}: {:?}",
                                    tc.highest_qc.view_number,
                                    e
                                );
                                DatabaseError::ValidationError
                            })?;

                            // Insert QC (Bug #7 fix - extract and integrate TC's highest_qc)
                            insert_qc_unsafe_tx(&tx, &tc.highest_qc)?;
                            tracing::info!(
                                "Inserted missing QC from TC for view {}",
                                tc.highest_qc.view_number
                            );
                        }
                        Err(_) => {
                            // Missing both block and QC - reject TC
                            // Cannot safely advance view without justification
                            tracing::warn!(
                                "TC for view {} references unknown block {:?}, rejecting - catch-up needed",
                                tc.view_number,
                                tc.highest_qc.block_hash
                            );
                            return Err(DatabaseError::ValidationError);
                        }
                    }
                }
            }

            // Now safe: QC and block both exist, can advance view
            insert_tc_unsafe_tx(&tx, &tc)?;

            crate::db::shared::commit_timed(tx).map_err(|_| DatabaseError::InsertError)?;
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Insert QC within an existing transaction (UNSAFE - skips validation)
/// Caller MUST ensure qc.verify() has been called before using this function
/// Use cases: genesis setup (trusted), post-verification insertion
pub fn insert_qc_unsafe_tx(
    tx: &rusqlite::Transaction,
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
            tracing::debug!(
                "[DB WRITE this_node] About to UPDATE for Propose QC in view {}",
                qc.view_number
            );
            tx.execute(
                "UPDATE this_node SET highest_qc_block_hash = ?, highest_qc_phase = 0, current_phase = 1, prepared_block_hash = ? WHERE internal_id = 1",
                params![qc.block_hash, qc.block_hash]
            ).map_err(|e| {
                tracing::error!("[DB WRITE this_node] UPDATE failed for Propose QC: {:?}", e);
                DatabaseError::InsertError
            })?;
            tracing::info!(
                "[DB WRITE this_node] Updated consensus state: propose -> lock phase for view {}, set prepared_block_hash",
                qc.view_number
            );
        }
        ConsensusPhase::Lock => {
            // If QC phase is lock, change to propose, set current_view to QC view + 1,
            // commit the block, and clear prepared_block_hash + last_propose_vote_block_hash (consensus completed)
            tracing::debug!(
                "[DB WRITE this_node] About to UPDATE for Lock QC in view {}",
                qc.view_number
            );
            tx.execute(
                "UPDATE this_node SET highest_qc_block_hash = ?, highest_qc_phase = 1, committed_block_hash = ?, current_phase = 0, current_view = ?, prepared_block_hash = NULL, last_propose_vote_block_hash = NULL WHERE internal_id = 1",
                params![qc.block_hash, qc.block_hash, qc.view_number + 1]
            ).map_err(|e| {
                tracing::error!("[DB WRITE this_node] UPDATE failed for Lock QC: {:?}", e);
                DatabaseError::InsertError
            })?;
            tracing::info!(
                "[DB WRITE this_node] Updated consensus state: lock -> propose, view {} -> {}, committed block {:?}, cleared prepared_block_hash and last_propose_vote_block_hash",
                qc.view_number,
                qc.view_number + 1,
                qc.block_hash
            );
        }
    }

    Ok(())
}

pub fn get_node_pubkey(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    node_id: i32,
) -> Result<PubKey, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock
                .prepare("SELECT pubkey FROM nodes WHERE node_id = ?")
                .map_err(|_| DatabaseError::RecallError)?;

            let node_pubkey: PubKey = stmt
                .query_row([node_id], |row| row.get(0))
                .map_err(|_| DatabaseError::RecallError)?;

            Ok(node_pubkey)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn get_all_node_pubkeys(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
) -> Result<std::collections::HashMap<i32, PubKey>, DatabaseError> {
    let mut stmt = db_lock
        .prepare("SELECT node_id, pubkey FROM nodes")
        .map_err(|_| DatabaseError::RecallError)?;

    let rows = stmt
        .query_map([], |row| {
            let node_id: i32 = row.get(0)?;
            let pubkey: PubKey = row.get(1)?;
            Ok((node_id, pubkey))
        })
        .map_err(|_| DatabaseError::RecallError)?;

    let mut map = std::collections::HashMap::new();
    for row in rows {
        let (node_id, pubkey) = row.map_err(|_| DatabaseError::RecallError)?;
        map.insert(node_id, pubkey);
    }

    Ok(map)
}

pub fn get_all_user_pubkeys(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
) -> Result<std::collections::HashMap<i32, PubKey>, DatabaseError> {
    let mut stmt = db_lock
        .prepare("SELECT user_id, pubkey FROM users")
        .map_err(|_| DatabaseError::RecallError)?;

    let rows = stmt
        .query_map([], |row| {
            let user_id: i32 = row.get(0)?;
            let pubkey: PubKey = row.get(1)?;
            Ok((user_id, pubkey))
        })
        .map_err(|_| DatabaseError::RecallError)?;

    let mut map = std::collections::HashMap::new();
    for row in rows {
        let (user_id, pubkey) = row.map_err(|_| DatabaseError::RecallError)?;
        map.insert(user_id, pubkey);
    }

    Ok(map)
}

pub fn get_me(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<MyNode, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock
                .prepare(
                    "
                SELECT node_id, privkey FROM this_node
                ",
                )
                .map_err(|_| DatabaseError::RecallError)?;

            let result = stmt
                .query_row([], |row| {
                    let node_id: i32 = row.get(0)?;
                    let privkey: PrivKey = row.get(1)?;

                    Ok(MyNode { node_id, privkey })
                })
                .map_err(|_| DatabaseError::RecallError)?;

            Ok(result)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Startup state loaded from database on node restart
pub struct StartupState {
    pub node_id: i32,
    pub user_id: i32,
    pub node_privkey: PrivKey,
}

/// Load all necessary state from database for node restart
/// This includes node_id, user_id, node private key, and user private key
pub fn get_startup_state(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<StartupState, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // First, get node_id and privkey from this_node table
            let mut stmt = db_lock
                .prepare("SELECT node_id, privkey FROM this_node")
                .map_err(|_| DatabaseError::RecallError)?;

            let (node_id, node_privkey) = stmt
                .query_row([], |row| {
                    let node_id: i32 = row.get(0)?;
                    let node_privkey: PrivKey = row.get(1)?;
                    Ok((node_id, node_privkey))
                })
                .map_err(|_| DatabaseError::RecallError)?;

            // Now get user_id from nodes table
            let mut stmt = db_lock
                .prepare("SELECT owner FROM nodes WHERE node_id = ?")
                .map_err(|_| DatabaseError::RecallError)?;

            let user_id: i32 = stmt
                .query_row([node_id], |row| row.get(0))
                .map_err(|_| DatabaseError::RecallError)?;

            Ok(StartupState {
                node_id,
                user_id,
                node_privkey,
            })
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn mark_timeout_vote_issued_with_conn(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
    view: i32,
) -> Result<(), DatabaseError> {
    tracing::debug!(
        "[DB WRITE this_node] About to UPDATE for timeout vote in view {}",
        view
    );
    db_lock
        .execute(
            "UPDATE this_node SET last_timeout_vote_view = ? WHERE internal_id = 1",
            params![view],
        )
        .map_err(|e| {
            tracing::error!(
                "[DB WRITE this_node] UPDATE failed for timeout vote: {:?}",
                e
            );
            DatabaseError::InsertError
        })?;
    tracing::debug!(
        "[DB WRITE this_node] Marked timeout vote issued for view {}",
        view
    );
    Ok(())
}

pub fn mark_timeout_vote_issued(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    view: i32,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(db_lock) => mark_timeout_vote_issued_with_conn(&db_lock, view),
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Update last_propose_vote within an existing transaction (core implementation)
/// This allows the update to participate in the same transaction as Lock QC insertion
pub fn update_last_propose_vote_tx(
    tx: &rusqlite::Transaction,
    block_hash: Blake3Hash,
) -> Result<(), DatabaseError> {
    tracing::debug!(
        "[DB WRITE this_node] About to UPDATE for last_propose_vote with block {:?}",
        block_hash
    );
    tx.execute(
        "UPDATE this_node SET last_propose_vote_block_hash = ? WHERE internal_id = 1",
        params![block_hash],
    )
    .map_err(|e| {
        tracing::error!(
            "[DB WRITE this_node] UPDATE failed for last_propose_vote: {:?}",
            e
        );
        DatabaseError::InsertError
    })?;
    tracing::debug!(
        "[DB WRITE this_node] Updated last_propose_vote_block_hash to {:?}",
        block_hash
    );
    Ok(())
}

/// Update last_propose_vote (wrapper for standalone calls)
/// Creates its own transaction and commits immediately
pub fn update_last_propose_vote(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    block_hash: Blake3Hash,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|_| DatabaseError::LockError)?;
            update_last_propose_vote_tx(&tx, block_hash)?;
            crate::db::shared::commit_timed(tx).map_err(|_| DatabaseError::InsertError)?;
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn get_view_consensus_data(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    view: i32,
) -> Result<ViewConsensusData, DatabaseError> {
    use crate::consensus::types::*;
    use rusqlite::OptionalExtension;

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
            let mut qc_stmt = db_lock
                .prepare(
                    "SELECT view_number, phase, block_hash, proposer_signature, voter_signatures
                 FROM quorum_certificates WHERE view_number = ?",
                )
                .map_err(|_| DatabaseError::RecallError)?;

            let qc_rows = qc_stmt
                .query_map(params![view], |row| {
                    Ok(QuorumCertificate {
                        view_number: row.get(0)?,
                        phase: row.get(1)?,
                        block_hash: row.get(2)?,
                        proposer_signature: row.get(3)?,
                        voter_signatures: row.get(4)?,
                    })
                })
                .map_err(|_| DatabaseError::RecallError)?;

            let mut propose_qc = None;
            let mut lock_qc = None;
            let mut block_hashes = Vec::new();

            for qc_result in qc_rows {
                let qc = qc_result.map_err(|_| DatabaseError::RecallError)?;
                block_hashes.push(qc.block_hash);
                match qc.phase {
                    ConsensusPhase::Propose => propose_qc = Some(qc),
                    ConsensusPhase::Lock => lock_qc = Some(qc),
                }
            }

            // Add block from timeout certificate if present
            if let Some(ref tc) = timeout_certificate {
                block_hashes.push(tc.highest_qc.block_hash);
            }

            // Get all referenced blocks (deduplicate hashes)
            let mut blocks = Vec::new();
            let mut seen_hashes = std::collections::HashSet::new();
            for block_hash in block_hashes {
                if seen_hashes.insert(block_hash)
                    && let Some(block) = db_lock
                        .query_row(
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
                                    },
                                })
                            },
                        )
                        .optional()
                        .map_err(|_| DatabaseError::RecallError)?
                {
                    blocks.push(block);
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
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get the current consensus height (height of the committed block)
/// This is used consistently across the system for modification tracking
pub fn get_current_consensus_height(tx: &rusqlite::Transaction) -> Result<i32, DatabaseError> {
    use rusqlite::OptionalExtension;

    let current_height: Option<i32> = tx
        .query_row(
            "SELECT COALESCE(b.height, 0) as committed_height
         FROM this_node t
         LEFT JOIN blocks b ON t.committed_block_hash = b.block_hash
         WHERE t.internal_id = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    // Return 0 if this_node doesn't exist yet (genesis case)
    Ok(current_height.unwrap_or(0))
}

/// Check if a node is active at a given height
/// Returns true if the node has an active validator entry effective at or before the given height
pub fn is_node_active(
    tx: &rusqlite::Transaction,
    node_id: i32,
    height: i32,
) -> Result<bool, DatabaseError> {
    use rusqlite::OptionalExtension;

    // Get the most recent validator record at or before this height
    let is_active: Option<bool> = tx
        .query_row(
            "SELECT is_active FROM validators
         WHERE node_id = ? AND effective_height <= ?
         ORDER BY effective_height DESC
         LIMIT 1",
            params![node_id, height],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    // If no record found, node is not active
    Ok(is_active.unwrap_or(false))
}

/// Activate a validator at a specific effective height
/// If the node already has a future activation (after current height), it will be updated
/// This enables hot-swap operations where validator-elect activation can be moved forward
pub fn activate_validator(
    tx: &rusqlite::Transaction,
    node_id: i32,
    effective_height: i32,
) -> Result<(), DatabaseError> {
    use rusqlite::OptionalExtension;

    let current_height = get_current_consensus_height(tx)?;

    // Check if node already has the NEXT future activation (earliest after current height)
    let existing_future_activation: Option<i32> = tx
        .query_row(
            "SELECT effective_height FROM validators
         WHERE node_id = ? AND effective_height > ? AND is_active = true
         ORDER BY effective_height ASC
         LIMIT 1",
            params![node_id, current_height],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    if let Some(old_height) = existing_future_activation {
        // UPDATE the existing next activation to new height
        tx.execute(
            "UPDATE validators SET effective_height = ?
             WHERE node_id = ? AND effective_height = ?",
            params![effective_height, node_id, old_height],
        )
        .map_err(|_| DatabaseError::InsertError)?;

        tracing::info!(
            "Updated activation for node {} from height {} to height {}",
            node_id,
            old_height,
            effective_height
        );
    } else {
        // INSERT new activation record
        tx.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
            params![effective_height, node_id, true],
        )
        .map_err(|_| DatabaseError::InsertError)?;

        tracing::info!(
            "Scheduled activation for node {} at height {}",
            node_id,
            effective_height
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SqliteConnectionManager;
    use ed25519_dalek::SigningKey;
    use rand_core::OsRng;

    fn setup_test_db() -> r2d2::Pool<SqliteConnectionManager> {
        let manager = SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();

        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        pool
    }

    fn generate_test_pubkey() -> crate::db::PubKey {
        let signing_key = SigningKey::generate(&mut OsRng);
        crate::db::PubKey(signing_key.verifying_key())
    }

    #[test]
    fn test_get_validators_empty() {
        let pool = setup_test_db();

        // Query at height 0 with no validators should return empty list
        let validators = get_validators(pool.get(), 0).unwrap();
        assert_eq!(validators.len(), 0);
    }

    #[test]
    fn test_get_validators_basic_activation() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();
        let node1_pubkey = generate_test_pubkey();
        let node2_pubkey = generate_test_pubkey();

        // Insert test user
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        // Insert test nodes
        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![1, "node1", 1, &node1_pubkey],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![2, "node2", 1, &node2_pubkey],
        )
        .unwrap();

        // Activate validators at different heights
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, 1, true)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (20, 2, true)",
            [],
        )
        .unwrap();

        drop(conn);

        // At height 5: no validators active yet
        let validators = get_validators(pool.get(), 5).unwrap();
        assert_eq!(validators.len(), 0);

        // At height 15: only node 1 active
        let validators = get_validators(pool.get(), 15).unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].node_id, 1);
        // At height 25: both nodes active
        let validators = get_validators(pool.get(), 25).unwrap();
        assert_eq!(validators.len(), 2);
        // Results should be ordered by node_id
        assert_eq!(validators[0].node_id, 1);
        assert_eq!(validators[1].node_id, 2);
    }

    #[test]
    fn test_get_validators_with_deactivation() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();
        let node_pubkey = generate_test_pubkey();

        // Insert test user and node
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![1, "node1", 1, &node_pubkey],
        )
        .unwrap();

        // Activate at height 10, deactivate at height 30
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, 1, true)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (30, 1, false)",
            [],
        )
        .unwrap();

        drop(conn);

        // At height 20: active
        let validators = get_validators(pool.get(), 20).unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].node_id, 1);

        // At height 35: deactivated
        let validators = get_validators(pool.get(), 35).unwrap();
        assert_eq!(validators.len(), 0);
    }

    #[test]
    fn test_get_validators_reactivation() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();
        let node_pubkey = generate_test_pubkey();

        // Insert test user and node
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![1, "node1", 1, &node_pubkey],
        )
        .unwrap();

        // Activate, deactivate, reactivate
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, 1, true)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (30, 1, false)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (50, 1, true)",
            [],
        )
        .unwrap();

        drop(conn);

        // At height 20: active (first activation)
        let validators = get_validators(pool.get(), 20).unwrap();
        assert_eq!(validators.len(), 1);

        // At height 40: inactive (deactivated)
        let validators = get_validators(pool.get(), 40).unwrap();
        assert_eq!(validators.len(), 0);

        // At height 60: active again (reactivated)
        let validators = get_validators(pool.get(), 60).unwrap();
        assert_eq!(validators.len(), 1);
    }

    #[test]
    fn test_get_validators_multiple_nodes() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();

        // Insert test user
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        // Insert 5 test nodes
        for i in 1..=5 {
            let node_pubkey = generate_test_pubkey();
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey)
                 VALUES (?, ?, ?, ?)",
                params![i, format!("node{}", i), 1, &node_pubkey],
            )
            .unwrap();

            // All activate at height 10
            conn.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, ?, true)",
                params![i]
            ).unwrap();
        }

        // Deactivate nodes 2 and 4 at height 30
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (30, 2, false)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (30, 4, false)",
            [],
        )
        .unwrap();

        drop(conn);

        // At height 20: all 5 nodes active
        let validators = get_validators(pool.get(), 20).unwrap();
        assert_eq!(validators.len(), 5);

        // At height 40: only nodes 1, 3, 5 active (2 and 4 deactivated)
        let validators = get_validators(pool.get(), 40).unwrap();
        assert_eq!(validators.len(), 3);
        assert_eq!(validators[0].node_id, 1);
        assert_eq!(validators[1].node_id, 3);
        assert_eq!(validators[2].node_id, 5);
    }

    #[test]
    fn test_get_validators_query_past_height() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();
        let node_pubkey = generate_test_pubkey();

        // Insert test user and node
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![1, "node1", 1, &node_pubkey],
        )
        .unwrap();

        // Activate at height 10
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, 1, true)",
            [],
        )
        .unwrap();

        drop(conn);

        // Query at height 100 (far in future): should still return node 1 as active
        let validators = get_validators(pool.get(), 100).unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].node_id, 1);
    }
}
