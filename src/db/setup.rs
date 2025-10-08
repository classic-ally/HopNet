use super::*;
use crate::consensus::{types::{Block, BlockData, VoteSignMessage}, ConsensusPhase, QuorumCertificate};
use axum::http::StatusCode;

pub fn get_initial_setup(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<StatusCode, DatabaseError> {
    match db_connection {
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

/// Initialize sequences to 0 - used by genesis block and network creation
/// Operates within provided transaction for atomicity
pub fn initialize_sequences_tx(tx: &duckdb::Transaction) -> Result<(), DatabaseError> {
    tx.execute_batch("
        INSERT INTO sequences (name, next_id) VALUES ('users', 0);
        INSERT INTO sequences (name, next_id) VALUES ('nodes', 0);
    ").map_err(|_| DatabaseError::InsertError)?;
    Ok(())
}

/// Initialize a new HopNet network by creating a genesis block with genesis transaction
///
/// This creates the initial network state by:
/// 1. Creating a genesis transaction containing the initial user and node
/// 2. Creating a genesis block (height 0) containing this transaction
/// 3. Processing the transaction through the InsertGenesisHandler (initializes sequences, creates user/node, activates validator)
/// 4. Creating genesis QCs for the block
/// 5. Initializing this_node state
///
/// This approach ensures the genesis block contains transactions (not empty), allowing new nodes
/// to bootstrap by replaying the genesis transaction through catch-up rather than requiring
/// a separate checkpoint synchronization mechanism.
pub fn post_initial_setup(
    state: &crate::AppState,
    user: User,
    node: Node,
    user_privkey: PrivKey
) -> Result<(i32, i32), DatabaseError> {
    use crate::consensus::handlers::GenesisPayload;
    use crate::consensus::types::{Transaction, Transactions};

    tracing::debug!("post_initial_setup: Starting genesis setup");

    // Create genesis payload with user and node
    let genesis_payload = GenesisPayload {
        user: user.clone(),
        node: node.clone(),
    };
    tracing::debug!("post_initial_setup: Created genesis payload");

    // Encode the payload
    let payload_bytes = bincode::serde::encode_to_vec(&genesis_payload, bincode::config::standard())
        .map_err(|e| {
            tracing::error!("post_initial_setup: Failed to encode genesis payload: {:?}", e);
            DatabaseError::ProcessingError
        })?;
    tracing::debug!("post_initial_setup: Encoded payload ({} bytes)", payload_bytes.len());

    // Create genesis transaction (signed by node)
    let genesis_tx = Transaction::new(
        "insert_genesis".to_string(),
        payload_bytes,
        0,  // Genesis node_id
        &state.private_key
    ).map_err(|e| {
        tracing::error!("post_initial_setup: Failed to create genesis transaction: {:?}", e);
        DatabaseError::ProcessingError
    })?;
    tracing::debug!("post_initial_setup: Created genesis transaction");

    // Create genesis block with the transaction
    let genesis_block = Block::new(
        BlockData {
            height: 0,
            view_number: 0,
            parent_hash: None,
            transactions: Some(Transactions(vec![genesis_tx.clone()])),
        }
    ).map_err(|e| {
        tracing::error!("post_initial_setup: Failed to create genesis block: {:?}", e);
        DatabaseError::ProcessingError
    })?;
    tracing::debug!("post_initial_setup: Created genesis block with hash {:?}", genesis_block.block_hash);

    // === TRANSACTION 1: Genesis block only ===
    {
        let mut conn = state.db_pool.get().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to get DB connection for block: {:?}", e);
            DatabaseError::LockError
        })?;
        tracing::debug!("post_initial_setup: Got database connection for block");

        let tx_db = conn.transaction().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to start transaction for block: {:?}", e);
            DatabaseError::LockError
        })?;
        tracing::debug!("post_initial_setup: Started transaction for block");

        // Insert genesis block
        tx_db.execute(
            "INSERT INTO blocks (block_hash, height, view_number, transactions) VALUES (?, ?, ?, ?)",
            params![genesis_block.block_hash, genesis_block.data.height, genesis_block.data.view_number, genesis_block.data.transactions]
        ).map_err(|e| {
            tracing::error!("post_initial_setup: Failed to insert genesis block: {:?}", e);
            DatabaseError::InsertError
        })?;
        tracing::debug!("post_initial_setup: Inserted genesis block into database");

        // Commit block
        tx_db.commit().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to commit genesis block: {:?}", e);
            DatabaseError::InsertError
        })?;
        tracing::debug!("post_initial_setup: Committed genesis block");
    }

    // === PROCESS GENESIS TRANSACTION VIA HANDLER ===
    // Handler can now see committed genesis block
    // get_current_consensus_height returns 0 (genesis bypass - this_node doesn't exist yet)
    // This will initialize sequences, insert user/node, activate validator
    tracing::debug!("post_initial_setup: About to process genesis transaction via handler");
    {
        let mut conn = state.db_pool.get().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to get DB connection for genesis transaction: {:?}", e);
            DatabaseError::LockError
        })?;
        let genesis_tx_db = conn.transaction().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to begin transaction for genesis transaction: {:?}", e);
            DatabaseError::InsertError
        })?;

        crate::consensus::functions::process_transaction(&genesis_tx, state, true, &genesis_tx_db).map_err(|e| {
            tracing::error!("post_initial_setup: Handler failed to process genesis transaction: {:?}", e);
            e
        })?;

        genesis_tx_db.commit().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to commit genesis transaction: {:?}", e);
            DatabaseError::InsertError
        })?;
        tracing::debug!("post_initial_setup: Handler completed successfully");
    }

    // === TRANSACTION 2: this_node + QCs (atomic) ===
    // Now node_id=0 exists (created by handler), so this_node foreign key will work
    {
        let mut conn = state.db_pool.get().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to get DB connection for this_node+QCs: {:?}", e);
            DatabaseError::LockError
        })?;
        tracing::debug!("post_initial_setup: Got database connection for this_node+QCs");

        let tx_db = conn.transaction().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to start transaction for this_node+QCs: {:?}", e);
            DatabaseError::LockError
        })?;
        tracing::debug!("post_initial_setup: Started transaction for this_node+QCs");

        // Initialize this_node with genesis state
        tracing::debug!("post_initial_setup: Inserting this_node entry");
        tx_db.execute(
            "INSERT INTO this_node (internal_id, node_id, privkey, current_view, current_phase, committed_block_hash, highest_qc_block_hash, user_privkey) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![1, 0, state.private_key, 0, ConsensusPhase::Propose, genesis_block.block_hash, genesis_block.block_hash, user_privkey]
        ).map_err(|e| {
            tracing::error!("post_initial_setup: Failed to insert this_node: {:?}", e);
            DatabaseError::InsertError
        })?;
        tracing::debug!("post_initial_setup: Inserted this_node");

        // Create quorum certificates for genesis block
        let signatures: Vec<VoteSignMessage> = Vec::new();
        tracing::debug!("post_initial_setup: Creating genesis QCs");

        let genesis_qc_1 = QuorumCertificate::create(
            &genesis_block,
            ConsensusPhase::Propose,
            0,  // node_id will be 0 for genesis
            &state.private_key,
            signatures.clone()
        ).map_err(|e| {
            tracing::error!("post_initial_setup: Failed to create genesis propose QC: {:?}", e);
            DatabaseError::ProcessingError
        })?;
        tracing::debug!("post_initial_setup: Created propose QC");

        let genesis_qc_2 = QuorumCertificate::create(
            &genesis_block,
            ConsensusPhase::Lock,
            0,  // node_id will be 0 for genesis
            &state.private_key,
            signatures
        ).map_err(|e| {
            tracing::error!("post_initial_setup: Failed to create genesis lock QC: {:?}", e);
            DatabaseError::ProcessingError
        })?;
        tracing::debug!("post_initial_setup: Created lock QC");

        // Use insert_qc_tx to handle QC insertion and state transitions
        tracing::debug!("post_initial_setup: Inserting propose QC");
        super::consensus::insert_qc_tx(&tx_db, &genesis_qc_1).map_err(|e| {
            tracing::error!("post_initial_setup: Failed to insert propose QC: {:?}", e);
            e
        })?;
        tracing::debug!("post_initial_setup: Inserted propose QC");

        tracing::debug!("post_initial_setup: Inserting lock QC");
        super::consensus::insert_qc_tx(&tx_db, &genesis_qc_2).map_err(|e| {
            tracing::error!("post_initial_setup: Failed to insert lock QC: {:?}", e);
            e
        })?;
        tracing::debug!("post_initial_setup: Inserted lock QC");

        // Commit this_node + QCs atomically
        tracing::debug!("post_initial_setup: Committing this_node+QCs transaction");
        tx_db.commit().map_err(|e| {
            tracing::error!("post_initial_setup: Failed to commit this_node+QCs: {:?}", e);
            DatabaseError::InsertError
        })?;
        tracing::debug!("post_initial_setup: Committed this_node+QCs");
    }

    tracing::info!("Successfully completed initial database setup for node 0");

    Ok((0, 0))  // Genesis always creates user_id=0, node_id=0
}

/// Initialize a joining node's database for catch-up based bootstrap
///
/// This creates ONLY the this_node table entry with identity and keys.
/// All other state (sequences, users, nodes, validators, blocks, QCs) comes from
/// catch-up replay starting at genesis (view 0).
///
/// After this initialization, the node should:
/// 1. Perform catch-up from view 0 to current_height
/// 2. Submit activation request after catching up
pub fn initialize_joining_node(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    join_info: crate::types::JoinInfo,
    node_privkey: PrivKey,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // Initialize this_node with identity and keys
            // All consensus state starts at view 0, will be populated by catch-up
            db_lock.execute(
                "INSERT INTO this_node (internal_id, node_id, privkey, user_privkey, current_view, current_phase, committed_block_hash, highest_qc_block_hash, prepared_block_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    1,
                    join_info.node_id,
                    node_privkey,
                    join_info.user_privkey,
                    0,  // Start at view 0 for catch-up
                    ConsensusPhase::Propose,
                    None::<Blake3Hash>,  // Will be set by genesis catch-up
                    None::<Blake3Hash>,  // Will be set by genesis catch-up
                    None::<Blake3Hash>,  // Will be set by genesis catch-up
                ]
            ).map_err(|e| {
                tracing::error!("Failed to initialize this_node for joining node {}: {:?}", join_info.node_id, e);
                DatabaseError::InsertError
            })?;

            tracing::info!(
                "Initialized joining node {} (user_id={}) for catch-up from view 0",
                join_info.node_id,
                join_info.user_id
            );

            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to get database connection for initialize_joining_node: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}

