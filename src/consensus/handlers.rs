use crate::{
    AppState,
    consensus::types::Transaction,
    db::{
        DatabaseError,
        consensus::{activate_validator, get_current_consensus_height, is_node_active},
        nodes::insert_node_tx,
        setup::initialize_sequences_tx,
        users::insert_user_tx,
    },
    handlers::{HandlerResult, TransactionHandler},
    types::{Node, User},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActivationRequest {
    pub node_id: i32,
    pub current_height: i32, // Proof node is caught up
                             // Activation height computed automatically during execution as committed_height + 1
}

pub struct ValidatorActivationHandler;

impl TransactionHandler for ValidatorActivationHandler {
    fn name(&self) -> &'static str {
        "validator_activation"
    }

    fn process(
        &self,
        state: &AppState,
        tx: &Transaction,
        execute: bool,
        db_tx: &rusqlite::Transaction,
    ) -> HandlerResult {
        // Decode activation request payload
        let (activation_req, _) = bincode::serde::decode_from_slice::<ActivationRequest, _>(
            &tx.rpc.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: Only the node itself can request its own activation
        // This check happens regardless of execute flag (determines vote)
        if activation_req.node_id != tx.submitter.id {
            tracing::warn!(
                "Authorization failed: node {} attempted to activate node {}",
                tx.submitter.id,
                activation_req.node_id
            );
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation phase: checks determine vote (YES/NO) but don't affect execution
        if !execute {
            // Synchronization check: Node must be caught up (within 10 views tolerance)
            let consensus_height = get_current_consensus_height(db_tx)?;
            if activation_req.current_height < consensus_height - 10 {
                tracing::warn!(
                    "Node {} activation failed validation: not caught up (reported height {}, consensus height {})",
                    activation_req.node_id,
                    activation_req.current_height,
                    consensus_height
                );
                return Err(DatabaseError::ProcessingError);
            }

            // Validation passed - vote YES
            tracing::debug!(
                "Node {} activation request validated (current_height={}, consensus_height={})",
                activation_req.node_id,
                activation_req.current_height,
                consensus_height
            );
            return Ok(());
        }

        // Execution phase: always execute if consensus succeeded (≥2/3 voted YES)
        // This ensures all nodes perform identical state changes regardless of individual validation
        if execute {
            // Compute activation height deterministically during execution
            // All nodes are synchronized to same committed height by Lock QC insertion
            let committed_height = get_current_consensus_height(db_tx)?;
            let effective_height = committed_height; // Activate immediately.

            tracing::info!(
                "Activating node {} at effective height {} (committed_height={}, next_block={})",
                activation_req.node_id,
                effective_height,
                committed_height,
                committed_height + 1
            );

            // Activate the validator (INSERT or UPDATE future activation)
            activate_validator(db_tx, activation_req.node_id, effective_height)?;
        }

        Ok(())
    }
}

inventory::submit! {
    &ValidatorActivationHandler as &dyn TransactionHandler
}

// ============================================================================
// Genesis Handler - Creates initial network state from genesis transaction
// ============================================================================
//
// The genesis handler initializes a new HopNet network by processing the
// genesis transaction embedded in the genesis block (height 0). This handler
// is called in two contexts:
//
// 1. Initial network creation (post_initial_setup):
//    - Creates genesis transaction with initial user and node
//    - Processes it to initialize sequences and create first validator
//
// 2. New node bootstrap (catch-up):
//    - New nodes replay the genesis transaction from the genesis block
//    - Builds identical initial state without separate checkpoint sync
//
// Security: Genesis handler validates it's only called once (checks sequences
// table is empty). Attempting to process genesis on initialized database fails.

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenesisPayload {
    pub user: User,
    pub node: Node,
}

pub struct InsertGenesisHandler;

impl TransactionHandler for InsertGenesisHandler {
    fn name(&self) -> &'static str {
        "insert_genesis"
    }

    fn process(
        &self,
        state: &AppState,
        tx: &Transaction,
        execute: bool,
        db_tx: &rusqlite::Transaction,
    ) -> HandlerResult {
        tracing::debug!("InsertGenesisHandler: Starting (execute={})", execute);

        // Decode genesis payload
        let (genesis_data, _) = bincode::serde::decode_from_slice::<GenesisPayload, _>(
            &tx.rpc.payload,
            bincode::config::standard(),
        )
        .map_err(|e| {
            tracing::error!(
                "InsertGenesisHandler: Failed to decode genesis payload: {:?}",
                e
            );
            DatabaseError::InvalidPayload
        })?;
        tracing::debug!("InsertGenesisHandler: Decoded genesis payload");

        // Safety check: Only allow genesis handler at height 0
        // Check if sequences already exist - if so, genesis already processed
        // Fail if query fails (don't proceed with unknown database state)
        tracing::debug!("InsertGenesisHandler: Checking if sequences exist");
        let existing_sequences: i32 = db_tx
            .query_row("SELECT COUNT(*) FROM sequences", [], |row| row.get(0))
            .map_err(|e| {
                tracing::error!("InsertGenesisHandler: Failed to query sequences: {:?}", e);
                DatabaseError::RecallError
            })?;
        tracing::debug!(
            "InsertGenesisHandler: Found {} existing sequences",
            existing_sequences
        );

        if existing_sequences > 0 {
            tracing::error!(
                "insert_genesis called on already-initialized database (sequences exist: {})",
                existing_sequences
            );
            return Err(DatabaseError::ProcessingError);
        }

        // === ALL LOGIC OUTSIDE EXECUTE FLAG ===
        // Genesis has no validation - it's the fiat root of trust

        // 1. Initialize sequences
        tracing::debug!("InsertGenesisHandler: Initializing sequences");
        initialize_sequences_tx(db_tx).map_err(|e| {
            tracing::error!(
                "InsertGenesisHandler: Failed to initialize sequences: {:?}",
                e
            );
            e
        })?;
        tracing::debug!("InsertGenesisHandler: Initialized sequences");

        // 2. Insert user (returns user_id=0)
        tracing::debug!("InsertGenesisHandler: Inserting user");
        let user_id = insert_user_tx(db_tx, genesis_data.user).map_err(|e| {
            tracing::error!("InsertGenesisHandler: Failed to insert user: {:?}", e);
            e
        })?;
        tracing::debug!("InsertGenesisHandler: Inserted user with id={}", user_id);

        // 3. Insert node (returns node_id=0)
        tracing::debug!("InsertGenesisHandler: Inserting node");
        let node_id = insert_node_tx(db_tx, genesis_data.node).map_err(|e| {
            tracing::error!("InsertGenesisHandler: Failed to insert node: {:?}", e);
            e
        })?;
        tracing::debug!("InsertGenesisHandler: Inserted node with id={}", node_id);

        // 4. Activate genesis validator (special case - only for genesis)
        // Uses same activate_validator function as normal activation for consistency
        tracing::debug!("InsertGenesisHandler: Activating validator");
        activate_validator(db_tx, node_id, 0).map_err(|e| {
            tracing::error!(
                "InsertGenesisHandler: Failed to activate validator: {:?}",
                e
            );
            e
        })?;
        tracing::debug!("InsertGenesisHandler: Activated validator");

        // === EXECUTION PHASE ===
        if execute {
            tracing::info!(
                "Genesis initialized: user_id={}, node_id={} (validator active at height 0)",
                user_id,
                node_id
            );
        } else {
            // Validation phase - genesis always valid
            tracing::debug!("Genesis handler validated successfully");
        }

        tracing::debug!("InsertGenesisHandler: Completed successfully");
        Ok(())
    }
}

inventory::submit! {
    &InsertGenesisHandler as &dyn TransactionHandler
}

// ============================================================================
// Nonce Cleanup Handler - Consensus-tracked cleanup of committed_tx_nonces
// ============================================================================

pub struct CleanupNoncesHandler;

impl TransactionHandler for CleanupNoncesHandler {
    fn name(&self) -> &'static str {
        "system.cleanup_nonces"
    }

    fn process(
        &self,
        _state: &AppState,
        tx: &Transaction,
        execute: bool,
        db_tx: &rusqlite::Transaction,
    ) -> HandlerResult {
        let (cutoff, _) = bincode::serde::decode_from_slice::<hopnet_common::CustomUUID, _>(
            &tx.rpc.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        if !execute {
            return Ok(());
        }

        let deleted = crate::db::consensus::cleanup_old_nonces(db_tx, &cutoff)?;
        tracing::debug!(
            "Cleaned up {} old transaction nonces (cutoff: {})",
            deleted,
            cutoff
        );
        Ok(())
    }
}

inventory::submit! {
    &CleanupNoncesHandler as &dyn TransactionHandler
}
