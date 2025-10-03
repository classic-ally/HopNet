use crate::{
    db::{DatabaseError, consensus::{activate_validator, is_node_active, get_current_consensus_height}},
    handlers::{HandlerResult, TransactionHandler},
    consensus::types::Transaction,
    AppState,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActivationRequest {
    pub node_id: i32,
    pub current_height: i32,              // Proof node is caught up
    pub requested_effective_height: i32,  // Deterministic activation height
}

pub struct ValidatorActivationHandler;

impl TransactionHandler for ValidatorActivationHandler {
    fn name(&self) -> &'static str { "validator_activation" }

    fn process(&self, state: &AppState, tx: &Transaction, execute: bool) -> HandlerResult {
        // Decode activation request payload
        let (activation_req, _) = bincode::serde::decode_from_slice::<ActivationRequest, _>(
            &tx.rpc.payload,
            bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        // Get database connection and transaction immediately for validation queries
        let mut conn = state.db_pool.get().map_err(|_| DatabaseError::LockError)?;
        let tx_db = conn.transaction().map_err(|_| DatabaseError::LockError)?;

        // === ALL VALIDATION OUTSIDE EXECUTE FLAG ===
        // This determines whether we vote YES or NO in the propose phase

        // Authorization: Only the node itself can request its own activation
        if activation_req.node_id != tx.submitter.id {
            tracing::warn!(
                "Authorization failed: node {} attempted to activate node {}",
                tx.submitter.id,
                activation_req.node_id
            );
            return Err(DatabaseError::AuthorizationError);
        }

        // Get current consensus height for validation checks
        let consensus_height = get_current_consensus_height(&tx_db)?;

        // Synchronization check: Node must be caught up (within 2 views tolerance)
        if activation_req.current_height < consensus_height - 2 {
            tracing::warn!(
                "Node {} activation failed: not caught up (reported height {}, consensus height {})",
                activation_req.node_id,
                activation_req.current_height,
                consensus_height
            );
            return Err(DatabaseError::ProcessingError);
        }

        // Activation window check: Must be in reasonable future (3-10 views ahead)
        let min_activation = consensus_height + 3;  // Observation period for validator-elect
        let max_activation = consensus_height + 10; // Reasonable upper bound

        if activation_req.requested_effective_height < min_activation {
            tracing::warn!(
                "Node {} activation failed: requested height {} too soon (minimum {})",
                activation_req.node_id,
                activation_req.requested_effective_height,
                min_activation
            );
            return Err(DatabaseError::ProcessingError);
        }

        if activation_req.requested_effective_height > max_activation {
            tracing::warn!(
                "Node {} activation failed: requested height {} too far in future (maximum {})",
                activation_req.node_id,
                activation_req.requested_effective_height,
                max_activation
            );
            return Err(DatabaseError::ProcessingError);
        }

        // Not-already-passed check: Requested height must be in the future
        if activation_req.requested_effective_height <= consensus_height {
            tracing::warn!(
                "Node {} activation failed: requested height {} already passed (current {})",
                activation_req.node_id,
                activation_req.requested_effective_height,
                consensus_height
            );
            return Err(DatabaseError::ProcessingError);
        }

        // Check if node is already active at requested height (idempotency check)
        // This is informational - we allow UPDATE of future activations for hot-swap
        if is_node_active(&tx_db, activation_req.node_id, activation_req.requested_effective_height)? {
            tracing::debug!(
                "Node {} already has activation at or before height {}",
                activation_req.node_id,
                activation_req.requested_effective_height
            );
        }

        // === VALIDATION COMPLETE - All checks passed, safe to vote YES ===

        // Only perform state changes if in execute mode (lock phase)
        if execute {
            tracing::info!(
                "Activating node {} at effective height {}",
                activation_req.node_id,
                activation_req.requested_effective_height
            );

            // Activate the validator (INSERT or UPDATE future activation)
            activate_validator(
                &tx_db,
                activation_req.node_id,
                activation_req.requested_effective_height
            )?;

            // Commit the database transaction
            tx_db.commit().map_err(|_| DatabaseError::InsertError)?;
        }

        Ok(())
    }
}

inventory::submit! {
    &ValidatorActivationHandler as &dyn TransactionHandler
}
