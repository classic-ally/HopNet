use crate::{
    db::{DatabaseError, takeout::{self, TakeoutPayload, TakeoutStatusPayload}},
    handlers::{HandlerResult, TransactionHandler},
    AppState,
    consensus::types::Transaction,
};

/// Handler for create_takeout consensus transactions
pub struct CreateTakeoutHandler;

impl TransactionHandler for CreateTakeoutHandler {
    fn name(&self) -> &'static str {
        "create_takeout"
    }

    fn process(&self, state: &AppState, tx: &Transaction, execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        match bincode::serde::decode_from_slice::<TakeoutPayload, _>(&tx.rpc.payload, bincode::config::standard()) {
            Ok((takeout_payload, _)) => {
                // Authorization: verify user and node match authenticated identities
                if let Some(ref user) = tx.user {
                    if takeout_payload.user_id != user.id {
                        tracing::warn!("Authorization failed: user {} attempted to create takeout for user {}", user.id, takeout_payload.user_id);
                        return Err(DatabaseError::AuthorizationError);
                    }
                } else {
                    tracing::warn!("Authorization failed: create_takeout requires user authentication");
                    return Err(DatabaseError::AuthorizationError);
                }

                if takeout_payload.owner_node_id != tx.submitter.id {
                    tracing::warn!("Authorization failed: node {} attempted to create takeout owned by node {}", tx.submitter.id, takeout_payload.owner_node_id);
                    return Err(DatabaseError::AuthorizationError);
                }

                // Get current node ID
                let current_node_id = state.get_node_id().map_err(|_| DatabaseError::ProcessingError)?;

                // Process the takeout creation using shared transaction
                takeout::process_takeout_creation(
                    state,
                    &takeout_payload,
                    current_node_id,
                    execute,
                    db_tx,
                )?;

                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &CreateTakeoutHandler as &dyn TransactionHandler
}

/// Handler for update_takeout_status consensus transactions
pub struct UpdateTakeoutStatusHandler;

impl TransactionHandler for UpdateTakeoutStatusHandler {
    fn name(&self) -> &'static str {
        "update_takeout_status"
    }

    fn process(&self, state: &AppState, tx: &Transaction, execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        match bincode::serde::decode_from_slice::<TakeoutStatusPayload, _>(&tx.rpc.payload, bincode::config::standard()) {
            Ok((status_payload, _)) => {
                // Process the takeout status update using shared transaction
                takeout::process_takeout_status_update(
                    state,
                    &status_payload,
                    execute,
                    db_tx,
                )?;

                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &UpdateTakeoutStatusHandler as &dyn TransactionHandler
}