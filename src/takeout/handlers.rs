use crate::{
    db::{DatabaseError, takeout::{self, TakeoutPayload, TakeoutStatusPayload}},
    handlers::{HandlerResult, TransactionHandler},
    AppState,
};

/// Handler for create_takeout consensus transactions
pub struct CreateTakeoutHandler;

impl TransactionHandler for CreateTakeoutHandler {
    fn name(&self) -> &'static str { 
        "create_takeout" 
    }

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<TakeoutPayload, _>(payload, bincode::config::standard()) {
            Ok((takeout_payload, _)) => {
                // Get current node ID
                let current_node_id = state.get_node_id().map_err(|_| DatabaseError::ProcessingError)?;
                
                // Process the takeout creation (includes validation)
                takeout::process_takeout_creation(
                    state,
                    &takeout_payload,
                    current_node_id,
                    execute,
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

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<TakeoutStatusPayload, _>(payload, bincode::config::standard()) {
            Ok((status_payload, _)) => {
                // Process the takeout status update (includes validation)
                takeout::process_takeout_status_update(
                    state,
                    &status_payload,
                    execute,
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