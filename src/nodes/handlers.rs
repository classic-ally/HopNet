use crate::{db::{DatabaseError, nodes::insert_node_consensus}, handlers::{HandlerResult, TransactionHandler}, types::Node};
use crate::AppState;

pub struct InsertNodeHandler;

impl TransactionHandler for InsertNodeHandler {
    fn name(&self) -> &'static str { "insert_node" }

    fn handle(&self, state: &AppState, payload: &[u8]) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Node, _>(payload, bincode::config::standard()) {
            Ok((node_data, _)) => {
                // Insert the node using consensus-safe version (no DB dump/sync)
                insert_node_consensus(state.db_pool.get(), node_data)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertNodeHandler as &dyn TransactionHandler
}