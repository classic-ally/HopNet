use crate::{db::{DatabaseError, nodes::insert_node_consensus}, handlers::{HandlerResult, TransactionHandler}, types::Node, consensus::types::Transaction};
use crate::AppState;

pub struct InsertNodeHandler;

impl TransactionHandler for InsertNodeHandler {
    fn name(&self) -> &'static str { "insert_node" }

    fn process(&self, state: &AppState, tx: &Transaction, execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Node, _>(&tx.rpc.payload, bincode::config::standard()) {
            Ok((node_data, _)) => {
                // Authorization: verify user owns the node being inserted
                if let Some(ref user) = tx.user {
                    if node_data.owner != user.id {
                        tracing::warn!("Authorization failed: user {} attempted to insert node owned by user {}", user.id, node_data.owner);
                        return Err(DatabaseError::AuthorizationError);
                    }
                } else {
                    tracing::warn!("Authorization failed: insert_node requires user authentication");
                    return Err(DatabaseError::AuthorizationError);
                }

                // Insert the node using consensus-safe version with execute flag
                insert_node_consensus(state.db_pool.get(), node_data, execute)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertNodeHandler as &dyn TransactionHandler
}