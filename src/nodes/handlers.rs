use crate::{
    db::{DatabaseError, nodes::insert_node_tx},
    handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta},
    types::Node,
};

pub struct InsertNodeHandler;

impl TransactionHandler for InsertNodeHandler {
    fn name(&self) -> &'static str {
        "insert_node"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Node, _>(tx.payload, bincode::config::standard())
        {
            Ok((node_data, _)) => {
                // Authorization: verify user owns the node being inserted
                if let Some(user_id) = tx.user_id {
                    if node_data.owner != user_id {
                        tracing::warn!(
                            "Authorization failed: user {} attempted to insert node owned by user {}",
                            user_id,
                            node_data.owner
                        );
                        return Err(DatabaseError::AuthorizationError);
                    }
                } else {
                    tracing::warn!(
                        "Authorization failed: insert_node requires user authentication"
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                // Insert the node using shared transaction
                insert_node_tx(db_tx, node_data)?;
                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertNodeHandler as &dyn TransactionHandler
}
