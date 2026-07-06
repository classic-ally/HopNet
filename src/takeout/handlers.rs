use crate::{
    AppState,
    consensus::types::Transaction,
    db::{
        DatabaseError,
        imports::{self, ImportPayload, ImportStatusPayload},
        takeout::{self, TakeoutPayload, TakeoutStatusPayload},
    },
    handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta},
};

/// Handler for create_takeout consensus transactions
pub struct CreateTakeoutHandler;

impl TransactionHandler for CreateTakeoutHandler {
    fn name(&self) -> &'static str {
        "create_takeout"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        // TEMPORARY (RFC-015, dies at Stage D5): takeout's creation flow
        // still spawns host-side background work from apply and needs the
        // full AppState via the ctx.host escape hatch.
        let Some(state) = ctx.host.and_then(|h| h.downcast_ref::<AppState>()) else {
            tracing::error!("create_takeout: host state unavailable");
            return Err(DatabaseError::ProcessingError);
        };
        match bincode::serde::decode_from_slice::<TakeoutPayload, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((takeout_payload, _)) => {
                // Authorization: verify user and node match authenticated identities
                if let Some(user_id) = tx.user_id {
                    if takeout_payload.user_id != user_id {
                        tracing::warn!(
                            "Authorization failed: user {} attempted to create takeout for user {}",
                            user_id,
                            takeout_payload.user_id
                        );
                        return Err(DatabaseError::AuthorizationError);
                    }
                } else {
                    tracing::warn!(
                        "Authorization failed: create_takeout requires user authentication"
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                if takeout_payload.owner_node_id != tx.submitter_node {
                    tracing::warn!(
                        "Authorization failed: node {} attempted to create takeout owned by node {}",
                        tx.submitter_node,
                        takeout_payload.owner_node_id
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                // Get current node ID
                let current_node_id = state
                    .get_node_id()
                    .map_err(|_| DatabaseError::ProcessingError)?;

                // Process the takeout creation using shared transaction
                takeout::process_takeout_creation(
                    state,
                    &takeout_payload,
                    current_node_id,
                    execute,
                    db_tx,
                )?;

                Ok(())
            }
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

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        // TEMPORARY (RFC-015, dies at Stage D5): status updates trigger
        // host-side materialization work and need AppState via ctx.host.
        let Some(state) = ctx.host.and_then(|h| h.downcast_ref::<AppState>()) else {
            tracing::error!("update_takeout_status: host state unavailable");
            return Err(DatabaseError::ProcessingError);
        };
        match bincode::serde::decode_from_slice::<TakeoutStatusPayload, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((status_payload, _)) => {
                // Process the takeout status update using shared transaction
                takeout::process_takeout_status_update(state, &status_payload, execute, db_tx)?;

                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &UpdateTakeoutStatusHandler as &dyn TransactionHandler
}

/// Handler for `create_import` consensus transactions.
///
/// Performs dual authorization (the user signing the transaction matches
/// `payload.user_id` and the submitting node matches `payload.owner_node_id`)
/// and delegates to `process_import_creation` for eligibility checks + insertion.
pub struct CreateImportHandler;

impl TransactionHandler for CreateImportHandler {
    fn name(&self) -> &'static str {
        "create_import"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<ImportPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        match tx.user_id {
            Some(user_id) if user_id == payload.user_id => {}
            Some(user_id) => {
                tracing::warn!(
                    "Authorization failed: user {} attempted to create import for user {}",
                    user_id,
                    payload.user_id
                );
                return Err(DatabaseError::AuthorizationError);
            }
            None => {
                tracing::warn!("Authorization failed: create_import requires user authentication");
                return Err(DatabaseError::AuthorizationError);
            }
        }

        if payload.owner_node_id != tx.submitter_node {
            tracing::warn!(
                "Authorization failed: node {} attempted to create import owned by node {}",
                tx.submitter_node,
                payload.owner_node_id
            );
            return Err(DatabaseError::AuthorizationError);
        }

        imports::process_import_creation(&payload, execute, db_tx)
    }
}

inventory::submit! {
    &CreateImportHandler as &dyn TransactionHandler
}

/// Handler for `update_import_status` consensus transactions.
/// Single handler covers all status transitions (Pending → Importing → Completed/Failed).
pub struct UpdateImportStatusHandler;

impl TransactionHandler for UpdateImportStatusHandler {
    fn name(&self) -> &'static str {
        "update_import_status"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<ImportStatusPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        imports::process_import_status_update(&payload, execute, db_tx)
    }
}

inventory::submit! {
    &UpdateImportStatusHandler as &dyn TransactionHandler
}
