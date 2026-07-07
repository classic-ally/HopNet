//! Takeout/import consensus transaction handlers. Moved from the host at
//! RFC-015 Stage D5b — they register cross-crate via `inventory::submit!`
//! (typetag pattern); the host's boot tripwire asserts `TX_FUNCTIONS` all
//! made it into the dispatch table (linker-drop guard).

use hopnet_projection::{
    DatabaseError, HandlerCtx, HandlerResult, TransactionHandler, TxMeta,
};

use crate::db::{
    imports::{self, ImportPayload, ImportStatusPayload},
    takeout::{self, TakeoutPayload, TakeoutStatusPayload},
};

/// Function names this crate registers handlers for — the host's boot
/// tripwire asserts each is present in the dispatch table.
pub const TX_FUNCTIONS: &[&str] = &[
    "create_takeout",
    "update_takeout_status",
    "create_import",
    "update_import_status",
];

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

                // This node's consensus id must be initialized to apply.
                if ctx.node_id.is_none() {
                    tracing::error!("create_takeout: node id not initialized");
                    return Err(DatabaseError::ProcessingError);
                }

                // Pure-DB apply half (validation + takeouts row) on the shared
                // transaction. Enumeration happens in the scheduled task now
                // (RFC-015 D5 decision 3) — no in-apply inode snapshot.
                takeout::apply_takeout_creation(db_tx, &takeout_payload, ctx.node_id, execute)?;

                // Owner node schedules materialization as named background
                // work — the host routes it post-apply (RFC-015 Stage D5a).
                if execute && ctx.node_id == Some(takeout_payload.owner_node_id) {
                    tracing::info!(
                        "Owner node will trigger materialization for takeout {} after transaction commit",
                        takeout_payload.takeout_id
                    );
                    ctx.work.schedule(
                        "takeout.materialize",
                        takeout_payload.takeout_id.to_string(),
                    );
                }

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
        match bincode::serde::decode_from_slice::<TakeoutStatusPayload, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((status_payload, _)) => {
                // Pure-DB apply half; returns the owner when an EXECUTED
                // update reached a terminal state (Expired/Cancelled).
                let cleanup_owner =
                    takeout::apply_takeout_status_update(db_tx, &status_payload, execute)?;

                // Owner node schedules local cleanup as named background
                // work — the host routes it post-apply (RFC-015 Stage D5a).
                if let Some(owner_node_id) = cleanup_owner {
                    match ctx.node_id {
                        Some(node_id) if node_id == owner_node_id => {
                            tracing::info!(
                                "Owner node will trigger cleanup for takeout {} (status: {:?})",
                                status_payload.takeout_id,
                                status_payload.new_status
                            );
                            ctx.work.schedule(
                                "takeout.cleanup",
                                status_payload.takeout_id.to_string(),
                            );
                        }
                        Some(_) => {
                            tracing::debug!(
                                "Non-owner node ignoring cleanup for takeout {} owned by node {}",
                                status_payload.takeout_id,
                                owner_node_id
                            );
                        }
                        None => {
                            tracing::warn!("Node ID not initialized, skipping cleanup trigger");
                        }
                    }
                }

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
