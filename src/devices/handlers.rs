use crate::{
    db::{DatabaseError, devices::{insert_device_token_tx, delete_device_token_tx}},
    handlers::{HandlerResult, TransactionHandler},
    consensus::types::Transaction,
    AppState,
};
use super::types::{RegisterDevicePayload, RevokeDevicePayload};

/// Handler for register_device consensus transactions
pub struct RegisterDeviceHandler;

impl TransactionHandler for RegisterDeviceHandler {
    fn name(&self) -> &'static str {
        "register_device"
    }

    fn process(&self, _state: &AppState, tx: &Transaction, _execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<RegisterDevicePayload, _>(
            &tx.rpc.payload,
            bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: user in transaction must match payload user_id
        if let Some(ref user) = tx.user {
            if payload.user_id != user.id {
                tracing::warn!(
                    "Authorization failed: user {} attempted to register device for user {}",
                    user.id, payload.user_id
                );
                return Err(DatabaseError::AuthorizationError);
            }
        } else {
            tracing::warn!("Authorization failed: register_device requires user authentication");
            return Err(DatabaseError::AuthorizationError);
        }

        insert_device_token_tx(
            db_tx,
            &payload.id,
            payload.user_id,
            &payload.api_key_hash,
            &payload.encrypted_device_name,
        )?;

        Ok(())
    }
}

inventory::submit! {
    &RegisterDeviceHandler as &dyn TransactionHandler
}

/// Handler for revoke_device consensus transactions
pub struct RevokeDeviceHandler;

impl TransactionHandler for RevokeDeviceHandler {
    fn name(&self) -> &'static str {
        "revoke_device"
    }

    fn process(&self, _state: &AppState, tx: &Transaction, _execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<RevokeDevicePayload, _>(
            &tx.rpc.payload,
            bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: user in transaction must match payload user_id
        if let Some(ref user) = tx.user {
            if payload.user_id != user.id {
                tracing::warn!(
                    "Authorization failed: user {} attempted to revoke device for user {}",
                    user.id, payload.user_id
                );
                return Err(DatabaseError::AuthorizationError);
            }
        } else {
            tracing::warn!("Authorization failed: revoke_device requires user authentication");
            return Err(DatabaseError::AuthorizationError);
        }

        // Idempotent delete - succeeds even if device doesn't exist
        delete_device_token_tx(db_tx, &payload.device_id, payload.user_id)?;

        Ok(())
    }
}

inventory::submit! {
    &RevokeDeviceHandler as &dyn TransactionHandler
}
