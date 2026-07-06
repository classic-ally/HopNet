use super::types::{RegisterDevicePayload, RevokeDevicePayload};
use crate::{
    db::{
        DatabaseError,
        devices::{delete_device_token_tx, insert_device_token_tx},
    },
    handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta},
};

/// Handler for register_device consensus transactions
pub struct RegisterDeviceHandler;

impl TransactionHandler for RegisterDeviceHandler {
    fn name(&self) -> &'static str {
        "register_device"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<RegisterDevicePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: user in transaction must match payload user_id
        if let Some(user_id) = tx.user_id {
            if payload.user_id != user_id {
                tracing::warn!(
                    "Authorization failed: user {} attempted to register device for user {}",
                    user_id,
                    payload.user_id
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
            &payload.wrapped_user_key,
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

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<RevokeDevicePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: user in transaction must match payload user_id
        if let Some(user_id) = tx.user_id {
            if payload.user_id != user_id {
                tracing::warn!(
                    "Authorization failed: user {} attempted to revoke device for user {}",
                    user_id,
                    payload.user_id
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
