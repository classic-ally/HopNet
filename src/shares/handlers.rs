use crate::handlers::{TransactionHandler, HandlerResult};
use crate::AppState;
use crate::consensus::types::Transaction;
use crate::db::DatabaseError;
use duckdb::params;

use super::types::{ShareFilePayload, AcceptSharePayload, DeclineSharePayload, UnsharePayload};

// --- ShareFileHandler ---

pub struct ShareFileHandler;

impl TransactionHandler for ShareFileHandler {
    fn name(&self) -> &'static str { "share_file" }

    fn process(&self, _state: &AppState, tx: &Transaction, _execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<ShareFilePayload, _>(
            &tx.rpc.payload, bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: sender must be the authenticated user
        let user = tx.user.as_ref().ok_or(DatabaseError::AuthorizationError)?;
        if user.id != payload.sender_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation: sender != recipient
        if payload.sender_id == payload.recipient_id {
            return Err(DatabaseError::InvalidPayload);
        }

        // Validation: data_block exists
        db_tx.query_row(
            "SELECT 1 FROM data_blocks WHERE id = ?",
            params![payload.data_block_id],
            |_| Ok(())
        ).map_err(|_| DatabaseError::NotFound)?;

        // Validation: recipient exists
        db_tx.query_row(
            "SELECT 1 FROM users WHERE user_id = ?",
            [payload.recipient_id],
            |_| Ok(())
        ).map_err(|_| DatabaseError::NotFound)?;

        // Validation: no duplicate share
        if crate::db::shares::share_exists_for_recipient(db_tx, &payload.data_block_id, payload.recipient_id)? {
            return Err(DatabaseError::ConflictError);
        }

        // Execute: insert incoming_share
        crate::db::shares::insert_incoming_share(
            db_tx,
            payload.id,
            payload.data_block_id,
            payload.sender_id,
            payload.recipient_id,
            &payload.file_access,
            &payload.display_ephemeral_pubkey,
            &payload.encrypted_display_name,
        )?;

        Ok(())
    }
}

inventory::submit! { &ShareFileHandler as &dyn TransactionHandler }

// --- AcceptShareHandler ---

pub struct AcceptShareHandler;

impl TransactionHandler for AcceptShareHandler {
    fn name(&self) -> &'static str { "accept_share" }

    fn process(&self, state: &AppState, tx: &Transaction, execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<AcceptSharePayload, _>(
            &tx.rpc.payload, bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: recipient must be the authenticated user
        let user = tx.user.as_ref().ok_or(DatabaseError::AuthorizationError)?;
        if user.id != payload.recipient_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation: incoming_share exists and belongs to this recipient
        let incoming_share = crate::db::shares::get_incoming_share_by_id(db_tx, &payload.incoming_share_id)?
            .ok_or(DatabaseError::NotFound)?;

        if incoming_share.recipient_id != payload.recipient_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation: path not already taken
        let path_exists: bool = db_tx.query_row(
            "SELECT COUNT(*) > 0 FROM inodes WHERE path = ? AND owner_id = ?",
            params![payload.encrypted_path, payload.recipient_id],
            |row| row.get(0)
        ).map_err(|_| DatabaseError::RecallError)?;

        if path_exists {
            return Err(DatabaseError::ConflictError);
        }

        // 1. Deserialize FileAccess from blob and insert into file_access table
        let (file_access_entry, _) = bincode::serde::decode_from_slice::<crate::db::types::FileAccess, _>(
            &incoming_share.file_access, bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        db_tx.execute(
            "INSERT INTO file_access (data_block_id, user_id, ephemeral_pubkey, encrypted_file_key) VALUES (?, ?, ?, ?)",
            params![file_access_entry.data_block_id, file_access_entry.user_id, file_access_entry.ephemeral_pubkey, file_access_entry.encrypted_file_key]
        ).map_err(|e| {
            tracing::error!("Failed to insert file_access for share accept: {:?}", e);
            DatabaseError::InsertError
        })?;

        // 2. Create parent directories for the encrypted path
        let missing_parents = crate::db::files::find_missing_parents(db_tx, &[payload.encrypted_path.clone()])?;
        if !missing_parents.is_empty() {
            crate::db::files::insert_parent_directories(db_tx, &missing_parents, payload.recipient_id)?;
        }

        // 3. Insert inode in recipient's namespace
        db_tx.execute(
            "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, 'file', ?)",
            params![payload.inode_id, payload.recipient_id, payload.encrypted_path, incoming_share.data_block_id]
        ).map_err(|e| {
            tracing::error!("Failed to insert inode for share accept: {:?}", e);
            DatabaseError::InsertError
        })?;

        // 4. Insert share members for both sender and recipient
        crate::db::shares::insert_share_members(
            db_tx,
            incoming_share.data_block_id,
            &[incoming_share.sender_id, payload.recipient_id],
        )?;

        // 5. Delete the incoming_share record
        crate::db::shares::delete_incoming_share(db_tx, &payload.incoming_share_id)?;

        // 6. Log modification for FileProvider
        let current_height = crate::db::consensus::get_current_consensus_height(db_tx)?;
        crate::db::files::log_modification(
            db_tx, payload.inode_id, payload.recipient_id,
            None, None, Some(&payload.encrypted_path), current_height,
        )?;

        // 7. Signal FileProvider refresh
        if execute {
            #[cfg(target_os = "macos")]
            {
                let test_mode = state.test_mode;
                tokio::spawn(async move {
                    if let Err(e) = crate::fileprovider::domain::signal_fileprovider_refresh(test_mode).await {
                        tracing::warn!("Failed to signal FileProvider refresh after share accept: {}", e);
                    }
                });
            }
        }

        Ok(())
    }
}

inventory::submit! { &AcceptShareHandler as &dyn TransactionHandler }

// --- DeclineShareHandler ---

pub struct DeclineShareHandler;

impl TransactionHandler for DeclineShareHandler {
    fn name(&self) -> &'static str { "decline_share" }

    fn process(&self, _state: &AppState, tx: &Transaction, _execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<DeclineSharePayload, _>(
            &tx.rpc.payload, bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: must be the authenticated user
        let user = tx.user.as_ref().ok_or(DatabaseError::AuthorizationError)?;
        if user.id != payload.user_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation: incoming_share exists
        let share = crate::db::shares::get_incoming_share_by_id(db_tx, &payload.incoming_share_id)?
            .ok_or(DatabaseError::NotFound)?;

        // Authorization: must be sender or recipient
        if share.sender_id != payload.user_id && share.recipient_id != payload.user_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Execute: delete
        crate::db::shares::delete_incoming_share(db_tx, &payload.incoming_share_id)?;

        Ok(())
    }
}

inventory::submit! { &DeclineShareHandler as &dyn TransactionHandler }

// --- UnshareHandler ---

pub struct UnshareHandler;

impl TransactionHandler for UnshareHandler {
    fn name(&self) -> &'static str { "unshare" }

    fn process(&self, _state: &AppState, tx: &Transaction, _execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<UnsharePayload, _>(
            &tx.rpc.payload, bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: must be the authenticated user
        let user = tx.user.as_ref().ok_or(DatabaseError::AuthorizationError)?;
        if user.id != payload.user_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Look up inode → get data_block_id
        let data_block_id = crate::db::shares::get_data_block_for_inode(db_tx, &payload.inode_id, payload.user_id)?
            .ok_or(DatabaseError::NotFound)?;

        // Verify user is in shares table for this data_block
        let sharers = crate::db::shares::get_sharers_for_data_block(db_tx, &data_block_id)?;
        if !sharers.contains(&payload.user_id) {
            return Err(DatabaseError::NotFound);
        }

        // Remove user from shares — they keep their inode and file_access (copy-on-write)
        crate::db::shares::remove_user_from_shares(db_tx, &data_block_id, payload.user_id)?;

        // Also clean up any pending outgoing shares from this user for this data_block
        crate::db::shares::remove_sender_incoming_shares(db_tx, &data_block_id, payload.user_id)?;

        Ok(())
    }
}

inventory::submit! { &UnshareHandler as &dyn TransactionHandler }
