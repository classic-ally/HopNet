//! Drive consensus transaction handlers (RFC-015, Stage D3).
//!
//! Moved verbatim from the host's `files::handlers` / `shares::handlers` —
//! bodies are byte-identical; only import paths were re-pointed to
//! drive-internal modules and the projection seam. Registration crosses the
//! crate boundary via `inventory::submit!` (the registries live in
//! hopnet-projection); the host's boot tripwire asserts nothing was dropped
//! at link time.

use crate::db::files::insert_files;
use crate::envelopes::{
    AcceptSharePayload, DeclineSharePayload, DeleteFilesPayload, DriveInsertPayload,
    ModifyItemPayload, ShareFilePayload, UnsharePayload,
};
use hopnet_projection::{DatabaseError, HandlerCtx, HandlerResult, TransactionHandler, TxMeta};
use rusqlite::params;

/// Every consensus function this projection registers — the host boot
/// tripwire asserts these are present in its dispatch table (guards
/// against a linker dropping cross-crate inventory registrations).
pub const TX_FUNCTIONS: &[&str] = &[
    "insert_files",
    "modify_item",
    "delete_files",
    "share_file",
    "accept_share",
    "decline_share",
    "unshare",
];

pub struct InsertFilesHandler;

impl TransactionHandler for InsertFilesHandler {
    fn name(&self) -> &'static str {
        "insert_files"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<DriveInsertPayload, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((payload, _)) => {
                let DriveInsertPayload { blob_ops, inodes } = payload;
                // Authorization: verify user owns the files being inserted
                if let Some(user_id) = tx.user_id {
                    for inode in &inodes {
                        // (Owner narrowing, RFC-015: a legacy Either::Right
                        // payload now fails DECODE above — same rejection,
                        // one step earlier.)
                        let owner_id = inode.owner.id();
                        if owner_id != user_id {
                            tracing::warn!(
                                "Authorization failed: user {} attempted to insert file for user {}",
                                user_id,
                                owner_id
                            );
                            return Err(DatabaseError::AuthorizationError);
                        }
                    }
                } else {
                    tracing::warn!(
                        "Authorization failed: insert_files requires user authentication"
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                // Substrate half: register blobs (stored_locally probed
                // against this node's disk in-crate), then the projection
                // half: inodes referencing them by id.
                insert_files(db_tx, &blob_ops, inodes, ctx.fragments_dir)?;

                // Signal FileProvider to refresh when files are actually inserted (execute=true)
                if execute {
                    ctx.notifier.files_changed();
                }

                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertFilesHandler as &dyn TransactionHandler
}

pub struct ModifyItemHandler;

impl TransactionHandler for ModifyItemHandler {
    fn name(&self) -> &'static str {
        "modify_item"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<ModifyItemPayload, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((payload_data, _)) => {
                // Authorization: verify user matches authenticated user
                if let Some(user_id) = tx.user_id {
                    if payload_data.user_id != user_id {
                        tracing::warn!(
                            "Authorization failed: user {} attempted to modify item for user {}",
                            user_id,
                            payload_data.user_id
                        );
                        return Err(DatabaseError::AuthorizationError);
                    }
                } else {
                    tracing::warn!(
                        "Authorization failed: modify_item requires user authentication"
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                tracing::debug!(
                    "ModifyItemHandler processing: inode_id={} user_id={} new_encrypted_path={:?}",
                    payload_data.inode_id,
                    payload_data.user_id,
                    payload_data.new_encrypted_path
                );

                // stored_locally is probed inside the substrate apply
                // (apply_blob_insert) — no preprocessing pass needed.
                crate::db::files::modify_item(
                    db_tx,
                    payload_data.user_id,
                    payload_data.inode_id.clone(),
                    payload_data.new_encrypted_path.clone(),
                    payload_data.content_update.clone().map(|u| u.blob_op),
                    payload_data.incoming_share_updates.clone(),
                    ctx.fragments_dir,
                )?;

                if execute {
                    tracing::info!("Modified item at path for user {}", payload_data.user_id);

                    // Signal FileProvider to refresh when item is actually modified
                    ctx.notifier.files_changed();
                } else {
                    tracing::debug!(
                        "Validation passed: item exists for modification for user {}",
                        payload_data.user_id
                    );
                }

                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &ModifyItemHandler as &dyn TransactionHandler
}

pub struct DeleteFilesHandler;

impl TransactionHandler for DeleteFilesHandler {
    fn name(&self) -> &'static str {
        "delete_files"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<DeleteFilesPayload, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((payload_data, _)) => {
                // Authorization: verify user matches authenticated user
                if let Some(user_id) = tx.user_id {
                    if payload_data.user_id != user_id {
                        tracing::warn!(
                            "Authorization failed: user {} attempted to delete files for user {}",
                            user_id,
                            payload_data.user_id
                        );
                        return Err(DatabaseError::AuthorizationError);
                    }
                } else {
                    tracing::warn!(
                        "Authorization failed: delete_files requires user authentication"
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                // Idempotent delete: NotFound is not an error (file may already be deleted,
                // or validation snapshot may not see recent moves/renames)
                match crate::db::files::delete_files(
                    db_tx,
                    payload_data.encrypted_path,
                    payload_data.user_id,
                ) {
                    Ok(()) => {}
                    Err(DatabaseError::NotFound) => {} // Idempotent: already deleted or not found
                    Err(e) => return Err(e),
                }

                if execute {
                    tracing::info!("Deleted files at path for user {}", payload_data.user_id);

                    // Signal FileProvider to refresh when files are actually deleted
                    ctx.notifier.files_changed();
                }
                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &DeleteFilesHandler as &dyn TransactionHandler
}

// --- ShareFileHandler ---

pub struct ShareFileHandler;

impl TransactionHandler for ShareFileHandler {
    fn name(&self) -> &'static str {
        "share_file"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<ShareFilePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: sender must be the authenticated user
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;
        if user_id != payload.sender_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation: sender != recipient
        if payload.sender_id == payload.recipient_id {
            return Err(DatabaseError::InvalidPayload);
        }

        // Validation: data_block exists
        db_tx
            .query_row(
                "SELECT 1 FROM data_blocks WHERE id = ?",
                params![payload.data_block_id],
                |_| Ok(()),
            )
            .map_err(|_| DatabaseError::NotFound)?;

        // Validation: recipient exists
        db_tx
            .query_row(
                "SELECT 1 FROM users WHERE user_id = ?",
                [payload.recipient_id],
                |_| Ok(()),
            )
            .map_err(|_| DatabaseError::NotFound)?;

        // Validation: no duplicate share
        if crate::db::shares::share_exists_for_recipient(
            db_tx,
            &payload.data_block_id,
            payload.recipient_id,
        )? {
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
    fn name(&self) -> &'static str {
        "accept_share"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<AcceptSharePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: recipient must be the authenticated user
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;
        if user_id != payload.recipient_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation: incoming_share exists and belongs to this recipient
        let incoming_share =
            crate::db::shares::get_incoming_share_by_id(db_tx, &payload.incoming_share_id)?
                .ok_or(DatabaseError::NotFound)?;

        if incoming_share.recipient_id != payload.recipient_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation: path not already taken
        let path_exists: bool = db_tx
            .query_row(
                "SELECT COUNT(*) > 0 FROM inodes WHERE path = ? AND owner_id = ?",
                params![payload.encrypted_path, payload.recipient_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::RecallError)?;

        if path_exists {
            return Err(DatabaseError::ConflictError);
        }

        // 1. Deserialize BlobAccess from blob and insert into blob_access table
        let (file_access_entry, _) =
            bincode::serde::decode_from_slice::<hopnet_storage::BlobAccess, _>(
                &incoming_share.file_access,
                bincode::config::standard(),
            )
            .map_err(|_| DatabaseError::InvalidPayload)?;

        db_tx.execute(
            "INSERT OR REPLACE INTO blob_access (blob_id, recipient_pubkey, ephemeral_pubkey, wrapped_key) VALUES (?, ?, ?, ?)",
            params![file_access_entry.blob_id, file_access_entry.recipient_pubkey.to_vec(), file_access_entry.ephemeral_pubkey.to_vec(), file_access_entry.wrapped_key]
        ).map_err(|e| {
            tracing::error!("Failed to insert blob_access for share accept: {:?}", e);
            DatabaseError::InsertError
        })?;

        // 2. Create parent directories from pre-generated folder inodes
        for (folder_id, folder_path) in &payload.parent_folder_inodes {
            db_tx.execute(
                "INSERT OR IGNORE INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, 1, NULL)",
                rusqlite::params![folder_id, payload.recipient_id, folder_path],
            ).map_err(|_| DatabaseError::InsertError)?;
        }

        // 3. Insert inode in recipient's namespace
        db_tx
            .execute(
                "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, 0, ?)",
                params![
                    payload.inode_id,
                    payload.recipient_id,
                    payload.encrypted_path,
                    incoming_share.data_block_id
                ],
            )
            .map_err(|e| {
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
        let current_height = crate::db::current_height(db_tx)?;
        crate::db::files::log_modification(
            db_tx,
            payload.inode_id,
            payload.recipient_id,
            None,
            None,
            Some(&payload.encrypted_path),
            current_height,
        )?;

        // 7. Signal FileProvider refresh
        if execute {
            ctx.notifier.files_changed();
        }

        Ok(())
    }
}

inventory::submit! { &AcceptShareHandler as &dyn TransactionHandler }

// --- DeclineShareHandler ---

pub struct DeclineShareHandler;

impl TransactionHandler for DeclineShareHandler {
    fn name(&self) -> &'static str {
        "decline_share"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<DeclineSharePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: must be the authenticated user
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;
        if user_id != payload.user_id {
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
    fn name(&self) -> &'static str {
        "unshare"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<UnsharePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: must be the authenticated user
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;
        if user_id != payload.user_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Look up inode → get data_block_id
        let data_block_id =
            crate::db::shares::get_data_block_for_inode(db_tx, &payload.inode_id, payload.user_id)?
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
