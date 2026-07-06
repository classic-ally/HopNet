use crate::{
    db::Inode,
    db::{
        CustomUUID, DatabaseError, files::insert_files,
        fragments::delete_orphaned_data_blocks_consensus,
    },
    handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta},
};
use either::Either;
use serde::{Deserialize, Serialize};

/// The DRIVE projection's insert envelope: substrate blob registrations
/// (crate sub-payloads) alongside the inodes that reference them by id.
/// Both halves apply in ONE handler transaction — blob + first reference
/// are atomic, so mark-and-sweep never observes a zero-ref blob.
/// (Envelope ownership: this type belongs to the drive projection and
/// extracts with it to hopnet-drive.)
#[derive(Serialize, Deserialize, Debug)]
pub struct DriveInsertPayload {
    pub blob_ops: Vec<hopnet_storage::store::BlobInsertOp>,
    pub inodes: Vec<Inode>,
}

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
                        match &inode.owner {
                            Either::Left(owner_id) => {
                                if *owner_id != user_id {
                                    tracing::warn!(
                                        "Authorization failed: user {} attempted to insert file for user {}",
                                        user_id,
                                        owner_id
                                    );
                                    return Err(DatabaseError::AuthorizationError);
                                }
                            }
                            Either::Right(_) => {
                                tracing::error!(
                                    "Authorization failed: unexpected User object in owner field"
                                );
                                return Err(DatabaseError::AuthorizationError);
                            }
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

pub struct UpdatePlacementHeightsHandler;

impl TransactionHandler for UpdatePlacementHeightsHandler {
    fn name(&self) -> &'static str {
        "update_placement_heights"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        // Storage-owned tx (RFC-014): payload type and apply both live in
        // the substrate crate; this shim only decodes and delegates.
        match bincode::serde::decode_from_slice::<Vec<hopnet_storage::PlacementUpdate>, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((updates, _)) => {
                let crate_updates: Vec<(hopnet_storage::BlobId, i32)> = updates
                    .into_iter()
                    .map(|u| (u.blob_id, u.placement_height))
                    .collect();
                hopnet_storage::store::apply_placement_commit(db_tx, &crate_updates).map_err(
                    |e| {
                        tracing::error!("apply_placement_commit failed: {e}");
                        DatabaseError::ProcessingError
                    },
                )?;
                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &UpdatePlacementHeightsHandler as &dyn TransactionHandler
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteOrphanedDataBlocksPayload {
    pub data_block_ids: Vec<CustomUUID>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteFilesPayload {
    pub encrypted_path: String,
    pub user_id: i32,
}

pub struct DeleteOrphanedDataBlocksHandler;

impl TransactionHandler for DeleteOrphanedDataBlocksHandler {
    fn name(&self) -> &'static str {
        "delete_orphaned_data_blocks"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<DeleteOrphanedDataBlocksPayload, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((payload_data, _)) => {
                let deleted_fragment_hashes =
                    delete_orphaned_data_blocks_consensus(db_tx, payload_data.data_block_ids)?;

                // If executing, opportunistically delete local fragment files
                if execute && !deleted_fragment_hashes.is_empty() {
                    tracing::info!(
                        "Opportunistically cleaning up {} local fragment files",
                        deleted_fragment_hashes.len()
                    );

                    let mut successfully_deleted = 0;
                    for fragment_hash in &deleted_fragment_hashes {
                        match crate::files::functions::delete_fragment(
                            ctx.fragments_dir,
                            fragment_hash,
                        ) {
                            Ok(()) => {
                                successfully_deleted += 1;
                                tracing::debug!(
                                    "Deleted local fragment file: {}",
                                    fragment_hash.to_hex()
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to delete local fragment file {}: {:?}",
                                    fragment_hash.to_hex(),
                                    e
                                );
                                // Continue with other deletions - this fragment will be caught by filesystem cleanup job
                            }
                        }
                    }

                    tracing::info!(
                        "Successfully deleted {}/{} local fragment files",
                        successfully_deleted,
                        deleted_fragment_hashes.len()
                    );
                }

                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &DeleteOrphanedDataBlocksHandler as &dyn TransactionHandler
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ModifyItemPayload {
    pub user_id: i32,
    pub inode_id: crate::db::CustomUUID, // Stable inode identifier
    pub new_encrypted_path: Option<String>, // New path if renaming/moving
    /// Content change, when present. `blob_op: None` means the content is
    /// now EMPTY — the inode's data_id becomes NULL (no blob exists for
    /// empty content; RFC-014).
    pub content_update: Option<DriveContentUpdate>,
    // Phase 2b: Share propagation — pre-computed updates for pending incoming_shares
    pub incoming_share_updates: Option<Vec<crate::shares::types::IncomingShareUpdate>>,
}

/// Drive-scoped content-update sub-payload (extracts with the projection).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DriveContentUpdate {
    pub blob_op: Option<hopnet_storage::store::BlobInsertOp>,
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
                    payload_data
                        .content_update
                        .clone()
                        .map(|u| u.blob_op),
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

pub struct SelfCheckFragmentsHandler;

impl TransactionHandler for SelfCheckFragmentsHandler {
    fn name(&self) -> &'static str {
        "self_check_fragments"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<crate::files::types::SelfCheckFragments, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((report, _)) => {
                // Authorization: verify node can only submit attestations for itself
                if report.node_id != tx.submitter_node {
                    tracing::warn!(
                        "Authorization failed: node {} attempted to submit self-attestation for node {}",
                        tx.submitter_node,
                        report.node_id
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                // Apply the self-check updates using the inventory module
                crate::db::inventory::apply_self_check_updates(db_tx, &report)?;

                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &SelfCheckFragmentsHandler as &dyn TransactionHandler
}
