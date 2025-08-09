use crate::{
    db::{DatabaseError, CustomUUID, files::{insert_files, update_placement_heights_batch, PlacementHeightUpdate}, fragments::delete_orphaned_data_blocks_consensus}, 
    handlers::{HandlerResult, TransactionHandler}, 
    db::Inode
};
use crate::AppState;
use crate::files::functions::fragment_exists_and_valid;
use either::Either;
use serde::{Serialize, Deserialize};

pub struct InsertFilesHandler;

impl TransactionHandler for InsertFilesHandler {
    fn name(&self) -> &'static str { "insert_files" }

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Vec<Inode>, _>(payload, bincode::config::standard()) {
            Ok((mut inodes, _)) => {
                // Preprocess inodes to verify fragments exist locally and update stored_locally flags
                for inode in &mut inodes {
                    if let Some(Either::Right(data_record)) = &mut inode.data_id {
                        for fragment in &mut data_record.data.fragments {
                            // Check if fragment exists and is valid on this node
                            fragment.stored_locally = fragment_exists_and_valid(
                                &state.fragments_dir, 
                                &fragment.fragment_hash
                            );
                        }
                    }
                }
                
                // Insert the files into the database with execute flag
                insert_files(state.db_pool.get(), inodes, execute)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertFilesHandler as &dyn TransactionHandler
}

pub struct UpdatePlacementHeightsHandler;

impl TransactionHandler for UpdatePlacementHeightsHandler {
    fn name(&self) -> &'static str { "update_placement_heights" }

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Vec<PlacementHeightUpdate>, _>(payload, bincode::config::standard()) {
            Ok((updates_data, _)) => {
                // Update placement heights using the consensus-safe version with execute flag
                update_placement_heights_batch(state.db_pool.get(), updates_data, execute)?;
                Ok(())
            },
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
    fn name(&self) -> &'static str { "delete_orphaned_data_blocks" }

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<DeleteOrphanedDataBlocksPayload, _>(payload, bincode::config::standard()) {
            Ok((payload_data, _)) => {
                let deleted_fragment_hashes = delete_orphaned_data_blocks_consensus(
                    state.db_pool.get(), 
                    payload_data.data_block_ids, 
                    execute
                )?;
                
                // If executing, opportunistically delete local fragment files
                if execute && !deleted_fragment_hashes.is_empty() {
                    tracing::info!("Opportunistically cleaning up {} local fragment files", deleted_fragment_hashes.len());
                    
                    let mut successfully_deleted = 0;
                    for fragment_hash in &deleted_fragment_hashes {
                        match crate::files::functions::delete_fragment(&state.fragments_dir, fragment_hash) {
                            Ok(()) => {
                                successfully_deleted += 1;
                                tracing::debug!("Deleted local fragment file: {}", fragment_hash.to_hex());
                            }
                            Err(e) => {
                                tracing::warn!("Failed to delete local fragment file {}: {:?}", fragment_hash.to_hex(), e);
                                // Continue with other deletions - this fragment will be caught by filesystem cleanup job
                            }
                        }
                    }
                    
                    tracing::info!("Successfully deleted {}/{} local fragment files", successfully_deleted, deleted_fragment_hashes.len());
                }
                
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &DeleteOrphanedDataBlocksHandler as &dyn TransactionHandler
}

pub struct DeleteFilesHandler;

impl TransactionHandler for DeleteFilesHandler {
    fn name(&self) -> &'static str { "delete_files" }

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<DeleteFilesPayload, _>(payload, bincode::config::standard()) {
            Ok((payload_data, _)) => {
                crate::db::files::delete_files(
                    state.db_pool.get(), 
                    payload_data.encrypted_path, 
                    payload_data.user_id,
                    execute
                )?;
                
                if execute {
                    tracing::info!("Deleted files at path for user {}", payload_data.user_id);
                } else {
                    tracing::debug!("Validation passed: files exist for deletion for user {}", payload_data.user_id);
                }
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &DeleteFilesHandler as &dyn TransactionHandler
}