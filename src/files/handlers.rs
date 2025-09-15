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
                
                // Signal FileProvider to refresh when files are actually inserted (execute=true)
                if execute {
                    #[cfg(target_os = "macos")]
                    {
                        let test_mode = state.test_mode;
                        tokio::spawn(async move {
                            if let Err(e) = crate::fileprovider::domain::signal_fileprovider_refresh(test_mode).await {
                                tracing::warn!("Failed to signal FileProvider refresh after file insertion: {}", e);
                            }
                        });
                    }
                }
                
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ModifyItemPayload {
    pub user_id: i32,
    pub inode_id: crate::db::CustomUUID,  // Stable inode identifier
    pub new_encrypted_path: Option<String>,  // New path if renaming/moving
    // Phase 4b: Content update fields
    pub new_data_block_id: Option<crate::db::CustomUUID>,  // New content version
    pub new_data_record: Option<crate::db::DataRecord>,    // Fragment info for new version
}

pub struct ModifyItemHandler;

impl TransactionHandler for ModifyItemHandler {
    fn name(&self) -> &'static str { "modify_item" }

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<ModifyItemPayload, _>(payload, bincode::config::standard()) {
            Ok((mut payload_data, _)) => {
                tracing::debug!("ModifyItemHandler processing: inode_id={} user_id={} execute={} new_encrypted_path={:?}", 
                    payload_data.inode_id, payload_data.user_id, execute, payload_data.new_encrypted_path);
                
                // Validate fragments exist locally and update stored_locally flags (like InsertFilesHandler)
                if let Some(ref mut data_record) = payload_data.new_data_record {
                    for fragment in &mut data_record.data.fragments {
                        // Check if fragment exists and is valid on this node
                        fragment.stored_locally = fragment_exists_and_valid(
                            &state.fragments_dir, 
                            &fragment.fragment_hash
                        );
                    }
                }
                
                match crate::db::files::modify_item(
                    state.db_pool.get(),
                    payload_data.user_id,
                    payload_data.inode_id.clone(),
                    payload_data.new_encrypted_path.clone(),
                    payload_data.new_data_block_id.clone(),
                    payload_data.new_data_record.clone(),
                    execute
                ) {
                    Ok(()) => {
                        tracing::debug!("modify_item succeeded for inode_id={} user_id={} execute={}", 
                            payload_data.inode_id, payload_data.user_id, execute);
                    },
                    Err(e) => {
                        tracing::error!("modify_item failed for inode_id={} user_id={} execute={} error={:?}", 
                            payload_data.inode_id, payload_data.user_id, execute, e);
                        return Err(e);
                    }
                }
                
                if execute {
                    tracing::info!("Modified item at path for user {}", payload_data.user_id);
                    
                    // Signal FileProvider to refresh when item is actually modified
                    #[cfg(target_os = "macos")]
                    {
                        let test_mode = state.test_mode;
                        tokio::spawn(async move {
                            if let Err(e) = crate::fileprovider::domain::signal_fileprovider_refresh(test_mode).await {
                                tracing::warn!("Failed to signal FileProvider refresh after item modification: {}", e);
                            }
                        });
                    }
                } else {
                    tracing::debug!("Validation passed: item exists for modification for user {}", payload_data.user_id);
                }
                
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &ModifyItemHandler as &dyn TransactionHandler
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
                    
                    // Signal FileProvider to refresh when files are actually deleted
                    #[cfg(target_os = "macos")]
                    {
                        let test_mode = state.test_mode;
                        tokio::spawn(async move {
                            if let Err(e) = crate::fileprovider::domain::signal_fileprovider_refresh(test_mode).await {
                                tracing::warn!("Failed to signal FileProvider refresh after file deletion: {}", e);
                            }
                        });
                    }
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