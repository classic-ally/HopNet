use axum::{
    extract::{
        Multipart, Path, Query, State, Extension
    },
    Json,
    response::{Response, IntoResponse},
    http::header,
    body::Body
};
use reed_solomon_simd::ReedSolomonEncoder;
use reqwest::StatusCode;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
use rand::RngCore;

use crate::{db::{self, Blake3Hash, Data, DataRecord, DatabaseError, FragmentHash, Inode}, files::functions::{calculate_chunk_padding, calculate_encrypted_chunk_length, calculate_optimal_chunks, encrypt_chunk, encrypt_part, encrypt_path, store_fragment}};
use hopnet_common::FileItem;
use serde::{Deserialize, Serialize};

use super::*;
use crate::db::CustomUUID;
use either::Either::{Left, Right};
use crate::consensus::{functions::consensus_middleware, types::Transaction};
use axum::extract::multipart::Field;

#[derive(Deserialize)]
pub struct GetQueryParams {
    path: String
}

#[derive(Deserialize)]
pub struct CleanupQueryParams {
    batch_size: i32,
    retention_days: i64,
}

/// Shared file processing function for both create and modify operations
/// Handles Reed-Solomon encoding, fragmentation, encryption, and storage
/// Returns a DataRecord ready for database insertion
pub async fn process_uploaded_file(
    mut field: Field<'_>,
    file_size: usize,
    dataid: CustomUUID,
    per_file_key: &chacha20poly1305::Key,
    fragments_dir: &str,
) -> Result<DataRecord, StatusCode> {
    let mut chunk_buffer: Vec<u8> = Vec::new();
    let mut output_chunk_metadata = Vec::new();

    let (num_original_chunks, num_recovery_chunks) = calculate_optimal_chunks(file_size);
    let needed_padding = calculate_chunk_padding(file_size, num_original_chunks);
    let target_chunk_length = (file_size + needed_padding) / num_original_chunks;
    let encrypted_chunk_length = calculate_encrypted_chunk_length(target_chunk_length);
    
    tracing::debug!("process_uploaded_file: file_size={}, num_original_chunks={}, num_recovery_chunks={}", 
                   file_size, num_original_chunks, num_recovery_chunks);
    tracing::debug!("process_uploaded_file: needed_padding={}, target_chunk_length={}, encrypted_chunk_length={}", 
                   needed_padding, target_chunk_length, encrypted_chunk_length);
    
    let mut full_file_hasher = blake3::Hasher::new();

    let mut encoder = ReedSolomonEncoder::new(
        num_original_chunks,
        num_recovery_chunks,
        encrypted_chunk_length
    ).map_err(|e| {
        tracing::error!("process_uploaded_file: Reed-Solomon encoder creation failed: {:?}", e);
        tracing::error!("process_uploaded_file: Parameters - original: {}, recovery: {}, encrypted_length: {}", 
                       num_original_chunks, num_recovery_chunks, encrypted_chunk_length);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Process original chunks
    while let Some(chunk) = field.chunk().await.map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)? {
        tracing::debug!("process_uploaded_file: received HTTP chunk of {} bytes", chunk.len());
        chunk_buffer.extend_from_slice(&chunk);

        while chunk_buffer.len() >= target_chunk_length {
            let file_chunk = chunk_buffer.drain(..target_chunk_length).collect::<Vec<u8>>();
            tracing::debug!("process_uploaded_file: processing chunk {} of {} bytes", 
                   output_chunk_metadata.len(), file_chunk.len());
            full_file_hasher.update(&file_chunk);
            
            let fragment_id = CustomUUID::new(None);
            let encrypted_chunk = encrypt_chunk(file_chunk, per_file_key, &fragment_id).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            encoder.add_original_shard(&encrypted_chunk).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let chunk_hash = Blake3Hash::new(blake3::hash(&encrypted_chunk));
            store_fragment(fragments_dir, &chunk_hash, encrypted_chunk).map_err(|_| StatusCode::INSUFFICIENT_STORAGE)?;
            
            let my_chunk = FragmentHash {
                data_block_id: dataid.clone(),
                fragment_index: output_chunk_metadata.len() as i32,
                fragment_id: fragment_id,
                fragment_hash: chunk_hash,
                chunk_type: crate::db::ChunkType::Original,
                stored_locally: false
            };
            output_chunk_metadata.push(my_chunk);
        }
    }

    // Process final chunk with padding
    if !chunk_buffer.is_empty() {
        tracing::debug!("process_uploaded_file: processing final chunk of {} bytes", chunk_buffer.len());
        full_file_hasher.update(&chunk_buffer);
        let current_len = chunk_buffer.len();
        chunk_buffer.resize(target_chunk_length, 0);
        if current_len < target_chunk_length {
            rand::rng().fill_bytes(&mut chunk_buffer[current_len..]);
        }

        let final_chunk = chunk_buffer;
        let fragment_id = CustomUUID::new(None);
        let encrypted_chunk = encrypt_chunk(final_chunk, per_file_key, &fragment_id).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        encoder.add_original_shard(&encrypted_chunk).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let chunk_hash = Blake3Hash::new(blake3::hash(&encrypted_chunk));
        store_fragment(fragments_dir, &chunk_hash, encrypted_chunk).map_err(|_| StatusCode::INSUFFICIENT_STORAGE)?;
        
        let my_chunk = FragmentHash {
            data_block_id: dataid.clone(),
            fragment_index: output_chunk_metadata.len() as i32,
            fragment_id: fragment_id,
            fragment_hash: chunk_hash,
            chunk_type: crate::db::ChunkType::Original,
            stored_locally: false
        };
        output_chunk_metadata.push(my_chunk);
    }
    
    tracing::debug!("process_uploaded_file: processed {} original chunks, need {} total", 
                   output_chunk_metadata.len(), num_original_chunks);

    // For small files, ensure we have exactly the required number of original chunks
    while output_chunk_metadata.len() < num_original_chunks {
        tracing::debug!("process_uploaded_file: adding empty chunk {} for Reed-Solomon", output_chunk_metadata.len());
        let mut empty_chunk = vec![0u8; target_chunk_length];
        rand::rng().fill_bytes(&mut empty_chunk);
        let fragment_id = CustomUUID::new(None);
        let encrypted_chunk = encrypt_chunk(empty_chunk, per_file_key, &fragment_id).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        encoder.add_original_shard(&encrypted_chunk).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let chunk_hash = Blake3Hash::new(blake3::hash(&encrypted_chunk));
        store_fragment(fragments_dir, &chunk_hash, encrypted_chunk).map_err(|_| StatusCode::INSUFFICIENT_STORAGE)?;
        
        let my_chunk = FragmentHash {
            data_block_id: dataid.clone(),
            fragment_index: output_chunk_metadata.len() as i32,
            fragment_id: fragment_id,
            fragment_hash: chunk_hash,
            chunk_type: crate::db::ChunkType::Original,
            stored_locally: false
        };
        output_chunk_metadata.push(my_chunk);
    }

    // Generate file hash including data_block_id
    full_file_hasher.update(dataid.as_bytes());
    let full_file_hash = Blake3Hash::new(full_file_hasher.finalize());

    // Generate recovery chunks
    let recovery_generator = encoder.encode().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut recovery_iter = recovery_generator.recovery_iter();
    while let Some(recovery_chunk) = recovery_iter.next() {
        let fragment_id = CustomUUID::new(None);
        let chunk_hash = Blake3Hash::new(blake3::hash(&recovery_chunk));
        store_fragment(fragments_dir, &chunk_hash, recovery_chunk.to_vec()).map_err(|_| StatusCode::INSUFFICIENT_STORAGE)?;
        
        let my_chunk = FragmentHash {
            data_block_id: dataid.clone(),
            fragment_index: output_chunk_metadata.len() as i32,
            fragment_id: fragment_id,
            fragment_hash: chunk_hash,
            chunk_type: crate::db::ChunkType::Recovery,
            stored_locally: false
        };
        output_chunk_metadata.push(my_chunk);
    }

    // Create DataRecord
    let data = Data {
        hash: full_file_hash,
        fragments: output_chunk_metadata,
        added_bytes: needed_padding as u8
    };

    Ok(DataRecord {
        id: dataid,
        modified_at: None, // Deprecated - timestamps come from UUIDv7
        data: data,
        file_access_entries: None, // Will be set by caller
        file_size: file_size as u64,
    })
}

#[derive(Deserialize)]
pub struct RebalanceQueryParams {
    max_data_blocks: i32,
    min_age_heights: i32,
}

#[derive(Serialize)]
pub struct FileFragmentsResponse {
    pub file_hash: Blake3Hash,
    pub fragments: Vec<(Blake3Hash, crate::db::ChunkType)>,
}

pub async fn get_files(
    State(app_state): State<AppState>,
    Query(params): Query<GetQueryParams>
) -> Result<Json<Vec<FileItem>>, StatusCode> {
    // let's encrypt the path so we can search for it
    let enc_path = encrypt_path(params.path, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match db::files::get_files(app_state.db_pool.get(), enc_path, app_state.get_siv_key()?, app_state.get_siv_nonce()?) {
        Ok(files) => {
            Ok(Json(files))
        }
        Err(e) => {
            tracing::error!("Error getting files: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_file_fragments(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,  // Extract user_id from JWT via auth middleware
    Path(path): Path<String>
) -> Result<Response<Body>, StatusCode> {
    // Convert the path: /files/ -> "/" and /files/test -> "/test"
    let file_path = if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path)
    };
    
    // Extract filename from path for Content-Disposition header
    let filename = path.split('/').last().unwrap_or("download");
    
    // Encrypt the path for database lookup
    let enc_path = encrypt_path(file_path, app_state.get_siv_key()?, app_state.get_siv_nonce()?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    // Use shared file reconstruction logic
    let file_contents = match crate::files::download::reconstruct_file_for_user(
        &app_state,
        enc_path,
        user_id,
        &app_state.fragments_dir,
    ).await {
        Ok(contents) => contents,
        Err(e) => {
            tracing::error!("Error reconstructing file: {:?}", e);
            return Err(StatusCode::from(e));
        }
    };
    
    // Build response with proper headers
    let response = Response::builder()
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(file_contents))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    Ok(response)
}

/// Build a SelfCheckFragments transaction for fragments being uploaded
/// Only includes the new fragments from this upload, not a full reconciliation
/// Filters out fragments already in inventory to avoid PRIMARY KEY violations
async fn build_upload_attestation(
    app_state: &AppState,
    node_id: i32,
    inodes: &[Inode],
) -> Result<Option<Transaction>, Box<dyn std::error::Error>> {
    use std::collections::HashSet;
    use crate::files::types::SelfCheckFragments;

    // Extract all fragment hashes from inodes being uploaded
    // Note: stored_locally flag is not yet set at this point (happens in InsertFilesHandler)
    // but we know these fragments were just written to disk during upload
    let uploaded_fragments: Vec<Blake3Hash> = inodes
        .iter()
        .filter_map(|inode| {
            if let Some(Right(data_record)) = &inode.data_id {
                Some(
                    data_record
                        .data
                        .fragments
                        .iter()
                        .map(|f| f.fragment_hash.clone())
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            }
        })
        .flatten()
        .collect();

    if uploaded_fragments.is_empty() {
        tracing::debug!("No fragments to attest (empty upload or folder-only)");
        return Ok(None);
    }

    // Get database connection and create transaction for consistent snapshot
    let mut db_conn = app_state.db_pool.get()
        .map_err(|e| format!("Failed to get DB connection: {:?}", e))?;

    let db_tx = db_conn.transaction()
        .map_err(|e| format!("Failed to create transaction: {:?}", e))?;

    // Query current inventory count (this is our compare-and-swap value)
    let previous_count: u32 = {
        let mut stmt = db_tx
            .prepare("SELECT COUNT(*) FROM fragment_inventory WHERE node_id = ?")
            .map_err(|e| format!("Failed to prepare count query: {:?}", e))?;

        let count: i64 = stmt
            .query_row(duckdb::params![node_id], |row| row.get(0))
            .map_err(|e| format!("Failed to query inventory count: {:?}", e))?;

        count as u32
    };

    // Filter out fragments already in inventory (avoid PRIMARY KEY violation)
    let existing_fragments: HashSet<Blake3Hash> = if !uploaded_fragments.is_empty() {
        let placeholders = uploaded_fragments.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "SELECT fragment_hash FROM fragment_inventory
             WHERE node_id = ? AND fragment_hash IN ({})",
            placeholders
        );

        let mut stmt = db_tx.prepare(&query)
            .map_err(|e| format!("Failed to prepare duplicate check: {:?}", e))?;

        // Build params: node_id first, then all fragment hashes
        let mut params: Vec<Box<dyn duckdb::ToSql>> = vec![Box::new(node_id)];
        for hash in &uploaded_fragments {
            params.push(Box::new(hash.clone()));
        }
        let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let mut rows = stmt.query(param_refs.as_slice())
            .map_err(|e| format!("Failed to query existing fragments: {:?}", e))?;

        let mut set = HashSet::new();
        while let Some(row) = rows.next()
            .map_err(|e| format!("Failed to iterate rows: {:?}", e))? {
            let hash: Blake3Hash = row.get(0)
                .map_err(|e| format!("Failed to get hash: {:?}", e))?;
            set.insert(hash);
        }
        set
    } else {
        HashSet::new()
    };

    // Only include truly new fragments
    let new_fragments: Vec<Blake3Hash> = uploaded_fragments
        .into_iter()
        .filter(|h| !existing_fragments.contains(h))
        .collect();

    if new_fragments.is_empty() {
        // Rollback read-only transaction
        db_tx.rollback()
            .map_err(|e| format!("Failed to rollback transaction: {:?}", e))?;
        tracing::debug!("All uploaded fragments already in inventory, skipping attestation");
        return Ok(None);
    }

    // Get current consensus height for verification timestamp
    let self_verified_height = crate::db::consensus::get_current_consensus_height(&db_tx)
        .map_err(|e| format!("Failed to get consensus height: {:?}", e))?;

    // Rollback read-only transaction
    db_tx.rollback()
        .map_err(|e| format!("Failed to rollback transaction: {:?}", e))?;

    // Build the differential (only additions, no removals for upload)
    let attestation = SelfCheckFragments {
        node_id,
        self_verified_height,
        previous_count,
        fragments_added: new_fragments.clone(),
        fragments_removed: Vec::new(), // Upload never removes fragments
    };

    // Serialize and sign
    let payload = bincode::serde::encode_to_vec(&attestation, bincode::config::standard())
        .map_err(|e| format!("Failed to serialize attestation: {:?}", e))?;

    let transaction = crate::consensus::functions::create_signed_transaction(
        app_state,
        "self_check_fragments".to_string(),
        payload,
    )
    .map_err(|e| format!("Failed to sign attestation: {:?}", e))?;

    tracing::info!(
        "Built upload attestation: {} new fragments (filtered {} existing), previous_count={}, height={}",
        new_fragments.len(),
        existing_fragments.len(),
        attestation.previous_count,
        attestation.self_verified_height
    );

    Ok(Some(transaction))
}

pub async fn post_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,  // Extract user_id from JWT via auth middleware
    mut multipart: Multipart
) -> Result<(), StatusCode> {
    // Get user from database to access their X25519 public key
    let user = match crate::db::users::get_user_by_userid(app_state.db_pool.get(), user_id) {
        Ok(Some(user)) => user,
        Ok(None) => return Err(StatusCode::UNAUTHORIZED),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // read path part first (either "path" or "parent_item_identifier" + "filename")
    // need path in later file processing
    let mut inodes: Vec<Inode> = Vec::new();
    let mut has_files = false;
    let mut uploaded_data_block_ids: Vec<crate::db::types::CustomUUID> = Vec::new();
    let mut folder_name: Option<String> = None;

    // Handle both regular path and FileProvider parent_item_identifier approaches
    let path = match multipart.next_field().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
        Some(part) => {
            match part.name() {
                Some("path") => {
                    // Regular path approach
                    let unencrypted_path = part.text().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                    encrypt_path(unencrypted_path, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                },
                Some("parent_item_identifier") => {
                    // FileProvider approach - need to look up parent path and construct full path
                    let parent_item_identifier = part.text().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                    
                    tracing::debug!("Received parent_item_identifier: '{}'", parent_item_identifier);
                    
                    // Get parent path and return it encrypted
                    if parent_item_identifier == "NSFileProviderRootContainerItemIdentifier" {
                        tracing::debug!("Handling root container case");
                        encrypt_path("/".to_string(), app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                    } else if parent_item_identifier.starts_with("item:") {
                        // Extract inode_id and look up encrypted path
                        let inode_id_str = &parent_item_identifier[5..];
                        tracing::debug!("Trying to parse inode_id: '{}' from parent_item_identifier: '{}'", inode_id_str, parent_item_identifier);
                        let inode_id = crate::db::CustomUUID::from_str(inode_id_str).map_err(|_| StatusCode::BAD_REQUEST)?;
                
                        tracing::debug!("Looking up inode_id: {} for user_id: {}", inode_id, user_id);
                        match crate::db::fileprovider::get_item_metadata_by_inode_id(
                            app_state.db_pool.get(),
                            inode_id,
                            user_id,
                        ) {
                            Ok((encrypted_path, _, _, _, _, _)) => {
                                tracing::debug!("Found encrypted_path: {}", &encrypted_path);
                                encrypted_path
                            },
                            Err(e) => {
                                tracing::error!("Failed to find item metadata: {:?}", e);
                                return Err(StatusCode::NOT_FOUND);
                            }
                        }
                    } else {
                        return Err(StatusCode::BAD_REQUEST);
                    }
                },
                _ => return Err(StatusCode::BAD_REQUEST),
            }
        },
        None => return Err(StatusCode::BAD_REQUEST),
    };
    
    tracing::debug!("Final path after ALL path processing: '{}'", path);
    
    while let Some(mut part) = multipart.next_field().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
        match part.name() {
            Some(field_name) if field_name.starts_with("file_") => {
                has_files = true;
                // instantiate data
                let filename = part.file_name().map(|s| s.to_string()).ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                // Parse file size from field name (format: file_123456)
                let file_size_str = field_name.strip_prefix("file_").ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
                let file_size = file_size_str.parse::<usize>().map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;

                // encrypt filename - deterministic AES-SIV
                let filepath = path.clone() + &encrypt_part(&filename, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                // Generate data block ID before sharding
                let dataid = CustomUUID::new(None);
                
                // Generate per-file encryption key
                let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
                
                // Create file access entry for the authenticated user
                let file_access = crate::db::types::FileAccess::new_for_user(
                    app_state.db_pool.get(), 
                    dataid.clone(), 
                    user_id, 
                    &per_file_key
                ).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                
                // Process the uploaded file using shared function
                let datarecord = if file_size == 0 {
                    // Empty file - no need for Reed-Solomon processing, create minimal DataRecord
                    tracing::debug!("Creating empty file (size=0) without Reed-Solomon processing");
                    None
                } else {
                    match process_uploaded_file(
                        part,
                        file_size,
                        dataid.clone(),
                        &per_file_key,
                        &app_state.fragments_dir
                    ).await {
                        Ok(mut data_record) => {
                            // Add file access entry and set file size
                            data_record.file_access_entries = Some(vec![file_access]);
                            data_record.file_size = file_size as u64;
                            Some(data_record)
                        }
                        Err(status) => return Err(status),
                    }
                };

                // assemble inode for database
                let inode = match datarecord {
                    Some(data_record) => {
                        // Track this data block for distribution
                        uploaded_data_block_ids.push(dataid.clone());
                        
                        Inode {
                            id: CustomUUID::new(None),
                            owner: Left(user_id),
                            path: filepath,
                            inode_type: hopnet_common::InodeType::File,
                            data_id: Some(Right(data_record))
                        }
                    }
                    None => {
                        // Empty file - no data record, no access entries needed
                        Inode {
                            id: CustomUUID::new(None),
                            owner: Left(user_id),
                            path: filepath,
                            inode_type: hopnet_common::InodeType::File,
                            data_id: None
                        }
                    }
                };
                inodes.push(inode);
                
                    }
            Some("folder_name") => {
                folder_name = Some(part.text().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?);
            }
            Some(_) => return Err(StatusCode::UNPROCESSABLE_ENTITY),
            None => {}
        }
    }
    
    // If no files were found, create a folder
    if !has_files {
        tracing::debug!("NO FILES FOUND, FOLDER CREATION");
        let folder_path = if let Some(folder_name) = folder_name {
            // FileProvider approach - concatenate parent path + folder name (same as file creation)
            tracing::debug!("FOLDER CREATION FILEPROVIDER: '{}'", &folder_name);
            path + &encrypt_part(&folder_name, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        } else {
            // Old API approach - path already contains the full folder path
            path
        };
        tracing::debug!("CREATING: '{}'", &folder_path);
        let folder_inode = Inode {
            id: CustomUUID::new(None),
            owner: Left(user_id),
            path: folder_path,
            inode_type: hopnet_common::InodeType::Folder,
            data_id: None
        };
        inodes.push(folder_inode);
    }
    
    // Insert the collected inodes into the database via consensus
    match bincode::serde::encode_to_vec(&inodes, bincode::config::standard()) {
        Ok(encoded_inodes) => {
            let insert_files_tx = match crate::consensus::functions::create_signed_user_transaction(
                &app_state,
                "insert_files".to_string(),
                encoded_inodes,
                user_id,
            ) {
                Ok(tx) => tx,
                Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            };

            // Build upload attestation for fragments being inserted
            let mut transactions = vec![insert_files_tx];

            if let Ok(node_id) = app_state.get_node_id() {
                match build_upload_attestation(&app_state, node_id, &inodes).await {
                    Ok(Some(attestation_tx)) => {
                        tracing::debug!("Including fragment attestation in upload consensus batch");
                        transactions.push(attestation_tx);
                    }
                    Ok(None) => {
                        tracing::debug!("No new fragments to attest (folder-only or all fragments already in inventory)");
                    }
                    Err(e) => {
                        tracing::warn!("Failed to build upload attestation: {:?}. Continuing with file insert only - periodic self-check will handle attestation", e);
                        // Continue with just insert_files - eventual consistency via periodic self-check
                    }
                }
            }

            // Use consensus middleware to ensure distributed agreement
            match consensus_middleware(&app_state, transactions).await {
                Ok(()) => {
                    // Trigger fragment distribution for each uploaded file
                    for data_block_id in uploaded_data_block_ids {
                        tracing::info!("Triggering fragment distribution for uploaded file {}", data_block_id);
                        
                        // Spawn distribution task to avoid blocking the upload response
                        let app_state_clone = app_state.clone();
                        let data_block_id_clone = data_block_id.clone();
                        tokio::spawn(async move {
                            match crate::files::distribution::distribute_fragments_for_upload(&app_state_clone, data_block_id_clone).await {
                                Ok(()) => {
                                    tracing::info!("Successfully completed fragment distribution for {}", data_block_id);
                                }
                                Err(e) => {
                                    tracing::error!("Fragment distribution failed for {}: {:?}", data_block_id, e);
                                    // TODO: Add to orphan recovery queue for retry
                                }
                            }
                        });
                    }
                    
                    return Ok(());
                },
                Err(e) => {
                    tracing::error!("Consensus middleware error: {:?}", e);
                    return Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Err(e) => {
            tracing::error!("Bincode encoding error: {:?}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }

}

pub async fn delete_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,  // Extract user_id from JWT via auth middleware
    Query(params): Query<GetQueryParams>
) -> Result<(), StatusCode> {
    let enc_path = encrypt_path(params.path, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Validate that files exist before submitting to consensus
    {
        let mut conn = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let db_tx = conn.transaction().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        match crate::db::files::delete_files(&db_tx, enc_path.clone(), user_id) {
            Ok(_) => {
                // Files exist, roll back validation transaction
                db_tx.rollback().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            },
            Err(DatabaseError::NotFound) => {
                return Err(StatusCode::NOT_FOUND);
            },
            Err(e) => {
                tracing::error!("Error validating file deletion: {:?}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }
    
    // Create payload for consensus
    let payload = crate::files::handlers::DeleteFilesPayload {
        encrypted_path: enc_path,
        user_id,
    };
    
    // Serialize payload for consensus submission
    match bincode::serde::encode_to_vec(&payload, bincode::config::standard()) {
        Ok(encoded_payload) => {
            let transaction = match crate::consensus::functions::create_signed_user_transaction(
                &app_state,
                "delete_files".to_string(),
                encoded_payload,
                user_id,
            ) {
                Ok(tx) => tx,
                Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            };
            let transactions = vec![transaction];

            // Use consensus middleware to ensure distributed agreement
            match consensus_middleware(&app_state, transactions).await {
                Ok(()) => {
                    tracing::info!("Successfully submitted file deletion to consensus for user {}", user_id);
                    Ok(())
                },
                Err(e) => {
                    tracing::error!("Failed to submit file deletion to consensus: {:?}", e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        },
        Err(e) => {
            tracing::error!("Failed to serialize delete files payload: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// =============================================================================
// FRAGMENT TRANSFER ENDPOINTS (RFC-003)
// Inter-node fragment transfer for distributed storage
// =============================================================================

use crate::consensus::routes::AuthenticatedNode;
use crate::files::functions::{fetch_and_verify_fragment, fragment_exists_and_valid, MAX_FRAGMENT_SIZE};

/// GET /fragments/{fragment_hash}
/// Retrieve a fragment by its Blake3 hash from local storage
/// Used by other nodes to fetch missing fragments during file reconstruction
pub async fn get_fragment(
    State(app_state): State<AppState>,
    Path(fragment_hash): Path<Blake3Hash>,
    Extension(auth): Extension<AuthenticatedNode>,
) -> impl IntoResponse {
    // Allow any authenticated user to request fragments (supports roaming users)
    
    // Check if we have this fragment locally and verify it's valid
    if !fragment_exists_and_valid(&app_state.fragments_dir, &fragment_hash) {
        tracing::debug!("Fragment not found locally: {}", fragment_hash.to_hex());
        return StatusCode::NOT_FOUND.into_response();
    }
    
    // Fetch and verify the fragment from local storage
    match fetch_and_verify_fragment(&fragment_hash, &app_state.fragments_dir) {
        Ok(fragment_data) => {
            tracing::debug!("Successfully served fragment: {} ({} bytes)", fragment_hash.to_hex(), fragment_data.len());
            
            // Return the raw fragment bytes with appropriate content type
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", fragment_data.len().to_string())
                .header("X-Fragment-Hash", fragment_hash.to_hex())
                .body(Body::from(fragment_data))
                .unwrap()
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to fetch fragment {}: {:?}", fragment_hash.to_hex(), e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /fragments/{fragment_hash}
/// Store a fragment received from another node
/// Used during upload synchronization and background fragment replication
pub async fn post_fragment(
    State(app_state): State<AppState>,
    Path(expected_hash): Path<Blake3Hash>,
    Extension(auth): Extension<AuthenticatedNode>,
    body: Body
) -> impl IntoResponse {
    // Read the request body (fragment data) with size limit matching our fragment chunking
    let fragment_data = match axum::body::to_bytes(body, MAX_FRAGMENT_SIZE + 1024).await { // Add small buffer for headers
        Ok(data) => data.to_vec(),
        Err(e) => {
            tracing::error!("Failed to read fragment data: {:?}", e);
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    
    // Verify fragment size doesn't exceed maximum
    if fragment_data.len() > MAX_FRAGMENT_SIZE {
        tracing::warn!("Fragment too large: {} bytes (max: {})", fragment_data.len(), MAX_FRAGMENT_SIZE);
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    
    // Verify the fragment hash matches the provided hash
    let actual_hash = Blake3Hash::new(blake3::hash(&fragment_data));
    if actual_hash != expected_hash {
        tracing::warn!("Fragment hash mismatch: expected {}, got {}", expected_hash.to_hex(), actual_hash.to_hex());
        return StatusCode::BAD_REQUEST.into_response();
    }
    
    // Check if we already have this fragment to avoid redundant storage
    if fragment_exists_and_valid(&app_state.fragments_dir, &expected_hash) {
        tracing::debug!("Fragment already exists locally: {}", expected_hash.to_hex());
        return StatusCode::OK.into_response(); // Already have it, that's fine
    }
    
    // Store the fragment to local storage
    let fragment_len = fragment_data.len(); // Get length before moving data
    match store_fragment(&app_state.fragments_dir, &expected_hash, fragment_data) {
        Ok(_) => {
            // Mark the fragment as stored locally in the database
            match crate::db::files::mark_fragment_local_state(app_state.db_pool.get(), &expected_hash, true) {
                Ok(rows_affected) => {
                    tracing::debug!("Successfully stored fragment: {} ({} bytes), updated {} database records", 
                           expected_hash.to_hex(), fragment_len, rows_affected);
                }
                Err(e) => {
                    tracing::warn!("Fragment stored to disk but failed to update database for {}: {:?}", 
                          expected_hash.to_hex(), e);
                    // Continue since fragment is physically stored - database can be updated later
                }
            }
            StatusCode::CREATED.into_response()
        }
        Err(e) => {
            tracing::error!("Failed to store fragment {}: {:?}", expected_hash.to_hex(), e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /fragments  
/// Get count of fragments stored locally on this node
pub async fn get_fragments_count(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,  // Extract user_id from JWT via auth middleware
) -> impl IntoResponse {
    match crate::db::files::get_local_fragment_count(app_state.db_pool.get()) {
        Ok(count) => {
            #[derive(Serialize)]
            struct FragmentCountResponse {
                locally_stored_fragments: i64,
            }
            
            (StatusCode::OK, Json(FragmentCountResponse { locally_stored_fragments: count })).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to get local fragment count: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /fragments/{fragment_hash}/health
/// Health check endpoint that verifies fragment exists and has correct checksum
/// Used by background monitoring jobs to verify fragment integrity across the network
pub async fn get_fragment_health(
    State(app_state): State<AppState>,
    Path(fragment_hash): Path<Blake3Hash>,
    Extension(auth): Extension<AuthenticatedNode>,
) -> impl IntoResponse {
    // Only allow node owners to perform health checks for inter-node operations
    
    // Perform comprehensive health check: existence + disk read + checksum verification
    match fragment_exists_and_valid(&app_state.fragments_dir, &fragment_hash) {
        true => {
            // Double-check by actually reading and verifying the fragment
            match fetch_and_verify_fragment(&fragment_hash, &app_state.fragments_dir) {
                Ok(_) => {
                    tracing::debug!("Fragment health check passed: {}", fragment_hash.to_hex());
                    
                    #[derive(Serialize)]
                    struct HealthResponse {
                fragment_hash: String,
                status: String,
                verified: bool,
                    }
                    
                    let response = HealthResponse {
                fragment_hash: fragment_hash.to_hex(),
                status: "healthy".to_string(),
                verified: true,
                    };
                    
                    (StatusCode::OK, Json(response)).into_response()
                }
                Err(e) => {
                    tracing::warn!("Fragment health check failed for {}: {:?}", fragment_hash.to_hex(), e);
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        false => {
            tracing::debug!("Fragment health check failed - not found: {}", fragment_hash.to_hex());
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// Manual trigger for orphaned data block cleanup
pub async fn post_cleanup_orphaned_data_blocks(
    State(app_state): State<AppState>,
    Query(params): Query<CleanupQueryParams>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!("Manual cleanup trigger requested by user {} (batch_size: {}, retention_days: {})", 
                   uid, params.batch_size, params.retention_days);
    
    // Run the cleanup job directly with parameters
    match super::jobs::run_orphaned_data_block_cleanup(&app_state, params.batch_size, params.retention_days).await {
        Ok(data_blocks_cleaned) => {
            #[derive(Serialize)]
            struct CleanupResponse {
                status: String,
                data_blocks_cleaned: usize,
            }
            
            let response = CleanupResponse {
                status: "success".to_string(),
                data_blocks_cleaned,
            };
            
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!("Manual cleanup failed: {:?}", e);
            
            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }
            
            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Cleanup failed: {:?}", e),
            };
            
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// POST /rpc/fetch-fragments
/// RPC endpoint for rebalancing - instructs node to fetch specific fragments
/// Used during network rebalancing to distribute fragments to optimal nodes
pub async fn post_fetch_fragments(
    State(app_state): State<AppState>,
    Extension(auth): Extension<AuthenticatedNode>,
    Json(request): Json<FetchFragmentsRequest>
) -> impl IntoResponse {
    // Only allow node owners to receive fragment fetch instructions
    
    tracing::info!("Received fragment fetch request for {} fragments", request.fragments.len());
    
    let mut successful_fetches = 0;
    let mut failed_fetches = Vec::new();
    
    // Create node auth info for fragment discovery
    let node_auth = match crate::NodeAuthInfo::from_app_state(&app_state) {
        Ok(auth_info) => auth_info,
        Err(_) => {
            tracing::error!("Failed to create node auth info for fragment discovery");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Batch query fragment inventory for all requested fragments
    let all_hashes: Vec<Blake3Hash> = request.fragments.iter()
        .map(|f| f.fragment_hash)
        .collect();

    let mut inventory_map = match crate::db::inventory::batch_query_fragment_inventory(
        app_state.db_pool.get(),
        &all_hashes,
        None,  // Use default
    ) {
        Ok(map) => map,
        Err(e) => {
            tracing::warn!("Fragment inventory query failed, falling back to placement algorithm: {:?}", e);
            std::collections::HashMap::new()  // Empty map = all lookups return None
        }
    };

    // Process each fragment
    for fragment_info in &request.fragments {
        let fragment_hash = &fragment_info.fragment_hash;
        let placement_height = fragment_info.placement_height;

        // Check if we already have this fragment locally
        if crate::files::functions::fragment_exists_and_valid(&app_state.fragments_dir, fragment_hash) {
            tracing::debug!("Fragment {} already exists locally, skipping", fragment_hash.to_hex());
            successful_fetches += 1;
            continue;
        }

        // Get inventory hint for this fragment
        let inventory_hint = inventory_map.remove(fragment_hash);

        // Fetch and cache the fragment using existing discovery infrastructure with provided placement height
        match crate::files::functions::fetch_and_cache_fragment(
            fragment_hash,
            &app_state.fragments_dir,
            &app_state,
            Some(placement_height),
            &node_auth,
            inventory_hint,
        ).await {
            Ok(()) => {
                tracing::info!("Successfully fetched and cached fragment {} (height {})", 
                     fragment_hash.to_hex(), placement_height);
                successful_fetches += 1;
            }
            Err(e) => {
                tracing::error!("Failed to fetch fragment {} (height {}): {:?}", 
                      fragment_hash.to_hex(), placement_height, e);
                failed_fetches.push(fragment_hash.to_hex());
            }
        }
    }
    
    let response = FetchFragmentsResponse {
        status: if failed_fetches.is_empty() { "success".to_string() } else { "partial".to_string() },
        successful_fetches,
        failed_fetches,
        total_requested: request.fragments.len(),
    };
    
    tracing::info!("Fragment fetch completed: {}/{} successful", 
                  successful_fetches, request.fragments.len());
    
    (StatusCode::OK, Json(response)).into_response()
}

#[derive(Deserialize, Serialize)]
pub struct FetchFragmentsRequest {
    pub fragments: Vec<FragmentFetchInfo>,
}

#[derive(Deserialize, Serialize)]
pub struct FragmentFetchInfo {
    pub fragment_hash: Blake3Hash,
    pub placement_height: i32,
}

#[derive(Serialize, Deserialize)]
pub struct FetchFragmentsResponse {
    pub status: String,
    pub successful_fetches: usize,
    pub failed_fetches: Vec<String>,
    pub total_requested: usize,
}

/// POST /maintenance/rebalance
/// Manually trigger network rebalancing to redistribute fragments to optimal nodes
pub async fn post_rebalance_network(
    State(app_state): State<AppState>,
    Query(params): Query<RebalanceQueryParams>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!("Manual rebalancing trigger requested by user {} (max_data_blocks: {}, min_age_heights: {})", 
                   uid, params.max_data_blocks, params.min_age_heights);
    
    // Validate parameters
    if params.max_data_blocks <= 0 {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "status": "error",
            "error": "max_data_blocks must be positive"
        }))).into_response();
    }
    
    if params.min_age_heights < 0 {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "status": "error",
            "error": "min_age_heights cannot be negative"
        }))).into_response();
    }
    
    // Run the rebalancing job directly with parameters
    match super::jobs::run_network_rebalancing(&app_state, params.max_data_blocks, params.min_age_heights).await {
        Ok(result) => {
            tracing::info!("Manual rebalancing completed: {:?}", result);
            (StatusCode::OK, Json(result)).into_response()
        }
        Err(e) => {
            tracing::error!("Manual rebalancing failed: {:?}", e);
            
            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }
            
            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Rebalancing failed: {:?}", e),
            };
            
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// GET /diagnostics/fragment-inventory-differential
/// Returns the differential between consensus inventory and local fragments
/// Used for testing and monitoring the self-attestation system
pub async fn get_fragment_inventory_differential(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    let node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match crate::db::inventory::compute_inventory_differential(
        app_state.db_pool.get(),
        node_id,
    ) {
        Ok(differential) => {
            tracing::debug!("Fragment inventory differential computed for node {}: {} added, {} removed",
                          node_id, differential.fragments_added.len(), differential.fragments_removed.len());
            (StatusCode::OK, Json(differential)).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to compute fragment inventory differential: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /maintenance/fragment-inventory-self-check
/// Manually trigger fragment inventory self-check and consensus submission
pub async fn post_fragment_inventory_self_check(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!("Manual fragment inventory self-check triggered by user {}", uid);

    match super::jobs::run_fragment_inventory_self_check(&app_state).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::error!("Manual fragment inventory self-check failed: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /diagnostics/file-fragments
/// Returns complete fragment distribution data for a specific file
/// Shows which nodes have which fragments according to fragment_inventory
pub async fn get_file_fragment_distribution(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(params): Query<GetQueryParams>,
) -> impl IntoResponse {
    // Encrypt path server-side (following existing pattern)
    let siv_key = match app_state.get_siv_key() {
        Ok(key) => key,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let siv_nonce = match app_state.get_siv_nonce() {
        Ok(nonce) => nonce,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let encrypted_path = match encrypt_path(params.path, siv_key, siv_nonce).await {
        Ok(path) => path,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Query database for fragment distribution
    match crate::db::debug::get_file_fragment_distribution(
        app_state.db_pool.get(),
        encrypted_path,
        user_id,
    ) {
        Ok(distribution) => {
            tracing::debug!(
                "Fragment distribution query for file {}: {} fragments ({} original, {} recovery)",
                distribution.inode_id,
                distribution.fragment_count,
                distribution.original_count,
                distribution.recovery_count
            );
            (StatusCode::OK, Json(distribution)).into_response()
        }
        Err(DatabaseError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("Failed to get file fragment distribution: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /diagnostics/network-resilience
/// Returns network-wide file resilience statistics for system overview dashboard
/// Shows distribution of files across fault tolerance levels (cliff chart data)
pub async fn get_network_resilience_stats(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<Json<hopnet_common::db::NetworkResilienceStats>, StatusCode> {
    tracing::info!("Network resilience statistics requested by user {}", uid);

    match crate::db::resilience::compute_network_resilience_stats(app_state.db_pool.get()) {
        Ok(stats) => {
            tracing::info!(
                "Network resilience computed: {} total files ({} unknown, {} unrecoverable, {} critical, {} good, {} excellent, {} exceptional) in {}ms",
                stats.total_files,
                stats.unknown.file_count,
                stats.unrecoverable.file_count,
                stats.critical.file_count,
                stats.good.file_count,
                stats.excellent.file_count,
                stats.exceptional.file_count,
                stats.computation_time_ms
            );
            Ok(Json(stats))
        }
        Err(e) => {
            tracing::error!("Failed to compute network resilience statistics: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
