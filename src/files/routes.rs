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
use axum::http::StatusCode;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
use rand::RngCore;

use crate::{db::{self, Blake3Hash, Data, DataRecord, DatabaseError, FragmentHash, Inode}, files::functions::{calculate_chunk_padding, encrypt_chunk, encrypt_part, encrypt_path, store_fragment}};
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

/// Process a single logical chunk with Reed-Solomon encoding (10 original + 20 recovery)
/// This is called for each 40MB chunk (or smaller for the last chunk)
/// Returns the padding bytes added to this chunk
fn process_logical_chunk(
    encoder: &mut ReedSolomonEncoder,
    chunk_data: &[u8],
    chunk_number: u32,
    data_block_id: &CustomUUID,
    per_file_key: &chacha20poly1305::Key,
    fragments_dir: &str,
    output_metadata: &mut Vec<FragmentHash>,
) -> Result<usize, StatusCode> {
    use crate::files::functions::{ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK, calculate_chunk_padding, calculate_padding_and_chunks};

    let chunk_size = chunk_data.len();

    // Calculate padding needed to evenly divide into 10 fragments
    let padding = calculate_chunk_padding(chunk_size, ORIGINAL_FRAGMENTS_PER_CHUNK);
    let padded_size = chunk_size + padding;
    let fragment_size = padded_size / ORIGINAL_FRAGMENTS_PER_CHUNK;

    tracing::debug!("process_logical_chunk: chunk_number={}, size={}, padding={}, fragment_size={}",
                   chunk_number, chunk_size, padding, fragment_size);

    // Pad and split into 10 equal fragments
    let mut padded_chunk = chunk_data.to_vec();
    if padding > 0 {
        padded_chunk.resize(padded_size, 0);
        rand::rng().fill_bytes(&mut padded_chunk[chunk_size..]);
    }

    let (fragment_chunks, _) = calculate_padding_and_chunks(padded_chunk, ORIGINAL_FRAGMENTS_PER_CHUNK);

    // Encrypt each fragment and calculate encrypted size for RS encoder
    let mut encrypted_fragments = Vec::new();
    for (local_index, fragment_data) in fragment_chunks.into_iter().enumerate() {
        let fragment_id = CustomUUID::new(None);
        let encrypted_fragment = encrypt_chunk(fragment_data, per_file_key, &fragment_id)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        encrypted_fragments.push((fragment_id, encrypted_fragment));
    }

    // All encrypted fragments should have the same size (RS requirement)
    let encrypted_fragment_size = encrypted_fragments[0].1.len();

    // Reset Reed-Solomon encoder for this chunk's shard size (reuses tables + workspace)
    encoder.reset(
        ORIGINAL_FRAGMENTS_PER_CHUNK,
        RECOVERY_FRAGMENTS_PER_CHUNK,
        encrypted_fragment_size
    ).map_err(|e| {
        tracing::error!("Reed-Solomon encoder reset failed for chunk {}: {:?}", chunk_number, e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Add encrypted fragments to encoder and store them
    for (local_index, (fragment_id, encrypted_fragment)) in encrypted_fragments.into_iter().enumerate() {
        // Calculate hash and add to encoder first (both only need borrows)
        let fragment_hash = Blake3Hash::new(blake3::hash(&encrypted_fragment));
        encoder.add_original_shard(&encrypted_fragment)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Now move encrypted_fragment to storage (no clone needed)
        store_fragment(fragments_dir, &fragment_hash, encrypted_fragment)
            .map_err(|_| StatusCode::INSUFFICIENT_STORAGE)?;

        output_metadata.push(FragmentHash {
            data_block_id: data_block_id.clone(),
            chunk_number,
            local_index: local_index as u32,
            fragment_id,
            fragment_hash,
            chunk_type: crate::db::ChunkType::Original,
            stored_locally: false,
        });
    }

    // Generate recovery fragments
    let recovery_generator = encoder.encode().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut recovery_iter = recovery_generator.recovery_iter();

    let mut recovery_index = ORIGINAL_FRAGMENTS_PER_CHUNK;
    while let Some(recovery_fragment) = recovery_iter.next() {
        let fragment_id = CustomUUID::new(None);
        let fragment_hash = Blake3Hash::new(blake3::hash(&recovery_fragment));

        store_fragment(fragments_dir, &fragment_hash, recovery_fragment.to_vec())
            .map_err(|_| StatusCode::INSUFFICIENT_STORAGE)?;

        output_metadata.push(FragmentHash {
            data_block_id: data_block_id.clone(),
            chunk_number,
            local_index: recovery_index as u32,
            fragment_id,
            fragment_hash,
            chunk_type: crate::db::ChunkType::Recovery,
            stored_locally: false,
        });

        recovery_index += 1;
    }

    tracing::debug!("process_logical_chunk: chunk {} complete, created {} fragments (10 original + 20 recovery), padding={} bytes",
                   chunk_number, output_metadata.len() - (chunk_number as usize * 30), padding);

    Ok(padding)
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
    use crate::files::functions::{calculate_chunked_fragments, CHUNK_SIZE, ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK};

    let mut output_chunk_metadata = Vec::new();
    let mut full_file_hasher = blake3::Hasher::new();

    // Calculate chunked Reed-Solomon parameters
    let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(file_size);

    tracing::debug!("process_uploaded_file: file_size={}, num_chunks={}, total_original={}, total_recovery={}",
                   file_size, num_chunks, total_original, total_recovery);

    // Create Reed-Solomon encoder once with max shard size (for 40MB chunk)
    // Max encrypted fragment size = (40MB / 10 fragments) + 28 bytes encryption overhead
    let max_fragment_size = (CHUNK_SIZE / ORIGINAL_FRAGMENTS_PER_CHUNK) + 28;
    let mut encoder = ReedSolomonEncoder::new(
        ORIGINAL_FRAGMENTS_PER_CHUNK,
        RECOVERY_FRAGMENTS_PER_CHUNK,
        max_fragment_size
    ).map_err(|e| {
        tracing::error!("Reed-Solomon encoder creation failed: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Stream file data and process each logical 40MB chunk independently
    let mut logical_chunk_buffer: Vec<u8> = Vec::new();
    let mut current_chunk_number = 0u32;
    let mut last_chunk_padding = 0usize;

    // Read HTTP chunks and buffer them into logical chunks
    while let Some(http_chunk) = field.chunk().await.map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)? {
        tracing::debug!("process_uploaded_file: received HTTP chunk of {} bytes", http_chunk.len());
        logical_chunk_buffer.extend_from_slice(&http_chunk);
        full_file_hasher.update(&http_chunk);

        // Process complete 40MB logical chunks
        while logical_chunk_buffer.len() >= CHUNK_SIZE {
            let chunk_data: Vec<u8> = logical_chunk_buffer.drain(..CHUNK_SIZE).collect();

            tracing::debug!("process_uploaded_file: encoding logical chunk {} ({} bytes)",
                           current_chunk_number, chunk_data.len());

            // Process this chunk with Reed-Solomon encoding (10 original + 20 recovery)
            last_chunk_padding = process_logical_chunk(
                &mut encoder,
                &chunk_data,
                current_chunk_number,
                &dataid,
                per_file_key,
                fragments_dir,
                &mut output_chunk_metadata,
            )?;

            current_chunk_number += 1;
        }
    }

    // Process final partial chunk (if any remaining data < 40MB)
    if !logical_chunk_buffer.is_empty() {
        tracing::debug!("process_uploaded_file: processing final partial chunk {} ({} bytes)",
                       current_chunk_number, logical_chunk_buffer.len());

        // Process the final chunk (will be < 40MB, gets padded internally)
        last_chunk_padding = process_logical_chunk(
            &mut encoder,
            &logical_chunk_buffer,
            current_chunk_number,
            &dataid,
            per_file_key,
            fragments_dir,
            &mut output_chunk_metadata,
        )?;
    }

    // Generate file hash including data_block_id
    full_file_hasher.update(dataid.as_bytes());
    let full_file_hash = Blake3Hash::new(full_file_hasher.finalize());

    tracing::debug!("process_uploaded_file: complete - {} chunks, {} fragments, {} bytes padding in last chunk",
                   num_chunks, output_chunk_metadata.len(), last_chunk_padding);

    // Create DataRecord
    let data = Data {
        hash: full_file_hash,
        fragments: output_chunk_metadata,
        added_bytes: last_chunk_padding as u8  // Only padding from last chunk matters
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

/// Build share propagation data for a content update on a shared file.
/// Returns (extra_file_access_entries, incoming_share_updates).
/// Called by both PATCH /files and fileprovider modify routes.
pub fn build_share_propagation(
    conn: &duckdb::Connection,
    inode_id: &CustomUUID,
    user_id: i32,
    new_data_block_id: &CustomUUID,
    per_file_key: &chacha20poly1305::Key,
) -> Result<(Vec<crate::db::types::FileAccess>, Option<Vec<crate::shares::types::IncomingShareUpdate>>), StatusCode> {
    // Look up the inode's current data_id (old data_block)
    let inode_info = crate::db::files::get_inode_by_id(conn, inode_id, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let old_data_block_id = match inode_info.0 {
        Some(id) => id,
        None => return Ok((vec![], None)), // No data block → no shares to propagate
    };

    let sharers = crate::db::shares::get_sharers_for_data_block_conn(conn, &old_data_block_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if sharers.is_empty() {
        return Ok((vec![], None));
    }

    // Create FileAccess entries for other accepted sharers
    let mut extra_file_access_entries = Vec::new();
    for &sharer_id in &sharers {
        if sharer_id == user_id { continue; }
        let fa = crate::db::types::FileAccess::new_for_user_with_conn(
            conn, new_data_block_id.clone(), sharer_id, per_file_key,
        ).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        extra_file_access_entries.push(fa);
    }

    // Build IncomingShareUpdate entries for pending shares
    let pending = crate::db::shares::get_incoming_shares_for_data_block_conn(conn, &old_data_block_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let incoming_share_updates = if pending.is_empty() {
        None
    } else {
        let mut updates = Vec::new();
        for incoming in &pending {
            let fa = crate::db::types::FileAccess::new_for_user_with_conn(
                conn, new_data_block_id.clone(), incoming.recipient_id, per_file_key,
            ).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let blob = bincode::serde::encode_to_vec(&fa, bincode::config::standard())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            updates.push(crate::shares::types::IncomingShareUpdate {
                incoming_share_id: incoming.id.clone(),
                new_file_access_blob: blob,
            });
        }
        Some(updates)
    };

    Ok((extra_file_access_entries, incoming_share_updates))
}

pub async fn get_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(params): Query<GetQueryParams>
) -> Result<Json<Vec<FileItem>>, StatusCode> {
    let session = app_state.get_session(user_id).await?;
    // let's encrypt the path so we can search for it
    let enc_path = encrypt_path(params.path, &session.siv_key, &session.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match db::files::get_files(app_state.db_pool.get(), enc_path, user_id, &session.siv_key, &session.siv_nonce) {
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
    let session = app_state.get_session(user_id).await?;
    // Convert the path: /files/ -> "/" and /files/test -> "/test"
    let file_path = if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path)
    };

    // Extract filename from path for Content-Disposition header
    let filename = path.split('/').last().unwrap_or("download");

    // Encrypt the path for database lookup
    let enc_path = encrypt_path(file_path, &session.siv_key, &session.siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    // Use shared file reconstruction logic (returns stream for memory efficiency)
    let stream = match crate::files::download::reconstruct_file_for_user(
        &app_state,
        enc_path,
        user_id,
        &app_state.fragments_dir,
    ).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::error!("Error reconstructing file: {:?}", e);
            return Err(StatusCode::from(e));
        }
    };

    // Build streaming response with proper headers
    let response = Response::builder()
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from_stream(stream))
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
    let session = app_state.get_session(user_id).await?;
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
                    encrypt_path(unencrypted_path, &session.siv_key, &session.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                },
                Some("parent_item_identifier") => {
                    // FileProvider approach - need to look up parent path and construct full path
                    let parent_item_identifier = part.text().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                    
                    tracing::debug!("Received parent_item_identifier: '{}'", parent_item_identifier);
                    
                    // Get parent path and return it encrypted
                    if parent_item_identifier == "NSFileProviderRootContainerItemIdentifier" {
                        tracing::debug!("Handling root container case");
                        encrypt_path("/".to_string(), &session.siv_key, &session.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
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
                let filepath = path.clone() + &encrypt_part(&filename, &session.siv_key, &session.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
            path + &encrypt_part(&folder_name, &session.siv_key, &session.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
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
            ).await {
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
    let session = app_state.get_session(user_id).await?;
    let enc_path = encrypt_path(params.path, &session.siv_key, &session.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Validate that files exist before submitting to consensus
    // IMPORTANT: Use a fresh transaction to avoid snapshot isolation issues
    // Transactions capture a snapshot at creation time, which may not see recently checkpointed data
    {
        let conn = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Quick existence check without full transaction to avoid stale snapshot
        let exists: Result<i32, _> = conn.query_row(
            "SELECT COUNT(*) FROM inodes WHERE path = ? AND owner_id = ?",
            duckdb::params![enc_path.clone(), user_id],
            |row| row.get(0)
        );

        match exists {
            Ok(count) if count > 0 => {
                // File exists, proceed to consensus
            },
            Ok(_) => {
                return Err(StatusCode::NOT_FOUND);
            },
            Err(e) => {
                tracing::error!("Error validating file existence: {:?}", e);
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
            ).await {
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

/// PATCH /files — JWT-authenticated content update for an existing file
pub async fn patch_files(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    mut multipart: Multipart,
) -> Result<(), StatusCode> {
    // First field must be inode_id
    let inode_id = match multipart.next_field().await.map_err(|_| StatusCode::BAD_REQUEST)? {
        Some(field) if field.name() == Some("inode_id") => {
            let id_str = field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?;
            CustomUUID::from_str(&id_str).map_err(|_| StatusCode::BAD_REQUEST)?
        }
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    // Validate inode exists, belongs to user, and is a file
    let conn = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let inode_info = crate::db::files::get_inode_by_id(&conn, &inode_id, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if inode_info.2 != hopnet_common::InodeType::File {
        return Err(StatusCode::BAD_REQUEST);
    }
    drop(conn);

    // Second field must be file_<size>
    let field = multipart.next_field().await.map_err(|_| StatusCode::BAD_REQUEST)?
        .ok_or(StatusCode::BAD_REQUEST)?;
    let field_name = field.name().ok_or(StatusCode::BAD_REQUEST)?.to_string();
    let file_size_str = field_name.strip_prefix("file_").ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
    let file_size = file_size_str.parse::<usize>().map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;

    let (dataid, data_record, incoming_share_updates, _per_file_key) =
        crate::files::functions::prepare_content_update(
            &app_state, user_id, &inode_id, field, file_size,
        ).await?;

    // Build, validate, and submit ModifyItemPayload
    let payload = crate::files::handlers::ModifyItemPayload {
        user_id,
        inode_id,
        new_encrypted_path: None,
        new_data_block_id: Some(dataid),
        new_data_record: Some(data_record),
        incoming_share_updates,
    };

    {
        let mut conn = app_state.db_pool.get().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let db_tx = conn.transaction().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        crate::db::files::modify_item(
            &db_tx, payload.user_id, payload.inode_id.clone(),
            payload.new_encrypted_path.clone(), payload.new_data_block_id.clone(),
            payload.new_data_record.clone(), None,
        ).map_err(|e| match e {
            crate::db::DatabaseError::NotFound => StatusCode::NOT_FOUND,
            crate::db::DatabaseError::ConflictError => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
        db_tx.rollback().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let transaction = crate::consensus::functions::create_signed_user_transaction(
        &app_state, "modify_item".to_string(), encoded, user_id,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    consensus_middleware(&app_state, vec![transaction]).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(())
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

#[derive(Deserialize)]
pub struct OrphanedFragmentsScanParams {
    #[serde(default = "default_grace_period_hours")]
    grace_period_hours: i64,
}

fn default_grace_period_hours() -> i64 {
    1
}

/// GET /maintenance/orphaned-fragments
/// Scan filesystem for fragments not in database (older than grace_period_hours)
/// Returns scan results and stores them for subsequent DELETE operation
pub async fn get_orphaned_fragments_scan(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
    Query(params): Query<OrphanedFragmentsScanParams>,
) -> impl IntoResponse {
    tracing::info!("Orphaned fragments scan triggered by user {} (grace_period_hours: {})",
                   uid, params.grace_period_hours);

    if params.grace_period_hours < 0 {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "status": "error",
            "error": "grace_period_hours cannot be negative"
        }))).into_response();
    }

    match super::jobs::run_orphaned_fragments_scan(&app_state, params.grace_period_hours).await {
        Ok(scan_result) => {
            tracing::info!("Scan complete: {} orphaned fragments found ({} bytes)",
                          scan_result.orphaned_fragments.len(), scan_result.total_bytes);
            (StatusCode::OK, Json(scan_result)).into_response()
        }
        Err(e) => {
            tracing::error!("Orphaned fragments scan failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Scan failed: {:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// DELETE /maintenance/orphaned-fragments
/// Delete orphaned fragments based on previous scan results
/// Validates scan exists and isn't stale (> 1 hour old)
pub async fn delete_orphaned_fragments(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!("Orphaned fragments cleanup triggered by user {}", uid);

    match super::jobs::run_orphaned_fragments_cleanup(&app_state).await {
        Ok(result) => {
            tracing::info!("Cleanup complete: {} deleted, {} failed, {} bytes freed",
                          result.deleted_count, result.failed_count, result.bytes_freed);
            (StatusCode::OK, Json(result)).into_response()
        }
        Err(e) => {
            tracing::error!("Orphaned fragments cleanup failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("{:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
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
    let session = match app_state.get_session(user_id).await {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };

    let encrypted_path = match encrypt_path(params.path, &session.siv_key, &session.siv_nonce).await {
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
