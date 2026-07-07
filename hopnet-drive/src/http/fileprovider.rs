//! /integrations/fileprovider routes — macOS FileProvider surface.
//! Moved from the host's `fileprovider::routes` (RFC-015 Stage D4). The
//! host keeps the handlers that touch host-only concerns (health/setup DB,
//! test-mode device registration, domain signal counters) and layers the
//! device-token auth middleware + body limit around this router.

use axum::{
    Extension, Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, patch, post},
};
use serde::Deserialize;
use std::str::FromStr;

use crate::db;
use crate::host::{DriveState, TxSigner, TxSpec};
use crate::paths::{build_encrypted_path, decrypt_path, encrypt_part, encrypt_path};
use crate::upload::session_or_status;
use hopnet_common::CustomUUID;
use hopnet_common::fileprovider::{
    ChangesQuery, ChangesResponse, DeleteItemRequest, DownloadQuery, EnumerateResponse,
    FileProviderItem, ItemQuery, ModifyItemResponse,
};
use hopnet_projection::DatabaseError;

pub fn router<S: Clone + Send + Sync + 'static>(state: DriveState) -> Router<S> {
    // Reads bypass the import gate; writes have the gate applied via a
    // sub-router so attachment is explicit (no in-middleware method peek).
    let reads = Router::new()
        .route("/enumerate", get(get_enumerate))
        .route("/changes", get(get_changes))
        .route("/download", get(download_file))
        .route("/item", get(get_item));

    let writes = Router::new()
        .route("/delete", delete(delete_item))
        .route("/create", post(create_item))
        .route("/modify", patch(modify_item))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            super::write_gate,
        ));

    reads.merge(writes).with_state(state)
}

#[derive(Debug, Deserialize)]
pub struct EnumerateQuery {
    /// Parent folder path (decoded by FileProvider binary, e.g., "/documents" or "/" for root)
    pub parent_path: Option<String>,
    /// Parent item identifier (e.g., "item:uuid" or "NSFileProviderRootContainerItemIdentifier")
    pub parent_item_identifier: Option<String>,
    /// Pagination token (base64-encoded cursor)
    pub page: Option<String>,
}

/// File enumeration endpoint for FileProvider extension
/// Returns directory contents with pagination support
pub async fn get_enumerate(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<EnumerateQuery>,
) -> Result<Json<EnumerateResponse>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;
    let siv_key = &session.siv_key;
    let siv_nonce = &session.siv_nonce;

    // Handle both parent_path and parent_item_identifier approaches
    let (_parent_path, encrypted_parent_path) = if let Some(path) = query.parent_path {
        // Legacy path approach - encrypt the decrypted path
        let encrypted = encrypt_path(path.clone(), siv_key, siv_nonce)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        (path, encrypted)
    } else if let Some(identifier) = query.parent_item_identifier {
        // New identifier approach
        if identifier == "NSFileProviderRootContainerItemIdentifier" {
            let encrypted = encrypt_path("/".to_string(), siv_key, siv_nonce)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            ("/".to_string(), encrypted)
        } else if let Some(inode_id_str) = identifier.strip_prefix("item:") {
            let inode_id =
                CustomUUID::from_str(inode_id_str).map_err(|_| StatusCode::BAD_REQUEST)?;

            match db::fileprovider::get_item_metadata_by_inode_id(
                state.db_pool.get(),
                inode_id,
                user_id,
            ) {
                Ok((encrypted_path, _, _, _, _, _)) => {
                    // Decrypt for response construction, but use encrypted for database query
                    let decrypted = decrypt_path(encrypted_path.clone(), siv_key, siv_nonce)
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                    (decrypted, encrypted_path)
                }
                Err(_) => return Err(StatusCode::NOT_FOUND),
            }
        } else {
            return Err(StatusCode::BAD_REQUEST);
        }
    } else {
        // Neither provided - default to root
        let encrypted = encrypt_path("/".to_string(), siv_key, siv_nonce)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        ("/".to_string(), encrypted)
    };

    // Create SQL pattern for direct children only (same as get_files)
    let parent_path_pattern = format!("{}/%", encrypted_parent_path);

    // Decode pagination cursor if provided (hex-encoded path)
    let cursor = query.page.as_ref().and_then(|page| {
        hex::decode(page)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
    });

    const PAGE_SIZE: usize = 100;
    let limit = PAGE_SIZE + 1; // Request one extra to check if there are more pages

    match db::fileprovider::get_folder_contents(
        state.db_pool.get(),
        user_id,
        &parent_path_pattern,
        siv_key,
        siv_nonce,
        cursor.as_deref(),
        limit,
    ) {
        Ok(result) => {
            let mut items = result.items;
            let current_consensus_height = result.current_consensus_height;

            // Check if there are more pages (if we got more items than page size)
            let next_page = if items.len() > PAGE_SIZE {
                // Remove the extra item
                items.pop();
                // Create next page cursor from the last item's filename (hex-encoded)
                items
                    .last()
                    .map(|item| hex::encode(item.filename.as_bytes()))
            } else {
                None
            };

            // Convert database items to API response format
            let response_items: Vec<FileProviderItem> = items
                .into_iter()
                .map(|item| {
                    FileProviderItem {
                        identifier: item.identifier,
                        item_type: item.item_type, // Already InodeType from database
                        filename: item.filename,
                        parent_item_identifier: item.parent_item_identifier, // Use parent_item_identifier from database
                        file_size: item.file_size.map(|size| size.to_string()), // Convert u64 to String for API
                        creation_date: item.creation_date.map(|dt| (*dt).to_rfc3339()), // Dereference CustomDateTime and convert to ISO 8601 string
                        content_modification_date: item
                            .content_modification_date
                            .map(|dt| (*dt).to_rfc3339()), // Dereference CustomDateTime and convert to ISO 8601 string
                        modification_height: item.modification_height, // Pass through the consensus height
                    }
                })
                .collect();

            Ok(Json(EnumerateResponse {
                items: response_items,
                next_page,
                current_consensus_height,
            }))
        }
        Err(_) => {
            // Return error status code, axum will handle the response
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Changes endpoint for FileProvider incremental sync
/// Returns all folders + files changed since the given consensus height
pub async fn get_changes(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ChangesQuery>,
) -> Result<Json<ChangesResponse>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;
    let siv_key = &session.siv_key;
    let siv_nonce = &session.siv_nonce;

    // Encrypt the parent path since FileProvider sends decrypted paths but database stores encrypted paths
    let parent_path = query.parent_path.unwrap_or_else(|| "/".to_string());
    let encrypted_parent_path = encrypt_path(parent_path.clone(), siv_key, siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Default to height 0 for initial sync if not provided
    let since_height = query.since_height.unwrap_or(0);

    match db::fileprovider::get_folder_changes_since_height(
        state.db_pool.get(),
        user_id,
        &encrypted_parent_path,
        since_height,
        siv_key,
        siv_nonce,
    ) {
        Ok(result) => {
            let items = result.items;
            let deleted_identifiers = result.deleted_identifiers.unwrap_or_default();
            let current_consensus_height = result.current_consensus_height;

            // Convert database items to API response format
            let response_items: Vec<FileProviderItem> = items
                .into_iter()
                .map(|item| {
                    FileProviderItem {
                        identifier: item.identifier,
                        item_type: item.item_type,
                        filename: item.filename,
                        parent_item_identifier: item.parent_item_identifier, // Use parent_item_identifier from database
                        file_size: item.file_size.map(|size| size.to_string()), // Convert u64 to String for API
                        creation_date: item.creation_date.map(|dt| (*dt).to_rfc3339()), // Dereference CustomDateTime and convert to ISO 8601 string
                        content_modification_date: item
                            .content_modification_date
                            .map(|dt| (*dt).to_rfc3339()), // Dereference CustomDateTime and convert to ISO 8601 string
                        modification_height: item.modification_height, // Pass through the consensus height
                    }
                })
                .collect();

            Ok(Json(ChangesResponse {
                items: response_items,
                deleted_identifiers,
                current_consensus_height,
            }))
        }
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Delete endpoint for FileProvider
/// Accepts an item identifier via JSON body, resolves it to a path, and submits deletion through consensus
pub async fn delete_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Json(request): Json<DeleteItemRequest>,
) -> impl IntoResponse {
    // Parse the unified item: identifier
    let (encrypted_path, is_folder): (String, bool) = if request.identifier.starts_with("item:") {
        // Extract inode_id from unified identifier
        let inode_id_str = &request.identifier[5..]; // Skip "item:" prefix
        let inode_id = match CustomUUID::from_str(inode_id_str) {
            Ok(uuid) => uuid,
            Err(_) => return StatusCode::BAD_REQUEST,
        };

        // Get metadata for the item
        match db::fileprovider::get_item_metadata_by_inode_id(
            state.db_pool.get(),
            inode_id,
            user_id,
        ) {
            Ok((encrypted_path, item_type, _, _, _, _)) => {
                let is_folder = item_type == hopnet_common::InodeType::Folder;
                (encrypted_path, is_folder)
            }
            Err(DatabaseError::NotFound) => return StatusCode::NOT_FOUND,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        }
    } else if request.identifier == "NSFileProviderRootContainerItemIdentifier" {
        // Cannot delete root container
        return StatusCode::FORBIDDEN;
    } else {
        // Invalid identifier format - only item: identifiers supported
        return StatusCode::BAD_REQUEST;
    };

    // If this is a folder and recursive is false, check if it's empty
    if is_folder && !request.recursive {
        match db::fileprovider::is_folder_empty(state.db_pool.get(), &encrypted_path, user_id) {
            Ok(true) => {
                // Folder is empty, proceed with deletion
            }
            Ok(false) => {
                // Folder is not empty and recursive is false
                tracing::info!(
                    "Folder not empty and recursive=false for path: {}",
                    encrypted_path
                );
                return StatusCode::CONFLICT; // 409 Conflict - folder not empty
            }
            Err(e) => {
                tracing::error!("Error checking if folder is empty: {:?}", e);
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        }
    }

    // Validate that the item exists and user has access before submitting to consensus
    {
        let mut conn = match state.db_pool.get() {
            Ok(c) => c,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        };
        let db_tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        };

        match db::files::delete_files(&db_tx, encrypted_path.clone(), user_id) {
            Ok(_) => {
                // Item exists and user has access, roll back validation transaction
                if db_tx.rollback().is_err() {
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            Err(DatabaseError::NotFound) => {
                return StatusCode::NOT_FOUND;
            }
            Err(e) => {
                tracing::error!("Error validating file deletion: {:?}", e);
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        }
    }

    // Create payload for consensus
    let payload = crate::envelopes::DeleteFilesPayload {
        encrypted_path,
        user_id,
    };

    // Serialize payload for consensus submission
    let encoded_payload = match bincode::serde::encode_to_vec(&payload, bincode::config::standard())
    {
        Ok(encoded) => encoded,
        Err(e) => {
            tracing::error!("Failed to encode delete payload: {:?}", e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    // Sign (host-side) and submit through the consensus gateway
    match state
        .txs
        .submit(TxSpec {
            function: "delete_files",
            payload: encoded_payload,
            signer: TxSigner::User(user_id),
        })
        .await
    {
        Ok(()) => {
            tracing::info!(
                "Successfully submitted FileProvider deletion to consensus for user {}",
                user_id
            );
            StatusCode::OK
        }
        Err(crate::host::TxSubmitError::Signing) => StatusCode::INTERNAL_SERVER_ERROR,
        Err(e) => {
            tracing::error!("Failed to submit deletion to consensus: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Download endpoint for FileProvider
/// Downloads file content by identifier and returns streaming response
pub async fn download_file(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<DownloadQuery>,
) -> impl IntoResponse {
    // Only handle item: identifiers for downloads
    if !query.identifier.starts_with("item:") {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }

    let session = match state.sessions.user_session(user_id).await {
        Ok(s) => s,
        Err(_) => return axum::http::StatusCode::PRECONDITION_REQUIRED.into_response(),
    };
    let siv_key = &session.siv_key;
    let siv_nonce = &session.siv_nonce;

    // Extract inode_id from unified identifier
    let inode_id_str = &query.identifier[5..]; // Skip "item:" prefix
    let inode_id = match CustomUUID::from_str(inode_id_str) {
        Ok(uuid) => uuid,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };

    // Get metadata and ensure it's a file
    let (encrypted_path, item_type, file_size, _, _, _) =
        match db::fileprovider::get_item_metadata_by_inode_id(
            state.db_pool.get(),
            inode_id,
            user_id,
        ) {
            Ok(metadata) => metadata,
            Err(DatabaseError::NotFound) => {
                return axum::http::StatusCode::NOT_FOUND.into_response();
            }
            Err(_) => return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

    // Only allow downloads for files, not folders
    if item_type != hopnet_common::InodeType::File {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }

    // Decrypt path to get the route path format
    let decrypted_path = match decrypt_path(encrypted_path, siv_key, siv_nonce) {
        Ok(path) => path,
        Err(_) => return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    tracing::debug!("Download request - file_size: {:?}", file_size);

    // Handle empty files (0 bytes) directly without going through fragment system
    // Empty files can have file_size == Some(0) OR file_size == None (no data_blocks entry)
    if file_size == Some(0) || file_size.is_none() {
        use axum::body::Body;
        use axum::http::header;
        use axum::response::Response;

        // Extract filename from path for Content-Disposition header
        let filename = decrypted_path.split('/').next_back().unwrap_or("download");

        return Response::builder()
            .status(axum::http::StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, "0")
            .header(
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", filename),
            )
            .body(Body::empty())
            .unwrap_or_else(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    // Convert to route path format (remove leading slash)
    let route_path = decrypted_path.trim_start_matches('/').to_string();

    // Forward to existing file download route (no Range header from fileprovider)
    super::files::get_file_fragments(
        State(state),
        axum::extract::Extension(user_id),
        axum::extract::Path(route_path),
        axum::http::HeaderMap::new(),
    )
    .await
    .into_response()
}

/// Get individual item metadata by identifier
/// Returns metadata for a single file or folder by its FileProvider identifier
pub async fn get_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
) -> Result<Json<FileProviderItem>, StatusCode> {
    let session = session_or_status(&state, user_id).await?;
    let siv_key = &session.siv_key;
    let siv_nonce = &session.siv_nonce;

    // Handle special root container case
    if query.identifier == "NSFileProviderRootContainerItemIdentifier" {
        return Ok(Json(FileProviderItem {
            identifier: query.identifier,
            filename: "HopNet".to_string(),
            parent_item_identifier: "NSFileProviderRootContainerItemIdentifier".to_string(),
            item_type: hopnet_common::InodeType::Folder,
            file_size: None,                 // Folders don't have size
            creation_date: None,             // Root container doesn't have timestamps
            content_modification_date: None, // Root container doesn't have timestamps
            modification_height: None,       // Root container doesn't have modification height
        }));
    }

    // Parse the unified identifier to get inode_id
    if !query.identifier.starts_with("item:") {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Extract inode_id from unified identifier
    let inode_id_str = &query.identifier[5..]; // Skip "item:" prefix
    let inode_id = match CustomUUID::from_str(inode_id_str) {
        Ok(uuid) => uuid,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    // Get metadata for any item type (file or folder)
    let (
        encrypted_path,
        item_type,
        file_size,
        creation_date,
        content_modification_date,
        modification_height,
    ) = match db::fileprovider::get_item_metadata_by_inode_id(state.db_pool.get(), inode_id, user_id)
    {
        Ok(metadata) => metadata,
        Err(DatabaseError::NotFound) => return Err(StatusCode::NOT_FOUND),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Decrypt path to get filename and parent
    let decrypted_path = match decrypt_path(encrypted_path.clone(), siv_key, siv_nonce) {
        Ok(path) => path,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Extract filename from path
    let filename = decrypted_path
        .split('/')
        .next_back()
        .unwrap_or("item")
        .to_string();

    // Determine parent identifier by extracting parent from encrypted path
    let parent_item_identifier = if let Some(last_slash) = encrypted_path.rfind('/') {
        let encrypted_parent_path = &encrypted_path[..last_slash];

        if encrypted_parent_path.is_empty() {
            // Root level item
            "NSFileProviderRootContainerItemIdentifier".to_string()
        } else {
            // Look up parent inode_id using encrypted parent path
            match db::fileprovider::get_inode_id_by_path(
                state.db_pool.get(),
                encrypted_parent_path,
                user_id,
            ) {
                Ok(parent_inode_id) => format!("item:{}", parent_inode_id),
                Err(_) => {
                    // Fallback to root if parent lookup fails
                    tracing::warn!(
                        "Failed to find parent inode for encrypted path: {}",
                        encrypted_parent_path
                    );
                    "NSFileProviderRootContainerItemIdentifier".to_string()
                }
            }
        }
    } else {
        // No slash found, must be root level
        "NSFileProviderRootContainerItemIdentifier".to_string()
    };

    Ok(Json(FileProviderItem {
        identifier: query.identifier,
        filename,
        parent_item_identifier,
        item_type,
        file_size: file_size.map(|size| size.to_string()), // Convert Option<u64> to Option<String> for API
        creation_date: Some((*creation_date).to_rfc3339()), // Always present from inode.id UUIDv7
        content_modification_date: content_modification_date.map(|date| (*date).to_rfc3339()), // Optional, from data_id UUIDv7
        modification_height, // Pass through the consensus height
    }))
}

/// Create item endpoint for FileProvider
/// Forwards to post_files which handles both path and parent_item_identifier approaches
pub async fn create_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    multipart: axum::extract::Multipart,
) -> impl IntoResponse {
    // Forward to post_files implementation which now handles parent_item_identifier + filename
    match super::files::post_files(
        State(state),
        axum::extract::Extension(user_id),
        multipart,
    )
    .await
    {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(status_code) => status_code.into_response(),
    }
}

/// Helper function to get parent path from a full path
/// Modify item endpoint for FileProvider
/// Handles rename and move operations via multipart form data
pub async fn modify_item(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    mut multipart: axum::extract::Multipart,
) -> Result<Json<ModifyItemResponse>, StatusCode> {
    tracing::info!("modify_item: Request received, starting multipart processing");

    let mut identifier: Option<String> = None;
    let mut new_filename: Option<String> = None;
    let mut new_parent_item_identifier: Option<String> = None;
    let mut content_result: Option<(
        CustomUUID,
        Option<hopnet_storage::store::BlobInsertOp>,
        Option<Vec<crate::envelopes::IncomingShareUpdate>>,
    )> = None;

    // Parse multipart fields
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        let name = field.name().ok_or(StatusCode::BAD_REQUEST)?.to_string();
        tracing::debug!("modify_item: Processing multipart field: '{}'", name);

        match name.as_str() {
            "identifier" => {
                let value = field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?;
                tracing::debug!("modify_item: Got identifier: {}", value);
                identifier = Some(value);
            }
            "filename" => {
                let value = field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?;
                tracing::debug!("modify_item: Got filename: {}", value);
                new_filename = Some(value);
            }
            "parent_item_identifier" => {
                let value = field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?;
                tracing::debug!("modify_item: Got parent_item_identifier: {}", value);
                new_parent_item_identifier = Some(value);
            }
            field_name if field_name.starts_with("file_") => {
                tracing::debug!("modify_item: Processing file field: {}", field_name);

                // Validate identifier before expensive processing
                let current_identifier = identifier.as_ref().ok_or(StatusCode::BAD_REQUEST)?;
                if !current_identifier.starts_with("item:") {
                    return Err(StatusCode::BAD_REQUEST);
                }

                let inode_id_str = &current_identifier[5..];
                let inode_id =
                    CustomUUID::from_str(inode_id_str).map_err(|_| StatusCode::BAD_REQUEST)?;

                let (_, item_type, _, _, _, _) = db::fileprovider::get_item_metadata_by_inode_id(
                    state.db_pool.get(),
                    inode_id.clone(),
                    user_id,
                )
                .map_err(|_| StatusCode::NOT_FOUND)?;

                if item_type != hopnet_common::InodeType::File {
                    return Err(StatusCode::BAD_REQUEST);
                }

                let file_size_str = field_name
                    .strip_prefix("file_")
                    .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
                let file_size = file_size_str
                    .parse::<usize>()
                    .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;

                let result = super::files::prepare_content_update(
                    &state, user_id, &inode_id, field, file_size,
                )
                .await?;

                content_result = Some(result);
                break;
            }
            _ => {
                // Ignore unknown fields
            }
        }
    }

    // Validate required identifier and get metadata in one call
    let identifier = identifier.ok_or(StatusCode::BAD_REQUEST)?;

    tracing::debug!(
        "Processing modify_item request for identifier: {}",
        identifier
    );

    // Parse unified item: identifier to get inode_id (validation already done in multipart processing)
    let inode_id = if let Some(inode_id_str) = identifier.strip_prefix("item:") {
        // Skip "item:" prefix
        let parsed_inode_id =
            CustomUUID::from_str(inode_id_str).map_err(|_| StatusCode::BAD_REQUEST)?;

        tracing::debug!(
            "Parsed inode_id: {} for identifier: {}",
            parsed_inode_id,
            identifier
        );
        parsed_inode_id
    } else {
        tracing::error!("Invalid identifier format (not item:): {}", identifier);
        return Err(StatusCode::BAD_REQUEST);
    };

    // Build new encrypted path if changes requested using per-segment AES-SIV encryption
    let new_encrypted_path = if new_filename.is_some() || new_parent_item_identifier.is_some() {
        let session = session_or_status(&state, user_id).await?;
        let siv_key = &session.siv_key;
        let siv_nonce = &session.siv_nonce;

        // Get current item metadata once (if needed for parent or filename extraction)
        let current_item_metadata =
            if new_parent_item_identifier.is_none() || new_filename.is_none() {
                tracing::debug!(
                    "Fetching current item metadata for inode_id: {} user_id: {}",
                    inode_id,
                    user_id
                );
                match db::fileprovider::get_item_metadata_by_inode_id(
                    state.db_pool.get(),
                    inode_id.clone(),
                    user_id,
                ) {
                    Ok(metadata) => {
                        tracing::debug!("Successfully fetched metadata for inode_id: {}", inode_id);
                        Some(metadata)
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to fetch metadata for inode_id: {} user_id: {} error: {:?}",
                            inode_id,
                            user_id,
                            e
                        );
                        return Err(StatusCode::NOT_FOUND);
                    }
                }
            } else {
                None
            };

        // Get encrypted parent path
        let parent_encrypted_path = if let Some(parent_id) = new_parent_item_identifier {
            tracing::debug!(
                "modify_item: Using new parent_item_identifier: '{}'",
                parent_id
            );
            if parent_id == "NSFileProviderRootContainerItemIdentifier" {
                tracing::debug!("modify_item: Parent is root container");
                "".to_string() // Root has no encrypted path segments
            } else if let Some(parent_inode_id_str) = parent_id.strip_prefix("item:") {
                // Get parent item metadata
                // Skip "item:" prefix
                let parent_inode_id = CustomUUID::from_str(parent_inode_id_str)
                    .map_err(|_| StatusCode::BAD_REQUEST)?;

                let (parent_encrypted_path, parent_item_type, _, _, _, _) =
                    db::fileprovider::get_item_metadata_by_inode_id(
                        state.db_pool.get(),
                        parent_inode_id,
                        user_id,
                    )
                    .map_err(|_| StatusCode::NOT_FOUND)?;

                // Ensure parent is actually a folder
                if parent_item_type != hopnet_common::InodeType::Folder {
                    return Err(StatusCode::BAD_REQUEST);
                }

                tracing::debug!(
                    "modify_item: Found parent encrypted path: '{}'",
                    parent_encrypted_path
                );
                parent_encrypted_path
            } else {
                return Err(StatusCode::BAD_REQUEST);
            }
        } else {
            // Extract parent path from current item's path
            let (current_encrypted_path, _, _, _, _, _) = current_item_metadata.as_ref().unwrap();
            tracing::debug!(
                "modify_item: Extracting parent from current path: '{}'",
                current_encrypted_path
            );

            // Find last slash to extract parent portion
            let parent_path = if let Some(last_slash_pos) = current_encrypted_path.rfind('/') {
                current_encrypted_path[..last_slash_pos].to_string()
            } else {
                "".to_string() // Item is in root
            };
            tracing::debug!("modify_item: Extracted parent path: '{}'", parent_path);
            parent_path
        };

        // Get encrypted filename segment
        let filename_encrypted_segment = if let Some(filename) = new_filename {
            // Encrypt new filename as a segment
            tracing::debug!("modify_item: Encrypting new filename: '{}'", filename);
            let encrypted = encrypt_part(&filename, siv_key, siv_nonce)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            tracing::debug!(
                "modify_item: New encrypted filename segment: '{}'",
                encrypted
            );
            encrypted
        } else {
            // Extract current filename segment from current item's path
            let (current_encrypted_path, _, _, _, _, _) = current_item_metadata.as_ref().unwrap();
            tracing::debug!(
                "modify_item: Extracting filename from current path: '{}'",
                current_encrypted_path
            );

            let extracted = if let Some(last_slash_pos) = current_encrypted_path.rfind('/') {
                current_encrypted_path[last_slash_pos + 1..].to_string() // Skip the slash
            } else {
                current_encrypted_path.clone() // Entire path is the filename segment
            };
            tracing::debug!("modify_item: Extracted filename segment: '{}'", extracted);
            extracted
        };

        // Concatenate parent path + filename segment with proper slash handling
        tracing::debug!(
            "modify_item: Path construction - parent_empty: {}, filename_starts_with_slash: {}",
            parent_encrypted_path.is_empty(),
            filename_encrypted_segment.starts_with('/')
        );
        let new_path = build_encrypted_path(&parent_encrypted_path, &filename_encrypted_segment);
        tracing::debug!(
            "modify_item: Constructed new_path='{}' from parent='{}' + filename='{}'",
            new_path,
            parent_encrypted_path,
            &filename_encrypted_segment
        );
        Some(new_path)
    } else {
        None
    };

    // Extract content processing results if provided
    let (content_update, incoming_share_updates) =
        if let Some((_dataid, blob_op, share_updates)) = content_result {
            (
                Some(crate::envelopes::DriveContentUpdate { blob_op }),
                share_updates,
            )
        } else {
            (None, None)
        };

    // Validate modification before consensus
    tracing::debug!(
        "Validating modify_item for inode_id: {} user_id: {} new_encrypted_path: {:?}",
        inode_id,
        user_id,
        new_encrypted_path
    );

    // Create validation transaction
    {
        let mut conn = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let db_tx = conn
            .transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        match db::files::modify_item(
            &db_tx,
            user_id,
            inode_id.clone(),
            new_encrypted_path.clone(),
            content_update.clone().map(|u| u.blob_op),
            None, // incoming_share_updates not needed for validation
            &state.fragments_dir,
        ) {
            Ok(_) => {
                // Validation passed, roll back transaction
                db_tx
                    .rollback()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            }
            Err(e) => {
                tracing::error!(
                    "modify_item validation failed for inode_id: {} user_id: {} error: {:?}",
                    inode_id,
                    user_id,
                    e
                );
                return Err(match e {
                    DatabaseError::NotFound => StatusCode::NOT_FOUND,
                    DatabaseError::ConflictError => StatusCode::CONFLICT,
                    DatabaseError::InvalidPayload => StatusCode::BAD_REQUEST, // Circular reference, etc.
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                });
            }
        }
    }

    tracing::debug!(
        "Submitting modify_item to consensus for inode_id: {} user_id: {}",
        inode_id,
        user_id
    );

    // Create consensus payload
    let payload = crate::envelopes::ModifyItemPayload {
        user_id,
        inode_id,
        new_encrypted_path: new_encrypted_path.clone(),
        content_update,
        incoming_share_updates,
    };

    // Serialize and submit to consensus
    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    state
        .txs
        .submit(TxSpec {
            function: "modify_item",
            payload: encoded_payload,
            signer: TxSigner::User(user_id),
        })
        .await
        .map_err(|e| {
            if matches!(e, crate::host::TxSubmitError::Signing) {
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            tracing::error!(
                "Failed to submit modification to consensus for user_id: {} error: {:?}",
                user_id,
                e
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // With unified item: identifiers, the identifier never changes (stable inode ID)
    let new_identifier = identifier;

    tracing::info!("Successfully submitted item modification to consensus");
    Ok(Json(ModifyItemResponse { new_identifier }))
}
