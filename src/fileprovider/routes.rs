use axum::{
    Extension, Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use rand::RngExt;
use serde::Deserialize;
use std::str::FromStr;

use super::types::{EnumerateResponse, FileProviderItem, HealthResponse, HealthStatus};
use crate::AppState;
use crate::db::{self, DatabaseError};
use crate::files::functions::{build_encrypted_path, encrypt_path};
use hopnet_common::fileprovider::TestResponse;
use hopnet_common::fileprovider::{
    ChangesQuery, ChangesResponse, DeleteItemRequest, DownloadQuery, ItemQuery, ModifyItemResponse,
};

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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<EnumerateQuery>,
) -> Result<Json<EnumerateResponse>, StatusCode> {
    let session = app_state.get_session(user_id).await?;
    let siv_key = &session.siv_key;
    let siv_nonce = &session.siv_nonce;

    // Handle both parent_path and parent_item_identifier approaches
    let (parent_path, encrypted_parent_path) = if let Some(path) = query.parent_path {
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
            let inode_id = crate::db::CustomUUID::from_str(inode_id_str)
                .map_err(|_| StatusCode::BAD_REQUEST)?;

            match crate::db::fileprovider::get_item_metadata_by_inode_id(
                app_state.db_pool.get(),
                inode_id,
                user_id,
            ) {
                Ok((encrypted_path, _, _, _, _, _)) => {
                    // Decrypt for response construction, but use encrypted for database query
                    let decrypted = crate::files::functions::decrypt_path(
                        encrypted_path.clone(),
                        siv_key,
                        siv_nonce,
                    )
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
        app_state.db_pool.get(),
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

/// Health check endpoint for FileProvider extension
/// Returns ready if database setup is completed, not_ready otherwise
pub async fn get_health(State(app_state): State<AppState>) -> impl IntoResponse {
    // Check if database setup is completed using the same pattern as /setup
    match db::setup::get_initial_setup(app_state.db_pool.get()) {
        Ok(StatusCode::OK) => {
            // Database is initialized, FileProvider can operate
            Json(HealthResponse {
                status: HealthStatus::Ready,
            })
        }
        Ok(StatusCode::NOT_FOUND) => {
            // Database is not initialized, FileProvider cannot operate
            Json(HealthResponse {
                status: HealthStatus::NotReady,
            })
        }
        Ok(_) | Err(_) => {
            // Database error, FileProvider cannot operate
            Json(HealthResponse {
                status: HealthStatus::NotReady,
            })
        }
    }
}

/// Test endpoint for FileProvider testing - only available in test mode
/// Registers a device token via consensus and returns it for test configuration
pub async fn get_test(State(app_state): State<AppState>) -> Result<Json<TestResponse>, StatusCode> {
    if !app_state.test_mode {
        return Err(StatusCode::NOT_FOUND);
    }

    let user_id = app_state.get_user_id()?;
    let session = app_state.get_session(user_id).await?;

    // Generate device token (same pattern as post_register_device)
    let device_id = crate::db::CustomUUID::new(None);
    let secret: Vec<u8> = (0..32).map(|_| rand::rng().random::<u8>()).collect();
    let secret_hex = hex::encode(&secret);
    let api_key_hash = crate::db::Blake3Hash::new(blake3::hash(secret_hex.as_bytes()));

    let encrypted_device_name = crate::files::functions::encrypt_part(
        "test-fileprovider",
        &session.siv_key,
        &session.siv_nonce,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let wrapped_user_key =
        crate::auth::wrap_user_key_for_device(&secret, &session.user_keys.private_key)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Submit through consensus
    let payload = crate::devices::types::RegisterDevicePayload {
        id: device_id.clone(),
        user_id,
        api_key_hash,
        encrypted_device_name,
        wrapped_user_key,
    };

    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transaction = crate::consensus::functions::create_signed_user_transaction(
        &app_state,
        "register_device".to_string(),
        encoded_payload,
        user_id,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    app_state
        .consensus_queue
        .submit(transaction)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let api_key = format!("{}.{}", device_id, secret_hex);
    let backend_url = format!("http://localhost:{}", app_state.port);

    Ok(Json(TestResponse {
        api_key,
        backend_url,
    }))
}

/// Test endpoint to get FileProvider signal count - only available in test mode
pub async fn get_test_signals(State(app_state): State<AppState>) -> Result<String, StatusCode> {
    if !app_state.test_mode {
        return Err(StatusCode::NOT_FOUND);
    }

    let signal_count = crate::fileprovider::domain::get_signal_count();
    Ok(signal_count.to_string())
}

/// Changes endpoint for FileProvider incremental sync
/// Returns all folders + files changed since the given consensus height
pub async fn get_changes(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ChangesQuery>,
) -> Result<Json<ChangesResponse>, StatusCode> {
    let session = app_state.get_session(user_id).await?;
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
        app_state.db_pool.get(),
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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(request): Json<DeleteItemRequest>,
) -> impl IntoResponse {
    // Parse the unified item: identifier
    let (encrypted_path, is_folder): (String, bool) = if request.identifier.starts_with("item:") {
        // Extract inode_id from unified identifier
        let inode_id_str = &request.identifier[5..]; // Skip "item:" prefix
        let inode_id = match crate::db::CustomUUID::from_str(inode_id_str) {
            Ok(uuid) => uuid,
            Err(_) => return StatusCode::BAD_REQUEST,
        };

        // Get metadata for the item
        match db::fileprovider::get_item_metadata_by_inode_id(
            app_state.db_pool.get(),
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
        match db::fileprovider::is_folder_empty(app_state.db_pool.get(), &encrypted_path, user_id) {
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
        let mut conn = match app_state.db_pool.get() {
            Ok(c) => c,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        };
        let db_tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
        };

        match crate::db::files::delete_files(&db_tx, encrypted_path.clone(), user_id) {
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
    let payload = crate::files::handlers::DeleteFilesPayload {
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

    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state,
        "delete_files".to_string(),
        encoded_payload,
        user_id,
    )
    .await
    {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Submit to consensus queue
    match app_state.consensus_queue.submit(transaction).await {
        Ok(()) => {
            tracing::info!(
                "Successfully submitted FileProvider deletion to consensus for user {}",
                user_id
            );
            StatusCode::OK
        }
        Err(e) => {
            tracing::error!("Failed to submit deletion to consensus: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Download endpoint for FileProvider
/// Downloads file content by identifier and returns streaming response
pub async fn download_file(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<DownloadQuery>,
) -> impl IntoResponse {
    // Only handle item: identifiers for downloads
    if !query.identifier.starts_with("item:") {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }

    let session = match app_state.get_session(user_id).await {
        Ok(s) => s,
        Err(_) => return axum::http::StatusCode::PRECONDITION_REQUIRED.into_response(),
    };
    let siv_key = &session.siv_key;
    let siv_nonce = &session.siv_nonce;

    // Extract inode_id from unified identifier
    let inode_id_str = &query.identifier[5..]; // Skip "item:" prefix
    let inode_id = match crate::db::CustomUUID::from_str(inode_id_str) {
        Ok(uuid) => uuid,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };

    // Get metadata and ensure it's a file
    let (encrypted_path, item_type, file_size, _, _, _) =
        match db::fileprovider::get_item_metadata_by_inode_id(
            app_state.db_pool.get(),
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
    let decrypted_path =
        match crate::files::functions::decrypt_path(encrypted_path, siv_key, siv_nonce) {
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
    crate::files::routes::get_file_fragments(
        State(app_state),
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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<ItemQuery>,
) -> Result<Json<FileProviderItem>, StatusCode> {
    let session = app_state.get_session(user_id).await?;
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
    let inode_id = match crate::db::CustomUUID::from_str(inode_id_str) {
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
    ) = match db::fileprovider::get_item_metadata_by_inode_id(
        app_state.db_pool.get(),
        inode_id,
        user_id,
    ) {
        Ok(metadata) => metadata,
        Err(DatabaseError::NotFound) => return Err(StatusCode::NOT_FOUND),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Decrypt path to get filename and parent
    let decrypted_path =
        match crate::files::functions::decrypt_path(encrypted_path.clone(), siv_key, siv_nonce) {
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
                app_state.db_pool.get(),
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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    multipart: axum::extract::Multipart,
) -> impl IntoResponse {
    // Forward to post_files implementation which now handles parent_item_identifier + filename
    match crate::files::routes::post_files(
        State(app_state),
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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    mut multipart: axum::extract::Multipart,
) -> Result<Json<ModifyItemResponse>, StatusCode> {
    tracing::info!("modify_item: Request received, starting multipart processing");

    let mut identifier: Option<String> = None;
    let mut new_filename: Option<String> = None;
    let mut new_parent_item_identifier: Option<String> = None;
    let mut content_result: Option<(
        crate::db::CustomUUID,
        crate::db::DataRecord,
        Option<Vec<crate::shares::types::IncomingShareUpdate>>,
        chacha20poly1305::Key,
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
                let inode_id = crate::db::CustomUUID::from_str(inode_id_str)
                    .map_err(|_| StatusCode::BAD_REQUEST)?;

                let (_, item_type, _, _, _, _) = db::fileprovider::get_item_metadata_by_inode_id(
                    app_state.db_pool.get(),
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

                let result = crate::files::functions::prepare_content_update(
                    &app_state, user_id, &inode_id, field, file_size,
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
            crate::db::CustomUUID::from_str(inode_id_str).map_err(|_| StatusCode::BAD_REQUEST)?;

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
        let session = app_state.get_session(user_id).await?;
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
                    app_state.db_pool.get(),
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
                let parent_inode_id = crate::db::CustomUUID::from_str(parent_inode_id_str)
                    .map_err(|_| StatusCode::BAD_REQUEST)?;

                let (parent_encrypted_path, parent_item_type, _, _, _, _) =
                    db::fileprovider::get_item_metadata_by_inode_id(
                        app_state.db_pool.get(),
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
            let encrypted = crate::files::functions::encrypt_part(&filename, siv_key, siv_nonce)
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
    let (new_data_block_id, new_data_record, incoming_share_updates, _content_per_file_key) =
        if let Some((dataid, data_record, share_updates, per_file_key)) = content_result {
            (
                Some(dataid),
                Some(data_record),
                share_updates,
                Some(per_file_key),
            )
        } else {
            (None, None, None, None)
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
        let mut conn = app_state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let db_tx = conn
            .transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        match crate::db::files::modify_item(
            &db_tx,
            user_id,
            inode_id.clone(),
            new_encrypted_path.clone(),
            new_data_block_id.clone(),
            new_data_record.clone(),
            None, // incoming_share_updates not needed for validation
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
                    crate::db::DatabaseError::NotFound => StatusCode::NOT_FOUND,
                    crate::db::DatabaseError::ConflictError => StatusCode::CONFLICT,
                    crate::db::DatabaseError::InvalidPayload => StatusCode::BAD_REQUEST, // Circular reference, etc.
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
    let payload = crate::files::handlers::ModifyItemPayload {
        user_id,
        inode_id,
        new_encrypted_path: new_encrypted_path.clone(),
        new_data_block_id,
        new_data_record,
        incoming_share_updates,
    };

    // Serialize and submit to consensus
    let encoded_payload = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state,
        "modify_item".to_string(),
        encoded_payload,
        user_id,
    )
    .await
    {
        Ok(tx) => tx,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    app_state
        .consensus_queue
        .submit(transaction)
        .await
        .map_err(|e| {
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

fn get_parent_path(path: &str) -> String {
    if path == "/" {
        return "/".to_string();
    }

    let path = path.trim_end_matches('/');
    if let Some(last_slash) = path.rfind('/') {
        if last_slash == 0 {
            "/".to_string()
        } else {
            path[..last_slash].to_string()
        }
    } else {
        "/".to_string()
    }
}
