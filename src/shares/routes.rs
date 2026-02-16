use axum::{
    extract::{State, Extension, Path, Json},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, delete},
    Router,
};
use crate::AppState;
use crate::db::CustomUUID;
use crate::db::DatabaseError;
use crate::db::types::FileAccess;
use crate::consensus::types::Transaction;
use super::types::*;

/// Run the transaction handler with execute=false against current DB state.
/// Returns the appropriate HTTP error response if validation fails, None if it passes.
fn preflight_check(app_state: &AppState, transaction: &Transaction) -> Option<axum::response::Response> {
    let mut conn = app_state.db_pool.get().ok()?;
    let db_tx = conn.transaction().ok()?;
    if let Err(e) = crate::consensus::functions::process_transaction(transaction, app_state, false, &db_tx) {
        Some(database_error_to_status(e).into_response())
    } else {
        None
    }
}

fn database_error_to_status(e: DatabaseError) -> StatusCode {
    match e {
        DatabaseError::ConflictError => StatusCode::CONFLICT,
        DatabaseError::NotFound => StatusCode::NOT_FOUND,
        DatabaseError::AuthorizationError => StatusCode::FORBIDDEN,
        DatabaseError::InvalidPayload => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(post_share))
        .route("/incoming", get(get_incoming_shares))
        .route("/incoming/count", get(get_incoming_share_count))
        .route("/incoming/{id}", delete(delete_incoming_share))
        .route("/{id}/accept", post(post_accept_share))
        .route("/file/{inode_id}", get(get_share_details).delete(delete_unshare))
}

/// POST /shares — share a file with another user
pub async fn post_share(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(payload): Json<ShareRequest>,
) -> impl IntoResponse {
    let inode_id = match CustomUUID::from_str(&payload.inode_id) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    // Get sender's session
    let session = match app_state.get_session(user_id).await {
        Ok(s) => s,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    // Look up recipient by username
    let recipient = match crate::db::users::get_user_by_username(app_state.db_pool.get(), payload.recipient_username.clone()) {
        Ok(Some(u)) => u,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Self-share check
    if recipient.user_id == user_id {
        return StatusCode::BAD_REQUEST.into_response();
    }

    // Get a single connection for the remaining lookups
    let conn = match app_state.db_pool.get() {
        Ok(c) => c,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Look up sender's inode → data_block_id + encrypted_path
    let inode_info = match crate::db::files::get_inode_by_id(&conn, &inode_id, user_id) {
        Ok(Some(info)) => info,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let (data_block_id, encrypted_path) = match inode_info {
        (Some(data_id), path, hopnet_common::InodeType::File) => (data_id, path),
        _ => return StatusCode::BAD_REQUEST.into_response(), // folder or empty file
    };

    // Get sender's FileAccess → decrypt per-file key
    let file_access_entry = match crate::db::files::get_file_access(&conn, &data_block_id, user_id) {
        Ok(Some(fa)) => fa,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let x25519_privkey = crate::auth::derive_x25519_privkey_from_user(&session.user_keys.private_key);
    let per_file_key = match crate::auth::decrypt_wrapped_file_key(&file_access_entry, &x25519_privkey) {
        Ok(key) => key,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Create FileAccess for recipient
    let recipient_file_access = match FileAccess::new_for_user(
        app_state.db_pool.get(), data_block_id.clone(), recipient.user_id, &per_file_key,
    ) {
        Ok(fa) => fa,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Extract filename: decrypt last path segment
    let last_segment = encrypted_path.rsplit('/').next().unwrap_or("");
    let filename = match crate::files::functions::decrypt_part(last_segment, &session.siv_key, &session.siv_nonce) {
        Ok(name) => name,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Encrypt display name for recipient
    let (display_ephemeral_pubkey, encrypted_display_name) = match encrypt_display_name(&filename, &recipient.x25519_pubkey) {
        Ok(result) => result,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Serialize FileAccess as bincode blob
    let file_access_blob = match bincode::serde::encode_to_vec(&recipient_file_access, bincode::config::standard()) {
        Ok(blob) => blob,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Build consensus payload
    let share_payload = ShareFilePayload {
        id: CustomUUID::new(None),
        data_block_id,
        sender_id: user_id,
        recipient_id: recipient.user_id,
        file_access: file_access_blob,
        display_ephemeral_pubkey,
        encrypted_display_name,
    };

    let encoded = match bincode::serde::encode_to_vec(&share_payload, bincode::config::standard()) {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state, "share_file".to_string(), encoded, user_id,
    ).await {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    if let Some(err_response) = preflight_check(&app_state, &transaction) {
        return err_response;
    }

    match crate::consensus::functions::consensus_middleware(&app_state, vec![transaction]).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /shares/incoming — list pending incoming shares
pub async fn get_incoming_shares(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> impl IntoResponse {
    let session = match app_state.get_session(user_id).await {
        Ok(s) => s,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let x25519_privkey = crate::auth::derive_x25519_privkey_from_user(&session.user_keys.private_key);

    let shares = match crate::db::shares::get_incoming_shares_for_user(app_state.db_pool.get(), user_id) {
        Ok(s) => s,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let mut response = Vec::new();
    for (share, sender_username) in shares {
        let display_name = match decrypt_display_name(
            &share.display_ephemeral_pubkey,
            &share.encrypted_display_name,
            &x25519_privkey,
        ) {
            Ok(name) => name,
            Err(_) => continue,
        };

        let created_at = share.id.extract_timestamp()
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();

        response.push(IncomingShareResponse {
            id: share.id.to_string(),
            sender_username,
            display_name,
            created_at,
        });
    }

    (StatusCode::OK, Json(response)).into_response()
}

/// GET /shares/incoming/count — badge count for pending shares
pub async fn get_incoming_share_count(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> impl IntoResponse {
    match crate::db::shares::get_incoming_share_count(app_state.db_pool.get(), user_id) {
        Ok(count) => (StatusCode::OK, Json(ShareCountResponse { count })).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// POST /shares/{id}/accept — accept a pending share and place in filesystem
pub async fn post_accept_share(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Path(share_id): Path<String>,
    Json(payload): Json<AcceptShareRequest>,
) -> impl IntoResponse {
    let share_uuid = match CustomUUID::from_str(&share_id) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let session = match app_state.get_session(user_id).await {
        Ok(s) => s,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    // Encrypt placement path with recipient's SIV key
    let encrypted_path = match crate::files::functions::encrypt_path(
        payload.placement_path, &session.siv_key, &session.siv_nonce,
    ).await {
        Ok(p) => p,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let accept_payload = AcceptSharePayload {
        incoming_share_id: share_uuid,
        recipient_id: user_id,
        encrypted_path,
        inode_id: CustomUUID::new(None),
    };

    let encoded = match bincode::serde::encode_to_vec(&accept_payload, bincode::config::standard()) {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state, "accept_share".to_string(), encoded, user_id,
    ).await {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    if let Some(err_response) = preflight_check(&app_state, &transaction) {
        return err_response;
    }

    match crate::consensus::functions::consensus_middleware(&app_state, vec![transaction]).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// DELETE /shares/incoming/{id} — decline or cancel a pending share
pub async fn delete_incoming_share(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Path(share_id): Path<String>,
) -> impl IntoResponse {
    let share_uuid = match CustomUUID::from_str(&share_id) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let decline_payload = DeclineSharePayload {
        incoming_share_id: share_uuid,
        user_id,
    };

    let encoded = match bincode::serde::encode_to_vec(&decline_payload, bincode::config::standard()) {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state, "decline_share".to_string(), encoded, user_id,
    ).await {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    if let Some(err_response) = preflight_check(&app_state, &transaction) {
        return err_response;
    }

    match crate::consensus::functions::consensus_middleware(&app_state, vec![transaction]).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /shares/file/{inode_id} — sharing details for a file
pub async fn get_share_details(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Path(inode_id): Path<String>,
) -> impl IntoResponse {
    let inode_uuid = match CustomUUID::from_str(&inode_id) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let conn = match app_state.db_pool.get() {
        Ok(c) => c,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Look up inode to get data_block_id (verify caller owns it)
    let inode_info = match crate::db::files::get_inode_by_id(&conn, &inode_uuid, user_id) {
        Ok(Some(info)) => info,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let data_block_id = match inode_info.0 {
        Some(id) => id,
        None => return (StatusCode::OK, Json(ShareDetailResponse { users: vec![] })).into_response(),
    };

    let members = match crate::db::shares::get_share_details(app_state.db_pool.get(), &data_block_id) {
        Ok(m) => m,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let users: Vec<ShareParticipant> = members.into_iter().map(|m| ShareParticipant {
        username: m.username,
        user_id: m.user_id,
        status: m.status,
    }).collect();

    (StatusCode::OK, Json(ShareDetailResponse { users })).into_response()
}

/// DELETE /shares/file/{inode_id} — unshare (remove self from a shared file, keep current version)
pub async fn delete_unshare(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Path(inode_id): Path<String>,
) -> impl IntoResponse {
    let inode_uuid = match CustomUUID::from_str(&inode_id) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let unshare_payload = UnsharePayload {
        inode_id: inode_uuid,
        user_id,
    };

    let encoded = match bincode::serde::encode_to_vec(&unshare_payload, bincode::config::standard()) {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state, "unshare".to_string(), encoded, user_id,
    ).await {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    if let Some(err_response) = preflight_check(&app_state, &transaction) {
        return err_response;
    }

    match crate::consensus::functions::consensus_middleware(&app_state, vec![transaction]).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
