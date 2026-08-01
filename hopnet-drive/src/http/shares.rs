//! /shares routes — sharing surface (create/accept/decline/unshare + reads).
//! Moved from the host's `shares::routes` (RFC-015 Stage D4), including the
//! display-name crypto it used locally (the host re-exports it at the old
//! path for its unit tests).

use axum::{
    Router,
    extract::{Extension, Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use chacha20poly1305::{
    ChaCha20Poly1305,
    aead::{Aead, KeyInit, OsRng},
};
use std::str::FromStr;
use x25519_dalek::PublicKey as X25519PublicKey;

use crate::db;
use crate::envelopes::{
    AcceptSharePayload, DeclineSharePayload, ShareFilePayload, UnsharePayload,
};
use crate::host::{DriveState, TxSigner, TxSpec, TxSubmitError};
use crate::paths::{decrypt_part, encrypt_path};
use hopnet_common::CustomUUID;
use hopnet_common::shares::{
    AcceptShareRequest, IncomingShareResponse, ShareCountResponse, ShareDetailResponse,
    ShareParticipant, ShareRequest,
};

pub fn router<S: Clone + Send + Sync + 'static>(state: DriveState) -> Router<S> {
    let reads = Router::new()
        .route("/incoming", get(get_incoming_shares))
        .route("/incoming/count", get(get_incoming_share_count))
        .route("/file/{inode_id}", get(get_share_details));

    let writes = Router::new()
        .route("/", post(post_share))
        .route("/incoming/{id}", delete(delete_incoming_share))
        .route("/{id}/accept", post(post_accept_share))
        .route("/file/{inode_id}", delete(delete_unshare))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            super::write_gate,
        ));

    reads.merge(writes).with_state(state)
}

// --- Display name crypto ---

/// Encrypt a display name for a specific recipient using ECDH + Blake3 KDF + ChaCha20-Poly1305.
/// Returns (ephemeral_pubkey_bytes, ciphertext).
pub fn encrypt_display_name(
    plaintext: &str,
    recipient_x25519_pubkey: &X25519PublicKey,
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    let ephemeral_secret = x25519_dalek::EphemeralSecret::random_from_rng(OsRng);
    let ephemeral_public = X25519PublicKey::from(&ephemeral_secret);

    let shared_secret = ephemeral_secret.diffie_hellman(recipient_x25519_pubkey);

    // Derive wrapping key
    let mut wrap_key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet display_name_wrap");
    hasher.update(shared_secret.as_bytes());
    hasher.finalize_xof().fill(&mut wrap_key_bytes);
    let key = chacha20poly1305::Key::from(wrap_key_bytes);

    // Derive nonce from ephemeral pubkey
    let mut nonce_bytes = [0u8; 12];
    let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet display_name_nonce");
    nonce_hasher.update(ephemeral_public.as_bytes());
    nonce_hasher.finalize_xof().fill(&mut nonce_bytes);
    let nonce = chacha20poly1305::Nonce::from(nonce_bytes);

    let cipher = ChaCha20Poly1305::new(&key);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|e| format!("Display name encryption failed: {:?}", e))?;

    Ok((ephemeral_public.as_bytes().to_vec(), ciphertext))
}

/// Decrypt a display name using the recipient's X25519 private key.
pub fn decrypt_display_name(
    ephemeral_pubkey_bytes: &[u8],
    ciphertext: &[u8],
    recipient_x25519_privkey: &x25519_dalek::StaticSecret,
) -> Result<String, Box<dyn std::error::Error>> {
    if ephemeral_pubkey_bytes.len() != 32 {
        return Err("Invalid ephemeral pubkey length".into());
    }

    let mut pubkey_arr = [0u8; 32];
    pubkey_arr.copy_from_slice(ephemeral_pubkey_bytes);
    let ephemeral_pubkey = X25519PublicKey::from(pubkey_arr);

    let shared_secret = recipient_x25519_privkey.diffie_hellman(&ephemeral_pubkey);

    let mut wrap_key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet display_name_wrap");
    hasher.update(shared_secret.as_bytes());
    hasher.finalize_xof().fill(&mut wrap_key_bytes);
    let key = chacha20poly1305::Key::from(wrap_key_bytes);

    let mut nonce_bytes = [0u8; 12];
    let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet display_name_nonce");
    nonce_hasher.update(ephemeral_pubkey.as_bytes());
    nonce_hasher.finalize_xof().fill(&mut nonce_bytes);
    let nonce = chacha20poly1305::Nonce::from(nonce_bytes);

    let cipher = ChaCha20Poly1305::new(&key);
    let plaintext = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|e| format!("Display name decryption failed: {:?}", e))?;

    String::from_utf8(plaintext).map_err(|e| e.into())
}

/// POST /shares — share a file with another user
pub async fn post_share(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Json(payload): Json<ShareRequest>,
) -> impl IntoResponse {
    let inode_id = match CustomUUID::from_str(&payload.inode_id) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    // Get sender's session
    let session = match state.sessions.user_session(user_id).await {
        Ok(s) => s,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    // Get a single connection for the remaining lookups
    let conn = match state.db_pool.get() {
        Ok(c) => c,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Look up recipient by username
    let recipient =
        match db::users::get_recipient_by_username(&conn, &payload.recipient_username) {
            Ok(Some(u)) => u,
            Ok(None) => return StatusCode::NOT_FOUND.into_response(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

    // Self-share check
    if recipient.user_id == user_id {
        return StatusCode::BAD_REQUEST.into_response();
    }

    // Look up sender's inode → data_block_id + encrypted_path
    let inode_info = match db::files::get_inode_by_id(&conn, &inode_id, user_id) {
        Ok(Some(info)) => info,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let (data_block_id, encrypted_path) = match inode_info {
        (Some(data_id), path, hopnet_common::InodeType::File) => (data_id, path),
        _ => return StatusCode::BAD_REQUEST.into_response(), // folder or empty file
    };

    // Get sender's FileAccess → decrypt per-file key
    let file_access_entry = match db::files::get_file_access(&conn, &data_block_id, user_id) {
        Ok(Some(fa)) => fa,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let per_file_key = match hopnet_storage::crypto::unwrap_blob_key(
        &file_access_entry,
        &hopnet_storage::crypto::StaticRecipient(session.x25519_privkey.clone()),
    ) {
        Ok(key) => key,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Create FileAccess for recipient
    let recipient_file_access = match db::users::blob_access_for_user(
        state.db_pool.get(),
        data_block_id.clone(),
        recipient.user_id,
        &per_file_key,
    ) {
        Ok(fa) => fa,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Extract filename: decrypt last path segment
    let last_segment = encrypted_path.rsplit('/').next().unwrap_or("");
    let filename = match decrypt_part(last_segment, &session.siv_key, &session.siv_nonce) {
        Ok(name) => name,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Encrypt display name for recipient
    let (display_ephemeral_pubkey, encrypted_display_name) =
        match encrypt_display_name(&filename, &recipient.x25519_pubkey) {
            Ok(result) => result,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

    // Serialize FileAccess as bincode blob
    let file_access_blob =
        match bincode::serde::encode_to_vec(&recipient_file_access, bincode::config::standard()) {
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

    match state
        .txs
        .submit(TxSpec {
            function: "share_file",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(TxSubmitError::Rejected(r)) => {
            tracing::warn!("Share rejected: {}", r);
            StatusCode::CONFLICT.into_response()
        }
        Err(TxSubmitError::Unavailable(_)) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /shares/incoming — list pending incoming shares
pub async fn get_incoming_shares(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
) -> impl IntoResponse {
    let session = match state.sessions.user_session(user_id).await {
        Ok(s) => s,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let shares = match db::shares::get_incoming_shares_for_user(state.db_pool.get(), user_id) {
        Ok(s) => s,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let mut response = Vec::new();
    for (share, sender_username) in shares {
        let display_name = match decrypt_display_name(
            &share.display_ephemeral_pubkey,
            &share.encrypted_display_name,
            &session.x25519_privkey,
        ) {
            Ok(name) => name,
            Err(_) => continue,
        };

        let created_at = share
            .id
            .extract_timestamp()
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
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
) -> impl IntoResponse {
    match db::shares::get_incoming_share_count(state.db_pool.get(), user_id) {
        Ok(count) => (StatusCode::OK, Json(ShareCountResponse { count })).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// POST /shares/{id}/accept — accept a pending share and place in filesystem
pub async fn post_accept_share(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Path(share_id): Path<String>,
    Json(payload): Json<AcceptShareRequest>,
) -> impl IntoResponse {
    let share_uuid = match CustomUUID::from_str(&share_id) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let session = match state.sessions.user_session(user_id).await {
        Ok(s) => s,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    // Encrypt placement path with recipient's SIV key
    let encrypted_path = match encrypt_path(
        payload.placement_path,
        &session.siv_key,
        &session.siv_nonce,
    )
    .await
    {
        Ok(p) => p,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let parent_folder_inodes: Vec<(CustomUUID, String)> = {
        let conn = match state.db_pool.get() {
            Ok(c) => c,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let tx = match conn.unchecked_transaction() {
            Ok(t) => t,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let missing = match db::files::find_missing_parents(&tx, &[encrypted_path.as_str()]) {
            Ok(m) => m,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        missing
            .into_iter()
            .map(|p| (CustomUUID::new(None), p))
            .collect()
    };

    let accept_payload = AcceptSharePayload {
        incoming_share_id: share_uuid,
        recipient_id: user_id,
        encrypted_path,
        inode_id: CustomUUID::new(None),
        parent_folder_inodes,
    };

    let encoded = match bincode::serde::encode_to_vec(&accept_payload, bincode::config::standard())
    {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match state
        .txs
        .submit(TxSpec {
            function: "accept_share",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(TxSubmitError::Rejected(r)) => {
            tracing::warn!("Accept share rejected: {}", r);
            StatusCode::CONFLICT.into_response()
        }
        Err(TxSubmitError::Unavailable(_)) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// DELETE /shares/incoming/{id} — decline or cancel a pending share
pub async fn delete_incoming_share(
    State(state): State<DriveState>,
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

    let encoded = match bincode::serde::encode_to_vec(&decline_payload, bincode::config::standard())
    {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match state
        .txs
        .submit(TxSpec {
            function: "decline_share",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(TxSubmitError::Rejected(r)) => {
            tracing::warn!("Decline share rejected: {}", r);
            StatusCode::CONFLICT.into_response()
        }
        Err(TxSubmitError::Unavailable(_)) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /shares/file/{inode_id} — sharing details for a file
pub async fn get_share_details(
    State(state): State<DriveState>,
    Extension(user_id): Extension<i32>,
    Path(inode_id): Path<String>,
) -> impl IntoResponse {
    let inode_uuid = match CustomUUID::from_str(&inode_id) {
        Ok(id) => id,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let conn = match state.db_pool.get() {
        Ok(c) => c,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Look up inode to get data_block_id (verify caller owns it)
    let inode_info = match db::files::get_inode_by_id(&conn, &inode_uuid, user_id) {
        Ok(Some(info)) => info,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let data_block_id = match inode_info.0 {
        Some(id) => id,
        None => {
            return (StatusCode::OK, Json(ShareDetailResponse { users: vec![] })).into_response();
        }
    };

    let members = match db::shares::get_share_details(state.db_pool.get(), &data_block_id) {
        Ok(m) => m,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let users: Vec<ShareParticipant> = members
        .into_iter()
        .map(|m| ShareParticipant {
            username: m.username,
            user_id: m.user_id,
            status: m.status,
        })
        .collect();

    (StatusCode::OK, Json(ShareDetailResponse { users })).into_response()
}

/// DELETE /shares/file/{inode_id} — unshare (remove self from a shared file, keep current version)
pub async fn delete_unshare(
    State(state): State<DriveState>,
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

    let encoded = match bincode::serde::encode_to_vec(&unshare_payload, bincode::config::standard())
    {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match state
        .txs
        .submit(TxSpec {
            function: "unshare",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(TxSubmitError::Rejected(r)) => {
            tracing::warn!("Unshare rejected: {}", r);
            StatusCode::CONFLICT.into_response()
        }
        Err(TxSubmitError::Unavailable(_)) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
