use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Multipart, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
};
use base64::Engine;
use serde::{Deserialize, Serialize};

use super::helpers::submit_onboarding_update;
use super::types::UpdateUserProfilePayload;
use crate::{
    AppState,
    db::{PrivKey, PubKey, users},
    types::User,
};
use hopnet_common::{OnboardingFlag, OnboardingFlags, PublicUserInfo, SelfUserInfo};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(get_users).post(post_users))
        .route("/me", get(get_me))
        .route("/me/profile", put(put_profile))
        .route("/me/avatar", put(put_avatar))
        .route("/me/onboarding", put(put_onboarding))
        .layer(DefaultBodyLimit::max(16_000_000)) // 16MB — matches frontend avatar size check
}

fn user_to_public(u: &User) -> PublicUserInfo {
    PublicUserInfo {
        user_id: u.user_id,
        username: u.username.clone(),
        first_name: u.first_name.clone(),
        last_name: u.last_name.clone(),
        avatar: u
            .avatar
            .as_ref()
            .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

fn user_to_self(u: &User) -> SelfUserInfo {
    SelfUserInfo {
        user_id: u.user_id,
        username: u.username.clone(),
        first_name: u.first_name.clone(),
        last_name: u.last_name.clone(),
        avatar: u
            .avatar
            .as_ref()
            .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
        onboarding_flags: u.onboarding_flags.raw(),
    }
}

pub async fn get_users(State(app_state): State<AppState>) -> impl IntoResponse {
    match users::get_users(app_state.db_pool.get()) {
        Ok(users) => {
            let public: Vec<PublicUserInfo> = users.iter().map(user_to_public).collect();
            (StatusCode::OK, Json(public)).into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /users/me — current user's self-profile (includes onboarding flags)
pub async fn get_me(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> impl IntoResponse {
    match users::get_user_by_userid(app_state.db_pool.get(), user_id) {
        Ok(Some(user)) => (StatusCode::OK, Json(user_to_self(&user))).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Serialize, Deserialize)]
pub struct ProfileUpdateRequest {
    pub first_name: Option<Option<String>>,
    pub last_name: Option<Option<String>>,
}

/// PUT /users/me/profile — update display name fields
pub async fn put_profile(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(payload): Json<ProfileUpdateRequest>,
) -> impl IntoResponse {
    let update = UpdateUserProfilePayload {
        user_id,
        first_name: payload.first_name,
        last_name: payload.last_name,
        avatar: None,
    };

    let encoded = match bincode::serde::encode_to_vec(&update, bincode::config::standard()) {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state,
        "update_user_profile".to_string(),
        encoded,
        user_id,
    )
    .await
    {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match app_state.consensus_queue.submit(transaction).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// PUT /users/me/avatar — upload and resize avatar image
pub async fn put_avatar(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    // Read image bytes from multipart field
    let image_bytes = match multipart.next_field().await {
        Ok(Some(field)) => match field.bytes().await {
            Ok(bytes) => bytes,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };

    // Reject if > 15MB input
    if image_bytes.len() > 15_000_000 {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }

    tracing::debug!("Avatar upload: input size {} bytes", image_bytes.len());

    // Decode, resize to 256x256, encode to JPEG — blocking work
    // (image crate only supports lossless WebP which is too large for avatars)
    let avatar_bytes = match tokio::task::spawn_blocking(move || -> Result<Vec<u8>, String> {
        let img =
            image::load_from_memory(&image_bytes).map_err(|e| format!("Invalid image: {}", e))?;

        tracing::debug!("Avatar decoded: {}x{}", img.width(), img.height());

        let resized = img.resize_to_fill(256, 256, image::imageops::FilterType::Lanczos3);
        // JPEG doesn't support alpha — convert RGBA→RGB
        let rgb = resized.to_rgb8();

        let mut buf = std::io::Cursor::new(Vec::new());
        let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buf, 85);
        encoder
            .encode(
                &rgb,
                rgb.width(),
                rgb.height(),
                image::ExtendedColorType::Rgb8,
            )
            .map_err(|e| format!("JPEG encoding failed: {}", e))?;

        let result = buf.into_inner();
        tracing::debug!("Avatar JPEG output: {} bytes", result.len());
        Ok(result)
    })
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => {
            tracing::warn!("Avatar processing failed: {}", e);
            return StatusCode::BAD_REQUEST.into_response();
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let update = UpdateUserProfilePayload {
        user_id,
        first_name: None,
        last_name: None,
        avatar: Some(Some(avatar_bytes)),
    };

    let encoded = match bincode::serde::encode_to_vec(&update, bincode::config::standard()) {
        Ok(e) => e,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let transaction = match crate::consensus::functions::create_signed_user_transaction(
        &app_state,
        "update_user_profile".to_string(),
        encoded,
        user_id,
    )
    .await
    {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match app_state.consensus_queue.submit(transaction).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserRequest {
    username: String,
}

pub async fn post_users(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(payload): Json<UserRequest>,
) -> impl IntoResponse {
    // Server generates all key material
    let (user_priv_key, user_pub_key) = crate::consensus::functions::generate_ed25519_key();
    let privkey = PrivKey(user_priv_key);
    let pubkey = PubKey(user_pub_key);
    let x25519_pubkey = crate::auth::derive_x25519_pubkey_from_user(&privkey);

    // Generate passphrase server-side
    let passphrase = crate::passphrase::generate_passphrase();

    // Wrap the user private key with the passphrase (3-5s Argon2id)
    let passphrase_clone = passphrase.clone();
    let privkey_clone = privkey.clone();
    let wrap_result = tokio::task::spawn_blocking(move || {
        crate::auth::wrap_user_privkey(&privkey_clone, &passphrase_clone).map_err(|e| e.to_string())
    })
    .await;

    let (encrypted_privkey, key_salt) = match wrap_result {
        Ok(Ok(result)) => result,
        _ => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let user = User::new(
        0,
        payload.username,
        pubkey,
        x25519_pubkey,
        encrypted_privkey,
        key_salt,
    );

    // Encode user with bincode::serde standard config
    match bincode::serde::encode_to_vec(&user, bincode::config::standard()) {
        Ok(encoded_user) => {
            let transaction = match crate::consensus::functions::create_signed_user_transaction(
                &app_state,
                "insert_user".to_string(),
                encoded_user,
                user_id,
            )
            .await
            {
                Ok(tx) => tx,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let transactions = vec![transaction];

            let results = app_state.consensus_queue.submit_batch(transactions).await;
            if results.iter().any(|r| r.is_err()) {
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            } else {
                (
                    StatusCode::CREATED,
                    Json(hopnet_common::setup::PassphraseResponse { passphrase }),
                )
                    .into_response()
            }
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Body for `PUT /users/me/onboarding`. Typed flag enum prevents stringly-
/// typed boundary; serde deserialization rejects unknown variants for free.
#[derive(Serialize, Deserialize)]
pub struct OnboardingUpdateRequest {
    pub set: Vec<OnboardingFlag>,
    pub clear: Vec<OnboardingFlag>,
}

/// PUT /users/me/onboarding — flip onboarding bitfield. Replicated via consensus.
pub async fn put_onboarding(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Json(req): Json<OnboardingUpdateRequest>,
) -> impl IntoResponse {
    let set_flags: OnboardingFlags = req.set.into_iter().collect();
    let clear_flags: OnboardingFlags = req.clear.into_iter().collect();

    match submit_onboarding_update(&app_state, user_id, set_flags, clear_flags).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::error!("put_onboarding submit failed for user {}: {}", user_id, e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
