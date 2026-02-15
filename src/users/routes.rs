use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Extension,
    Json,
};
use bincode::config;
use serde::{Serialize,Deserialize};

use crate::{
    consensus::{
        functions::consensus_middleware, types::Transaction
    }, db::{users, PubKey, PrivKey}, types::User, AppState};

pub async fn get_users(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match users::get_users(app_state.db_pool.get()) {
        Ok(users) => {
            (StatusCode::OK, Json(users))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Vec::<User>::new())
        ),
    }
}


#[derive(Serialize, Deserialize)]
pub struct UserRequest {
    username: String,
}

pub async fn post_users (
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,  // Extract user_id from JWT via auth middleware
    Json(payload): Json<UserRequest>
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
        crate::auth::wrap_user_privkey(&privkey_clone, &passphrase_clone)
            .map_err(|e| e.to_string())
    }).await;

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
            ).await {
                Ok(tx) => tx,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let transactions = vec![transaction];

            match consensus_middleware(&app_state, transactions).await {
                Ok(()) => (StatusCode::CREATED, Json(hopnet_common::setup::PassphraseResponse { passphrase })).into_response(),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}
