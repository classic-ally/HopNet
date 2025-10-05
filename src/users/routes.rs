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
    }, db::{users, PubKey}, types::User, AppState};

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
    password: String,
    pubkey: PubKey,
    xpubkey: crate::db::types::XPubKey
}

impl UserRequest {
    pub fn encode(&self) -> Result<Vec<u8>, StatusCode> {
        return bincode::serde::encode_to_vec(&self, config::standard()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

pub async fn post_users (
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,  // Extract user_id from JWT via auth middleware
    Json(payload): Json<UserRequest>
) -> impl IntoResponse {
    // Consensus block generation - hash password at input boundary
    let user = match User::new_with_password(
        0,
        payload.username,
        payload.password,
        payload.pubkey,
        payload.xpubkey,
    ) {
        Ok(user) => user,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response()
    };

    // Encode user with bincode::serde standard config
    match bincode::serde::encode_to_vec(&user, bincode::config::standard()) {
        Ok(encoded_user) => {
            let transaction = match crate::consensus::functions::create_signed_user_transaction(
                &app_state,
                "insert_user".to_string(),
                encoded_user,
                user_id,
            ) {
                Ok(tx) => tx,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let transactions = vec![transaction];

            // quorum middleware test
            match consensus_middleware(&app_state, transactions).await {
                Ok(()) => StatusCode::CREATED.into_response(),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
    
    
}
