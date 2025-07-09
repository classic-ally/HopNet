use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
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
    match users::get_users(&app_state.db) {
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
    pubkey: PubKey
}

impl UserRequest {
    pub fn encode(&self) -> Result<Vec<u8>, StatusCode> {
        return bincode::serde::encode_to_vec(&self, config::standard()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

pub async fn post_users (
    State(app_state): State<AppState>,
    Json(payload): Json<UserRequest>
) -> impl IntoResponse {
    // Consensus block generation
    let user = User {
        user_id: 0,
        username: payload.username,
        password: payload.password,
        pubkey: payload.pubkey,
    };

    // Encode user with bincode::serde standard config
    match bincode::serde::encode_to_vec(&user, bincode::config::standard()) {
        Ok(encoded_user) => {
            let transaction = Transaction {
                function: "insert_user".to_string(),
                payload: encoded_user,
            };
            let transactions = vec![transaction];

            // quorum middleware test
            match consensus_middleware(&app_state, transactions).await {
                Ok(()) => StatusCode::CREATED,
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR
            }
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR
    }
    
    
}
