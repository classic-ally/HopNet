use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use bincode::{encode_to_vec, Encode, config};
use serde::Deserialize;

use crate::{
    consensus::{
        types::Transaction,
        routes::consensus_middleware
    },
    types::User,
    db::users, 
AppState};

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


#[derive(Deserialize, Encode)]
pub struct UserRequest {
    username: String,
    password: String,
}

impl UserRequest {
    pub fn encode(&self) -> Result<Vec<u8>, StatusCode> {
        return encode_to_vec(&self, config::standard()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

pub async fn post_users (
    State(app_state): State<AppState>,
    Json(payload): Json<UserRequest>
) -> impl IntoResponse {
    // Consensus block generation
    match payload.encode() {
        Ok(encoded_payload) => {
            let transaction = Transaction {
                function: "post_users".to_string(),
                payload: encoded_payload,
            };
            let transactions = vec![transaction];

            // quorum middleware test
            match consensus_middleware(&app_state, transactions).await {
                Ok(()) => {},
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR
            }

            let user = User {
                user_id: 0,
                username: payload.username,
                password: payload.password,
            };

            match users::insert_user(&app_state.db, user) {
                Ok(()) => StatusCode::CREATED,
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR
    }
}