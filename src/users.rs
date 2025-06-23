use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::{AppState, db};

pub async fn get_users(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match db::get_users(&app_state.db) {
        Ok(users) => {
            (StatusCode::OK, Json(users))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Vec::<db::User>::new())
        ),
    }
}


#[derive(Deserialize)]
pub struct UserRequest {
    username: String,
    password: String,
}

pub async fn post_users (
    State(app_state): State<AppState>,
    Json(payload): Json<UserRequest>
) -> impl IntoResponse {
    let user = db::User {
        user_id: 0,
        username: payload.username,
        password: payload.password,
    };

    match db::insert_user(&app_state.db, user) {
        Ok(()) => StatusCode::CREATED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}