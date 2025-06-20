use axum::{
    extract::State, 
    response::IntoResponse,
    http::StatusCode,
    Json
};
use serde::{Serialize,Deserialize};


use crate::db::Sequence;
use crate::AppState;
use crate::{
    db,
    db::User,
    db::Node,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct ThisNode {
    pub node_id: i32
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InitialSetupObject {
    pub user: User,
    pub node: Node,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncSetupObject {
    pub users: Vec<User>,
    pub nodes: Vec<Node>,
    pub sequences: Vec<Sequence>,
    pub yournode: ThisNode,
}

pub async fn get_setup(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match db::get_initial_setup(&app_state.db) {
        Ok(setupstatus) => setupstatus,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR
    }
}

// join a network, it's put by another device
pub async fn put_setup(
    State(app_state): State<AppState>,
    Json(payload): Json<SyncSetupObject>
) -> impl IntoResponse {
    match db::put_join_setup(&app_state.db, payload) {
        Ok(()) => StatusCode::CREATED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR
    }
}

// full setup from scratch
pub async fn post_setup(
    State(app_state): State<AppState>,
    Json(payload): Json<InitialSetupObject>
) -> impl IntoResponse {
    match db::post_initial_setup(&app_state.db, payload) {
        Ok(()) => StatusCode::CREATED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR
    }
}