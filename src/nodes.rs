use axum::{
    Extension,
    extract::State, 
    response::IntoResponse,
    http::StatusCode,
    Json
};

use crate::{
    db,
    db::Node
};
use crate::AppState;

pub async fn get_nodes(
    State(app_state): State<AppState>
) -> impl IntoResponse {
    match db::get_nodes(&app_state.db) {
        Ok(nodes) => return (StatusCode::OK, Json(nodes)),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::<db::Node>::new())),
    }
}


// route to add a new node
pub async fn post_nodes(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
    Json(payload): Json<Node>,
) -> impl IntoResponse {
    // check if uid matches requester
    if uid != payload.owner {
        return StatusCode::FORBIDDEN
    }

    let node = Node {
        node_id: 0,
        name: payload.name,
        ip_address: payload.ip_address,
        port: payload.port,
        owner: payload.owner,
    };

    // can we see the other server + is it set up already?
    

    match db::insert_node(&app_state.db, node) {
        Ok(()) => StatusCode::CREATED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR
    }
}
