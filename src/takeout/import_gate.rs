//! Per-user import gate. Returns 409 Conflict on any request hitting a route
//! this layer is attached to while the authenticated user has an active
//! import (`status IN (Pending, Importing)`). Reads `user_id` from request
//! extensions populated upstream by `auth_middleware` or
//! `device_token_auth_middleware`.
//!
//! Attachment is explicit per route (or per write-only sub-router) — the
//! middleware itself does no method discrimination. Routes that should bypass
//! the gate simply don't have the layer applied.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use crate::db::imports;
use crate::AppState;

pub async fn import_gate(
    State(app_state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let user_id = req
        .extensions()
        .get::<i32>()
        .copied()
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let conn = app_state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let active = imports::has_active_import(&conn, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if active {
        return Err(StatusCode::CONFLICT);
    }
    Ok(next.run(req).await)
}
