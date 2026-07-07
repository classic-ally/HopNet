//! The drive's HTTP surface (RFC-015 Stage D4): axum routers for files,
//! shares, and the FileProvider/DocumentProvider integrations, moved from
//! the host behind the `host` seams. Route paths, status codes, and
//! response shapes are preserved EXACTLY; the host mounts these routers and
//! layers its own auth middleware (JWT / device token) around them.

pub mod documentprovider;
pub mod files;
pub mod fileprovider;
pub mod shares;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use crate::host::{DriveState, WriteCheckError};

/// Per-user write gate. Returns 409 Conflict (empty body) on any request
/// hitting a route this layer is attached to while the host's write
/// admission denies the authenticated user (an active takeout import
/// today). Reads `user_id` from request extensions populated upstream by
/// the host's auth middleware. Reproduces the host's
/// `takeout::import_gate::import_gate` responses exactly: missing user →
/// 401, check failure → 500, denied → 409.
///
/// Attachment is explicit per route (or per write-only sub-router) — the
/// middleware itself does no method discrimination. Routes that should
/// bypass the gate simply don't have the layer applied.
pub(crate) async fn write_gate(
    State(state): State<DriveState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let user_id = req
        .extensions()
        .get::<i32>()
        .copied()
        .ok_or(StatusCode::UNAUTHORIZED)?;

    match state.write_admission.check_write(user_id).await {
        Ok(()) => Ok(next.run(req).await),
        Err(WriteCheckError::Denied(_)) => Err(StatusCode::CONFLICT),
        Err(WriteCheckError::Internal) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
