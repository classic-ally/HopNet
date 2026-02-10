use serde::{Deserialize, Serialize};
use crate::files::rpc;

/// Request envelope for all iroh communication
#[derive(Serialize, Deserialize, Debug)]
pub enum IrohRequest {
    /// Ping for connection health/warmup
    Ping { nonce: u64 },
    /// Fragment health check
    FragmentHealthCheck(rpc::FragmentHealthRequest),
}

/// Response envelope for all iroh communication
#[derive(Serialize, Deserialize, Debug)]
pub enum IrohResponse {
    /// Pong response to Ping
    Pong { nonce: u64 },
    /// Fragment health check result
    FragmentHealthResult(rpc::FragmentHealthResponse),
    /// Error response
    Error { message: String },
}
