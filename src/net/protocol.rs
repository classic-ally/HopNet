use serde::{Deserialize, Serialize};
use crate::files::rpc as files_rpc;
use crate::consensus::rpc as consensus_rpc;

/// Request envelope for all iroh communication
#[derive(Serialize, Deserialize, Debug)]
pub enum IrohRequest {
    /// Ping for connection health/warmup
    Ping { nonce: u64 },
    /// Fragment health check
    FragmentHealthCheck(files_rpc::FragmentHealthRequest),
    /// Fetch consensus data for a specific view (catch-up)
    ViewDataFetch(consensus_rpc::ViewDataRequest),
    /// Poll for current view number (sync detection)
    ViewPoll(consensus_rpc::ViewPollRequest),
}

/// Response envelope for all iroh communication
#[derive(Serialize, Deserialize, Debug)]
pub enum IrohResponse {
    /// Pong response to Ping
    Pong { nonce: u64 },
    /// Fragment health check result
    FragmentHealthCheckResponse(files_rpc::FragmentHealthResponse),
    /// View consensus data for a specific view
    ViewDataFetchResponse(consensus_rpc::ViewDataResponse),
    /// Current view number
    ViewPollResponse(consensus_rpc::ViewPollResponse),
    /// Error response
    Error { message: String },
}
