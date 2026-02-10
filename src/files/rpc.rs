use serde::{Deserialize, Serialize};
use std::time::Duration;
use crate::types::Blake3Hash;
use crate::net::{IrohError, IrohTransport};
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::net::transport::ProtocolError;

// ============================================================================
// Fragment Health Check
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct FragmentHealthRequest {
    pub fragment_hash: Blake3Hash,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FragmentHealthResponse {
    pub healthy: bool,
}

/// Server-side: handle a fragment health check from a peer node.
/// No auth needed — iroh connections are already authenticated by the PeerValidator hook.
pub fn handle_fragment_health_check(req: FragmentHealthRequest, fragments_dir: &str) -> FragmentHealthResponse {
    let healthy = crate::files::functions::fragment_exists_and_valid(fragments_dir, &req.fragment_hash);
    FragmentHealthResponse { healthy }
}

/// Stream I/O timeout for health checks. Connection establishment has its own
/// budget (10s in transport); this covers only the request/response exchange
/// on an already-connected peer (~50 bytes out, ~10 bytes back).
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_millis(500);

/// Client-side: ask a remote node whether it has a healthy copy of a fragment.
pub async fn check_fragment_health(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    fragment_hash: Blake3Hash,
) -> Result<bool, IrohError> {
    let req = IrohRequest::FragmentHealthCheck(FragmentHealthRequest { fragment_hash });
    let response = transport.request(node_id, peer_node_id, &req, HEALTH_CHECK_TIMEOUT).await?;

    match response {
        IrohResponse::FragmentHealthResult(result) => Ok(result.healthy),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => {
            Err(IrohError::Protocol(ProtocolError::MalformedResponse(
                format!("unexpected response to FragmentHealthCheck: {:?}", other),
            )))
        }
    }
}
