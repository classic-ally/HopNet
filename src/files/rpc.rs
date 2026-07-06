use crate::AppState;
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::net::transport::ProtocolError;
use crate::net::{IrohError, IrohTransport};
use crate::types::Blake3Hash;
use serde::{Deserialize, Serialize};
use std::time::Duration;

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

/// Server-side: handle a fragment health check from a peer node — delegates
/// to the substrate's serve half.
/// No auth needed — iroh connections are already authenticated by the PeerValidator hook.
pub fn handle_fragment_health_check(
    req: FragmentHealthRequest,
    fragments_dir: &str,
) -> FragmentHealthResponse {
    let healthy = hopnet_storage::serve::serve_fragment_health(fragments_dir, &req.fragment_hash);
    FragmentHealthResponse { healthy }
}

/// Stream I/O timeout for health checks. Connection establishment has its own
/// budget (10s in transport); this covers only the request/response exchange
/// on an already-connected peer (~50 bytes out, ~10 bytes back).
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_millis(500);

/// Stream I/O timeout for fragment data transfers (fetch/store).
/// Fragments are ~4MB encrypted, so allow more time than health checks.
const FRAGMENT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);

/// Client-side: ask a remote node whether it has a healthy copy of a fragment.
pub async fn check_fragment_health(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    fragment_hash: Blake3Hash,
) -> Result<bool, IrohError> {
    let req = IrohRequest::FragmentHealthCheck(FragmentHealthRequest { fragment_hash });
    let response = transport
        .request(node_id, peer_node_id, &req, HEALTH_CHECK_TIMEOUT)
        .await?;

    match response {
        IrohResponse::FragmentHealthCheckResponse(result) => Ok(result.healthy),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to FragmentHealthCheck: {:?}", other),
        ))),
    }
}

// ============================================================================
// Fragment Fetch
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct FragmentFetchRequest {
    pub fragment_hash: Blake3Hash,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FragmentFetchResponse {
    pub found: bool,
    pub data: Option<Vec<u8>>,
}

/// Server-side: handle a fragment fetch request from a peer node — delegates
/// to the substrate's serve half.
pub fn handle_fragment_fetch(
    req: FragmentFetchRequest,
    fragments_dir: &str,
) -> FragmentFetchResponse {
    match hopnet_storage::serve::serve_fragment_fetch(fragments_dir, &req.fragment_hash) {
        Some(data) => FragmentFetchResponse {
            found: true,
            data: Some(data),
        },
        None => FragmentFetchResponse {
            found: false,
            data: None,
        },
    }
}

/// Client-side: fetch a fragment from a remote node over iroh.
pub async fn fetch_fragment(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    fragment_hash: Blake3Hash,
) -> Result<Vec<u8>, IrohError> {
    let req = IrohRequest::FragmentFetch(FragmentFetchRequest { fragment_hash });
    let response = transport
        .request(node_id, peer_node_id, &req, FRAGMENT_TRANSFER_TIMEOUT)
        .await?;

    match response {
        IrohResponse::FragmentFetchResponse(result) => {
            if result.found {
                result.data.ok_or_else(|| {
                    IrohError::Protocol(ProtocolError::MalformedResponse(
                        "fragment marked found but data is None".into(),
                    ))
                })
            } else {
                Err(IrohError::Protocol(ProtocolError::PeerError(
                    "fragment not found".into(),
                )))
            }
        }
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to FragmentFetch: {:?}", other),
        ))),
    }
}

// ============================================================================
// Fragment Store
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct FragmentStoreRequest {
    pub fragment_hash: Blake3Hash,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FragmentStoreResponse {
    pub success: bool,
    pub already_existed: bool,
}

/// Server-side: handle a fragment store request from a peer node — the
/// substrate's serve half enforces size cap + content addressing, persists,
/// and queues stored_locally settlement via the LocalStateSink seam; this
/// shell only maps the outcome onto wire responses.
pub async fn handle_fragment_store(
    req: FragmentStoreRequest,
    app_state: &AppState,
) -> IrohResponse {
    use hopnet_storage::serve::StoreOutcome;
    let sink = crate::files::substrate_host::SubstrateHost::new(app_state.clone());
    match hopnet_storage::serve::serve_fragment_store(
        &app_state.fragments_dir,
        &sink,
        &req.fragment_hash,
        req.data,
    ) {
        StoreOutcome::Stored => IrohResponse::FragmentStoreResponse(FragmentStoreResponse {
            success: true,
            already_existed: false,
        }),
        StoreOutcome::AlreadyExisted => {
            IrohResponse::FragmentStoreResponse(FragmentStoreResponse {
                success: true,
                already_existed: true,
            })
        }
        StoreOutcome::TooLarge { got, max } => IrohResponse::Error {
            message: format!("fragment too large: {} bytes (max: {})", got, max),
        },
        StoreOutcome::HashMismatch { expected, actual } => IrohResponse::Error {
            message: format!("hash mismatch: expected {}, got {}", expected, actual),
        },
        StoreOutcome::Io(e) => IrohResponse::Error {
            message: format!("failed to store fragment: {:?}", e),
        },
    }
}

/// Client-side: store a fragment on a remote node over iroh.
pub async fn store_fragment_remote(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    fragment_hash: Blake3Hash,
    data: Vec<u8>,
) -> Result<FragmentStoreResponse, IrohError> {
    let req = IrohRequest::FragmentStore(FragmentStoreRequest {
        fragment_hash,
        data,
    });
    let response = transport
        .request(node_id, peer_node_id, &req, FRAGMENT_TRANSFER_TIMEOUT)
        .await?;

    match response {
        IrohResponse::FragmentStoreResponse(result) => Ok(result),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to FragmentStore: {:?}", other),
        ))),
    }
}
