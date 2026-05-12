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

/// Server-side: handle a fragment health check from a peer node.
/// No auth needed — iroh connections are already authenticated by the PeerValidator hook.
pub fn handle_fragment_health_check(
    req: FragmentHealthRequest,
    fragments_dir: &str,
) -> FragmentHealthResponse {
    let healthy =
        crate::files::functions::fragment_exists_and_valid(fragments_dir, &req.fragment_hash);
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

/// Server-side: handle a fragment fetch request from a peer node.
pub fn handle_fragment_fetch(
    req: FragmentFetchRequest,
    fragments_dir: &str,
) -> FragmentFetchResponse {
    match crate::files::functions::fetch_and_verify_fragment(&req.fragment_hash, fragments_dir) {
        Ok(data) => FragmentFetchResponse {
            found: true,
            data: Some(data),
        },
        Err(_) => FragmentFetchResponse {
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

/// Server-side: handle a fragment store request from a peer node.
pub async fn handle_fragment_store(
    req: FragmentStoreRequest,
    app_state: &AppState,
) -> IrohResponse {
    let max_encrypted_size = crate::files::functions::calculate_encrypted_chunk_length(
        crate::files::functions::MAX_FRAGMENT_SIZE,
    );

    // Verify fragment size
    if req.data.len() > max_encrypted_size {
        return IrohResponse::Error {
            message: format!(
                "fragment too large: {} bytes (max: {})",
                req.data.len(),
                max_encrypted_size
            ),
        };
    }

    // Verify hash matches
    let actual_hash = Blake3Hash::new(blake3::hash(&req.data));
    if actual_hash != req.fragment_hash {
        return IrohResponse::Error {
            message: format!(
                "hash mismatch: expected {}, got {}",
                req.fragment_hash.to_hex(),
                actual_hash.to_hex()
            ),
        };
    }

    // Check if already exists
    if crate::files::functions::fragment_exists_and_valid(
        &app_state.fragments_dir,
        &req.fragment_hash,
    ) {
        return IrohResponse::FragmentStoreResponse(FragmentStoreResponse {
            success: true,
            already_existed: true,
        });
    }

    // Store to disk
    if let Err(e) = crate::files::functions::store_fragment(
        &app_state.fragments_dir,
        &req.fragment_hash,
        req.data,
    ) {
        return IrohResponse::Error {
            message: format!("failed to store fragment: {:?}", e),
        };
    }

    // Queue async DB update (drain task will batch-flush through write gate)
    if let Err(e) =
        app_state
            .local_state_tx
            .try_send(crate::db::write_gate::LocalStateUpdate::MarkLocal {
                fragment_hash: req.fragment_hash,
            })
    {
        tracing::warn!(
            "Local state queue full, dropping mark-local for {}: {}",
            req.fragment_hash.to_hex(),
            e
        );
    }

    IrohResponse::FragmentStoreResponse(FragmentStoreResponse {
        success: true,
        already_existed: false,
    })
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
