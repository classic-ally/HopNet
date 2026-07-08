//! Fragment transfer RPC over the "storage" comms scope. This module owns
//! the scope's wire vocabulary ([`StorageNetRequest`]/[`StorageNetResponse`]);
//! the server side lives in `net::scopes::StorageScope`.
//!
//! INTERIM (RFC-017 Stage 2 moves this into hopnet-storage behind the
//! substrate's Transport seam) — the reshape here is deliberately minimal.

use crate::AppState;
use crate::types::Blake3Hash;
use hopnet_comms::{CommsError, IrohComms, PeerRef, ProtocolError, Rpc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Wire request for the "storage" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum StorageNetRequest {
    /// Fragment health check
    Health(FragmentHealthRequest),
    /// Fetch a fragment from a remote node
    Fetch(FragmentFetchRequest),
    /// Store a fragment on a remote node
    Store(FragmentStoreRequest),
}

/// Wire response for the "storage" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum StorageNetResponse {
    Health(FragmentHealthResponse),
    Fetch(FragmentFetchResponse),
    Store(FragmentStoreResponse),
    Error { message: String },
}

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
/// No auth needed — mesh connections are already authenticated by the peer
/// directory's before-registration hook.
pub fn handle_fragment_health_check(
    req: FragmentHealthRequest,
    fragments_dir: &str,
) -> FragmentHealthResponse {
    let healthy = hopnet_storage::serve::serve_fragment_health(fragments_dir, &req.fragment_hash);
    FragmentHealthResponse { healthy }
}

/// Stream I/O timeout for health checks. Connection establishment has its own
/// budget (10s in the transport); this covers only the request/response
/// exchange on an already-connected peer (~50 bytes out, ~10 bytes back).
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_millis(500);

/// Stream I/O timeout for fragment data transfers (fetch/store).
/// Fragments are ~4MB encrypted, so allow more time than health checks.
const FRAGMENT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);

/// Client-side: ask a remote node whether it has a healthy copy of a fragment.
pub async fn check_fragment_health(
    comms: &IrohComms,
    peer: &PeerRef,
    fragment_hash: Blake3Hash,
) -> Result<bool, CommsError> {
    let payload = crate::net::encode_payload(&StorageNetRequest::Health(FragmentHealthRequest {
        fragment_hash,
    }));
    let reply = comms
        .rpc(peer, "storage", payload, HEALTH_CHECK_TIMEOUT)
        .await?;

    match crate::net::decode_payload::<StorageNetResponse>(&reply)? {
        StorageNetResponse::Health(result) => Ok(result.healthy),
        StorageNetResponse::Error { message } => {
            Err(CommsError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(CommsError::Protocol(ProtocolError::MalformedResponse(
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

/// Client-side: fetch a fragment from a remote node over the mesh.
pub async fn fetch_fragment(
    comms: &IrohComms,
    peer: &PeerRef,
    fragment_hash: Blake3Hash,
) -> Result<Vec<u8>, CommsError> {
    let payload = crate::net::encode_payload(&StorageNetRequest::Fetch(FragmentFetchRequest {
        fragment_hash,
    }));
    let reply = comms
        .rpc(peer, "storage", payload, FRAGMENT_TRANSFER_TIMEOUT)
        .await?;

    match crate::net::decode_payload::<StorageNetResponse>(&reply)? {
        StorageNetResponse::Fetch(result) => {
            if result.found {
                result.data.ok_or_else(|| {
                    CommsError::Protocol(ProtocolError::MalformedResponse(
                        "fragment marked found but data is None".into(),
                    ))
                })
            } else {
                Err(CommsError::Protocol(ProtocolError::PeerError(
                    "fragment not found".into(),
                )))
            }
        }
        StorageNetResponse::Error { message } => {
            Err(CommsError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(CommsError::Protocol(ProtocolError::MalformedResponse(
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
) -> StorageNetResponse {
    use hopnet_storage::serve::StoreOutcome;
    let sink = crate::storage_host::substrate_host::SubstrateHost::new(app_state.clone());
    match hopnet_storage::serve::serve_fragment_store(
        &app_state.fragments_dir,
        &sink,
        &req.fragment_hash,
        req.data,
    ) {
        StoreOutcome::Stored => StorageNetResponse::Store(FragmentStoreResponse {
            success: true,
            already_existed: false,
        }),
        StoreOutcome::AlreadyExisted => StorageNetResponse::Store(FragmentStoreResponse {
            success: true,
            already_existed: true,
        }),
        StoreOutcome::TooLarge { got, max } => StorageNetResponse::Error {
            message: format!("fragment too large: {} bytes (max: {})", got, max),
        },
        StoreOutcome::HashMismatch { expected, actual } => StorageNetResponse::Error {
            message: format!("hash mismatch: expected {}, got {}", expected, actual),
        },
        StoreOutcome::Io(e) => StorageNetResponse::Error {
            message: format!("failed to store fragment: {:?}", e),
        },
    }
}

/// Client-side: store a fragment on a remote node over the mesh.
pub async fn store_fragment_remote(
    comms: &IrohComms,
    peer: &PeerRef,
    fragment_hash: Blake3Hash,
    data: Vec<u8>,
) -> Result<FragmentStoreResponse, CommsError> {
    let payload = crate::net::encode_payload(&StorageNetRequest::Store(FragmentStoreRequest {
        fragment_hash,
        data,
    }));
    let reply = comms
        .rpc(peer, "storage", payload, FRAGMENT_TRANSFER_TIMEOUT)
        .await?;

    match crate::net::decode_payload::<StorageNetResponse>(&reply)? {
        StorageNetResponse::Store(result) => Ok(result),
        StorageNetResponse::Error { message } => {
            Err(CommsError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(CommsError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to FragmentStore: {:?}", other),
        ))),
    }
}
