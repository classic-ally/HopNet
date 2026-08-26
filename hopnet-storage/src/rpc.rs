//! The substrate's fragment data-plane protocol over the "storage" comms
//! scope (RFC-017 Stage 2). The crate owns the wire vocabulary, the server
//! dispatch, and the client half — the host contributes only a
//! [`hopnet_comms::Rpc`] implementation and the scope registration. Zero
//! iroh in this crate; zero fragment-protocol knowledge in the host.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::serve::{self, StoreOutcome};
use crate::traits::{LocalStateSink, PeerRef, StoreResult, Transport, TransportError};
use crate::types::Blake3Hash;

/// The comms scope this protocol rides on.
pub const SCOPE: &str = "storage";

/// Stream I/O timeout for health checks. Connection establishment has its own
/// budget (10s in the transport); this covers only the request/response
/// exchange on an already-connected peer (~50 bytes out, ~10 bytes back).
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_millis(500);

/// Stream I/O timeout for fragment data transfers (fetch/store).
/// Fragments are ~4MB encrypted, so allow more time than health checks.
const FRAGMENT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);

/// Wire request for the "storage" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum FragmentRequest {
    /// Does the peer hold a healthy (hash-verified) copy?
    Health { fragment_hash: Blake3Hash },
    /// Fetch a fragment's bytes.
    Fetch { fragment_hash: Blake3Hash },
    /// Store a fragment (content-addressed; the server verifies).
    Store {
        fragment_hash: Blake3Hash,
        data: Vec<u8>,
    },
}

/// Wire response for the "storage" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum FragmentResponse {
    Health {
        healthy: bool,
    },
    Fetch {
        found: bool,
        data: Option<Vec<u8>>,
    },
    Store {
        success: bool,
        already_existed: bool,
    },
    /// Substrate-level rejection (size cap, hash mismatch, disk IO).
    Error {
        message: String,
    },
}

fn encode<T: Serialize>(msg: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(msg, bincode::config::standard())
        .expect("fragment wire types always encode")
}

/// Server dispatch: one request against the local fragstore. Subsumes the
/// per-arm serve shells and the [`StoreOutcome`] → wire mapping the host
/// used to own. No auth needed — mesh connections are already authenticated
/// by the peer directory's before-registration hook.
pub fn serve<L: LocalStateSink + ?Sized>(
    fragments_dir: &str,
    sink: &L,
    req: FragmentRequest,
) -> FragmentResponse {
    match req {
        FragmentRequest::Health { fragment_hash } => FragmentResponse::Health {
            healthy: serve::serve_fragment_health(fragments_dir, &fragment_hash),
        },
        FragmentRequest::Fetch { fragment_hash } => {
            match serve::serve_fragment_fetch(fragments_dir, &fragment_hash) {
                Some(data) => FragmentResponse::Fetch {
                    found: true,
                    data: Some(data),
                },
                None => FragmentResponse::Fetch {
                    found: false,
                    data: None,
                },
            }
        }
        FragmentRequest::Store {
            fragment_hash,
            data,
        } => match serve::serve_fragment_store(fragments_dir, sink, &fragment_hash, data) {
            StoreOutcome::Stored => FragmentResponse::Store {
                success: true,
                already_existed: false,
            },
            StoreOutcome::AlreadyExisted => FragmentResponse::Store {
                success: true,
                already_existed: true,
            },
            StoreOutcome::TooLarge { got, max } => FragmentResponse::Error {
                message: format!("fragment too large: {} bytes (max: {})", got, max),
            },
            StoreOutcome::HashMismatch { expected, actual } => FragmentResponse::Error {
                message: format!("hash mismatch: expected {}, got {}", expected, actual),
            },
            StoreOutcome::Io(e) => FragmentResponse::Error {
                message: format!("failed to store fragment: {:?}", e),
            },
        },
    }
}

/// The engine's [`Transport`] seam implemented over ANY comms [`Rpc`] —
/// encode, decode, timeout selection, and error classification all live
/// here, crate-owned. Peer-reported errors ([`FragmentResponse::Error`] and
/// comms' `PeerError`) classify as [`TransportError::Peer`] so the engine's
/// domain retry covers them; everything else is transport-class.
pub struct RpcTransport<R> {
    pub rpc: R,
}

// RFC-025 S4 note: a CommsError::Refused currently falls into the
// catch-all below and reads as a retryable transport fault (the engine
// rotates candidates, which is behaviorally fine). This crate deps the
// zero-dependency comms face and stays host-agnostic, so the future
// shape is a Refused classification variant here with the host-side
// defuser at the substrate boundary; until then the status prober is
// the named-diagnosis backstop.
fn classify(e: hopnet_comms::CommsError) -> TransportError {
    match e {
        hopnet_comms::CommsError::Protocol(hopnet_comms::ProtocolError::PeerError(msg)) => {
            TransportError::Peer(msg)
        }
        other => TransportError::Transport(other.to_string()),
    }
}

fn decode_response(reply: &[u8]) -> Result<FragmentResponse, TransportError> {
    bincode::serde::decode_from_slice(reply, bincode::config::standard())
        .map(|(msg, _)| msg)
        .map_err(|e| TransportError::Transport(format!("malformed fragment response: {e}")))
}

impl<R: hopnet_comms::Rpc> Transport for RpcTransport<R> {
    async fn store_fragment(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
        data: Vec<u8>,
    ) -> Result<StoreResult, TransportError> {
        let payload = encode(&FragmentRequest::Store {
            fragment_hash: *fragment_hash,
            data,
        });
        let reply = self
            .rpc
            .rpc(peer, SCOPE, payload, FRAGMENT_TRANSFER_TIMEOUT)
            .await
            .map_err(classify)?;
        match decode_response(&reply)? {
            FragmentResponse::Store {
                success,
                already_existed,
            } => {
                if success {
                    Ok(StoreResult { already_existed })
                } else {
                    // success=false shouldn't happen (errors come via
                    // FragmentResponse::Error) — classify as peer-side so the
                    // engine's domain retry covers it.
                    Err(TransportError::Peer(
                        "fragment store returned success=false".to_string(),
                    ))
                }
            }
            FragmentResponse::Error { message } => Err(TransportError::Peer(message)),
            other => Err(TransportError::Transport(format!(
                "unexpected response to Store: {other:?}"
            ))),
        }
    }

    async fn fetch_fragment(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
    ) -> Result<Vec<u8>, TransportError> {
        let payload = encode(&FragmentRequest::Fetch {
            fragment_hash: *fragment_hash,
        });
        let reply = self
            .rpc
            .rpc(peer, SCOPE, payload, FRAGMENT_TRANSFER_TIMEOUT)
            .await
            .map_err(classify)?;
        match decode_response(&reply)? {
            FragmentResponse::Fetch { found, data } => {
                if found {
                    data.ok_or_else(|| {
                        TransportError::Transport("fragment marked found but data is None".into())
                    })
                } else {
                    Err(TransportError::Peer("fragment not found".into()))
                }
            }
            FragmentResponse::Error { message } => Err(TransportError::Peer(message)),
            other => Err(TransportError::Transport(format!(
                "unexpected response to Fetch: {other:?}"
            ))),
        }
    }

    async fn fragment_health(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
    ) -> Result<bool, TransportError> {
        let payload = encode(&FragmentRequest::Health {
            fragment_hash: *fragment_hash,
        });
        let reply = self
            .rpc
            .rpc(peer, SCOPE, payload, HEALTH_CHECK_TIMEOUT)
            .await
            .map_err(classify)?;
        match decode_response(&reply)? {
            FragmentResponse::Health { healthy } => Ok(healthy),
            FragmentResponse::Error { message } => Err(TransportError::Peer(message)),
            other => Err(TransportError::Transport(format!(
                "unexpected response to Health: {other:?}"
            ))),
        }
    }
}
