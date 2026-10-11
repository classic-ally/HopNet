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
        // As bytes, not a sequence of u8: one allocation of the exact size
        // and a memcpy, where the sequence form grows a Vec element by
        // element. The bincode encoding is identical either way.
        #[serde(with = "serde_bytes")]
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
        // As bytes (see FragmentRequest::Store).
        #[serde(with = "serde_bytes")]
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
pub async fn serve<L: LocalStateSink + ?Sized>(
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
        } => match serve::serve_fragment_store(fragments_dir, sink, &fragment_hash, data).await {
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
            StoreOutcome::NoSpace => FragmentResponse::Error {
                message: "no space: below the pull floor".to_string(),
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
        // Transfer timing (S7): bytes and elapsed are both in hand here,
        // so this is where the fetch histograms are fed.
        let start = std::time::Instant::now();
        let result = async {
            let reply = self
                .rpc
                .rpc(peer, SCOPE, payload, FRAGMENT_TRANSFER_TIMEOUT)
                .await
                .map_err(classify)?;
            match decode_response(&reply)? {
                FragmentResponse::Fetch { found, data } => {
                    if found {
                        data.ok_or_else(|| {
                            TransportError::Transport(
                                "fragment marked found but data is None".into(),
                            )
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
        .await;
        match &result {
            Ok(data) => crate::observe::record_fetch(start.elapsed(), data.len()),
            Err(_) => crate::observe::record_fetch_failure(),
        }
        result
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The messages as they were encoded before the byte fields were
    /// marked `serde_bytes`: what a peer on an older release sends and
    /// expects.
    mod sequence_form {
        use super::*;

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        pub enum FragmentRequest {
            Health {
                fragment_hash: Blake3Hash,
            },
            Fetch {
                fragment_hash: Blake3Hash,
            },
            Store {
                fragment_hash: Blake3Hash,
                data: Vec<u8>,
            },
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
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
            Error {
                message: String,
            },
        }
    }

    fn bytes(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 7 + 3) as u8).collect()
    }

    // Impact: the byte fields' serde form is a wire contract with peers on
    // older releases; a different encoding would break fetch and store
    // across a mixed mesh.
    // Should: encode fetch replies and store requests to the same bytes as
    // the sequence form, for empty, small and multi-megabyte payloads.
    // Should: decode what the sequence form encodes, and vice versa.
    #[test]
    fn byte_fields_keep_the_sequence_wire_format() {
        let hash = Blake3Hash::new(blake3::hash(b"fragment"));
        for len in [0, 1, 300, 4 << 20] {
            let data = bytes(len);

            let new = encode(&FragmentRequest::Store {
                fragment_hash: hash,
                data: data.clone(),
            });
            let old = encode(&sequence_form::FragmentRequest::Store {
                fragment_hash: hash,
                data: data.clone(),
            });
            assert_eq!(new, old, "store request, {len} bytes");
            let (back, _): (FragmentRequest, usize) =
                bincode::serde::decode_from_slice(&old, bincode::config::standard()).unwrap();
            assert!(matches!(back, FragmentRequest::Store { data: d, .. } if d == data));

            for payload in [None, Some(data.clone())] {
                let new = encode(&FragmentResponse::Fetch {
                    found: payload.is_some(),
                    data: payload.clone(),
                });
                let old = encode(&sequence_form::FragmentResponse::Fetch {
                    found: payload.is_some(),
                    data: payload.clone(),
                });
                assert_eq!(
                    new,
                    old,
                    "fetch reply, {len} bytes, some={}",
                    payload.is_some()
                );
                assert!(matches!(
                    decode_response(&old).unwrap(),
                    FragmentResponse::Fetch { data: d, .. } if d == payload
                ));
                let (back, _): (sequence_form::FragmentResponse, usize) =
                    bincode::serde::decode_from_slice(&new, bincode::config::standard()).unwrap();
                assert_eq!(
                    back,
                    sequence_form::FragmentResponse::Fetch {
                        found: payload.is_some(),
                        data: payload
                    }
                );
            }
        }
    }
}
