use serde::{Deserialize, Serialize};
use std::time::Duration;
use crate::net::{IrohError, IrohTransport};
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::net::transport::ProtocolError;
use crate::AppState;
use crate::db::consensus as db;
use super::types::{TimeoutVote, TimeoutCertificate};

// ============================================================================
// View Data Fetch (catch-up)
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewDataRequest {
    pub view: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewDataResponse {
    pub view_data: super::types::ViewConsensusData,
}

/// Server: return consensus data for a specific view.
pub fn handle_view_data_request(req: ViewDataRequest, app_state: &AppState) -> IrohResponse {
    match db::get_view_consensus_data(app_state.db_pool.get(), req.view) {
        Ok(view_data) => IrohResponse::ViewDataFetchResponse(ViewDataResponse { view_data }),
        Err(e) => IrohResponse::Error {
            message: format!("failed to get view {} data: {:?}", req.view, e),
        },
    }
}

/// ViewConsensusData can contain blocks — allow a generous timeout.
const VIEW_DATA_TIMEOUT: Duration = Duration::from_secs(10);

/// Client: fetch consensus data for a specific view from a remote peer.
pub async fn fetch_view_data(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    view: i32,
) -> Result<super::types::ViewConsensusData, IrohError> {
    let req = IrohRequest::ViewDataFetch(ViewDataRequest { view });
    let response = transport.request(node_id, peer_node_id, &req, VIEW_DATA_TIMEOUT).await?;

    match response {
        IrohResponse::ViewDataFetchResponse(result) => Ok(result.view_data),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => {
            Err(IrohError::Protocol(ProtocolError::MalformedResponse(
                format!("unexpected response to ViewDataFetch: {:?}", other),
            )))
        }
    }
}

// ============================================================================
// View Poll (sync detection)
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewPollRequest {}

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewPollResponse {
    pub view: i32,
}

/// Server: return our current view number.
pub fn handle_view_poll_request(app_state: &AppState) -> IrohResponse {
    match db::get_consensus(app_state.db_pool.get()) {
        Ok(state) => IrohResponse::ViewPollResponse(ViewPollResponse { view: state.view }),
        Err(e) => IrohResponse::Error {
            message: format!("failed to get consensus state: {:?}", e),
        },
    }
}

/// View poll returns a single i32 — fast.
const VIEW_POLL_TIMEOUT: Duration = Duration::from_secs(3);

/// Client: poll a remote peer for its current view number.
pub async fn poll_view(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
) -> Result<i32, IrohError> {
    let req = IrohRequest::ViewPoll(ViewPollRequest {});
    let response = transport.request(node_id, peer_node_id, &req, VIEW_POLL_TIMEOUT).await?;

    match response {
        IrohResponse::ViewPollResponse(result) => Ok(result.view),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => {
            Err(IrohError::Protocol(ProtocolError::MalformedResponse(
                format!("unexpected response to ViewPoll: {:?}", other),
            )))
        }
    }
}

// ============================================================================
// Timeout Vote Broadcast
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct TimeoutVoteBroadcastRequest {
    pub timeout_vote: TimeoutVote,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TimeoutVoteBroadcastResponse {}

/// Server: process an incoming timeout vote.
pub async fn handle_timeout_vote_broadcast(
    req: TimeoutVoteBroadcastRequest,
    app_state: &AppState,
) -> IrohResponse {
    match super::routes::process_incoming_timeout_vote(req.timeout_vote, app_state).await {
        Ok(()) => IrohResponse::TimeoutVoteBroadcastResponse(TimeoutVoteBroadcastResponse {}),
        Err(e) => IrohResponse::Error {
            message: format!("timeout vote processing failed: {:?}", e),
        },
    }
}

/// Broadcast timeout: small messages, 3s is plenty on warm connections.
const BROADCAST_TIMEOUT: Duration = Duration::from_secs(3);

/// Client: broadcast a timeout vote to a remote peer.
pub async fn broadcast_timeout_vote(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    timeout_vote: &TimeoutVote,
) -> Result<(), IrohError> {
    let req = IrohRequest::TimeoutVoteBroadcast(TimeoutVoteBroadcastRequest {
        timeout_vote: timeout_vote.clone(),
    });
    let response = transport.request(node_id, peer_node_id, &req, BROADCAST_TIMEOUT).await?;

    match response {
        IrohResponse::TimeoutVoteBroadcastResponse(_) => Ok(()),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => {
            Err(IrohError::Protocol(ProtocolError::MalformedResponse(
                format!("unexpected response to TimeoutVoteBroadcast: {:?}", other),
            )))
        }
    }
}

// ============================================================================
// TC Broadcast
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct TcBroadcastRequest {
    pub tc: TimeoutCertificate,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TcBroadcastResponse {}

/// Server: process an incoming timeout certificate.
/// Stale TCs (already applied / view advanced) are acked as success — the sender
/// doesn't need to distinguish "applied now" from "already applied".
pub async fn handle_tc_broadcast(
    req: TcBroadcastRequest,
    app_state: &AppState,
) -> IrohResponse {
    if let Err(e) = req.tc.verify(app_state) {
        return match e {
            super::types::CertificateError::ValidationError => {
                // Stale or duplicate TC — already applied or view moved on. Ack.
                IrohResponse::TcBroadcastResponse(TcBroadcastResponse {})
            }
            _ => IrohResponse::Error {
                message: format!("TC verification failed: {:?}", e),
            },
        };
    }

    match super::routes::apply_timeout_certificate(req.tc, app_state, false, None).await {
        Ok(()) => IrohResponse::TcBroadcastResponse(TcBroadcastResponse {}),
        Err(super::types::CertificateError::ValidationError) => {
            // Race: view advanced between verify and apply. Ack.
            IrohResponse::TcBroadcastResponse(TcBroadcastResponse {})
        }
        Err(e) => IrohResponse::Error {
            message: format!("TC application failed: {:?}", e),
        },
    }
}

/// Client: broadcast a timeout certificate to a remote peer.
pub async fn broadcast_tc(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    tc: &TimeoutCertificate,
) -> Result<(), IrohError> {
    let req = IrohRequest::TcBroadcast(TcBroadcastRequest {
        tc: tc.clone(),
    });
    let response = transport.request(node_id, peer_node_id, &req, BROADCAST_TIMEOUT).await?;

    match response {
        IrohResponse::TcBroadcastResponse(_) => Ok(()),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => {
            Err(IrohError::Protocol(ProtocolError::MalformedResponse(
                format!("unexpected response to TcBroadcast: {:?}", other),
            )))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::types::ViewConsensusData;

    #[test]
    fn view_data_bincode_roundtrip() {
        let view_data = ViewConsensusData {
            view: 42,
            timeout_certificate: None,
            propose_qc: None,
            lock_qc: None,
            blocks: vec![],
        };

        let response = ViewDataResponse { view_data };
        let encoded = bincode::serde::encode_to_vec(&response, bincode::config::standard()).unwrap();
        let (decoded, _): (ViewDataResponse, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.view_data.view, 42);
        assert!(decoded.view_data.blocks.is_empty());
    }

    #[test]
    fn view_poll_bincode_roundtrip() {
        let response = ViewPollResponse { view: 99 };
        let encoded = bincode::serde::encode_to_vec(&response, bincode::config::standard()).unwrap();
        let (decoded, _): (ViewPollResponse, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.view, 99);
    }

    #[test]
    fn timeout_vote_request_bincode_roundtrip() {
        use crate::consensus::types::{VoteSignMessage, TimeoutSignData, ConsensusPhase};

        let req = TimeoutVoteBroadcastRequest {
            timeout_vote: TimeoutVote {
                sender: VoteSignMessage {
                    replica_id: 1,
                    signature: ed25519_dalek::Signature::from_bytes(&[0u8; 64]),
                },
                data: TimeoutSignData {
                    view_number: 5,
                    highest_qc_view: 4,
                    highest_qc_phase: ConsensusPhase::Propose,
                    highest_qc_hash: crate::types::Blake3Hash::from_bytes([0u8; 32]),
                },
            },
        };
        let encoded = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();
        let (decoded, _): (TimeoutVoteBroadcastRequest, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.timeout_vote.data.view_number, 5);
        assert_eq!(decoded.timeout_vote.sender.replica_id, 1);
    }

    #[test]
    fn tc_request_bincode_roundtrip() {
        use crate::consensus::types::{VoteSignMessage, VoteSignMessages, QuorumCertificate, ConsensusPhase};

        let req = TcBroadcastRequest {
            tc: TimeoutCertificate {
                view_number: 10,
                highest_qc: QuorumCertificate {
                    view_number: 9,
                    block_hash: crate::types::Blake3Hash::from_bytes([0u8; 32]),
                    phase: ConsensusPhase::Propose,
                    proposer_signature: VoteSignMessage {
                        replica_id: 1,
                        signature: ed25519_dalek::Signature::from_bytes(&[0u8; 64]),
                    },
                    voter_signatures: VoteSignMessages(vec![]),
                },
                signatures: VoteSignMessages(vec![]),
            },
        };
        let encoded = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();
        let (decoded, _): (TcBroadcastRequest, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.tc.view_number, 10);
        assert_eq!(decoded.tc.highest_qc.view_number, 9);
    }
}
