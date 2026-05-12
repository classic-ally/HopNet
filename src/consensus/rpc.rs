use super::types::{Ballot, QuorumCertificate, TimeoutCertificate, TimeoutVote, VoteSignMessage};
use crate::AppState;
use crate::db::consensus as db;
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::net::transport::{ProtocolError, TransportError};
use crate::net::{IrohError, IrohTransport};
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
        Ok(view_data) => {
            IrohResponse::ViewDataFetchResponse(Box::new(ViewDataResponse { view_data }))
        }
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
    let response = transport
        .request(node_id, peer_node_id, &req, VIEW_DATA_TIMEOUT)
        .await?;

    match response {
        IrohResponse::ViewDataFetchResponse(result) => Ok(result.view_data),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to ViewDataFetch: {:?}", other),
        ))),
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
    let response = transport
        .request(node_id, peer_node_id, &req, VIEW_POLL_TIMEOUT)
        .await?;

    match response {
        IrohResponse::ViewPollResponse(result) => Ok(result.view),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to ViewPoll: {:?}", other),
        ))),
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
    let response = transport
        .request(node_id, peer_node_id, &req, BROADCAST_TIMEOUT)
        .await?;

    match response {
        IrohResponse::TimeoutVoteBroadcastResponse(_) => Ok(()),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to TimeoutVoteBroadcast: {:?}", other),
        ))),
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
pub async fn handle_tc_broadcast(req: TcBroadcastRequest, app_state: &AppState) -> IrohResponse {
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
    let req = IrohRequest::TcBroadcast(TcBroadcastRequest { tc: tc.clone() });
    let response = transport
        .request(node_id, peer_node_id, &req, BROADCAST_TIMEOUT)
        .await?;

    match response {
        IrohResponse::TcBroadcastResponse(_) => Ok(()),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to TcBroadcast: {:?}", other),
        ))),
    }
}

// ============================================================================
// QC Broadcast
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct QcBroadcastRequest {
    pub qc: QuorumCertificate,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QcBroadcastResponse {}

/// Server: process an incoming quorum certificate.
pub async fn handle_qc_broadcast(req: QcBroadcastRequest, app_state: &AppState) -> IrohResponse {
    match super::routes::process_incoming_qc(req.qc, app_state).await {
        Ok(()) => IrohResponse::QcBroadcastResponse(QcBroadcastResponse {}),
        Err(e) => IrohResponse::Error {
            message: format!("QC processing failed: {:?}", e),
        },
    }
}

/// Client: broadcast a quorum certificate to a remote peer.
pub async fn broadcast_qc_to_peer(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    qc: &QuorumCertificate,
) -> Result<(), IrohError> {
    let req = IrohRequest::QcBroadcast(QcBroadcastRequest { qc: qc.clone() });
    let response = transport
        .request(node_id, peer_node_id, &req, BROADCAST_TIMEOUT)
        .await?;

    match response {
        IrohResponse::QcBroadcastResponse(_) => Ok(()),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to QcBroadcast: {:?}", other),
        ))),
    }
}

// ============================================================================
// Ballot Submission
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct BallotRequest {
    pub ballot: Ballot,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BallotResponse {
    pub vote: VoteSignMessage,
}

/// Server: process an incoming ballot and return signed vote.
pub async fn handle_ballot_request(req: BallotRequest, app_state: &AppState) -> IrohResponse {
    match super::routes::process_incoming_ballot(req.ballot, app_state).await {
        Ok(vote) => IrohResponse::BallotSubmissionResponse(BallotResponse { vote }),
        Err(e) => IrohResponse::Error {
            message: format!("ballot processing failed: {:?}", e),
        },
    }
}

/// Lock-phase ballots may trigger intra-view sync (network call) and Propose-phase
/// does block insertion + parallel transaction validation.
const BALLOT_TIMEOUT: Duration = Duration::from_secs(5);

/// Client: submit a ballot to a remote peer and return the signed vote.
pub async fn submit_ballot_to_peer(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    ballot: &Ballot,
) -> Result<VoteSignMessage, IrohError> {
    let req = IrohRequest::BallotSubmission(BallotRequest {
        ballot: ballot.clone(),
    });
    let response = transport
        .request(node_id, peer_node_id, &req, BALLOT_TIMEOUT)
        .await?;

    match response {
        IrohResponse::BallotSubmissionResponse(result) => Ok(result.vote),
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to BallotSubmission: {:?}", other),
        ))),
    }
}

// ============================================================================
// Transaction Forward
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionForwardRequest {
    pub transactions: Vec<super::types::Transaction>,
    pub view: i32, // Forwarder's current view — catch-up hint, not a gate
}

/// Per-transaction result returned by the leader after forwarding.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TransactionForwardResult {
    Committed,
    Rejected {
        reason: String,
    },
    /// Transient failure — caller should re-queue for retry after view change
    Retry {
        reason: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionForwardResponse {
    pub results: Vec<TransactionForwardResult>,
}

/// Server: process forwarded transactions by pushing them into the local consensus queue.
/// The handler's message-driven catch-up already ran before dispatch.
/// Performs nonce dedup before enqueueing — already-committed transactions get immediate Committed.
pub async fn handle_transaction_forward(
    req: TransactionForwardRequest,
    app_state: &AppState,
) -> IrohResponse {
    // Early nonce dedup: check which transactions were already committed
    let nonces: Vec<_> = req.transactions.iter().map(|tx| tx.nonce.clone()).collect();
    let committed_nonces = match app_state.db_pool.get() {
        Ok(conn) => {
            crate::db::consensus::check_committed_nonces(&conn, &nonces).unwrap_or_default()
        }
        Err(_) => std::collections::HashSet::new(),
    };

    // Separate already-committed from pending transactions
    let mut results_map: Vec<Option<TransactionForwardResult>> = vec![None; req.transactions.len()];
    let mut pending_txs = Vec::new();
    let mut pending_indices = Vec::new();

    for (i, tx) in req.transactions.into_iter().enumerate() {
        if committed_nonces.contains(&tx.nonce.to_string()) {
            results_map[i] = Some(TransactionForwardResult::Committed);
        } else {
            pending_indices.push(i);
            pending_txs.push(tx);
        }
    }

    // Enqueue remaining pending transactions
    if !pending_txs.is_empty() {
        let submit_results = app_state
            .consensus_queue
            .enqueue_forwarded(pending_txs)
            .await;
        for (idx, result) in pending_indices.into_iter().zip(submit_results.into_iter()) {
            results_map[idx] = Some(match result {
                Ok(()) => TransactionForwardResult::Committed,
                Err(super::queue::ConsensusSubmitError::Rejected(reason)) => {
                    TransactionForwardResult::Rejected { reason }
                }
                Err(e) => TransactionForwardResult::Retry {
                    reason: format!("{}", e),
                },
            });
        }
    }

    let results = results_map.into_iter().map(|r| r.unwrap()).collect();
    IrohResponse::TransactionForwardResponse(TransactionForwardResponse { results })
}

/// Result from the two-phase forward protocol.
#[derive(Debug)]
pub enum ForwardAckResult {
    /// Leader never received the request (no ACK within timeout)
    NoAck,
    /// Leader ACKed and returned final results
    AckedWithResult(Vec<TransactionForwardResult>),
    /// Leader ACKed, view advanced before result arrived — nonces are committed in local DB
    AckedViewChanged,
    /// Leader ACKed but safety timeout fired — view hasn't changed, nonces not committed yet
    AckedNoResult,
    /// Rejection: handler is not the leader (includes handler's view for catch-up)
    NotLeader { view: i32 },
    /// Rejection: consensus lock is held (leader is busy)
    Busy,
}

/// ACK timeout: how long to wait for the immediate ACK from the leader.
const FORWARD_ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// Result timeout: how long to wait for the final result after ACK.
/// Consensus completes in 1-3s normally; 15s covers slow rounds without
/// blocking the batch processor for a full timeout detection cycle.
const FORWARD_RESULT_TIMEOUT: Duration = Duration::from_secs(15);

/// Client: forward transactions to the leader with two-phase ACK protocol.
///
/// Phase 1: Send request, wait for immediate ACK (leader received it).
/// Phase 2: Wait for final result (leader processed it through consensus).
///
/// The two-phase protocol lets the forwarder distinguish "leader never got it" (safe to retry)
/// from "leader has it but hasn't finished" (check nonce table instead of retrying).
pub async fn forward_transactions_with_ack(
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    transactions: Vec<super::types::Transaction>,
    view: i32,
    view_changed: &tokio::sync::Notify,
) -> Result<ForwardAckResult, IrohError> {
    // Capture view change signal BEFORE any network I/O
    let view_notified = view_changed.notified();

    let req = IrohRequest::TransactionForward(TransactionForwardRequest { transactions, view });

    let request_id: u64 = rand::random();
    let conn = transport.get_connection(node_id, peer_node_id).await?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;

    // Send request with request_id prefix
    use tokio::io::AsyncWriteExt;
    send.write_all(&request_id.to_le_bytes())
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
    crate::net::transport::send_message(&mut send, &req).await?;
    send.finish()
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;

    // Phase 1: Wait for ACK or direct response
    let first_msg: Result<IrohResponse, IrohError> = tokio::time::timeout(
        FORWARD_ACK_TIMEOUT,
        crate::net::transport::recv_message(&mut recv),
    )
    .await
    .map_err(|_| IrohError::Transport(TransportError::Timeout))?;

    let first_msg = match first_msg {
        Ok(msg) => msg,
        Err(_) => return Ok(ForwardAckResult::NoAck),
    };

    // Check if first message is ACK, rejection, or direct result (backward compat)
    match first_msg {
        IrohResponse::TransactionForwardNotLeader { view } => {
            Ok(ForwardAckResult::NotLeader { view })
        }
        IrohResponse::TransactionForwardBusy => Ok(ForwardAckResult::Busy),
        IrohResponse::TransactionForwardAck => {
            // Got ACK — leader has received and is processing
            // Phase 2: Race result against view change notification.
            // View advances (Lock QC received, nonces committed) before the leader
            // finishes processing — resolve via nonce table when view wins.
            tokio::pin!(view_notified);
            let result = tokio::select! {
                msg = crate::net::transport::recv_message(&mut recv) => msg,
                _ = &mut view_notified => return Ok(ForwardAckResult::AckedViewChanged),
                _ = tokio::time::sleep(FORWARD_RESULT_TIMEOUT) => {
                    return Ok(ForwardAckResult::AckedNoResult)
                }
            };

            match result {
                Ok(IrohResponse::TransactionForwardResponse(resp)) => {
                    Ok(ForwardAckResult::AckedWithResult(resp.results))
                }
                Ok(IrohResponse::Error { message }) => {
                    Err(IrohError::Protocol(ProtocolError::PeerError(message)))
                }
                Ok(_) => Ok(ForwardAckResult::AckedNoResult),
                Err(_) => Ok(ForwardAckResult::AckedNoResult),
            }
        }
        IrohResponse::TransactionForwardResponse(resp) => {
            // Backward compat: old leader sent direct result without ACK
            Ok(ForwardAckResult::AckedWithResult(resp.results))
        }
        IrohResponse::Error { message } => {
            Err(IrohError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to TransactionForward: {:?}", other),
        ))),
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
        let encoded =
            bincode::serde::encode_to_vec(&response, bincode::config::standard()).unwrap();
        let (decoded, _): (ViewDataResponse, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.view_data.view, 42);
        assert!(decoded.view_data.blocks.is_empty());
    }

    #[test]
    fn view_poll_bincode_roundtrip() {
        let response = ViewPollResponse { view: 99 };
        let encoded =
            bincode::serde::encode_to_vec(&response, bincode::config::standard()).unwrap();
        let (decoded, _): (ViewPollResponse, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.view, 99);
    }

    #[test]
    fn timeout_vote_request_bincode_roundtrip() {
        use crate::consensus::types::{ConsensusPhase, TimeoutSignData, VoteSignMessage};

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
                lock_vote_evidence: None,
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
        use crate::consensus::types::{
            ConsensusPhase, QuorumCertificate, VoteSignMessage, VoteSignMessages,
        };

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

    #[test]
    fn ballot_request_bincode_roundtrip() {
        use crate::consensus::types::{
            Ballot, Block, BlockData, ConsensusPhase, VoteSignData, VoteSignMessage,
        };

        let req = BallotRequest {
            ballot: Ballot {
                initiator: VoteSignMessage {
                    replica_id: 1,
                    signature: ed25519_dalek::Signature::from_bytes(&[0u8; 64]),
                },
                data: VoteSignData {
                    block_hash: crate::types::Blake3Hash::from_bytes([0u8; 32]),
                    block_height: 5,
                    view: 10,
                    phase: ConsensusPhase::Propose,
                },
                block: Block {
                    block_hash: crate::types::Blake3Hash::from_bytes([0u8; 32]),
                    data: BlockData {
                        height: 5,
                        view_number: 10,
                        parent_hash: None,
                        transactions: None,
                    },
                },
            },
        };
        let encoded = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();
        let (decoded, _): (BallotRequest, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.ballot.data.view, 10);
        assert_eq!(decoded.ballot.data.phase, ConsensusPhase::Propose);
        assert_eq!(decoded.ballot.initiator.replica_id, 1);
        assert_eq!(decoded.ballot.block.data.height, 5);

        // Also test BallotResponse roundtrip
        let resp = BallotResponse {
            vote: VoteSignMessage {
                replica_id: 3,
                signature: ed25519_dalek::Signature::from_bytes(&[1u8; 64]),
            },
        };
        let encoded = bincode::serde::encode_to_vec(&resp, bincode::config::standard()).unwrap();
        let (decoded, _): (BallotResponse, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.vote.replica_id, 3);
    }

    #[test]
    fn qc_broadcast_request_bincode_roundtrip() {
        use crate::consensus::types::{ConsensusPhase, VoteSignMessage, VoteSignMessages};

        let req = QcBroadcastRequest {
            qc: QuorumCertificate {
                view_number: 7,
                block_hash: crate::types::Blake3Hash::from_bytes([1u8; 32]),
                phase: ConsensusPhase::Lock,
                proposer_signature: VoteSignMessage {
                    replica_id: 2,
                    signature: ed25519_dalek::Signature::from_bytes(&[0u8; 64]),
                },
                voter_signatures: VoteSignMessages(vec![]),
            },
        };
        let encoded = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();
        let (decoded, _): (QcBroadcastRequest, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.qc.view_number, 7);
        assert_eq!(decoded.qc.phase, ConsensusPhase::Lock);
        assert_eq!(decoded.qc.proposer_signature.replica_id, 2);
    }

    #[test]
    fn transaction_forward_request_bincode_roundtrip() {
        let req = TransactionForwardRequest {
            transactions: vec![],
            view: 15,
        };
        let encoded = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();
        let (decoded, _): (TransactionForwardRequest, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.view, 15);
        assert!(decoded.transactions.is_empty());

        // Also test response roundtrip
        let resp = TransactionForwardResponse {
            results: vec![
                TransactionForwardResult::Committed,
                TransactionForwardResult::Rejected {
                    reason: "test".into(),
                },
                TransactionForwardResult::Retry {
                    reason: "already proposed".into(),
                },
            ],
        };
        let encoded = bincode::serde::encode_to_vec(&resp, bincode::config::standard()).unwrap();
        let (decoded_resp, _): (TransactionForwardResponse, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded_resp.results.len(), 3);
    }
}
