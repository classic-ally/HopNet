//! Inter-node RPC for the consensus queue: the two-phase transaction-forward
//! protocol over the "txforward" streamed comms scope. This module owns the
//! scope's wire vocabulary ([`TransactionForwardRequest`] as the request
//! payload, [`ForwardReply`] frames back); the server side lives in
//! `net::scopes::TxForwardScope`.

use crate::AppState;
use hopnet_comms::{Call, CallOptions, CommsError, IrohComms, PeerRef, ProtocolError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ============================================================================
// Transaction Forward
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionForwardRequest {
    pub transactions: Vec<super::types::Transaction>,
    pub height: u64, // Forwarder's target height — diagnostic hint, not a gate
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

/// Frame vocabulary for the "txforward" streamed scope (server → client).
#[derive(Serialize, Deserialize, Debug)]
pub enum ForwardReply {
    /// Rejection: this node is not the proposer for its current (height,
    /// round). Includes the handler's position so the forwarder can retarget.
    NotProposer { height: u64, round: u32 },
    /// Immediate ACK before processing (phase 1).
    Ack,
    /// Final result after processing (phase 2).
    Result(TransactionForwardResponse),
    Error {
        message: String,
    },
}

/// Server: process forwarded transactions by pushing them into the local consensus queue.
/// The handler's message-driven catch-up already ran before dispatch.
/// Performs nonce dedup before enqueueing — already-committed transactions get immediate Committed.
pub async fn handle_transaction_forward(
    req: TransactionForwardRequest,
    app_state: &AppState,
) -> TransactionForwardResponse {
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
        for (idx, result) in pending_indices.into_iter().zip(submit_results) {
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
    TransactionForwardResponse { results }
}

/// Result from the two-phase forward protocol.
#[derive(Debug)]
pub enum ForwardAckResult {
    /// Proposer never received the request (no ACK within timeout)
    NoAck,
    /// Proposer ACKed and returned final results
    AckedWithResult(Vec<TransactionForwardResult>),
    /// Proposer ACKed, a height decided before the result arrived — resolve
    /// via the local nonce table
    AckedDecided,
    /// Proposer ACKed but safety timeout fired — nothing decided yet
    AckedNoResult,
    /// Rejection: handler is not the proposer (includes its position so the
    /// forwarder can retarget)
    NotProposer { height: u64, round: u32 },
}

/// ACK timeout: how long to wait for the immediate ACK from the leader.
const FORWARD_ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// Result timeout: how long to wait for the final result after ACK.
/// Consensus completes in 1-3s normally; 15s covers slow rounds without
/// blocking the batch processor for a full timeout detection cycle.
const FORWARD_RESULT_TIMEOUT: Duration = Duration::from_secs(15);
/// Connect bound for the forward path, tighter than the transport-level
/// connect budget (10s). The batch processor is single-threaded: while a
/// dial to an unresponsive proposer hangs, NO transactions move on this node.
/// Failing fast keeps the stall under one consensus propose timeout, so the
/// caller's resume-own-engine path advances rounds past the dead proposer
/// instead of arriving after the fact.
const FORWARD_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Client: forward transactions to the proposer with two-phase ACK protocol.
///
/// Phase 1: Send request, wait for immediate ACK (proposer received it).
/// Phase 2: Wait for final result (proposer committed it through consensus).
///
/// The two-phase protocol lets the forwarder distinguish "proposer never got
/// it" (safe to retry) from "proposer has it but hasn't finished" (check the
/// nonce table instead of retrying). Phase 2 races the decided watch: a
/// decide that lands before the result means the nonce table already knows.
pub async fn forward_transactions_with_ack(
    comms: &IrohComms,
    peer: &PeerRef,
    transactions: Vec<super::types::Transaction>,
    height: u64,
    decided: &mut tokio::sync::watch::Receiver<u64>,
) -> Result<ForwardAckResult, CommsError> {
    // Mark the watch as seen BEFORE any network I/O so only NEW decides win
    // the phase-2 race.
    decided.borrow_and_update();

    let payload = crate::net::encode_payload(&TransactionForwardRequest {
        transactions,
        height,
    });
    let mut call: Call = comms
        .open_call(
            peer,
            "txforward",
            payload,
            CallOptions {
                connect_timeout: Some(FORWARD_CONNECT_TIMEOUT),
            },
        )
        .await?;

    // Phase 1: wait for ACK or rejection. Any recv failure — timeout, stream
    // error, undecodable frame — means the proposer never acknowledged:
    // NoAck (the caller evicts the connection and retries).
    let first_msg: ForwardReply = match call.recv(FORWARD_ACK_TIMEOUT).await {
        Ok(bytes) => match crate::net::decode_payload(&bytes) {
            Ok(msg) => msg,
            Err(_) => return Ok(ForwardAckResult::NoAck),
        },
        Err(_) => return Ok(ForwardAckResult::NoAck),
    };

    match first_msg {
        ForwardReply::NotProposer { height, round } => {
            Ok(ForwardAckResult::NotProposer { height, round })
        }
        ForwardReply::Ack => {
            // Got ACK — proposer has received and is processing.
            // Phase 2: race the result against the decided watch. A decide
            // that lands first means the nonce table can already answer; the
            // recv timeout is the safety bound for a proposer that never
            // finishes.
            let result = tokio::select! {
                frame = call.recv(FORWARD_RESULT_TIMEOUT) => frame,
                _ = decided.changed() => return Ok(ForwardAckResult::AckedDecided),
            };

            match result {
                Ok(bytes) => match crate::net::decode_payload::<ForwardReply>(&bytes) {
                    Ok(ForwardReply::Result(resp)) => {
                        Ok(ForwardAckResult::AckedWithResult(resp.results))
                    }
                    Ok(ForwardReply::Error { message }) => {
                        Err(CommsError::Protocol(ProtocolError::PeerError(message)))
                    }
                    Ok(_) | Err(_) => Ok(ForwardAckResult::AckedNoResult),
                },
                // Timeout (the safety bound) or stream failure — nothing
                // decided yet from our perspective.
                Err(_) => Ok(ForwardAckResult::AckedNoResult),
            }
        }
        ForwardReply::Error { message } => {
            Err(CommsError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(CommsError::Protocol(ProtocolError::MalformedResponse(
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

    #[test]
    fn transaction_forward_request_bincode_roundtrip() {
        let req = TransactionForwardRequest {
            transactions: vec![],
            height: 15,
        };
        let encoded = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();
        let (decoded, _): (TransactionForwardRequest, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.height, 15);
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
