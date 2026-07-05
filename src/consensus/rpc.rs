//! Inter-node RPC for the consensus queue: the two-phase transaction-forward
//! protocol. The bespoke engine's ballot/QC/TC/view-poll RPC was deleted at
//! Stage 5b — consensus messages now travel as `IrohRequest::ConsensusMsg`
//! straight to the malachite shell.

use crate::AppState;
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::net::transport::{ProtocolError, TransportError};
use crate::net::{IrohError, IrohTransport};
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ============================================================================
// Transaction Forward
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionForwardRequest {
    pub transactions: Vec<super::types::Transaction>,
    pub height: i64, // Forwarder's target height — diagnostic hint, not a gate
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
    NotProposer { height: i64, round: u32 },
    /// Rejection: node is busy (legacy; malachite path never sends it)
    Busy,
}

/// ACK timeout: how long to wait for the immediate ACK from the leader.
const FORWARD_ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// Result timeout: how long to wait for the final result after ACK.
/// Consensus completes in 1-3s normally; 15s covers slow rounds without
/// blocking the batch processor for a full timeout detection cycle.
const FORWARD_RESULT_TIMEOUT: Duration = Duration::from_secs(15);

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
    transport: &IrohTransport,
    node_id: i32,
    peer_node_id: iroh::PublicKey,
    transactions: Vec<super::types::Transaction>,
    height: i64,
    decided: &mut tokio::sync::watch::Receiver<u64>,
) -> Result<ForwardAckResult, IrohError> {
    // Mark the watch as seen BEFORE any network I/O so only NEW decides win
    // the phase-2 race.
    decided.borrow_and_update();

    let req = IrohRequest::TransactionForward(TransactionForwardRequest {
        transactions,
        height,
    });

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
        IrohResponse::TransactionForwardNotProposer { height, round } => {
            Ok(ForwardAckResult::NotProposer { height, round })
        }
        IrohResponse::TransactionForwardBusy => Ok(ForwardAckResult::Busy),
        IrohResponse::TransactionForwardAck => {
            // Got ACK — proposer has received and is processing.
            // Phase 2: race the result against the decided watch. A decide
            // that lands first means the nonce table can already answer.
            let result = tokio::select! {
                msg = crate::net::transport::recv_message(&mut recv) => msg,
                _ = decided.changed() => return Ok(ForwardAckResult::AckedDecided),
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
