use serde::{Deserialize, Serialize};
use std::time::Duration;
use crate::net::{IrohError, IrohTransport};
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::net::transport::ProtocolError;
use crate::AppState;
use crate::db::consensus as db;

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

/// Server-side: return consensus data for a specific view.
pub fn handle_view_data_request(req: ViewDataRequest, app_state: &AppState) -> IrohResponse {
    match db::get_view_consensus_data(app_state.db_pool.get(), req.view) {
        Ok(view_data) => IrohResponse::ViewDataFetchResponse(ViewDataResponse { view_data }),
        Err(e) => IrohResponse::Error {
            message: format!("failed to get view {} data: {:?}", req.view, e),
        },
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

/// Server-side: return our current view number.
pub fn handle_view_poll_request(app_state: &AppState) -> IrohResponse {
    match db::get_consensus(app_state.db_pool.get()) {
        Ok(state) => IrohResponse::ViewPollResponse(ViewPollResponse { view: state.view }),
        Err(e) => IrohResponse::Error {
            message: format!("failed to get consensus state: {:?}", e),
        },
    }
}

// ============================================================================
// Client callers
// ============================================================================

/// ViewConsensusData can contain blocks — allow a generous timeout.
const VIEW_DATA_TIMEOUT: Duration = Duration::from_secs(10);

/// View poll returns a single i32 — fast.
const VIEW_POLL_TIMEOUT: Duration = Duration::from_secs(3);

/// Fetch consensus data for a specific view from a remote peer.
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

/// Poll a remote peer for its current view number.
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
}
