use iroh::Endpoint;

use super::protocol::{IrohRequest, IrohResponse};
use super::transport::{recv_message, send_message, IrohError, TransportError};
use crate::AppState;
use crate::types::PubKey;

/// Handle incoming iroh connections
/// This runs in a loop accepting connections from the endpoint
pub async fn handle_incoming_connections(endpoint: Endpoint, app_state: AppState) {
    loop {
        match endpoint.accept().await {
            Some(incoming) => {
                let app_state = app_state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(incoming, app_state).await {
                        tracing::warn!("iroh connection error: {}", e);
                    }
                });
            }
            None => {
                tracing::info!("iroh endpoint closed, stopping accept loop");
                break;
            }
        }
    }
}

/// Look up the node_id for a known peer's public key.
/// The before_registration hook already rejected unknown peers before the connection
/// was established, so this is just for resolving the node_id for logging/routing.
fn lookup_node_id(app_state: &AppState, peer_pubkey: &iroh::PublicKey) -> Option<i32> {
    let conn = app_state.db_pool.get().ok()?;
    let pubkey = PubKey(
        ed25519_dalek::VerifyingKey::from_bytes(peer_pubkey.as_bytes()).ok()?
    );
    let pubkey_encoded = bincode::serde::encode_to_vec(
        &pubkey, bincode::config::standard()
    ).ok()?;
    conn.query_row(
        "SELECT node_id FROM nodes WHERE pubkey = ?",
        [pubkey_encoded.as_slice()],
        |row| row.get(0),
    ).ok()
}

async fn handle_connection(
    incoming: iroh::endpoint::Incoming,
    app_state: AppState,
) -> Result<(), IrohError> {
    // Unknown peers are already rejected by the before_registration hook
    // before the connection reaches this point (no holepunching occurs).
    let conn = incoming.await
        .map_err(|e| IrohError::Transport(TransportError::ConnectionFailed(e.to_string())))?;
    let peer_pubkey = conn.remote_id();

    let peer_node_id = lookup_node_id(&app_state, &peer_pubkey).unwrap_or(-1);
    tracing::debug!("accepted iroh connection from node {}", peer_node_id);

    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let app_state = app_state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(send, recv, peer_node_id, app_state).await {
                        tracing::debug!("iroh stream error from node {}: {}", peer_node_id, e);
                    }
                });
            }
            Err(e) => {
                tracing::debug!("iroh connection closed from node {}: {}", peer_node_id, e);
                break;
            }
        }
    }

    Ok(())
}

async fn handle_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    _peer_node_id: i32,
    _app_state: AppState,
) -> Result<(), IrohError> {
    let request: IrohRequest = recv_message(&mut recv).await?;

    let response = match request {
        IrohRequest::Ping { nonce } => IrohResponse::Pong { nonce },
    };

    send_message(&mut send, &response).await?;
    send.finish()
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;

    Ok(())
}
