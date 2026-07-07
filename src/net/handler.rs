use std::sync::Arc;
use std::time::Duration;

use iroh::Endpoint;
use tokio::io::AsyncReadExt;
use tracing::Instrument;

use super::protocol::{IrohRequest, IrohResponse};
use super::transport::{
    IrohError, ProtocolError, TransportError, encode_message, recv_message, send_raw,
};
use crate::AppState;
use crate::db::consensus as db;
use crate::types::PubKey;

/// Handle incoming iroh connections
/// This runs in a loop accepting connections from the endpoint.
///
/// RUNTIME CONTRACT: this loop is spawned on the dedicated net runtime (see
/// `transport::net_rt`); per-connection and per-stream tasks inherit it, so
/// nothing on this path may block (no r2d2, no rusqlite, no sync I/O).
/// Requests that need the database are handed off to `app_rt` — the main
/// runtime's Handle, captured at spawn time — inside `handle_stream`.
pub async fn handle_incoming_connections(
    endpoint: Endpoint,
    app_state: AppState,
    app_rt: tokio::runtime::Handle,
) {
    loop {
        match endpoint.accept().await {
            Some(incoming) => {
                let app_state = app_state.clone();
                let app_rt = app_rt.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(incoming, app_state, app_rt).await {
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
/// Blocking pool checkout — runs on the blocking pool, never on a net worker.
async fn lookup_node_id(app_state: &AppState, peer_pubkey: &iroh::PublicKey) -> Option<i32> {
    let db_pool = app_state.db_pool.clone();
    let pubkey = PubKey(ed25519_dalek::VerifyingKey::from_bytes(peer_pubkey.as_bytes()).ok()?);
    tokio::task::spawn_blocking(move || {
        let conn = db_pool.get().ok()?;
        let pubkey_encoded =
            bincode::serde::encode_to_vec(pubkey, bincode::config::standard()).ok()?;
        conn.query_row(
            "SELECT node_id FROM nodes WHERE pubkey = ?",
            [pubkey_encoded.as_slice()],
            |row| row.get(0),
        )
        .ok()
    })
    .await
    .ok()
    .flatten()
}

async fn handle_connection(
    incoming: iroh::endpoint::Incoming,
    app_state: AppState,
    app_rt: tokio::runtime::Handle,
) -> Result<(), IrohError> {
    // Unknown peers are already rejected by the before_registration hook
    // before the connection reaches this point (no holepunching occurs).
    let conn = incoming
        .await
        .map_err(|e| IrohError::Transport(TransportError::ConnectionFailed(e.to_string())))?;
    let peer_pubkey = conn.remote_id();

    let peer_node_id = lookup_node_id(&app_state, &peer_pubkey).await.unwrap_or(-1);
    tracing::debug!("accepted iroh connection from node {}", peer_node_id);

    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let app_state = app_state.clone();
                let app_rt = app_rt.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        handle_stream(send, recv, peer_node_id, app_state, app_rt).await
                    {
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

/// Read one request off the stream, then route it by plane:
/// - mesh plane (Ping, LatencyPing, ThroughputUpload, ConsensusMsg — pure
///   channel/CPU work) is served inline on the net runtime;
/// - app plane (anything touching the DB or disk: TransactionForward,
///   DecidedFetch, Fragment*, StorageQuery, JoinDeliver) is handed off to the
///   main runtime, so API-load starvation degrades peer RPC service but can
///   never stall consensus message flow.
async fn handle_stream(
    send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    peer_node_id: i32,
    app_state: AppState,
    app_rt: tokio::runtime::Handle,
) -> Result<(), IrohError> {
    // Read request_id prefix (8 bytes)
    let mut id_buf = [0u8; 8];
    recv.read_exact(&mut id_buf)
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
    let request_id = u64::from_le_bytes(id_buf);

    let span =
        tracing::debug_span!("rpc_req", id = %format!("{:016x}", request_id), from = peer_node_id);
    // Decode before routing (cheap CPU; the full request is already framed).
    let request: IrohRequest = recv_message(&mut recv)
        .instrument(span.clone())
        .await?;

    // Mesh plane (pure channel/CPU work) — inline on the net runtime.
    let mesh_plane = matches!(
        request,
        IrohRequest::Ping { .. }
            | IrohRequest::LatencyPing(_)
            | IrohRequest::ThroughputUpload(_)
            | IrohRequest::ConsensusMsg(_)
    );
    if mesh_plane {
        return process_request(send, request_id, request, peer_node_id, app_state)
            .instrument(span)
            .await;
    }
    // Consensus-support plane — the QUEUE runtime (blocking DB allowed, never
    // starved by API load). TransactionForward is consensus INTAKE: if its ACK
    // can't beat the forwarder's timeout under load, proposers never receive
    // batches and blocks decide empty (the image-12 finding). DecidedFetch
    // serves laggard sync — same liveness class.
    let queue_plane = matches!(
        request,
        IrohRequest::TransactionForward(_) | IrohRequest::DecidedFetch { .. }
    );
    if queue_plane {
        return crate::consensus::queue::queue_rt()
            .spawn(
                process_request(send, request_id, request, peer_node_id, app_state)
                    .instrument(span),
            )
            .await
            .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
    }
    // App plane (fragments, storage, join) — the main runtime; degrades under
    // API overload by design.
    app_rt
        .spawn(
            process_request(send, request_id, request, peer_node_id, app_state).instrument(span),
        )
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?
}

async fn process_request(
    mut send: iroh::endpoint::SendStream,
    request_id: u64,
    request: IrohRequest,
    peer_node_id: i32,
    app_state: AppState,
) -> Result<(), IrohError> {
    {
        // TransactionForward uses two-phase ACK (bypasses OnceCell — nonce table handles dedup)
        if let IrohRequest::TransactionForward(req) = request {
            // Validate proposer status before ACKing — avoids multi-hop
            // forwarding. Best-effort: if the target is unreadable, ACK and
            // let the local queue route (it forwards onward if needed).
            'reject: {
                let my_node_id = match app_state.get_node_id() {
                    Ok(id) => id,
                    Err(_) => break 'reject,
                };
                let Some((height, round, proposer)) =
                    crate::consensus::malachite::engine::proposal_target(&app_state)
                else {
                    break 'reject;
                };

                if proposer != my_node_id {
                    // The forwarder targeting a height above ours proves peers
                    // decided past us — kick a sync instead of waiting seconds
                    // for timeout-driven republish to drag us forward.
                    if req.height > height as i64 {
                        crate::consensus::malachite::engine::kick_sync_if_behind(
                            &app_state,
                            (req.height - 1) as u64,
                            peer_node_id,
                        );
                    }
                    let reject = encode_message(&IrohResponse::TransactionForwardNotProposer {
                        height: height as i64,
                        round,
                    })?;
                    send_raw(&mut send, &reject).await?;
                    send.finish().map_err(|e| {
                        IrohError::Transport(TransportError::StreamFailed(e.to_string()))
                    })?;
                    return Ok(());
                }
            }

            // Phase 1: Send immediate ACK (validated as proposer)
            let ack_bytes = encode_message(&IrohResponse::TransactionForwardAck)?;
            send_raw(&mut send, &ack_bytes).await?;

            // Phase 2: Process and send final result
            let response = crate::consensus::rpc::handle_transaction_forward(req, &app_state).await;
            let result_bytes = encode_message(&response)?;
            send_raw(&mut send, &result_bytes).await?;
            send.finish()
                .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
            return Ok(());
        }

        // All other requests: existing OnceCell dedup path
        let cell = {
            let mut cache = app_state.dedup_cache.lock().unwrap();
            cache
                .entry(request_id)
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };

        // Schedule cleanup after TTL
        let cache = app_state.dedup_cache.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(300)).await;
            cache.lock().unwrap().remove(&request_id);
        });

        // First caller processes; duplicates wait for the same result
        let response_bytes = cell
            .get_or_try_init(|| async {
                let response = match request {
                    IrohRequest::Ping { nonce } => IrohResponse::Pong { nonce },
                    IrohRequest::FragmentHealthCheck(req) => {
                        IrohResponse::FragmentHealthCheckResponse(
                            crate::storage_host::rpc::handle_fragment_health_check(
                                req,
                                &app_state.fragments_dir,
                            ),
                        )
                    }
                    IrohRequest::TransactionForward(_) => unreachable!("handled above"),
                    IrohRequest::FragmentFetch(req) => IrohResponse::FragmentFetchResponse(
                        crate::storage_host::rpc::handle_fragment_fetch(req, &app_state.fragments_dir),
                    ),
                    IrohRequest::FragmentStore(req) => {
                        crate::storage_host::rpc::handle_fragment_store(req, &app_state).await
                    }
                    IrohRequest::LatencyPing(req) => {
                        IrohResponse::LatencyPong(crate::metrics::rpc::handle_latency_ping(req))
                    }
                    IrohRequest::ThroughputUpload(_) => {
                        IrohResponse::ThroughputAck(crate::metrics::rpc::ThroughputAckResponse)
                    }
                    IrohRequest::StorageQuery(_) => {
                        crate::metrics::rpc::handle_storage_query(&app_state).await
                    }
                    IrohRequest::JoinDeliver(req) => {
                        match crate::setup::process_join_info(&app_state, req.join_info).await {
                            Ok(()) => IrohResponse::JoinAck(crate::setup::JoinAckResponse {
                                success: true,
                            }),
                            Err(e) => IrohResponse::Error {
                                message: format!("join failed: {}", e),
                            },
                        }
                    }
                    // Malachite-engine traffic → the consensus shell (and the
                    // decided-block store for sync serving). "Not active"
                    // until spawn_engine installs the handle (pre-setup).
                    req @ (IrohRequest::ConsensusMsg(_) | IrohRequest::DecidedFetch { .. }) => {
                        match app_state.malachite.get() {
                            Some(engine) => {
                                let server = crate::consensus::malachite::gossip::ConsensusServer {
                                    input_tx: engine.input_tx.clone(),
                                    db_pool: app_state.db_pool.clone(),
                                    barriers: app_state
                                        .test_mode
                                        .then(|| app_state.consensus_barriers.clone()),
                                };
                                server.serve(peer_node_id, req).await
                            }
                            None => IrohResponse::Error {
                                message: "malachite engine not active".into(),
                            },
                        }
                    }
                };

                encode_message(&response)
            })
            .await?;

        // Send cached/computed response bytes
        send_raw(&mut send, response_bytes).await?;
        send.finish()
            .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;

        Ok(())
    }
}
