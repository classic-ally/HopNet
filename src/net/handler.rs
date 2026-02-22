use std::sync::Arc;
use std::time::Duration;

use iroh::Endpoint;
use tokio::io::AsyncReadExt;
use tracing::Instrument;

use super::protocol::{IrohRequest, IrohResponse};
use super::transport::{recv_message, encode_message, send_raw, IrohError, TransportError, ProtocolError};
use crate::AppState;
use crate::types::PubKey;
use crate::db::consensus as db;
use crate::consensus::routes::perform_catch_up;

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

/// Message-driven catch-up for consensus messages.
///
/// Before dispatching any consensus message, check if the message's view is ahead of ours.
/// If so, catch up to that view. For Lock-phase ballots, also ensure intra-view sync
/// (we have the Propose QC for the current view).
///
/// This replaces the old HTTP middleware (`ensure_caught_up_and_active`) with a cheaper
/// approach: the incoming message itself tells us where we need to be. Zero overhead on
/// the happy path (one lightweight DB read).
///
/// Lock ordering: catch-up acquires consensus_lock, releases it, then the handler acquires
/// its own lock. The gap is safe — if the view advances further, the handler re-reads state
/// and rejects stale messages via its own verification (e.g., verify_proposal() view-match).
async fn ensure_caught_up_for_message(
    request: &IrohRequest,
    app_state: &AppState,
) -> Result<(), IrohError> {
    let target_view = match request.consensus_view() {
        Some(v) => v,
        None => return Ok(()), // Non-consensus message, no catch-up needed
    };

    // Hold one connection for all lightweight reads (avoids pool contention between phases)
    let conn = app_state.db_pool.get()
        .map_err(|_| IrohError::Protocol(ProtocolError::PeerError("db pool error".into())))?;

    let (our_view, mut highest_qc_view) = db::get_consensus_progress(&conn)
        .map_err(|_| IrohError::Protocol(ProtocolError::PeerError("db error".into())))?;

    // Cross-view catch-up: message is for a future view
    if target_view > our_view {
        tracing::info!("Message-driven catch-up: view {} -> {}", our_view, target_view);
        let _guard = app_state.consensus_lock.lock().await;
        // Re-check after acquiring lock (may have caught up while waiting)
        let (our_view, _) = db::get_consensus_progress(&conn)
            .map_err(|_| IrohError::Protocol(ProtocolError::PeerError("db error".into())))?;
        if target_view > our_view {
            perform_catch_up(app_state, our_view, target_view, None).await
                .map_err(|e| IrohError::Protocol(ProtocolError::PeerError(
                    format!("catch-up failed: {:?}", e)
                )))?;
        }
        // State changed — re-read highest_qc_view for intra-view check below
        (_, highest_qc_view) = db::get_consensus_progress(&conn)
            .map_err(|_| IrohError::Protocol(ProtocolError::PeerError("db error".into())))?;
    }

    // Intra-view catch-up: Lock-phase ballot but we're missing the Propose QC for this view
    if let IrohRequest::BallotSubmission(req) = request {
        if req.ballot.data.phase == crate::consensus::types::ConsensusPhase::Lock
            && highest_qc_view < target_view
        {
            tracing::debug!(
                "Lock ballot intra-view sync: highest_qc_view={}, ballot_view={} - syncing",
                highest_qc_view, target_view
            );
            crate::consensus::routes::ensure_intra_view_synced(app_state).await
                .map_err(|e| IrohError::Protocol(ProtocolError::PeerError(
                    format!("intra-view sync failed: {:?}", e)
                )))?;
        }
    }

    Ok(())
}

async fn handle_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    peer_node_id: i32,
    app_state: AppState,
) -> Result<(), IrohError> {
    // Read request_id prefix (8 bytes)
    let mut id_buf = [0u8; 8];
    recv.read_exact(&mut id_buf).await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
    let request_id = u64::from_le_bytes(id_buf);

    let span = tracing::debug_span!("rpc_req", id = %format!("{:016x}", request_id), from = peer_node_id);
    async {
        // Read the request outside the OnceCell so we can branch on it
        let request: IrohRequest = recv_message(&mut recv).await?;

        // Message-driven catch-up (idempotent — safe outside OnceCell)
        ensure_caught_up_for_message(&request, &app_state).await?;

        // TransactionForward uses two-phase ACK (bypasses OnceCell — nonce table handles dedup)
        if let IrohRequest::TransactionForward(req) = request {
            // Validate leader status before ACKing — avoids multi-hop forwarding
            'reject: {
                let my_node_id = match app_state.get_node_id() {
                    Ok(id) => id,
                    Err(_) => break 'reject,
                };
                let conn = match app_state.db_pool.get() {
                    Ok(c) => c,
                    Err(_) => break 'reject,
                };
                let state = match db::get_consensus_with_conn(&conn) {
                    Ok(s) => s,
                    Err(_) => break 'reject,
                };

                if state.leader.node_id != my_node_id {
                    let reject = encode_message(
                        &IrohResponse::TransactionForwardNotLeader { view: state.view }
                    )?;
                    send_raw(&mut send, &reject).await?;
                    send.finish().map_err(|e|
                        IrohError::Transport(TransportError::StreamFailed(e.to_string()))
                    )?;
                    return Ok(());
                }
                if app_state.consensus_lock.try_lock().is_err() {
                    let reject = encode_message(&IrohResponse::TransactionForwardBusy)?;
                    send_raw(&mut send, &reject).await?;
                    send.finish().map_err(|e|
                        IrohError::Transport(TransportError::StreamFailed(e.to_string()))
                    )?;
                    return Ok(());
                }
            }

            // Phase 1: Send immediate ACK (validated as leader, hasn't proposed)
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
            cache.entry(request_id).or_insert_with(|| Arc::new(tokio::sync::OnceCell::new())).clone()
        };

        // Schedule cleanup after TTL
        let cache = app_state.dedup_cache.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(300)).await;
            cache.lock().unwrap().remove(&request_id);
        });

        // First caller processes; duplicates wait for the same result
        let response_bytes = cell.get_or_try_init(|| async {
            if app_state.test_mode {
                if let IrohRequest::BallotSubmission(_) = &request {
                    app_state.consensus_barriers.wait(
                        crate::consensus::barriers::names::BEFORE_BALLOT_DISPATCH
                    ).await;
                }
            }

            let response = match request {
                IrohRequest::Ping { nonce } => IrohResponse::Pong { nonce },
                IrohRequest::FragmentHealthCheck(req) => {
                    IrohResponse::FragmentHealthCheckResponse(
                        crate::files::rpc::handle_fragment_health_check(req, &app_state.fragments_dir)
                    )
                }
                IrohRequest::ViewDataFetch(req) => {
                    crate::consensus::rpc::handle_view_data_request(req, &app_state)
                }
                IrohRequest::ViewPoll(_) => {
                    crate::consensus::rpc::handle_view_poll_request(&app_state)
                }
                IrohRequest::TimeoutVoteBroadcast(req) => {
                    crate::consensus::rpc::handle_timeout_vote_broadcast(req, &app_state).await
                }
                IrohRequest::TcBroadcast(req) => {
                    crate::consensus::rpc::handle_tc_broadcast(req, &app_state).await
                }
                IrohRequest::QcBroadcast(req) => {
                    crate::consensus::rpc::handle_qc_broadcast(req, &app_state).await
                }
                IrohRequest::BallotSubmission(req) => {
                    crate::consensus::rpc::handle_ballot_request(req, &app_state).await
                }
                IrohRequest::TransactionForward(_) => unreachable!("handled above"),
                IrohRequest::FragmentFetch(req) => {
                    IrohResponse::FragmentFetchResponse(
                        crate::files::rpc::handle_fragment_fetch(req, &app_state.fragments_dir)
                    )
                }
                IrohRequest::FragmentStore(req) => {
                    crate::files::rpc::handle_fragment_store(req, &app_state).await
                }
                IrohRequest::LatencyPing(req) => {
                    IrohResponse::LatencyPong(
                        crate::metrics::rpc::handle_latency_ping(req)
                    )
                }
                IrohRequest::ThroughputUpload(_) => {
                    IrohResponse::ThroughputAck(crate::metrics::rpc::ThroughputAckResponse)
                }
                IrohRequest::StorageQuery(_) => {
                    crate::metrics::rpc::handle_storage_query(&app_state).await
                }
                IrohRequest::JoinDeliver(req) => {
                    match crate::setup::process_join_info(&app_state, req.join_info).await {
                        Ok(()) => IrohResponse::JoinAck(crate::setup::JoinAckResponse { success: true }),
                        Err(e) => IrohResponse::Error { message: format!("join failed: {}", e) },
                    }
                }
            };

            encode_message(&response)
        }).await?;

        // Send cached/computed response bytes
        send_raw(&mut send, response_bytes).await?;
        send.finish()
            .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;

        Ok(())
    }.instrument(span).await
}
