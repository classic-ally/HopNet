//! Consensus gossip over iroh: the outbound publisher (production send path)
//! and the standalone accept loop (Stage-4 test harness; folds into
//! `net::handler` dispatch at the Stage-5 cutover).

use std::time::Duration;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use tokio::sync::mpsc;

use hopnet_consensus::codec::{self, WireConsensusMsg, WireVoteType};
use hopnet_consensus::context::Height;
use hopnet_consensus::shell::HostInput;
use hopnet_consensus::store;

use crate::consensus::barriers::names as barrier_names;
use crate::db::PubKey;
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::net::transport::{IrohTransport, encode_message, recv_message, send_raw};
use crate::AppState;

/// Per-publish send timeout (matches the old broadcast paths' 3s).
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(3);

/// Largest decided-range chunk the fetch server returns per request.
const DECIDED_FETCH_MAX: i64 = 100;

/// All known peers (node_id, pubkey), excluding ourselves.
///
/// TODO(stage-5): restrict to the validator set at the current height; today
/// every node is a validator so the sets coincide.
fn peers(
    db_pool: &Pool<SqliteConnectionManager>,
    my_node_id: i32,
) -> Result<Vec<(i32, PubKey)>, String> {
    // NB: a pool checkout failure here used to be masked as
    // rusqlite::Error::InvalidQuery ("Query is not read-only") — keep the
    // real error visible; pool starvation under API load is a live failure
    // mode and the publisher's cache is what rides it out.
    let conn = db_pool.get().map_err(|e| format!("db pool: {e}"))?;
    let mut stmt = conn
        .prepare_cached("SELECT node_id, pubkey FROM nodes WHERE node_id != ?")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([my_node_id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
}

/// How long the publisher trusts its cached peer list before re-reading the
/// nodes table. Node membership changes only via decided blocks, so staleness
/// here costs at most a few seconds of gossip to a brand-new node — which
/// catches up through decided-value sync anyway.
const PEER_CACHE_TTL: Duration = Duration::from_secs(5);

/// Long-lived publisher: drains the shell's outbound channel and fire-and-
/// forgets each message to every peer (spawn-per-peer, matching the bespoke
/// engine's broadcast shape). Returns when the channel closes.
///
/// Under test_mode this is a barrier tap: full-value proposals hold at
/// `before_publish_proposal`, precommit votes at `before_publish_precommit`
/// — holding here pauses the node's OUTBOUND consensus effect without
/// touching the engine.
pub async fn run_publisher(
    app_state: AppState,
    my_node_id: i32,
    mut outbound: mpsc::UnboundedReceiver<WireConsensusMsg>,
) {
    let transport: IrohTransport = app_state.iroh_transport.clone();
    let db_pool: Pool<SqliteConnectionManager> = app_state.db_pool.clone();
    // Peer-list cache: consensus publishing must not depend on winning a pool
    // checkout race against API load. The blocking r2d2 get() runs on the
    // blocking pool (not an async worker), at most once per TTL; on refresh
    // failure a non-empty cache keeps publishing with the stale list.
    let mut peer_cache: Vec<(i32, PubKey)> = Vec::new();
    let mut last_refresh: Option<tokio::time::Instant> = None;
    while let Some(msg) = outbound.recv().await {
        if app_state.test_mode {
            match &msg {
                WireConsensusMsg::ProposedValue(_) => {
                    app_state
                        .consensus_barriers
                        .wait(barrier_names::BEFORE_PUBLISH_PROPOSAL)
                        .await;
                }
                WireConsensusMsg::Vote(v) if matches!(v.typ, WireVoteType::Precommit) => {
                    app_state
                        .consensus_barriers
                        .wait(barrier_names::BEFORE_PUBLISH_PRECOMMIT)
                        .await;
                }
                _ => {}
            }
        }
        let bytes = match codec::encode(&msg) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("consensus msg encode failed (dropped): {e}");
                continue;
            }
        };
        let stale = last_refresh.is_none_or(|t| t.elapsed() > PEER_CACHE_TTL);
        if peer_cache.is_empty() || stale {
            let pool = db_pool.clone();
            match tokio::task::spawn_blocking(move || peers(&pool, my_node_id)).await {
                Ok(Ok(p)) => {
                    peer_cache = p;
                    last_refresh = Some(tokio::time::Instant::now());
                }
                Ok(Err(e)) if peer_cache.is_empty() => {
                    tracing::warn!("peer enumeration failed, dropping publish: {e}");
                    continue;
                }
                Ok(Err(e)) => {
                    tracing::warn!("peer refresh failed, publishing with cached list: {e}");
                }
                Err(e) => {
                    tracing::error!("peer refresh task died: {e}");
                    if peer_cache.is_empty() {
                        continue;
                    }
                }
            }
        }
        for (node_id, pubkey) in &peer_cache {
            let node_id = *node_id;
            let transport = transport.clone();
            let req = IrohRequest::ConsensusMsg(bytes.clone());
            let iroh_id = pubkey.to_iroh_node_id();
            tokio::spawn(async move {
                if let Err(e) = transport
                    .request(node_id, iroh_id, &req, PUBLISH_TIMEOUT)
                    .await
                {
                    tracing::debug!("consensus publish to node {node_id} failed: {e}");
                }
            });
        }
    }
}

/// What the accept loop needs to serve consensus traffic.
#[derive(Clone)]
pub struct ConsensusServer {
    pub input_tx: mpsc::Sender<HostInput>,
    pub db_pool: Pool<SqliteConnectionManager>,
    /// Barrier tap for sync serving (`before_sync_response`), set under
    /// test_mode only.
    pub barriers: Option<std::sync::Arc<crate::barriers::Barriers>>,
}

impl ConsensusServer {
    fn lookup_node_id(&self, peer_pubkey: &iroh::PublicKey) -> Option<i32> {
        let conn = self.db_pool.get().ok()?;
        let pubkey = PubKey(ed25519_dalek::VerifyingKey::from_bytes(peer_pubkey.as_bytes()).ok()?);
        let encoded =
            bincode::serde::encode_to_vec(pubkey, bincode::config::standard()).ok()?;
        conn.query_row(
            "SELECT node_id FROM nodes WHERE pubkey = ?",
            [encoded.as_slice()],
            |row| row.get(0),
        )
        .ok()
    }

    /// Serve one already-decoded request (also the Stage-5 dispatch target).
    pub async fn serve(&self, from_node: i32, request: IrohRequest) -> IrohResponse {
        match request {
            IrohRequest::ConsensusMsg(bytes) => {
                match codec::decode::<WireConsensusMsg>(&bytes) {
                    Ok(msg) => {
                        if self
                            .input_tx
                            .send(HostInput::Wire {
                                from: from_node,
                                msg,
                            })
                            .await
                            .is_err()
                        {
                            return IrohResponse::Error {
                                message: "consensus shell stopped".into(),
                            };
                        }
                        IrohResponse::ConsensusMsgAck
                    }
                    Err(e) => IrohResponse::Error {
                        message: format!("bad consensus msg: {e}"),
                    },
                }
            }
            IrohRequest::DecidedFetch {
                from_height,
                to_height,
            } => {
                if let Some(barriers) = &self.barriers {
                    barriers.wait(barrier_names::BEFORE_SYNC_RESPONSE).await;
                }
                let to = to_height.min(from_height.saturating_add(DECIDED_FETCH_MAX - 1));
                if from_height < 0 || to < from_height {
                    return IrohResponse::Error {
                        message: "bad height range".into(),
                    };
                }
                let conn = match self.db_pool.get() {
                    Ok(c) => c,
                    Err(_) => {
                        return IrohResponse::Error {
                            message: "db pool exhausted".into(),
                        }
                    }
                };
                match store::decided_range(&conn, Height::from_db(from_height), Height::from_db(to))
                {
                    Ok(pairs) => {
                        let mut items = Vec::with_capacity(pairs.len());
                        for (block, cert) in &pairs {
                            match (codec::encode(block), codec::encode(cert)) {
                                (Ok(b), Ok(c)) => items.push((b, c)),
                                _ => break,
                            }
                        }
                        IrohResponse::DecidedFetchResponse { items }
                    }
                    Err(e) => IrohResponse::Error {
                        message: format!("decided fetch failed: {e}"),
                    },
                }
            }
            _ => IrohResponse::Error {
                message: "not a consensus request".into(),
            },
        }
    }
}

/// Standalone accept loop for the consensus ALPN traffic. Stage-4 only: the
/// integration test binds nodes with this; at Stage 5 `net::handler` gains
/// these arms and this loop dies.
pub async fn run_accept_loop(endpoint: iroh::Endpoint, server: ConsensusServer) {
    loop {
        match endpoint.accept().await {
            Some(incoming) => {
                let server = server.clone();
                tokio::spawn(async move {
                    let conn = match incoming.await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::debug!("consensus accept failed: {e}");
                            return;
                        }
                    };
                    let from_node = server.lookup_node_id(&conn.remote_id()).unwrap_or(-1);
                    loop {
                        let (send, recv) = match conn.accept_bi().await {
                            Ok(s) => s,
                            Err(_) => break,
                        };
                        let server = server.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_stream(send, recv, from_node, server).await {
                                tracing::debug!("consensus stream error: {e}");
                            }
                        });
                    }
                });
            }
            None => break,
        }
    }
}

async fn handle_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    from_node: i32,
    server: ConsensusServer,
) -> Result<(), crate::net::transport::IrohError> {
    use tokio::io::AsyncReadExt;
    // Same wire shape as the main handler: [8B request_id][4B len][bincode].
    let mut id_buf = [0u8; 8];
    recv.read_exact(&mut id_buf).await.map_err(|e| {
        crate::net::transport::IrohError::Transport(
            crate::net::transport::TransportError::StreamFailed(e.to_string()),
        )
    })?;
    let request: IrohRequest = recv_message(&mut recv).await?;
    let response = server.serve(from_node, request).await;
    let bytes = encode_message(&response)?;
    send_raw(&mut send, &bytes).await?;
    send.finish().map_err(|e| {
        crate::net::transport::IrohError::Transport(
            crate::net::transport::TransportError::StreamFailed(e.to_string()),
        )
    })?;
    Ok(())
}
