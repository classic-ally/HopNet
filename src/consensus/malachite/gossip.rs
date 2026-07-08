//! Consensus gossip over the comms transport: the "consensus" scope's wire
//! vocabulary (this module owns the payload codec; `net::scopes` serves it)
//! and the outbound publisher (production send path).

use std::time::Duration;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use hopnet_comms::{Broadcast, PeerRef};
use hopnet_consensus::codec::{self, WireConsensusMsg, WireVoteType};

use crate::consensus::barriers::names as barrier_names;
use crate::AppState;

/// Wire request for the "consensus" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusNetRequest {
    /// Malachite consensus gossip: a bincode-encoded
    /// `hopnet_consensus::codec::WireConsensusMsg`. Fire-and-forget (ack only).
    Gossip(Vec<u8>),
    /// Fetch decided (block, certificate) pairs for `[from_height, to_height]`
    /// — the decided-value sync protocol.
    DecidedFetch { from_height: i64, to_height: i64 },
}

/// Wire response for the "consensus" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusNetResponse {
    /// Ack for Gossip (the publish is fire-and-forget)
    Ack,
    /// Decided (block bytes, certificate bytes) pairs, ascending and
    /// contiguous from `from_height` (bincode-encoded engine types)
    Decided { items: Vec<(Vec<u8>, Vec<u8>)> },
    Error { message: String },
}

/// Per-publish send timeout (matches the old broadcast paths' 3s).
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(3);

/// All known peers, excluding ourselves.
///
/// TODO(stage-5): restrict to the validator set at the current height; today
/// every node is a validator so the sets coincide.
fn peers(
    db_pool: &Pool<SqliteConnectionManager>,
    my_node_id: i32,
) -> Result<Vec<PeerRef>, String> {
    // NB: a pool checkout failure here used to be masked as
    // rusqlite::Error::InvalidQuery ("Query is not read-only") — keep the
    // real error visible; pool starvation under API load is a live failure
    // mode and the publisher's cache is what rides it out.
    let conn = db_pool.get().map_err(|e| format!("db pool: {e}"))?;
    let mut stmt = conn
        .prepare_cached("SELECT node_id, pubkey FROM nodes WHERE node_id != ?")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([my_node_id], |row| {
            let node_id: i32 = row.get(0)?;
            let pubkey: crate::db::PubKey = row.get(1)?;
            Ok(PeerRef {
                node_id,
                pubkey: pubkey.0.to_bytes(),
            })
        })
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
}

/// How long the publisher trusts its cached peer list before re-reading the
/// nodes table. Node membership changes only via decided blocks, so staleness
/// here costs at most a few seconds of gossip to a brand-new node — which
/// catches up through decided-value sync anyway.
const PEER_CACHE_TTL: Duration = Duration::from_secs(5);

/// Long-lived publisher: drains the shell's outbound channel and fire-and-
/// forgets each message to every peer (comms' broadcast spawns one send per
/// peer on the net runtime, matching the bespoke engine's broadcast shape).
/// Returns when the channel closes.
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
    let comms = app_state.comms.clone();
    let db_pool: Pool<SqliteConnectionManager> = app_state.db_pool.clone();
    // Peer-list cache: consensus publishing must not depend on winning a pool
    // checkout race against API load. The blocking r2d2 get() runs on the
    // blocking pool (not an async worker), at most once per TTL; on refresh
    // failure a non-empty cache keeps publishing with the stale list.
    let mut peer_cache: Vec<PeerRef> = Vec::new();
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
        let payload = crate::net::encode_payload(&ConsensusNetRequest::Gossip(bytes));
        comms.broadcast(&peer_cache, "consensus", payload, PUBLISH_TIMEOUT);
    }
}
