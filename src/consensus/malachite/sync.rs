//! Decided-value sync client: brings a lagging node to the mesh tip by
//! fetching (block, certificate) pairs from peers and feeding them to the
//! consensus shell, which verifies each certificate and applies each block
//! through the SAME atomic decide path as live consensus.
//!
//! Replaces `catch_up_state` for the new engine. Trust model: certificates
//! are verified by the ENGINE against the validator set (a lying peer cannot
//! forge history without quorum keys); this client only performs structural
//! checks and peer rotation.

use std::time::Duration;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use tokio::sync::{mpsc, watch};

use hopnet_comms::{IrohComms, PeerRef, Rpc};
use hopnet_consensus::codec::{self, WireCommitCertificate};
use hopnet_consensus::shell::HostInput;
use hopnet_consensus::types::Block;

use super::gossip::{ConsensusNetRequest, ConsensusNetResponse};

/// Heights fetched per request (server caps at 100).
const CHUNK: u64 = 50;
/// Stream-level timeout per fetch.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// How long we wait for the shell to apply a fed chunk before suspecting a
/// bad peer (certificate rejected, etc.).
const APPLY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum SyncError {
    /// No peer could serve the range.
    Exhausted { reached: u64, target: u64 },
    /// The consensus shell is gone.
    ShellStopped,
    /// A peer refused with a NEWER epoch (RFC-019 S7): this node's decided
    /// history is from a sealed epoch and no amount of block sync helps —
    /// the caller must pivot into the epoch-join path. Surfaced on the
    /// FIRST such refusal: one claim suffices because the join verifies
    /// everything cryptographically before acting on it.
    EpochAhead { peer: i32, peer_epoch: u64 },
}

/// A single fetch's failure, classified: transport/malformed strikes count
/// against the peer; an epoch-ahead refusal is a signpost, not a strike.
#[derive(Debug)]
enum FetchError {
    Transport(String),
    EpochMismatch { peer_epoch: u64 },
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Transport(m) => write!(f, "{m}"),
            FetchError::EpochMismatch { peer_epoch } => {
                write!(f, "epoch mismatch: peer is on epoch {peer_epoch}")
            }
        }
    }
}

/// Sync until `decided` reaches `target`. Tries `hint_peer` first, then
/// rotates through all known peers; a peer whose data fails structural checks
/// or does not apply is skipped for the remainder of this sync.
// A sync driver threading every seam it touches; a params struct would just
// relocate the same eight names.
#[allow(clippy::too_many_arguments)]
pub async fn sync_to_target(
    comms: &IrohComms,
    epoch: u64,
    db_pool: &Pool<SqliteConnectionManager>,
    my_node_id: i32,
    input_tx: &mpsc::Sender<HostInput>,
    decided: &mut watch::Receiver<u64>,
    target: u64,
    hint_peer: Option<i32>,
    evidence: Option<std::sync::Arc<crate::consensus::evidence::EvidenceMap>>,
) -> Result<(), SyncError> {
    let peers = peer_list(db_pool, my_node_id, hint_peer);
    sync_loop(comms, epoch, input_tx, decided, Some(target), &peers, evidence).await?;
    Ok(())
}

/// Sync to the mesh TIP when the target height is unknown (join bootstrap):
/// keep fetching chunks until every peer answers "nothing more". Returns the
/// height reached. `peers` is explicit — a joining node's `nodes` table only
/// fills as blocks apply, so callers pass JoinInfo's bootstrap validators.
pub async fn sync_to_tip(
    comms: &IrohComms,
    epoch: u64,
    input_tx: &mpsc::Sender<HostInput>,
    decided: &mut watch::Receiver<u64>,
    peers: &[PeerRef],
    evidence: Option<std::sync::Arc<crate::consensus::evidence::EvidenceMap>>,
) -> Result<u64, SyncError> {
    sync_loop(comms, epoch, input_tx, decided, None, peers, evidence).await
}

/// Shared fetch/feed/apply loop. With `target = Some(h)`: sync until decided
/// reaches h, error out when every peer failed for the current position. With
/// `target = None`: tip mode — every peer EXPLICITLY reporting an empty range
/// is success; transport failures never count as tip evidence.
async fn sync_loop(
    comms: &IrohComms,
    epoch: u64,
    input_tx: &mpsc::Sender<HostInput>,
    decided: &mut watch::Receiver<u64>,
    target: Option<u64>,
    peers: &[PeerRef],
    evidence: Option<std::sync::Arc<crate::consensus::evidence::EvidenceMap>>,
) -> Result<u64, SyncError> {
    let mut cursor = 0usize;
    // Peers that failed (transport error, malformed data, chunk didn't apply).
    let mut failures = 0usize;
    // Peers that answered "nothing past this height" (tip evidence).
    let mut empty_answers = 0usize;

    loop {
        if let Some(t) = target
            && *decided.borrow() >= t
        {
            return Ok(*decided.borrow());
        }
        let reached = *decided.borrow();
        if target.is_none() && !peers.is_empty() && empty_answers >= peers.len() {
            return Ok(reached); // tip: everyone agrees there's nothing more
        }
        if peers.is_empty() || failures + empty_answers >= peers.len() {
            // Couldn't advance to `target` this pass. Returning Err lets the
            // driver retry on the next SyncNeeded — important when the target
            // came from an in-flight vote for a height peers haven't decided
            // YET (they will shortly; a premature Ok would stop retrying and
            // strand the node one height short).
            return match target {
                None if failures == 0 => Ok(reached),
                None => Err(SyncError::Exhausted {
                    reached,
                    target: reached,
                }),
                Some(target) => Err(SyncError::Exhausted { reached, target }),
            };
        }
        let peer = peers[cursor % peers.len()];
        cursor += 1;

        let from = *decided.borrow() + 1;
        let to = match target {
            Some(t) => (from + CHUNK - 1).min(t),
            None => from + CHUNK - 1,
        };
        match fetch_chunk(comms, epoch, &peer, from, to).await {
            Ok(pairs) if !pairs.is_empty() => {
                // Reachability evidence (RFC-CONSENSUS-002): the peer served
                // us an authenticated chunk.
                if let Some(ref ev) = evidence {
                    ev.record_contact(peer.node_id);
                }
                let mut last_fed = *decided.borrow();
                // Heights must arrive contiguous from `from` — a gap means
                // the peer served a chunk we cannot apply in order.
                for (expected, (block, cert)) in (from..).zip(pairs) {
                    // Structural checks; certificate verification is the
                    // engine's job.
                    if block.data.height != expected
                        || block.verify().is_err()
                        || block.block_hash != cert.value_id
                    {
                        tracing::warn!("sync: node {} served malformed chunk", peer.node_id);
                        break;
                    }
                    last_fed = block.data.height;
                    if input_tx
                        .send(HostInput::SyncValue {
                            peer_node: peer.node_id,
                            block,
                            cert,
                        })
                        .await
                        .is_err()
                    {
                        return Err(SyncError::ShellStopped);
                    }
                }

                // Wait for the shell to apply what we fed.
                let applied = tokio::time::timeout(APPLY_TIMEOUT, async {
                    while *decided.borrow() < last_fed {
                        if decided.changed().await.is_err() {
                            return Err(SyncError::ShellStopped);
                        }
                    }
                    Ok(())
                })
                .await;
                match applied {
                    Ok(Err(e)) => return Err(e),
                    Ok(Ok(())) => {
                        // Progress — reset both strike counts.
                        failures = 0;
                        empty_answers = 0;
                    }
                    Err(_) => {
                        tracing::warn!(
                            "sync: chunk from node {} did not apply (at {}, fed to {last_fed})",
                            peer.node_id,
                            *decided.borrow()
                        );
                        failures += 1;
                    }
                }
            }
            Ok(_) => {
                tracing::debug!("sync: node {} has nothing for [{from}, {to}]", peer.node_id);
                empty_answers += 1;
            }
            // A peer on a NEWER epoch: block sync is over — the caller
            // pivots into the epoch-join path (RFC-019 S7).
            Err(FetchError::EpochMismatch { peer_epoch }) if peer_epoch > epoch => {
                tracing::info!(
                    peer = peer.node_id,
                    peer_epoch,
                    local_epoch = epoch,
                    "sync: peer answered from a newer epoch (epoch join needed)"
                );
                return Err(SyncError::EpochAhead {
                    peer: peer.node_id,
                    peer_epoch,
                });
            }
            // A peer BEHIND us (it is the straggler) counts like any
            // other unusable answer.
            Err(e) => {
                tracing::debug!("sync: fetch from node {} failed: {e}", peer.node_id);
                failures += 1;
            }
        }
    }
}

/// Fetch the genesis (block, certificate) pair from a bootstrap peer. The
/// synthetic genesis certificate carries no signatures — it is TRUSTED by
/// construction (there is no validator set before the genesis transaction
/// creates one); only structural checks apply. Everything after height 0 is
/// engine-verified against the validator sets genesis establishes.
pub async fn fetch_genesis(
    comms: &IrohComms,
    epoch: u64,
    peers: &[PeerRef],
) -> Result<(Block, WireCommitCertificate), String> {
    let mut last_err = "no bootstrap peers".to_string();
    for peer in peers {
        match fetch_chunk(comms, epoch, peer, 0, 0).await {
            Ok(pairs) => {
                let Some((block, cert)) = pairs.into_iter().next() else {
                    last_err = format!("node {} has no genesis", peer.node_id);
                    continue;
                };
                if block.data.height != 0
                    || block.data.parent_hash.is_some()
                    || block.verify().is_err()
                    || cert.height != 0
                    || cert.value_id != block.block_hash
                {
                    last_err = format!("node {} served a malformed genesis", peer.node_id);
                    continue;
                }
                return Ok((block, cert));
            }
            Err(e) => last_err = format!("node {}: {e}", peer.node_id),
        }
    }
    Err(last_err)
}

pub(crate) fn peer_list(
    db_pool: &Pool<SqliteConnectionManager>,
    my_node_id: i32,
    hint: Option<i32>,
) -> Vec<PeerRef> {
    let Ok(conn) = db_pool.get() else {
        return Vec::new();
    };
    let Ok(mut stmt) = conn.prepare_cached("SELECT node_id, pubkey FROM nodes WHERE node_id != ?")
    else {
        return Vec::new();
    };
    let Ok(rows) = stmt.query_map([my_node_id], |row| {
        let node_id: i32 = row.get(0)?;
        let pubkey: crate::db::PubKey = row.get(1)?;
        Ok(PeerRef {
            node_id,
            pubkey: pubkey.0.to_bytes(),
        })
    }) else {
        return Vec::new();
    };
    let mut peers: Vec<PeerRef> = rows.flatten().collect();
    // Hint peer first (it demonstrably has the target height).
    if let Some(h) = hint {
        peers.sort_by_key(|p| (p.node_id != h) as u8);
    }
    peers
}

async fn fetch_chunk(
    comms: &IrohComms,
    epoch: u64,
    peer: &PeerRef,
    from: u64,
    to: u64,
) -> Result<Vec<(Block, WireCommitCertificate)>, FetchError> {
    let payload = crate::net::encode_payload(&ConsensusNetRequest::DecidedFetch {
        from_height: from,
        to_height: to,
        epoch,
    });
    let reply = comms
        .rpc(peer, "consensus", payload, FETCH_TIMEOUT)
        .await
        .map_err(|e| FetchError::Transport(e.to_string()))?;
    parse_fetch_reply(&reply)
}

/// Decode and classify a decided-fetch reply. Split from the transport so
/// the classification — the epoch-join signpost vs an ordinary strike —
/// is testable without a mesh.
fn parse_fetch_reply(reply: &[u8]) -> Result<Vec<(Block, WireCommitCertificate)>, FetchError> {
    let response: ConsensusNetResponse =
        crate::net::decode_payload(reply).map_err(|e| FetchError::Transport(e.to_string()))?;

    match response {
        ConsensusNetResponse::Decided { items } => {
            let mut out = Vec::with_capacity(items.len());
            for (block_bytes, cert_bytes) in &items {
                let block: Block =
                    codec::decode(block_bytes).map_err(|e| FetchError::Transport(e.to_string()))?;
                let cert: WireCommitCertificate =
                    codec::decode(cert_bytes).map_err(|e| FetchError::Transport(e.to_string()))?;
                out.push((block, cert));
            }
            Ok(out)
        }
        ConsensusNetResponse::EpochMismatch { local_epoch } => Err(FetchError::EpochMismatch {
            peer_epoch: local_epoch,
        }),
        ConsensusNetResponse::Error { message } => Err(FetchError::Transport(message)),
        other => Err(FetchError::Transport(format!("unexpected response: {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the classification is the whole point of the structured
    // refusal — an epoch-ahead peer must read as "pivot to epoch join",
    // never as one more transport strike that exhausts the peer list.
    // Should: classify an EpochMismatch reply as the epoch signpost and
    // a bare Error reply as a transport failure.
    #[test]
    fn fetch_reply_classification() {
        let mismatch = crate::net::encode_payload(&ConsensusNetResponse::EpochMismatch {
            local_epoch: 3,
        });
        match parse_fetch_reply(&mismatch) {
            Err(FetchError::EpochMismatch { peer_epoch: 3 }) => {}
            other => panic!("expected epoch signpost, got {other:?}"),
        }

        let error = crate::net::encode_payload(&ConsensusNetResponse::Error {
            message: "db pool exhausted".into(),
        });
        match parse_fetch_reply(&error) {
            Err(FetchError::Transport(m)) => assert!(m.contains("db pool")),
            other => panic!("expected transport strike, got {other:?}"),
        }

        let empty = crate::net::encode_payload(&ConsensusNetResponse::Decided { items: vec![] });
        assert!(matches!(parse_fetch_reply(&empty), Ok(ref v) if v.is_empty()));
    }
}
