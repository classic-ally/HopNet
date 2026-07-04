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

use hopnet_consensus::codec::{self, WireCommitCertificate};
use hopnet_consensus::shell::HostInput;
use hopnet_consensus::types::Block;

use crate::db::PubKey;
use crate::net::protocol::{IrohRequest, IrohResponse};
use crate::net::transport::IrohTransport;

/// Heights fetched per request (server caps at 100).
const CHUNK: i64 = 50;
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
}

/// Sync until `decided` reaches `target`. Tries `hint_peer` first, then
/// rotates through all known peers; a peer whose data fails structural checks
/// or does not apply is skipped for the remainder of this sync.
pub async fn sync_to_target(
    transport: &IrohTransport,
    db_pool: &Pool<SqliteConnectionManager>,
    my_node_id: i32,
    input_tx: &mpsc::Sender<HostInput>,
    decided: &mut watch::Receiver<u64>,
    target: u64,
    hint_peer: Option<i32>,
) -> Result<(), SyncError> {
    let peers = peer_list(db_pool, my_node_id, hint_peer);
    let mut cursor = 0usize;
    let mut failures = 0usize;

    while *decided.borrow() < target {
        if peers.is_empty() || failures >= peers.len() {
            return Err(SyncError::Exhausted {
                reached: *decided.borrow(),
                target,
            });
        }
        let (peer_node, peer_key) = peers[cursor % peers.len()].clone();
        cursor += 1;

        let from = (*decided.borrow() as i64) + 1;
        let to = (from + CHUNK - 1).min(target as i64);
        match fetch_chunk(transport, peer_node, &peer_key, from, to).await {
            Ok(pairs) if !pairs.is_empty() => {
                let mut last_fed = *decided.borrow();
                let mut expected = from as u64;
                for (block, cert) in pairs {
                    // Structural checks; certificate verification is the
                    // engine's job.
                    if block.data.height != expected
                        || block.verify().is_err()
                        || block.block_hash != cert.value_id
                    {
                        tracing::warn!("sync: node {peer_node} served malformed chunk");
                        break;
                    }
                    expected += 1;
                    last_fed = block.data.height;
                    if input_tx
                        .send(HostInput::SyncValue {
                            peer_node,
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
                    Ok(Ok(())) => failures = 0, // progress — reset the strike count
                    Err(_) => {
                        tracing::warn!(
                            "sync: chunk from node {peer_node} did not apply (at {}, fed to {last_fed})",
                            *decided.borrow()
                        );
                        failures += 1;
                    }
                }
            }
            Ok(_) => {
                tracing::debug!("sync: node {peer_node} has nothing for [{from}, {to}]");
                failures += 1;
            }
            Err(e) => {
                tracing::debug!("sync: fetch from node {peer_node} failed: {e}");
                failures += 1;
            }
        }
    }
    Ok(())
}

fn peer_list(
    db_pool: &Pool<SqliteConnectionManager>,
    my_node_id: i32,
    hint: Option<i32>,
) -> Vec<(i32, PubKey)> {
    let Ok(conn) = db_pool.get() else {
        return Vec::new();
    };
    let Ok(mut stmt) = conn.prepare_cached("SELECT node_id, pubkey FROM nodes WHERE node_id != ?")
    else {
        return Vec::new();
    };
    let Ok(rows) = stmt.query_map([my_node_id], |row| {
        Ok((row.get::<_, i32>(0)?, row.get::<_, PubKey>(1)?))
    }) else {
        return Vec::new();
    };
    let mut peers: Vec<(i32, PubKey)> = rows.flatten().collect();
    // Hint peer first (it demonstrably has the target height).
    if let Some(h) = hint {
        peers.sort_by_key(|(id, _)| (*id != h) as u8);
    }
    peers
}

async fn fetch_chunk(
    transport: &IrohTransport,
    peer_node: i32,
    peer_key: &PubKey,
    from: i64,
    to: i64,
) -> Result<Vec<(Block, WireCommitCertificate)>, String> {
    let response = transport
        .request(
            peer_node,
            peer_key.to_iroh_node_id(),
            &IrohRequest::DecidedFetch {
                from_height: from,
                to_height: to,
            },
            FETCH_TIMEOUT,
        )
        .await
        .map_err(|e| e.to_string())?;

    match response {
        IrohResponse::DecidedFetchResponse { items } => {
            let mut out = Vec::with_capacity(items.len());
            for (block_bytes, cert_bytes) in &items {
                let block: Block = codec::decode(block_bytes).map_err(|e| e.to_string())?;
                let cert: WireCommitCertificate =
                    codec::decode(cert_bytes).map_err(|e| e.to_string())?;
                out.push((block, cert));
            }
            Ok(out)
        }
        IrohResponse::Error { message } => Err(message),
        other => Err(format!("unexpected response: {other:?}")),
    }
}
