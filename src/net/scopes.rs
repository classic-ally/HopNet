//! Server-side handlers for the host's comms scopes, plus the registry
//! constructor shared by `main.rs` and the in-process integration tests.
//!
//! SPAWN POLICY (load-bearing): comms invokes handlers INLINE on its net
//! runtime's stream tasks, and that runtime must never block (no r2d2, no
//! rusqlite, no sync I/O — see `hopnet_comms::net_rt`). Each handler routes
//! its work by plane:
//!
//! - mesh plane (pure channel/CPU work: consensus gossip intake, latency and
//!   throughput echoes) is served inline on the net runtime;
//! - consensus-support plane (TransactionForward, DecidedFetch, the
//!   regenesis scope) hops to the QUEUE runtime (blocking DB allowed, never
//!   starved by API load). TransactionForward is consensus INTAKE: if its
//!   ACK can't beat the forwarder's timeout under load, proposers never
//!   receive batches and blocks decide empty (the image-12 finding).
//!   DecidedFetch serves laggard sync — same liveness class. The regenesis
//!   scope serves epoch rejoin (lineage + snapshot artifact, RFC-019 S7)
//!   and, like DecidedFetch, answers WITHOUT a live engine — parked and
//!   sealed nodes rescuing stragglers is load-bearing;
//! - app plane (fragments, storage query, join) hops to the MAIN runtime and
//!   degrades under API overload by design.

use std::sync::Arc;

use hopnet_comms::{BoxFuture, FrameSink, PeerRef, RpcHandler, ScopeRegistry, StreamHandler};

use hopnet_consensus::codec::{self, WireConsensusMsg};
use hopnet_consensus::context::Height;
use hopnet_consensus::shell::HostInput;
use hopnet_consensus::store;

use super::{decode_payload, encode_payload};
use crate::AppState;
use crate::consensus::barriers::names as barrier_names;
use crate::consensus::malachite::gossip::{ConsensusNetRequest, ConsensusNetResponse};
use crate::consensus::rpc::{ForwardReply, TransactionForwardRequest};
use crate::metrics::rpc::{MetricsRequest, MetricsResponse};
use crate::setup::{SetupRequest, SetupResponse};

/// The host's full scope map — one construction shared by `main.rs` and the
/// integration tests so the registries cannot drift.
pub fn build_registry(app_state: &AppState) -> ScopeRegistry {
    let mut scopes = ScopeRegistry::new();
    scopes.rpc(
        "consensus",
        Arc::new(ConsensusScope {
            app_state: app_state.clone(),
        }),
    );
    scopes.streamed(
        "txforward",
        Arc::new(TxForwardScope {
            app_state: app_state.clone(),
        }),
    );
    scopes.rpc(
        "storage",
        Arc::new(StorageScope {
            app_state: app_state.clone(),
        }),
    );
    scopes.rpc(
        "metrics",
        Arc::new(MetricsScope {
            app_state: app_state.clone(),
        }),
    );
    scopes.rpc(
        "setup",
        Arc::new(SetupScope {
            app_state: app_state.clone(),
        }),
    );
    // The compat class (RFC-025 §Scope Classes) — an allowlist, not a
    // default: each admission has a named cross-version consumer. status:
    // diagnosing any mismatched peer (the Pong is the policy readout);
    // regenesis: RFC-019 S7 stragglers staging on the old binary. The
    // class-pin test below is the table's enforcement.
    scopes.rpc_compat(
        "status",
        Arc::new(crate::consensus::evidence::StatusScope {
            app_state: app_state.clone(),
        }),
    );
    scopes.rpc_compat(
        "regenesis",
        Arc::new(crate::regenesis::rpc::RegenesisScope {
            app_state: app_state.clone(),
        }),
    );
    scopes
}

// ============================================================================
// "consensus" — gossip intake (inline) + decided-value sync serving (queue rt)
// ============================================================================

/// Largest decided-range chunk the fetch server returns per request.
const DECIDED_FETCH_MAX: u64 = 100;

pub struct ConsensusScope {
    pub(crate) app_state: AppState,
}

impl ConsensusScope {
    pub(crate) async fn serve(&self, peer: PeerRef, payload: Vec<u8>) -> ConsensusNetResponse {
        let request: ConsensusNetRequest = match decode_payload(&payload) {
            Ok(r) => r,
            Err(e) => {
                return ConsensusNetResponse::Error {
                    message: format!("bad consensus request: {e}"),
                };
            }
        };
        // Reachability evidence (RFC-CONSENSUS-002 S3): an authenticated,
        // well-formed exchange from this peer — covers gossip (votes
        // received first-hand) and decided-fetch serving.
        self.app_state.evidence.record_contact(peer.node_id);
        match request {
            // Mesh plane: pure channel work — INLINE on the net runtime.
            // Needs the live engine ("not active" until spawn_engine
            // installs the handle — pre-setup, or parked on a seal).
            ConsensusNetRequest::Gossip(bytes) => {
                let Some(engine) = self.app_state.malachite.get() else {
                    return ConsensusNetResponse::Error {
                        message: "malachite engine not active".into(),
                    };
                };
                match codec::decode::<WireConsensusMsg>(&bytes) {
                    Ok(msg) => {
                        if engine
                            .input_tx
                            .send(HostInput::Wire {
                                from: peer.node_id,
                                msg,
                            })
                            .await
                            .is_err()
                        {
                            return ConsensusNetResponse::Error {
                                message: "consensus shell stopped".into(),
                            };
                        }
                        ConsensusNetResponse::Ack
                    }
                    Err(e) => ConsensusNetResponse::Error {
                        message: format!("bad consensus msg: {e}"),
                    },
                }
            }
            // Consensus-support plane: blocking DB read — the QUEUE runtime.
            // Deliberately served WITHOUT a live engine: decided history
            // is a DB fact, and a sealed/parked node answering laggards
            // their final blocks is load-bearing for rejoin (RFC-019).
            ConsensusNetRequest::DecidedFetch {
                from_height,
                to_height,
                epoch,
            } => {
                // Epoch gate (RFC-019 S6 handshake): decided history is
                // only meaningful within one epoch — a cross-epoch
                // requester needs the lineage record, not blocks. The
                // structured refusal is the requester's signpost into the
                // epoch-join path (S7); the "regenesis" scope answers it.
                let local_epoch = self
                    .app_state
                    .epoch
                    .load(std::sync::atomic::Ordering::Relaxed);
                if epoch != local_epoch {
                    return ConsensusNetResponse::EpochMismatch { local_epoch };
                }
                let app_state = self.app_state.clone();
                crate::consensus::queue::queue_rt()
                    .spawn(
                        async move { serve_decided_fetch(&app_state, from_height, to_height).await },
                    )
                    .await
                    .expect("decided-fetch task panicked")
            }
        }
    }
}

async fn serve_decided_fetch(
    app_state: &AppState,
    from_height: u64,
    to_height: u64,
) -> ConsensusNetResponse {
    // Barrier tap for sync serving, test_mode only.
    if app_state.test_mode {
        app_state
            .consensus_barriers
            .wait(barrier_names::BEFORE_SYNC_RESPONSE)
            .await;
    }
    let to = to_height.min(from_height.saturating_add(DECIDED_FETCH_MAX - 1));
    if to < from_height {
        return ConsensusNetResponse::Error {
            message: "bad height range".into(),
        };
    }
    let conn = match app_state.db_pool.get() {
        Ok(c) => c,
        Err(_) => {
            return ConsensusNetResponse::Error {
                message: "db pool exhausted".into(),
            };
        }
    };
    match store::decided_range(&conn, Height(from_height), Height(to)) {
        Ok(pairs) => {
            let mut items = Vec::with_capacity(pairs.len());
            for (block, cert) in &pairs {
                match (codec::encode(block), codec::encode(cert)) {
                    (Ok(b), Ok(c)) => items.push((b, c)),
                    _ => break,
                }
            }
            ConsensusNetResponse::Decided { items }
        }
        Err(e) => ConsensusNetResponse::Error {
            message: format!("decided fetch failed: {e}"),
        },
    }
}

impl RpcHandler for ConsensusScope {
    fn handle(&self, peer: PeerRef, payload: Vec<u8>) -> BoxFuture<'_, Vec<u8>> {
        Box::pin(async move { encode_payload(&self.serve(peer, payload).await) })
    }
}

// ============================================================================
// "txforward" — two-phase transaction forward (streamed, queue rt)
// ============================================================================

pub struct TxForwardScope {
    app_state: AppState,
}

impl StreamHandler for TxForwardScope {
    fn handle(
        &self,
        peer: PeerRef,
        payload: Vec<u8>,
        out: Box<dyn FrameSink>,
    ) -> BoxFuture<'_, ()> {
        // Consensus-support plane: the whole two-phase protocol (proposer
        // validation, ACK, enqueue) touches the DB — the QUEUE runtime. No
        // transport-level dedup: the consensus nonce table owns idempotency.
        let app_state = self.app_state.clone();
        Box::pin(async move {
            crate::consensus::queue::queue_rt()
                .spawn(serve_tx_forward(app_state, peer, payload, out))
                .await
                .expect("txforward task panicked");
        })
    }
}

async fn serve_tx_forward(
    app_state: AppState,
    peer: PeerRef,
    payload: Vec<u8>,
    mut out: Box<dyn FrameSink>,
) {
    let req: TransactionForwardRequest = match decode_payload(&payload) {
        Ok(r) => r,
        Err(e) => {
            let frame = encode_payload(&ForwardReply::Error {
                message: format!("bad forward request: {e}"),
            });
            let _ = out.send(frame).await;
            let _ = out.finish();
            return;
        }
    };

    // Reachability evidence: the forwarder reached us (RFC-CONSENSUS-002).
    app_state.evidence.record_contact(peer.node_id);

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
            if req.height > height {
                crate::consensus::malachite::engine::kick_sync_if_behind(
                    &app_state,
                    req.height - 1,
                    peer.node_id,
                );
            }
            let reject = encode_payload(&ForwardReply::NotProposer { height, round });
            if let Err(e) = out.send(reject).await {
                tracing::debug!("txforward reject to node {} failed: {}", peer.node_id, e);
                return;
            }
            let _ = out.finish();
            return;
        }
    }

    // Phase 1: Send immediate ACK (validated as proposer)
    if let Err(e) = out.send(encode_payload(&ForwardReply::Ack)).await {
        tracing::debug!("txforward ack to node {} failed: {}", peer.node_id, e);
        return;
    }

    // Phase 2: Process and send final result
    let response = crate::consensus::rpc::handle_transaction_forward(req, &app_state).await;
    if let Err(e) = out
        .send(encode_payload(&ForwardReply::Result(response)))
        .await
    {
        tracing::debug!("txforward result to node {} failed: {}", peer.node_id, e);
        return;
    }
    let _ = out.finish();
}

// ============================================================================
// "storage" — fragment health/fetch/store (app plane, main runtime)
// ============================================================================

pub struct StorageScope {
    app_state: AppState,
}

impl RpcHandler for StorageScope {
    fn handle(&self, peer: PeerRef, payload: Vec<u8>) -> BoxFuture<'_, Vec<u8>> {
        // App plane: fragments touch disk and the DB — the MAIN runtime;
        // degrades under API overload by design. The protocol itself is
        // crate-owned (RFC-017 Stage 2): this shell only decodes the request
        // and hands the substrate its LocalStateSink.
        let app_state = self.app_state.clone();
        Box::pin(async move {
            // Reachability evidence (RFC-CONSENSUS-002): fragment traffic
            // keeps pool nodes passively bright.
            app_state.evidence.record_contact(peer.node_id);
            self.app_state
                .runtime
                .spawn(async move {
                    use hopnet_storage::rpc::{FragmentRequest, FragmentResponse};
                    let response = match decode_payload::<FragmentRequest>(&payload) {
                        Ok(req) => {
                            let sink = crate::storage_host::substrate_host::SubstrateHost::new(
                                app_state.clone(),
                            );
                            hopnet_storage::rpc::serve(&app_state.fragments_dir, &sink, req)
                        }
                        Err(e) => FragmentResponse::Error {
                            message: format!("bad storage request: {e}"),
                        },
                    };
                    encode_payload(&response)
                })
                .await
                .expect("storage task panicked")
        })
    }
}

// ============================================================================
// "metrics" — latency/throughput echoes (inline) + storage query (main rt)
// ============================================================================

pub struct MetricsScope {
    app_state: AppState,
}

impl RpcHandler for MetricsScope {
    fn handle(&self, peer: PeerRef, payload: Vec<u8>) -> BoxFuture<'_, Vec<u8>> {
        let app_state = self.app_state.clone();
        Box::pin(async move {
            // Reachability evidence (RFC-CONSENSUS-002).
            app_state.evidence.record_contact(peer.node_id);
            let response = match decode_payload::<MetricsRequest>(&payload) {
                Err(e) => MetricsResponse::Error {
                    message: format!("bad metrics request: {e}"),
                },
                // Mesh plane: pure CPU echoes — INLINE on the net runtime.
                Ok(MetricsRequest::Latency(req)) => {
                    MetricsResponse::Latency(crate::metrics::rpc::handle_latency_ping(req))
                }
                Ok(MetricsRequest::Throughput(_)) => MetricsResponse::ThroughputAck,
                // App plane: storage usage walks the fragments dir — the
                // MAIN runtime.
                Ok(MetricsRequest::Storage) => {
                    let app_state = self.app_state.clone();
                    self.app_state
                        .runtime
                        .spawn(async move {
                            crate::metrics::rpc::handle_storage_query(&app_state).await
                        })
                        .await
                        .expect("storage-query task panicked")
                }
            };
            encode_payload(&response)
        })
    }
}

// ============================================================================
// "setup" — JoinInfo delivery (app plane, main runtime)
// ============================================================================

pub struct SetupScope {
    app_state: AppState,
}

impl RpcHandler for SetupScope {
    fn handle(&self, peer: PeerRef, payload: Vec<u8>) -> BoxFuture<'_, Vec<u8>> {
        // App plane: join processing writes the DB — the MAIN runtime.
        let app_state = self.app_state.clone();
        Box::pin(async move {
            // Reachability evidence; a joining peer may not be registered
            // yet — stray entries are harmless (the estimator iterates
            // registered ids, never map keys).
            app_state.evidence.record_contact(peer.node_id);
            self.app_state
                .runtime
                .spawn(async move {
                    let response = match decode_payload::<SetupRequest>(&payload) {
                        Err(e) => SetupResponse::Error {
                            message: format!("bad setup request: {e}"),
                        },
                        Ok(SetupRequest::JoinDeliver(req)) => {
                            match crate::setup::process_join_info(&app_state, req.join_info).await {
                                Ok(()) => SetupResponse::JoinAck { success: true },
                                Err(e) => SetupResponse::Error {
                                    message: format!("join failed: {}", e),
                                },
                            }
                        }
                    };
                    encode_payload(&response)
                })
                .await
                .expect("setup task panicked")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the §Scope Classes table (RFC-025) is enacted HERE and
    // nowhere else — a scope registered under the wrong class either
    // freezes a vocabulary nobody consumes or strands stragglers at a
    // boundary. The full-list equality makes every future scope
    // addition, removal, or reclassification a deliberate act.
    // Should: register exactly the RFC's table — locked: consensus,
    // metrics, setup, storage, txforward; compat: regenesis, status.
    // Should not: pass if any scope is added, removed, or reclassified
    // without this list changing.
    #[test]
    fn registry_matches_the_scope_class_table() {
        use hopnet_comms::ScopeClass::{Compat, Locked};
        let app_state = crate::consensus::tests::create_test_app_state();
        let registry = build_registry(&app_state);
        let mut scopes: Vec<(&str, hopnet_comms::ScopeClass)> = registry.scopes().collect();
        scopes.sort_by_key(|(name, _)| *name);
        assert_eq!(
            scopes,
            vec![
                ("consensus", Locked),
                ("metrics", Locked),
                ("regenesis", Compat),
                ("setup", Locked),
                ("status", Compat),
                ("storage", Locked),
                ("txforward", Locked),
            ]
        );
    }
}
