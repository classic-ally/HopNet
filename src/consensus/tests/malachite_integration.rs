//! Stage-4 integration: the Malachite engine adapters end-to-end over REAL
//! loopback iroh — HopNetApplication (dispatch table + Rule-8), SqliteStorage
//! on the shared app pool, the tokio shell, fire-and-forget gossip, and the
//! decided-value sync protocol. The bespoke engine is not involved.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::context::{Address, Height};
use hopnet_consensus::host::HostCore;
use hopnet_consensus::shell::{self, ConsensusHandle, HostEvent, HostInput};
use hopnet_consensus::store::SqliteStorage;
use hopnet_consensus::traits::Application;
use hopnet_consensus::{HopNetContext, LinearTimeouts, Params, ValuePayload};

use crate::consensus::malachite::app::{build_value, HopNetApplication};
use crate::consensus::malachite::{gossip, sync};
use crate::consensus::tests::MockNetwork;
use crate::AppState;

type PoolStorage = SqliteStorage<r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>>;

struct EngineNode {
    input_tx: mpsc::Sender<HostInput>,
    decided: watch::Receiver<u64>,
}

fn engine_params(node_id: i32) -> Params<HopNetContext> {
    Params {
        address: Address(node_id),
        threshold_params: QuorumProfile::Majority.thresholds(),
        value_payload: ValuePayload::PartsOnly,
        enabled: true,
    }
}

fn chain_id() -> hopnet_consensus::types::Blake3Hash {
    hopnet_consensus::types::Blake3Hash::from_bytes([0x11; 32])
}

fn cleanup_payload() -> Vec<u8> {
    let cutoff_ts = (chrono::Utc::now() - chrono::Duration::hours(1)).timestamp() as u64;
    let cutoff = hopnet_common::CustomUUID::new(Some(&uuid::Timestamp::from_unix(
        uuid::NoContext,
        cutoff_ts,
        0,
    )));
    bincode::serde::encode_to_vec(&cutoff, bincode::config::standard()).unwrap_or_default()
}

/// Start a node's consensus NETWORK side only: schema install + accept loop
/// feeding a proxy channel. Runs before any dialing — QUIC handshakes only
/// complete once the server polls accept() — and before the engine exists
/// (a late joiner buffers inbound messages until its shell starts).
async fn start_net(app_state: &AppState) -> mpsc::Receiver<HostInput> {
    let db_pool = app_state.db_pool.clone();
    hopnet_consensus::store::install_schema(&db_pool.get().unwrap()).unwrap();
    let (proxy_tx, proxy_rx) = mpsc::channel::<HostInput>(1024);
    let server = gossip::ConsensusServer {
        input_tx: proxy_tx,
        db_pool,
    };
    tokio::spawn(gossip::run_accept_loop(
        app_state.iroh_transport.endpoint().clone(),
        server,
    ));
    proxy_rx
}

/// Spawn the full engine stack for one node: publisher, shell (HostCore over
/// HopNetApplication + pool-backed SqliteStorage), the proxy forwarder from
/// the accept loop, and the driver task answering NeedValue / SyncNeeded.
async fn start_engine(
    app_state: &AppState,
    node_id: i32,
    mut proxy_rx: mpsc::Receiver<HostInput>,
) -> EngineNode {
    let db_pool = app_state.db_pool.clone();

    let (out_tx, out_rx) = mpsc::unbounded_channel();
    tokio::spawn(gossip::run_publisher(
        app_state.iroh_transport.clone(),
        db_pool.clone(),
        node_id,
        out_rx,
    ));

    let storage_conn = db_pool.get().unwrap();
    let signer = hopnet_consensus::types::PrivKey(app_state.private_key.0.clone());
    let app_state_for_core = app_state.clone();
    let handle: ConsensusHandle = shell::spawn(
        move |gossip_seam, timers| {
            let storage = PoolStorage::from_handle(storage_conn, crate::db::shared::commit_timed)
                .expect("consensus storage");
            let mut app = HopNetApplication::new(app_state_for_core);
            let valset = <HopNetApplication as Application<PoolStorage>>::validator_set(
                &mut app,
                Height::INITIAL,
            );
            HostCore::new(
                chain_id(),
                signer,
                Address(node_id),
                engine_params(node_id),
                Height::INITIAL,
                valset,
                app,
                storage,
                gossip_seam,
                timers,
            )
        },
        Height::INITIAL,
        LinearTimeouts::default(),
        out_tx,
    );

    let ConsensusHandle {
        input_tx,
        decided,
        mut events,
    } = handle;

    // Forward the accept loop's proxy channel (including anything buffered
    // while this node was "down") into the shell.
    {
        let fw_tx = input_tx.clone();
        tokio::spawn(async move {
            while let Some(input) = proxy_rx.recv().await {
                if fw_tx.send(input).await.is_err() {
                    break;
                }
            }
        });
    }

    // Driver: the app-side loop the Stage-5 cutover will formalize.
    {
        let app_state = app_state.clone();
        let input_tx = input_tx.clone();
        let decided = decided.clone();
        let sync_inflight = Arc::new(AtomicBool::new(false));
        tokio::spawn(async move {
            while let Some(ev) = events.recv().await {
                match ev {
                    HostEvent::NeedValue { height, round } => {
                        // Throttle so the mesh doesn't sprint unboundedly
                        // while the laggard tries to catch up.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        // build_value blocks (write gate + Immediate tx
                        // preflight) — keep it off the async workers.
                        let build_state = app_state.clone();
                        let built = tokio::task::spawn_blocking(move || {
                            let candidates =
                                match crate::consensus::functions::create_signed_transaction(
                                    &build_state,
                                    "system.cleanup_nonces".to_string(),
                                    cleanup_payload(),
                                ) {
                                    Ok(tx) => vec![tx],
                                    Err(_) => Vec::new(),
                                };
                            let mut conn = build_state.db_pool.get().unwrap();
                            build_value(&build_state, &mut conn, height, round, candidates)
                        })
                        .await
                        .expect("build task");
                        match built {
                            Ok(built) => {
                                let _ = input_tx
                                    .send(HostInput::Propose {
                                        height,
                                        round,
                                        block: built.block,
                                    })
                                    .await;
                            }
                            Err(e) => tracing::error!("build_value failed: {e}"),
                        }
                    }
                    HostEvent::SyncNeeded { target, hint_peer } => {
                        if sync_inflight.swap(true, Ordering::SeqCst) {
                            continue; // one sync at a time
                        }
                        let app_state = app_state.clone();
                        let input_tx = input_tx.clone();
                        let mut decided = decided.clone();
                        let flag = sync_inflight.clone();
                        tokio::spawn(async move {
                            if let Err(e) = sync::sync_to_target(
                                &app_state.iroh_transport,
                                &app_state.db_pool,
                                node_id,
                                &input_tx,
                                &mut decided,
                                target.0,
                                Some(hint_peer),
                            )
                            .await
                            {
                                tracing::warn!("sync failed: {e:?}");
                            }
                            flag.store(false, Ordering::SeqCst);
                        });
                    }
                    HostEvent::SyncInvalid { .. } => {}
                }
            }
        });
    }

    EngineNode { input_tx, decided }
}

async fn wait_decided(node: &mut EngineNode, target: u64, secs: u64) {
    tokio::time::timeout(Duration::from_secs(secs), async {
        while *node.decided.borrow() < target {
            node.decided.changed().await.expect("shell alive");
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "node did not reach height {target} in {secs}s (at {})",
            *node.decided.borrow()
        )
    });
}

/// Full-mesh direct connections between the fixture's endpoints — loopback
/// tests must not depend on external discovery infrastructure. Addresses are
/// built from the bound sockets (wildcard → 127.0.0.1) because the endpoint's
/// self-reported addr may hold no usable direct address until discovery runs.
async fn connect_mesh(network: &MockNetwork) {
    let addrs: Vec<iroh::EndpointAddr> = network
        .nodes
        .iter()
        .map(|n| {
            let ep = n.app_state.iroh_transport.endpoint();
            let mut addr = iroh::EndpointAddr::new(ep.id());
            for sock in ep.bound_sockets() {
                let sock = if sock.ip().is_unspecified() {
                    std::net::SocketAddr::new(
                        match sock {
                            std::net::SocketAddr::V4(_) => {
                                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                            }
                            std::net::SocketAddr::V6(_) => {
                                std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
                            }
                        },
                        sock.port(),
                    )
                } else {
                    sock
                };
                addr = addr.with_ip_addr(sock);
            }
            addr
        })
        .collect();
    for i in 0..network.nodes.len() {
        for (j, addr) in addrs.iter().enumerate() {
            if i != j {
                network.nodes[i]
                    .app_state
                    .iroh_transport
                    .connect_to_addr(j as i32, addr.clone())
                    .await
                    .expect("direct loopback connect");
            }
        }
    }
}

// Should: a 2-of-3 Majority mesh decide real blocks (dispatch-table
// transactions, Rule-8 validation, one-transaction commits) over REAL
// loopback iroh — including round-advance past the absent proposer — and a
// LATE-JOINING node catch up via decided-value sync, then follow live, ending
// byte-identical to the mesh.
// Should not: depend on external discovery, the bespoke engine, or any
// mocked component — every adapter in this test ships.
// Impact: the Stage-4 exit proof — all real adapters composed end-to-end.
#[test]
fn malachite_mesh_decides_and_laggard_syncs_over_loopback_iroh() {
    let network = MockNetwork::setup_with_validators(3);
    let rt = crate::consensus::tests::test_iroh_rt();
    rt.block_on(async move {
        // Network side first: accept loops must be polling before any dial
        // (QUIC handshakes complete only when the server accepts).
        let rx0 = start_net(&network.nodes[0].app_state).await;
        let rx1 = start_net(&network.nodes[1].app_state).await;
        let rx2 = start_net(&network.nodes[2].app_state).await;
        connect_mesh(&network).await;

        // Nodes 0 and 1 run the engine; node 2 is a validator but offline
        // (Majority profile: quorum 2 of 3 — heights where node 2 is the
        // proposer advance by propose-timeout round skip).
        let mut n0 = start_engine(&network.nodes[0].app_state, 0, rx0).await;
        let mut n1 = start_engine(&network.nodes[1].app_state, 1, rx1).await;

        wait_decided(&mut n0, 4, 150).await;
        wait_decided(&mut n1, 4, 150).await;

        // Refresh direct connections (the idle pre-connects may have died
        // while node 2 was "down"), then bring node 2 up: buffered + live
        // gossip ahead of its height triggers SyncNeeded → decided-value
        // sync → live participation.
        connect_mesh(&network).await;
        let mut n2 = start_engine(&network.nodes[2].app_state, 2, rx2).await;
        wait_decided(&mut n2, 6, 180).await;

        // The laggard's decided history is byte-identical to the mesh's.
        let rows = |st: &AppState| -> Vec<(i64, Vec<u8>)> {
            let conn = st.db_pool.get().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT height, block_hash FROM decided_blocks WHERE height <= 6 ORDER BY height",
                )
                .unwrap();
            let r = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap();
            r.collect::<Result<_, _>>().unwrap()
        };
        let h0 = rows(&network.nodes[0].app_state);
        let h2 = rows(&network.nodes[2].app_state);
        assert_eq!(h0.len(), 6, "mesh must have decided heights 1..=6");
        assert_eq!(h0, h2, "laggard history must match the mesh exactly");

        // Nonces from applied blocks landed on the laggard too (app-state
        // replication through sync, not just consensus metadata).
        let nonce_count = |st: &AppState| -> i64 {
            st.db_pool
                .get()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM committed_tx_nonces", [], |r| r.get(0))
                .unwrap()
        };
        assert!(nonce_count(&network.nodes[2].app_state) > 0);

        for n in [&n0, &n1, &n2] {
            let _ = n.input_tx.send(HostInput::Shutdown).await;
        }
    });
}

// Should: connect two in-process endpoints directly by socket address (no
// discovery) and round-trip a ping.
// Should not: require an external discovery service, a relay, or the full
// engine stack — this isolates `IrohTransport::connect_to_addr` plus the
// accept-loop requirement (QUIC handshakes complete only under accept()).
// Impact: the transport-layer regression test for loopback meshes.
#[test]
fn probe_loopback_direct_connect() {
    let a = crate::consensus::tests::MockNode::new(0);
    let b = crate::consensus::tests::MockNode::new(1);
    // PeerValidator requires the dialer's pubkey in the receiver's nodes table.
    for (me, other) in [(&a, &b), (&b, &a)] {
        let conn = me.app_state.db_pool.get().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 0, ?)",
            rusqlite::params![
                other.node_id,
                format!("node_{}", other.node_id),
                other.verifying_key
            ],
        )
        .unwrap();
    }
    let rt = crate::consensus::tests::test_iroh_rt();
    rt.block_on(async move {
        // QUIC handshakes only complete once the server polls accept().
        tokio::spawn(crate::net::handler::handle_incoming_connections(
            b.app_state.iroh_transport.endpoint().clone(),
            b.app_state.clone(),
        ));
        let ep_b = b.app_state.iroh_transport.endpoint();
        eprintln!("b bound sockets: {:?}", ep_b.bound_sockets());
        let mut addr = iroh::EndpointAddr::new(ep_b.id());
        for sock in ep_b.bound_sockets() {
            let sock = if sock.ip().is_unspecified() {
                std::net::SocketAddr::new(
                    match sock {
                        std::net::SocketAddr::V4(_) => std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                        std::net::SocketAddr::V6(_) => std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                    },
                    sock.port(),
                )
            } else {
                sock
            };
            addr = addr.with_ip_addr(sock);
        }
        eprintln!("dialing addr: {addr:?}");
        a.app_state
            .iroh_transport
            .connect_to_addr(1, addr)
            .await
            .expect("direct connect");
        let rtt = a
            .app_state
            .iroh_transport
            .ping(1, ep_b.id())
            .await
            .expect("ping");
        eprintln!("ping rtt: {rtt}ns");
    });
}
