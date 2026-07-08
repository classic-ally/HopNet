//! Stage-4 integration: the Malachite engine adapters end-to-end over REAL
//! loopback iroh — HopNetApplication (dispatch table + Rule-8), SqliteStorage
//! on the shared app pool, the tokio shell, fire-and-forget gossip, and the
//! decided-value sync protocol. The bespoke engine is not involved.
//!
//! Networking runs through the SAME path as production: the comms accept
//! loop (started at test-AppState construction) dispatches the "consensus"
//! scope, which routes into whatever shell `app_state.malachite` holds. A
//! node that is "down" (engine not started) answers gossip with "engine not
//! active" — the message is dropped, exactly like a down production node —
//! and catches up through decided-value sync once live gossip lands above
//! its height.

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
use crate::consensus::malachite::{gossip, sync, EngineHandle};
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

/// Install the consensus schema — the network side (accept loop + consensus
/// scope) is already live from AppState construction.
fn install_consensus_schema(app_state: &AppState) {
    hopnet_consensus::store::install_schema(&app_state.db_pool.get().unwrap()).unwrap();
}

/// Spawn the full engine stack for one node: publisher, shell (HostCore over
/// HopNetApplication + pool-backed SqliteStorage), the driver task answering
/// NeedValue / SyncNeeded, and the `app_state.malachite` handle that the
/// comms "consensus" scope routes inbound traffic through.
async fn start_engine(app_state: &AppState, node_id: i32) -> EngineNode {
    let db_pool = app_state.db_pool.clone();

    let (out_tx, out_rx) = mpsc::unbounded_channel();
    tokio::spawn(gossip::run_publisher(app_state.clone(), node_id, out_rx));

    // Resume from persisted state (meta last_decided + 1) — the same rule
    // spawn_engine applies; Height::INITIAL for a fresh store.
    let start_height = {
        let conn = db_pool.get().unwrap();
        let last = hopnet_consensus::store::last_decided_height(&conn)
            .unwrap()
            .map(|h| h.0)
            .unwrap_or(0);
        Height(last + 1)
    };

    let storage_conn = db_pool.get().unwrap();
    let signer = hopnet_consensus::types::PrivKey(app_state.private_key.0.clone());
    let app_state_for_core = app_state.clone();
    let app_conn = app_state.db_pool.get().expect("app conn");
    let handle: ConsensusHandle = shell::spawn(
        move |gossip_seam, timers| {
            let storage = PoolStorage::from_handle(storage_conn, crate::db::shared::commit_timed)
                .expect("consensus storage");
            let mut app = HopNetApplication::new(app_state_for_core, app_conn);
            let valset = <HopNetApplication as Application<PoolStorage>>::validator_set(
                &mut app,
                start_height,
            );
            HostCore::new(
                chain_id(),
                signer,
                Address(node_id),
                engine_params(node_id),
                start_height,
                valset,
                app,
                storage,
                gossip_seam,
                timers,
            )
        },
        start_height,
        LinearTimeouts::default(),
        out_tx,
    );

    let ConsensusHandle {
        input_tx,
        decided,
        round,
        mut events,
    } = handle;

    let sync_inflight = Arc::new(AtomicBool::new(false));

    // Route inbound comms traffic into this shell (mirrors spawn_engine's
    // handle install). The OnceCell set can lose on an engine RESTART within
    // one AppState — the scope then still points at the dead first shell,
    // which only matters for tests that need inbound traffic post-restart
    // (the restart test is a single-node mesh; nothing inbound).
    let _ = app_state.malachite.set(EngineHandle {
        input_tx: input_tx.clone(),
        decided: decided.clone(),
        round,
        sync_inflight: sync_inflight.clone(),
    });

    // Driver: the app-side loop the Stage-5 cutover formalized.
    {
        let app_state = app_state.clone();
        let input_tx = input_tx.clone();
        let decided = decided.clone();
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
                                match crate::consensus::dispatch::create_signed_transaction(
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
                                &app_state.comms,
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

/// Build a directly-dialable address for an in-process endpoint. Addresses
/// are built from the bound sockets (wildcard → 127.0.0.1) because the
/// endpoint's self-reported addr may hold no usable direct address until
/// discovery runs.
fn loopback_addr(app_state: &AppState) -> hopnet_comms::EndpointAddr {
    let ep = app_state.comms.endpoint();
    let mut addr = hopnet_comms::EndpointAddr::new(ep.id());
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
}

/// Full-mesh direct connections between the fixture's endpoints — loopback
/// tests must not depend on external discovery infrastructure.
async fn connect_mesh(network: &MockNetwork) {
    let addrs: Vec<hopnet_comms::EndpointAddr> = network
        .nodes
        .iter()
        .map(|n| loopback_addr(&n.app_state))
        .collect();
    for i in 0..network.nodes.len() {
        for (j, addr) in addrs.iter().enumerate() {
            if i != j {
                network.nodes[i]
                    .app_state
                    .comms
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
        // Schema first; the comms accept loops (with the consensus scope)
        // have been polling since AppState construction, so dials complete.
        install_consensus_schema(&network.nodes[0].app_state);
        install_consensus_schema(&network.nodes[1].app_state);
        install_consensus_schema(&network.nodes[2].app_state);
        connect_mesh(&network).await;

        // Nodes 0 and 1 run the engine; node 2 is a validator but offline
        // (Majority profile: quorum 2 of 3 — heights where node 2 is the
        // proposer advance by propose-timeout round skip). Gossip to node 2
        // is answered "engine not active" and dropped, like any down node.
        let mut n0 = start_engine(&network.nodes[0].app_state, 0).await;
        let mut n1 = start_engine(&network.nodes[1].app_state, 1).await;

        wait_decided(&mut n0, 4, 300).await;
        wait_decided(&mut n1, 4, 300).await;

        // Refresh direct connections (the idle pre-connects may have died
        // while node 2 was "down"), then bring node 2 up: live gossip ahead
        // of its height triggers SyncNeeded → decided-value sync → live
        // participation.
        connect_mesh(&network).await;
        let mut n2 = start_engine(&network.nodes[2].app_state, 2).await;
        wait_decided(&mut n2, 6, 300).await;

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
        // 7 rows: the installed genesis (height 0) + decided heights 1..=6.
        assert_eq!(h0.len(), 7, "mesh must have genesis + decided heights 1..=6");
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
// engine stack — this isolates `IrohComms::connect_to_addr` plus the
// accept-loop requirement (QUIC handshakes complete only under accept()).
// Impact: the transport-layer regression test for loopback meshes.
#[test]
fn probe_loopback_direct_connect() {
    let a = crate::consensus::tests::MockNode::new(0);
    let b = crate::consensus::tests::MockNode::new(1);
    // The peer directory requires the dialer's pubkey in the receiver's nodes table.
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
        // QUIC handshakes only complete once the server polls accept() — the
        // comms accept loop has been running since AppState construction.
        let addr = loopback_addr(&b.app_state);
        eprintln!("dialing addr: {addr:?}");
        a.app_state
            .comms
            .connect_to_addr(1, addr)
            .await
            .expect("direct connect");
        let peer_b = hopnet_comms::PeerRef {
            node_id: 1,
            pubkey: b.app_state.comms.local_pubkey(),
        };
        let rtt = a.app_state.comms.ping(&peer_b).await.expect("ping");
        eprintln!("ping rtt: {rtt}ns");
    });
}

// Should: an engine shut down mid-flight resume from persisted state (meta
// last_decided + WAL) and keep deciding CONTIGUOUSLY — no gap, no repeat, no
// equivocation on the restart boundary.
// Should not: restart from genesis or corrupt the decided history.
// Impact: the Stage-5a restart-recovery proof over the production storage
// path (a single-node Majority mesh decides alone, so no peers needed).
#[test]
fn malachite_engine_restarts_from_persisted_state() {
    let network = MockNetwork::setup_with_validators(1);
    let rt = crate::consensus::tests::test_iroh_rt();
    rt.block_on(async move {
        let state = &network.nodes[0].app_state;
        install_consensus_schema(state);

        // First engine incarnation: decide a few heights, then shut down.
        let mut n0 = start_engine(state, 0).await;
        wait_decided(&mut n0, 3, 120).await;
        let _ = n0.input_tx.send(HostInput::Shutdown).await;

        // Let the shell thread stop and release its storage connection, then
        // read the height it durably reached.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let stopped_at = {
            let conn = state.db_pool.get().unwrap();
            hopnet_consensus::store::last_decided_height(&conn)
                .unwrap()
                .map(|h| h.0)
                .unwrap_or(0)
        };
        assert!(stopped_at >= 3, "engine must have decided ≥3 before restart");

        // Second incarnation resumes from meta (+ WAL replay if the shutdown
        // landed mid-height) and keeps going.
        let mut n0b = start_engine(state, 0).await;
        wait_decided(&mut n0b, stopped_at + 1, 120).await;
        let _ = n0b.input_tx.send(HostInput::Shutdown).await;

        // Decided history is contiguous across the restart boundary: exactly
        // one row per height from genesis to the tip.
        let conn = state.db_pool.get().unwrap();
        let heights: Vec<i64> = {
            let mut stmt = conn
                .prepare("SELECT height FROM decided_blocks ORDER BY height ASC")
                .unwrap();
            let rows = stmt.query_map([], |row| row.get(0)).unwrap();
            rows.collect::<Result<_, _>>().unwrap()
        };
        for (i, h) in heights.iter().enumerate() {
            assert_eq!(*h, i as i64, "decided history must be contiguous from 0");
        }
        assert!(heights.len() as u64 > stopped_at + 1);
    });
}
