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

use hopnet_consensus::config::{MalachiteThresholds, QuorumProfile};
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
        threshold_params: QuorumProfile::Majority.thresholds_for(1),
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
type ExtraCandidates =
    std::sync::Arc<std::sync::Mutex<Vec<crate::consensus::types::Transaction>>>;

async fn start_engine(app_state: &AppState, node_id: i32) -> EngineNode {
    start_engine_with_candidates(app_state, node_id, ExtraCandidates::default()).await
}

/// start_engine with injectable extra proposal candidates (cloned every
/// NeedValue, never drained — the committed-nonce dedup retires them after
/// commit; the solo-block rule strips siblings from a membership block).
async fn start_engine_with_candidates(
    app_state: &AppState,
    node_id: i32,
    extra: ExtraCandidates,
) -> EngineNode {
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
                hopnet_consensus::config::QuorumProfile::Majority,
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
        let extra_for_build_outer = extra.clone();
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
                        let extra_for_build = extra_for_build_outer.clone();
                        let built = tokio::task::spawn_blocking(move || {
                            let mut candidates =
                                match crate::consensus::dispatch::create_signed_transaction(
                                    &build_state,
                                    "system.cleanup_nonces".to_string(),
                                    cleanup_payload(),
                                ) {
                                    Ok(tx) => vec![tx],
                                    Err(_) => Vec::new(),
                                };
                            candidates.extend(extra_for_build.lock().unwrap().iter().cloned());
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
                                app_state.epoch.load(std::sync::atomic::Ordering::Relaxed),
                                &app_state.db_pool,
                                node_id,
                                &input_tx,
                                &mut decided,
                                target.0,
                                Some(hint_peer),
                                Some(app_state.evidence.clone()),
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

// Should: a fresh/late node replay a chain CONTAINING a committed vote-out
// without wedging — the vote-out block validates at ValidationOrigin::Sync,
// where the subjective membership guard is structurally skipped (it lives
// in validate_inner's Live block only). The replaying node's own evidence
// says everyone is live (it just booted), and it IS the target: were the
// guard consulted at Sync, it would judge the block invalid and the host
// would hold it at that height forever (SyncInvalid).
// Should not: depend on the replaying node's evidence, wall clock, or its
// own opinion of the removal.
// Impact: the RT1 rule — the single biggest integration risk of subjective
// validation. A regression here permanently bricks every new joiner of any
// mesh whose history contains a vote-out.
#[test]
fn fresh_node_syncs_vote_out_chain_without_wedging() {
    let network = MockNetwork::setup_with_validators(3);
    let rt = crate::consensus::tests::test_iroh_rt();
    rt.block_on(async move {
        for node in &network.nodes {
            install_consensus_schema(&node.app_state);
            // Tiny windows so node 2's synthetic age crosses t_out fast:
            // probe_base=1, grace=1 -> t_out(Cliff) = 3 s.
            let conn = node.app_state.db_pool.get().unwrap();
            hopnet_consensus::store::apply_policy_rows(
                &conn,
                &[
                    ("probe_base".to_string(), "1".to_string()),
                    ("grace".to_string(), "1".to_string()),
                ],
            )
            .unwrap();
        }
        connect_mesh(&network).await;

        // Nodes 0 and 1 run engines; node 2 never starts (dark from the
        // observers' map origins).
        let extra = ExtraCandidates::default();
        let mut n0 =
            start_engine_with_candidates(&network.nodes[0].app_state, 0, extra.clone()).await;
        let mut n1 = start_engine(&network.nodes[1].app_state, 1).await;

        wait_decided(&mut n0, 2, 300).await;

        // Age node 2 past t_out on both observers and meet the attestation
        // floor (>= 2 probe attempts since contact).
        tokio::time::sleep(Duration::from_secs(4)).await;
        for node in &network.nodes[..2] {
            node.app_state.evidence.record_probe_sent(2);
            node.app_state.evidence.record_probe_sent(2);
        }

        // Inject the vote-out, signed by node 0.
        let payload = bincode::serde::encode_to_vec(
            &crate::consensus::handlers::VoteOutRequest { node_id: 2 },
            bincode::config::standard(),
        )
        .unwrap();
        let tx = crate::consensus::dispatch::create_signed_transaction(
            &network.nodes[0].app_state,
            "validator_vote_out".to_string(),
            payload,
        )
        .unwrap();
        extra.lock().unwrap().push(tx);

        // The mesh commits the removal (node 1's Live guard attests too —
        // quorum 2 of the 2 live validators).
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            {
                let conn = network.nodes[0].app_state.db_pool.get().unwrap();
                let pending = *n0.decided.borrow() + 1;
                if !hopnet_consensus::validators::is_node_active(&conn, 2, pending).unwrap() {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "vote-out of node 2 never committed"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let voteout_tip = *n0.decided.borrow();

        // A couple more heights on top so the replay crosses the vote-out.
        wait_decided(&mut n0, voteout_tip + 2, 300).await;
        wait_decided(&mut n1, voteout_tip + 2, 300).await;
        let mesh_tip = *n0.decided.borrow();

        // Bring node 2 up. It is OUT of the valset, so consensus gossip
        // never reaches it — drive sync explicitly, exactly as the
        // production tip-poll does.
        connect_mesh(&network).await;
        let mut n2 = start_engine(&network.nodes[2].app_state, 2).await;
        let peers = vec![
            hopnet_comms::PeerRef {
                node_id: 0,
                pubkey: network.nodes[0].verifying_key.0.to_bytes(),
            },
            hopnet_comms::PeerRef {
                node_id: 1,
                pubkey: network.nodes[1].verifying_key.0.to_bytes(),
            },
        ];
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            let mut decided = n2.decided.clone();
            let _ = sync::sync_to_tip(
                &network.nodes[2].app_state.comms,
                network.nodes[2].app_state.epoch.load(std::sync::atomic::Ordering::Relaxed),
                &n2.input_tx,
                &mut decided,
                &peers,
                None,
            )
            .await;
            if *n2.decided.borrow() >= mesh_tip {
                break; // NON-WEDGE: replayed the vote-out block at Sync
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 2 wedged replaying the vote-out chain (decided {} < tip {mesh_tip})",
                *n2.decided.borrow()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Byte-identical histories through the mesh tip.
        let rows = |st: &AppState| -> Vec<(i64, Vec<u8>)> {
            let conn = st.db_pool.get().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT height, block_hash FROM decided_blocks WHERE height <= ? ORDER BY height",
                )
                .unwrap();
            let r = stmt
                .query_map([mesh_tip as i64], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap();
            r.collect::<Result<_, _>>().unwrap()
        };
        assert_eq!(
            rows(&network.nodes[0].app_state),
            rows(&network.nodes[2].app_state),
            "replayed history must match the mesh exactly"
        );

        // The replayed chain told node 2 about its own removal.
        let conn = network.nodes[2].app_state.db_pool.get().unwrap();
        let pending = mesh_tip + 1;
        assert!(!hopnet_consensus::validators::is_node_active(&conn, 2, pending).unwrap());
        assert_eq!(
            hopnet_consensus::validators::last_departure(&conn, 2, pending).unwrap(),
            Some(hopnet_consensus::validators::DepartureKind::VotedOut)
        );
    });
}

/// The PRODUCTION driver stack over a test shell — spawn_driver + settler
/// + drain watcher, spawn_engine parity minus its config reads. The
/// regenesis boundary must be exercised through the code the real node
/// runs (proposer commit injection, drain watcher, terminal halt), not a
/// test-local approximation.
async fn start_engine_production(app_state: &AppState, node_id: i32) -> EngineNode {
    let db_pool = app_state.db_pool.clone();
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    tokio::spawn(gossip::run_publisher(app_state.clone(), node_id, out_rx));

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
            // ON-DEMAND, like spawn_engine: a drained moratorium is
            // height-stable until the commit proposal — no free-running
            // empty blocks racing a node past the seal (a straggler that
            // misses the final block is S7's rejoin case, not this
            // test's).
            HostCore::new(
                chain_id(),
                signer,
                Address(node_id),
                hopnet_consensus::config::QuorumProfile::Majority,
                engine_params(node_id),
                start_height,
                valset,
                app,
                storage,
                gossip_seam,
                timers,
            )
            .on_demand()
        },
        start_height,
        LinearTimeouts::default(),
        out_tx,
    );
    let ConsensusHandle {
        input_tx,
        decided,
        round,
        events,
    } = handle;
    let sync_inflight = Arc::new(AtomicBool::new(false));
    let _ = app_state.malachite.set(EngineHandle {
        input_tx: input_tx.clone(),
        decided: decided.clone(),
        round,
        sync_inflight: sync_inflight.clone(),
    });

    // Settler (spawn_engine parity): resolve pool notifiers on decide —
    // without it, inflight entries never clear and drain never completes.
    {
        let pool = app_state.consensus_queue.pending_pool();
        let db_pool = app_state.db_pool.clone();
        let mut decided_watch = decided.clone();
        tokio::spawn(async move {
            while decided_watch.changed().await.is_ok() {
                let h = *decided_watch.borrow_and_update();
                if let Ok(conn) = db_pool.get() {
                    pool.settle(&conn, h);
                }
            }
        });
    }

    let build_conn = app_state.db_pool.get().expect("build conn");
    crate::consensus::malachite::engine::spawn_driver(
        app_state.clone(),
        node_id,
        input_tx.clone(),
        decided.clone(),
        events,
        build_conn,
        sync_inflight,
    );
    crate::consensus::malachite::engine::spawn_drain_watcher(
        app_state.clone(),
        input_tx.clone(),
        app_state.consensus_queue.pending_pool(),
    );
    // Tip-poll (spawn_engine parity): during a boundary it also covers
    // SEATED stragglers — a node that misses the final block pulls the
    // seal via decided-value sync from the halted-but-serving peers.
    crate::consensus::malachite::engine::spawn_tip_poll(app_state.clone());

    EngineNode { input_tx, decided }
}

// Should: drive a real 3-node loopback mesh through the whole boundary
// over the PRODUCTION driver stack — regenesis_start decides and the
// moratorium commits mesh-wide; once every pool drains, the proposer
// injects the commit bound to its exact height; vote-iff-match passes on
// identical replicas; every engine halts at the same terminal H; the
// seal work leaves the durable marker and the byte-certified artifact.
// Should not: decide anything past the terminal height, on any node.
// Impact: OQ4's drain rule, the drain watcher, the proposer injection,
// the halt, and the artifact writer are the shipping code paths — S5's
// real-engine end-to-end.
#[test]
fn regenesis_seal_halts_mesh_and_writes_artifact() {
    // The artifact path derives from XDG_DATA_HOME; isolate it for this
    // process. set_var is process-global, but nothing else in the lib
    // tests resolves the data dir.
    let data_dir = std::env::temp_dir().join(format!("hopnet-seal-e2e-{}", std::process::id()));
    std::fs::create_dir_all(data_dir.join("hopnet")).unwrap();
    unsafe { std::env::set_var("XDG_DATA_HOME", &data_dir) };

    let network = MockNetwork::setup_with_validators(3);
    let rt = crate::consensus::tests::test_iroh_rt();
    let data_dir_in = data_dir.clone();
    rt.block_on(async move {
        for node in &network.nodes {
            install_consensus_schema(&node.app_state);
            // The start precondition: every seated validator runs the
            // target. Identical direct writes on every replica keep the
            // fixtures consistent (the S3 attestation path needs the very
            // engine we are about to start).
            node.app_state
                .db_pool
                .get()
                .unwrap()
                .execute("UPDATE nodes SET running_version_code = 20260800", [])
                .unwrap();
        }
        connect_mesh(&network).await;

        let mut engines = Vec::new();
        for (i, node) in network.nodes.iter().enumerate() {
            engines.push(start_engine_production(&node.app_state, i as i32).await);
        }

        // Freeze: a seated validator's node-signed start, routed into the
        // PENDING PROPOSER's pool exactly as production forwarding would
        // (the pool push wakes that node's on-demand engine — wake rule
        // 1; the settler resolves the notifier on decide).
        let start_tx = crate::consensus::types::Transaction::new(
            "regenesis_start".to_string(),
            bincode::serde::encode_to_vec(
                &crate::regenesis::RegenesisStart {
                    target_version_code: 20260800,
                },
                bincode::config::standard(),
            )
            .unwrap(),
            network.nodes[0].node_id,
            &network.nodes[0].signing_key,
        )
        .unwrap();
        let (_pending, _round, proposer) =
            crate::consensus::malachite::engine::proposal_target(&network.nodes[0].app_state)
                .expect("proposal target");
        let queue = network.nodes[proposer as usize]
            .app_state
            .consensus_queue
            .clone();
        tokio::spawn(async move { queue.enqueue_forwarded(vec![start_tx]).await });

        let state_of = |st: &AppState| {
            let conn = st.db_pool.get().unwrap();
            crate::db::regenesis::read_regenesis_state(&conn).unwrap()
        };

        // Moratorium mesh-wide, then the drain watcher + proposer carry it
        // to the seal with no further help from the test.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
        loop {
            let states: Vec<_> = network
                .nodes
                .iter()
                .map(|n| state_of(&n.app_state))
                .collect();
            if states
                .iter()
                .all(|s| s.phase == crate::db::regenesis::RegenesisPhase::Sealed)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "mesh never sealed: {states:?}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let sealed: Vec<_> = network
            .nodes
            .iter()
            .map(|n| state_of(&n.app_state))
            .collect();
        let seal_height = sealed[0].seal_height.unwrap();
        let committed_hash = sealed[0].snapshot_hash.clone().unwrap();
        for s in &sealed {
            assert_eq!(s.seal_height, Some(seal_height), "terminal height differs");
            assert_eq!(
                s.snapshot_hash.as_deref(),
                Some(committed_hash.as_slice()),
                "certified hash differs"
            );
        }

        // Halt: every engine converges on the terminal height and stops.
        for e in engines.iter_mut() {
            wait_decided(e, seal_height, 120).await;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        for e in &engines {
            assert_eq!(*e.decided.borrow(), seal_height, "decided past the seal");
        }

        // Durable marker on every node.
        for node in &network.nodes {
            let conn = node.app_state.db_pool.get().unwrap();
            assert_eq!(
                crate::regenesis::seal::sealed_marker(&conn),
                Some(seal_height),
                "seal marker missing"
            );
        }

        // The artifact next to the (XDG-derived) database: byte-certified
        // against the committed hash. All three in-process nodes write the
        // same path with identical bytes (unique tmp names, last rename
        // wins).
        let artifact = data_dir_in
            .join("hopnet")
            .join(crate::regenesis::seal::SEAL_ARTIFACT_FILENAME);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while !artifact.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "artifact never written"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let bytes = std::fs::read(&artifact).unwrap();
        assert_eq!(
            blake3::hash(&bytes).as_bytes(),
            committed_hash.as_slice(),
            "artifact bytes are not the certified bytes"
        );

        for e in &engines {
            let _ = e.input_tx.send(HostInput::Shutdown).await;
        }
    });
    std::fs::remove_dir_all(&data_dir).ok();
}
