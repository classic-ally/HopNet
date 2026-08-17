//! Tokio-shell smoke tests: the production wrapper (dedicated thread,
//! current_thread runtime, DelayQueue timers) drives the same HostCore the
//! deterministic simulator fuzzes — here over real time and real channels.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use malachitebft_core_types::LinearTimeouts;
use tokio::sync::{mpsc, watch};

use common::{build_block, storage_from_conn, SqlApp};
use hopnet_consensus::codec::WireConsensusMsg;
use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::context::{Address, Height};
use hopnet_consensus::host::HostCore;
use hopnet_consensus::shell::{self, ConsensusHandle, HostEvent, HostInput};

struct ShellNode {
    input_tx: mpsc::Sender<HostInput>,
    decided: watch::Receiver<u64>,
    running: Arc<AtomicBool>,
}

/// Spawn one shell node over an in-memory SQLite DB; wire its NeedValue
/// events to a builder task. Returns the node plus its raw outbound stream.
fn spawn_node(
    node_id: i32,
    n: i32,
    profile: QuorumProfile,
) -> (ShellNode, mpsc::UnboundedReceiver<WireConsensusMsg>) {
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let valset = common::valset(n);
    let handle: ConsensusHandle = shell::spawn(
        move |gossip, timers| {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            let storage = storage_from_conn(conn);
            HostCore::new(
                common::chain_id(),
                common::key(node_id),
                Address(node_id),
                profile,
                common::params(node_id, profile),
                Height::INITIAL,
                valset.clone(),
                SqlApp { valset },
                storage,
                gossip,
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
        running,
        ..
    } = handle;

    // Value-builder driver: answer NeedValue with a deterministic block.
    let builder_tx = input_tx.clone();
    tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            if let HostEvent::NeedValue { height, round } = ev {
                let block = build_block(height, round, node_id, None);
                let _ = builder_tx
                    .send(HostInput::Propose {
                        height,
                        round,
                        block,
                    })
                    .await;
            }
        }
    });

    (
        ShellNode {
            input_tx,
            decided,
            running,
        },
        out_rx,
    )
}

/// Like [`spawn_node`] but over a FILE-backed DB (so a second raw connection
/// can contend for the write lock) with production pragmas and the
/// contention-classifying [`common::ContendedApp`]. The 5000 ms busy_timeout
/// keeps the genuinely-fatal paths (WAL append, decide) waiting out a test's
/// lock hold instead of failing — post-fix, a fatal there aborts the whole
/// test binary.
fn spawn_contended_node(
    node_id: i32,
    n: i32,
    profile: QuorumProfile,
    path: std::path::PathBuf,
) -> (ShellNode, mpsc::UnboundedReceiver<WireConsensusMsg>) {
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let valset = common::valset(n);
    let handle: ConsensusHandle = shell::spawn(
        move |gossip, timers| {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;
                 CREATE TABLE IF NOT EXISTS victim_probe (id INTEGER);",
            )
            .unwrap();
            let storage = storage_from_conn(conn);
            HostCore::new(
                common::chain_id(),
                common::key(node_id),
                Address(node_id),
                profile,
                common::params(node_id, profile),
                Height::INITIAL,
                valset.clone(),
                common::ContendedApp::new(valset),
                storage,
                gossip,
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
        running,
        ..
    } = handle;

    let builder_tx = input_tx.clone();
    tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            if let HostEvent::NeedValue { height, round } = ev {
                let block = build_block(height, round, node_id, None);
                let _ = builder_tx
                    .send(HostInput::Propose {
                        height,
                        round,
                        block,
                    })
                    .await;
            }
        }
    });

    (
        ShellNode {
            input_tx,
            decided,
            running,
        },
        out_rx,
    )
}

/// Full-mesh router: every broadcast goes to every other node.
fn route(nodes: &[ShellNode], outs: Vec<mpsc::UnboundedReceiver<WireConsensusMsg>>) {
    let input_txs: Vec<_> = nodes.iter().map(|n| n.input_tx.clone()).collect();
    for (i, mut out) in outs.into_iter().enumerate() {
        let txs = input_txs.clone();
        tokio::spawn(async move {
            while let Some(msg) = out.recv().await {
                for (j, tx) in txs.iter().enumerate() {
                    if j != i {
                        let _ = tx
                            .send(HostInput::Wire {
                                from: i as i32,
                                msg: msg.clone(),
                            })
                            .await;
                    }
                }
            }
        });
    }
}

async fn wait_decided(node: &mut ShellNode, target: u64, secs: u64) {
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

// Should: three shell-wrapped nodes decide consecutive heights over real
// channels and real time (DelayQueue timers armed but unused on happy path).
// Should not: need a timeout fire, deadlock, or decide out of order.
// Impact: proves the production shell (thread + runtime + channel plumbing)
// drives the fuzz-hardened core correctly end-to-end.
#[tokio::test]
async fn three_shells_decide_over_channels() {
    let n = 3;
    let mut nodes = Vec::new();
    let mut outs = Vec::new();
    for i in 0..n {
        let (node, out) = spawn_node(i, n, QuorumProfile::Bft);
        nodes.push(node);
        outs.push(out);
    }
    route(&nodes, outs);

    for node in &mut nodes {
        wait_decided(node, 4, 30).await;
    }

    for node in &nodes {
        let _ = node.input_tx.send(HostInput::Shutdown).await;
    }
}

// Impact: the 2026-08-17 production wedge reproduced over the real shell —
// two of three nodes hit a transient BUSY acquiring the IMMEDIATE validation
// retry, the shell treated it as fatal and stopped, and the zombies served
// HTTP at a fixed height until a manual restart. This is the end-to-end
// guard that the shell now survives it and recovers.
// Should: survive a write-lock hold longer than the IMMEDIATE retry bound
// during live proposal validation — keep running, and decide again once
// contention clears.
// Should not: stop the shell (pre-fix the decided watch closes and this
// test's wait panics "shell alive").
// Should: report not-running through the liveness flag after a clean
// shutdown (the flag /health reads).
#[tokio::test]
async fn shell_survives_validation_contention() {
    let n = 2;
    let path0 = common::temp_db("shell-contend-n0");
    let path1 = common::temp_db("shell-contend-n1");
    let mut nodes = Vec::new();
    let mut outs = Vec::new();
    for (i, path) in [path0.clone(), path1.clone()].into_iter().enumerate() {
        let (node, out) = spawn_contended_node(i as i32, n, QuorumProfile::Majority, path);
        nodes.push(node);
        outs.push(out);
    }
    route(&nodes, outs);

    // A healthy baseline first, so the contention window hits a live mesh.
    wait_decided(&mut nodes[0], 2, 30).await;
    let decided_before = *nodes[0].decided.borrow();

    // Hold node 0's write lock well past UNDETERMINED_RETRY_BUSY_TIMEOUT_MS
    // (300 ms). Organic node-1 proposals plus the injected ones below hit
    // ingest_proposal while the lock is held: the DEFERRED dry-run fails
    // fast, the app classifies Undetermined, and the IMMEDIATE retry cannot
    // BEGIN inside its bound — the exact production failure.
    let hold = common::hold_write_lock(&path0, Duration::from_millis(1200));

    // Belt and braces: inject node 1's (deterministic, byte-identical)
    // proposal for the current height every 100 ms so validation contention
    // is exercised even if the mesh's organic timing misses the window.
    for _ in 0..12 {
        let h = *nodes[0].decided.borrow() + 1;
        let wire = WireConsensusMsg::ProposedValue(hopnet_consensus::codec::WireProposedValue {
            height: h,
            round: 0,
            valid_round: -1,
            proposer: 1,
            block: build_block(Height(h), malachitebft_core_types::Round::new(0), 1, None),
        });
        let _ = nodes[0]
            .input_tx
            .send(HostInput::Wire { from: 1, msg: wire })
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::task::spawn_blocking(move || hold.join().unwrap())
        .await
        .unwrap();

    // The shell survived and the mesh decides again.
    wait_decided(&mut nodes[0], decided_before + 2, 30).await;
    assert!(
        nodes[0].running.load(Ordering::SeqCst),
        "shell must still be running after surviving contention"
    );
    assert!(nodes[1].running.load(Ordering::SeqCst));

    // Clean shutdown clears the liveness flag (the Drop guard /health reads).
    for node in &nodes {
        let _ = node.input_tx.send(HostInput::Shutdown).await;
    }
    for _ in 0..50 {
        if !nodes[0].running.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !nodes[0].running.load(Ordering::SeqCst),
        "clean shutdown must clear the liveness flag"
    );
    let _ = std::fs::remove_file(&path0);
    let _ = std::fs::remove_file(&path1);
}

// Should: a two-node Majority mesh decide through the shell (home-mesh
// profile over the production wrapper).
// Should not: require a third node.
// Impact: the CFT profile exercised through the real shell.
#[tokio::test]
async fn two_shell_cft_mesh_decides() {
    let n = 2;
    let mut nodes = Vec::new();
    let mut outs = Vec::new();
    for i in 0..n {
        let (node, out) = spawn_node(i, n, QuorumProfile::Majority);
        nodes.push(node);
        outs.push(out);
    }
    route(&nodes, outs);

    for node in &mut nodes {
        wait_decided(node, 3, 30).await;
    }

    for node in &nodes {
        let _ = node.input_tx.send(HostInput::Shutdown).await;
    }
}
