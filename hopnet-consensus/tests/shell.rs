//! Tokio-shell smoke tests: the production wrapper (dedicated thread,
//! current_thread runtime, DelayQueue timers) drives the same HostCore the
//! deterministic simulator fuzzes — here over real time and real channels.

mod common;

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

    (ShellNode { input_tx, decided }, out_rx)
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
        let (node, out) = spawn_node(i, n as i32, QuorumProfile::Bft);
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
        let (node, out) = spawn_node(i, n as i32, QuorumProfile::Majority);
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
