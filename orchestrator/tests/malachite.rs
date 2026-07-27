//! Malachite-engine protocol tests: leader-down round advance (which is also
//! the on-demand wake-rule test — the mesh is idle when the proposer dies),
//! lagging-node decided-value sync, the BFT quorum-loss negative control,
//! and the effect-tap barrier windows.
//!
//! Profile requirements: the tests that kill a node and still expect progress
//! need the CFT majority profile (BFT n=3 needs all 3 validators). Run the
//! orchestrator with:
//!
//!   HOPNET_QUORUM_PROFILE=majority HOPNET_CONSENSUS_TIMEOUT_MS=2000 \
//!     orchestrator test --test consensus-leader-down
//!
//! (both env vars are forwarded into mesh containers at creation).

use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::NodeInfo;
use crate::tests::files::upload_file;
use crate::tests::persistence::{start_node, stop_node, wait_for_node_ready};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

// ============================================================================
// Helpers
// ============================================================================

/// Read one node's /consensus shim: (proposal-target height, proposer
/// node_id, last decided height).
async fn consensus_status(node: &NodeInfo) -> Result<(u64, u32, u64)> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/consensus", node.ip_address, node.port);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(3))
        .send()
        .await?;
    anyhow::ensure!(resp.status().is_success(), "/consensus {}", resp.status());
    let json: serde_json::Value = resp.json().await?;
    let height = json["view"].as_u64().unwrap_or(0);
    let proposer = json["leader"]["node_id"].as_u64().unwrap_or(0) as u32;
    let decided = json["last_decided_height"].as_u64().unwrap_or(0);
    Ok((height, proposer, decided))
}

async fn decided_height(node: &NodeInfo) -> Result<u64> {
    Ok(consensus_status(node).await?.2)
}

/// Refresh a NodeInfo's JWT after a restart. Node processes roll their JWT
/// signing key on every startup, so a pre-restart token hard-fails auth
/// (jwt_or_rpc middleware short-circuits on a Bearer header) — poll loops
/// would silently read 401 as "not caught up".
async fn refresh_jwt(node: &mut NodeInfo, mesh_id: u32) -> Result<()> {
    let docker = Docker::connect_with_local_defaults()?;
    node.jwt_token = crate::get_jwt_token(
        &docker,
        mesh_id,
        node.node_id,
        crate::sys::ContainerRuntime::Docker,
    )
    .await?;
    Ok(())
}

/// Wait until `node`'s decided height reaches `target`.
async fn wait_decided(node: &NodeInfo, target: u64, timeout: Duration) -> Result<bool> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Ok(false);
        }
        if let Ok(h) = decided_height(node).await
            && h >= target
        {
            return Ok(true);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Wait for a lagging/restarted `node` to catch up to a FIXED target tip.
///
/// On-demand heights mean a rejoined node stays PAUSED until a peer message
/// at a higher height wakes it (SyncNeeded → decided-value sync, which is
/// BULK — one wake pulls the whole gap). So: submit a couple of uploads to
/// advance + wake the mesh, snapshot the tip as a FIXED target, then poll the
/// laggard against that fixed target (re-nudging occasionally for liveness
/// WITHOUT moving the target — the earlier moving-tip version never
/// converged because each nudge advanced the very tip it was chasing).
async fn wait_caught_up_to_tip(
    laggard: &NodeInfo,
    live: &NodeInfo,
    dir: &str,
    timeout: Duration,
) -> Result<bool> {
    // Advance + wake, then freeze the target.
    for i in 0..2 {
        let _ = upload_file(live, dir, &format!("wake-{i}.txt"), vec![i as u8; 64]).await;
    }
    sleep(Duration::from_secs(2)).await;
    let target = decided_height(live).await.unwrap_or(0);

    let start = Instant::now();
    let mut nudge = 0;
    loop {
        if decided_height(laggard).await.unwrap_or(0) >= target && target > 0 {
            return Ok(true);
        }
        if start.elapsed() > timeout {
            return Ok(false);
        }
        sleep(Duration::from_secs(5)).await;
        // Liveness re-nudge if the wake gossip was missed — but only every
        // ~15s and the target stays fixed at its original snapshot.
        if nudge % 3 == 2 {
            let _ = upload_file(live, dir, &format!("renudge-{nudge}.txt"), vec![0xEE; 64]).await;
        }
        nudge += 1;
    }
}

/// Hold or release a consensus barrier on one node (test-mode HTTP routes).
async fn set_barrier(node: &NodeInfo, name: &str, action: &str) -> Result<()> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/test/barriers/consensus/{}/{}",
        node.ip_address, node.port, name, action
    );
    let resp = client
        .post(&url)
        .timeout(Duration::from_secs(3))
        .send()
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "barrier {action} {name} on node {}: {}",
        node.node_id,
        resp.status()
    );
    Ok(())
}

// ============================================================================
// consensus-leader-down
// ============================================================================

/// Stop the PENDING proposer of an idle mesh, then submit work elsewhere.
/// The wake rules (work-holder starts its height; peers wake on its messages)
/// plus round rotation must decide without the dead proposer.
/// Requires the majority profile (quorum 2 of 3).
pub struct ConsensusLeaderDown;

impl TestScenario for ConsensusLeaderDown {
    fn name(&self) -> &'static str {
        "consensus-leader-down"
    }
    fn description(&self) -> &'static str {
        "Idle mesh decides after its pending proposer dies (majority profile required)"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        let start = Instant::now();
        anyhow::ensure!(nodes.len() >= 3, "needs a 3-node mesh");
        let docker = Docker::connect_with_local_defaults()?;

        // The mesh is idle (on-demand heights) — read the pending proposer.
        let (height, proposer, decided_before) = consensus_status(&nodes[0]).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Read pending proposal target".to_string(),
                passed: true,
                detail: Some(format!(
                    "height {height}, proposer node {proposer}, decided {decided_before}"
                )),
            },
        );

        // Kill the proposer while the mesh is idle: no timers are armed
        // anywhere — the deadlock the wake rules must prevent (risk #18).
        stop_node(&docker, mesh_id, proposer).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Stopped pending proposer (node {proposer})"),
                passed: true,
                detail: None,
            },
        );

        // Submit work at a surviving node. Its engine starts the height, its
        // round-0 propose timeout expires, votes wake the other survivor,
        // rounds rotate past the dead proposer, and majority quorum decides.
        let submit_to = nodes
            .iter()
            .find(|n| n.node_id != proposer)
            .expect("a surviving node");
        let upload = upload_file(
            submit_to,
            "/leader-down",
            "survives.txt",
            vec![0x5A; 1024],
        )
        .await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload commits without the proposer".to_string(),
                passed: upload.is_ok(),
                detail: upload.as_ref().err().map(|e| e.to_string()),
            },
        );

        // Both survivors advance their decided height.
        let mut survivors_ok = true;
        for node in nodes.iter().filter(|n| n.node_id != proposer) {
            let reached = wait_decided(node, decided_before + 1, Duration::from_secs(60)).await?;
            survivors_ok &= reached;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Survivors decided past the dead proposer".to_string(),
                passed: survivors_ok,
                detail: None,
            },
        );

        // Bring the proposer back so the auto-managed divergence check sees a
        // full mesh; it restarts PAUSED (on-demand) and catches up once fresh
        // work wakes it — wait_caught_up_to_tip drives that.
        start_node(&docker, mesh_id, proposer).await?;
        let mut back = nodes.iter().find(|n| n.node_id == proposer).unwrap().clone();
        wait_for_node_ready(&back, Duration::from_secs(30)).await?;
        refresh_jwt(&mut back, mesh_id).await?; // rolled JWT key on restart
        let caught_up =
            wait_caught_up_to_tip(&back, submit_to, "/leader-down", Duration::from_secs(90)).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Restarted proposer catches up".to_string(),
                passed: caught_up,
                detail: None,
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// consensus-lagging-catch-up
// ============================================================================

/// A node offline for several decided heights syncs back to the tip once it
/// rejoins and the mesh wakes. Requires the majority profile.
pub struct ConsensusLaggingCatchUp;

impl TestScenario for ConsensusLaggingCatchUp {
    fn name(&self) -> &'static str {
        "consensus-lagging-catch-up"
    }
    fn description(&self) -> &'static str {
        "Offline node decided-value-syncs to the tip on rejoin (majority profile required)"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        let start = Instant::now();
        anyhow::ensure!(nodes.len() >= 3, "needs a 3-node mesh");
        let docker = Docker::connect_with_local_defaults()?;

        let mut laggard = nodes.last().unwrap().clone();

        stop_node(&docker, mesh_id, laggard.node_id).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Stopped node {}", laggard.node_id),
                passed: true,
                detail: None,
            },
        );

        // Advance the chain while it is down.
        let mut uploads_ok = 0;
        for i in 0..3 {
            if upload_file(
                &nodes[0],
                "/catch-up",
                &format!("while-down-{i}.txt"),
                vec![i as u8; 2048],
            )
            .await
            .is_ok()
            {
                uploads_ok += 1;
            }
        }
        let live_height = decided_height(&nodes[0]).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Chain advanced while node was down".to_string(),
                passed: uploads_ok == 3 && live_height > 0,
                detail: Some(format!("{uploads_ok}/3 uploads, tip {live_height}")),
            },
        );

        // Rejoin, then wake the mesh with fresh decides until the laggard
        // syncs to the (frozen) target tip.
        start_node(&docker, mesh_id, laggard.node_id).await?;
        wait_for_node_ready(&laggard, Duration::from_secs(30)).await?;
        refresh_jwt(&mut laggard, mesh_id).await?; // rolled JWT key on restart
        let synced =
            wait_caught_up_to_tip(&laggard, &nodes[0], "/catch-up", Duration::from_secs(120)).await?;
        let tip = decided_height(&nodes[0]).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Laggard reached the tip".to_string(),
                passed: synced,
                detail: Some(format!("tip {tip}")),
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// consensus-bft-quorum-loss
// ============================================================================

/// Negative control for the PINNED BFT profile: a 4-node BFT mesh has
/// quorum(4)=3, so with TWO down nothing may decide — and progress must
/// resume once they return. (Pinned bft + 4 nodes because a default-majority
/// mesh continues below full membership, and a 3-node BFT mesh cannot form
/// under mesh-initiated seating — RFC-CONSENSUS-002 S5.)
pub struct ConsensusBftQuorumLoss;

impl TestScenario for ConsensusBftQuorumLoss {
    fn name(&self) -> &'static str {
        "consensus-bft-quorum-loss"
    }
    fn description(&self) -> &'static str {
        "BFT 4-node mesh must NOT decide with two nodes down (pinned bft)"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        let start = Instant::now();
        anyhow::ensure!(nodes.len() >= 4, "needs a 4-node BFT mesh");
        let docker = Docker::connect_with_local_defaults()?;

        let decided_before = decided_height(&nodes[0]).await?;
        // Kill two of four: quorum(4) = 3 (BFT), so two down loses quorum.
        stop_node(&docker, mesh_id, nodes[3].node_id).await?;
        stop_node(&docker, mesh_id, nodes[2].node_id).await?;

        // Submit with a bounded wait: the upload must NOT commit (2-of-3 is
        // below the BFT quorum). The queue holds it; we only sample for 30s.
        let submit = tokio::time::timeout(
            Duration::from_secs(30),
            upload_file(&nodes[0], "/bft-neg", "must-stall.txt", vec![0x33; 512]),
        )
        .await;
        let stalled = !matches!(submit, Ok(Ok(()))); // timeout or error = good
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload stalls without BFT quorum".to_string(),
                passed: stalled,
                detail: Some(if stalled {
                    "no commit within 30s (expected)".to_string()
                } else {
                    "COMMITTED with 2/3 — quorum violation!".to_string()
                }),
            },
        );
        let decided_during = decided_height(&nodes[0]).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "No decide during quorum loss".to_string(),
                passed: decided_during == decided_before,
                detail: Some(format!("{decided_before} -> {decided_during}")),
            },
        );

        // Restore both validators: the held transaction (or a retry) must
        // now commit. (Below quorum, the vote-out scan itself cannot commit,
        // so the dead nodes are not removed — the mesh genuinely stalls.)
        start_node(&docker, mesh_id, nodes[2].node_id).await?;
        start_node(&docker, mesh_id, nodes[3].node_id).await?;
        wait_for_node_ready(&nodes[2], Duration::from_secs(30)).await?;
        wait_for_node_ready(&nodes[3], Duration::from_secs(30)).await?;
        let resumed = wait_decided(&nodes[0], decided_before + 1, Duration::from_secs(120)).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Progress resumes at full quorum".to_string(),
                passed: resumed,
                detail: None,
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// consensus-barrier-decide-window
// ============================================================================

/// Hold `before_decide` on one node: the quorum decides while that node's
/// commit stalls at the gate — an observable divergence window — then release
/// and it converges. Requires the majority profile (the held node must not be
/// needed for quorum).
pub struct ConsensusBarrierDecideWindow;

impl TestScenario for ConsensusBarrierDecideWindow {
    fn name(&self) -> &'static str {
        "consensus-barrier-decide-window"
    }
    fn description(&self) -> &'static str {
        "before_decide hold opens a divergence window, release converges (majority profile)"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        let start = Instant::now();
        anyhow::ensure!(nodes.len() >= 3, "needs a 3-node mesh");

        let held = nodes.last().unwrap();
        set_barrier(held, "before_decide", "hold").await?;
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Held before_decide on node {}", held.node_id),
                passed: true,
                detail: None,
            },
        );

        // Sample the pre-upload height from the HELD node: consensus is now
        // fast enough that the quorum can already be a height ahead of a
        // freshly-joined node at this instant, so nodes[0]'s height is not a
        // valid stand-in for the held node's.
        let decided_before = decided_height(held).await?;
        let quorum_before = decided_height(&nodes[0]).await?;
        upload_file(&nodes[0], "/decide-window", "window.txt", vec![0x77; 512]).await?;

        // Quorum (nodes 0,1) decides; the held node's decide is stuck at the
        // commit gate.
        let quorum_advanced =
            wait_decided(&nodes[0], quorum_before + 1, Duration::from_secs(60)).await?;
        let held_height = decided_height(held).await.unwrap_or(0);
        print_and_add_check(
            &mut result,
            Check {
                name: "Divergence window open".to_string(),
                passed: quorum_advanced && held_height == decided_before,
                detail: Some(format!(
                    "quorum at {}, held node at {held_height} (held before: {decided_before})",
                    quorum_before + 1
                )),
            },
        );

        set_barrier(held, "before_decide", "release").await?;
        let converged = wait_decided(held, decided_before + 1, Duration::from_secs(60)).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Held node converges after release".to_string(),
                passed: converged,
                detail: None,
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// consensus-barrier-proposal-hold
// ============================================================================

/// Hold `before_publish_proposal` on the pending proposer: its value never
/// reaches the mesh, so nothing commits; release lets a later round carry the
/// transaction through. Requires the majority profile.
pub struct ConsensusBarrierProposalHold;

impl TestScenario for ConsensusBarrierProposalHold {
    fn name(&self) -> &'static str {
        "consensus-barrier-proposal-hold"
    }
    fn description(&self) -> &'static str {
        "before_publish_proposal hold stalls commits; release recovers (majority profile)"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        let start = Instant::now();
        anyhow::ensure!(nodes.len() >= 3, "needs a 3-node mesh");

        let (_, proposer, decided_before) = consensus_status(&nodes[0]).await?;
        let held = nodes
            .iter()
            .find(|n| n.node_id == proposer)
            .expect("proposer in mesh");
        set_barrier(held, "before_publish_proposal", "hold").await?;
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Held before_publish_proposal on proposer node {proposer}"),
                passed: true,
                detail: None,
            },
        );

        // Submit AT the proposer: it builds a block, but the publisher holds
        // the full-value message — no peer can validate, nothing commits.
        let submit = tokio::time::timeout(
            Duration::from_secs(20),
            upload_file(held, "/proposal-hold", "held.txt", vec![0x99; 512]),
        )
        .await;
        let stalled = !matches!(submit, Ok(Ok(())));
        print_and_add_check(
            &mut result,
            Check {
                name: "Commit stalls while the proposal is held".to_string(),
                passed: stalled,
                detail: Some(format!(
                    "decided still {}",
                    decided_height(&nodes[0]).await.unwrap_or(0)
                )),
            },
        );

        set_barrier(held, "before_publish_proposal", "release").await?;
        // While held, OTHER proposers may decide empty blocks (the height
        // still moves) — so the recovery assertion is on the DATA: the held
        // node's re-staged transaction must commit and the file must be
        // servable from a peer. (`_ = decided_before`: heights alone can't
        // prove recovery here.)
        let _ = decided_before;
        let fetched = crate::tests::files::download_file_with_timeout(
            &nodes[(proposer as usize + 1) % nodes.len()],
            "/proposal-hold/held.txt",
            Duration::from_secs(90),
        )
        .await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Held transaction commits after release".to_string(),
                passed: matches!(&fetched, Ok(bytes) if bytes == &vec![0x99u8; 512]),
                detail: fetched.as_ref().err().map(|e| e.to_string()),
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
