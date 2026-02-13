use anyhow::Result;
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::tests::files::upload_file;
use crate::NodeInfo;

// ============================================================================
// Barrier HTTP helpers
// ============================================================================

async fn barrier_hold(node: &NodeInfo, barrier_name: &str) -> Result<()> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/test/barrier/{}/hold",
        node.ip_address, node.port, barrier_name
    );
    let resp = client.post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("barrier hold failed: {}", resp.status());
    }
    Ok(())
}

async fn barrier_release(node: &NodeInfo, barrier_name: &str) -> Result<()> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/test/barrier/{}/release",
        node.ip_address, node.port, barrier_name
    );
    let resp = client.post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("barrier release failed: {}", resp.status());
    }
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct BarrierStatus {
    held: bool,
    waiting: bool,
}

async fn barrier_status(node: &NodeInfo, barrier_name: &str) -> Result<BarrierStatus> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/test/barrier/{}/status",
        node.ip_address, node.port, barrier_name
    );
    let resp = client.get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("barrier status failed: {}", resp.status());
    }
    Ok(resp.json().await?)
}

/// Poll until the barrier shows `waiting=true` on the given node, or timeout.
async fn wait_for_barrier_waiting(
    node: &NodeInfo,
    barrier_name: &str,
    timeout: Duration,
) -> Result<bool> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Ok(false);
        }
        if let Ok(status) = barrier_status(node, barrier_name).await {
            if status.waiting {
                return Ok(true);
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

// ============================================================================
// Helpers shared across barrier tests
// ============================================================================

async fn get_leader_node_id(node: &NodeInfo) -> Result<u32> {
    let client = Client::new();
    let url = format!("http://{}:{}/consensus", node.ip_address, node.port);
    let resp = client.get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("consensus query failed: {}", resp.status());
    }
    let json: serde_json::Value = resp.json().await?;
    let leader_id = json["leader"]["node_id"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("leader.node_id not found"))?;
    Ok(leader_id as u32)
}

/// Find which node is waiting at a barrier (polls all nodes).
async fn find_waiting_node<'a>(
    nodes: &'a [NodeInfo],
    barrier_name: &str,
    timeout: Duration,
) -> Option<&'a NodeInfo> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return None;
        }
        for node in nodes {
            if let Ok(status) = barrier_status(node, barrier_name).await {
                if status.waiting {
                    return Some(node);
                }
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

#[derive(Debug, serde::Deserialize)]
struct ViewHistoryEntry {
    view: i64,
    has_propose_qc: bool,
    has_lock_qc: bool,
    has_tc: bool,
    #[serde(flatten)]
    _rest: serde_json::Value,
}

async fn get_consensus_history(node: &NodeInfo) -> Result<Vec<ViewHistoryEntry>> {
    let client = Client::new();
    let url = format!("http://{}:{}/consensus/history", node.ip_address, node.port);
    let resp = client.get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("consensus history query failed: {}", resp.status());
    }
    Ok(resp.json().await?)
}

// ============================================================================
// Test: consensus-barrier-basic
// ============================================================================

/// Verifies barrier mechanism works: hold leader between Propose and Lock,
/// confirm Propose QC propagated, then release and confirm Lock QC completes.
pub struct ConsensusBarrierBasic;

impl TestScenario for ConsensusBarrierBasic {
    fn name(&self) -> &'static str { "consensus-barrier-basic" }
    fn description(&self) -> &'static str {
        "Verify barrier hold/release works by pausing leader between Propose and Lock phases"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(&mut result, Check {
                name: "Insufficient nodes".into(),
                passed: false,
                detail: Some(format!("Need >=3, found {}", nodes.len())),
            });
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 1: Hold barrier on ALL nodes (only the leader hits after_propose_qc_broadcast)
        let barrier_name = "after_propose_qc_broadcast";
        for node in nodes {
            if let Err(e) = barrier_hold(node, barrier_name).await {
                print_and_add_check(&mut result, Check {
                    name: format!("Hold barrier on node {}", node.node_id),
                    passed: false, detail: Some(e.to_string()),
                });
                for n in nodes { let _ = barrier_release(n, barrier_name).await; }
                result.duration = start.elapsed();
                return Ok(result);
            }
        }
        print_and_add_check(&mut result, Check {
            name: "Hold barrier on all nodes".into(), passed: true, detail: None,
        });

        // Step 2: Upload file to trigger consensus (spawned in background — the barrier
        // blocks consensus_middleware, so the upload HTTP response won't arrive until release)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
        let filename = format!("barrier-basic-{}.txt", timestamp);
        let contents = format!("barrier test {}", timestamp).into_bytes();

        let upload_node = nodes[0].clone();
        let upload_filename = filename.clone();
        let upload_handle = tokio::spawn(async move {
            upload_file(&upload_node, "/", &upload_filename, contents).await
        });

        // Step 3: Find which node is waiting — that's the leader
        println!("  ... waiting for leader to reach barrier");
        let leader_node = match find_waiting_node(nodes, barrier_name, Duration::from_secs(30)).await {
            Some(n) => {
                print_and_add_check(&mut result, Check {
                    name: "Leader reached barrier".into(), passed: true,
                    detail: Some(format!("node {} waiting after Propose QC broadcast", n.node_id)),
                });
                n
            }
            None => {
                print_and_add_check(&mut result, Check {
                    name: "Leader reached barrier".into(), passed: false,
                    detail: Some("no node reached waiting=true within 30s".into()),
                });
                for n in nodes { let _ = barrier_release(n, barrier_name).await; }
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let followers: Vec<&NodeInfo> = nodes.iter()
            .filter(|n| n.node_id != leader_node.node_id)
            .collect();

        // Step 4: Get the view the leader is proposing for
        let target_view = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(_) => {
                // Fallback: query the leader's consensus state
                get_leader_node_id(leader_node).await.ok();
                0 // Will be checked in verification
            }
        };

        // Step 5: Verify followers have the Propose QC
        // The QC broadcast returns after quorum (1 follower in a 3-node mesh), so the
        // second follower may still be receiving it. Poll with retries to account for this.
        let mut propose_qc_ok = true;
        for follower in &followers {
            let mut has_propose = false;
            for attempt in 0..10 {
                if attempt > 0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                match get_consensus_history(follower).await {
                    Ok(history) => {
                        has_propose = history.iter().any(|e| {
                            e.view == target_view as i64 && e.has_propose_qc
                        });
                        if has_propose { break; }
                    }
                    Err(_) => {}
                }
            }
            if !has_propose {
                propose_qc_ok = false;
                print_and_add_check(&mut result, Check {
                    name: format!("Propose QC on node {}", follower.node_id),
                    passed: false,
                    detail: Some(format!("view {} missing propose QC after retries", target_view)),
                });
            }
        }
        if propose_qc_ok {
            print_and_add_check(&mut result, Check {
                name: "Followers have Propose QC".into(), passed: true,
                detail: Some(format!("view {} has_propose_qc=true on {} followers", target_view, followers.len())),
            });
        }

        // Step 6: Release barriers on all nodes
        for n in nodes { let _ = barrier_release(n, barrier_name).await; }
        print_and_add_check(&mut result, Check {
            name: "Release barriers".into(), passed: true, detail: None,
        });

        // Step 7: Wait for view to advance (Lock phase completes)
        let next_view = target_view + 1;
        match wait_for_minimum_view(nodes, next_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "View advanced after release".into(), passed: true,
                    detail: Some(format!("all nodes reached view {}", next_view)),
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "View advanced after release".into(), passed: false,
                    detail: Some(format!("not all nodes reached view {} in 30s", next_view)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "View advance check".into(), passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // Verify the background upload completed successfully
        match upload_handle.await {
            Ok(Ok(_)) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload completed after release".into(), passed: true, detail: None,
                });
            }
            Ok(Err(e)) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload completed after release".into(), passed: false,
                    detail: Some(e.to_string()),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload task".into(), passed: false,
                    detail: Some(format!("join error: {}", e)),
                });
            }
        }

        // Step 8: Verify Lock QC on all nodes
        let mut lock_qc_ok = true;
        for node in nodes {
            match get_consensus_history(node).await {
                Ok(history) => {
                    let has_lock = history.iter().any(|e| {
                        e.view == target_view as i64 && e.has_lock_qc
                    });
                    if !has_lock {
                        lock_qc_ok = false;
                        print_and_add_check(&mut result, Check {
                            name: format!("Lock QC on node {}", node.node_id),
                            passed: false,
                            detail: Some(format!("view {} missing lock QC", target_view)),
                        });
                    }
                }
                Err(e) => {
                    lock_qc_ok = false;
                    print_and_add_check(&mut result, Check {
                        name: format!("Lock QC check node {}", node.node_id),
                        passed: false, detail: Some(e.to_string()),
                    });
                }
            }
        }
        if lock_qc_ok {
            print_and_add_check(&mut result, Check {
                name: "All nodes have Lock QC".into(), passed: true,
                detail: Some(format!("view {} has_lock_qc=true on all nodes", target_view)),
            });
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Test: consensus-barrier-missed-ballot
// ============================================================================

/// Tests that a node missing a ballot round catches up via message-driven catch-up.
pub struct ConsensusBarrierMissedBallot;

impl TestScenario for ConsensusBarrierMissedBallot {
    fn name(&self) -> &'static str { "consensus-barrier-missed-ballot" }
    fn description(&self) -> &'static str {
        "Hold a follower's ballot dispatch, let consensus complete without it, then release and verify catch-up"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(&mut result, Check {
                name: "Insufficient nodes".into(), passed: false,
                detail: Some(format!("Need >=3, found {}", nodes.len())),
            });
            result.duration = start.elapsed();
            return Ok(result);
        }

        let barrier_name = "before_ballot_dispatch";

        // Step 1: Hold barrier on ALL nodes (only followers receive BallotSubmission)
        for node in nodes {
            if let Err(e) = barrier_hold(node, barrier_name).await {
                print_and_add_check(&mut result, Check {
                    name: format!("Hold barrier on node {}", node.node_id),
                    passed: false, detail: Some(e.to_string()),
                });
                for n in nodes { let _ = barrier_release(n, barrier_name).await; }
                result.duration = start.elapsed();
                return Ok(result);
            }
        }
        print_and_add_check(&mut result, Check {
            name: "Hold ballot dispatch barrier on all nodes".into(), passed: true, detail: None,
        });

        // Step 2: Upload file — followers will block at ballot dispatch.
        // Spawned in background because consensus_middleware blocks until ballots complete,
        // and followers are held at the barrier.
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
        let filename = format!("barrier-missed-{}.txt", timestamp);
        let contents = format!("missed ballot test {}", timestamp).into_bytes();

        let upload_node = nodes[0].clone();
        let upload_filename = filename.clone();
        let upload_handle = tokio::spawn(async move {
            upload_file(&upload_node, "/", &upload_filename, contents).await
        });

        // Step 3: Find the first follower that hits the barrier
        println!("  ... waiting for a follower to reach barrier");
        let first_follower = match find_waiting_node(nodes, barrier_name, Duration::from_secs(30)).await {
            Some(n) => {
                print_and_add_check(&mut result, Check {
                    name: "First follower at barrier".into(), passed: true,
                    detail: Some(format!("node {} waiting", n.node_id)),
                });
                n.clone()
            }
            None => {
                print_and_add_check(&mut result, Check {
                    name: "First follower at barrier".into(), passed: false,
                    detail: Some("no node reached waiting=true within 30s".into()),
                });
                for n in nodes { let _ = barrier_release(n, barrier_name).await; }
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 4: Release this follower so it can vote (gives leader 2/3 quorum)
        let _ = barrier_release(&first_follower, barrier_name).await;
        print_and_add_check(&mut result, Check {
            name: "Release first follower".into(), passed: true,
            detail: Some(format!("node {} released to vote", first_follower.node_id)),
        });

        // Step 5: Give consensus a moment to complete with the released follower
        sleep(Duration::from_secs(3)).await;

        // Verify the first upload completed (leader got quorum from released follower)
        match upload_handle.await {
            Ok(Ok(_)) => {
                print_and_add_check(&mut result, Check {
                    name: "First upload completed".into(), passed: true, detail: None,
                });
            }
            Ok(Err(e)) => {
                print_and_add_check(&mut result, Check {
                    name: "First upload completed".into(), passed: false,
                    detail: Some(e.to_string()),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "First upload task".into(), passed: false,
                    detail: Some(format!("join error: {}", e)),
                });
            }
        }

        // Identify the held follower: whichever node (other than first_follower) is still waiting
        let held_node = {
            let mut held = None;
            for node in nodes {
                if node.node_id == first_follower.node_id { continue; }
                if let Ok(status) = barrier_status(node, barrier_name).await {
                    if status.waiting {
                        held = Some(node);
                        break;
                    }
                }
            }
            match held {
                Some(n) => {
                    print_and_add_check(&mut result, Check {
                        name: "Identify held follower".into(), passed: true,
                        detail: Some(format!("node {} still held at barrier", n.node_id)),
                    });
                    n
                }
                None => {
                    // No node is waiting — both followers already processed.
                    // This means the second follower voted before we could observe it,
                    // or the leader is the one we think is held. Continue gracefully.
                    print_and_add_check(&mut result, Check {
                        name: "Identify held follower".into(), passed: false,
                        detail: Some("no node still waiting — barrier may not have caught second follower".into()),
                    });
                    for n in nodes { let _ = barrier_release(n, barrier_name).await; }
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        };

        // The leader is the remaining node
        let leader_id = nodes.iter()
            .find(|n| n.node_id != first_follower.node_id && n.node_id != held_node.node_id)
            .map(|n| n.node_id)
            .unwrap_or(0);

        // Step 6: Wait for view to advance on non-held nodes
        let non_held_nodes: Vec<NodeInfo> = nodes.iter()
            .filter(|n| n.node_id != held_node.node_id)
            .cloned()
            .collect();

        let view_after_first = match get_max_view(&non_held_nodes).await {
            Ok(v) => v,
            Err(_) => 0,
        };

        match wait_for_minimum_view(&non_held_nodes, view_after_first, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "Non-held nodes advanced".into(), passed: true,
                    detail: Some(format!("leader={}, released follower={}, view {}", leader_id, first_follower.node_id, view_after_first)),
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Non-held nodes advanced".into(), passed: false,
                    detail: Some("did not advance in 30s".into()),
                });
                for n in nodes { let _ = barrier_release(n, barrier_name).await; }
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "View check".into(), passed: false,
                    detail: Some(e.to_string()),
                });
                for n in nodes { let _ = barrier_release(n, barrier_name).await; }
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 7: Release held follower — stale ballots process
        let _ = barrier_release(held_node, barrier_name).await;
        print_and_add_check(&mut result, Check {
            name: "Release held follower".into(), passed: true,
            detail: Some(format!("node {} released", held_node.node_id)),
        });

        // Step 8: Upload another file to trigger catch-up via message-driven dispatch
        let filename2 = format!("barrier-missed2-{}.txt", timestamp);
        let contents2 = format!("catch-up trigger {}", timestamp).into_bytes();

        match upload_file(&nodes[0], "/", &filename2, contents2).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload second file (catch-up trigger)".into(), passed: true, detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload second file".into(), passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 9: Wait for ALL nodes (including previously held) to converge
        let target_view = view_after_first + 1;
        match wait_for_minimum_view(nodes, target_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "All nodes caught up".into(), passed: true,
                    detail: Some(format!("all at view {}", target_view)),
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "All nodes caught up".into(), passed: false,
                    detail: Some(format!("not all nodes reached view {} in 30s", target_view)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Catch-up check".into(), passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Test: consensus-barrier-tc-qc-race (Scenario A)
// ============================================================================

/// Tests the Lock QC vs TC safety property: Lock QC arrives while TC is pending.
///
/// Flow:
/// 1. Warm up with a consensus round, then hold both `before_lock_qc_broadcast`
///    and `before_tc_gst_wait` barriers on all nodes.
/// 2. Trigger a new consensus round — leader forms Lock QC and hits the barrier.
/// 3. Wait ~130s for followers to form TC and hit the TC GST wait barrier.
/// 4. Release `before_lock_qc_broadcast` on the leader — Lock QC broadcasts and
///    propagates to followers (their consensus_lock is free since TC is held at barrier).
/// 5. Release `before_tc_gst_wait` on all nodes — Layer 2 staleness check catches
///    that the view already advanced, rejecting TC.
/// 6. Verify: Lock QC present, TC absent for the competing view on all nodes.
pub struct ConsensusBarrierTcQcRace;

impl TestScenario for ConsensusBarrierTcQcRace {
    fn name(&self) -> &'static str { "consensus-barrier-tc-qc-race" }
    fn description(&self) -> &'static str {
        "Verify Lock QC wins over TC when Lock QC arrives during GST wait (safety property)"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(&mut result, Check {
                name: "Insufficient nodes".into(), passed: false,
                detail: Some(format!("Need >=3, found {}", nodes.len())),
            });
            result.duration = start.elapsed();
            return Ok(result);
        }

        let lock_qc_barrier = "before_lock_qc_broadcast";
        let tc_barrier = "before_tc_gst_wait";

        // Helper closure for cleanup
        let release_all = |nodes: &[NodeInfo]| {
            let nodes = nodes.to_vec();
            async move {
                for n in &nodes {
                    let _ = barrier_release(n, lock_qc_barrier).await;
                    let _ = barrier_release(n, tc_barrier).await;
                }
            }
        };

        // Step 1: Warm-up consensus round
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
        let warmup_filename = format!("barrier-tcqc-warmup-{}.txt", timestamp);
        let warmup_contents = format!("warmup {}", timestamp).into_bytes();
        match upload_file(&nodes[0], "/", &warmup_filename, warmup_contents).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Warm-up consensus round".into(), passed: true, detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Warm-up round".into(), passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        let warmup_view = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Get warmup view".into(), passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match wait_for_minimum_view(nodes, warmup_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "Warm-up complete".into(), passed: true,
                    detail: Some(format!("all at view {}", warmup_view)),
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "Warm-up complete".into(), passed: false,
                    detail: Some("warm-up round did not complete".into()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Extra settle time for cascade settling
        sleep(Duration::from_secs(2)).await;

        // Step 2: Hold both barriers on ALL nodes
        for node in nodes {
            if let Err(e) = barrier_hold(node, lock_qc_barrier).await {
                print_and_add_check(&mut result, Check {
                    name: format!("Hold Lock QC barrier on node {}", node.node_id),
                    passed: false, detail: Some(e.to_string()),
                });
                release_all(nodes).await;
                result.duration = start.elapsed();
                return Ok(result);
            }
            if let Err(e) = barrier_hold(node, tc_barrier).await {
                print_and_add_check(&mut result, Check {
                    name: format!("Hold TC barrier on node {}", node.node_id),
                    passed: false, detail: Some(e.to_string()),
                });
                release_all(nodes).await;
                result.duration = start.elapsed();
                return Ok(result);
            }
        }
        print_and_add_check(&mut result, Check {
            name: "Hold barriers on all nodes".into(), passed: true,
            detail: Some("before_lock_qc_broadcast + before_tc_gst_wait".into()),
        });

        // Step 3: Upload file to trigger consensus (background — leader will block at barrier)
        let filename = format!("barrier-tcqc-race-{}.txt", timestamp);
        let contents = format!("tc-qc race test {}", timestamp).into_bytes();
        let upload_node = nodes[0].clone();
        let upload_filename = filename.clone();
        let upload_handle = tokio::spawn(async move {
            upload_file(&upload_node, "/", &upload_filename, contents).await
        });

        // Step 4: Poll all nodes for before_lock_qc_broadcast waiting=true → identifies leader
        println!("  ... waiting for leader to form Lock QC and hit barrier");
        let leader_node = match find_waiting_node(nodes, lock_qc_barrier, Duration::from_secs(30)).await {
            Some(n) => {
                print_and_add_check(&mut result, Check {
                    name: "Leader formed Lock QC".into(), passed: true,
                    detail: Some(format!("node {} waiting at before_lock_qc_broadcast", n.node_id)),
                });
                n
            }
            None => {
                print_and_add_check(&mut result, Check {
                    name: "Leader formed Lock QC".into(), passed: false,
                    detail: Some("no node reached before_lock_qc_broadcast within 30s".into()),
                });
                release_all(nodes).await;
                let _ = upload_handle.await;
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let followers: Vec<&NodeInfo> = nodes.iter()
            .filter(|n| n.node_id != leader_node.node_id)
            .collect();

        // The competing view is the next view after warmup
        let competing_view = warmup_view + 1;

        // Step 5: Wait for non-leader nodes to form TC and hit before_tc_gst_wait
        println!("  ... waiting up to 130s for followers to form TC");
        let tc_waiting = {
            let mut count = 0;
            let deadline = Instant::now() + Duration::from_secs(130);
            while Instant::now() < deadline {
                count = 0;
                for node in &followers {
                    if let Ok(status) = barrier_status(node, tc_barrier).await {
                        if status.waiting {
                            count += 1;
                        }
                    }
                }
                if count >= followers.len() { break; }
                sleep(Duration::from_secs(2)).await;
            }
            count
        };

        if tc_waiting >= followers.len() {
            print_and_add_check(&mut result, Check {
                name: "TC formed on followers".into(), passed: true,
                detail: Some(format!("{} followers waiting at before_tc_gst_wait", tc_waiting)),
            });
        } else {
            print_and_add_check(&mut result, Check {
                name: "TC formed on followers".into(), passed: false,
                detail: Some(format!("only {}/{} followers reached TC barrier within 130s", tc_waiting, followers.len())),
            });
            release_all(nodes).await;
            let _ = upload_handle.await;
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 6: Release before_lock_qc_broadcast on leader → Lock QC broadcasts
        // Leader also applies Lock QC locally (DB insert + transaction processing)
        let _ = barrier_release(leader_node, lock_qc_barrier).await;
        print_and_add_check(&mut result, Check {
            name: "Release Lock QC barrier on leader".into(), passed: true,
            detail: Some(format!("node {} broadcasting Lock QC", leader_node.node_id)),
        });

        // Step 7: Give Lock QC time to propagate to followers
        // Followers' consensus_lock is NOT held (TC is blocked before GST wait, before lock acquisition)
        // So Lock QC can be processed via process_incoming_qc(), advancing the view
        sleep(Duration::from_secs(2)).await;

        // Step 8: Release before_tc_gst_wait on all nodes → TC enters GST wait
        // Layer 2 staleness check: TC view < current_view → rejected
        for node in nodes {
            let _ = barrier_release(node, tc_barrier).await;
        }
        print_and_add_check(&mut result, Check {
            name: "Release TC barriers on all nodes".into(), passed: true, detail: None,
        });

        // Step 9: Settlement — wait for everything to finish
        sleep(Duration::from_secs(5)).await;

        // Wait for upload to complete
        match upload_handle.await {
            Ok(Ok(_)) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload completed".into(), passed: true, detail: None,
                });
            }
            Ok(Err(e)) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload completed".into(), passed: false,
                    detail: Some(e.to_string()),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload task".into(), passed: false,
                    detail: Some(format!("join error: {}", e)),
                });
            }
        }

        // Step 10: Verify Lock QC won and TC was rejected on all nodes
        let mut lock_qc_ok = true;
        let mut tc_absent = true;
        for node in nodes {
            match get_consensus_history(node).await {
                Ok(history) => {
                    let entry = history.iter().find(|e| e.view == competing_view as i64);
                    match entry {
                        Some(e) => {
                            if !e.has_lock_qc {
                                lock_qc_ok = false;
                                print_and_add_check(&mut result, Check {
                                    name: format!("Lock QC on node {}", node.node_id),
                                    passed: false,
                                    detail: Some(format!("view {} missing Lock QC", competing_view)),
                                });
                            }
                            if e.has_tc {
                                tc_absent = false;
                                print_and_add_check(&mut result, Check {
                                    name: format!("TC absent on node {}", node.node_id),
                                    passed: false,
                                    detail: Some(format!("view {} has TC (should have been rejected)", competing_view)),
                                });
                            }
                        }
                        None => {
                            lock_qc_ok = false;
                            print_and_add_check(&mut result, Check {
                                name: format!("History for node {}", node.node_id),
                                passed: false,
                                detail: Some(format!("view {} not found in history", competing_view)),
                            });
                        }
                    }
                }
                Err(e) => {
                    lock_qc_ok = false;
                    print_and_add_check(&mut result, Check {
                        name: format!("History check node {}", node.node_id),
                        passed: false, detail: Some(e.to_string()),
                    });
                }
            }
        }

        if lock_qc_ok {
            print_and_add_check(&mut result, Check {
                name: "Lock QC present on all nodes".into(), passed: true,
                detail: Some(format!("view {} has Lock QC on all nodes", competing_view)),
            });
        }
        if tc_absent {
            print_and_add_check(&mut result, Check {
                name: "TC rejected on all nodes".into(), passed: true,
                detail: Some(format!("view {} has no TC on any node (safety property holds)", competing_view)),
            });
        }

        // Verify all nodes advanced past the competing view
        match wait_for_minimum_view(nodes, competing_view + 1, Duration::from_secs(10)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "All nodes advanced".into(), passed: true,
                    detail: Some(format!("all at view >= {}", competing_view + 1)),
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "All nodes advanced".into(), passed: false,
                    detail: Some(format!("not all nodes reached view {}", competing_view + 1)),
                });
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Test: consensus-barrier-tc-late (Scenario B)
// ============================================================================

/// Tests behavior when TC commits before Lock QC arrives (diagnostic test).
///
/// Flow:
/// 1. Warm up, then hold only `before_lock_qc_broadcast` on all nodes.
/// 2. Trigger consensus — leader forms Lock QC and hits barrier.
/// 3. Wait ~130s for followers to form and fully apply TC (no TC barrier held).
/// 4. Release `before_lock_qc_broadcast` — leader broadcasts Lock QC + applies locally.
/// 5. Verify convergence: do all nodes have same committed state? Is there a
///    `timeout_certificates` table mismatch (leader has no TC, followers do)?
///
/// This test is diagnostic — it reveals whether the current implementation correctly
/// handles the "too late" case or if a fix is needed for metadata divergence.
pub struct ConsensusBarrierTcLate;

impl TestScenario for ConsensusBarrierTcLate {
    fn name(&self) -> &'static str { "consensus-barrier-tc-late" }
    fn description(&self) -> &'static str {
        "Diagnostic: TC commits before Lock QC arrives — check for metadata divergence"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(&mut result, Check {
                name: "Insufficient nodes".into(), passed: false,
                detail: Some(format!("Need >=3, found {}", nodes.len())),
            });
            result.duration = start.elapsed();
            return Ok(result);
        }

        let lock_qc_barrier = "before_lock_qc_broadcast";

        // Step 1: Warm-up consensus round
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
        let warmup_filename = format!("barrier-tclate-warmup-{}.txt", timestamp);
        let warmup_contents = format!("warmup {}", timestamp).into_bytes();
        match upload_file(&nodes[0], "/", &warmup_filename, warmup_contents).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Warm-up consensus round".into(), passed: true, detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Warm-up round".into(), passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        let warmup_view = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Get warmup view".into(), passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match wait_for_minimum_view(nodes, warmup_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "Warm-up complete".into(), passed: true,
                    detail: Some(format!("all at view {}", warmup_view)),
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "Warm-up complete".into(), passed: false,
                    detail: Some("warm-up round did not complete".into()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Extra settle time
        sleep(Duration::from_secs(2)).await;

        // Step 2: Hold ONLY before_lock_qc_broadcast on all nodes (do NOT hold TC barrier)
        for node in nodes {
            if let Err(e) = barrier_hold(node, lock_qc_barrier).await {
                print_and_add_check(&mut result, Check {
                    name: format!("Hold Lock QC barrier on node {}", node.node_id),
                    passed: false, detail: Some(e.to_string()),
                });
                for n in nodes { let _ = barrier_release(n, lock_qc_barrier).await; }
                result.duration = start.elapsed();
                return Ok(result);
            }
        }
        print_and_add_check(&mut result, Check {
            name: "Hold Lock QC barriers on all nodes".into(), passed: true,
            detail: Some("before_lock_qc_broadcast only (TC barriers NOT held)".into()),
        });

        // Step 3: Upload file to trigger consensus (background)
        let filename = format!("barrier-tclate-{}.txt", timestamp);
        let contents = format!("tc-late test {}", timestamp).into_bytes();
        let upload_node = nodes[0].clone();
        let upload_filename = filename.clone();
        let upload_handle = tokio::spawn(async move {
            upload_file(&upload_node, "/", &upload_filename, contents).await
        });

        // Step 4: Poll all nodes for before_lock_qc_broadcast waiting=true → identifies leader
        println!("  ... waiting for leader to form Lock QC and hit barrier");
        let leader_node = match find_waiting_node(nodes, lock_qc_barrier, Duration::from_secs(30)).await {
            Some(n) => {
                print_and_add_check(&mut result, Check {
                    name: "Leader formed Lock QC".into(), passed: true,
                    detail: Some(format!("node {} waiting at before_lock_qc_broadcast", n.node_id)),
                });
                n
            }
            None => {
                print_and_add_check(&mut result, Check {
                    name: "Leader formed Lock QC".into(), passed: false,
                    detail: Some("no node reached before_lock_qc_broadcast within 30s".into()),
                });
                for n in nodes { let _ = barrier_release(n, lock_qc_barrier).await; }
                let _ = upload_handle.await;
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let followers: Vec<&NodeInfo> = nodes.iter()
            .filter(|n| n.node_id != leader_node.node_id)
            .collect();

        let competing_view = warmup_view + 1;

        // Step 5: Wait ~130s for TC to form AND apply on followers
        // No TC barrier held — TC goes through all 3 layers and commits since
        // Lock QC isn't in any DB (leader is held at barrier before broadcast/DB write)
        println!("  ... waiting up to 140s for TC to form and apply on followers");
        let tc_applied = {
            let mut all_applied = false;
            let deadline = Instant::now() + Duration::from_secs(140);
            while Instant::now() < deadline {
                let mut count = 0;
                for node in &followers {
                    if let Ok(history) = get_consensus_history(node).await {
                        if history.iter().any(|e| e.view == competing_view as i64 && e.has_tc) {
                            count += 1;
                        }
                    }
                }
                if count >= followers.len() {
                    all_applied = true;
                    break;
                }
                sleep(Duration::from_secs(2)).await;
            }
            all_applied
        };

        if tc_applied {
            print_and_add_check(&mut result, Check {
                name: "TC applied on followers".into(), passed: true,
                detail: Some(format!("view {} has TC on all {} followers", competing_view, followers.len())),
            });
        } else {
            print_and_add_check(&mut result, Check {
                name: "TC applied on followers".into(), passed: false,
                detail: Some(format!("not all followers applied TC for view {} within 140s", competing_view)),
            });
            for n in nodes { let _ = barrier_release(n, lock_qc_barrier).await; }
            let _ = upload_handle.await;
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 6: Release before_lock_qc_broadcast → leader broadcasts Lock QC + applies locally
        for n in nodes { let _ = barrier_release(n, lock_qc_barrier).await; }
        print_and_add_check(&mut result, Check {
            name: "Release Lock QC barrier".into(), passed: true,
            detail: Some("leader broadcasting Lock QC (TC already committed on followers)".into()),
        });

        // Step 7: Wait for Lock QC propagation and settlement
        sleep(Duration::from_secs(5)).await;

        // Wait for upload to complete
        match upload_handle.await {
            Ok(Ok(_)) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload completed".into(), passed: true, detail: None,
                });
            }
            Ok(Err(e)) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload completed".into(), passed: false,
                    detail: Some(e.to_string()),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload task".into(), passed: false,
                    detail: Some(format!("join error: {}", e)),
                });
            }
        }

        // Step 8: Diagnostic checks — examine the state of each node
        // Leader should have Lock QC but may NOT have TC (stale TC rejected by Layer 2)
        // Followers should have TC, and Lock QC (from broadcast via process_incoming_qc)
        let mut leader_has_lock_qc = false;
        let mut leader_has_tc = false;
        let mut follower_lock_qc_count = 0;
        let mut follower_tc_count = 0;

        // Check leader
        match get_consensus_history(leader_node).await {
            Ok(history) => {
                if let Some(entry) = history.iter().find(|e| e.view == competing_view as i64) {
                    leader_has_lock_qc = entry.has_lock_qc;
                    leader_has_tc = entry.has_tc;
                    print_and_add_check(&mut result, Check {
                        name: "Leader state".into(), passed: true,
                        detail: Some(format!(
                            "view {}: has_lock_qc={}, has_tc={}",
                            competing_view, entry.has_lock_qc, entry.has_tc
                        )),
                    });
                } else {
                    print_and_add_check(&mut result, Check {
                        name: "Leader state".into(), passed: false,
                        detail: Some(format!("view {} not found in leader history", competing_view)),
                    });
                }
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Leader history".into(), passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // Check followers
        for node in &followers {
            match get_consensus_history(node).await {
                Ok(history) => {
                    if let Some(entry) = history.iter().find(|e| e.view == competing_view as i64) {
                        if entry.has_lock_qc { follower_lock_qc_count += 1; }
                        if entry.has_tc { follower_tc_count += 1; }
                        print_and_add_check(&mut result, Check {
                            name: format!("Follower {} state", node.node_id), passed: true,
                            detail: Some(format!(
                                "view {}: has_lock_qc={}, has_tc={}",
                                competing_view, entry.has_lock_qc, entry.has_tc
                            )),
                        });
                    } else {
                        print_and_add_check(&mut result, Check {
                            name: format!("Follower {} state", node.node_id), passed: false,
                            detail: Some(format!("view {} not found in history", competing_view)),
                        });
                    }
                }
                Err(e) => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Follower {} history", node.node_id), passed: false,
                        detail: Some(e.to_string()),
                    });
                }
            }
        }

        // Step 9: Diagnose divergence
        // Expected divergence: leader has Lock QC but no TC; followers have both Lock QC and TC
        let metadata_divergence = leader_has_lock_qc && !leader_has_tc
            && follower_tc_count == followers.len();

        if metadata_divergence {
            print_and_add_check(&mut result, Check {
                name: "Metadata divergence detected".into(), passed: true,
                detail: Some(format!(
                    "EXPECTED: leader has Lock QC (no TC), {} followers have TC + Lock QC. \
                     timeout_certificates table will differ.",
                    followers.len()
                )),
            });
        } else if leader_has_tc && follower_tc_count == followers.len() {
            print_and_add_check(&mut result, Check {
                name: "No metadata divergence".into(), passed: true,
                detail: Some("all nodes have TC — leader accepted incoming TC".into()),
            });
        } else {
            print_and_add_check(&mut result, Check {
                name: "Divergence analysis".into(), passed: true,
                detail: Some(format!(
                    "leader: lock_qc={} tc={}, followers: lock_qc={}/{} tc={}/{}",
                    leader_has_lock_qc, leader_has_tc,
                    follower_lock_qc_count, followers.len(),
                    follower_tc_count, followers.len()
                )),
            });
        }

        // All nodes should have Lock QC (leader applied locally, followers via broadcast)
        let all_have_lock_qc = leader_has_lock_qc && follower_lock_qc_count == followers.len();
        print_and_add_check(&mut result, Check {
            name: "Lock QC on all nodes".into(),
            passed: all_have_lock_qc,
            detail: Some(format!(
                "leader={}, followers={}/{}",
                leader_has_lock_qc, follower_lock_qc_count, followers.len()
            )),
        });

        // Verify all nodes advanced past the competing view
        match wait_for_minimum_view(nodes, competing_view + 1, Duration::from_secs(10)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "All nodes advanced".into(), passed: true,
                    detail: Some(format!("all at view >= {}", competing_view + 1)),
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "All nodes advanced".into(), passed: false,
                    detail: Some(format!("not all nodes reached view {}", competing_view + 1)),
                });
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}
