use anyhow::Result;
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::NodeInfo;
use crate::tests::files::upload_file;
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

// ============================================================================
// Barrier HTTP helpers
// ============================================================================

async fn barrier_hold(node: &NodeInfo, barrier_name: &str) -> Result<()> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/test/barriers/consensus/{}/hold",
        node.ip_address, node.port, barrier_name
    );
    let resp = client
        .post(&url)
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
        "http://{}:{}/test/barriers/consensus/{}/release",
        node.ip_address, node.port, barrier_name
    );
    let resp = client
        .post(&url)
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
        "http://{}:{}/test/barriers/consensus/{}/status",
        node.ip_address, node.port, barrier_name
    );
    let resp = client
        .get(&url)
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
        if let Ok(status) = barrier_status(node, barrier_name).await
            && status.waiting {
                return Ok(true);
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
    let resp = client
        .get(&url)
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
            if let Ok(status) = barrier_status(node, barrier_name).await
                && status.waiting {
                    return Some(node);
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
    let resp = client
        .get(&url)
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
    fn name(&self) -> &'static str {
        "consensus-barrier-basic"
    }
    fn description(&self) -> &'static str {
        "Verify barrier hold/release works by pausing leader between Propose and Lock phases"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Insufficient nodes".into(),
                    passed: false,
                    detail: Some(format!("Need >=3, found {}", nodes.len())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 1: Hold barrier on ALL nodes (only the leader hits after_propose_qc_broadcast)
        let barrier_name = "after_propose_qc_broadcast";
        for node in nodes {
            if let Err(e) = barrier_hold(node, barrier_name).await {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Hold barrier on node {}", node.node_id),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                for n in nodes {
                    let _ = barrier_release(n, barrier_name).await;
                }
                result.duration = start.elapsed();
                return Ok(result);
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Hold barrier on all nodes".into(),
                passed: true,
                detail: None,
            },
        );

        // Step 2: Upload file to trigger consensus (spawned in background — the barrier
        // blocks consensus_middleware, so the upload HTTP response won't arrive until release)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let filename = format!("barrier-basic-{}.txt", timestamp);
        let contents = format!("barrier test {}", timestamp).into_bytes();

        let upload_node = nodes[0].clone();
        let upload_filename = filename.clone();
        let upload_handle = tokio::spawn(async move {
            upload_file(&upload_node, "/", &upload_filename, contents).await
        });

        // Step 3: Find which node is waiting — that's the leader
        println!("  ... waiting for leader to reach barrier");
        let leader_node =
            match find_waiting_node(nodes, barrier_name, Duration::from_secs(30)).await {
                Some(n) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Leader reached barrier".into(),
                            passed: true,
                            detail: Some(format!(
                                "node {} waiting after Propose QC broadcast",
                                n.node_id
                            )),
                        },
                    );
                    n
                }
                None => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Leader reached barrier".into(),
                            passed: false,
                            detail: Some("no node reached waiting=true within 30s".into()),
                        },
                    );
                    for n in nodes {
                        let _ = barrier_release(n, barrier_name).await;
                    }
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            };

        let followers: Vec<&NodeInfo> = nodes
            .iter()
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
                if let Ok(history) = get_consensus_history(follower).await {
                    has_propose = history
                        .iter()
                        .any(|e| e.view == target_view as i64 && e.has_propose_qc);
                    if has_propose {
                        break;
                    }
                }
            }
            if !has_propose {
                propose_qc_ok = false;
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Propose QC on node {}", follower.node_id),
                        passed: false,
                        detail: Some(format!(
                            "view {} missing propose QC after retries",
                            target_view
                        )),
                    },
                );
            }
        }
        if propose_qc_ok {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Followers have Propose QC".into(),
                    passed: true,
                    detail: Some(format!(
                        "view {} has_propose_qc=true on {} followers",
                        target_view,
                        followers.len()
                    )),
                },
            );
        }

        // Step 6: Release barriers on all nodes
        for n in nodes {
            let _ = barrier_release(n, barrier_name).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Release barriers".into(),
                passed: true,
                detail: None,
            },
        );

        // Step 7: Wait for view to advance (Lock phase completes)
        let next_view = target_view + 1;
        match wait_for_minimum_view(nodes, next_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "View advanced after release".into(),
                        passed: true,
                        detail: Some(format!("all nodes reached view {}", next_view)),
                    },
                );
            }
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "View advanced after release".into(),
                        passed: false,
                        detail: Some(format!("not all nodes reached view {} in 30s", next_view)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "View advance check".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Verify the background upload completed successfully
        match upload_handle.await {
            Ok(Ok(_)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload completed after release".into(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(Err(e)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload completed after release".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload task".into(),
                        passed: false,
                        detail: Some(format!("join error: {}", e)),
                    },
                );
            }
        }

        // Step 8: Verify Lock QC on all nodes
        let mut lock_qc_ok = true;
        for node in nodes {
            match get_consensus_history(node).await {
                Ok(history) => {
                    let has_lock = history
                        .iter()
                        .any(|e| e.view == target_view as i64 && e.has_lock_qc);
                    if !has_lock {
                        lock_qc_ok = false;
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: format!("Lock QC on node {}", node.node_id),
                                passed: false,
                                detail: Some(format!("view {} missing lock QC", target_view)),
                            },
                        );
                    }
                }
                Err(e) => {
                    lock_qc_ok = false;
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Lock QC check node {}", node.node_id),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                }
            }
        }
        if lock_qc_ok {
            print_and_add_check(
                &mut result,
                Check {
                    name: "All nodes have Lock QC".into(),
                    passed: true,
                    detail: Some(format!(
                        "view {} has_lock_qc=true on all nodes",
                        target_view
                    )),
                },
            );
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
    fn name(&self) -> &'static str {
        "consensus-barrier-missed-ballot"
    }
    fn description(&self) -> &'static str {
        "Hold a follower's ballot dispatch, let consensus complete without it, then release and verify catch-up"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Insufficient nodes".into(),
                    passed: false,
                    detail: Some(format!("Need >=3, found {}", nodes.len())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        let barrier_name = "before_ballot_dispatch";

        // Step 1: Hold barrier on ALL nodes (only followers receive BallotSubmission)
        for node in nodes {
            if let Err(e) = barrier_hold(node, barrier_name).await {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Hold barrier on node {}", node.node_id),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                for n in nodes {
                    let _ = barrier_release(n, barrier_name).await;
                }
                result.duration = start.elapsed();
                return Ok(result);
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Hold ballot dispatch barrier on all nodes".into(),
                passed: true,
                detail: None,
            },
        );

        // Step 2: Upload file — followers will block at ballot dispatch.
        // Spawned in background because consensus_middleware blocks until ballots complete,
        // and followers are held at the barrier.
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let filename = format!("barrier-missed-{}.txt", timestamp);
        let contents = format!("missed ballot test {}", timestamp).into_bytes();

        let upload_node = nodes[0].clone();
        let upload_filename = filename.clone();
        let upload_handle = tokio::spawn(async move {
            upload_file(&upload_node, "/", &upload_filename, contents).await
        });

        // Step 3: Find the first follower that hits the barrier
        println!("  ... waiting for a follower to reach barrier");
        let first_follower =
            match find_waiting_node(nodes, barrier_name, Duration::from_secs(30)).await {
                Some(n) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "First follower at barrier".into(),
                            passed: true,
                            detail: Some(format!("node {} waiting", n.node_id)),
                        },
                    );
                    n.clone()
                }
                None => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "First follower at barrier".into(),
                            passed: false,
                            detail: Some("no node reached waiting=true within 30s".into()),
                        },
                    );
                    for n in nodes {
                        let _ = barrier_release(n, barrier_name).await;
                    }
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            };

        // Step 4: Release this follower so it can vote (gives leader 2/3 quorum)
        let _ = barrier_release(&first_follower, barrier_name).await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Release first follower".into(),
                passed: true,
                detail: Some(format!("node {} released to vote", first_follower.node_id)),
            },
        );

        // Step 5: Give consensus a moment to complete with the released follower
        sleep(Duration::from_secs(3)).await;

        // Verify the first upload completed (leader got quorum from released follower)
        match upload_handle.await {
            Ok(Ok(_)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "First upload completed".into(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(Err(e)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "First upload completed".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "First upload task".into(),
                        passed: false,
                        detail: Some(format!("join error: {}", e)),
                    },
                );
            }
        }

        // Identify the held follower: whichever node (other than first_follower) is still waiting
        let held_node = {
            let mut held = None;
            for node in nodes {
                if node.node_id == first_follower.node_id {
                    continue;
                }
                if let Ok(status) = barrier_status(node, barrier_name).await
                    && status.waiting {
                        held = Some(node);
                        break;
                    }
            }
            match held {
                Some(n) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Identify held follower".into(),
                            passed: true,
                            detail: Some(format!("node {} still held at barrier", n.node_id)),
                        },
                    );
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
                    for n in nodes {
                        let _ = barrier_release(n, barrier_name).await;
                    }
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        };

        // The leader is the remaining node
        let leader_id = nodes
            .iter()
            .find(|n| n.node_id != first_follower.node_id && n.node_id != held_node.node_id)
            .map(|n| n.node_id)
            .unwrap_or(0);

        // Step 6: Wait for view to advance on non-held nodes
        let non_held_nodes: Vec<NodeInfo> = nodes
            .iter()
            .filter(|n| n.node_id != held_node.node_id)
            .cloned()
            .collect();

        let view_after_first = get_max_view(&non_held_nodes).await.unwrap_or_default();

        match wait_for_minimum_view(&non_held_nodes, view_after_first, Duration::from_secs(30))
            .await
        {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Non-held nodes advanced".into(),
                        passed: true,
                        detail: Some(format!(
                            "leader={}, released follower={}, view {}",
                            leader_id, first_follower.node_id, view_after_first
                        )),
                    },
                );
            }
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Non-held nodes advanced".into(),
                        passed: false,
                        detail: Some("did not advance in 30s".into()),
                    },
                );
                for n in nodes {
                    let _ = barrier_release(n, barrier_name).await;
                }
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "View check".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                for n in nodes {
                    let _ = barrier_release(n, barrier_name).await;
                }
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 7: Release held follower — stale ballots process
        let _ = barrier_release(held_node, barrier_name).await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Release held follower".into(),
                passed: true,
                detail: Some(format!("node {} released", held_node.node_id)),
            },
        );

        // Step 8: Upload another file to trigger catch-up via message-driven dispatch
        let filename2 = format!("barrier-missed2-{}.txt", timestamp);
        let contents2 = format!("catch-up trigger {}", timestamp).into_bytes();

        match upload_file(&nodes[0], "/", &filename2, contents2).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload second file (catch-up trigger)".into(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload second file".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 9: Wait for ALL nodes (including previously held) to converge
        let target_view = view_after_first + 1;
        match wait_for_minimum_view(nodes, target_view, Duration::from_secs(60)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "All nodes caught up".into(),
                        passed: true,
                        detail: Some(format!("all at view {}", target_view)),
                    },
                );
            }
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "All nodes caught up".into(),
                        passed: false,
                        detail: Some(format!("not all nodes reached view {} in 30s", target_view)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Catch-up check".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Test: consensus-barrier-tc-late
// ============================================================================

/// Tests Lock QC reconstruction from timeout votes when leader's Lock QC broadcast is delayed.
///
/// Flow:
/// 1. Warm up, then hold only `before_lock_qc_broadcast` on all nodes.
/// 2. Trigger consensus — leader forms Lock QC and hits barrier.
/// 3. Wait ~130s for followers to time out. Since followers voted Lock, their timeout
///    votes carry Lock ballot evidence. The TC assembler reconstructs the Lock QC
///    from that evidence instead of forming a TC.
/// 4. Release `before_lock_qc_broadcast` — leader broadcasts Lock QC (redundant, followers
///    already have it from reconstruction).
/// 5. Verify: all nodes have Lock QC, no nodes have TC. No divergence.
pub struct ConsensusBarrierTcLate;

impl TestScenario for ConsensusBarrierTcLate {
    fn name(&self) -> &'static str {
        "consensus-barrier-tc-late"
    }
    fn description(&self) -> &'static str {
        "Lock QC reconstruction from timeout votes when leader broadcast is delayed"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Insufficient nodes".into(),
                    passed: false,
                    detail: Some(format!("Need >=3, found {}", nodes.len())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        let lock_qc_barrier = "before_lock_qc_broadcast";

        // Step 1: Warm-up consensus round
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let warmup_filename = format!("barrier-tclate-warmup-{}.txt", timestamp);
        let warmup_contents = format!("warmup {}", timestamp).into_bytes();
        match upload_file(&nodes[0], "/", &warmup_filename, warmup_contents).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Warm-up consensus round".into(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Warm-up round".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        let warmup_view = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Get warmup view".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match wait_for_minimum_view(nodes, warmup_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Warm-up complete".into(),
                        passed: true,
                        detail: Some(format!("all at view {}", warmup_view)),
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Warm-up complete".into(),
                        passed: false,
                        detail: Some("warm-up round did not complete".into()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Extra settle time
        sleep(Duration::from_secs(2)).await;

        // Step 2: Hold ONLY before_lock_qc_broadcast on all nodes (do NOT hold TC barrier)
        for node in nodes {
            if let Err(e) = barrier_hold(node, lock_qc_barrier).await {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Hold Lock QC barrier on node {}", node.node_id),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                for n in nodes {
                    let _ = barrier_release(n, lock_qc_barrier).await;
                }
                result.duration = start.elapsed();
                return Ok(result);
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Hold Lock QC barriers on all nodes".into(),
                passed: true,
                detail: Some("before_lock_qc_broadcast only (TC barriers NOT held)".into()),
            },
        );

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
        let leader_node = match find_waiting_node(nodes, lock_qc_barrier, Duration::from_secs(30))
            .await
        {
            Some(n) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Leader formed Lock QC".into(),
                        passed: true,
                        detail: Some(format!(
                            "node {} waiting at before_lock_qc_broadcast",
                            n.node_id
                        )),
                    },
                );
                n
            }
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Leader formed Lock QC".into(),
                        passed: false,
                        detail: Some("no node reached before_lock_qc_broadcast within 30s".into()),
                    },
                );
                for n in nodes {
                    let _ = barrier_release(n, lock_qc_barrier).await;
                }
                let _ = upload_handle.await;
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let followers: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| n.node_id != leader_node.node_id)
            .collect();

        let competing_view = warmup_view + 1;

        // Step 5: Wait ~130s for followers to time out and reconstruct Lock QC from timeout votes.
        // Followers voted Lock, so their timeout votes carry Lock ballot evidence.
        // The TC assembler detects this evidence and reconstructs the Lock QC instead of forming a TC.
        println!(
            "  ... waiting up to 180s for Lock QC reconstruction from timeout votes on followers"
        );
        let lock_qc_reconstructed = {
            let mut all_applied = false;
            let deadline = Instant::now() + Duration::from_secs(180);
            while Instant::now() < deadline {
                let mut count = 0;
                for node in &followers {
                    if let Ok(history) = get_consensus_history(node).await
                        && history
                            .iter()
                            .any(|e| e.view == competing_view as i64 && e.has_lock_qc)
                        {
                            count += 1;
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

        if lock_qc_reconstructed {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Lock QC reconstructed on followers".into(),
                    passed: true,
                    detail: Some(format!(
                        "view {} has Lock QC on all {} followers (from timeout vote evidence)",
                        competing_view,
                        followers.len()
                    )),
                },
            );
        } else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Lock QC reconstructed on followers".into(),
                    passed: false,
                    detail: Some(format!(
                        "not all followers reconstructed Lock QC for view {} within 140s",
                        competing_view
                    )),
                },
            );
            for n in nodes {
                let _ = barrier_release(n, lock_qc_barrier).await;
            }
            let _ = upload_handle.await;
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 6: Release before_lock_qc_broadcast → leader broadcasts Lock QC + applies locally
        // (followers already have Lock QC from reconstruction, so broadcast is redundant for them)
        for n in nodes {
            let _ = barrier_release(n, lock_qc_barrier).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Release Lock QC barrier".into(),
                passed: true,
                detail: Some(
                    "leader broadcasting Lock QC (followers already have it from reconstruction)"
                        .into(),
                ),
            },
        );

        // Step 7: Wait for Lock QC propagation and settlement
        sleep(Duration::from_secs(5)).await;

        // Wait for upload to complete
        match upload_handle.await {
            Ok(Ok(_)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload completed".into(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(Err(e)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload completed".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload task".into(),
                        passed: false,
                        detail: Some(format!("join error: {}", e)),
                    },
                );
            }
        }

        // Step 8: Verify expected state — all nodes have Lock QC, no TC
        // Leader: has Lock QC (applied locally when barrier released)
        // Followers: have Lock QC (reconstructed from timeout vote evidence, no TC ever formed)
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
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Leader state".into(),
                            passed: true,
                            detail: Some(format!(
                                "view {}: has_lock_qc={}, has_tc={}",
                                competing_view, entry.has_lock_qc, entry.has_tc
                            )),
                        },
                    );
                } else {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Leader state".into(),
                            passed: false,
                            detail: Some(format!(
                                "view {} not found in leader history",
                                competing_view
                            )),
                        },
                    );
                }
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Leader history".into(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Check followers
        for node in &followers {
            match get_consensus_history(node).await {
                Ok(history) => {
                    if let Some(entry) = history.iter().find(|e| e.view == competing_view as i64) {
                        if entry.has_lock_qc {
                            follower_lock_qc_count += 1;
                        }
                        if entry.has_tc {
                            follower_tc_count += 1;
                        }
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: format!("Follower {} state", node.node_id),
                                passed: true,
                                detail: Some(format!(
                                    "view {}: has_lock_qc={}, has_tc={}",
                                    competing_view, entry.has_lock_qc, entry.has_tc
                                )),
                            },
                        );
                    } else {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: format!("Follower {} state", node.node_id),
                                passed: false,
                                detail: Some(format!(
                                    "view {} not found in history",
                                    competing_view
                                )),
                            },
                        );
                    }
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Follower {} history", node.node_id),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                }
            }
        }

        // Step 9: Verify no TC formed (Lock QC reconstruction should prevent TC entirely)
        let no_tc_anywhere = !leader_has_tc && follower_tc_count == 0;
        print_and_add_check(
            &mut result,
            Check {
                name: "No TC formed".into(),
                passed: no_tc_anywhere,
                detail: Some(format!(
                    "leader has_tc={}, followers with TC: {}/{} (expected: no TC anywhere)",
                    leader_has_tc,
                    follower_tc_count,
                    followers.len()
                )),
            },
        );

        // All nodes should have Lock QC (leader applied locally, followers via reconstruction)
        let all_have_lock_qc = leader_has_lock_qc && follower_lock_qc_count == followers.len();
        print_and_add_check(
            &mut result,
            Check {
                name: "Lock QC on all nodes".into(),
                passed: all_have_lock_qc,
                detail: Some(format!(
                    "leader={}, followers={}/{}",
                    leader_has_lock_qc,
                    follower_lock_qc_count,
                    followers.len()
                )),
            },
        );

        // Verify all nodes advanced past the competing view
        match wait_for_minimum_view(nodes, competing_view + 1, Duration::from_secs(10)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "All nodes advanced".into(),
                        passed: true,
                        detail: Some(format!("all at view >= {}", competing_view + 1)),
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "All nodes advanced".into(),
                        passed: false,
                        detail: Some(format!("not all nodes reached view {}", competing_view + 1)),
                    },
                );
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}
