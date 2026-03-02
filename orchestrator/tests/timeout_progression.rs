use anyhow::Result;
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::tests::persistence::{stop_node, start_node, wait_for_node_ready};
use crate::NodeInfo;

/// Test that timeout votes propagate over iroh and form a TC that advances the view.
///
/// Procedure:
/// 1. Get initial view V and identify the leader
/// 2. Stop the leader — no proposals can succeed
/// 3. Wait for the timeout job to fire on surviving nodes (~65s)
/// 4. Verify view advanced past V (TC formed and applied)
/// 5. Query consensus history — verify has_tc = true for view V on all surviving nodes
/// 6. Restart the stopped node and verify it catches up past the TC
pub struct TimeoutProgression;

impl TestScenario for TimeoutProgression {
    fn name(&self) -> &'static str {
        "timeout-progression"
    }

    fn description(&self) -> &'static str {
        "Verify timeout votes broadcast over iroh, form TC, and advance the view when leader is down"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(&mut result, Check {
                name: "Insufficient nodes".to_string(),
                passed: false,
                detail: Some(format!("Need at least 3 nodes, found {}", nodes.len())),
            });
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 1: Get initial view and identify the leader
        let initial_view = match get_max_view(nodes).await {
            Ok(view) => {
                print_and_add_check(&mut result, Check {
                    name: "Get initial consensus view".to_string(),
                    passed: true,
                    detail: Some(format!("view {}", view)),
                });
                view
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get initial view".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let leader_node_id = match get_leader_node_id(&nodes[0]).await {
            Ok(id) => {
                print_and_add_check(&mut result, Check {
                    name: "Identify current leader".to_string(),
                    passed: true,
                    detail: Some(format!("node {}", id)),
                });
                id
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to identify leader".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Find the leader in our node list
        let leader_idx = match nodes.iter().position(|n| n.node_id == leader_node_id) {
            Some(idx) => idx,
            None => {
                print_and_add_check(&mut result, Check {
                    name: "Leader not in node list".to_string(),
                    passed: false,
                    detail: Some(format!("Leader node {} not found in {} nodes", leader_node_id, nodes.len())),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 2: Stop the leader
        let docker = bollard::Docker::connect_with_local_defaults()?;
        match stop_node(&docker, mesh_id, leader_node_id).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Stop leader (node {})", leader_node_id),
                    passed: true,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to stop leader".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Build list of surviving nodes (everyone except the stopped leader)
        let surviving_nodes: Vec<NodeInfo> = nodes.iter()
            .enumerate()
            .filter(|(i, _)| *i != leader_idx)
            .map(|(_, n)| n.clone())
            .collect();

        // Step 3: Wait for timeout job to fire and TC to form
        // Timeout detector waits exactly 60s after the last view change before issuing
        // a timeout vote. With some margin for catch-up and TC formation, 130s is safe.
        println!("  ... waiting up to 130s for timeout votes and TC formation");

        let target_view = initial_view + 1;
        match wait_for_minimum_view(&surviving_nodes, target_view, Duration::from_secs(130)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "View advanced via timeout".to_string(),
                    passed: true,
                    detail: Some(format!("reached view {} (from {})", target_view, initial_view)),
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Timeout progression failed".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Surviving nodes did not advance past view {} within 130s",
                        initial_view
                    )),
                });
                // Still try to restart the leader before returning
                let _ = start_node(&docker, mesh_id, leader_node_id).await;
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "View check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                let _ = start_node(&docker, mesh_id, leader_node_id).await;
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Verify TC exists in consensus history on all surviving nodes
        let mut tc_check_passed = true;
        for node in &surviving_nodes {
            match get_consensus_history(node).await {
                Ok(history) => {
                    let tc_for_view = history.iter().find(|entry| {
                        entry.view == initial_view as i64 && entry.has_tc
                    });
                    if tc_for_view.is_none() {
                        tc_check_passed = false;
                        print_and_add_check(&mut result, Check {
                            name: format!("TC present on node {}", node.node_id),
                            passed: false,
                            detail: Some(format!("No TC for view {} in history", initial_view)),
                        });
                    }
                }
                Err(e) => {
                    tc_check_passed = false;
                    print_and_add_check(&mut result, Check {
                        name: format!("TC check on node {}", node.node_id),
                        passed: false,
                        detail: Some(e.to_string()),
                    });
                }
            }
        }
        if tc_check_passed {
            print_and_add_check(&mut result, Check {
                name: "TC present on all surviving nodes".to_string(),
                passed: true,
                detail: Some(format!("view {} has_tc=true on {} nodes", initial_view, surviving_nodes.len())),
            });
        }

        // Step 5: Restart the stopped leader
        match start_node(&docker, mesh_id, leader_node_id).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Restart node {}", leader_node_id),
                    passed: true,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to restart node".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 6: Wait for restarted node to become responsive
        match wait_for_node_ready(&nodes[leader_idx], Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Node {} responsive after restart", leader_node_id),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Node startup timeout".to_string(),
                    passed: false,
                    detail: Some(format!("Node {} did not respond within 30s", leader_node_id)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Node readiness check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Brief settle time for node startup (DB init, iroh reconnection, convergence job)
        sleep(Duration::from_secs(5)).await;

        // Get fresh JWT for restarted node
        let docker = bollard::Docker::connect_with_local_defaults()?;
        let fresh_jwt = match crate::get_jwt_token(
            &docker, mesh_id, leader_node_id, crate::sys::ContainerRuntime::Docker,
        ).await {
            Ok(token) => token,
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get fresh JWT".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let restarted_node = NodeInfo {
            node_id: leader_node_id,
            ip_address: nodes[leader_idx].ip_address.clone(),
            port: nodes[leader_idx].port,
            jwt_token: fresh_jwt,
        };

        // Step 7: Verify restarted node catches up past the TC
        // Allow extra time for convergence catch-up — iroh reconnection + convergence job cycle
        match wait_for_minimum_view(&[restarted_node.clone()], target_view, Duration::from_secs(90)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: "Restarted node caught up past TC".to_string(),
                    passed: true,
                    detail: Some(format!("node {} reached view {}", leader_node_id, target_view)),
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Restarted node catch-up timeout".to_string(),
                    passed: false,
                    detail: Some(format!("node {} did not reach view {} in 90s", leader_node_id, target_view)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Catch-up check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // Step 8: Verify TC visible on restarted node too
        match get_consensus_history(&restarted_node).await {
            Ok(history) => {
                let has_tc = history.iter().any(|entry| {
                    entry.view == initial_view as i64 && entry.has_tc
                });
                print_and_add_check(&mut result, Check {
                    name: "TC present on restarted node".to_string(),
                    passed: has_tc,
                    detail: if has_tc {
                        Some(format!("view {} has_tc=true after catch-up", initial_view))
                    } else {
                        Some(format!("view {} missing TC after catch-up", initial_view))
                    },
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "TC check on restarted node".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Query a node's /consensus endpoint to get the current leader's node_id
async fn get_leader_node_id(node: &NodeInfo) -> Result<u32> {
    let client = Client::new();
    let url = format!("http://{}:{}/consensus", node.ip_address, node.port);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to get consensus state: {}", response.status());
    }

    let json: serde_json::Value = response.json().await?;
    let leader_id = json["leader"]["node_id"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("leader.node_id not found in consensus response"))?;

    Ok(leader_id as u32)
}

/// A single entry from /consensus/history
#[derive(Debug, serde::Deserialize)]
struct ViewHistoryEntry {
    view: i64,
    has_tc: bool,
    // Other fields exist but we only need these
    #[serde(flatten)]
    _rest: serde_json::Value,
}

/// Query a node's /consensus/history endpoint
async fn get_consensus_history(node: &NodeInfo) -> Result<Vec<ViewHistoryEntry>> {
    let client = Client::new();
    let url = format!("http://{}:{}/consensus/history", node.ip_address, node.port);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to get consensus history: {}", response.status());
    }

    let history: Vec<ViewHistoryEntry> = response.json().await?;
    Ok(history)
}
