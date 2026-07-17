use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::tests::files::{download_file_from_all_nodes_with_timeout, upload_file};
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

/// Voluntary leave round trip (RFC-CONSENSUS-002 S1).
///
/// A validator leaves the set through POST /consensus/leave, the mesh
/// continues at v−1, the departed node keeps following the chain with no
/// consensus gossip (the tip-poll), and the legacy re-activation trigger
/// restores it. Runs on the DEFAULT BFT profile deliberately: the leave
/// block needs the leaver's own precommit (quorum(3) = 3), which is
/// exactly why an orderly shutdown must await the leave commit before
/// stopping — this test never stops the node.
pub struct GracefulLeave;

#[derive(Debug, Deserialize)]
struct ViewState {
    node_id: i32,
    is_active_at_height: bool,
    validators_at_height: Vec<serde_json::Value>,
}

async fn view_state(client: &Client, node: &NodeInfo, height: i64) -> Result<ViewState> {
    let url = format!("http://{}:{}/consensus/view", node.ip_address, node.port);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .json(&(height as i32))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    anyhow::ensure!(resp.status().is_success(), "HTTP {}", resp.status());
    Ok(resp.json().await?)
}

/// Poll until EVERY node reports `expect` validators at the current tip,
/// with `absent` (consensus id) missing from the set when given.
async fn wait_validator_count(
    client: &Client,
    nodes: &[NodeInfo],
    expect: usize,
    absent: Option<i32>,
    timeout: Duration,
) -> Result<bool> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Ok(false);
        }
        let tip = get_max_view(nodes).await.unwrap_or(0);
        let mut all_match = true;
        for node in nodes {
            match view_state(client, node, tip as i64 + 1).await {
                Ok(vs) => {
                    let ids: Vec<i64> = vs
                        .validators_at_height
                        .iter()
                        .filter_map(|v| v["node_id"].as_i64())
                        .collect();
                    let count_ok = ids.len() == expect;
                    let absent_ok = absent.is_none_or(|a| !ids.contains(&(a as i64)));
                    if !count_ok || !absent_ok {
                        all_match = false;
                        break;
                    }
                }
                Err(_) => {
                    all_match = false;
                    break;
                }
            }
        }
        if all_match {
            return Ok(true);
        }
        sleep(Duration::from_secs(2)).await;
    }
}

impl TestScenario for GracefulLeave {
    fn name(&self) -> &'static str {
        "graceful-leave"
    }

    fn description(&self) -> &'static str {
        "Validator leaves voluntarily, mesh continues at v-1, departed node tip-polls, rejoins"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        let client = Client::new();

        anyhow::ensure!(nodes.len() == 3, "graceful-leave expects a 3-node mesh");
        let leaver = &nodes[1];
        let stayers = [nodes[0].clone(), nodes[2].clone()];

        println!("\nRunning graceful-leave checks:");

        // 1. Baseline: all three seated.
        let baseline = wait_validator_count(&client, nodes, 3, None, Duration::from_secs(30))
            .await
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline: 3 validators on every node".to_string(),
                passed: baseline,
                detail: None,
            },
        );

        // The leaver's consensus id, from its own report.
        let tip0 = get_max_view(nodes).await.unwrap_or(0);
        let leaver_id = view_state(&client, leaver, tip0 as i64 + 1)
            .await
            .map(|vs| vs.node_id)
            .unwrap_or(-1);

        // 2. Leave: the response returns only after the commit.
        let leave_url = format!(
            "http://{}:{}/consensus/leave",
            leaver.ip_address, leaver.port
        );
        let leave_ok = client
            .post(&leave_url)
            .header("Authorization", format!("Bearer {}", leaver.jwt_token))
            .timeout(Duration::from_secs(90))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: format!("POST /consensus/leave (node {leaver_id}) commits"),
                passed: leave_ok,
                detail: None,
            },
        );

        // 3. Every node — including the leaver — reports v = 2 without it.
        let shrunk = wait_validator_count(
            &client,
            nodes,
            2,
            Some(leaver_id),
            Duration::from_secs(30),
        )
        .await
        .unwrap_or(false);
        // And the leaver's own view of itself agrees.
        let tip = get_max_view(nodes).await.unwrap_or(0);
        let self_inactive = view_state(&client, leaver, tip as i64 + 1)
            .await
            .map(|vs| !vs.is_active_at_height)
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "All nodes report v=2 without the leaver; leaver sees itself inactive"
                    .to_string(),
                passed: shrunk && self_inactive,
                detail: None,
            },
        );

        // 4. Consensus continues at v=2: upload through a stayer, verify on
        // both stayers (2-of-2 quorum under BFT).
        let content = b"graceful-leave: consensus continues at v-1".to_vec();
        let upload_ok = upload_file(&stayers[0], "/", "leave_test.txt", content.clone())
            .await
            .is_ok();
        let download_ok = upload_ok
            && download_file_from_all_nodes_with_timeout(
                &stayers,
                "/leave_test.txt",
                Duration::from_secs(30),
            )
            .await
            .map(|d| d.iter().all(|bytes| bytes == &content))
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Consensus continues at v=2 (upload + download on stayers)".to_string(),
                passed: upload_ok && download_ok,
                detail: None,
            },
        );

        // 5. Tip-poll: the departed node reaches the new tip with NO
        // consensus gossip (gossip is valset-only now). Window covers
        // several 5s poll ticks.
        let tip_after_upload = get_max_view(&stayers).await.unwrap_or(0);
        let leaver_synced = wait_for_minimum_view(
            std::slice::from_ref(leaver),
            tip_after_upload,
            Duration::from_secs(30),
        )
        .await
        .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Departed node tip-polls to height {tip_after_upload}"),
                passed: leaver_synced,
                detail: None,
            },
        );

        // 6. Rejoin via the legacy self-request trigger.
        let activate_url = format!(
            "http://{}:{}/consensus/activate",
            leaver.ip_address, leaver.port
        );
        let activate_ok = client
            .post(&activate_url)
            .header("Authorization", format!("Bearer {}", leaver.jwt_token))
            .timeout(Duration::from_secs(90))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);
        let restored = activate_ok
            && wait_validator_count(&client, nodes, 3, None, Duration::from_secs(30))
                .await
                .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Rejoin: POST /consensus/activate restores v=3 on every node".to_string(),
                passed: restored,
                detail: None,
            },
        );

        // 7. The rejoined node participates: second upload, downloadable
        // from all three (including the returnee).
        let content2 = b"graceful-leave: rejoined and serving".to_vec();
        let upload2_ok = upload_file(&nodes[0], "/", "rejoin_test.txt", content2.clone())
            .await
            .is_ok();
        let download2_ok = upload2_ok
            && download_file_from_all_nodes_with_timeout(
                nodes,
                "/rejoin_test.txt",
                Duration::from_secs(45),
            )
            .await
            .map(|d| d.iter().all(|bytes| bytes == &content2))
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Rejoined node participates (upload visible on all 3)".to_string(),
                passed: upload2_ok && download2_ok,
                detail: None,
            },
        );

        Ok(result)
    }
}
