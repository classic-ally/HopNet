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
pub(crate) struct ViewState {
    pub(crate) node_id: i32,
    pub(crate) is_active_at_height: bool,
    #[serde(default)]
    pub(crate) last_departure_kind: Option<String>,
    pub(crate) validators_at_height: Vec<serde_json::Value>,
}

pub(crate) async fn view_state(client: &Client, node: &NodeInfo, height: i64) -> Result<ViewState> {
    let url = format!("http://{}:{}/api/consensus/view", node.ip_address, node.port);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .json(&height)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    anyhow::ensure!(resp.status().is_success(), "HTTP {}", resp.status());
    Ok(resp.json().await?)
}

/// Poll until EVERY node reports `expect` validators at the current tip,
/// with `absent` (consensus id) missing from the set when given.
pub(crate) async fn wait_validator_count(
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
            "http://{}:{}/api/consensus/leave",
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

        // 3. Height-scoped assertion at the leave height (race-immune vs
        // the auto-reseat that follows): find the height where v dropped to
        // 2 and assert the leaver is absent with departure_kind voluntary.
        // Under the majority default a healthy voluntary leaver is
        // re-seated within seconds (exposure-free => zero required span),
        // so tip-state v=2 assertions would race the reseat — walk history
        // instead.
        let mut leave_height: Option<i64> = None;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline && leave_height.is_none() {
            let tip = get_max_view(nodes).await.unwrap_or(0) as i64;
            for h in (1..=tip + 1).rev() {
                if let Ok(vs) = view_state(&client, &stayers[0], h).await {
                    let ids: Vec<i64> = vs
                        .validators_at_height
                        .iter()
                        .filter_map(|v| v["node_id"].as_i64())
                        .collect();
                    if ids.len() == 2 && !ids.contains(&(leaver_id as i64)) {
                        leave_height = Some(h);
                        break;
                    }
                }
            }
            if leave_height.is_none() {
                sleep(Duration::from_secs(2)).await;
            }
        }
        let kind_ok = if let Some(h) = leave_height {
            view_state(&client, leaver, h)
                .await
                .ok()
                .and_then(|vs| vs.last_departure_kind)
                .as_deref()
                == Some("voluntary")
        } else {
            false
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Leave committed: v=2 without leaver, departure_kind voluntary (height-scoped)"
                    .to_string(),
                passed: leave_height.is_some() && kind_ok,
                detail: None,
            },
        );

        // 4. Consensus proceeded during the v=2 window: a file uploaded at
        // the leave height is retrievable from the stayers.
        let content = b"graceful-leave: consensus continues".to_vec();
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
                name: "Consensus continues while shrunk (upload + download on stayers)".to_string(),
                passed: upload_ok && download_ok,
                detail: None,
            },
        );

        // 5. MONEY: v=3 restored on every node with NO request anywhere —
        // mesh-initiated auto-reseat. This transitively proves the leaver
        // caught up (seating requires it) and was noticed by the mesh.
        let restored =
            wait_validator_count(&client, nodes, 3, None, Duration::from_secs(60))
                .await
                .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Auto-reseat restores v=3 on every node (no request)".to_string(),
                passed: restored,
                detail: None,
            },
        );

        // 6. Full-mesh participation post-reseat.
        let content2 = b"graceful-leave: reseated and serving".to_vec();
        let upload2 = upload_file(&nodes[0], "/", "reseat_test.txt", content2.clone())
            .await
            .is_ok();
        let download2 = upload2
            && download_file_from_all_nodes_with_timeout(
                nodes,
                "/reseat_test.txt",
                Duration::from_secs(45),
            )
            .await
            .map(|d| d.iter().all(|bytes| bytes == &content2))
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Reseated node participates (upload visible on all 3)".to_string(),
                passed: upload2 && download2,
                detail: None,
            },
        );

        Ok(result)
    }
}
