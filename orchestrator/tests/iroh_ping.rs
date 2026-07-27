use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};

/// Test that verifies iroh connectivity between all nodes in the mesh
pub struct IrohPing;

#[derive(Debug, Deserialize)]
struct IrohPingAllResponse {
    total_nodes: usize,
    successful: usize,
    failed: usize,
    results: Vec<NodePingResult>,
}

#[derive(Debug, Deserialize)]
struct NodePingResult {
    node_id: i32,
    success: bool,
    latency_ms: Option<f64>,
    error: Option<String>,
}

impl TestScenario for IrohPing {
    fn name(&self) -> &'static str {
        "iroh-ping"
    }

    fn description(&self) -> &'static str {
        "Verify iroh connectivity between all nodes in the mesh"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = Client::new();

        println!("\nRunning iroh connectivity checks:");

        let mut all_successful = true;

        // Each node pings all other nodes
        for source in nodes {
            let url = format!(
                "http://{}:{}/api/debug/iroh-ping",
                source.ip_address, source.port
            );

            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", source.jwt_token))
                .timeout(Duration::from_secs(30))
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<IrohPingAllResponse>().await {
                        Ok(ping_result) => {
                            if ping_result.failed == 0
                                && ping_result.successful == ping_result.total_nodes
                            {
                                print_and_add_check(
                                    &mut result,
                                    Check {
                                        name: format!("Node {} iroh ping", source.node_id),
                                        passed: true,
                                        detail: Some(format!(
                                            "{}/{} nodes reachable",
                                            ping_result.successful, ping_result.total_nodes
                                        )),
                                    },
                                );
                            } else {
                                all_successful = false;
                                let failed_nodes: Vec<String> = ping_result
                                    .results
                                    .iter()
                                    .filter(|r| !r.success)
                                    .map(|r| {
                                        format!(
                                            "node {} ({})",
                                            r.node_id,
                                            r.error.as_deref().unwrap_or("unknown")
                                        )
                                    })
                                    .collect();
                                print_and_add_check(
                                    &mut result,
                                    Check {
                                        name: format!("Node {} iroh ping", source.node_id),
                                        passed: false,
                                        detail: Some(format!(
                                            "{}/{} failed: {}",
                                            ping_result.failed,
                                            ping_result.total_nodes,
                                            failed_nodes.join(", ")
                                        )),
                                    },
                                );
                            }
                        }
                        Err(e) => {
                            all_successful = false;
                            print_and_add_check(
                                &mut result,
                                Check {
                                    name: format!("Node {} iroh ping", source.node_id),
                                    passed: false,
                                    detail: Some(format!("Failed to parse response: {}", e)),
                                },
                            );
                        }
                    }
                }
                Ok(resp) => {
                    all_successful = false;
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} iroh ping", source.node_id),
                            passed: false,
                            detail: Some(format!("HTTP {}: {}", status, body)),
                        },
                    );
                }
                Err(e) => {
                    all_successful = false;
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} iroh ping", source.node_id),
                            passed: false,
                            detail: Some(format!("Request failed: {}", e)),
                        },
                    );
                }
            }
        }

        // Summary check
        if all_successful {
            print_and_add_check(
                &mut result,
                Check {
                    name: "All nodes have iroh connectivity".to_string(),
                    passed: true,
                    detail: Some(format!("{} nodes verified", nodes.len())),
                },
            );
        }

        result.duration = start.elapsed();
        result.details = format!("Verified iroh connectivity across {} nodes", nodes.len());

        Ok(result)
    }
}
