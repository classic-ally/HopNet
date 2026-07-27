use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::tests::files::{get_fragment_distribution, upload_file, wait_for_fragment_distribution};
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

/// Test that fragment health checks work end-to-end over iroh between nodes
pub struct FragmentHealthCheck;

#[derive(Debug, Deserialize)]
struct FragmentHealthCheckResponse {
    fragment_hash: String,
    total_nodes: usize,
    healthy: usize,
    unhealthy: usize,
    errors: usize,
    results: Vec<NodeHealthResult>,
}

#[derive(Debug, Deserialize)]
struct NodeHealthResult {
    node_id: i32,
    healthy: Option<bool>,
    error: Option<String>,
    latency_ms: f64,
}

impl TestScenario for FragmentHealthCheck {
    fn name(&self) -> &'static str {
        "fragment-health-check"
    }

    fn description(&self) -> &'static str {
        "Upload a file, distribute fragments, then verify inter-node fragment health checks work over iroh"
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

        println!("\nRunning fragment health check test:");

        // Step 1: Upload a file to node 0
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let test_path = "/";
        let test_filename = format!("test-health-{}.txt", timestamp);
        let test_contents = format!("Fragment health check test file {}", timestamp).into_bytes();
        let full_path = format!("{}{}", test_path, test_filename);

        let current_max_view = get_max_view(nodes).await?;

        match upload_file(&nodes[0], test_path, &test_filename, test_contents.clone()).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Upload {} to node 0", test_filename),
                        passed: true,
                        detail: Some(format!("{} bytes", test_contents.len())),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 2: Wait for consensus + fragment distribution
        let target_view = current_max_view + 1;
        wait_for_minimum_view(nodes, target_view, Duration::from_secs(30)).await?;

        let distribution =
            match wait_for_fragment_distribution(&nodes[0], &full_path, Duration::from_secs(30))
                .await
            {
                Ok(dist) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Fragment distribution completed".to_string(),
                            passed: true,
                            detail: Some(format!("{} fragments distributed", dist.fragment_count)),
                        },
                    );
                    dist
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Fragment distribution failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            };

        // Step 3: Pick a fragment that we know is distributed to at least one node
        let test_fragment = match distribution
            .fragments
            .iter()
            .find(|f| !f.nodes_with_fragment.is_empty())
        {
            Some(f) => f,
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "No distributed fragments found".to_string(),
                        passed: false,
                        detail: Some(
                            "Cannot test health checks without distributed fragments".to_string(),
                        ),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let fragment_hash = &test_fragment.fragment_hash;
        let expected_node_ids: Vec<i32> = test_fragment.nodes_with_fragment.clone();

        print_and_add_check(
            &mut result,
            Check {
                name: "Selected fragment for health check".to_string(),
                passed: true,
                detail: Some(format!(
                    "hash={:.16}... stored on nodes {:?}",
                    fragment_hash, expected_node_ids
                )),
            },
        );

        // Step 4: Ask each node to health-check this fragment against all peers
        let mut all_checks_passed = true;

        for source in nodes {
            let url = format!(
                "http://{}:{}/api/test/fragment-health-check/{}",
                source.ip_address, source.port, fragment_hash
            );

            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", source.jwt_token))
                .timeout(Duration::from_secs(10))
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<FragmentHealthCheckResponse>().await {
                        Ok(health) => {
                            // Verify: nodes that have the fragment should report healthy
                            // (the source node itself isn't in the results — it queries peers)
                            let mut node_ok = true;
                            for peer_result in &health.results {
                                let peer_should_have =
                                    expected_node_ids.contains(&peer_result.node_id);
                                match peer_result.healthy {
                                    Some(true) if peer_should_have => {}   // correct
                                    Some(false) if !peer_should_have => {} // correct
                                    Some(true) if !peer_should_have => {} // node has it but wasn't in inventory — fine
                                    Some(false) if peer_should_have => {
                                        node_ok = false;
                                    }
                                    None => {
                                        // RPC error
                                        node_ok = false;
                                    }
                                    _ => {}
                                }
                            }

                            if health.errors > 0 {
                                node_ok = false;
                            }

                            if !node_ok {
                                all_checks_passed = false;
                            }

                            print_and_add_check(
                                &mut result,
                                Check {
                                    name: format!("Node {} fragment health check", source.node_id),
                                    passed: node_ok,
                                    detail: Some(format!(
                                        "{} healthy, {} unhealthy, {} errors (checked {} peers)",
                                        health.healthy,
                                        health.unhealthy,
                                        health.errors,
                                        health.total_nodes
                                    )),
                                },
                            );
                        }
                        Err(e) => {
                            all_checks_passed = false;
                            print_and_add_check(
                                &mut result,
                                Check {
                                    name: format!("Node {} fragment health check", source.node_id),
                                    passed: false,
                                    detail: Some(format!("Failed to parse response: {}", e)),
                                },
                            );
                        }
                    }
                }
                Ok(resp) => {
                    all_checks_passed = false;
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} fragment health check", source.node_id),
                            passed: false,
                            detail: Some(format!("HTTP {}: {}", status, body)),
                        },
                    );
                }
                Err(e) => {
                    all_checks_passed = false;
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} fragment health check", source.node_id),
                            passed: false,
                            detail: Some(format!("Request failed: {}", e)),
                        },
                    );
                }
            }
        }

        if all_checks_passed {
            print_and_add_check(
                &mut result,
                Check {
                    name: "All fragment health checks consistent".to_string(),
                    passed: true,
                    detail: Some(format!("{} nodes verified", nodes.len())),
                },
            );
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Verified fragment health checks across {} nodes for fragment {:.16}...",
            nodes.len(),
            fragment_hash
        );

        Ok(result)
    }
}
