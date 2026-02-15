use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check, wait_for_minimum_view};

pub struct MetricsCollection;

#[derive(Debug, Deserialize)]
struct TriggerResponse {
    collected: usize,
    available_nodes: usize,
    metrics: Vec<MetricEntry>,
}

#[derive(Debug, Deserialize)]
struct MetricEntry {
    from_node: i32,
    to_node: i32,
    rtt_latency: Option<f64>,
    rtt_variance: Option<f64>,
    rtt_jitter: Option<f64>,
    throughput: Option<i64>,
    height: i32,
    available: bool,
    storage_total_gb: Option<u32>,
    storage_used_gb: Option<u32>,
}

impl TestScenario for MetricsCollection {
    fn name(&self) -> &'static str {
        "metrics-collection"
    }

    fn description(&self) -> &'static str {
        "Trigger metrics collection via iroh RPC and verify consensus storage"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = Client::new();
        let num_nodes = nodes.len();

        println!("\nRunning metrics collection checks:");

        // 1. Get initial consensus view
        let initial_view = get_max_view(nodes).await?;
        print_and_add_check(&mut result, Check {
            name: "Get initial consensus view".to_string(),
            passed: true,
            detail: Some(format!("Initial max view: {}", initial_view)),
        });

        // 2. Trigger metrics collection on node 0
        let trigger_node = &nodes[0];
        let url = format!(
            "http://{}:{}/metrics/trigger",
            trigger_node.ip_address, trigger_node.port
        );
        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", trigger_node.jwt_token))
            .timeout(Duration::from_secs(120))
            .send()
            .await;

        let trigger_resp = match response {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<TriggerResponse>().await {
                    Ok(data) => {
                        print_and_add_check(&mut result, Check {
                            name: "Trigger metrics collection on node 0".to_string(),
                            passed: true,
                            detail: Some(format!(
                                "Collected {} metrics, {} nodes available",
                                data.collected, data.available_nodes
                            )),
                        });
                        data
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: "Trigger metrics collection on node 0".to_string(),
                            passed: false,
                            detail: Some(format!("Failed to parse response: {}", e)),
                        });
                        result.details = "Metrics trigger response parse failed".to_string();
                        result.duration = start.elapsed();
                        return Ok(result);
                    }
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                print_and_add_check(&mut result, Check {
                    name: "Trigger metrics collection on node 0".to_string(),
                    passed: false,
                    detail: Some(format!("HTTP {} - {}", status, body)),
                });
                result.details = "Metrics trigger returned error status".to_string();
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Trigger metrics collection on node 0".to_string(),
                    passed: false,
                    detail: Some(format!("Request failed: {}", e)),
                });
                result.details = "Metrics trigger request failed".to_string();
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // 3. Correct metric count (N-1 cross-node + 1 self = N)
        let expected_count = num_nodes;
        print_and_add_check(&mut result, Check {
            name: "Correct metric count".to_string(),
            passed: trigger_resp.collected == expected_count,
            detail: Some(format!(
                "collected={}, expected={} ({} cross-node + 1 self)",
                trigger_resp.collected, expected_count, num_nodes - 1
            )),
        });

        // 4. All nodes available
        print_and_add_check(&mut result, Check {
            name: "All nodes available".to_string(),
            passed: trigger_resp.available_nodes == num_nodes,
            detail: Some(format!(
                "available={}, expected={}",
                trigger_resp.available_nodes, num_nodes
            )),
        });

        // 5. Cross-node latency populated
        let cross_node: Vec<&MetricEntry> = trigger_resp
            .metrics
            .iter()
            .filter(|m| m.from_node != m.to_node)
            .collect();
        let latency_ok = cross_node
            .iter()
            .all(|m| m.rtt_latency.map(|v| v > 0.0).unwrap_or(false));
        print_and_add_check(&mut result, Check {
            name: "Cross-node latency populated".to_string(),
            passed: latency_ok,
            detail: Some(format!(
                "{} cross-node metrics, latencies: {:?}",
                cross_node.len(),
                cross_node.iter().map(|m| m.rtt_latency).collect::<Vec<_>>()
            )),
        });

        // 6. Cross-node throughput populated
        let throughput_ok = cross_node
            .iter()
            .all(|m| m.throughput.map(|v| v > 0).unwrap_or(false));
        print_and_add_check(&mut result, Check {
            name: "Cross-node throughput populated".to_string(),
            passed: throughput_ok,
            detail: Some(format!(
                "throughputs: {:?}",
                cross_node.iter().map(|m| m.throughput).collect::<Vec<_>>()
            )),
        });

        // 7. Self-metric storage populated
        let self_metric: Vec<&MetricEntry> = trigger_resp
            .metrics
            .iter()
            .filter(|m| m.from_node == m.to_node)
            .collect();
        let storage_ok = self_metric.len() == 1
            && self_metric[0].storage_total_gb.is_some()
            && self_metric[0].storage_used_gb.is_some();
        print_and_add_check(&mut result, Check {
            name: "Self-metric storage populated".to_string(),
            passed: storage_ok,
            detail: Some(format!(
                "self_metrics={}, total_gb={:?}, used_gb={:?}",
                self_metric.len(),
                self_metric.first().and_then(|m| m.storage_total_gb),
                self_metric.first().and_then(|m| m.storage_used_gb),
            )),
        });

        // 8. Wait for consensus propagation
        let propagated = wait_for_minimum_view(
            nodes,
            initial_view + 1,
            Duration::from_secs(10),
        )
        .await?;
        print_and_add_check(&mut result, Check {
            name: "Consensus propagation".to_string(),
            passed: propagated,
            detail: Some(format!(
                "Waited for view >= {}",
                initial_view + 1
            )),
        });

        // 9-10. Query GET /metrics on all nodes and verify consistency
        let mut all_counts: Vec<usize> = Vec::new();
        let mut all_heights: Vec<Vec<i32>> = Vec::new();
        let mut fetch_ok = true;

        for node in nodes {
            let url = format!(
                "http://{}:{}/metrics",
                node.ip_address, node.port
            );
            match client
                .get(&url)
                .header("Authorization", format!("Bearer {}", node.jwt_token))
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<Vec<MetricEntry>>().await {
                        Ok(metrics) => {
                            let heights: Vec<i32> =
                                metrics.iter().map(|m| m.height).collect();
                            all_counts.push(metrics.len());
                            all_heights.push(heights);
                        }
                        Err(_) => {
                            fetch_ok = false;
                            all_counts.push(0);
                        }
                    }
                }
                _ => {
                    fetch_ok = false;
                    all_counts.push(0);
                }
            }
        }

        print_and_add_check(&mut result, Check {
            name: "Metrics stored on all nodes".to_string(),
            passed: fetch_ok && all_counts.iter().all(|&c| c >= expected_count),
            detail: Some(format!(
                "Metric counts per node: {:?} (expected >= {})",
                all_counts, expected_count
            )),
        });

        let counts_consistent = all_counts.windows(2).all(|w| w[0] == w[1]);
        print_and_add_check(&mut result, Check {
            name: "Metrics consistent across nodes".to_string(),
            passed: counts_consistent,
            detail: Some(format!("Counts: {:?}", all_counts)),
        });

        result.details = format!(
            "Triggered metrics collection on node 0, verified {} metrics across {} nodes",
            trigger_resp.collected, num_nodes
        );
        result.duration = start.elapsed();
        Ok(result)
    }
}
