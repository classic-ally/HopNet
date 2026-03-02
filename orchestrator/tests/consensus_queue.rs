use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;

use crate::divergence::build_divergence_report;
use crate::tests::files::{
    delete_file, download_file, list_files_from_all_nodes, upload_file, verify_listings_identical,
};
use crate::tests::multi_user::fetch_state_snapshots;
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::tests::{print_and_add_check, Check, TestResult, TestScenario};
use crate::NodeInfo;

// ============================================================================
// Helpers
// ============================================================================

fn timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

/// Consensus-queue-aware timeout: submit() blocks up to 120s awaiting commit,
/// so HTTP requests need generous timeouts when multiple txs are queued.
const CONSENSUS_TIMEOUT: Duration = Duration::from_secs(135);

/// PUT /users/me/profile
async fn update_user_profile(node: &NodeInfo, first_name: &str, last_name: &str) -> Result<()> {
    let client = Client::new();
    let url = format!("http://{}:{}/users/me/profile", node.ip_address, node.port);

    let response = client
        .put(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "first_name": first_name,
            "last_name": last_name,
        }))
        .timeout(CONSENSUS_TIMEOUT)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Profile update failed with status {}: {}", status, body);
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct RegisterDeviceResponse {
    device_id: String,
    #[allow(dead_code)]
    api_key: String,
}

/// POST /devices/register
async fn register_device(node: &NodeInfo, device_name: &str) -> Result<RegisterDeviceResponse> {
    let client = Client::new();
    let url = format!("http://{}:{}/devices/register", node.ip_address, node.port);

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({ "device_name": device_name }))
        .timeout(CONSENSUS_TIMEOUT)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Device registration failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

/// DELETE /devices/{device_id}
async fn revoke_device(node: &NodeInfo, device_id: &str) -> Result<()> {
    let client = Client::new();
    let url = format!("http://{}:{}/devices/{}", node.ip_address, node.port, device_id);

    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(CONSENSUS_TIMEOUT)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Device revocation failed with status {}: {}", status, body);
    }

    Ok(())
}

/// Fetch snapshots, build divergence report, check full consensus.
async fn check_zero_divergence(
    mesh_id: u32,
    nodes: &[NodeInfo],
    result: &mut TestResult,
) -> bool {
    match fetch_state_snapshots(nodes).await {
        Ok(snapshots) => match build_divergence_report(mesh_id, snapshots) {
            Ok(report) => {
                if report.is_full_consensus() {
                    print_and_add_check(
                        result,
                        Check {
                            name: "Zero divergence".to_string(),
                            passed: true,
                            detail: Some(format!(
                                "{} tables, views {}-{}",
                                report.table_reports.len(),
                                report.view_range.0,
                                report.view_range.1,
                            )),
                        },
                    );
                    true
                } else {
                    let divergent: Vec<_> = report
                        .divergent_tables()
                        .iter()
                        .map(|t| t.table_name.as_str())
                        .collect();
                    print_and_add_check(
                        result,
                        Check {
                            name: "Divergence detected".to_string(),
                            passed: false,
                            detail: Some(format!("Divergent tables: {:?}", divergent)),
                        },
                    );
                    false
                }
            }
            Err(e) => {
                print_and_add_check(
                    result,
                    Check {
                        name: "Divergence report failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                false
            }
        },
        Err(e) => {
            print_and_add_check(
                result,
                Check {
                    name: "State snapshot fetch failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            false
        }
    }
}

// ============================================================================
// Test 1: consensus-queue-burst
// ============================================================================

pub struct ConsensusQueueBurst;

impl TestScenario for ConsensusQueueBurst {
    fn name(&self) -> &'static str {
        "consensus-queue-burst"
    }

    fn description(&self) -> &'static str {
        "Fire 10 concurrent mixed operations at one node. Verify batching and consistency."
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning consensus queue burst test:");

        // ── Record starting view ─────────────────────────────────────────
        let view_before = match get_max_view(nodes).await {
            Ok(v) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Starting view: {}", v),
                        passed: true,
                        detail: None,
                    },
                );
                v
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get starting view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let ts = timestamp_millis();
        let dir = format!("/burst-{}", ts);
        let target = &nodes[0];

        // ── Spawn 10 concurrent operations on nodes[0] ──────────────────
        let mut set = JoinSet::new();

        // 5 file uploads (1KB each)
        for i in 0..5 {
            let node = target.clone();
            let d = dir.clone();
            set.spawn(async move {
                let contents = vec![0x41u8; 1024]; // 1KB
                upload_file(&node, &d, &format!("burst-{}.txt", i), contents).await
            });
        }

        // 1 larger file upload (10KB)
        {
            let node = target.clone();
            let d = dir.clone();
            set.spawn(async move {
                let contents = vec![0x42u8; 10240];
                upload_file(&node, &d, "burst-large.txt", contents).await
            });
        }

        // 2 device registrations
        for i in 0..2 {
            let node = target.clone();
            let ts_copy = ts;
            set.spawn(async move {
                register_device(&node, &format!("burst-dev-{}-{}", ts_copy, i))
                    .await
                    .map(|_| ())
            });
        }

        // 2 profile updates
        for i in 0..2 {
            let node = target.clone();
            set.spawn(async move {
                update_user_profile(
                    &node,
                    &format!("Burst{}", i),
                    &format!("Test{}", i),
                )
                .await
            });
        }

        // ── Collect results ──────────────────────────────────────────────
        let mut success_count = 0u32;
        let mut fail_count = 0u32;
        while let Some(join_result) = set.join_next().await {
            match join_result {
                Ok(Ok(())) => success_count += 1,
                Ok(Err(e)) => {
                    fail_count += 1;
                    eprintln!("    burst op failed: {}", e);
                }
                Err(e) => {
                    fail_count += 1;
                    eprintln!("    burst task panicked: {}", e);
                }
            }
        }

        let all_ok = fail_count == 0;
        print_and_add_check(
            &mut result,
            Check {
                name: "All 10 operations succeed".to_string(),
                passed: all_ok,
                detail: Some(format!("{} succeeded, {} failed", success_count, fail_count)),
            },
        );

        // ── Wait for consensus + fragment distribution to settle ─────────
        // File uploads trigger async fragment distribution that writes to
        // the `blocks` table. Wait for views to stabilize, then an extra
        // pause for background jobs to finish.
        let view_after_ops = get_max_view(nodes).await.unwrap_or(view_before);
        wait_for_minimum_view(nodes, view_after_ops + 1, Duration::from_secs(60))
            .await
            .ok();
        tokio::time::sleep(Duration::from_secs(5)).await;

        // ── Batching efficiency ──────────────────────────────────────────
        let view_after = get_max_view(nodes).await.unwrap_or(view_before);
        let views_consumed = view_after.saturating_sub(view_before);

        let batched = views_consumed < 10;
        print_and_add_check(
            &mut result,
            Check {
                name: "Batching efficiency".to_string(),
                passed: batched,
                detail: Some(format!(
                    "{} views consumed for 10 ops (views {}-{})",
                    views_consumed, view_before, view_after
                )),
            },
        );

        // ── File listings identical ──────────────────────────────────────
        let listings = list_files_from_all_nodes(nodes, &dir).await?;
        let listings_ok = verify_listings_identical(&listings).is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "File listings identical across all nodes".to_string(),
                passed: listings_ok,
                detail: None,
            },
        );

        // ── Download spot-check ──────────────────────────────────────────
        {
            let path = format!("{}/burst-0.txt", dir);
            let mut all_match = true;
            let expected = vec![0x41u8; 1024];
            for (i, node) in nodes.iter().enumerate() {
                match download_file(node, &path).await {
                    Ok(data) if data == expected => {}
                    Ok(data) => {
                        all_match = false;
                        eprintln!(
                            "    node {} burst-0.txt mismatch: expected 1024 bytes, got {}",
                            i,
                            data.len()
                        );
                        break;
                    }
                    Err(e) => {
                        all_match = false;
                        eprintln!("    node {} download failed: {}", i, e);
                        break;
                    }
                }
            }
            print_and_add_check(
                &mut result,
                Check {
                    name: "Download spot-check burst-0.txt".to_string(),
                    passed: all_match,
                    detail: Some(format!("Checked {} nodes", nodes.len())),
                },
            );
        }

        // ── Zero divergence ──────────────────────────────────────────────
        check_zero_divergence(mesh_id, nodes, &mut result).await;

        result.duration = start.elapsed();
        result.details = format!(
            "Burst: {} ops, {} views consumed ({:.1} ops/view)",
            success_count,
            views_consumed,
            if views_consumed > 0 {
                success_count as f64 / views_consumed as f64
            } else {
                0.0
            }
        );

        Ok(result)
    }
}

// ============================================================================
// Test 2: consensus-queue-cross-node
// ============================================================================

pub struct ConsensusQueueCrossNode;

impl TestScenario for ConsensusQueueCrossNode {
    fn name(&self) -> &'static str {
        "consensus-queue-cross-node"
    }

    fn description(&self) -> &'static str {
        "Fire concurrent operations at different nodes. Tests two-phase ACK forwarding."
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning consensus queue cross-node test:");

        if nodes.len() < 3 {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Insufficient nodes".to_string(),
                    passed: false,
                    detail: Some(format!("Need >= 3 nodes, got {}", nodes.len())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        let ts = timestamp_millis();
        let dir = format!("/crossnode-{}", ts);

        // ── Record starting view ─────────────────────────────────────────
        let view_before_w1 = match get_max_view(nodes).await {
            Ok(v) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Starting view: {}", v),
                        passed: true,
                        detail: None,
                    },
                );
                v
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get starting view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // ── Wave 1: one op per node, concurrent ─────────────────────────
        let mut wave1 = JoinSet::new();
        let mut wave1_count = 0u32;

        // Node 0: file upload
        {
            let node = nodes[0].clone();
            let d = dir.clone();
            wave1.spawn(async move {
                upload_file(&node, &d, "from-node0.txt", vec![0x30u8; 512]).await
            });
            wave1_count += 1;
        }

        // Node 1: file upload
        {
            let node = nodes[1].clone();
            let d = dir.clone();
            wave1.spawn(async move {
                upload_file(&node, &d, "from-node1.txt", vec![0x31u8; 512]).await
            });
            wave1_count += 1;
        }

        // Node 2: device registration
        {
            let node = nodes[2].clone();
            let ts_copy = ts;
            wave1.spawn(async move {
                register_device(&node, &format!("crossnode-dev-{}", ts_copy))
                    .await
                    .map(|_| ())
            });
            wave1_count += 1;
        }

        // Nodes 3+: additional file uploads
        for (i, node) in nodes.iter().enumerate().skip(3) {
            let node = node.clone();
            let d = dir.clone();
            let idx = i;
            wave1.spawn(async move {
                upload_file(&node, &d, &format!("from-node{}.txt", idx), vec![0x33u8; 512]).await
            });
            wave1_count += 1;
        }

        // Collect wave 1 results
        let mut w1_success = 0u32;
        let mut w1_fail = 0u32;
        while let Some(join_result) = wave1.join_next().await {
            match join_result {
                Ok(Ok(())) => w1_success += 1,
                Ok(Err(e)) => {
                    w1_fail += 1;
                    eprintln!("    wave1 op failed: {}", e);
                }
                Err(e) => {
                    w1_fail += 1;
                    eprintln!("    wave1 task panicked: {}", e);
                }
            }
        }

        print_and_add_check(
            &mut result,
            Check {
                name: "Wave 1 operations succeed".to_string(),
                passed: w1_fail == 0,
                detail: Some(format!(
                    "{}/{} succeeded across {} nodes",
                    w1_success, wave1_count, nodes.len()
                )),
            },
        );

        // Wait for wave 1 to settle
        let settle_w1 = view_before_w1 + 2;
        wait_for_minimum_view(nodes, settle_w1, Duration::from_secs(60))
            .await
            .ok();

        let view_after_w1 = get_max_view(nodes).await.unwrap_or(view_before_w1);
        let w1_views = view_after_w1.saturating_sub(view_before_w1);

        print_and_add_check(
            &mut result,
            Check {
                name: "Wave 1 batching".to_string(),
                passed: true,
                detail: Some(format!(
                    "{} ops in {} views",
                    wave1_count, w1_views
                )),
            },
        );

        // ── Wave 2: concurrent again ─────────────────────────────────────
        let view_before_w2 = get_max_view(nodes).await.unwrap_or(view_after_w1);
        let mut wave2 = JoinSet::new();
        let mut wave2_count = 0u32;

        // Node 0: file upload
        {
            let node = nodes[0].clone();
            let d = dir.clone();
            wave2.spawn(async move {
                upload_file(&node, &d, "wave2-from-node0.txt", vec![0x40u8; 768]).await
            });
            wave2_count += 1;
        }

        // Node 1: profile update
        {
            let node = nodes[1].clone();
            wave2.spawn(async move {
                update_user_profile(&node, "CrossNode", "Wave2").await
            });
            wave2_count += 1;
        }

        // Node 2: file upload
        {
            let node = nodes[2].clone();
            let d = dir.clone();
            wave2.spawn(async move {
                upload_file(&node, &d, "wave2-from-node2.txt", vec![0x42u8; 768]).await
            });
            wave2_count += 1;
        }

        // Collect wave 2 results
        let mut w2_success = 0u32;
        let mut w2_fail = 0u32;
        while let Some(join_result) = wave2.join_next().await {
            match join_result {
                Ok(Ok(())) => w2_success += 1,
                Ok(Err(e)) => {
                    w2_fail += 1;
                    eprintln!("    wave2 op failed: {}", e);
                }
                Err(e) => {
                    w2_fail += 1;
                    eprintln!("    wave2 task panicked: {}", e);
                }
            }
        }

        print_and_add_check(
            &mut result,
            Check {
                name: "Wave 2 operations succeed".to_string(),
                passed: w2_fail == 0,
                detail: Some(format!(
                    "{}/{} succeeded",
                    w2_success, wave2_count
                )),
            },
        );

        // Wait for wave 2 to settle
        let settle_w2 = view_before_w2 + 2;
        wait_for_minimum_view(nodes, settle_w2, Duration::from_secs(60))
            .await
            .ok();

        let view_after_w2 = get_max_view(nodes).await.unwrap_or(view_before_w2);
        let w2_views = view_after_w2.saturating_sub(view_before_w2);

        print_and_add_check(
            &mut result,
            Check {
                name: "Wave 2 batching".to_string(),
                passed: true,
                detail: Some(format!(
                    "{} ops in {} views",
                    wave2_count, w2_views
                )),
            },
        );

        // ── File listings identical ──────────────────────────────────────
        let listings = list_files_from_all_nodes(nodes, &dir).await?;
        let listings_ok = verify_listings_identical(&listings).is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "File listings identical across all nodes".to_string(),
                passed: listings_ok,
                detail: None,
            },
        );

        // ── Cross-node download spot-check ───────────────────────────────
        {
            let path = format!("{}/from-node0.txt", dir);
            let expected = vec![0x30u8; 512];
            // Download from node 2 (file was uploaded on node 0)
            match download_file(&nodes[2], &path).await {
                Ok(data) if data == expected => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Cross-node download (node0 file from node2)".to_string(),
                            passed: true,
                            detail: Some("512 bytes match".to_string()),
                        },
                    );
                }
                Ok(data) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Cross-node download mismatch".to_string(),
                            passed: false,
                            detail: Some(format!(
                                "Expected 512 bytes, got {}",
                                data.len()
                            )),
                        },
                    );
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Cross-node download failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                }
            }
        }

        // ── Zero divergence ──────────────────────────────────────────────
        check_zero_divergence(mesh_id, nodes, &mut result).await;

        let total_ops = w1_success + w2_success;
        let total_views = view_after_w2.saturating_sub(view_before_w1);
        result.duration = start.elapsed();
        result.details = format!(
            "Cross-node: {} ops across {} nodes, {} views consumed ({:.1} ops/view)",
            total_ops,
            nodes.len(),
            total_views,
            if total_views > 0 {
                total_ops as f64 / total_views as f64
            } else {
                0.0
            }
        );

        Ok(result)
    }
}

// ============================================================================
// Test 3: consensus-queue-throughput
// ============================================================================

pub struct ConsensusQueueThroughput;

impl TestScenario for ConsensusQueueThroughput {
    fn name(&self) -> &'static str {
        "consensus-queue-throughput"
    }

    fn description(&self) -> &'static str {
        "High-volume sustained mixed-operation load (~6000 ops). Measures throughput and enforces 100% success with zero divergence."
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning consensus queue throughput test:");

        if nodes.len() < 3 {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Insufficient nodes".to_string(),
                    passed: false,
                    detail: Some(format!("Need >= 3 nodes, got {}", nodes.len())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        let ts = timestamp_millis();
        let dir = format!("/throughput-{}", ts);

        // ── Record starting view ─────────────────────────────────────────
        let view_before = match get_max_view(nodes).await {
            Ok(v) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Starting view: {}", v),
                        passed: true,
                        detail: None,
                    },
                );
                v
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get starting view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // ── Load phase: 30 seconds of high-volume operations ─────────────
        // With the WriteGate ensuring consensus priority, we can push
        // aggressively — ~10ms between rounds yields ~6000 ops in 30s.
        let load_duration = Duration::from_secs(30);
        let load_start = Instant::now();
        let mut set = JoinSet::new();
        let mut spawned_count = 0u32;
        let mut round = 0u32;
        let mut file_names: Vec<String> = Vec::new();

        while load_start.elapsed() < load_duration {
            let node_count = nodes.len();

            // Op 1: file upload distributed across nodes
            {
                let node_idx = (round as usize) % node_count;
                let node = nodes[node_idx].clone();
                let d = dir.clone();
                let r = round;
                let size = 500 + ((round * 137) % 1500) as usize;
                let fname = format!("tp-{}.txt", r);
                file_names.push(fname.clone());
                set.spawn(async move {
                    upload_file(&node, &d, &fname, vec![0x54u8; size]).await
                });
                spawned_count += 1;
            }

            // Op 2: alternating between profile update and device registration
            if round % 2 == 0 {
                let node_idx = ((round + 1) as usize) % node_count;
                let node = nodes[node_idx].clone();
                let r = round;
                set.spawn(async move {
                    update_user_profile(
                        &node,
                        &format!("Tp{}", r % 5),
                        &format!("Load{}", r % 5),
                    )
                    .await
                });
                spawned_count += 1;
            } else {
                let node_idx = ((round + 2) as usize) % node_count;
                let node = nodes[node_idx].clone();
                let ts_copy = ts;
                let r = round;
                set.spawn(async move {
                    register_device(&node, &format!("tp-dev-{}-{}", ts_copy, r))
                        .await
                        .map(|_| ())
                });
                spawned_count += 1;
            }

            round += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        print_and_add_check(
            &mut result,
            Check {
                name: "Load phase complete".to_string(),
                passed: true,
                detail: Some(format!(
                    "{} ops spawned in {} rounds over {:.1}s",
                    spawned_count,
                    round,
                    load_start.elapsed().as_secs_f64()
                )),
            },
        );

        // ── Drain: wait for all tasks (120s timeout) ─────────────────────
        let drain_start = Instant::now();
        let drain_timeout = Duration::from_secs(120);
        let mut success_count = 0u32;
        let mut fail_count = 0u32;

        while let Some(join_result) = tokio::select! {
            r = set.join_next() => r,
            _ = tokio::time::sleep(drain_timeout.saturating_sub(drain_start.elapsed())) => None,
        } {
            match join_result {
                Ok(Ok(())) => success_count += 1,
                Ok(Err(_)) => fail_count += 1,
                Err(_) => fail_count += 1,
            }
        }

        // Abort any remaining tasks after drain timeout
        set.abort_all();

        let total = success_count + fail_count;
        let success_rate = if spawned_count > 0 {
            (success_count as f64 / spawned_count as f64) * 100.0
        } else {
            0.0
        };

        print_and_add_check(
            &mut result,
            Check {
                name: "100% success rate".to_string(),
                passed: success_count == spawned_count,
                detail: Some(format!(
                    "{}/{} succeeded ({:.1}%), {} drained of {} spawned",
                    success_count, spawned_count, success_rate, total, spawned_count
                )),
            },
        );

        // ── Sequential deletes (exercise delete path post-load) ──────────
        let delete_count = 5.min(file_names.len());
        let mut deletes_ok = 0u32;
        for fname in file_names.iter().take(delete_count) {
            let path = format!("{}/{}", dir, fname);
            if delete_file(&nodes[0], &path).await.is_ok() {
                deletes_ok += 1;
            }
        }

        // Wait for deletes to settle
        let view_pre_settle = get_max_view(nodes).await.unwrap_or(view_before);
        wait_for_minimum_view(nodes, view_pre_settle + 1, Duration::from_secs(30))
            .await
            .ok();

        print_and_add_check(
            &mut result,
            Check {
                name: "Post-load deletes".to_string(),
                passed: deletes_ok > 0,
                detail: Some(format!("{}/{} deletes succeeded", deletes_ok, delete_count)),
            },
        );

        // ── Throughput metric ────────────────────────────────────────────
        let view_after = get_max_view(nodes).await.unwrap_or(view_before);
        let views_consumed = view_after.saturating_sub(view_before);
        let txs_per_view = if views_consumed > 0 {
            success_count as f64 / views_consumed as f64
        } else {
            0.0
        };

        print_and_add_check(
            &mut result,
            Check {
                name: "Throughput metric".to_string(),
                passed: true,
                detail: Some(format!(
                    "{} txs in {} views ({:.1} txs/view)",
                    success_count, views_consumed, txs_per_view
                )),
            },
        );

        let batching_confirmed = txs_per_view > 1.0;
        print_and_add_check(
            &mut result,
            Check {
                name: "Batching confirmed (txs/view > 1.0)".to_string(),
                passed: batching_confirmed,
                detail: Some(format!("{:.1} txs/view", txs_per_view)),
            },
        );

        // ── File listings identical ──────────────────────────────────────
        // Wait for consensus + fragment distribution to settle
        wait_for_minimum_view(nodes, view_after + 1, Duration::from_secs(30))
            .await
            .ok();
        tokio::time::sleep(Duration::from_secs(5)).await;

        let listings = list_files_from_all_nodes(nodes, &dir).await?;
        let listings_ok = verify_listings_identical(&listings).is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "File listings identical across all nodes".to_string(),
                passed: listings_ok,
                detail: None,
            },
        );

        // ── Zero divergence ──────────────────────────────────────────────
        check_zero_divergence(mesh_id, nodes, &mut result).await;

        result.duration = start.elapsed();
        result.details = format!(
            "Throughput: {} txs in {} views ({:.1} txs/view), {:.1}% success rate over {:.0}s",
            success_count,
            views_consumed,
            txs_per_view,
            success_rate,
            start.elapsed().as_secs_f64()
        );

        Ok(result)
    }
}
