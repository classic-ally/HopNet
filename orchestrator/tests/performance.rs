use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio_stream::StreamExt;

use crate::NodeInfo;
use crate::tests::files::{
    get_fragment_distribution, trigger_fragment_inventory_sync_all, upload_file,
    wait_for_fragment_distribution,
};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

/// Monitor Docker container memory usage in the background
/// Returns a handle that tracks peak memory usage in MB
async fn monitor_container_memory(
    docker: Docker,
    container_name: String,
    peak_memory_mb: Arc<AtomicU64>,
    stop_signal: Arc<AtomicU64>,
) {
    while stop_signal.load(Ordering::Relaxed) == 0 {
        // Get container stats
        let mut stats_stream = docker.stats(
            &container_name,
            Some(
                bollard::query_parameters::StatsOptionsBuilder::new()
                    .stream(false)
                    .one_shot(true)
                    .build(),
            ),
        );

        if let Some(Ok(stats)) = stats_stream.next().await
            && let Some(memory_stats) = stats.memory_stats
        {
            // Calculate working memory by measuring anonymous memory (heap allocations)
            // anon = anonymous memory (heap, stack, malloc) - this is actual application memory
            let usage_mb = if let Some(stats_map) = &memory_stats.stats {
                let anon = stats_map.get("anon").copied().unwrap_or(0);
                anon / 1_000_000 // Convert to MB
            } else if let Some(usage) = memory_stats.usage {
                // Fallback to total usage if stats not available
                usage / 1_000_000
            } else {
                0
            };

            // Update peak if current usage is higher
            let mut current_peak = peak_memory_mb.load(Ordering::Relaxed);
            while usage_mb > current_peak {
                match peak_memory_mb.compare_exchange_weak(
                    current_peak,
                    usage_mb,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(new_peak) => current_peak = new_peak,
                }
            }
        }

        // Poll every 100ms
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Generate random test data of specified size
fn generate_test_data(size_mb: usize) -> Vec<u8> {
    let size_bytes = size_mb * 1024 * 1024;
    // Use a repeating pattern for deterministic but efficient generation
    let pattern = b"HopNet Performance Test Data - ";
    let mut data = Vec::with_capacity(size_bytes);

    while data.len() < size_bytes {
        let remaining = size_bytes - data.len();
        let to_copy = remaining.min(pattern.len());
        data.extend_from_slice(&pattern[..to_copy]);
    }

    data
}

/// Download a file from a specific node and measure Time To First Chunk (TTFC) and total time
async fn download_file_with_ttfb(
    node: &NodeInfo,
    path: &str,
) -> Result<(Vec<u8>, Duration, Duration)> {
    let client = Client::new();
    let path_trimmed = path.strip_prefix('/').unwrap_or(path);
    let url = format!(
        "http://{}:{}/api/files/{}",
        node.ip_address, node.port, path_trimmed
    );

    let start = Instant::now();

    let mut response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("Download failed with status {}: {}", status, body);
    }

    // Stream the response data in chunks to handle large files efficiently
    let mut data = Vec::new();
    let mut ttfc = None;

    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read chunk: {}", e))?
    {
        // Measure TTFC - time to first actual data chunk (not just headers)
        if ttfc.is_none() {
            ttfc = Some(start.elapsed());
        }
        data.extend_from_slice(&chunk);
    }

    // Measure total download time
    let total_time = start.elapsed();

    // If no chunks received, TTFC = total time
    let ttfc = ttfc.unwrap_or(total_time);

    Ok((data, ttfc, total_time))
}

/// Test that chunked Reed-Solomon enables fast streaming for large files
///
/// This test verifies that TTFC (Time To First Chunk) is proportional to chunk size (40MB),
/// not total file size, enabling progressive streaming of large files.
///
/// Tests with a 1.5GB file and measures both TTFC and download throughput.
/// Downloads from the LAST node to force distributed fragment reconstruction,
/// demonstrating that chunked RS works across the network.
pub struct ChunkedStreamingPerformance;

impl TestScenario for ChunkedStreamingPerformance {
    fn name(&self) -> &'static str {
        "chunked-streaming-performance"
    }

    fn description(&self) -> &'static str {
        "Upload a 1.5GB file and verify that chunked Reed-Solomon enables fast TTFC (Time To First Chunk) and high throughput for streaming downloads from distributed fragments"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        // Create Docker client for memory monitoring
        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| anyhow::anyhow!("Failed to connect to Docker: {}", e))?;

        println!("\nRunning checks:");

        if nodes.len() < 2 {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Test requires at least 2 nodes".to_string(),
                    passed: false,
                    detail: Some(format!("Only {} node(s) available", nodes.len())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Test with 1.5GB file (38 chunks: 40MB × 38)
        // With chunked RS, TTFC should be ~same as 40MB file regardless of total size
        // Without chunked RS, TTFC would require reconstructing entire 1.5GB
        let test_size_mb = 1536;

        println!("  ℹ️  Generating {}MB test file...", test_size_mb);
        let test_contents = generate_test_data(test_size_mb);
        let expected_size = test_contents.len();

        // Hash the test data before upload for verification later
        let expected_hash = blake3::hash(&test_contents);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let test_path = "/";
        let test_filename = format!("perf-test-{}.bin", timestamp);
        let full_path = format!("{}{}", test_path, test_filename);

        // Step 1: Get current consensus view
        print_and_add_check(
            &mut result,
            Check {
                name: "Get initial consensus view".to_string(),
                passed: true,
                detail: None,
            },
        );

        let current_max_view = match get_max_view(nodes).await {
            Ok(view) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Initial max view across nodes: {}", view),
                        passed: true,
                        detail: None,
                    },
                );
                view
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get max view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 2: Upload large file to node 0 with memory monitoring
        println!("  ℹ️  Uploading {}MB file to node 0...", test_size_mb);

        // Start memory monitoring for upload node (node 0)
        let upload_peak_memory = Arc::new(AtomicU64::new(0));
        let upload_stop_signal = Arc::new(AtomicU64::new(0));
        let upload_container_name = format!("hopnet-orchestrator-{}-0", mesh_id);

        let monitor_handle = {
            let docker_clone = docker.clone();
            let container_name = upload_container_name.clone();
            let peak_memory = upload_peak_memory.clone();
            let stop_signal = upload_stop_signal.clone();
            tokio::spawn(async move {
                monitor_container_memory(docker_clone, container_name, peak_memory, stop_signal)
                    .await;
            })
        };

        let upload_start = Instant::now();
        match upload_file(&nodes[0], test_path, &test_filename, test_contents).await {
            Ok(_) => {
                let upload_duration = upload_start.elapsed();

                // Stop memory monitoring
                upload_stop_signal.store(1, Ordering::Relaxed);
                let _ = monitor_handle.await;

                let peak_mb = upload_peak_memory.load(Ordering::Relaxed);

                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Upload {} ({} MB)", test_filename, test_size_mb),
                        passed: true,
                        detail: Some(format!(
                            "Completed in {:.2}s",
                            upload_duration.as_secs_f64()
                        )),
                    },
                );

                // Check upload memory boundedness (streaming upload with RS encoding)
                let upload_memory_ok = peak_mb <= 650;
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload memory bounded".to_string(),
                        passed: upload_memory_ok,
                        detail: Some(format!(
                            "Peak {} MB (limit: 650 MB) {}",
                            peak_mb,
                            if upload_memory_ok {
                                "✓"
                            } else {
                                "✗ Too high!"
                            }
                        )),
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

        // Step 3: Wait for consensus to propagate
        let target_view = current_max_view + 1;
        let wait_timeout = Duration::from_secs(30);

        match wait_for_minimum_view(nodes, target_view, wait_timeout).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("All nodes reached view {}", target_view),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Timeout waiting for consensus".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "Not all nodes reached view {} within {:?}",
                            target_view, wait_timeout
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Error waiting for consensus".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 3.5: If wait-for-distribution flag is set, wait for fragments to be distributed
        let wait_for_distribution = flags.iter().any(|f| f == "wait-for-distribution");
        if wait_for_distribution {
            println!("  ℹ️  wait-for-distribution flag set, waiting for fragment distribution...");

            let distribution_timeout = Duration::from_secs(120);
            match wait_for_fragment_distribution(&nodes[0], &full_path, distribution_timeout).await
            {
                Ok(dist) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Fragment distribution completed".to_string(),
                            passed: true,
                            detail: Some(format!(
                                "{} fragments at height {:?}",
                                dist.fragment_count, dist.placement_height
                            )),
                        },
                    );
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Fragment distribution timeout or failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }

            // Also trigger fragment inventory sync and wait for settling
            match trigger_fragment_inventory_sync_all(nodes).await {
                Ok(_) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!(
                                "Trigger fragment inventory sync on all {} nodes",
                                nodes.len()
                            ),
                            passed: true,
                            detail: Some("Ensuring fragments are inventoried".to_string()),
                        },
                    );
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Fragment inventory sync failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }

            let settle_timeout = Duration::from_secs(60);
            let settle_start = Instant::now();
            let mut settled = false;
            while settle_start.elapsed() < settle_timeout {
                if let Ok(dist) = get_fragment_distribution(&nodes[0], &full_path).await
                    && !dist.fragments.is_empty()
                    && dist
                        .fragments
                        .iter()
                        .all(|f| !f.nodes_with_fragment.is_empty())
                {
                    settled = true;
                    break;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }

            if settled {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Fragment inventory settled".to_string(),
                        passed: true,
                        detail: Some(format!(
                            "All fragments have inventory data after {:.1}s",
                            settle_start.elapsed().as_secs_f64()
                        )),
                    },
                );
            } else {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Fragment inventory sync timeout".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "Not all fragments had inventory data within {}s",
                            settle_timeout.as_secs()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Download file from LAST node and measure TTFC with memory monitoring
        // This forces distributed fragment reconstruction
        let last_node = &nodes[nodes.len() - 1];
        let last_node_id = nodes.len() - 1;
        println!(
            "  ℹ️  Downloading from node {} (last) to test distributed reconstruction...",
            last_node_id
        );

        // Start memory monitoring for download node (last node)
        let download_peak_memory = Arc::new(AtomicU64::new(0));
        let download_stop_signal = Arc::new(AtomicU64::new(0));
        let download_container_name = format!("hopnet-orchestrator-{}-{}", mesh_id, last_node_id);

        let download_monitor_handle = {
            let docker_clone = docker.clone();
            let container_name = download_container_name.clone();
            let peak_memory = download_peak_memory.clone();
            let stop_signal = download_stop_signal.clone();
            tokio::spawn(async move {
                monitor_container_memory(docker_clone, container_name, peak_memory, stop_signal)
                    .await;
            })
        };

        match download_file_with_ttfb(last_node, &full_path).await {
            Ok((downloaded_data, ttfc, total_time)) => {
                let ttfc_secs = ttfc.as_secs_f64();
                let total_secs = total_time.as_secs_f64();

                // Verify download succeeded
                let size_match = downloaded_data.len() == expected_size;

                // Hash downloaded data and compare with original hash
                let downloaded_hash = blake3::hash(&downloaded_data);
                let content_match = downloaded_hash == expected_hash;

                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!(
                            "Download file from node {} ({} MB)",
                            nodes.len() - 1,
                            test_size_mb
                        ),
                        passed: size_match && content_match,
                        detail: Some(format!(
                            "Downloaded {} bytes (expected {}), hash {}",
                            downloaded_data.len(),
                            expected_size,
                            if content_match {
                                "matches"
                            } else {
                                "mismatch!"
                            }
                        )),
                    },
                );

                // Check TTFC is reasonable
                // With chunked RS (40MB chunks), TTFC should be < 10 seconds even for large files
                // This is because streaming starts after first chunk is reconstructed
                let ttfc_passed = ttfc_secs < 10.0;

                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Time To First Chunk (TTFC)".to_string(),
                        passed: ttfc_passed,
                        detail: Some(format!(
                            "{:.3}s (target: < 10.0s) {}",
                            ttfc_secs,
                            if ttfc_passed { "✓" } else { "✗ Too slow!" }
                        )),
                    },
                );

                // Calculate throughput
                let throughput_mbps = (test_size_mb as f64) / total_secs;

                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Download throughput".to_string(),
                        passed: true,
                        detail: Some(format!(
                            "{:.2} MB/s (total time: {:.2}s for {} MB)",
                            throughput_mbps, total_secs, test_size_mb
                        )),
                    },
                );

                // Stop download memory monitoring
                download_stop_signal.store(1, Ordering::Relaxed);
                let _ = download_monitor_handle.await;

                let download_peak_mb = download_peak_memory.load(Ordering::Relaxed);

                // Check download memory boundedness (streaming reconstruction with chunked RS)
                let download_memory_ok = download_peak_mb <= 900;
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Download memory bounded".to_string(),
                        passed: download_memory_ok,
                        detail: Some(format!(
                            "Peak {} MB (limit: 900 MB) {}",
                            download_peak_mb,
                            if download_memory_ok {
                                "✓"
                            } else {
                                "✗ Too high!"
                            }
                        )),
                    },
                );

                if ttfc_passed {
                    println!(
                        "  💡 TTFC {:.3}s demonstrates chunked streaming is working!",
                        ttfc_secs
                    );
                    println!(
                        "      (Without chunked RS, TTFC would require reconstructing entire {:.1}GB file)",
                        test_size_mb as f64 / 1024.0
                    );
                    println!(
                        "  💡 Throughput: {:.2} MB/s with distributed reconstruction",
                        throughput_mbps
                    );
                }
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Download failed".to_string(),
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
