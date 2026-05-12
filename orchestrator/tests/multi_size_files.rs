use anyhow::Result;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{
    download_file_from_all_nodes_with_timeout, upload_file, verify_all_identical,
};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

/// Generate test data of specified size
fn generate_test_data(size_mb: usize) -> Vec<u8> {
    let size_bytes = size_mb * 1024 * 1024;
    let pattern = b"HopNet Multi-Size Test - ";
    let mut data = Vec::with_capacity(size_bytes);

    while data.len() < size_bytes {
        let remaining = size_bytes - data.len();
        let to_copy = remaining.min(pattern.len());
        data.extend_from_slice(&pattern[..to_copy]);
    }

    data
}

/// Helper function to test a single file size
async fn test_file_size(
    nodes: &[NodeInfo],
    size_mb: usize,
    result: &mut TestResult,
) -> Result<bool> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let test_path = "/";
    let test_filename = format!("multi-size-{}mb-{}.bin", size_mb, timestamp);
    let test_contents = generate_test_data(size_mb);
    let full_path = format!("{}{}", test_path, test_filename);

    println!(
        "  Testing {}MB file ({} bytes)...",
        size_mb,
        test_contents.len()
    );

    // Get current consensus view (fresh for each file size)
    let current_max_view = match get_max_view(nodes).await {
        Ok(view) => view,
        Err(e) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{}MB: Failed to get max view", size_mb),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            return Ok(false);
        }
    };

    // Upload file to node 0
    match upload_file(&nodes[0], test_path, &test_filename, test_contents.clone()).await {
        Ok(_) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{}MB: Upload to node 0", size_mb),
                    passed: true,
                    detail: Some(format!("{} bytes", test_contents.len())),
                },
            );
        }
        Err(e) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{}MB: Upload failed", size_mb),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            return Ok(false);
        }
    }

    // Wait for consensus to propagate
    let target_view = current_max_view + 1;
    let consensus_timeout = Duration::from_secs(30);

    match wait_for_minimum_view(nodes, target_view, consensus_timeout).await {
        Ok(true) => {
            print_and_add_check(
                result,
                Check {
                    name: format!(
                        "{}MB: Consensus propagated to view {}",
                        size_mb, target_view
                    ),
                    passed: true,
                    detail: Some(format!("Waited {} views", target_view - current_max_view)),
                },
            );
        }
        Ok(false) | Err(_) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{}MB: Consensus propagation timeout", size_mb),
                    passed: false,
                    detail: Some(format!(
                        "Did not reach view {} within {}s",
                        target_view,
                        consensus_timeout.as_secs()
                    )),
                },
            );
            return Ok(false);
        }
    }

    // Download file from all nodes with size-appropriate timeout
    // Aggressive timeouts: ~1-2 seconds per MB + base overhead
    // With 3 retries, this gives reasonable bounds without being too lenient
    let download_timeout = if size_mb <= 10 {
        Duration::from_secs(15) // 1MB: 15s per attempt, 45s max with retries
    } else if size_mb <= 50 {
        Duration::from_secs(25) // 40MB: 25s per attempt, 75s max with retries
    } else {
        Duration::from_secs(45) // 100MB: 45s per attempt, 135s max with retries
    };

    let downloads = match download_file_from_all_nodes_with_timeout(
        nodes,
        &full_path,
        download_timeout,
    )
    .await
    {
        Ok(data) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{}MB: Download from all {} nodes", size_mb, nodes.len()),
                    passed: true,
                    detail: Some(format!("Retrieved {} downloads", data.len())),
                },
            );
            data
        }
        Err(e) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{}MB: Download failed", size_mb),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            return Ok(false);
        }
    };

    // Verify all downloads are identical
    match verify_all_identical(&downloads) {
        Ok(_) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{}MB: All nodes return identical content", size_mb),
                    passed: true,
                    detail: Some(format!("{} bytes per node", downloads[0].len())),
                },
            );
        }
        Err(e) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{}MB: Content mismatch", size_mb),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            return Ok(false);
        }
    }

    // Verify downloaded content matches uploaded content
    if downloads[0] == test_contents {
        print_and_add_check(
            result,
            Check {
                name: format!("{}MB: Downloaded content matches uploaded", size_mb),
                passed: true,
                detail: None,
            },
        );
    } else {
        print_and_add_check(
            result,
            Check {
                name: format!("{}MB: Content corruption detected", size_mb),
                passed: false,
                detail: Some(format!(
                    "Expected {} bytes, got {} bytes",
                    test_contents.len(),
                    downloads[0].len()
                )),
            },
        );
        return Ok(false);
    }

    Ok(true)
}

/// Test file upload/download consistency across multiple file sizes
/// Focuses on chunk boundary cases: 1MB (single chunk), 40MB (boundary), 100MB (multi-chunk)
pub struct MultiSizeFileConsistency;

impl TestScenario for MultiSizeFileConsistency {
    fn name(&self) -> &'static str {
        "multi-size-file-consistency"
    }

    fn description(&self) -> &'static str {
        "Upload and download files of varying sizes (1MB, 40MB, 100MB) to verify chunked Reed-Solomon works correctly across chunk boundaries"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning multi-size file consistency checks:");

        // Test various file sizes that exercise different chunking scenarios
        let test_sizes = vec![
            1,   // 1MB - single chunk, minimal padding
            40,  // 40MB - exactly at chunk boundary (critical edge case)
            100, // 100MB - 2.5 chunks (exercises multi-chunk logic)
        ];

        for size_mb in test_sizes {
            let success = test_file_size(nodes, size_mb, &mut result).await?;
            if !success {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Tested 1MB, 40MB, and 100MB files across {} nodes - all passed",
            nodes.len()
        );

        Ok(result)
    }
}
