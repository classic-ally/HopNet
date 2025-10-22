use anyhow::Result;
use std::time::{Duration, Instant};

use crate::tests::{Check, TestResult, TestScenario};
use crate::tests::files::{
    upload_file, download_file_from_all_nodes, verify_all_identical,
    list_files_from_all_nodes, verify_listings_identical,
};
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::NodeInfo;

/// Helper to print and add a check in real-time
fn print_and_add_check(result: &mut TestResult, check: Check) {
    let status = if check.passed { "✅" } else { "❌" };
    print!("  {} {}", status, check.name);
    if let Some(detail) = &check.detail {
        print!(" - {}", detail);
    }
    println!();
    result.add_check(check);
}

/// Test that a file uploaded to one node is consistently replicated across all nodes
pub struct FileUploadConsistency;

impl TestScenario for FileUploadConsistency {
    fn name(&self) -> &'static str {
        "file-upload-consistency"
    }

    fn description(&self) -> &'static str {
        "Upload a file to one node and verify it's consistently available across all nodes with identical content, metadata, and on-demand fragment retrieval"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning checks:");

        // Generate unique test filename using timestamp to avoid collisions
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let test_path = "/";
        let test_filename = format!("test-{}.txt", timestamp);
        let test_contents = format!("HopNet consistency test file created at {}", timestamp)
            .into_bytes();
        let full_path = format!("{}{}", test_path, test_filename);

        // Step 1: Get current consensus view
        print_and_add_check(&mut result, Check {
            name: "Get initial consensus view".to_string(),
            passed: true,
            detail: None,
        });

        let current_max_view = match get_max_view(nodes).await {
            Ok(view) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Initial max view across nodes: {}", view),
                    passed: true,
                    detail: None,
                });
                view
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get max view".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 2: Upload file to node 0
        match upload_file(&nodes[0], test_path, &test_filename, &test_contents).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Upload {} to node 0", test_filename),
                    passed: true,
                    detail: Some(format!("{} bytes", test_contents.len())),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 3: Wait for consensus to propagate
        // We expect view + 1 for insert_files transaction
        // (update_placement_heights happens asynchronously after fragment distribution)
        let target_view = current_max_view + 1;
        let consensus_timeout = Duration::from_secs(30);

        match wait_for_minimum_view(nodes, target_view, consensus_timeout).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Consensus propagated to view {}", target_view),
                    passed: true,
                    detail: Some(format!("Waited {} views", target_view - current_max_view)),
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Consensus propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Nodes did not reach view {} within {}s",
                        target_view,
                        consensus_timeout.as_secs()
                    )),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Consensus check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Download file from all nodes
        let downloads = match download_file_from_all_nodes(nodes, &full_path).await {
            Ok(data) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Download {} from all {} nodes", test_filename, nodes.len()),
                    passed: true,
                    detail: Some(format!("Retrieved {} downloads", data.len())),
                });
                data
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Download from nodes failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Verify all downloads are identical
        match verify_all_identical(&downloads) {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "All nodes return identical file content".to_string(),
                    passed: true,
                    detail: Some(format!("{} bytes per node", downloads[0].len())),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "File content mismatch across nodes".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 6: Verify downloaded content matches uploaded content
        if downloads[0] == test_contents {
            print_and_add_check(&mut result, Check {
                name: "Downloaded content matches uploaded content".to_string(),
                passed: true,
                detail: None,
            });
        } else {
            print_and_add_check(&mut result, Check {
                name: "Content corruption detected".to_string(),
                passed: false,
                detail: Some(format!(
                    "Expected {} bytes, got {} bytes",
                    test_contents.len(),
                    downloads[0].len()
                )),
            });
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 7: List files from all nodes
        let listings = match list_files_from_all_nodes(nodes, test_path).await {
            Ok(data) => {
                print_and_add_check(&mut result, Check {
                    name: format!("List files from all {} nodes", nodes.len()),
                    passed: true,
                    detail: Some(format!("Retrieved {} listings", data.len())),
                });
                data
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "List files failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 8: Verify all listings are identical (strict JSON comparison)
        match verify_listings_identical(&listings) {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "All nodes return identical file metadata".to_string(),
                    passed: true,
                    detail: Some("IDs, timestamps, and paths match".to_string()),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "File metadata mismatch across nodes".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 9: Verify the uploaded file appears in the listing
        if let Some(files_array) = listings[0].as_array() {
            let file_found = files_array.iter().any(|file| {
                file["path"].as_str() == Some(&full_path)
            });

            if file_found {
                print_and_add_check(&mut result, Check {
                    name: format!("File {} appears in directory listing", test_filename),
                    passed: true,
                    detail: None,
                });
            } else {
                print_and_add_check(&mut result, Check {
                    name: "Uploaded file not found in listing".to_string(),
                    passed: false,
                    detail: Some(format!("Expected to find {}", full_path)),
                });
            }
        } else {
            print_and_add_check(&mut result, Check {
                name: "Invalid listing format".to_string(),
                passed: false,
                detail: Some("Listing is not a JSON array".to_string()),
            });
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Uploaded {} ({} bytes) to node 0, verified consistency across {} nodes",
            test_filename,
            test_contents.len(),
            nodes.len()
        );

        Ok(result)
    }
}
