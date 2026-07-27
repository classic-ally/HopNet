use anyhow::Result;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{list_files, modify_file, upload_file};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

/// Test that GET /files/recent returns files ordered by modification height
pub struct RecentsOrdering;

impl TestScenario for RecentsOrdering {
    fn name(&self) -> &'static str {
        "recents-ordering"
    }

    fn description(&self) -> &'static str {
        "Upload files, modify one, and verify /files/recent returns correct ordering by modification height with consistent results across all nodes"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning checks:");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let filenames = [
            format!("recents-alpha-{}.txt", timestamp),
            format!("recents-beta-{}.txt", timestamp),
            format!("recents-gamma-{}.txt", timestamp),
        ];

        // Step 1: Upload 3 files in sequence, waiting for consensus after each
        let mut current_view = match get_max_view(nodes).await {
            Ok(view) => view,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Get initial consensus view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        for (i, filename) in filenames.iter().enumerate() {
            let contents = format!("File {} content v1 ({})", filename, timestamp).into_bytes();
            match upload_file(&nodes[0], "/", filename, contents).await {
                Ok(_) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Upload file {} ({})", i + 1, filename),
                            passed: true,
                            detail: None,
                        },
                    );
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Upload file {} failed", i + 1),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }

            current_view += 1;
            match wait_for_minimum_view(nodes, current_view, Duration::from_secs(30)).await {
                Ok(true) => {}
                _ => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Consensus after upload {}", i + 1),
                            passed: false,
                            detail: Some(format!("Timeout waiting for view {}", current_view)),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }

        print_and_add_check(
            &mut result,
            Check {
                name: "All 3 files uploaded with consensus".to_string(),
                passed: true,
                detail: Some(format!("Current view: {}", current_view)),
            },
        );

        // Step 2: Fetch recents and verify initial ordering (gamma > beta > alpha)
        let recents = match list_recent_files(&nodes[0], 50).await {
            Ok(r) => r,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Fetch initial recents".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let recent_paths = extract_paths(&recents);
        let test_paths: Vec<String> = filenames.iter().map(|f| format!("/{}", f)).collect();

        // All 3 files should be present
        let all_present = test_paths.iter().all(|p| recent_paths.contains(p));
        print_and_add_check(
            &mut result,
            Check {
                name: "All 3 files appear in recents".to_string(),
                passed: all_present,
                detail: Some(format!("Found {} total recent files", recent_paths.len())),
            },
        );
        if !all_present {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // gamma (last uploaded) should be first among our test files
        let gamma_pos = recent_paths.iter().position(|p| p == &test_paths[2]);
        let beta_pos = recent_paths.iter().position(|p| p == &test_paths[1]);
        let alpha_pos = recent_paths.iter().position(|p| p == &test_paths[0]);

        let initial_order_correct = match (gamma_pos, beta_pos, alpha_pos) {
            (Some(g), Some(b), Some(a)) => g < b && b < a,
            _ => false,
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Initial ordering: gamma > beta > alpha".to_string(),
                passed: initial_order_correct,
                detail: Some(format!(
                    "Positions: gamma={:?}, beta={:?}, alpha={:?}",
                    gamma_pos, beta_pos, alpha_pos
                )),
            },
        );

        // Step 3: Modify alpha (the oldest file) to bump it to the top
        let listing = match list_files(&nodes[0], "/").await {
            Ok(l) => l,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "List files to get inode IDs".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let alpha_inode_id = extract_inode_id(&listing, &filenames[0]);
        let alpha_inode_id = match alpha_inode_id {
            Some(id) => id,
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Extract alpha inode ID".to_string(),
                        passed: false,
                        detail: Some("Could not find alpha file in listing".to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let new_contents = format!("File alpha modified content v2 ({})", timestamp).into_bytes();
        match modify_file(&nodes[0], &alpha_inode_id, new_contents).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Modify alpha file content".to_string(),
                        passed: true,
                        detail: Some(format!("inode_id: {}", alpha_inode_id)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Modify alpha file failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Wait for consensus after modification
        current_view += 1;
        match wait_for_minimum_view(nodes, current_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Consensus after modification".to_string(),
                        passed: true,
                        detail: Some(format!("View: {}", current_view)),
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Consensus after modification".to_string(),
                        passed: false,
                        detail: Some(format!("Timeout waiting for view {}", current_view)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Fetch recents again — alpha should now be first
        let recents_after = match list_recent_files(&nodes[0], 50).await {
            Ok(r) => r,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Fetch recents after modification".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let paths_after = extract_paths(&recents_after);
        let alpha_pos_after = paths_after.iter().position(|p| p == &test_paths[0]);
        let gamma_pos_after = paths_after.iter().position(|p| p == &test_paths[2]);
        let beta_pos_after = paths_after.iter().position(|p| p == &test_paths[1]);

        let modified_order_correct = match (alpha_pos_after, gamma_pos_after, beta_pos_after) {
            (Some(a), Some(g), Some(b)) => a < g && g < b,
            _ => false,
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Modified ordering: alpha > gamma > beta".to_string(),
                passed: modified_order_correct,
                detail: Some(format!(
                    "Positions: alpha={:?}, gamma={:?}, beta={:?}",
                    alpha_pos_after, gamma_pos_after, beta_pos_after
                )),
            },
        );

        // Step 5: Test limit parameter
        let recents_limited = match list_recent_files(&nodes[0], 2).await {
            Ok(r) => r,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Fetch recents with limit=2".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let limited_count = recents_limited.as_array().map(|a| a.len()).unwrap_or(0);
        let limit_works = limited_count <= 2;
        print_and_add_check(
            &mut result,
            Check {
                name: "Limit parameter restricts results".to_string(),
                passed: limit_works,
                detail: Some(format!("Requested limit=2, got {} results", limited_count)),
            },
        );

        // Step 6: Verify consistency across all nodes
        let mut all_consistent = true;
        let reference_recents = list_recent_files(&nodes[0], 50).await?;
        let reference_paths = extract_paths(&reference_recents);

        for (i, node) in nodes.iter().enumerate().skip(1) {
            let node_recents = match list_recent_files(node, 50).await {
                Ok(r) => r,
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Fetch recents from node {}", i),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    all_consistent = false;
                    continue;
                }
            };

            let node_paths = extract_paths(&node_recents);
            if node_paths != reference_paths {
                all_consistent = false;
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Node {} recents mismatch", i),
                        passed: false,
                        detail: Some(format!(
                            "Node 0: {:?}\nNode {}: {:?}",
                            reference_paths, i, node_paths
                        )),
                    },
                );
            }
        }

        print_and_add_check(
            &mut result,
            Check {
                name: format!("Recents consistent across all {} nodes", nodes.len()),
                passed: all_consistent,
                detail: None,
            },
        );

        result.duration = start.elapsed();
        result.details = format!(
            "Uploaded 3 files, modified alpha, verified recents ordering and cross-node consistency across {} nodes",
            nodes.len()
        );

        Ok(result)
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Fetch recent files from a node
async fn list_recent_files(node: &NodeInfo, limit: u32) -> Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let url = format!(
        "http://{}:{}/api/files/recent?limit={}",
        node.ip_address, node.port, limit
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("List recent files failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

/// Extract ordered list of file paths from a recents JSON response
fn extract_paths(recents: &serde_json::Value) -> Vec<String> {
    recents
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|item| item["path"].as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Extract inode ID for a filename from a file listing
fn extract_inode_id(listing: &serde_json::Value, filename: &str) -> Option<String> {
    let path = format!("/{}", filename);
    listing
        .as_array()?
        .iter()
        .find(|item| item["path"].as_str() == Some(&path))
        .and_then(|item| item["id"].as_str())
        .map(|s| s.to_string())
}
