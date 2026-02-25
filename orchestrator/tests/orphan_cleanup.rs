use anyhow::Result;
use reqwest::Client;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::files::{
    upload_file, download_file_from_all_nodes, verify_all_identical, delete_file,
    list_files_from_all_nodes,
};
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::NodeInfo;

/// Test that orphaned data block cleanup works end-to-end through consensus:
/// - Orphaned blocks (no inode references) are cleaned up
/// - Referenced blocks (with active inodes) are preserved
pub struct OrphanCleanup;

impl TestScenario for OrphanCleanup {
    fn name(&self) -> &'static str {
        "orphan-cleanup"
    }

    fn description(&self) -> &'static str {
        "Upload two files, delete one, run orphan cleanup, verify the orphan is cleaned and the surviving file is intact"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning checks:");

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let test_path = "/";
        let survivor_filename = format!("survivor-{}.txt", timestamp);
        let orphan_filename = format!("orphan-{}.txt", timestamp);
        let survivor_contents = format!("This file should survive cleanup {}", timestamp).into_bytes();
        let orphan_contents = format!("This file will be deleted {}", timestamp).into_bytes();
        let survivor_path = format!("{}{}", test_path, survivor_filename);
        let orphan_path = format!("{}{}", test_path, orphan_filename);

        // Step 1: Get initial consensus view
        let initial_view = match get_max_view(nodes).await {
            Ok(view) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Initial consensus view: {}", view),
                    passed: true,
                    detail: None,
                });
                view
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get initial view".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 2: Upload both files
        for (filename, contents) in [
            (&survivor_filename, survivor_contents.clone()),
            (&orphan_filename, orphan_contents.clone()),
        ] {
            match upload_file(&nodes[0], test_path, filename, contents).await {
                Ok(_) => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Upload {}", filename),
                        passed: true,
                        detail: None,
                    });
                }
                Err(e) => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Upload {} failed", filename),
                        passed: false,
                        detail: Some(e.to_string()),
                    });
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }

        // Wait for both upload transactions
        let post_upload_view = initial_view + 2;
        match wait_for_minimum_view(nodes, post_upload_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Upload consensus reached view {}", post_upload_view),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload consensus timeout".to_string(),
                    passed: false,
                    detail: Some(format!("Did not reach view {} within 30s", post_upload_view)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload consensus check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 3: Delete the orphan file (removes inode, leaves data block)
        match delete_file(&nodes[0], &orphan_path).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Delete {}", orphan_filename),
                    passed: true,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Delete failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Wait for delete consensus
        let post_delete_view = post_upload_view + 1;
        match wait_for_minimum_view(nodes, post_delete_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Delete consensus reached view {}", post_delete_view),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Delete consensus timeout".to_string(),
                    passed: false,
                    detail: Some(format!("Did not reach view {} within 30s", post_delete_view)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Delete consensus check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Trigger orphan cleanup with retention_days=0 so fresh orphans qualify
        let pre_cleanup_view = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get pre-cleanup view".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let cleanup_result = match trigger_cleanup(&nodes[0], 50, 0).await {
            Ok(resp) => {
                print_and_add_check(&mut result, Check {
                    name: "Trigger orphan cleanup".to_string(),
                    passed: true,
                    detail: Some(format!("data_blocks_cleaned: {}", resp.data_blocks_cleaned)),
                });
                resp
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Trigger orphan cleanup failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Verify at least one block was cleaned
        let cleaned = cleanup_result.data_blocks_cleaned >= 1;
        print_and_add_check(&mut result, Check {
            name: "Orphaned data blocks cleaned".to_string(),
            passed: cleaned,
            detail: Some(format!(
                "Expected >= 1 cleaned, got {}",
                cleanup_result.data_blocks_cleaned
            )),
        });
        if !cleaned {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Wait for cleanup consensus transaction
        let post_cleanup_view = pre_cleanup_view + 1;
        match wait_for_minimum_view(nodes, post_cleanup_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Cleanup consensus reached view {}", post_cleanup_view),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Cleanup consensus timeout".to_string(),
                    passed: false,
                    detail: Some(format!("Did not reach view {} within 30s", post_cleanup_view)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Cleanup consensus check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 5: Verify surviving file is intact on all nodes
        match download_file_from_all_nodes(nodes, &survivor_path).await {
            Ok(downloads) => {
                match verify_all_identical(&downloads) {
                    Ok(_) => {
                        let content_matches = downloads[0] == survivor_contents;
                        print_and_add_check(&mut result, Check {
                            name: "Surviving file intact on all nodes".to_string(),
                            passed: content_matches,
                            detail: Some(if content_matches {
                                format!("{} bytes, identical across {} nodes", downloads[0].len(), nodes.len())
                            } else {
                                format!(
                                    "Content mismatch: expected {} bytes, got {}",
                                    survivor_contents.len(),
                                    downloads[0].len()
                                )
                            }),
                        });
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: "Surviving file diverged across nodes".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                        result.duration = start.elapsed();
                        return Ok(result);
                    }
                }
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to download surviving file".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 6: Verify deleted file is gone from all node listings
        match list_files_from_all_nodes(nodes, test_path).await {
            Ok(listings) => {
                let mut all_gone = true;
                for (i, listing) in listings.iter().enumerate() {
                    if let Some(files) = listing.as_array() {
                        if files.iter().any(|f| f["path"].as_str() == Some(&orphan_path.as_str())) {
                            all_gone = false;
                            print_and_add_check(&mut result, Check {
                                name: format!("Deleted file still in listing on node {}", i),
                                passed: false,
                                detail: Some(orphan_path.clone()),
                            });
                        }
                    }
                }
                if all_gone {
                    print_and_add_check(&mut result, Check {
                        name: format!("Deleted file absent from all {} node listings", nodes.len()),
                        passed: true,
                        detail: None,
                    });
                }
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to list files".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Uploaded 2 files, deleted 1, ran cleanup ({} blocks cleaned), verified survivor intact across {} nodes",
            cleanup_result.data_blocks_cleaned,
            nodes.len()
        );

        Ok(result)
    }
}

#[derive(Debug, serde::Deserialize)]
struct CleanupResponse {
    status: String,
    data_blocks_cleaned: usize,
}

async fn trigger_cleanup(
    node: &NodeInfo,
    batch_size: i32,
    retention_days: i64,
) -> Result<CleanupResponse> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/maintenance/cleanup-orphaned?batch_size={}&retention_days={}",
        node.ip_address, node.port, batch_size, retention_days
    );

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(60))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("Cleanup failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}
