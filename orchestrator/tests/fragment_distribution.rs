use anyhow::Result;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{
    download_file_from_all_nodes_with_timeout, get_fragment_distribution,
    trigger_fragment_inventory_sync_all, upload_file, verify_all_identical,
    verify_fragment_redundancy, wait_for_fragment_distribution,
};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

/// Test that fragment distribution algorithm correctly distributes fragments across nodes
pub struct FragmentDistribution;

impl TestScenario for FragmentDistribution {
    fn name(&self) -> &'static str {
        "fragment-distribution"
    }

    fn description(&self) -> &'static str {
        "Upload a file and verify the fragment distribution algorithm correctly places fragments across nodes with proper redundancy and balance, then verify file retrieval works with distributed fragments"
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

        // Generate unique test filename using timestamp to avoid collisions
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let test_path = "/";
        let test_filename = format!("test-dist-{}.txt", timestamp);
        let test_contents = format!(
            "HopNet fragment distribution test file created at {}",
            timestamp
        )
        .into_bytes();
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

        // Step 2: Upload file to node 0
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

        // Step 3: Wait for consensus to propagate
        let target_view = current_max_view + 1;
        let consensus_timeout = Duration::from_secs(30);

        match wait_for_minimum_view(nodes, target_view, consensus_timeout).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Consensus propagated to view {}", target_view),
                        passed: true,
                        detail: Some(format!("Waited {} views", target_view - current_max_view)),
                    },
                );
            }
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Consensus propagation timeout".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "Nodes did not reach view {} within {}s",
                            target_view,
                            consensus_timeout.as_secs()
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
                        name: "Consensus check failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Wait for fragment distribution to complete (placement_height to be set)
        let distribution_timeout = Duration::from_secs(30);
        let distribution =
            match wait_for_fragment_distribution(&nodes[0], &full_path, distribution_timeout).await
            {
                Ok(dist) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Fragment distribution completed for {}", test_filename),
                            passed: true,
                            detail: Some(format!(
                                "{} fragments ({} original + {} recovery) at height {:?}",
                                dist.fragment_count,
                                dist.original_count,
                                dist.recovery_count,
                                dist.placement_height
                            )),
                        },
                    );
                    dist
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
            };

        // Step 5: Trigger fragment inventory sync on all nodes
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
                        detail: Some("Manual self-check triggered".to_string()),
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

        // Step 6: Wait for inventory to settle by polling fragment distribution
        // Instead of predicting how many views are needed, poll until all fragments
        // have at least one node in their inventory (the actual settling condition)
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

        // Step 8: Re-query fragment distribution after inventory sync
        let distribution = match get_fragment_distribution(&nodes[0], &full_path).await {
            Ok(dist) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Re-query fragment distribution after inventory sync".to_string(),
                        passed: true,
                        detail: Some(format!(
                            "{} fragments ({} original + {} recovery) at height {:?}",
                            dist.fragment_count,
                            dist.original_count,
                            dist.recovery_count,
                            dist.placement_height
                        )),
                    },
                );
                dist
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Re-query fragment distribution failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 9: Verify fragment redundancy properties
        let redundancy_checks = verify_fragment_redundancy(&distribution, nodes.len());
        for (check_name, passed, detail) in redundancy_checks {
            print_and_add_check(
                &mut result,
                Check {
                    name: check_name,
                    passed,
                    detail: Some(detail),
                },
            );
        }

        // Step 10: Download file from all nodes to verify retrieval works with distributed fragments
        // Use 15-second timeout for small test file (bounded at ~48 seconds with 3 retries)
        let downloads = match download_file_from_all_nodes_with_timeout(
            nodes,
            &full_path,
            Duration::from_secs(15),
        )
        .await
        {
            Ok(data) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!(
                            "Download {} from all {} nodes (post-distribution)",
                            test_filename,
                            nodes.len()
                        ),
                        passed: true,
                        detail: Some(format!(
                            "Retrieved {} downloads using distributed fragments",
                            data.len()
                        )),
                    },
                );
                data
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Download from nodes failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 11: Verify all downloads are identical
        match verify_all_identical(&downloads) {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "All nodes return identical file content (distributed retrieval)"
                            .to_string(),
                        passed: true,
                        detail: Some(format!("{} bytes per node", downloads[0].len())),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File content mismatch across nodes".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 12: Verify downloaded content matches uploaded content
        if downloads[0] == test_contents {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Downloaded content matches uploaded content".to_string(),
                    passed: true,
                    detail: None,
                },
            );
        } else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Content corruption detected".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Expected {} bytes, got {} bytes",
                        test_contents.len(),
                        downloads[0].len()
                    )),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Uploaded {} ({} bytes) to node 0, verified fragment distribution across {} nodes, and retrieved file using distributed fragments",
            test_filename,
            test_contents.len(),
            nodes.len()
        );

        Ok(result)
    }
}
