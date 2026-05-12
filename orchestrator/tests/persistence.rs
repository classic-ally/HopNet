use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::NodeInfo;
use crate::tests::files::{download_file_from_all_nodes_with_timeout, upload_file};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

// ============================================================================
// Container Lifecycle Utilities
// ============================================================================

/// Stop a container by node ID
pub async fn stop_node(docker: &Docker, mesh_id: u32, node_id: u32) -> Result<()> {
    let containers = docker
        .list_containers(Some(
            bollard::query_parameters::ListContainersOptionsBuilder::new()
                .all(true)
                .build(),
        ))
        .await?;

    for container in containers {
        if let Some(labels) = &container.labels
            && labels.get("hopnet.mesh_id") == Some(&mesh_id.to_string())
            && labels.get("hopnet.node_id") == Some(&node_id.to_string())
            && let Some(id) = &container.id
        {
            docker
                .stop_container(id, None::<bollard::query_parameters::StopContainerOptions>)
                .await?;
            return Ok(());
        }
    }

    Err(anyhow::anyhow!(
        "Container for mesh {} node {} not found",
        mesh_id,
        node_id
    ))
}

/// Start a container by node ID
pub async fn start_node(docker: &Docker, mesh_id: u32, node_id: u32) -> Result<()> {
    let containers = docker
        .list_containers(Some(
            bollard::query_parameters::ListContainersOptionsBuilder::new()
                .all(true)
                .build(),
        ))
        .await?;

    for container in containers {
        if let Some(labels) = &container.labels
            && labels.get("hopnet.mesh_id") == Some(&mesh_id.to_string())
            && labels.get("hopnet.node_id") == Some(&node_id.to_string())
            && let Some(id) = &container.id
        {
            docker
                .start_container(id, None::<bollard::query_parameters::StartContainerOptions>)
                .await?;
            return Ok(());
        }
    }

    Err(anyhow::anyhow!(
        "Container for mesh {} node {} not found",
        mesh_id,
        node_id
    ))
}

/// Wait for a node to become responsive after restart
pub async fn wait_for_node_ready(node: &NodeInfo, timeout: Duration) -> Result<bool> {
    let start = Instant::now();
    let client = Client::new();

    loop {
        if start.elapsed() > timeout {
            return Ok(false);
        }

        // Use unauthenticated /setup endpoint since JWT tokens expire after restart
        let url = format!("http://{}:{}/setup", node.ip_address, node.port);
        let response = client
            .get(&url)
            .timeout(Duration::from_secs(2))
            .send()
            .await;

        if response.is_ok() && response.unwrap().status().is_success() {
            return Ok(true);
        }

        sleep(Duration::from_millis(500)).await;
    }
}

/// Get a node's public key from the setup endpoint
pub async fn get_node_pubkey(node: &NodeInfo) -> Result<String> {
    let client = Client::new();
    let url = format!("http://{}:{}/setup", node.ip_address, node.port);

    let response = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to get setup info: {}", response.status());
    }

    // The /setup endpoint returns the pubkey as a plain JSON string
    let pubkey: String = response.json().await?;
    Ok(pubkey)
}

// ============================================================================
// Test: Restart Persistence
// ============================================================================

/// Test that node identity and file data survive a restart
pub struct RestartPersistence;

impl TestScenario for RestartPersistence {
    fn name(&self) -> &'static str {
        "restart-persistence"
    }

    fn description(&self) -> &'static str {
        "Verify node identity (private key) and file data persist across node restart"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning checks:");

        if nodes.len() < 3 {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Insufficient nodes".to_string(),
                    passed: false,
                    detail: Some(format!("Need at least 3 nodes, found {}", nodes.len())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Generate unique test filename
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let test_path = "/";
        let test_filename = format!("persist-test-{}.txt", timestamp);
        let test_contents = format!("Persistence test file {}", timestamp).into_bytes();

        // Step 1: Get node 1's public key (proves identity)
        let pubkey_before = match get_node_pubkey(&nodes[1]).await {
            Ok(key) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Get node 1 pubkey before restart".to_string(),
                        passed: true,
                        detail: Some(format!("{}...", &key[..16])),
                    },
                );
                key
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get pubkey".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 2: Upload file to node 0
        let initial_view = match get_max_view(nodes).await {
            Ok(view) => view,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get initial view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

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
        let target_view = initial_view + 1;
        match wait_for_minimum_view(nodes, target_view, Duration::from_secs(15)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Consensus reached view {}", target_view),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Consensus timeout".to_string(),
                        passed: false,
                        detail: Some(format!("Failed to reach view {} in 15s", target_view)),
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

        // Step 4: Stop node 1
        let docker = Docker::connect_with_local_defaults()?;
        match stop_node(&docker, mesh_id, nodes[1].node_id).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Stop node {}", nodes[1].node_id),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to stop node".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Give it a moment to fully stop
        sleep(Duration::from_secs(2)).await;

        // Step 5: Restart node 1
        match start_node(&docker, mesh_id, nodes[1].node_id).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Restart node {}", nodes[1].node_id),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to restart node".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 6: Wait for node 1 to become ready
        match wait_for_node_ready(&nodes[1], Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Node 1 responsive after restart".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Node 1 startup timeout".to_string(),
                        passed: false,
                        detail: Some("Node did not respond within 30s".to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Node readiness check failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 7: Get fresh JWT token for restarted node (JWT keys are regenerated on startup)
        let docker = Docker::connect_with_local_defaults()?;
        let fresh_jwt = match crate::get_jwt_token(
            &docker,
            mesh_id,
            nodes[1].node_id,
            crate::sys::ContainerRuntime::Docker,
        )
        .await
        {
            Ok(token) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Obtain fresh JWT token after restart".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
                token
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get fresh JWT token".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Update node 1's JWT token with fresh token
        let mut nodes_vec = nodes.to_vec();
        nodes_vec[1].jwt_token = fresh_jwt;
        let nodes = &nodes_vec;

        // Step 8: Verify node 1's public key unchanged (private key persisted)
        match get_node_pubkey(&nodes[1]).await {
            Ok(pubkey_after) => {
                let keys_match = pubkey_before == pubkey_after;
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Node identity preserved".to_string(),
                        passed: keys_match,
                        detail: if keys_match {
                            Some("Public key matches (privkey persisted)".to_string())
                        } else {
                            Some(format!(
                                "Before: {}..., After: {}...",
                                &pubkey_before[..16],
                                &pubkey_after[..16]
                            ))
                        },
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get pubkey after restart".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 9: Wait for node 1 to catch up
        sleep(Duration::from_secs(5)).await;

        match wait_for_minimum_view(&[nodes[1].clone()], target_view, Duration::from_secs(20)).await
        {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Node 1 caught up to network".to_string(),
                        passed: true,
                        detail: Some(format!("Reached view {}", target_view)),
                    },
                );
            }
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Catch-up timeout".to_string(),
                        passed: false,
                        detail: Some(format!("Failed to reach view {} in 20s", target_view)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Catch-up check failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 10: Download file from node 1 (verify data accessible)
        let full_path = format!("{}{}", test_path, test_filename);
        match download_file_from_all_nodes_with_timeout(
            &[nodes[1].clone()],
            &full_path,
            Duration::from_secs(10),
        )
        .await
        {
            Ok(downloads) if downloads.len() == 1 => {
                let content_matches = downloads[0] == test_contents;
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File accessible from node 1".to_string(),
                        passed: content_matches,
                        detail: if content_matches {
                            Some("Content matches original".to_string())
                        } else {
                            Some(format!(
                                "Size mismatch: expected {}, got {}",
                                test_contents.len(),
                                downloads[0].len()
                            ))
                        },
                    },
                );
            }
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File download unexpected result".to_string(),
                        passed: false,
                        detail: Some("Wrong number of downloads".to_string()),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File download failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 11: Upload another file to verify node can participate in consensus
        let test_filename2 = format!("persist-test2-{}.txt", timestamp);
        let test_contents2 = b"Second file after restart".to_vec();

        let view_before_second_upload = match get_max_view(nodes).await {
            Ok(view) => view,
            Err(_) => target_view,
        };

        match upload_file(
            &nodes[0],
            test_path,
            &test_filename2,
            test_contents2.clone(),
        )
        .await
        {
            Ok(_) => {
                // Wait for consensus
                let target_view2 = view_before_second_upload + 1;
                match wait_for_minimum_view(nodes, target_view2, Duration::from_secs(15)).await {
                    Ok(true) => {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: "Node 1 participates in consensus post-restart".to_string(),
                                passed: true,
                                detail: Some(format!(
                                    "New consensus round completed (view {})",
                                    target_view2
                                )),
                            },
                        );
                    }
                    _ => {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: "Post-restart consensus".to_string(),
                                passed: false,
                                detail: Some("Timeout waiting for new consensus".to_string()),
                            },
                        );
                    }
                }
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Second upload failed".to_string(),
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
