use anyhow::Result;
use reqwest::Client;
use std::time::{Duration, Instant};

use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::files::{upload_file, download_file, list_files};
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::NodeInfo;

// ============================================================================
// Helpers
// ============================================================================

/// Clone a NodeInfo with a different JWT token (to act as a different user).
fn node_with_token(node: &NodeInfo, token: &str) -> NodeInfo {
    NodeInfo {
        node_id: node.node_id,
        ip_address: node.ip_address.clone(),
        port: node.port,
        jwt_token: token.to_string(),
    }
}

/// POST /users to create a new user, returns the generated passphrase.
async fn create_user(node: &NodeInfo, username: &str) -> Result<String> {
    let client = Client::new();
    let url = format!("http://{}:{}/users", node.ip_address, node.port);

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({ "username": username }))
        .timeout(Duration::from_secs(15))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("User creation failed with status {}: {}", status, body);
    }

    let body: serde_json::Value = response.json().await?;
    body["passphrase"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing passphrase in response"))
}

/// POST /login to authenticate, returns a JWT token.
async fn login_user(node: &NodeInfo, username: &str, passphrase: &str) -> Result<String> {
    let client = Client::new();
    let url = format!("http://{}:{}/login", node.ip_address, node.port);

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "username": username,
            "passphrase": passphrase,
        }))
        .timeout(Duration::from_secs(15))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Login failed with status {}: {}", status, body);
    }

    let body: serde_json::Value = response.json().await?;
    body["token"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing token in response"))
}

/// Non-panicking download. Returns Ok(Ok(bytes)) on success, Ok(Err(status_code))
/// on HTTP error. Single attempt, no retry — we expect 404 for cross-user access.
async fn try_download_file(node: &NodeInfo, path: &str) -> Result<std::result::Result<Vec<u8>, u16>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let path_trimmed = path.strip_prefix('/').unwrap_or(path);
    let url = format!("http://{}:{}/files/{}", node.ip_address, node.port, path_trimmed);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if response.status().is_success() {
        let bytes = response.bytes().await?.to_vec();
        Ok(Ok(bytes))
    } else {
        Ok(Err(response.status().as_u16()))
    }
}

/// Fetch /debug/state from all nodes in parallel. Returns (node_id, snapshot) pairs.
async fn fetch_state_snapshots(
    nodes: &[NodeInfo],
) -> Result<Vec<(u32, hopnet_common::StateSnapshot)>> {
    let mut handles = Vec::new();

    for node in nodes {
        let node = node.clone();
        handles.push(tokio::spawn(async move {
            let response = crate::call_node_api(&node, "/debug/state", true).await?;
            if !response.status().is_success() {
                anyhow::bail!("HTTP {}", response.status());
            }
            let snapshot: hopnet_common::StateSnapshot = response.json().await?;
            Ok::<_, anyhow::Error>((node.node_id, snapshot))
        }));
    }

    let mut snapshots = Vec::new();
    for handle in handles {
        snapshots.push(handle.await??);
    }

    Ok(snapshots)
}

// ============================================================================
// Test
// ============================================================================

pub struct MultiUserIsolation;

impl TestScenario for MultiUserIsolation {
    fn name(&self) -> &'static str {
        "multi-user-isolation"
    }

    fn description(&self) -> &'static str {
        "Create a second user, roaming login, per-user file isolation, and zero divergence"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning multi-user isolation checks:");

        if nodes.len() < 3 {
            print_and_add_check(&mut result, Check {
                name: "Insufficient nodes".to_string(),
                passed: false,
                detail: Some(format!("Need >= 3 nodes, got {}", nodes.len())),
            });
            result.duration = start.elapsed();
            return Ok(result);
        }

        // ── Step 1: Initial consensus view ──────────────────────────────
        let current_view = match get_max_view(nodes).await {
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

        // ── Step 2: Create user 'bob' ───────────────────────────────────
        let passphrase = match create_user(&nodes[0], "bob").await {
            Ok(pp) => {
                print_and_add_check(&mut result, Check {
                    name: "Create user 'bob'".to_string(),
                    passed: true,
                    detail: Some(format!("{} words", pp.split_whitespace().count())),
                });
                pp
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Create user 'bob' failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // ── Step 3: Wait for user-creation consensus ────────────────────
        let target_view = current_view + 1;
        match wait_for_minimum_view(nodes, target_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("User creation consensus (view {})", target_view),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "User creation consensus timeout".to_string(),
                    passed: false,
                    detail: Some(format!("Did not reach view {} within 30s", target_view)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "User creation consensus check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 4: Roaming login — Bob logs in on all nodes ────────────
        // Each node has its own JWT signing key, so Bob needs a token from
        // each node. We login on node 2 first (proving roaming works on a
        // non-creation node), then the rest.
        let mut bob_nodes: Vec<NodeInfo> = Vec::with_capacity(nodes.len());
        {
            // Login on node 2 first (roaming proof)
            let order: Vec<usize> = {
                let mut o = vec![2usize];
                for i in 0..nodes.len() {
                    if i != 2 { o.push(i); }
                }
                o
            };
            for (idx, &ni) in order.iter().enumerate() {
                match login_user(&nodes[ni], "bob", &passphrase).await {
                    Ok(token) => {
                        // Only report the first (roaming) login as the check
                        if idx == 0 {
                            print_and_add_check(&mut result, Check {
                                name: "Roaming login for 'bob' on node 2".to_string(),
                                passed: true,
                                detail: Some(format!("Then logged in on {} remaining nodes", nodes.len() - 1)),
                            });
                        }
                        // Insert into bob_nodes at the correct position
                        // (we iterate out of order, so collect and sort later)
                        bob_nodes.push(node_with_token(&nodes[ni], &token));
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Bob login failed on node {}", ni),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                        result.duration = start.elapsed();
                        return Ok(result);
                    }
                }
            }
            // Sort bob_nodes by node_id so indices align with `nodes`
            bob_nodes.sort_by_key(|n| n.node_id);
        }

        // ── Step 5: Owner uploads file ──────────────────────────────────
        let owner_content = b"owner secret data".to_vec();
        match upload_file(&nodes[0], "/", "owner-secret.txt", owner_content.clone()).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner uploads owner-secret.txt".to_string(),
                    passed: true,
                    detail: Some(format!("{} bytes", owner_content.len())),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner upload failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 6: Owner upload consensus ──────────────────────────────
        let view_after_owner_upload = get_max_view(nodes).await.unwrap_or(target_view) + 1;
        match wait_for_minimum_view(nodes, view_after_owner_upload, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Owner upload consensus (view {})", view_after_owner_upload),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) | Err(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner upload consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 7: Bob uploads file on node 2 ─────────────────────────
        let bob_content = b"bob private data".to_vec();
        match upload_file(&bob_nodes[2], "/", "bob-file.txt", bob_content.clone()).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob uploads bob-file.txt on node 2".to_string(),
                    passed: true,
                    detail: Some(format!("{} bytes", bob_content.len())),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob upload failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 8: Bob upload consensus ────────────────────────────────
        let view_after_bob_upload = get_max_view(nodes).await.unwrap_or(view_after_owner_upload) + 1;
        match wait_for_minimum_view(nodes, view_after_bob_upload, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Bob upload consensus (view {})", view_after_bob_upload),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) | Err(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob upload consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 9: Owner downloads own file from all nodes ─────────────
        {
            let mut all_ok = true;
            for (i, node) in nodes.iter().enumerate() {
                match download_file(node, "/owner-secret.txt").await {
                    Ok(data) if data == owner_content => {}
                    Ok(data) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Owner download mismatch on node {}", i),
                            passed: false,
                            detail: Some(format!("Expected {} bytes, got {}", owner_content.len(), data.len())),
                        });
                        all_ok = false;
                        break;
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Owner download failed on node {}", i),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok {
                print_and_add_check(&mut result, Check {
                    name: format!("Owner downloads own file from all {} nodes", nodes.len()),
                    passed: true,
                    detail: None,
                });
            } else {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 10: Bob downloads own file from all nodes ──────────────
        {
            let mut all_ok = true;
            for i in 0..nodes.len() {
                match download_file(&bob_nodes[i], "/bob-file.txt").await {
                    Ok(data) if data == bob_content => {}
                    Ok(data) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Bob download mismatch on node {}", i),
                            passed: false,
                            detail: Some(format!("Expected {} bytes, got {}", bob_content.len(), data.len())),
                        });
                        all_ok = false;
                        break;
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Bob download failed on node {}", i),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok {
                print_and_add_check(&mut result, Check {
                    name: format!("Bob downloads own file from all {} nodes", nodes.len()),
                    passed: true,
                    detail: None,
                });
            } else {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 11: Bob cannot download owner's file ───────────────────
        {
            let mut all_ok = true;
            for i in 0..nodes.len() {
                match try_download_file(&bob_nodes[i], "/owner-secret.txt").await {
                    Ok(Err(404)) => {} // expected
                    Ok(Err(status)) => {
                        // other error codes are also acceptable (403, etc.)
                        print_and_add_check(&mut result, Check {
                            name: format!("Bob got unexpected status {} for owner file on node {}", status, i),
                            passed: false,
                            detail: Some("Expected 404".to_string()),
                        });
                        all_ok = false;
                        break;
                    }
                    Ok(Ok(_)) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Bob downloaded owner's file on node {} (isolation breach!)", i),
                            passed: false,
                            detail: None,
                        });
                        all_ok = false;
                        break;
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Cross-user check error on node {}", i),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok {
                print_and_add_check(&mut result, Check {
                    name: "Bob cannot download owner's file (all nodes)".to_string(),
                    passed: true,
                    detail: Some("404 on all nodes".to_string()),
                });
            } else {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 12: Owner cannot download Bob's file ───────────────────
        {
            let mut all_ok = true;
            for (i, node) in nodes.iter().enumerate() {
                match try_download_file(node, "/bob-file.txt").await {
                    Ok(Err(404)) => {} // expected
                    Ok(Err(status)) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Owner got unexpected status {} for Bob's file on node {}", status, i),
                            passed: false,
                            detail: Some("Expected 404".to_string()),
                        });
                        all_ok = false;
                        break;
                    }
                    Ok(Ok(_)) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Owner downloaded Bob's file on node {} (isolation breach!)", i),
                            passed: false,
                            detail: None,
                        });
                        all_ok = false;
                        break;
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Cross-user check error on node {}", i),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok {
                print_and_add_check(&mut result, Check {
                    name: "Owner cannot download Bob's file (all nodes)".to_string(),
                    passed: true,
                    detail: Some("404 on all nodes".to_string()),
                });
            } else {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 13: Bob's listing shows only bob-file.txt ──────────────
        {
            let mut all_ok = true;
            for i in 0..nodes.len() {
                match list_files(&bob_nodes[i], "/").await {
                    Ok(listing) => {
                        let empty = vec![];
                        let files = listing.as_array().unwrap_or(&empty);
                        let paths: Vec<&str> = files
                            .iter()
                            .filter_map(|f| f["path"].as_str())
                            .collect();
                        if paths != vec!["/bob-file.txt"] {
                            print_and_add_check(&mut result, Check {
                                name: format!("Bob listing wrong on node {}", i),
                                passed: false,
                                detail: Some(format!("Expected [\"/bob-file.txt\"], got {:?}", paths)),
                            });
                            all_ok = false;
                            break;
                        }
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Bob listing failed on node {}", i),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok {
                print_and_add_check(&mut result, Check {
                    name: "Bob's listing shows only bob-file.txt (all nodes)".to_string(),
                    passed: true,
                    detail: None,
                });
            } else {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 14: Owner's listing shows only owner-secret.txt ────────
        {
            let mut all_ok = true;
            for (i, node) in nodes.iter().enumerate() {
                match list_files(node, "/").await {
                    Ok(listing) => {
                        let empty = vec![];
                        let files = listing.as_array().unwrap_or(&empty);
                        let paths: Vec<&str> = files
                            .iter()
                            .filter_map(|f| f["path"].as_str())
                            .collect();
                        if paths != vec!["/owner-secret.txt"] {
                            print_and_add_check(&mut result, Check {
                                name: format!("Owner listing wrong on node {}", i),
                                passed: false,
                                detail: Some(format!("Expected [\"/owner-secret.txt\"], got {:?}", paths)),
                            });
                            all_ok = false;
                            break;
                        }
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: format!("Owner listing failed on node {}", i),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok {
                print_and_add_check(&mut result, Check {
                    name: "Owner's listing shows only owner-secret.txt (all nodes)".to_string(),
                    passed: true,
                    detail: None,
                });
            } else {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // ── Step 15: Zero divergence ────────────────────────────────────
        match fetch_state_snapshots(nodes).await {
            Ok(snapshots) => {
                match crate::divergence::build_divergence_report(mesh_id, snapshots) {
                    Ok(report) => {
                        if report.is_full_consensus() {
                            print_and_add_check(&mut result, Check {
                                name: "Zero divergence".to_string(),
                                passed: true,
                                detail: Some(format!(
                                    "{} tables, views {}-{}",
                                    report.table_reports.len(),
                                    report.view_range.0,
                                    report.view_range.1,
                                )),
                            });
                        } else {
                            let divergent: Vec<_> = report
                                .divergent_tables()
                                .iter()
                                .map(|t| t.table_name.as_str())
                                .collect();
                            print_and_add_check(&mut result, Check {
                                name: "Divergence detected".to_string(),
                                passed: false,
                                detail: Some(format!("Divergent tables: {:?}", divergent)),
                            });
                        }
                    }
                    Err(e) => {
                        print_and_add_check(&mut result, Check {
                            name: "Divergence report failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        });
                    }
                }
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "State snapshot fetch failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Multi-user isolation test: created 'bob', roaming login, per-user file isolation across {} nodes",
            nodes.len()
        );

        Ok(result)
    }
}
