use anyhow::Result;
use reqwest::Client;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{delete_file, download_file, list_files, modify_file, upload_file};
use crate::tests::multi_user::{
    create_user, fetch_state_snapshots, login_user, node_with_token, try_download_file,
};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

// ============================================================================
// Sharing API helpers
// ============================================================================

/// POST /shares — share a file with another user by inode_id.
/// Returns Ok(status_code) so callers can assert on 200 vs 409 etc.
async fn share_file(node: &NodeInfo, inode_id: &str, recipient_username: &str) -> Result<u16> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/shares", node.ip_address, node.port);

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "inode_id": inode_id,
            "recipient_username": recipient_username,
        }))
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    Ok(response.status().as_u16())
}

/// GET /shares/incoming — list pending incoming shares.
async fn get_incoming_shares(node: &NodeInfo) -> Result<Vec<serde_json::Value>> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/shares/incoming", node.ip_address, node.port);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "GET /shares/incoming failed with status {}: {}",
            status,
            body
        );
    }

    Ok(response.json().await?)
}

/// GET /shares/incoming/count — badge count.
async fn get_incoming_share_count(node: &NodeInfo) -> Result<i64> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/shares/incoming/count",
        node.ip_address, node.port
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "GET /shares/incoming/count failed with status {}: {}",
            status,
            body
        );
    }

    let body: serde_json::Value = response.json().await?;
    body["count"]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("Missing 'count' in response"))
}

/// POST /shares/{id}/accept — accept a pending share.
async fn accept_share(node: &NodeInfo, share_id: &str, placement_path: &str) -> Result<u16> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/shares/{}/accept",
        node.ip_address, node.port, share_id
    );

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "placement_path": placement_path,
        }))
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    Ok(response.status().as_u16())
}

/// DELETE /shares/incoming/{id} — decline a pending share.
async fn decline_share(node: &NodeInfo, share_id: &str) -> Result<u16> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/shares/incoming/{}",
        node.ip_address, node.port, share_id
    );

    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    Ok(response.status().as_u16())
}

/// GET /shares/file/{inode_id} — sharing detail view.
async fn get_share_details(node: &NodeInfo, inode_id: &str) -> Result<serde_json::Value> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/shares/file/{}",
        node.ip_address, node.port, inode_id
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "GET /shares/file/{} failed with status {}: {}",
            inode_id,
            status,
            body
        );
    }

    Ok(response.json().await?)
}

/// DELETE /shares/file/{inode_id} — unshare (remove self from shared file).
async fn unshare_file(node: &NodeInfo, inode_id: &str) -> Result<u16> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/shares/file/{}",
        node.ip_address, node.port, inode_id
    );

    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    Ok(response.status().as_u16())
}

/// Login a user on all nodes, returning NodeInfo vec sorted by node_id.
async fn login_on_all_nodes(
    nodes: &[NodeInfo],
    username: &str,
    passphrase: &str,
) -> Result<Vec<NodeInfo>> {
    let mut user_nodes = Vec::with_capacity(nodes.len());
    for node in nodes {
        let token = login_user(node, username, passphrase).await?;
        user_nodes.push(node_with_token(node, &token));
    }
    user_nodes.sort_by_key(|n| n.node_id);
    Ok(user_nodes)
}

/// Extract inode_id for a file from a listing response.
fn extract_inode_id(listing: &serde_json::Value, filename: &str) -> Option<String> {
    let path = format!("/{}", filename);
    listing
        .as_array()?
        .iter()
        .find(|item| item["path"].as_str() == Some(&path))
        .and_then(|item| item["id"].as_str())
        .map(|s| s.to_string())
}

// ============================================================================
// Shared test helpers
// ============================================================================

/// Verify that all nodes return the expected content for a download.
/// Returns true if all match, false on first mismatch (and adds a failing check).
async fn verify_download_all_nodes(
    nodes: &[NodeInfo],
    path: &str,
    expected: &[u8],
    label: &str,
    result: &mut TestResult,
) -> bool {
    for (i, node) in nodes.iter().enumerate() {
        match download_file(node, path).await {
            Ok(data) if data == expected => {}
            Ok(data) => {
                let actual_str = String::from_utf8_lossy(&data);
                let expected_str = String::from_utf8_lossy(expected);
                print_and_add_check(
                    result,
                    Check {
                        name: format!("{} - node {} content mismatch", label, i),
                        passed: false,
                        detail: Some(format!(
                            "Expected {:?} ({} bytes), got {:?} ({} bytes)",
                            expected_str,
                            expected.len(),
                            actual_str,
                            data.len()
                        )),
                    },
                );
                return false;
            }
            Err(e) => {
                print_and_add_check(
                    result,
                    Check {
                        name: format!("{} - node {} download failed", label, i),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                return false;
            }
        }
    }
    print_and_add_check(
        result,
        Check {
            name: label.to_string(),
            passed: true,
            detail: Some(format!("Content matches on all {} nodes", nodes.len())),
        },
    );
    true
}

/// Wait for consensus to reach target_view. Returns true on success, false on timeout.
async fn wait_for_view(
    nodes: &[NodeInfo],
    target_view: u64,
    label: &str,
    result: &mut TestResult,
) -> bool {
    match wait_for_minimum_view(nodes, target_view, Duration::from_secs(30)).await {
        Ok(true) => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{} (view >= {})", label, target_view),
                    passed: true,
                    detail: None,
                },
            );
            true
        }
        _ => {
            print_and_add_check(
                result,
                Check {
                    name: format!("{} timeout", label),
                    passed: false,
                    detail: None,
                },
            );
            false
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

pub struct MultiUserSharing;

impl TestScenario for MultiUserSharing {
    fn name(&self) -> &'static str {
        "multi-user-sharing"
    }

    fn description(&self) -> &'static str {
        "Share file between users: accept flow, duplicate prevention, decline flow, and zero divergence"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning multi-user sharing checks:");

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

        // ── Phase 1: Setup ──────────────────────────────────────────────

        // Step 1: Initial consensus view
        let current_view = match get_max_view(nodes).await {
            Ok(view) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Initial consensus view: {}", view),
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
                        name: "Failed to get initial view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 2: Create user 'bob'
        let passphrase = match create_user(&nodes[0], "bob").await {
            Ok(pp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Create user 'bob'".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
                pp
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Create user 'bob' failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 3: Wait for user-creation consensus
        let target_view = current_view + 1;
        match wait_for_minimum_view(nodes, target_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("User creation consensus (view {})", target_view),
                        passed: true,
                        detail: None,
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "User creation consensus timeout".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Login bob on all nodes
        let bob_nodes = match login_on_all_nodes(nodes, "bob", &passphrase).await {
            Ok(bn) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Bob logged in on all {} nodes", bn.len()),
                        passed: true,
                        detail: None,
                    },
                );
                bn
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob login failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Owner uploads "shared-document.txt"
        let file_content = b"This is the shared document content for testing.".to_vec();
        match upload_file(&nodes[0], "/", "shared-document.txt", file_content.clone()).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner uploads shared-document.txt".to_string(),
                        passed: true,
                        detail: Some(format!("{} bytes", file_content.len())),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner upload failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 6: Wait for upload consensus
        // Use last confirmed view + 1 (not get_max_view, which may already see the new view)
        let view_after_upload = target_view + 1;
        match wait_for_minimum_view(nodes, view_after_upload, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Upload consensus (view >= {})", view_after_upload),
                        passed: true,
                        detail: None,
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload consensus timeout".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 7: Get inode_id from owner's listing
        let inode_id = match list_files(&nodes[0], "/").await {
            Ok(listing) => match extract_inode_id(&listing, "shared-document.txt") {
                Some(id) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Got inode_id for shared-document.txt".to_string(),
                            passed: true,
                            detail: Some(format!("id: {}...", &id[..8.min(id.len())])),
                        },
                    );
                    id
                }
                None => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "File not found in owner listing".to_string(),
                            passed: false,
                            detail: Some(format!("Listing: {}", listing)),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            },
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner listing failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // ── Phase 2: Share + Accept ─────────────────────────────────────

        // Step 8: Owner shares file with bob
        match share_file(&nodes[0], &inode_id, "bob").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner shares file with bob".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share file failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share file request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 9: Wait for share consensus
        let view_after_share = view_after_upload + 1;
        match wait_for_minimum_view(nodes, view_after_share, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Share consensus (view >= {})", view_after_share),
                        passed: true,
                        detail: None,
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share consensus timeout".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 10: Duplicate prevention — share same file with bob again → 409
        match share_file(&nodes[0], &inode_id, "bob").await {
            Ok(409) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Duplicate share returns 409".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Duplicate share did not return 409".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 409, got {}", status)),
                    },
                );
                // Non-fatal, continue testing
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Duplicate share request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 11: Bob checks incoming share count on a non-zero node
        match get_incoming_share_count(&bob_nodes[1]).await {
            Ok(1) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob has 1 incoming share".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(count) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Unexpected incoming share count".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 1, got {}", count)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Incoming share count failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 12: Bob lists incoming shares — verify display name + sender
        let share_id = match get_incoming_shares(&bob_nodes[1]).await {
            Ok(shares) if shares.len() == 1 => {
                let share = &shares[0];
                let sender = share["sender_username"].as_str().unwrap_or("");
                let display = share["display_name"].as_str().unwrap_or("");
                let id = share["id"].as_str().unwrap_or("").to_string();

                let sender_ok = !sender.is_empty();
                let display_ok = display == "shared-document.txt";

                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Incoming share metadata correct".to_string(),
                        passed: sender_ok && display_ok,
                        detail: Some(format!(
                            "sender='{}', display_name='{}'{}",
                            sender,
                            display,
                            if !display_ok {
                                " (expected 'shared-document.txt')"
                            } else {
                                ""
                            },
                        )),
                    },
                );

                if id.is_empty() {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Missing share id".to_string(),
                            passed: false,
                            detail: None,
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
                id
            }
            Ok(shares) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Unexpected incoming shares list".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 1 share, got {}", shares.len())),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "List incoming shares failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 13: Bob accepts the share
        match accept_share(&bob_nodes[1], &share_id, "/shared-document.txt").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob accepts share".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Accept share failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Accept share request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 14: Wait for accept consensus
        let view_after_accept = view_after_share + 1;
        match wait_for_minimum_view(nodes, view_after_accept, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Accept consensus (view >= {})", view_after_accept),
                        passed: true,
                        detail: None,
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Accept consensus timeout".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 15: Bob downloads shared file from all nodes — content matches
        {
            let mut all_ok = true;
            for (i, bob_node) in bob_nodes.iter().enumerate() {
                match download_file(bob_node, "/shared-document.txt").await {
                    Ok(data) if data == file_content => {}
                    Ok(data) => {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: format!("Bob download mismatch on node {}", i),
                                passed: false,
                                detail: Some(format!(
                                    "Expected {} bytes, got {}",
                                    file_content.len(),
                                    data.len()
                                )),
                            },
                        );
                        all_ok = false;
                        break;
                    }
                    Err(e) => {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: format!("Bob download failed on node {}", i),
                                passed: false,
                                detail: Some(e.to_string()),
                            },
                        );
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!(
                            "Bob downloads shared file from all {} nodes",
                            bob_nodes.len()
                        ),
                        passed: true,
                        detail: Some("Content matches original".to_string()),
                    },
                );
            } else {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 16: Bob's incoming count is now 0 (share was accepted)
        match get_incoming_share_count(&bob_nodes[0]).await {
            Ok(0) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob has 0 incoming shares after accept".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(count) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Incoming count not 0 after accept".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 0, got {}", count)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Incoming count check failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 17: Owner's listing shows shared_with_count >= 1
        match list_files(&nodes[0], "/").await {
            Ok(listing) => {
                let shared_count = listing
                    .as_array()
                    .and_then(|arr| {
                        arr.iter()
                            .find(|f| f["path"].as_str() == Some("/shared-document.txt"))
                    })
                    .and_then(|f| f["shared_with_count"].as_u64());
                let ok = shared_count.map(|c| c >= 1).unwrap_or(false);
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner listing shows shared_with_count".to_string(),
                        passed: ok,
                        detail: Some(format!("shared_with_count = {:?}", shared_count)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner listing failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 18: Share details shows both users
        match get_share_details(&nodes[0], &inode_id).await {
            Ok(details) => {
                let users = details["users"].as_array();
                let user_count = users.map(|u| u.len()).unwrap_or(0);
                // Expect 2 participants (owner + bob)
                let ok = user_count == 2;
                let usernames: Vec<&str> = users
                    .map(|arr| arr.iter().filter_map(|u| u["username"].as_str()).collect())
                    .unwrap_or_default();
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share details shows both users".to_string(),
                        passed: ok,
                        detail: Some(format!("{} participants: {:?}", user_count, usernames)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share details failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // ── Phase 3: Decline flow ───────────────────────────────────────

        // Step 19: Owner uploads a second file
        let second_content = b"Second file for decline testing.".to_vec();
        match upload_file(&nodes[0], "/", "decline-me.txt", second_content.clone()).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner uploads decline-me.txt".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
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
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 20: Wait for upload consensus
        let view_after_upload2 = view_after_accept + 1;
        match wait_for_minimum_view(nodes, view_after_upload2, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Second upload consensus (view >= {})", view_after_upload2),
                        passed: true,
                        detail: None,
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Second upload consensus timeout".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 21: Get inode_id for second file
        let inode_id_2 = match list_files(&nodes[0], "/").await {
            Ok(listing) => match extract_inode_id(&listing, "decline-me.txt") {
                Some(id) => id,
                None => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "decline-me.txt not found in listing".to_string(),
                            passed: false,
                            detail: None,
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            },
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner listing for second file failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 22: Owner shares second file with bob
        match share_file(&nodes[0], &inode_id_2, "bob").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner shares decline-me.txt with bob".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Second share failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Second share request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 23: Wait for share consensus
        let view_after_share2 = view_after_upload2 + 1;
        match wait_for_minimum_view(nodes, view_after_share2, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Second share consensus (view >= {})", view_after_share2),
                        passed: true,
                        detail: None,
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Second share consensus timeout".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 24: Bob checks incoming count → 1
        match get_incoming_share_count(&bob_nodes[2]).await {
            Ok(1) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob has 1 incoming share (pre-decline)".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(count) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Pre-decline count unexpected".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 1, got {}", count)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Pre-decline count failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 25: Bob gets the share id, then declines
        let decline_share_id = match get_incoming_shares(&bob_nodes[2]).await {
            Ok(shares) if shares.len() == 1 => shares[0]["id"].as_str().unwrap_or("").to_string(),
            Ok(shares) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Unexpected shares before decline".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 1, got {}", shares.len())),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "List shares before decline failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match decline_share(&bob_nodes[2], &decline_share_id).await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob declines share".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Decline share failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Decline share request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 26: Wait for decline consensus
        let view_after_decline = view_after_share2 + 1;
        match wait_for_minimum_view(nodes, view_after_decline, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Decline consensus (view >= {})", view_after_decline),
                        passed: true,
                        detail: None,
                    },
                );
            }
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Decline consensus timeout".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 27: Bob's incoming count is back to 0
        match get_incoming_share_count(&bob_nodes[0]).await {
            Ok(0) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob has 0 incoming shares after decline".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(count) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Post-decline count unexpected".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 0, got {}", count)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Post-decline count failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 28: Owner can still download decline-me.txt (unaffected)
        match download_file(&nodes[0], "/decline-me.txt").await {
            Ok(data) if data == second_content => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner's declined file unaffected".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(data) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner's declined file content mismatch".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "Expected {} bytes, got {}",
                            second_content.len(),
                            data.len()
                        )),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner download of declined file failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 29: Bob cannot download the declined file
        match try_download_file(&bob_nodes[0], "/decline-me.txt").await {
            Ok(Err(404)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob cannot download declined file".to_string(),
                        passed: true,
                        detail: Some("404 as expected".to_string()),
                    },
                );
            }
            Ok(Err(status)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Declined file unexpected status".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 404, got {}", status)),
                    },
                );
            }
            Ok(Ok(_)) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob downloaded declined file (should not happen)".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Declined file check error".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // ── Phase 4: Divergence ─────────────────────────────────────────

        match fetch_state_snapshots(nodes).await {
            Ok(snapshots) => match crate::divergence::build_divergence_report(mesh_id, snapshots) {
                Ok(report) => {
                    if report.is_full_consensus() {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: "Zero divergence".to_string(),
                                passed: true,
                                detail: Some(format!(
                                    "{} tables, heights {}-{}",
                                    report.table_reports.len(),
                                    report.height_range.0,
                                    report.height_range.1,
                                )),
                            },
                        );
                    } else {
                        let divergent: Vec<_> = report
                            .divergent_tables()
                            .iter()
                            .map(|t| t.table_name.as_str())
                            .collect();
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: "Divergence detected".to_string(),
                                passed: false,
                                detail: Some(format!("Divergent tables: {:?}", divergent)),
                            },
                        );
                    }
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Divergence report failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                }
            },
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "State snapshot fetch failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Multi-user sharing test: share+accept, duplicate prevention, decline flow across {} nodes",
            nodes.len()
        );

        Ok(result)
    }
}

pub struct MultiUserSharingLiveLink;

impl TestScenario for MultiUserSharingLiveLink {
    fn name(&self) -> &'static str {
        "multi-user-sharing-live-link"
    }

    fn description(&self) -> &'static str {
        "Live-link propagation, unshare copy-on-write, and delete cleanup across shared files"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning multi-user sharing live-link checks:");

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

        // ── Phase 1: Setup ──────────────────────────────────────────────

        // Step 1: Initial consensus view
        let mut current_view = match get_max_view(nodes).await {
            Ok(view) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Initial consensus view: {}", view),
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
                        name: "Failed to get initial view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 2: Create user 'bob', wait for consensus, login on all nodes
        let bob_passphrase = match create_user(&nodes[0], "bob").await {
            Ok(pp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Create user 'bob'".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
                pp
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Create user 'bob' failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        current_view += 1;
        if !wait_for_view(
            nodes,
            current_view,
            "User bob creation consensus",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        let bob_nodes = match login_on_all_nodes(nodes, "bob", &bob_passphrase).await {
            Ok(bn) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Bob logged in on all {} nodes", bn.len()),
                        passed: true,
                        detail: None,
                    },
                );
                bn
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob login failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 3: Owner uploads "live-link-test.txt" with content "version-1"
        let v1 = b"version-1".to_vec();
        match upload_file(&nodes[0], "/", "live-link-test.txt", v1.clone()).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner uploads live-link-test.txt (version-1)".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner upload failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Upload consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 4: Extract owner_inode_id from owner's listing
        let owner_inode_id = match list_files(&nodes[0], "/").await {
            Ok(listing) => match extract_inode_id(&listing, "live-link-test.txt") {
                Some(id) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Got owner inode_id".to_string(),
                            passed: true,
                            detail: Some(format!("id: {}...", &id[..8.min(id.len())])),
                        },
                    );
                    id
                }
                None => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "File not found in owner listing".to_string(),
                            passed: false,
                            detail: None,
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            },
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner listing failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Owner shares file with bob
        match share_file(&nodes[0], &owner_inode_id, "bob").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner shares file with bob".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share with bob failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share with bob request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Share consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 6: Bob accepts share
        let bob_share_id = match get_incoming_shares(&bob_nodes[0]).await {
            Ok(shares) if shares.len() == 1 => shares[0]["id"].as_str().unwrap_or("").to_string(),
            Ok(shares) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob incoming shares unexpected".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 1, got {}", shares.len())),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob get incoming shares failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match accept_share(&bob_nodes[0], &bob_share_id, "/live-link-test.txt").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob accepts share".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob accept failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob accept request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Accept consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 7: Verify bob can download and content matches version-1
        if !verify_download_all_nodes(
            &bob_nodes,
            "/live-link-test.txt",
            &v1,
            "Bob downloads version-1",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // ── Phase 2: Live-link propagation ──────────────────────────────

        // Step 8: Owner modifies file to version-2
        let v2 = b"version-2".to_vec();
        match modify_file(&nodes[0], &owner_inode_id, v2.clone()).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner modifies file to version-2".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Modify to version-2 failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Modify v2 consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 9: Bob downloads from all nodes → version-2
        if !verify_download_all_nodes(
            &bob_nodes,
            "/live-link-test.txt",
            &v2,
            "Bob downloads version-2 (live-link)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 10: Owner downloads from all nodes → version-2 (sanity)
        if !verify_download_all_nodes(
            nodes,
            "/live-link-test.txt",
            &v2,
            "Owner downloads version-2 (sanity)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // ── Phase 3: Multi-sharer propagation ───────────────────────────

        // Step 11: Create user 'carol', wait, login
        let carol_passphrase = match create_user(&nodes[0], "carol").await {
            Ok(pp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Create user 'carol'".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
                pp
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Create user 'carol' failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Carol creation consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        let carol_nodes = match login_on_all_nodes(nodes, "carol", &carol_passphrase).await {
            Ok(cn) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Carol logged in on all {} nodes", cn.len()),
                        passed: true,
                        detail: None,
                    },
                );
                cn
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Carol login failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 12: Owner shares file with carol
        match share_file(&nodes[0], &owner_inode_id, "carol").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner shares file with carol".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share with carol failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share with carol request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Share carol consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 13: Carol accepts share
        let carol_share_id = match get_incoming_shares(&carol_nodes[0]).await {
            Ok(shares) if shares.len() == 1 => shares[0]["id"].as_str().unwrap_or("").to_string(),
            Ok(shares) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Carol incoming shares unexpected".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 1, got {}", shares.len())),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Carol get incoming shares failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match accept_share(&carol_nodes[0], &carol_share_id, "/live-link-test.txt").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Carol accepts share".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Carol accept failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Carol accept request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Carol accept consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 14: Owner modifies → version-3
        let v3 = b"version-3".to_vec();
        match modify_file(&nodes[0], &owner_inode_id, v3.clone()).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner modifies file to version-3".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Modify to version-3 failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Modify v3 consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 15: Bob and carol both download → version-3
        if !verify_download_all_nodes(
            &bob_nodes,
            "/live-link-test.txt",
            &v3,
            "Bob downloads version-3 (multi-sharer)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }
        if !verify_download_all_nodes(
            &carol_nodes,
            "/live-link-test.txt",
            &v3,
            "Carol downloads version-3 (multi-sharer)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // ── Phase 4: Pending share propagation ──────────────────────────

        // Step 16: Create user 'dave', wait, login
        let dave_passphrase = match create_user(&nodes[0], "dave").await {
            Ok(pp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Create user 'dave'".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
                pp
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Create user 'dave' failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Dave creation consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        let dave_nodes = match login_on_all_nodes(nodes, "dave", &dave_passphrase).await {
            Ok(dn) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Dave logged in on all {} nodes", dn.len()),
                        passed: true,
                        detail: None,
                    },
                );
                dn
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Dave login failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 17: Owner shares file with dave (don't accept yet)
        match share_file(&nodes[0], &owner_inode_id, "dave").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner shares file with dave (pending)".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share with dave failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Share with dave request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Share dave consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 18: Owner modifies → version-4 (while dave's share is still pending)
        let v4 = b"version-4".to_vec();
        match modify_file(&nodes[0], &owner_inode_id, v4.clone()).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner modifies file to version-4 (dave pending)".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Modify to version-4 failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Modify v4 consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 19: Dave accepts share
        let dave_share_id = match get_incoming_shares(&dave_nodes[0]).await {
            Ok(shares) if shares.len() == 1 => shares[0]["id"].as_str().unwrap_or("").to_string(),
            Ok(shares) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Dave incoming shares unexpected".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 1, got {}", shares.len())),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Dave get incoming shares failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match accept_share(&dave_nodes[0], &dave_share_id, "/live-link-test.txt").await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Dave accepts share".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Dave accept failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Dave accept request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Dave accept consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 20: Dave downloads → version-4 (latest, not version at share-time)
        if !verify_download_all_nodes(
            &dave_nodes,
            "/live-link-test.txt",
            &v4,
            "Dave downloads version-4 (pending share propagation)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // ── Phase 5: Unshare (copy-on-write) ────────────────────────────

        // Step 21: Bob extracts bob_inode_id from bob's listing
        let bob_inode_id = match list_files(&bob_nodes[0], "/").await {
            Ok(listing) => match extract_inode_id(&listing, "live-link-test.txt") {
                Some(id) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Got bob's inode_id".to_string(),
                            passed: true,
                            detail: Some(format!("id: {}...", &id[..8.min(id.len())])),
                        },
                    );
                    id
                }
                None => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "File not found in bob's listing".to_string(),
                            passed: false,
                            detail: None,
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            },
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob listing failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 22: Bob unshares
        match unshare_file(&bob_nodes[0], &bob_inode_id).await {
            Ok(200) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob unshares file".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Ok(status) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob unshare failed".to_string(),
                        passed: false,
                        detail: Some(format!("Expected 200, got {}", status)),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Bob unshare request failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Unshare consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 23: Owner modifies → version-5
        let v5 = b"version-5".to_vec();
        match modify_file(&nodes[0], &owner_inode_id, v5.clone()).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner modifies file to version-5".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Modify to version-5 failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Modify v5 consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 24: Bob downloads → still version-4 (frozen at unshare point)
        if !verify_download_all_nodes(
            &bob_nodes,
            "/live-link-test.txt",
            &v4,
            "Bob downloads version-4 (frozen after unshare)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 25: Carol downloads → version-5 (still live-linked)
        if !verify_download_all_nodes(
            &carol_nodes,
            "/live-link-test.txt",
            &v5,
            "Carol downloads version-5 (still live-linked)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 26: Dave downloads → version-5 (still live-linked)
        if !verify_download_all_nodes(
            &dave_nodes,
            "/live-link-test.txt",
            &v5,
            "Dave downloads version-5 (still live-linked)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // ── Phase 6: Delete cleanup ─────────────────────────────────────

        // Step 27: Owner deletes file
        match delete_file(&nodes[0], "/live-link-test.txt").await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner deletes file".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner delete failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        current_view += 1;
        if !wait_for_view(nodes, current_view, "Delete consensus", &mut result).await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 28: Carol still has the file (own inode, unaffected by owner delete)
        if !verify_download_all_nodes(
            &carol_nodes,
            "/live-link-test.txt",
            &v5,
            "Carol downloads version-5 (after owner delete)",
            &mut result,
        )
        .await
        {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 29: Owner listing → file gone
        match list_files(&nodes[0], "/").await {
            Ok(listing) => {
                let found = extract_inode_id(&listing, "live-link-test.txt").is_some();
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner listing: file gone".to_string(),
                        passed: !found,
                        detail: if found {
                            Some("File still in listing".to_string())
                        } else {
                            None
                        },
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner listing check failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 30: Share details for carol's inode → should not include owner
        let carol_inode_id = match list_files(&carol_nodes[0], "/").await {
            Ok(listing) => extract_inode_id(&listing, "live-link-test.txt").unwrap_or_default(),
            Err(_) => String::new(),
        };
        if !carol_inode_id.is_empty() {
            match get_share_details(&carol_nodes[0], &carol_inode_id).await {
                Ok(details) => {
                    let users = details["users"].as_array();
                    let usernames: Vec<&str> = users
                        .map(|arr| arr.iter().filter_map(|u| u["username"].as_str()).collect())
                        .unwrap_or_default();
                    // Owner should not be in the share details anymore
                    let owner_present = usernames.iter().any(|u| {
                        // The owner is the default user (first node's user), check if it's NOT carol/bob/dave
                        *u != "carol" && *u != "bob" && *u != "dave"
                    });
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Carol share details: owner removed".to_string(),
                            passed: !owner_present,
                            detail: Some(format!("participants: {:?}", usernames)),
                        },
                    );
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Carol share details check failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                }
            }
        }

        // ── Phase 7: Divergence ─────────────────────────────────────────

        match fetch_state_snapshots(nodes).await {
            Ok(snapshots) => match crate::divergence::build_divergence_report(mesh_id, snapshots) {
                Ok(report) => {
                    if report.is_full_consensus() {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: "Zero divergence".to_string(),
                                passed: true,
                                detail: Some(format!(
                                    "{} tables, heights {}-{}",
                                    report.table_reports.len(),
                                    report.height_range.0,
                                    report.height_range.1,
                                )),
                            },
                        );
                    } else {
                        let divergent: Vec<_> = report
                            .divergent_tables()
                            .iter()
                            .map(|t| t.table_name.as_str())
                            .collect();
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: "Divergence detected".to_string(),
                                passed: false,
                                detail: Some(format!("Divergent tables: {:?}", divergent)),
                            },
                        );
                    }
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Divergence report failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                }
            },
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "State snapshot fetch failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Live-link sharing test: propagation, multi-sharer, pending share, unshare COW, delete cleanup across {} nodes",
            nodes.len()
        );

        Ok(result)
    }
}
