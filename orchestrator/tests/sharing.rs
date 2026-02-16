use anyhow::Result;
use reqwest::Client;
use std::time::{Duration, Instant};

use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::files::{upload_file, download_file, list_files};
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::tests::multi_user::{
    create_user, login_user, node_with_token, try_download_file, fetch_state_snapshots,
};
use crate::NodeInfo;

// ============================================================================
// Sharing API helpers
// ============================================================================

/// POST /shares — share a file with another user by inode_id.
/// Returns Ok(status_code) so callers can assert on 200 vs 409 etc.
async fn share_file(node: &NodeInfo, inode_id: &str, recipient_username: &str) -> Result<u16> {
    let client = Client::new();
    let url = format!("http://{}:{}/shares", node.ip_address, node.port);

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
    let url = format!("http://{}:{}/shares/incoming", node.ip_address, node.port);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GET /shares/incoming failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

/// GET /shares/incoming/count — badge count.
async fn get_incoming_share_count(node: &NodeInfo) -> Result<i64> {
    let client = Client::new();
    let url = format!("http://{}:{}/shares/incoming/count", node.ip_address, node.port);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GET /shares/incoming/count failed with status {}: {}", status, body);
    }

    let body: serde_json::Value = response.json().await?;
    body["count"]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("Missing 'count' in response"))
}

/// POST /shares/{id}/accept — accept a pending share.
async fn accept_share(node: &NodeInfo, share_id: &str, placement_path: &str) -> Result<u16> {
    let client = Client::new();
    let url = format!("http://{}:{}/shares/{}/accept", node.ip_address, node.port, share_id);

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
    let url = format!("http://{}:{}/shares/incoming/{}", node.ip_address, node.port, share_id);

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
    let url = format!("http://{}:{}/shares/file/{}", node.ip_address, node.port, inode_id);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GET /shares/file/{} failed with status {}: {}", inode_id, status, body);
    }

    Ok(response.json().await?)
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
    listing.as_array()?
        .iter()
        .find(|item| item["path"].as_str() == Some(&path))
        .and_then(|item| item["id"].as_str())
        .map(|s| s.to_string())
}

// ============================================================================
// Test
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
            print_and_add_check(&mut result, Check {
                name: "Insufficient nodes".to_string(),
                passed: false,
                detail: Some(format!("Need >= 3 nodes, got {}", nodes.len())),
            });
            result.duration = start.elapsed();
            return Ok(result);
        }

        // ── Phase 1: Setup ──────────────────────────────────────────────

        // Step 1: Initial consensus view
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

        // Step 2: Create user 'bob'
        let passphrase = match create_user(&nodes[0], "bob").await {
            Ok(pp) => {
                print_and_add_check(&mut result, Check {
                    name: "Create user 'bob'".to_string(),
                    passed: true,
                    detail: None,
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

        // Step 3: Wait for user-creation consensus
        let target_view = current_view + 1;
        match wait_for_minimum_view(nodes, target_view, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("User creation consensus (view {})", target_view),
                    passed: true,
                    detail: None,
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "User creation consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Login bob on all nodes
        let bob_nodes = match login_on_all_nodes(nodes, "bob", &passphrase).await {
            Ok(bn) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Bob logged in on all {} nodes", bn.len()),
                    passed: true,
                    detail: None,
                });
                bn
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob login failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Owner uploads "shared-document.txt"
        let file_content = b"This is the shared document content for testing.".to_vec();
        match upload_file(&nodes[0], "/", "shared-document.txt", file_content.clone()).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner uploads shared-document.txt".to_string(),
                    passed: true,
                    detail: Some(format!("{} bytes", file_content.len())),
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

        // Step 6: Wait for upload consensus
        // Use last confirmed view + 1 (not get_max_view, which may already see the new view)
        let view_after_upload = target_view + 1;
        match wait_for_minimum_view(nodes, view_after_upload, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Upload consensus (view >= {})", view_after_upload),
                    passed: true,
                    detail: None,
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "Upload consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 7: Get inode_id from owner's listing
        let inode_id = match list_files(&nodes[0], "/").await {
            Ok(listing) => {
                match extract_inode_id(&listing, "shared-document.txt") {
                    Some(id) => {
                        print_and_add_check(&mut result, Check {
                            name: "Got inode_id for shared-document.txt".to_string(),
                            passed: true,
                            detail: Some(format!("id: {}...", &id[..8.min(id.len())])),
                        });
                        id
                    }
                    None => {
                        print_and_add_check(&mut result, Check {
                            name: "File not found in owner listing".to_string(),
                            passed: false,
                            detail: Some(format!("Listing: {}", listing)),
                        });
                        result.duration = start.elapsed();
                        return Ok(result);
                    }
                }
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner listing failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // ── Phase 2: Share + Accept ─────────────────────────────────────

        // Step 8: Owner shares file with bob
        match share_file(&nodes[0], &inode_id, "bob").await {
            Ok(200) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner shares file with bob".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(status) => {
                print_and_add_check(&mut result, Check {
                    name: "Share file failed".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 200, got {}", status)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Share file request failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 9: Wait for share consensus
        let view_after_share = view_after_upload + 1;
        match wait_for_minimum_view(nodes, view_after_share, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Share consensus (view >= {})", view_after_share),
                    passed: true,
                    detail: None,
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "Share consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 10: Duplicate prevention — share same file with bob again → 409
        match share_file(&nodes[0], &inode_id, "bob").await {
            Ok(409) => {
                print_and_add_check(&mut result, Check {
                    name: "Duplicate share returns 409".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(status) => {
                print_and_add_check(&mut result, Check {
                    name: "Duplicate share did not return 409".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 409, got {}", status)),
                });
                // Non-fatal, continue testing
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Duplicate share request failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // Step 11: Bob checks incoming share count on a non-zero node
        match get_incoming_share_count(&bob_nodes[1]).await {
            Ok(1) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob has 1 incoming share".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(count) => {
                print_and_add_check(&mut result, Check {
                    name: "Unexpected incoming share count".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 1, got {}", count)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Incoming share count failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
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

                print_and_add_check(&mut result, Check {
                    name: "Incoming share metadata correct".to_string(),
                    passed: sender_ok && display_ok,
                    detail: Some(format!(
                        "sender='{}', display_name='{}'{}",
                        sender, display,
                        if !display_ok { " (expected 'shared-document.txt')" } else { "" },
                    )),
                });

                if id.is_empty() {
                    print_and_add_check(&mut result, Check {
                        name: "Missing share id".to_string(),
                        passed: false,
                        detail: None,
                    });
                    result.duration = start.elapsed();
                    return Ok(result);
                }
                id
            }
            Ok(shares) => {
                print_and_add_check(&mut result, Check {
                    name: "Unexpected incoming shares list".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 1 share, got {}", shares.len())),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "List incoming shares failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 13: Bob accepts the share
        match accept_share(&bob_nodes[1], &share_id, "/shared-document.txt").await {
            Ok(200) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob accepts share".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(status) => {
                print_and_add_check(&mut result, Check {
                    name: "Accept share failed".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 200, got {}", status)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Accept share request failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 14: Wait for accept consensus
        let view_after_accept = view_after_share + 1;
        match wait_for_minimum_view(nodes, view_after_accept, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Accept consensus (view >= {})", view_after_accept),
                    passed: true,
                    detail: None,
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "Accept consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
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
                        print_and_add_check(&mut result, Check {
                            name: format!("Bob download mismatch on node {}", i),
                            passed: false,
                            detail: Some(format!("Expected {} bytes, got {}", file_content.len(), data.len())),
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
                    name: format!("Bob downloads shared file from all {} nodes", bob_nodes.len()),
                    passed: true,
                    detail: Some("Content matches original".to_string()),
                });
            } else {
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 16: Bob's incoming count is now 0 (share was accepted)
        match get_incoming_share_count(&bob_nodes[0]).await {
            Ok(0) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob has 0 incoming shares after accept".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(count) => {
                print_and_add_check(&mut result, Check {
                    name: "Incoming count not 0 after accept".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 0, got {}", count)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Incoming count check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // Step 17: Owner's listing shows shared_with_count >= 1
        match list_files(&nodes[0], "/").await {
            Ok(listing) => {
                let shared_count = listing.as_array()
                    .and_then(|arr| arr.iter().find(|f| f["path"].as_str() == Some("/shared-document.txt")))
                    .and_then(|f| f["shared_with_count"].as_u64());
                let ok = shared_count.map(|c| c >= 1).unwrap_or(false);
                print_and_add_check(&mut result, Check {
                    name: "Owner listing shows shared_with_count".to_string(),
                    passed: ok,
                    detail: Some(format!("shared_with_count = {:?}", shared_count)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner listing failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
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
                print_and_add_check(&mut result, Check {
                    name: "Share details shows both users".to_string(),
                    passed: ok,
                    detail: Some(format!("{} participants: {:?}", user_count, usernames)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Share details failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // ── Phase 3: Decline flow ───────────────────────────────────────

        // Step 19: Owner uploads a second file
        let second_content = b"Second file for decline testing.".to_vec();
        match upload_file(&nodes[0], "/", "decline-me.txt", second_content.clone()).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner uploads decline-me.txt".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Second upload failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 20: Wait for upload consensus
        let view_after_upload2 = view_after_accept + 1;
        match wait_for_minimum_view(nodes, view_after_upload2, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Second upload consensus (view >= {})", view_after_upload2),
                    passed: true,
                    detail: None,
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "Second upload consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 21: Get inode_id for second file
        let inode_id_2 = match list_files(&nodes[0], "/").await {
            Ok(listing) => {
                match extract_inode_id(&listing, "decline-me.txt") {
                    Some(id) => id,
                    None => {
                        print_and_add_check(&mut result, Check {
                            name: "decline-me.txt not found in listing".to_string(),
                            passed: false,
                            detail: None,
                        });
                        result.duration = start.elapsed();
                        return Ok(result);
                    }
                }
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner listing for second file failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 22: Owner shares second file with bob
        match share_file(&nodes[0], &inode_id_2, "bob").await {
            Ok(200) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner shares decline-me.txt with bob".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(status) => {
                print_and_add_check(&mut result, Check {
                    name: "Second share failed".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 200, got {}", status)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Second share request failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 23: Wait for share consensus
        let view_after_share2 = view_after_upload2 + 1;
        match wait_for_minimum_view(nodes, view_after_share2, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Second share consensus (view >= {})", view_after_share2),
                    passed: true,
                    detail: None,
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "Second share consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 24: Bob checks incoming count → 1
        match get_incoming_share_count(&bob_nodes[2]).await {
            Ok(1) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob has 1 incoming share (pre-decline)".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(count) => {
                print_and_add_check(&mut result, Check {
                    name: "Pre-decline count unexpected".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 1, got {}", count)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Pre-decline count failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // Step 25: Bob gets the share id, then declines
        let decline_share_id = match get_incoming_shares(&bob_nodes[2]).await {
            Ok(shares) if shares.len() == 1 => {
                shares[0]["id"].as_str().unwrap_or("").to_string()
            }
            Ok(shares) => {
                print_and_add_check(&mut result, Check {
                    name: "Unexpected shares before decline".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 1, got {}", shares.len())),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "List shares before decline failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match decline_share(&bob_nodes[2], &decline_share_id).await {
            Ok(200) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob declines share".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(status) => {
                print_and_add_check(&mut result, Check {
                    name: "Decline share failed".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 200, got {}", status)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Decline share request failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 26: Wait for decline consensus
        let view_after_decline = view_after_share2 + 1;
        match wait_for_minimum_view(nodes, view_after_decline, Duration::from_secs(30)).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Decline consensus (view >= {})", view_after_decline),
                    passed: true,
                    detail: None,
                });
            }
            _ => {
                print_and_add_check(&mut result, Check {
                    name: "Decline consensus timeout".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 27: Bob's incoming count is back to 0
        match get_incoming_share_count(&bob_nodes[0]).await {
            Ok(0) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob has 0 incoming shares after decline".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(count) => {
                print_and_add_check(&mut result, Check {
                    name: "Post-decline count unexpected".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 0, got {}", count)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Post-decline count failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // Step 28: Owner can still download decline-me.txt (unaffected)
        match download_file(&nodes[0], "/decline-me.txt").await {
            Ok(data) if data == second_content => {
                print_and_add_check(&mut result, Check {
                    name: "Owner's declined file unaffected".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Ok(data) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner's declined file content mismatch".to_string(),
                    passed: false,
                    detail: Some(format!("Expected {} bytes, got {}", second_content.len(), data.len())),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Owner download of declined file failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // Step 29: Bob cannot download the declined file
        match try_download_file(&bob_nodes[0], "/decline-me.txt").await {
            Ok(Err(404)) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob cannot download declined file".to_string(),
                    passed: true,
                    detail: Some("404 as expected".to_string()),
                });
            }
            Ok(Err(status)) => {
                print_and_add_check(&mut result, Check {
                    name: "Declined file unexpected status".to_string(),
                    passed: false,
                    detail: Some(format!("Expected 404, got {}", status)),
                });
            }
            Ok(Ok(_)) => {
                print_and_add_check(&mut result, Check {
                    name: "Bob downloaded declined file (should not happen)".to_string(),
                    passed: false,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Declined file check error".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
            }
        }

        // ── Phase 4: Divergence ─────────────────────────────────────────

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
            "Multi-user sharing test: share+accept, duplicate prevention, decline flow across {} nodes",
            nodes.len()
        );

        Ok(result)
    }
}
