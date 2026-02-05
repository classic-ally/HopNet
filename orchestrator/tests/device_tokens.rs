use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::NodeInfo;

/// Test that device tokens are consistently replicated and work across all nodes
pub struct DeviceTokenConsistency;

#[derive(Debug, Deserialize)]
struct RegisterDeviceResponse {
    device_id: String,
    api_key: String,
}

impl TestScenario for DeviceTokenConsistency {
    fn name(&self) -> &'static str {
        "device-token-consistency"
    }

    fn description(&self) -> &'static str {
        "Register a device token on one node, verify it works across all nodes, then revoke and verify rejection"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = Client::new();

        println!("\nRunning device token consistency checks:");

        // Generate unique device name
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let device_name = format!("test-device-{}", timestamp);

        // Step 1: Get initial consensus view
        let current_max_view = match get_max_view(nodes).await {
            Ok(view) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Initial max view: {}", view),
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

        // Step 2: Register device on node 0 (using JWT auth)
        let register_response = match register_device(&client, &nodes[0], &device_name).await {
            Ok(resp) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Register device '{}' on node 0", device_name),
                    passed: true,
                    detail: Some(format!("device_id: {}", resp.device_id)),
                });
                resp
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Device registration failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 3: Wait for consensus to propagate
        let target_view = current_max_view + 1;
        let consensus_timeout = Duration::from_secs(30);

        match wait_for_minimum_view(nodes, target_view, consensus_timeout).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Consensus propagated to view {}", target_view),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Consensus propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!("Did not reach view {} within {}s", target_view, consensus_timeout.as_secs())),
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

        // Step 4: Verify device token works on ALL nodes (via documentprovider/enumerate)
        let mut all_nodes_accept = true;
        for (i, node) in nodes.iter().enumerate() {
            match test_device_token_auth(&client, node, &register_response.api_key).await {
                Ok(true) => {
                    // Token accepted
                }
                Ok(false) => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Node {} rejected valid token", i),
                        passed: false,
                        detail: Some("Token should be accepted".to_string()),
                    });
                    all_nodes_accept = false;
                }
                Err(e) => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Node {} auth test failed", i),
                        passed: false,
                        detail: Some(e.to_string()),
                    });
                    all_nodes_accept = false;
                }
            }
        }

        if all_nodes_accept {
            print_and_add_check(&mut result, Check {
                name: format!("Device token accepted by all {} nodes", nodes.len()),
                passed: true,
                detail: None,
            });
        } else {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 5: Get current view before revocation
        let pre_revoke_view = match get_max_view(nodes).await {
            Ok(view) => view,
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get view before revocation".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 6: Revoke device on node 1 (different node than registration)
        let revoke_node = if nodes.len() > 1 { 1 } else { 0 };
        match revoke_device(&client, &nodes[revoke_node], &register_response.device_id).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Revoke device on node {}", revoke_node),
                    passed: true,
                    detail: Some(format!("device_id: {}", register_response.device_id)),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Device revocation failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 7: Wait for revocation to propagate
        let revoke_target_view = pre_revoke_view + 1;

        match wait_for_minimum_view(nodes, revoke_target_view, consensus_timeout).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Revocation propagated to view {}", revoke_target_view),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Revocation propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!("Did not reach view {}", revoke_target_view)),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Revocation consensus check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 8: Verify revoked token is rejected on ALL nodes
        let mut all_nodes_reject = true;
        for (i, node) in nodes.iter().enumerate() {
            match test_device_token_auth(&client, node, &register_response.api_key).await {
                Ok(false) => {
                    // Token correctly rejected
                }
                Ok(true) => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Node {} accepted revoked token", i),
                        passed: false,
                        detail: Some("Revoked token should be rejected".to_string()),
                    });
                    all_nodes_reject = false;
                }
                Err(e) => {
                    // Error could mean rejection, but let's be strict
                    print_and_add_check(&mut result, Check {
                        name: format!("Node {} revoked token test error", i),
                        passed: false,
                        detail: Some(e.to_string()),
                    });
                    all_nodes_reject = false;
                }
            }
        }

        if all_nodes_reject {
            print_and_add_check(&mut result, Check {
                name: format!("Revoked token rejected by all {} nodes", nodes.len()),
                passed: true,
                detail: None,
            });
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Device '{}' registered, verified across {} nodes, revoked, and rejection verified",
            device_name,
            nodes.len()
        );

        Ok(result)
    }
}

/// Register a device on a node, returns the device_id and api_key
async fn register_device(client: &Client, node: &NodeInfo, device_name: &str) -> Result<RegisterDeviceResponse> {
    let url = format!("http://{}:{}/devices/register", node.ip_address, node.port);

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({ "device_name": device_name }))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Registration failed with status {}: {}", status, body);
    }

    let resp: RegisterDeviceResponse = response.json().await?;
    Ok(resp)
}

/// Revoke a device on a node
async fn revoke_device(client: &Client, node: &NodeInfo, device_id: &str) -> Result<()> {
    let url = format!("http://{}:{}/devices/{}", node.ip_address, node.port, device_id);

    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Revocation failed with status {}: {}", status, body);
    }

    Ok(())
}

/// Test if a device token is accepted by a node
/// Returns true if accepted, false if rejected (401/403)
async fn test_device_token_auth(client: &Client, node: &NodeInfo, api_key: &str) -> Result<bool> {
    let url = format!("http://{}:{}/integrations/documentprovider/enumerate", node.ip_address, node.port);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    // 2xx = accepted, 401/403 = rejected
    if response.status().is_success() {
        Ok(true)
    } else if response.status().as_u16() == 401 || response.status().as_u16() == 403 {
        Ok(false)
    } else {
        // Other error
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Unexpected status {}: {}", status, body);
    }
}
