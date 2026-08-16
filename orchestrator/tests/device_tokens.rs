use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

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

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = crate::insecure_client();

        println!("\nRunning device token consistency checks:");

        // Generate unique device name
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let device_name = format!("test-device-{}", timestamp);

        // Step 1: Register device on node 0 (using JWT auth)
        let register_response = match register_device(&client, &nodes[0], &device_name).await {
            Ok(resp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Register device '{}' on node 0", device_name),
                        passed: true,
                        detail: Some(format!("device_id: {}", resp.device_id)),
                    },
                );
                resp
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Device registration failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 2: Poll until device token is accepted on ALL nodes
        let propagation_timeout = Duration::from_secs(60);
        let poll_interval = Duration::from_secs(2);
        let propagated = poll_until_token_accepted(
            &client,
            nodes,
            &register_response.api_key,
            propagation_timeout,
            poll_interval,
        )
        .await;

        if propagated {
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Device token accepted by all {} nodes", nodes.len()),
                    passed: true,
                    detail: None,
                },
            );
        } else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Device token propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Not all nodes accepted the token within {}s",
                        propagation_timeout.as_secs()
                    )),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 3: Revoke device on node 1 (different node than registration)
        let revoke_node = if nodes.len() > 1 { 1 } else { 0 };
        match revoke_device(&client, &nodes[revoke_node], &register_response.device_id).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Revoke device on node {}", revoke_node),
                        passed: true,
                        detail: Some(format!("device_id: {}", register_response.device_id)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Device revocation failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Poll until revoked token is rejected on ALL nodes
        let revoked = poll_until_token_rejected(
            &client,
            nodes,
            &register_response.api_key,
            propagation_timeout,
            poll_interval,
        )
        .await;

        if revoked {
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Revoked token rejected by all {} nodes", nodes.len()),
                    passed: true,
                    detail: None,
                },
            );
        } else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Token revocation propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Not all nodes rejected the token within {}s",
                        propagation_timeout.as_secs()
                    )),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
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

/// Poll until all nodes accept a device token
async fn poll_until_token_accepted(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    timeout: Duration,
    interval: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut all_accepted = true;
        for node in nodes {
            match test_device_token_auth(client, node, api_key).await {
                Ok(true) => {}
                _ => {
                    all_accepted = false;
                    break;
                }
            }
        }
        if all_accepted {
            return true;
        }
        tokio::time::sleep(interval).await;
    }
    false
}

/// Poll until all nodes reject a device token
async fn poll_until_token_rejected(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    timeout: Duration,
    interval: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut all_rejected = true;
        for node in nodes {
            match test_device_token_auth(client, node, api_key).await {
                Ok(false) => {}
                _ => {
                    all_rejected = false;
                    break;
                }
            }
        }
        if all_rejected {
            return true;
        }
        tokio::time::sleep(interval).await;
    }
    false
}

/// Register a device on a node, returns the device_id and api_key
async fn register_device(
    client: &Client,
    node: &NodeInfo,
    device_name: &str,
) -> Result<RegisterDeviceResponse> {
    let url = format!(
        "https://{}:{}/api/devices/register",
        node.ip_address, node.port
    );

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
    let url = format!(
        "https://{}:{}/api/devices/{}",
        node.ip_address, node.port, device_id
    );

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
    let url = format!(
        "https://{}:{}/api/integrations/documentprovider/enumerate",
        node.ip_address, node.port
    );

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
