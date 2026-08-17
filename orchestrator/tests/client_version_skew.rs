//! RFC-023 S4 end-to-end: the client-version skew scenarios. A stale
//! (or header-less) client is cleanly 426'd at the health probe with the
//! structured body; a current client passes and reads the node's
//! version; a node claiming a version older than the mount's MIN_NODE is
//! refused CLIENT-side with the named error — the min_node half of the
//! skew window, exercised through the real daemon in a node container.

use anyhow::{Context, Result};
use bollard::Docker;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::{Check, TestResult, TestScenario, device_client, print_and_add_check};

pub struct ClientVersionSkew;

impl TestScenario for ClientVersionSkew {
    fn name(&self) -> &'static str {
        "client-version-skew"
    }

    fn description(&self) -> &'static str {
        "Stale/header-less clients get structured 426s at the probe; current clients pass; a too-old node is refused client-side by the mount daemon"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let node = &nodes[0];
        let health_url = crate::node_url(node, "/api/integrations/mount/health");

        println!("\nRunning checks:");

        // 1. Header-less probe: 426 with the full policy readout. Nodes
        // serve pinned-HTTPS with self-signed certs — every node-facing
        // client must skip verification (a default reqwest client fails the
        // handshake before the request leaves).
        let bare = crate::insecure_client_builder()
            .build()
            .context("bare client")?
            .get(&health_url)
            .send()
            .await
            .context("bare probe")?;
        let status = bare.status().as_u16();
        let body: serde_json::Value = bare.json().await.unwrap_or_default();
        let shape_ok = status == 426
            && body["surface"] == "/integrations/mount"
            && body["min_client"].as_u64().unwrap_or(0) > 0
            && body["node_version"].as_u64().unwrap_or(0) > 0;
        print_and_add_check(
            &mut result,
            Check {
                name: "headerless probe rejected with structured 426".to_string(),
                passed: shape_ok,
                detail: Some(format!("status {status}, body {body}")),
            },
        );
        if !shape_ok {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 2. Stale header: same rejection.
        let stale = crate::insecure_client_builder()
            .build()
            .context("stale client")?
            .get(&health_url)
            .header(hopnet_common::compat::CLIENT_VERSION_HEADER, "20250101")
            .send()
            .await
            .context("stale probe")?;
        print_and_add_check(
            &mut result,
            Check {
                name: "stale client (2025.1.1) rejected with 426".to_string(),
                passed: stale.status().as_u16() == 426,
                detail: Some(format!("status {}", stale.status())),
            },
        );

        // 3. Current client: 200 carrying the node's identity.
        let current = device_client()
            .get(&health_url)
            .send()
            .await
            .context("current probe")?;
        let current_status = current.status().as_u16();
        let health: serde_json::Value = current.json().await.unwrap_or_default();
        let node_version = health["node_version"].as_u64().unwrap_or(0) as u32;
        print_and_add_check(
            &mut result,
            Check {
                name: "current client passes the probe and reads node_version".to_string(),
                passed: current_status == 200
                    && hopnet_common::version::code_is_valid(node_version),
                detail: Some(format!(
                    "status {current_status}, node_version {node_version}"
                )),
            },
        );

        // 4. Node-too-old, refused CLIENT-side: recreate node 0 claiming
        // an ancient version, then run the real daemon in its container
        // and require the named min_node refusal on stderr.
        let docker = crate::sys::connect().context("docker connect")?;
        super::regenesis::recreate_node_with_env(
            &docker,
            mesh_id,
            node.node_id,
            &[("HOPNET_UPGRADE_VERSION_OVERRIDE", "2020.1.1")],
        )
        .await
        .context("recreate node with old claimed version")?;
        let node = super::regenesis::reauth_node(&docker, mesh_id, node)
            .await
            .context("reauth recreated node")?;

        // A device token so the daemon reaches its preflight (token
        // resolution precedes the probe).
        let register_url = crate::node_url(&node, "/api/devices/register");
        let register: serde_json::Value = device_client()
            .post(&register_url)
            .bearer_auth(&node.jwt_token)
            .json(&serde_json::json!({ "device_name": "skew-test-mount" }))
            .send()
            .await
            .context("register device")?
            .json()
            .await
            .context("register body")?;
        let api_key = register["api_key"]
            .as_str()
            .context("no api_key in register response")?
            .to_string();

        let container = crate::naming::container_name(mesh_id, node.node_id);
        let log = Arc::new(Mutex::new(String::new()));
        super::mount::launch_daemon(&docker, &container, &api_key, log.clone()).await?;

        let deadline = Instant::now() + Duration::from_secs(30);
        let mut refusal = String::new();
        while Instant::now() < deadline {
            {
                let buf = log.lock().expect("log lock");
                if buf.contains("upgrade the node") {
                    refusal = buf.clone();
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "daemon refuses a too-old node with the named min_node error".to_string(),
                passed: refusal.contains("upgrade the node"),
                detail: Some(if refusal.is_empty() {
                    format!("no refusal within 30s; log: {}", log.lock().unwrap())
                } else {
                    "refusal observed".to_string()
                }),
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
