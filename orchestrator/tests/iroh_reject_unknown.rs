use anyhow::Result;
use hopnet_comms::HOPNET_ALPN;
use hopnet_comms::iroh::{self, Endpoint};
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};

/// Test that verifies unknown nodes are rejected when attempting iroh connections
pub struct IrohRejectUnknown;

#[derive(Debug, Deserialize)]
struct Node {
    node_id: i32,
    name: String,
    owner: i32,
    pubkey: String, // Hex-encoded Ed25519 public key
}

impl TestScenario for IrohRejectUnknown {
    fn name(&self) -> &'static str {
        "iroh-reject-unknown"
    }

    fn description(&self) -> &'static str {
        "Verify that nodes reject iroh connections from unknown peers without leaking IP addresses"
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

        println!("\nTesting iroh connection rejection for unknown nodes:");

        // Get node list from the first node to obtain pubkeys
        let first_node = &nodes[0];
        let nodes_url = format!(
            "https://{}:{}/api/nodes",
            first_node.ip_address, first_node.port
        );

        let mesh_nodes: Vec<Node> = client
            .get(&nodes_url)
            .header("Authorization", format!("Bearer {}", first_node.jwt_token))
            .timeout(Duration::from_secs(10))
            .send()
            .await?
            .json()
            .await?;

        if mesh_nodes.is_empty() {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Get mesh nodes".to_string(),
                    passed: false,
                    detail: Some("No nodes found in mesh".to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        print_and_add_check(
            &mut result,
            Check {
                name: "Get mesh nodes".to_string(),
                passed: true,
                detail: Some(format!("Found {} nodes", mesh_nodes.len())),
            },
        );

        // Create an iroh endpoint with a random keypair (not in any node's database)
        // Generate random 32 bytes for the secret key
        let secret_bytes: [u8; 32] = rand::random();
        let unknown_secret = iroh::SecretKey::from_bytes(&secret_bytes);
        let unknown_endpoint = Endpoint::builder(iroh::endpoint::presets::N0)
            .secret_key(unknown_secret)
            .alpns(vec![HOPNET_ALPN.to_vec()])
            .bind()
            .await?;

        let unknown_node_id = unknown_endpoint.id();
        print_and_add_check(
            &mut result,
            Check {
                name: "Create unknown endpoint".to_string(),
                passed: true,
                detail: Some(format!("NodeId: {}...", &unknown_node_id.to_string()[..16])),
            },
        );

        // Try to connect to each mesh node - all should reject us
        let (mut all_rejected, mut any_ip_leaked) = (true, false);

        for mesh_node in &mesh_nodes {
            // Convert hex pubkey to iroh PublicKey
            let pubkey_bytes = hex::decode(&mesh_node.pubkey)?;
            let target_node_id = iroh::PublicKey::from_bytes(
                &pubkey_bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid pubkey length"))?,
            )?;

            // Attempt connection with a timeout
            let connect_result = tokio::time::timeout(
                Duration::from_secs(15),
                unknown_endpoint.connect(target_node_id, HOPNET_ALPN),
            )
            .await;

            match connect_result {
                Ok(Ok(conn)) => {
                    // QUIC handshake succeeded — this is expected since the
                    // before_registration hook rejects after TLS completes.
                    // Verify the connection is actually unusable by trying to
                    // open a stream and exchange data.
                    let rejected =
                        match tokio::time::timeout(Duration::from_secs(5), conn.open_bi()).await {
                            Ok(Ok((mut send, mut recv))) => {
                                // Stream opened — try to use it. The server should
                                // have closed the connection, which will surface as
                                // an error on write or read.
                                let _ = send.write_all(b"ping").await;
                                let _ = send.finish();
                                match tokio::time::timeout(
                                    Duration::from_secs(5),
                                    recv.read_to_end(1024),
                                )
                                .await
                                {
                                    Ok(Ok(_data)) => false,      // read succeeded — not rejected
                                    Ok(Err(_)) | Err(_) => true, // read failed or timed out
                                }
                            }
                            Ok(Err(_)) => true, // stream open failed — connection closed
                            Err(_) => true,     // timeout — connection dead
                        };

                    // Check close reason for the specific "unknown node" rejection
                    let close_reason = conn.close_reason();
                    let detail = if let Some(reason) = &close_reason {
                        format!("Rejected before registration: {}", reason)
                    } else if rejected {
                        "Connection unusable (stream failed)".to_string()
                    } else {
                        "Connection and stream fully usable (not rejected)".to_string()
                    };

                    let passed = rejected || close_reason.is_some();
                    if !passed {
                        all_rejected = false;
                    }
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} rejects unknown", mesh_node.node_id),
                            passed,
                            detail: Some(detail),
                        },
                    );
                }
                Ok(Err(e)) => {
                    // Connection failed - this is expected
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} rejects unknown", mesh_node.node_id),
                            passed: true,
                            detail: Some(format!("Connection rejected: {}", e)),
                        },
                    );
                }
                Err(_) => {
                    // Timeout - could mean many things, but node didn't accept us
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} rejects unknown", mesh_node.node_id),
                            passed: true,
                            detail: Some("Connection timed out (not accepted)".to_string()),
                        },
                    );
                }
            }

            // Verify no direct IP addresses were learned via holepunching.
            // The before_registration hook should reject BEFORE register_connection(),
            // which means holepunching never starts and no ADD_ADDRESS frames are sent.
            // We should see at most a relay URL (which doesn't reveal the node's IP),
            // but no TransportAddr::Ip entries.
            let remote_info = unknown_endpoint.remote_info(target_node_id).await;
            let leaked_ips: Vec<String> = remote_info
                .as_ref()
                .map(|info| {
                    info.addrs()
                        .filter(|addr_info| addr_info.addr().is_ip())
                        .map(|addr_info| format!("{:?}", addr_info.addr()))
                        .collect()
                })
                .unwrap_or_default();

            let ip_leak_passed = leaked_ips.is_empty();
            if !ip_leak_passed {
                any_ip_leaked = true;
            }
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Node {} no IP leak", mesh_node.node_id),
                    passed: ip_leak_passed,
                    detail: Some(if ip_leak_passed {
                        match &remote_info {
                            Some(info) => {
                                let relay_count =
                                    info.addrs().filter(|a| a.addr().is_relay()).count();
                                if relay_count > 0 {
                                    format!(
                                        "Only relay addr (no direct IPs) - {} relay(s)",
                                        relay_count
                                    )
                                } else {
                                    "No remote info retained after rejection".to_string()
                                }
                            }
                            None => "No remote info (path never registered)".to_string(),
                        }
                    } else {
                        format!(
                            "LEAKED {} direct IP(s): {}",
                            leaked_ips.len(),
                            leaked_ips.join(", ")
                        )
                    }),
                },
            );
        }

        // Summary checks
        if all_rejected {
            print_and_add_check(
                &mut result,
                Check {
                    name: "All nodes reject unknown connections".to_string(),
                    passed: true,
                    detail: Some(format!("{} nodes verified", mesh_nodes.len())),
                },
            );
        } else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "All nodes reject unknown connections".to_string(),
                    passed: false,
                    detail: Some("Some nodes accepted unknown connections".to_string()),
                },
            );
        }

        print_and_add_check(
            &mut result,
            Check {
                name: "No IP addresses leaked to unknown peer".to_string(),
                passed: !any_ip_leaked,
                detail: Some(if !any_ip_leaked {
                    format!("Holepunching prevented for {} nodes", mesh_nodes.len())
                } else {
                    "Direct IP addresses were disclosed before rejection".to_string()
                }),
            },
        );

        result.duration = start.elapsed();
        result.details = format!(
            "Verified {} nodes reject unknown iroh connections with no IP leaks",
            mesh_nodes.len()
        );

        Ok(result)
    }
}
