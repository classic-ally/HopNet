use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::{
    Check, TestResult, TestScenario, get_max_view, print_and_add_check, wait_for_minimum_view,
};

const COUNT: u32 = 4;

/// Consensus photo identity end to end: explicit-claim responsibility
/// gating, cloud-fingerprint dedupe, and remote adoption across two
/// simulated devices (two data dirs + two device tokens against one mesh —
/// the same shape as two Macs sharing one iCloud library, whose deterministic
/// `e2e-cloud-{i}` ids stand in for PHCloudIdentifiers).
///
/// Pre-build the driver as for photos-ingress-publish.
pub struct PhotosIngressIdentity;

fn base_url(node: &NodeInfo) -> String {
    format!("https://{}:{}", node.ip_address, node.port)
}

/// Run the e2e driver, tolerating scripted non-zero exits; returns
/// (exit code, stdout JSON). Exit 2 = unreachable-park, 3 = responsibility
/// park — both still print a full report.
fn run_driver_status(data_dir: &Path, args: &[String]) -> Result<(i32, serde_json::Value)> {
    let repo_root = env!("CARGO_MANIFEST_DIR");
    let output = std::process::Command::new("cargo")
        .current_dir(repo_root)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            "crates/ingress-publisher/Cargo.toml",
            "--features",
            "e2e-bin",
            "--bin",
            "ingress-publish-e2e",
            "--",
            "--data-dir",
        ])
        .arg(data_dir)
        .args(args)
        .output()
        .context("spawn ingress-publish-e2e")?;
    let code = output.status.code().unwrap_or(-1);
    let report = serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "driver output json (exit {code})\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
    })?;
    Ok((code, report))
}

async fn driver(data_dir: PathBuf, args: Vec<String>) -> Result<(i32, serde_json::Value)> {
    tokio::task::spawn_blocking(move || run_driver_status(&data_dir, &args))
        .await
        .context("driver task")?
}

fn publish_args(node: &NodeInfo, token: &str) -> Vec<String> {
    vec![
        "publish".into(),
        "--node-url".into(),
        base_url(node),
        "--device-token".into(),
        token.to_string(),
    ]
}

async fn register_device(client: &reqwest::Client, node: &NodeInfo, name: &str) -> Result<String> {
    let response = client
        .post(format!("{}/api/devices/register", base_url(node)))
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .json(&serde_json::json!({ "device_name": name }))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "register: {}",
        response.status()
    );
    let body: serde_json::Value = response.json().await?;
    body["api_key"]
        .as_str()
        .map(str::to_string)
        .context("api_key missing")
}

fn device_id(api_key: &str) -> Result<&str> {
    api_key.split('.').next().context("token shape")
}

async fn claim(client: &reqwest::Client, node: &NodeInfo, api_key: &str) -> Result<()> {
    let response = client
        .post(format!("{}/api/photos/ingress/claim", base_url(node)))
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .json(&serde_json::json!({ "device_id": device_id(api_key)? }))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "claim: {}",
        response.status()
    );
    Ok(())
}

async fn responsibility_holder(
    client: &reqwest::Client,
    node: &NodeInfo,
) -> Result<Option<String>> {
    let response = client
        .get(format!(
            "{}/api/photos/ingress/responsibility",
            base_url(node)
        ))
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "responsibility: {}",
        response.status()
    );
    let body: serde_json::Value = response.json().await?;
    Ok(body["device_id"].as_str().map(str::to_string))
}

impl TestScenario for PhotosIngressIdentity {
    fn name(&self) -> &'static str {
        "photos-ingress-identity"
    }

    fn description(&self) -> &'static str {
        "Explicit-claim responsibility gating + cloud-fingerprint remote adoption across two simulated ingress devices (dual-Mac shape) against live consensus"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = super::device_client();
        let dir_a = tempfile::tempdir().context("dir a")?;
        let dir_b = tempfile::tempdir().context("dir b")?;

        println!("\nRunning checks:");

        macro_rules! check_or_bail {
            ($name:expr, $outcome:expr) => {
                match $outcome {
                    Ok(value) => {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: $name.to_string(),
                                passed: true,
                                detail: None,
                            },
                        );
                        value
                    }
                    Err(e) => {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: $name.to_string(),
                                passed: false,
                                detail: Some(e.to_string()),
                            },
                        );
                        result.duration = start.elapsed();
                        return Ok(result);
                    }
                }
            };
        }

        let base_view = check_or_bail!("Get initial consensus view", get_max_view(nodes).await);

        // Device A: register + seed.
        let token_a = check_or_bail!(
            "Register device A",
            register_device(&client, &nodes[0], "Ingress Identity A").await
        );
        check_or_bail!(
            format!("Seed {COUNT} photos into dir A"),
            driver(
                dir_a.path().to_path_buf(),
                vec!["seed".into(), "--count".into(), COUNT.to_string()],
            )
            .await
        );

        // Explicit-claim contract: an unclaimed scope parks the publish
        // (exit 3, nothing submitted, no retry attempts burned).
        check_or_bail!(
            "Unclaimed scope parks publish (exit 3)",
            async {
                let (code, report) = driver(
                    dir_a.path().to_path_buf(),
                    publish_args(&nodes[0], &token_a),
                )
                .await?;
                anyhow::ensure!(code == 3, "expected exit 3, got {code}: {report}");
                anyhow::ensure!(
                    report["parked_responsibility"] == true
                        && report["published"].as_u64() == Some(0)
                        && report["failed"].as_u64() == Some(0),
                    "unexpected park report: {report}"
                );
                Ok(())
            }
            .await
        );

        // Claim for A (JWT route) and verify the holder reads back.
        check_or_bail!(
            "Claim responsibility for device A",
            claim(&client, &nodes[0], &token_a).await
        );
        check_or_bail!(
            "Responsibility reads back as device A",
            async {
                let holder = responsibility_holder(&client, &nodes[0]).await?;
                anyhow::ensure!(
                    holder.as_deref() == Some(device_id(&token_a)?),
                    "holder {holder:?}"
                );
                Ok(())
            }
            .await
        );

        // Holder publishes normally.
        let photos_a = check_or_bail!(
            "Device A publishes all photos",
            async {
                let (code, report) = driver(
                    dir_a.path().to_path_buf(),
                    publish_args(&nodes[0], &token_a),
                )
                .await?;
                anyhow::ensure!(
                    code == 0 && report["published"].as_u64() == Some(COUNT as u64),
                    "exit {code}: {report}"
                );
                let ids: HashSet<String> = report["photos"]
                    .as_array()
                    .context("photos")?
                    .iter()
                    .filter_map(|p| p["photo_id"].as_str().map(String::from))
                    .collect();
                Ok::<_, anyhow::Error>(ids)
            }
            .await
        );

        let consensus_ok =
            wait_for_minimum_view(nodes, base_view + 1, Duration::from_secs(60)).await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Consensus advances on all nodes".to_string(),
                passed: consensus_ok.is_ok(),
                detail: consensus_ok.err().map(|e| e.to_string()),
            },
        );

        // Device B: same asset identities (same deterministic cloud ids), a
        // fresh state.db — the dual-Mac condition. Adoption must converge B
        // on A's consensus ids with zero uploads, even though B holds no
        // responsibility.
        let token_b = check_or_bail!(
            "Register device B",
            register_device(&client, &nodes[0], "Ingress Identity B").await
        );
        check_or_bail!(
            format!("Seed the same {COUNT} identities into dir B"),
            driver(
                dir_b.path().to_path_buf(),
                vec!["seed".into(), "--count".into(), COUNT.to_string()],
            )
            .await
        );
        check_or_bail!(
            "Device B adopts everything, uploads nothing",
            async {
                let (code, report) = driver(
                    dir_b.path().to_path_buf(),
                    publish_args(&nodes[0], &token_b),
                )
                .await?;
                anyhow::ensure!(
                    code == 0
                        && report["adopted"].as_u64() == Some(COUNT as u64)
                        && report["published"].as_u64() == Some(0),
                    "exit {code}: {report}"
                );
                // Every adopted photo maps onto one of A's consensus ids.
                let adopted: Vec<&str> = report["photos"]
                    .as_array()
                    .context("photos")?
                    .iter()
                    .filter_map(|p| p["consensus_photo_id"].as_str())
                    .collect();
                anyhow::ensure!(adopted.len() == COUNT as usize, "adoption ids: {report}");
                for id in adopted {
                    anyhow::ensure!(photos_a.contains(id), "{id} not among device A's ids");
                }
                Ok(())
            }
            .await
        );

        // A genuinely-new asset on B is HELD (A still holds responsibility).
        check_or_bail!(
            "Seed one new asset into dir B",
            driver(
                dir_b.path().to_path_buf(),
                vec![
                    "seed".into(),
                    "--count".into(),
                    "1".into(),
                    "--start".into(),
                    "100".into()
                ],
            )
            .await
        );
        check_or_bail!(
            "Non-holder B parks its new asset (exit 3)",
            async {
                let (code, report) = driver(
                    dir_b.path().to_path_buf(),
                    publish_args(&nodes[0], &token_b),
                )
                .await?;
                anyhow::ensure!(
                    code == 3
                        && report["parked_responsibility"] == true
                        && report["published"].as_u64() == Some(0),
                    "exit {code}: {report}"
                );
                Ok(())
            }
            .await
        );

        // Transfer = re-claim for B; the held asset publishes.
        check_or_bail!(
            "Transfer responsibility to device B",
            claim(&client, &nodes[0], &token_b).await
        );
        check_or_bail!(
            "Responsibility reads back as device B",
            async {
                let holder = responsibility_holder(&client, &nodes[0]).await?;
                anyhow::ensure!(
                    holder.as_deref() == Some(device_id(&token_b)?),
                    "holder {holder:?}"
                );
                Ok(())
            }
            .await
        );
        check_or_bail!(
            "Post-transfer B publishes the held asset",
            async {
                let (code, report) = driver(
                    dir_b.path().to_path_buf(),
                    publish_args(&nodes[0], &token_b),
                )
                .await?;
                anyhow::ensure!(
                    code == 0 && report["published"].as_u64() == Some(1),
                    "exit {code}: {report}"
                );
                Ok(())
            }
            .await
        );

        // Negative: the device route must refuse self-claims — a daemon can
        // never designate itself.
        check_or_bail!(
            "Device-route claim is rejected (400)",
            async {
                let payload = hopnet_photos::envelopes::PhotoIngressClaimPayload {
                    device_id: device_id(&token_b)?.parse().context("device uuid")?,
                    operation_id: hopnet::db::CustomUUID::new(None),
                    library_id: None,
                };
                let bytes = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
                    .context("encode claim")?;
                let response = client
                    .post(format!(
                        "{}/api/photos/client/transaction",
                        base_url(&nodes[0])
                    ))
                    .header("Authorization", format!("Bearer {token_b}"))
                    .json(
                        &serde_json::json!({ "tx_type": "photo_ingress_claim", "payload": bytes }),
                    )
                    .send()
                    .await?;
                anyhow::ensure!(
                    response.status() == reqwest::StatusCode::BAD_REQUEST,
                    "expected 400, got {}",
                    response.status()
                );
                Ok(())
            }
            .await
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
