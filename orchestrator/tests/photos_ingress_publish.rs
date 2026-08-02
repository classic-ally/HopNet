use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::{
    Check, TestResult, TestScenario, get_max_view, print_and_add_check, wait_for_minimum_view,
};
use hopnet::dev_seed;

const COUNT: u32 = 6;

/// End-to-end ingress publish: fabricate real daemon state (state.db +
/// blobs + sidecars via the ingress drain pipeline — no PhotoKit) with the
/// `ingress-publish-e2e` driver, publish it into node 0 over the
/// device-token thin-client routes, then verify consensus advance, sidecar
/// parity, byte-identical content on every node, and the confirm-then-retry
/// idempotency contract against live consensus.
///
/// The driver is invoked via `cargo run` against the crates/ workspace —
/// pre-build it (`cargo build --manifest-path crates/ingress-publisher/Cargo.toml
/// --features e2e-bin`) to keep first-run compile time out of the scenario.
pub struct PhotosIngressPublish;

fn base_url(node: &NodeInfo) -> String {
    format!("http://{}:{}", node.ip_address, node.port)
}

/// Run the e2e driver; returns its stdout JSON.
fn run_driver(data_dir: &Path, args: &[String]) -> Result<serde_json::Value> {
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
    anyhow::ensure!(
        output.status.success(),
        "ingress-publish-e2e {:?} failed\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    serde_json::from_slice(&output.stdout).context("driver output json")
}

async fn run_driver_async(data_dir: PathBuf, args: Vec<String>) -> Result<serde_json::Value> {
    tokio::task::spawn_blocking(move || run_driver(&data_dir, &args))
        .await
        .context("driver task")?
}

/// (photo_id, [(resource type name, blake3 hex)]) from a driver report.
fn report_photos(report: &serde_json::Value) -> Result<Vec<(String, Vec<(String, String)>)>> {
    report["photos"]
        .as_array()
        .context("photos array")?
        .iter()
        .map(|photo| {
            let id = photo["photo_id"].as_str().context("photo_id")?.to_string();
            let resources = photo["resources"]
                .as_array()
                .context("resources")?
                .iter()
                .map(|r| {
                    Ok((
                        r["type"].as_str().context("type")?.to_string(),
                        r["blake3"].as_str().context("blake3")?.to_string(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((id, resources))
        })
        .collect()
}

/// Poll a node's gallery until every expected photo id appears.
pub(crate) async fn wait_for_ids(
    client: &reqwest::Client,
    node: &NodeInfo,
    expected: &[String],
    timeout: Duration,
) -> Result<usize> {
    let deadline = Instant::now() + timeout;
    loop {
        let response = client
            .get(format!("{}/api/photos/gallery?limit=200", base_url(node)))
            .header("Authorization", format!("Bearer {}", node.jwt_token))
            .send()
            .await;
        let rows: Vec<serde_json::Value> = match response {
            Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
            _ => Vec::new(),
        };
        let present = expected
            .iter()
            .filter(|id| {
                rows.iter()
                    .any(|row| row["photo_id"].as_str() == Some(id.as_str()))
            })
            .count();
        if present == expected.len() {
            return Ok(rows.len());
        }
        if Instant::now() > deadline {
            anyhow::bail!("{present} of {} expected photos in gallery", expected.len());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Content fetch with backoff (remote fragment discovery on non-uploader
/// nodes takes a few seconds on first access).
pub(crate) async fn fetch_resource_with_retry(
    client: &reqwest::Client,
    node: &NodeInfo,
    photo_id: &str,
    kind_name: &str,
    timeout: Duration,
) -> Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    let mut last_error;
    loop {
        let result = client
            .get(format!(
                "{}/api/photos/{photo_id}/resource/{kind_name}",
                base_url(node)
            ))
            .header("Authorization", format!("Bearer {}", node.jwt_token))
            .send()
            .await;
        match result {
            Ok(response) if response.status().is_success() => {
                return Ok(response.bytes().await?.to_vec());
            }
            Ok(response) => last_error = format!("{}", response.status()),
            Err(e) => last_error = e.to_string(),
        }
        if Instant::now() > deadline {
            anyhow::bail!("resource {kind_name} of {photo_id}: {last_error}");
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
}

impl TestScenario for PhotosIngressPublish {
    fn name(&self) -> &'static str {
        "photos-ingress-publish"
    }

    fn description(&self) -> &'static str {
        "Publish fabricated ingress-daemon state into node 0 over the device-token thin-client routes; verify cross-node content parity and confirm-then-retry idempotency"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = reqwest::Client::new();
        let data_dir = tempfile::tempdir().context("temp data dir")?;

        println!("\nRunning checks:");

        macro_rules! check_or_bail {
            ($name:expr, $outcome:expr) => {
                match $outcome {
                    Ok(value) => {
                        print_and_add_check(
                            &mut result,
                            Check { name: $name.to_string(), passed: true, detail: None },
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

        // Step 1: consensus baseline.
        let base_view = check_or_bail!("Get initial consensus view", get_max_view(nodes).await);

        // Step 2: register the daemon's device token on node 0 (RFC-012).
        let api_key = check_or_bail!("Register ingress device token", async {
            let response = client
                .post(format!("{}/api/devices/register", base_url(&nodes[0])))
                .header("Authorization", format!("Bearer {}", nodes[0].jwt_token))
                .json(&serde_json::json!({ "device_name": "Photo Ingress E2E" }))
                .send()
                .await?;
            anyhow::ensure!(response.status().is_success(), "register: {}", response.status());
            let body: serde_json::Value = response.json().await?;
            body["api_key"]
                .as_str()
                .map(str::to_string)
                .context("api_key missing")
        }
        .await);

        // Step 2b: claim ingress responsibility for the device (explicit-claim
        // contract — an unclaimed scope parks every publish).
        check_or_bail!("Claim ingress responsibility", async {
            let device_id = api_key.split('.').next().context("token shape")?;
            let response = client
                .post(format!("{}/api/photos/ingress/claim", base_url(&nodes[0])))
                .header("Authorization", format!("Bearer {}", nodes[0].jwt_token))
                .json(&serde_json::json!({ "device_id": device_id }))
                .send()
                .await?;
            anyhow::ensure!(response.status().is_success(), "claim: {}", response.status());
            Ok(())
        }
        .await);

        // Step 3: fabricate daemon state (seed → drain, real pipeline).
        let seed_report = check_or_bail!(
            format!("Fabricate {COUNT} ingress photos"),
            run_driver_async(
                data_dir.path().to_path_buf(),
                vec!["seed".into(), "--count".into(), COUNT.to_string()],
            )
            .await
        );
        let photos = check_or_bail!("Parse seed report", report_photos(&seed_report));

        // Step 4: publish into node 0 over the thin-client routes.
        let publish_args = |token: &str| {
            vec![
                "publish".into(),
                "--node-url".into(),
                base_url(&nodes[0]),
                "--device-token".into(),
                token.to_string(),
            ]
        };
        let publish_report = check_or_bail!(
            format!("Publish {COUNT} photos via device token"),
            run_driver_async(data_dir.path().to_path_buf(), publish_args(&api_key))
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["published"].as_u64() == Some(COUNT as u64),
                        "published {} of {COUNT}: {report}",
                        report["published"]
                    );
                    Ok(report)
                })
        );
        drop(publish_report);

        // Step 5: every photo_add is a consensus transaction.
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

        // Step 6: sidecars drain the published photos on every node.
        let expected_ids: Vec<String> = photos.iter().map(|(id, _)| id.clone()).collect();
        for node in nodes {
            let outcome = async {
                dev_seed::enable_sidecar(&client, &base_url(node), &node.jwt_token).await?;
                wait_for_ids(&client, node, &expected_ids, Duration::from_secs(45)).await
            }
            .await;
            let name = format!("Node {} gallery holds all {COUNT} photos", node.node_id);
            match outcome {
                Ok(total) => print_and_add_check(
                    &mut result,
                    Check {
                        name,
                        passed: true,
                        detail: Some(format!("{total} rows")),
                    },
                ),
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check { name, passed: false, detail: Some(e.to_string()) },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }

        // Step 7: byte-identical content on EVERY node (non-uploader nodes
        // exercise remote fragment discovery). The driver's blake3 is the
        // ingress content hash of the plaintext.
        let mut verified = 0usize;
        let mut content_failure: Option<String> = None;
        'content: for node in nodes {
            for (photo_id, resources) in &photos {
                for (kind_name, expected_hash) in resources {
                    match fetch_resource_with_retry(
                        &client,
                        node,
                        photo_id,
                        kind_name,
                        Duration::from_secs(30),
                    )
                    .await
                    {
                        Ok(bytes) => {
                            let hash = blake3::hash(&bytes).to_hex().to_string();
                            if &hash != expected_hash {
                                content_failure = Some(format!(
                                    "node {} {photo_id}/{kind_name}: hash mismatch",
                                    node.node_id
                                ));
                                break 'content;
                            }
                            verified += 1;
                        }
                        Err(e) => {
                            content_failure =
                                Some(format!("node {}: {e}", node.node_id));
                            break 'content;
                        }
                    }
                }
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Cross-node content parity".to_string(),
                passed: content_failure.is_none(),
                detail: content_failure
                    .clone()
                    .or(Some(format!("{verified} resources byte-verified"))),
            },
        );
        if content_failure.is_some() {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 8: idempotency — a drained queue publishes nothing.
        check_or_bail!(
            "Drained queue republishes nothing",
            run_driver_async(data_dir.path().to_path_buf(), publish_args(&api_key))
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["published"].as_u64() == Some(0)
                            && report["already_published"].as_u64() == Some(0),
                        "unexpected republish: {report}"
                    );
                    Ok(())
                })
        );

        // Step 9: confirm-then-retry against live consensus — after wiping
        // published_at, every photo resolves via the committed probe.
        check_or_bail!(
            "Reset + republish resolves as already_published",
            async {
                run_driver_async(
                    data_dir.path().to_path_buf(),
                    vec!["reset-published".into()],
                )
                .await?;
                let report =
                    run_driver_async(data_dir.path().to_path_buf(), publish_args(&api_key))
                        .await?;
                anyhow::ensure!(
                    report["already_published"].as_u64() == Some(COUNT as u64)
                        && report["published"].as_u64() == Some(0),
                    "confirm-first not honored: {report}"
                );
                Ok(())
            }
            .await
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
