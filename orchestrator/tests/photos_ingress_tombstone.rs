use anyhow::{Context, Result};
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::photos_ingress_publish::{report_photos, run_driver_async, wait_for_ids};
use crate::tests::{
    Check, TestResult, TestScenario, get_max_view, print_and_add_check, wait_for_minimum_view,
};
use hopnet::dev_seed;

const COUNT: u32 = 4;

/// End-to-end tombstone + restore propagation: publish fabricated daemon
/// state, delete every photo locally as PhotoKit would, and verify the
/// deletes reach consensus and disappear from EVERY node's gallery — then
/// restore them and verify they come back.
///
/// This is the scenario tombstone propagation exists for. The unit suite
/// proves the pass logic against a fake publisher; only this exercises the
/// real seam — `build_photo_delete` → bincode → the node's per-scope
/// device-tx gate → `PhotoDeleteHandler` → sidecar convergence on peers.
///
/// The driver is invoked via `cargo run` against the crates/ workspace —
/// pre-build it (`cargo build --manifest-path crates/ingress-publisher/Cargo.toml
/// --features e2e-bin`) to keep first-run compile time out of the scenario.
pub struct PhotosIngressTombstone;

fn base_url(node: &NodeInfo) -> String {
    format!("https://{}:{}", node.ip_address, node.port)
}

pub(crate) async fn gallery_ids(client: &reqwest::Client, node: &NodeInfo) -> Vec<String> {
    let response = client
        .get(format!("{}/api/photos/gallery?limit=200", base_url(node)))
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await;
    let rows: Vec<serde_json::Value> = match response {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
        _ => Vec::new(),
    };
    rows.iter()
        .filter_map(|row| row["photo_id"].as_str().map(str::to_string))
        .collect()
}

/// Poll a node's gallery until none of `gone` remains. The mirror of
/// `wait_for_ids` — the sidecar converges from consensus asynchronously, so
/// absence needs the same patience presence does.
pub(crate) async fn wait_for_absent(
    client: &reqwest::Client,
    node: &NodeInfo,
    gone: &[String],
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let ids = gallery_ids(client, node).await;
        let remaining = gone.iter().filter(|id| ids.contains(id)).count();
        if remaining == 0 {
            return Ok(());
        }
        if Instant::now() > deadline {
            anyhow::bail!(
                "{remaining} of {} tombstoned photos still in gallery",
                gone.len()
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Ids on a node's recently-deleted view — the tombstone's positive proof,
/// as against the gallery's absence.
async fn recently_deleted_ids(client: &reqwest::Client, node: &NodeInfo) -> Vec<String> {
    let response = client
        .get(format!(
            "{}/api/photos/recently-deleted?limit=200",
            base_url(node)
        ))
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await;
    let rows: Vec<serde_json::Value> = match response {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
        _ => Vec::new(),
    };
    rows.iter()
        .filter_map(|row| row["photo_id"].as_str().map(str::to_string))
        .collect()
}

impl TestScenario for PhotosIngressTombstone {
    fn name(&self) -> &'static str {
        "photos-ingress-tombstone"
    }

    fn description(&self) -> &'static str {
        "Propagate iCloud deletes and restores into the mesh: publish, tombstone, verify gone from every node's gallery, then restore and verify returned"
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
        let data_dir = tempfile::tempdir().context("temp data dir")?;

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

        // Register the daemon device and claim the personal scope — the
        // propagation gate is the same holder gate publishing uses.
        let api_key = check_or_bail!(
            "Register ingress device token",
            async {
                let response = client
                    .post(format!("{}/api/devices/register", base_url(&nodes[0])))
                    .header("Authorization", format!("Bearer {}", nodes[0].jwt_token))
                    .json(&serde_json::json!({ "device_name": "Photo Tombstone E2E" }))
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
            .await
        );
        let device_id = check_or_bail!(
            "Claim ingress responsibility",
            async {
                let device_id = api_key
                    .split('.')
                    .next()
                    .context("token shape")?
                    .to_string();
                let response = client
                    .post(format!("{}/api/photos/ingress/claim", base_url(&nodes[0])))
                    .header("Authorization", format!("Bearer {}", nodes[0].jwt_token))
                    .json(&serde_json::json!({ "device_id": device_id }))
                    .send()
                    .await?;
                anyhow::ensure!(
                    response.status().is_success(),
                    "claim: {}",
                    response.status()
                );
                Ok::<_, anyhow::Error>(device_id)
            }
            .await
        );
        drop(device_id);

        let publish_args = || {
            vec![
                "publish".into(),
                "--node-url".into(),
                base_url(&nodes[0]),
                "--device-token".into(),
                api_key.clone(),
            ]
        };

        // Publish a baseline the tombstones can act on.
        let seed_report = check_or_bail!(
            format!("Fabricate {COUNT} ingress photos"),
            run_driver_async(
                data_dir.path().to_path_buf(),
                vec!["seed".into(), "--count".into(), COUNT.to_string()],
            )
            .await
        );
        let photo_ids: Vec<String> =
            check_or_bail!("Parse seed report", report_photos(&seed_report))
                .into_iter()
                .map(|(id, _)| id)
                .collect();

        check_or_bail!(
            format!("Publish {COUNT} photos"),
            run_driver_async(data_dir.path().to_path_buf(), publish_args())
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["published"].as_u64() == Some(COUNT as u64),
                        "published {} of {COUNT}: {report}",
                        report["published"]
                    );
                    Ok(())
                })
        );
        // Every gallery/recently-deleted read below goes through the per-user
        // sidecar, which each node only opens once photos are enabled for
        // that user.
        check_or_bail!(
            "Enable per-user sidecar on every node",
            async {
                for node in nodes {
                    dev_seed::enable_sidecar(&client, &base_url(node), &node.jwt_token)
                        .await
                        .with_context(|| format!("node {}", node.node_id))?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );
        check_or_bail!(
            "Photos visible on every node",
            async {
                for node in nodes {
                    wait_for_ids(&client, node, &photo_ids, Duration::from_secs(90))
                        .await
                        .with_context(|| format!("node {}", node.node_id))?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );

        // The delete half.
        check_or_bail!(
            "Tombstone every photo locally",
            run_driver_async(data_dir.path().to_path_buf(), vec!["tombstone".into()])
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["tombstoned"].as_u64() == Some(COUNT as u64),
                        "tombstoned {}: {report}",
                        report["tombstoned"]
                    );
                    Ok(())
                })
        );
        check_or_bail!(
            format!("Propagate {COUNT} tombstones to the mesh"),
            run_driver_async(data_dir.path().to_path_buf(), publish_args())
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["tombstones_propagated"].as_u64() == Some(COUNT as u64),
                        "propagated {} of {COUNT}: {report}",
                        report["tombstones_propagated"]
                    );
                    anyhow::ensure!(
                        report["published"].as_u64() == Some(0),
                        "nothing should re-publish: {report}"
                    );
                    Ok(())
                })
        );
        check_or_bail!(
            "Deletes reached consensus",
            async {
                let advanced =
                    wait_for_minimum_view(nodes, base_view + 1, Duration::from_secs(60)).await?;
                anyhow::ensure!(advanced, "consensus did not advance past the deletes");
                Ok::<_, anyhow::Error>(())
            }
            .await
        );
        check_or_bail!(
            "Tombstoned photos gone from EVERY node's gallery",
            async {
                for node in nodes {
                    wait_for_absent(&client, node, &photo_ids, Duration::from_secs(90))
                        .await
                        .with_context(|| format!("node {}", node.node_id))?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );
        check_or_bail!(
            "Tombstones present in recently-deleted on every node",
            async {
                for node in nodes {
                    let deleted = recently_deleted_ids(&client, node).await;
                    let missing = photo_ids.iter().filter(|id| !deleted.contains(id)).count();
                    anyhow::ensure!(
                        missing == 0,
                        "node {}: {missing} of {COUNT} absent from recently-deleted",
                        node.node_id
                    );
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );

        // Idempotency: a converged tombstone must never re-submit.
        check_or_bail!(
            "Converged tombstones do not re-propagate",
            run_driver_async(data_dir.path().to_path_buf(), publish_args())
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["tombstones_propagated"].as_u64() == Some(0),
                        "re-propagated a converged tombstone: {report}"
                    );
                    Ok(())
                })
        );

        // The restore half — the direction the resettable marker exists for.
        check_or_bail!(
            "Restore every photo locally",
            run_driver_async(
                data_dir.path().to_path_buf(),
                vec!["tombstone".into(), "--restore".into()],
            )
            .await
            .and_then(|report| {
                anyhow::ensure!(
                    report["restored"].as_u64() == Some(COUNT as u64),
                    "restored {}: {report}",
                    report["restored"]
                );
                Ok(())
            })
        );
        check_or_bail!(
            format!("Propagate {COUNT} restores to the mesh"),
            run_driver_async(data_dir.path().to_path_buf(), publish_args())
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["restores_propagated"].as_u64() == Some(COUNT as u64),
                        "restored {} of {COUNT}: {report}",
                        report["restores_propagated"]
                    );
                    Ok(())
                })
        );
        check_or_bail!(
            "Restored photos back in EVERY node's gallery",
            async {
                for node in nodes {
                    wait_for_ids(&client, node, &photo_ids, Duration::from_secs(90))
                        .await
                        .with_context(|| format!("node {}", node.node_id))?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
