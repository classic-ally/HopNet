use anyhow::{Context, Result};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::photos_ingress_publish::{
    ReportPhoto, fetch_resource_with_retry, report_photos, run_driver_async, wait_for_ids,
};
use crate::tests::{Check, TestResult, TestScenario, get_max_view, print_and_add_check};
use hopnet::dev_seed;

const COUNT: u32 = 3;

/// End-to-end edit propagation: publish, edit every photo as PhotoKit would
/// after an adjustment, and verify the NEW bytes reach every node — then
/// revert and verify the edited render disappears, then refresh metadata
/// alone and verify that lands too.
///
/// This is the scenario edit propagation exists for. The unit suite proves
/// the pass logic against a fake publisher; only this exercises the real
/// seam — `publish_photo_edit` → upload → `photo_edit_content` → the node's
/// per-scope device-tx gate → sidecar convergence on peers → decrypted
/// bytes served back.
///
/// The driver is invoked via `cargo run` against the crates/ workspace —
/// pre-build it (`cargo build --manifest-path crates/ingress-publisher/Cargo.toml
/// --features e2e-bin`) to keep first-run compile time out of the scenario.
pub struct PhotosIngressEdit;

fn base_url(node: &NodeInfo) -> String {
    format!("https://{}:{}", node.ip_address, node.port)
}

/// The (photo_id, kind) → blake3 map a driver report describes.
fn hashes_by_kind(photos: &[ReportPhoto]) -> HashMap<(String, String), String> {
    photos
        .iter()
        .flat_map(|(id, resources)| {
            resources
                .iter()
                .map(move |(kind, hash)| ((id.clone(), kind.clone()), hash.clone()))
        })
        .collect()
}

/// Poll until a node serves EXACTLY these bytes for a resource. The edit
/// converges through consensus into each node's sidecar asynchronously, so
/// the first read may still return the pre-edit render — that is
/// convergence in progress, not a failure.
async fn wait_for_resource_hash(
    client: &reqwest::Client,
    node: &NodeInfo,
    photo_id: &str,
    kind_name: &str,
    expected: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    // Deferred init: the loop always writes before the bail reads.
    let mut last;
    loop {
        match fetch_resource_with_retry(client, node, photo_id, kind_name, Duration::from_secs(10))
            .await
        {
            Ok(bytes) => {
                let hash = blake3::hash(&bytes).to_hex().to_string();
                if hash == expected {
                    return Ok(());
                }
                last = format!("served {hash}, expected {expected}");
            }
            Err(e) => last = e.to_string(),
        }
        if Instant::now() > deadline {
            anyhow::bail!("{kind_name} of {photo_id} on node {}: {last}", node.node_id);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Poll until a node stops serving a resource — the positive proof that a
/// revert's removal landed, as against the edited render merely changing.
async fn wait_for_resource_absent(
    client: &reqwest::Client,
    node: &NodeInfo,
    photo_id: &str,
    kind_name: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let response = client
            .get(format!(
                "{}/api/photos/{photo_id}/resource/{kind_name}",
                base_url(node)
            ))
            .header("Authorization", format!("Bearer {}", node.jwt_token))
            .send()
            .await;
        match response {
            Ok(r) if !r.status().is_success() => return Ok(()),
            _ => {}
        }
        if Instant::now() > deadline {
            anyhow::bail!(
                "node {} still serves {kind_name} of {photo_id}",
                node.node_id
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Every photo's metadata still decrypts, on every node.
///
/// This is the whole `metadata_access` amendment observed from outside. The
/// gallery serves DECRYPTED rows and excludes any whose metadata the sidecar
/// could not open (`undecryptable = 0` in its query), so a photo that
/// vanishes here after an edit is one whose new ciphertext arrived without
/// wraps that open it — the silent, permanent failure the field exists to
/// prevent. A stored wrap still unwrapping to the OLD key looks exactly like
/// this, and like nothing else.
async fn assert_all_decryptable(
    client: &reqwest::Client,
    nodes: &[NodeInfo],
    photo_ids: &[String],
) -> Result<()> {
    for node in nodes {
        wait_for_ids(client, node, photo_ids, Duration::from_secs(90))
            .await
            .with_context(|| {
                format!(
                    "node {}: a photo left the gallery — its metadata no longer decrypts",
                    node.node_id
                )
            })?;
    }
    Ok(())
}

impl TestScenario for PhotosIngressEdit {
    fn name(&self) -> &'static str {
        "photos-ingress-edit"
    }

    fn description(&self) -> &'static str {
        "Propagate iCloud edits into the mesh: publish, edit, verify new bytes everywhere, revert, verify the edited render is gone, then refresh metadata alone"
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

        let _base_view = check_or_bail!("Get initial consensus view", get_max_view(nodes).await);

        // Register the daemon device and claim the personal scope — edits
        // pass through the same holder gate publishing does.
        let api_key = check_or_bail!(
            "Register ingress device token",
            async {
                let response = client
                    .post(format!("{}/api/devices/register", base_url(&nodes[0])))
                    .header("Authorization", format!("Bearer {}", nodes[0].jwt_token))
                    .json(&serde_json::json!({ "device_name": "Photo Edit E2E" }))
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
        check_or_bail!(
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
                Ok::<_, anyhow::Error>(())
            }
            .await
        );

        let publish_args = || {
            vec![
                "publish".into(),
                "--node-url".into(),
                base_url(&nodes[0]),
                "--device-token".into(),
                api_key.clone(),
            ]
        };

        // --- Step 1: a published baseline the edits act on. ---
        let seed_report = check_or_bail!(
            format!("Fabricate {COUNT} ingress photos"),
            run_driver_async(
                data_dir.path().to_path_buf(),
                vec!["seed".into(), "--count".into(), COUNT.to_string()],
            )
            .await
        );
        let seeded = check_or_bail!("Parse seed report", report_photos(&seed_report));
        let photo_ids: Vec<String> = seeded.iter().map(|(id, _)| id.clone()).collect();

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

        // --- Step 2: the edit. ---
        let edit_report = check_or_bail!(
            "Edit every photo locally",
            run_driver_async(data_dir.path().to_path_buf(), vec!["edit".into()])
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["edited"].as_u64() == Some(COUNT as u64),
                        "edited {}: {report}",
                        report["edited"]
                    );
                    Ok(report)
                })
        );
        let edited = check_or_bail!("Parse edit report", report_photos(&edit_report));
        let edited_hashes = hashes_by_kind(&edited);
        check_or_bail!(
            "Local edit produced an edited render",
            async {
                for (id, _) in &seeded {
                    anyhow::ensure!(
                        edited_hashes.contains_key(&(id.clone(), "edited".to_string())),
                        "photo {id} has no edited resource after the edit"
                    );
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );

        check_or_bail!(
            format!("Propagate {COUNT} edits to the mesh"),
            run_driver_async(data_dir.path().to_path_buf(), publish_args())
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["edits_propagated"].as_u64() == Some(COUNT as u64),
                        "propagated {} of {COUNT}: {report}",
                        report["edits_propagated"]
                    );
                    anyhow::ensure!(
                        report["published"].as_u64() == Some(0),
                        "an edit must not re-publish: {report}"
                    );
                    Ok(())
                })
        );

        // The load-bearing assertion: every node serves the POST-edit bytes.
        check_or_bail!(
            "Edited render byte-verified on EVERY node",
            async {
                for node in nodes {
                    for (id, _) in &seeded {
                        let expected = edited_hashes
                            .get(&(id.clone(), "edited".to_string()))
                            .context("edited hash")?;
                        wait_for_resource_hash(
                            &client,
                            node,
                            id,
                            "edited",
                            expected,
                            Duration::from_secs(90),
                        )
                        .await
                        .with_context(|| format!("node {}", node.node_id))?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );
        // An edit-set change refreshes the thumbnails too, so their bytes
        // must travel with it — a partial edit would leave the gallery
        // showing the pre-edit crop at grid size.
        check_or_bail!(
            "Refreshed thumbnails byte-verified on EVERY node",
            async {
                for node in nodes {
                    for (id, _) in &seeded {
                        for kind in ["thumbnail_small", "thumbnail_medium"] {
                            let expected = edited_hashes
                                .get(&(id.clone(), kind.to_string()))
                                .context("thumbnail hash")?;
                            wait_for_resource_hash(
                                &client,
                                node,
                                id,
                                kind,
                                expected,
                                Duration::from_secs(90),
                            )
                            .await
                            .with_context(|| format!("node {}", node.node_id))?;
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );
        check_or_bail!(
            "Original untouched by the edit",
            async {
                let baseline = hashes_by_kind(&seeded);
                for node in nodes {
                    for (id, _) in &seeded {
                        let expected = baseline
                            .get(&(id.clone(), "original".to_string()))
                            .context("original hash")?;
                        let bytes = fetch_resource_with_retry(
                            &client,
                            node,
                            id,
                            "original",
                            Duration::from_secs(30),
                        )
                        .await?;
                        anyhow::ensure!(
                            blake3::hash(&bytes).to_hex().to_string() == *expected,
                            "node {} served a changed original for {id}",
                            node.node_id
                        );
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );

        // The edit carried metadata inline (its modification date advanced
        // with the pixels), so a fresh key and fresh wraps travelled with
        // it. Every photo must still decrypt.
        check_or_bail!(
            "Metadata still decrypts after the edit, on EVERY node",
            assert_all_decryptable(&client, nodes, &photo_ids).await
        );

        // Idempotency: a converged edit must never re-submit.
        check_or_bail!(
            "Converged edits do not re-propagate",
            run_driver_async(data_dir.path().to_path_buf(), publish_args())
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["edits_propagated"].as_u64() == Some(0)
                            && report["metadata_propagated"].as_u64() == Some(0),
                        "re-propagated a converged edit: {report}"
                    );
                    Ok(())
                })
        );

        // --- Step 3: the revert — the direction the removal list exists for. ---
        let revert_report = check_or_bail!(
            "Revert every photo locally",
            run_driver_async(
                data_dir.path().to_path_buf(),
                vec!["edit".into(), "--revert".into()],
            )
            .await
            .and_then(|report| {
                anyhow::ensure!(
                    report["reverted"].as_u64() == Some(COUNT as u64),
                    "reverted {}: {report}",
                    report["reverted"]
                );
                Ok(report)
            })
        );
        let reverted = check_or_bail!("Parse revert report", report_photos(&revert_report));
        let reverted_hashes = hashes_by_kind(&reverted);

        check_or_bail!(
            format!("Propagate {COUNT} reverts to the mesh"),
            run_driver_async(data_dir.path().to_path_buf(), publish_args())
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["edits_propagated"].as_u64() == Some(COUNT as u64),
                        "propagated {} of {COUNT}: {report}",
                        report["edits_propagated"]
                    );
                    Ok(())
                })
        );
        check_or_bail!(
            "Edited render GONE from EVERY node",
            async {
                for node in nodes {
                    for (id, _) in &seeded {
                        wait_for_resource_absent(
                            &client,
                            node,
                            id,
                            "edited",
                            Duration::from_secs(90),
                        )
                        .await
                        .with_context(|| format!("node {}", node.node_id))?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );
        check_or_bail!(
            "Revert-refreshed thumbnails byte-verified on EVERY node",
            async {
                for node in nodes {
                    for (id, _) in &seeded {
                        let expected = reverted_hashes
                            .get(&(id.clone(), "thumbnail_small".to_string()))
                            .context("thumbnail hash")?;
                        wait_for_resource_hash(
                            &client,
                            node,
                            id,
                            "thumbnail_small",
                            expected,
                            Duration::from_secs(90),
                        )
                        .await
                        .with_context(|| format!("node {}", node.node_id))?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
        );

        // --- Step 4: metadata alone — no bytes move at all. ---
        check_or_bail!(
            "Refresh metadata locally",
            run_driver_async(
                data_dir.path().to_path_buf(),
                vec!["edit".into(), "--metadata-only".into()],
            )
            .await
            .and_then(|report| {
                anyhow::ensure!(
                    report["metadata_refreshed"].as_u64() == Some(COUNT as u64),
                    "refreshed {}: {report}",
                    report["metadata_refreshed"]
                );
                Ok(())
            })
        );
        check_or_bail!(
            format!("Propagate {COUNT} metadata refreshes to the mesh"),
            run_driver_async(data_dir.path().to_path_buf(), publish_args())
                .await
                .and_then(|report| {
                    anyhow::ensure!(
                        report["metadata_propagated"].as_u64() == Some(COUNT as u64),
                        "propagated {} of {COUNT}: {report}",
                        report["metadata_propagated"]
                    );
                    anyhow::ensure!(
                        report["edits_propagated"].as_u64() == Some(0),
                        "a metadata refresh must carry no resources: {report}"
                    );
                    Ok(())
                })
        );
        check_or_bail!(
            "Metadata still decrypts after the refresh, on EVERY node",
            assert_all_decryptable(&client, nodes, &photo_ids).await
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
