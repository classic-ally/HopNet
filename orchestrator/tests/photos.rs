use anyhow::{Context, Result};
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::{
    Check, TestResult, TestScenario, get_max_view, print_and_add_check, wait_for_minimum_view,
};
use hopnet::dev_seed;

const SEED: u64 = 42;
const COUNT: u32 = 12;
const MONTHS: u32 = 6;

/// End-to-end photos pipeline across the mesh: seed via the manual ingest
/// route on node 0, then verify sidecar sync, gallery/page/histogram parity,
/// and byte-identical content (incl. remote fragment discovery) on every
/// node. The divergence check in auto-managed mode covers the photo tables'
/// state hashes afterwards.
pub struct PhotosUploadConsistency;

fn base_url(node: &NodeInfo) -> String {
    format!("https://{}:{}", node.ip_address, node.port)
}

/// Field-selective gallery row identity — robust against node-local fields.
fn gallery_row_identity(row: &serde_json::Value) -> (String, String, i64, Vec<(i64, String)>) {
    let mut resources: Vec<(i64, String)> = row["resources"]
        .as_array()
        .map(|pairs| {
            pairs
                .iter()
                .filter_map(|pair| {
                    Some((pair.get(0)?.as_i64()?, pair.get(1)?.as_str()?.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();
    resources.sort();
    (
        row["photo_id"].as_str().unwrap_or_default().to_string(),
        row["date_taken"].as_str().unwrap_or_default().to_string(),
        row["media_type"].as_i64().unwrap_or(-1),
        resources,
    )
}

async fn fetch_gallery(
    client: &reqwest::Client,
    node: &NodeInfo,
) -> Result<Vec<serde_json::Value>> {
    let response = client
        .get(format!("{}/api/photos/gallery?limit=200", base_url(node)))
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "gallery: {}",
        response.status()
    );
    Ok(response.json().await?)
}

/// Poll until the node's sidecar has drained `expected` photos.
async fn wait_for_gallery(
    client: &reqwest::Client,
    node: &NodeInfo,
    expected: usize,
    timeout: Duration,
) -> Result<Vec<serde_json::Value>> {
    let deadline = Instant::now() + timeout;
    loop {
        match fetch_gallery(client, node).await {
            Ok(rows) if rows.len() >= expected => return Ok(rows),
            Ok(rows) if Instant::now() > deadline => {
                anyhow::bail!(
                    "gallery has {} of {} photos after timeout",
                    rows.len(),
                    expected
                )
            }
            Err(e) if Instant::now() > deadline => return Err(e),
            _ => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

/// Ordered photo_id walk of the keyset page endpoint.
async fn walk_pages(client: &reqwest::Client, node: &NodeInfo) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut url = format!("{}/api/photos/page?limit=5", base_url(node));
        if let Some(c) = &cursor {
            url.push_str(&format!("&cursor={c}"));
        }
        let response = client
            .get(url)
            .header("Authorization", format!("Bearer {}", node.jwt_token))
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "page: {}",
            response.status()
        );
        let page: serde_json::Value = response.json().await?;
        let items = page["items"].as_array().context("page items")?;
        for item in items {
            ids.push(item["photo_id"].as_str().context("photo_id")?.to_string());
        }
        match page.get("next_cursor").and_then(|c| c.as_str()) {
            Some(next) if !items.is_empty() => cursor = Some(next.to_string()),
            _ => return Ok(ids),
        }
    }
}

/// Content fetch with backoff: first fetches on non-uploader nodes trigger
/// remote fragment discovery over iroh, which can take a few seconds.
async fn fetch_resource_with_retry(
    client: &reqwest::Client,
    node: &NodeInfo,
    photo_id: &str,
    kind_name: &str,
    timeout: Duration,
) -> Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    // Deferred init: every path that reaches the `bail!` below has assigned it.
    let mut last_error;
    loop {
        let result = client
            .get(format!(
                "{}/api/photos/{}/resource/{}",
                base_url(node),
                photo_id,
                kind_name
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

impl TestScenario for PhotosUploadConsistency {
    fn name(&self) -> &'static str {
        "photos-upload-consistency"
    }

    fn description(&self) -> &'static str {
        "Seed photos through the manual ingest route on node 0, then verify sidecar sync, gallery/page/histogram parity, and byte-identical content on every node"
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

        println!("\nRunning checks:");

        // Step 1: consensus baseline.
        let base_view = match get_max_view(nodes).await {
            Ok(view) => view,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Get initial consensus view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Get initial consensus view".to_string(),
                passed: true,
                detail: Some(format!("view {base_view}")),
            },
        );

        // Step 2: seed on node 0 through the manual ingest route.
        let posted = match dev_seed::seed_photos(
            &client,
            &base_url(&nodes[0]),
            &nodes[0].jwt_token,
            SEED,
            COUNT,
            MONTHS,
        )
        .await
        {
            Ok(posted) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Seed {COUNT} photos on node 0"),
                        passed: posted.len() == COUNT as usize,
                        detail: Some(format!("{} photos ingested", posted.len())),
                    },
                );
                posted
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Seed {COUNT} photos on node 0"),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 3: every photo_add is a consensus transaction — all nodes
        // must reach at least one height past the baseline (usually many).
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

        // Step 4: enable the sidecar everywhere; enabling triggers an
        // immediate hydration drain, so the 30s sync interval is irrelevant.
        let mut galleries: Vec<Vec<serde_json::Value>> = Vec::with_capacity(nodes.len());
        for node in nodes {
            let outcome = async {
                dev_seed::enable_sidecar(&client, &base_url(node), &node.jwt_token).await?;
                wait_for_gallery(&client, node, COUNT as usize, Duration::from_secs(45)).await
            }
            .await;
            match outcome {
                Ok(rows) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} sidecar drains {COUNT} photos", node.node_id),
                            passed: rows.len() == COUNT as usize,
                            detail: Some(format!("{} rows", rows.len())),
                        },
                    );
                    galleries.push(rows);
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} sidecar drains {COUNT} photos", node.node_id),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }

        // Step 5: gallery parity (field-selective, sorted).
        let identities: Vec<Vec<_>> = galleries
            .iter()
            .map(|rows| {
                let mut ids: Vec<_> = rows.iter().map(gallery_row_identity).collect();
                ids.sort();
                ids
            })
            .collect();
        let gallery_parity = identities.iter().all(|ids| *ids == identities[0]);
        print_and_add_check(
            &mut result,
            Check {
                name: "Gallery parity across nodes".to_string(),
                passed: gallery_parity,
                detail: (!gallery_parity).then(|| "field-selective rows differ".to_string()),
            },
        );

        // Step 6: keyset page walk parity (ordered).
        let mut walks: Vec<Vec<String>> = Vec::with_capacity(nodes.len());
        for node in nodes {
            match walk_pages(&client, node).await {
                Ok(ids) => walks.push(ids),
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Keyset page walk parity".to_string(),
                            passed: false,
                            detail: Some(format!("node {}: {e}", node.node_id)),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }
        let walk_parity = walks.iter().all(|w| *w == walks[0]) && walks[0].len() == COUNT as usize;
        print_and_add_check(
            &mut result,
            Check {
                name: "Keyset page walk parity".to_string(),
                passed: walk_parity,
                detail: Some(format!("{} photos walked", walks[0].len())),
            },
        );

        // Step 7: histogram parity + shape.
        let mut histograms: Vec<serde_json::Value> = Vec::with_capacity(nodes.len());
        for node in nodes {
            let response = client
                .get(format!("{}/api/photos/histogram", base_url(node)))
                .header("Authorization", format!("Bearer {}", node.jwt_token))
                .send()
                .await;
            match response {
                Ok(r) if r.status().is_success() => {
                    histograms.push(r.json().await.unwrap_or(serde_json::Value::Null))
                }
                other => {
                    let detail = match other {
                        Ok(r) => format!("node {}: {}", node.node_id, r.status()),
                        Err(e) => format!("node {}: {e}", node.node_id),
                    };
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Month histogram parity".to_string(),
                            passed: false,
                            detail: Some(detail),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }
        let histogram_parity = histograms.iter().all(|h| *h == histograms[0]);
        let bucket_shape = histograms[0]
            .as_array()
            .map(|buckets| {
                let total: i64 = buckets.iter().filter_map(|b| b["count"].as_i64()).sum();
                buckets.len() == MONTHS as usize && total == COUNT as i64
            })
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Month histogram parity".to_string(),
                passed: histogram_parity && bucket_shape,
                detail: Some(format!(
                    "{} buckets, parity {histogram_parity}",
                    histograms[0].as_array().map(|b| b.len()).unwrap_or(0)
                )),
            },
        );

        // Step 8: content parity — regenerate deterministically and
        // byte-compare on every node (remote nodes exercise fragment
        // discovery over iroh).
        let mut content_failures: Vec<String> = Vec::new();
        for (index, posted_photo) in posted.iter().enumerate() {
            let expected = dev_seed::generate_photo(SEED, index as u32, MONTHS);
            for node in nodes {
                for (kind, expected_bytes) in [
                    ("original", &expected.resources[0].1),
                    ("thumbnail_small", &expected.resources[2].1),
                ] {
                    match fetch_resource_with_retry(
                        &client,
                        node,
                        &posted_photo.photo_id,
                        kind,
                        Duration::from_secs(30),
                    )
                    .await
                    {
                        Ok(bytes) if &bytes == expected_bytes => {}
                        Ok(bytes) => content_failures.push(format!(
                            "node {} photo {} {}: {} bytes != {} expected",
                            node.node_id,
                            index,
                            kind,
                            bytes.len(),
                            expected_bytes.len()
                        )),
                        Err(e) => content_failures
                            .push(format!("node {} photo {index} {kind}: {e}", node.node_id)),
                    }
                }
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Cross-node content parity".to_string(),
                passed: content_failures.is_empty(),
                detail: if content_failures.is_empty() {
                    Some(format!(
                        "{} fetches byte-identical",
                        COUNT as usize * nodes.len() * 2
                    ))
                } else {
                    Some(content_failures.join("; "))
                },
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
