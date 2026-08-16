use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{
    download_file_from_all_nodes_with_timeout, get_fragment_distribution,
    trigger_fragment_inventory_sync_all, upload_file, verify_all_identical,
    wait_for_fragment_distribution,
};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

/// Watermark eviction test (RFC-STORAGE-002 S5).
///
/// After distribution the ORIGIN node still holds every sent-away
/// fragment on disk — exactly the surplus the eviction loop exists to
/// reclaim. Forcing watermarks to (0,0) must evict that surplus while the
/// guard protects responsible copies and sole-holder copies; the file
/// must stay downloadable from every node afterwards, and a second pass
/// must find nothing left to evict.
pub struct EvictionUnderPressure;

#[derive(Debug, Deserialize)]
struct EvictionSummary {
    evicted: usize,
    bytes_freed: u64,
}

async fn trigger_eviction(node: &NodeInfo) -> Result<EvictionSummary> {
    let client = crate::insecure_client();
    let url = format!(
        "https://{}:{}/api/maintenance/watermark-eviction?high_pct=0&low_pct=0&grace_secs=0",
        node.ip_address, node.port
    );
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(60))
        .send()
        .await?;
    anyhow::ensure!(resp.status().is_success(), "HTTP {}", resp.status());
    Ok(resp.json().await?)
}

impl TestScenario for EvictionUnderPressure {
    fn name(&self) -> &'static str {
        "eviction-under-pressure"
    }

    fn description(&self) -> &'static str {
        "Watermark eviction reclaims surplus copies, never responsible or sole-holder copies"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let filename = format!("test-evict-{}.txt", timestamp);
        let contents = format!("HopNet eviction test {}", timestamp).into_bytes();
        let full_path = format!("/{}", filename);

        println!("\nRunning eviction-under-pressure checks:");

        // 1. Upload on node 0 (the origin — it will hold the surplus).
        let upload_ok = upload_file(&nodes[0], "/", &filename, contents.clone())
            .await
            .is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Upload {} to node 0", filename),
                passed: upload_ok,
                detail: None,
            },
        );
        if !upload_ok {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 2. Wait for distribution, then settle inventory attestations —
        // the eviction guard needs OTHER holders visible in inventory.
        let dist_ok =
            wait_for_fragment_distribution(&nodes[0], &full_path, Duration::from_secs(30))
                .await
                .is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "Fragment distribution completed".to_string(),
                passed: dist_ok,
                detail: None,
            },
        );
        if !dist_ok {
            result.duration = start.elapsed();
            return Ok(result);
        }
        let _ = trigger_fragment_inventory_sync_all(nodes).await;
        let settle_start = Instant::now();
        let mut settled = false;
        while settle_start.elapsed() < Duration::from_secs(60) {
            if let Ok(dist) = get_fragment_distribution(&nodes[0], &full_path).await
                && !dist.fragments.is_empty()
                && dist
                    .fragments
                    .iter()
                    .all(|f| !f.nodes_with_fragment.is_empty())
            {
                settled = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Fragment inventory settled".to_string(),
                passed: settled,
                detail: None,
            },
        );
        if !settled {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 3. Force eviction on the origin: watermarks (0,0), no grace.
        let summary = match trigger_eviction(&nodes[0]).await {
            Ok(s) => s,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Trigger watermark eviction on node 0".to_string(),
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
                name: "Surplus evicted on origin".to_string(),
                passed: summary.evicted > 0,
                detail: Some(format!(
                    "evicted={} bytes_freed={}",
                    summary.evicted, summary.bytes_freed
                )),
            },
        );

        // 4. Responsible copies survive: every class still has ≥1 holder.
        let _ = trigger_fragment_inventory_sync_all(nodes).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let coverage_ok = match get_fragment_distribution(&nodes[0], &full_path).await {
            Ok(dist) => {
                !dist.fragments.is_empty()
                    && dist
                        .fragments
                        .iter()
                        .all(|f| !f.nodes_with_fragment.is_empty())
            }
            Err(_) => false,
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Every fragment class still has a holder".to_string(),
                passed: coverage_ok,
                detail: None,
            },
        );

        // 5. File downloadable from EVERY node post-eviction.
        let downloads_ok = match download_file_from_all_nodes_with_timeout(
            nodes,
            &full_path,
            Duration::from_secs(15),
        )
        .await
        {
            Ok(data) => verify_all_identical(&data).is_ok() && data[0] == contents,
            Err(_) => false,
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "File downloadable from all nodes after eviction".to_string(),
                passed: downloads_ok,
                detail: None,
            },
        );

        // 6. Second pass finds nothing: responsible copies are never
        // evictable, whatever the pressure claims.
        let second = trigger_eviction(&nodes[0]).await;
        let idempotent = matches!(&second, Ok(s) if s.evicted == 0);
        print_and_add_check(
            &mut result,
            Check {
                name: "Second pass evicts nothing (responsible protected)".to_string(),
                passed: idempotent,
                detail: second.map(|s| format!("evicted={}", s.evicted)).ok(),
            },
        );

        result.details = format!(
            "Evicted {} surplus fragments on origin; data intact on all {} nodes",
            summary.evicted,
            nodes.len()
        );
        result.duration = start.elapsed();
        Ok(result)
    }
}
