use anyhow::{Context, Result};
use bollard::Docker;
use std::time::{Duration, Instant};
use tokio_stream::StreamExt;

use crate::tests::files::upload_file;
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};

/// The mixed-version mesh (RFC-025 §Validation, orchestrator gates): one
/// node claims a different CalVer over the version seam, so its locked
/// family diverges while its compat generation stays in-window. The gate
/// proves the whole diagnosability story end to end: locked scopes
/// refused (the defuser SCREAM in the logs), compat scopes answering
/// (pongs keep refreshing), the two evidence clocks splitting
/// (visible-not-live), and VersionSkew named on the status views — in
/// BOTH directions.
///
/// Runs on the default consensus policy deliberately: t_out >= 65s keeps
/// vote-out clear of the skew window, and the AUTO profile (majority at
/// v=3) keeps the mesh deciding while the skewed node is locked out. The
/// skew window is kept tight regardless — the assertions all complete
/// well inside the vote-out deadline, then the node is restored.
pub struct MixedVersionMesh;

/// The version the skewed node claims — any valid CalVer that differs
/// from the build's.
const SKEW_VERSION: &str = "2026.9.0";

async fn get_json(node: &NodeInfo, path: &str) -> Result<serde_json::Value> {
    let resp = crate::call_node_api(node, path, true).await?;
    anyhow::ensure!(resp.status().is_success(), "{path} {}", resp.status());
    Ok(resp.json().await?)
}

/// Full stdout+stderr of a node's container — the first log-assertion
/// helper (the defuser scream is a log contract, RFC-025 S4).
pub(crate) async fn container_logs(docker: &Docker, mesh_id: u32, node_id: u32) -> Result<String> {
    let id = super::regenesis::find_container_id(docker, mesh_id, node_id).await?;
    let opts = bollard::query_parameters::LogsOptionsBuilder::new()
        .stdout(true)
        .stderr(true)
        .tail("5000")
        .build();
    let mut stream = docker.logs(&id, Some(opts));
    let mut out = String::new();
    while let Some(chunk) = stream.next().await {
        if let Ok(chunk) = chunk {
            out.push_str(&String::from_utf8_lossy(&chunk.into_bytes()));
        }
    }
    Ok(out)
}

/// The evidence row for `node_id` as seen by `observer`, or Null.
async fn evidence_row(observer: &NodeInfo, node_id: i64) -> Result<serde_json::Value> {
    let doc = get_json(observer, "/api/consensus/evidence").await?;
    Ok(doc["nodes"]
        .as_array()
        .and_then(|rows| rows.iter().find(|r| r["node_id"] == node_id).cloned())
        .unwrap_or(serde_json::Value::Null))
}

const SCREAM: &str = "version skew: locked dial refused at the transport";

impl TestScenario for MixedVersionMesh {
    fn name(&self) -> &'static str {
        "mixed-version-mesh"
    }

    fn description(&self) -> &'static str {
        "Version-skewed node: locked refused, compat answering, VersionSkew named both ways"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "mixed-version-mesh expects a 3-node mesh");
        let docker = crate::sys::connect()?;

        println!("\nRunning mixed-version-mesh checks:");

        // 1. Baseline: nobody skewed, and the build's own version on the
        // summary.
        let base = get_json(&nodes[0], "/api/consensus/evidence").await?;
        let base_clean = base["nodes"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .all(|r| r["pong"].is_null() || r["pong"]["skew"] == false)
            })
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline: no skew anywhere".to_string(),
                passed: base_clean && base["summary"]["local_version"] == env!("CARGO_PKG_VERSION"),
                detail: None,
            },
        );

        // 2. The seam: node 2 comes back claiming SKEW_VERSION. Same
        // image, same volume — only the claimed identity moves, so its
        // locked ALPN diverges while compat/1 stays in-window.
        super::regenesis::recreate_node_with_env(
            &docker,
            mesh_id,
            2,
            None,
            &[("HOPNET_UPGRADE_VERSION_OVERRIDE", SKEW_VERSION)],
        )
        .await
        .context("recreate node 2 with the version override")?;
        let node2 = super::regenesis::reauth_node(&docker, mesh_id, &nodes[2])
            .await
            .context("reauth node 2")?;

        // 3. The mesh still decides — an upload through node 0 completes
        // on the majority pair, and its consensus traffic toward node 2
        // is exactly the locked dial the defuser classifies.
        let upload_ok = upload_file(
            &nodes[0],
            "/",
            "skew-window-probe.txt",
            b"decided without the skewed seat".to_vec(),
        )
        .await
        .is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "Mesh decides while the skewed node is locked out".to_string(),
                passed: upload_ok,
                detail: None,
            },
        );

        // 4. The observer's view of the skewed peer: skew named with the
        // claimed version, the window NOT moved by a version override,
        // not stranded — and the two clocks split: sightings fresh
        // (compat pongs), contact starving (locked refused). Sampled
        // twice so the pong is provably REFRESHING, not a relic.
        let mut named = false;
        let mut clocks_split = false;
        let mut window_pinned = false;
        let mut pong_refreshing = false;
        let deadline = Instant::now() + Duration::from_secs(25);
        while Instant::now() < deadline {
            let row = evidence_row(&nodes[0], 2).await?;
            let pong = &row["pong"];
            if pong["skew"] == true {
                named = pong["version"] == SKEW_VERSION && pong["stranded"] == false;
                window_pinned = pong["floor"] == 0 && pong["head"] == 1;
                let seen = row["seen_age_ms"].as_u64().unwrap_or(u64::MAX);
                let contact = row["age_ms"].as_u64().unwrap_or(0);
                clocks_split = seen < 10_000 && contact > seen;
                let first_age = pong["age_ms"].as_u64().unwrap_or(u64::MAX);
                tokio::time::sleep(Duration::from_secs(6)).await;
                let again = evidence_row(&nodes[0], 2).await?;
                let second_age = again["pong"]["age_ms"].as_u64().unwrap_or(u64::MAX);
                pong_refreshing = first_age < 8_000 && second_age < 8_000;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Skew named: claimed version, window unmoved, not stranded".to_string(),
                passed: named && window_pinned,
                detail: None,
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Visible-not-live: sightings fresh, contact starving, pong refreshing"
                    .to_string(),
                passed: clocks_split && pong_refreshing,
                detail: None,
            },
        );

        // 5. The banner and the scream on the healthy side.
        let view = get_json(&nodes[0], "/api/views/network-resilience").await?;
        let banner = view["consensus"]["version_skew"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .any(|r| r["node_id"] == 2 && r["version"] == SKEW_VERSION)
            })
            .unwrap_or(false)
            && view["consensus"]["stranded_peers"] == serde_json::json!([]);
        let mut healthy_scream = false;
        let scream_deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < scream_deadline {
            if container_logs(&docker, mesh_id, 0).await?.contains(SCREAM)
                || container_logs(&docker, mesh_id, 1).await?.contains(SCREAM)
            {
                healthy_scream = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Healthy side: VersionSkew banner + defuser scream".to_string(),
                passed: banner && healthy_scream,
                detail: None,
            },
        );

        // 6. BOTH directions: the skewed node names the mesh as skewed
        // from its side — its local_version is the claim, every peer's
        // pong reads skewed, its banner fills, its own defuser screams.
        let ev2 = get_json(&node2, "/api/consensus/evidence").await?;
        let peers_skewed = ev2["nodes"]
            .as_array()
            .map(|rows| {
                let judged: Vec<_> = rows
                    .iter()
                    .filter(|r| r["self"] == false && r["pong"].is_object())
                    .collect();
                !judged.is_empty() && judged.iter().all(|r| r["pong"]["skew"] == true)
            })
            .unwrap_or(false);
        let view2 = get_json(&node2, "/api/views/network-resilience").await?;
        let banner2 = view2["consensus"]["version_skew"]
            .as_array()
            .map(|rows| !rows.is_empty())
            .unwrap_or(false);
        let scream2 = container_logs(&docker, mesh_id, 2).await?.contains(SCREAM);
        print_and_add_check(
            &mut result,
            Check {
                name: "Skewed side: peers named, banner filled, its defuser screams".to_string(),
                passed: ev2["summary"]["local_version"] == SKEW_VERSION
                    && peers_skewed
                    && banner2
                    && scream2,
                detail: None,
            },
        );

        // 7. Restore: the node returns on its real identity; the skew
        // clears everywhere and all three seats decide again.
        super::regenesis::recreate_node_with_env(&docker, mesh_id, 2, None, &[])
            .await
            .context("restore node 2")?;
        let node2 = super::regenesis::reauth_node(&docker, mesh_id, &node2)
            .await
            .context("reauth restored node 2")?;
        let mut cleared = false;
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            let row = evidence_row(&nodes[0], 2).await?;
            let view = get_json(&nodes[0], "/api/views/network-resilience").await?;
            if row["pong"]["skew"] == false
                && view["consensus"]["version_skew"] == serde_json::json!([])
            {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        // The restored seat follows the chain again (its decided height
        // catches the tip the upload advanced).
        let mut caught_up = false;
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            let tip = crate::tests::get_max_view(&nodes[..2]).await.unwrap_or(0);
            let own = crate::tests::get_max_view(std::slice::from_ref(&node2))
                .await
                .unwrap_or(0);
            if own >= tip && tip > 0 {
                caught_up = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Restore: skew clears, the seat follows the chain again".to_string(),
                passed: cleared && caught_up,
                detail: None,
            },
        );

        Ok(result)
    }
}
