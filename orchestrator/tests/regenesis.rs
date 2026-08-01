use anyhow::Result;
use reqwest::Client;
use std::time::Duration;

use crate::tests::files::upload_file;
use crate::tests::multi_user::fetch_state_snapshots;
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check};

/// Regenesis boundary, S5 scope (RFC-019): freeze → abort round-trip →
/// freeze again → drain → seal → halt, over real containers.
///
/// The scenario asserts every layer of the freeze: client writes 503
/// during the moratorium (retryable, structured refusal), the abort
/// reopens the mesh losslessly, the drain watcher + proposer injection
/// carry a drained moratorium to the commit with no outside help, every
/// node freezes at the same terminal height, and the replicas are
/// hash-identical at the seal.
///
/// DELIBERATELY ENDS SEALED: there is no restart path until S6, so the
/// mesh finishes halted by design. The auto-managed divergence gate still
/// passes — it only needs the HTTP plane, and a sealed mesh reports one
/// top-hash cluster at the terminal height (arguably the strongest
/// coherence assertion this suite has).
pub struct RegenesisSeal;

async fn post_json(
    node: &NodeInfo,
    path: &str,
    body: Option<serde_json::Value>,
) -> Result<(u16, String)> {
    let client = Client::new();
    let url = format!("http://{}:{}{}", node.ip_address, node.port, path);
    let mut req = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(70));
    if let Some(body) = body {
        req = req.json(&body);
    }
    let resp = req.send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    Ok((status, text))
}

async fn get_json(node: &NodeInfo, path: &str) -> Result<serde_json::Value> {
    let resp = crate::call_node_api(node, path, true).await?;
    anyhow::ensure!(resp.status().is_success(), "{path} {}", resp.status());
    Ok(resp.json().await?)
}

async fn regenesis_phase(node: &NodeInfo) -> Result<String> {
    let v = get_json(node, "/api/views/regenesis-status").await?;
    Ok(v["phase"].as_str().unwrap_or("?").to_string())
}

async fn decided_height(node: &NodeInfo) -> Result<u64> {
    let v = get_json(node, "/api/consensus").await?;
    Ok(v["last_decided_height"].as_u64().unwrap_or(0))
}

impl TestScenario for RegenesisSeal {
    fn name(&self) -> &'static str {
        "regenesis-seal"
    }

    fn description(&self) -> &'static str {
        "Freeze/abort round-trip, then drain, seal, and halt the mesh (RFC-019 S5; ends sealed)"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "regenesis-seal expects a 3-node mesh");

        println!("\nRunning regenesis-seal checks:");

        // 1. Baseline traffic + the boot version attestations (S3) that
        //    the start precondition reads. Every seated validator must
        //    show a committed running version.
        upload_file(&nodes[0], "/", "pre-freeze.txt", b"before the boundary".to_vec()).await?;
        let running = {
            let mut running = None;
            for _ in 0..40 {
                let v = get_json(&nodes[0], "/api/views/upgrade-readiness").await?;
                let all_attested = v["mesh"]
                    .as_array()
                    .map(|m| !m.is_empty() && m.iter().all(|n| n["running"].is_string()))
                    .unwrap_or(false);
                if all_attested {
                    running = v["running"].as_str().map(str::to_string);
                    break;
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            running
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Every validator attested its running version (S3 precondition input)"
                    .to_string(),
                passed: running.is_some(),
                detail: running.clone(),
            },
        );
        let Some(running) = running else {
            return Ok(result);
        };

        // 2. Freeze: a same-version (housekeeping) regenesis_start.
        let (status, body) = post_json(
            &nodes[0],
            "/api/consensus/regenesis/start",
            Some(serde_json::json!({ "target_version": running })),
        )
        .await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "regenesis_start decided (moratorium opens)".to_string(),
                passed: status == 200,
                detail: Some(format!("{status}: {body}")),
            },
        );

        let phase = regenesis_phase(&nodes[1]).await.unwrap_or_default();
        print_and_add_check(
            &mut result,
            Check {
                name: "Committed phase is moratorium mesh-wide".to_string(),
                passed: phase == "moratorium",
                detail: Some(phase),
            },
        );

        // 3. The freeze is real at the client layer: a write 503s.
        let refused = match upload_file(
            &nodes[1],
            "/",
            "during-freeze.txt",
            b"must be refused".to_vec(),
        )
        .await
        {
            Ok(()) => Some("accepted (BUG)".to_string()),
            Err(e) if e.to_string().contains("503") => None,
            Err(e) => Some(format!("wrong refusal: {e}")),
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Client write refused with 503 during the moratorium".to_string(),
                passed: refused.is_none(),
                detail: refused,
            },
        );

        // 4. Abort round-trip: the window reopens losslessly.
        let (status, _) = post_json(&nodes[0], "/api/consensus/regenesis/abort", None).await?;
        let phase = regenesis_phase(&nodes[2]).await.unwrap_or_default();
        let reopened = status == 200 && phase == "normal";
        let tip_before = get_max_view(nodes).await.unwrap_or(0);
        upload_file(&nodes[1], "/", "post-abort.txt", b"writes work again".to_vec()).await?;
        let mut advanced = false;
        for _ in 0..20 {
            if get_max_view(nodes).await.unwrap_or(0) > tip_before {
                advanced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Abort reopens admission; writes and heights resume".to_string(),
                passed: reopened && advanced,
                detail: Some(format!("phase {phase}, heights advanced: {advanced}")),
            },
        );

        // 5. Freeze again — this time all the way: the drain watcher and
        //    the proposer's commit injection need NO help from the test.
        let (status, body) = post_json(
            &nodes[0],
            "/api/consensus/regenesis/start",
            Some(serde_json::json!({ "target_version": running })),
        )
        .await?;
        anyhow::ensure!(status == 200, "second start failed: {status} {body}");

        let mut sealed_everywhere = false;
        for _ in 0..120 {
            let mut all = true;
            for node in nodes {
                if regenesis_phase(node).await.unwrap_or_default() != "sealed" {
                    all = false;
                    break;
                }
            }
            if all {
                sealed_everywhere = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Drained moratorium seals on its own (watcher + proposer injection)"
                    .to_string(),
                passed: sealed_everywhere,
                detail: None,
            },
        );
        if !sealed_everywhere {
            return Ok(result);
        }

        // 6. The halt: every node frozen at the same terminal height.
        let status_view = get_json(&nodes[0], "/api/views/regenesis-status").await?;
        let seal_height: u64 = status_view["seal_height"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let mut before = Vec::new();
        for node in nodes {
            before.push(decided_height(node).await.unwrap_or(0));
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        let mut after = Vec::new();
        for node in nodes {
            after.push(decided_height(node).await.unwrap_or(0));
        }
        let frozen = before == after
            && after.iter().all(|h| *h == seal_height)
            && seal_height > 0;
        print_and_add_check(
            &mut result,
            Check {
                name: "Every engine halted at the terminal height".to_string(),
                passed: frozen,
                detail: Some(format!("seal_height {seal_height}, decided {after:?}")),
            },
        );

        // 7. Sealed admits nothing (forward-only).
        let sealed_refused = matches!(
            upload_file(&nodes[0], "/", "post-seal.txt", b"never".to_vec()).await,
            Err(e) if e.to_string().contains("503")
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Writes refused with 503 after the seal".to_string(),
                passed: sealed_refused,
                detail: None,
            },
        );

        // 8. Coherence at the seal: one top-hash cluster at one height —
        //    the mesh crossed nothing and diverged nowhere.
        let snapshots = fetch_state_snapshots(nodes).await?;
        let coherent = snapshots
            .windows(2)
            .all(|w| {
                w[0].1.consensus_height == w[1].1.consensus_height
                    && w[0].1.manifest.top_hash == w[1].1.manifest.top_hash
            })
            && snapshots
                .first()
                .map(|(_, s)| s.consensus_height == seal_height)
                .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Replicas hash-identical at the terminal height".to_string(),
                passed: coherent,
                detail: Some(format!(
                    "heights: {:?}",
                    snapshots
                        .iter()
                        .map(|(id, s)| (*id, s.consensus_height))
                        .collect::<Vec<_>>()
                )),
            },
        );

        // The mesh ends SEALED on purpose (see the struct doc).
        Ok(result)
    }
}
