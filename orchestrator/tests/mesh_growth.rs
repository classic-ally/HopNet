use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};

use crate::tests::files::{download_file_from_all_nodes_with_timeout, upload_file};
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check};

/// Mesh growth by mesh-initiated seating (RFC-CONSENSUS-002 S5) — the
/// parity math as visible behavior.
///
/// Adding ONE node to a majority 3-mesh is a lateral seating (3→4:
/// quorum(3)=2, quorum(4)=3, ΔH=0) — REFUSED by the posture rule, so the
/// new node stays a bright, synced POOL candidate. Adding a SECOND makes a
/// gaining batch (3→5: ΔH=2) — both seat together in ONE transition. The
/// single-lateral refusal and the atomic batch seat are the whole point of
/// the seating policy.
pub struct MeshGrowth;

pub(crate) async fn rebuild_nodes(
    docker: &Docker,
    mesh_id: u32,
) -> Result<Vec<NodeInfo>> {
    let addresses = crate::get_external_addresses(docker, mesh_id, crate::sys::detect_runtime(docker).await?).await?;
    let mut nodes = Vec::new();
    for (node_id, ip_address, port) in addresses {
        let jwt_token =
            crate::get_jwt_token(docker, mesh_id, node_id, crate::sys::detect_runtime(docker).await?)
                .await?;
        nodes.push(NodeInfo {
            node_id,
            ip_address,
            port: port as u32,
            jwt_token,
        });
    }
    nodes.sort_by_key(|n| n.node_id);
    Ok(nodes)
}

async fn seated_count(client: &Client, node: &NodeInfo, height: i64) -> usize {
    let url = format!("http://{}:{}/api/consensus/view", node.ip_address, node.port);
    let Ok(resp) = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .json(&height)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    else {
        return 0;
    };
    resp.json::<serde_json::Value>()
        .await
        .ok()
        .and_then(|doc| doc["validators_at_height"].as_array().map(|a| a.len()))
        .unwrap_or(0)
}

impl TestScenario for MeshGrowth {
    fn name(&self) -> &'static str {
        "mesh-growth"
    }

    fn description(&self) -> &'static str {
        "Single lateral join stays pooled; a second join batch-seats both (majority parity)"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        let client = Client::new();
        anyhow::ensure!(nodes.len() == 3, "mesh-growth expects a 3-node mesh");
        let docker = Docker::connect_with_local_defaults()?;

        println!("\nRunning mesh-growth checks:");

        // 1. Baseline: 3 seated (majority default).
        let tip = get_max_view(nodes).await.unwrap_or(0);
        let base = seated_count(&client, &nodes[0], tip as i64 + 1).await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline: 3 validators".to_string(),
                passed: base == 3,
                detail: Some(format!("seated={base}")),
            },
        );

        // 2. Add one node — it is a lateral candidate.
        crate::add_nodes_to_mesh(&docker, mesh_id, 1, crate::sys::detect_runtime(&docker).await?).await?;
        let after1 = rebuild_nodes(&docker, mesh_id).await?;
        let newcomer = after1.last().cloned().unwrap();

        // 3. It syncs and stays UNSEATED well past s_full (the lateral is
        // refused on posture, not span). Watch a window comfortably past
        // s_full=6 + the scan cadence: 25s, asserting it never seats.
        let mut stayed_pooled = true;
        let mut ever_synced = false;
        let deadline = Instant::now() + Duration::from_secs(25);
        while Instant::now() < deadline {
            let t = get_max_view(&after1).await.unwrap_or(0);
            let seated = seated_count(&client, &nodes[0], t as i64 + 1).await;
            if seated != 3 {
                stayed_pooled = false;
                break;
            }
            // Newcomer following the chain (its own reported height advances).
            if get_max_view(std::slice::from_ref(&newcomer))
                .await
                .unwrap_or(0)
                > 0
            {
                ever_synced = true;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Single lateral join stays pooled (v stays 3, node synced)".to_string(),
                passed: stayed_pooled && ever_synced,
                detail: None,
            },
        );

        // 4. Add a second node — now a gaining batch of 2 (3->5).
        crate::add_nodes_to_mesh(&docker, mesh_id, 1, crate::sys::detect_runtime(&docker).await?).await?;
        let after2 = rebuild_nodes(&docker, mesh_id).await?;

        // 5. MONEY: v reaches 5, and no height ever reports exactly 4 —
        // both new nodes seat in ONE transition (batch atomicity).
        let mut reached5 = false;
        let mut ever_saw_4 = false;
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let t = get_max_view(&after2).await.unwrap_or(0);
            let seated = seated_count(&client, &nodes[0], t as i64 + 1).await;
            if seated == 4 {
                ever_saw_4 = true;
            }
            if seated == 5 {
                reached5 = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Batch seats both new nodes: v -> 5 in one transition (never 4)".to_string(),
                passed: reached5 && !ever_saw_4,
                detail: None,
            },
        );

        // 6. Full-mesh participation at v=5.
        let content = b"mesh-growth: five and serving".to_vec();
        let upload_ok = upload_file(&after2[0], "/", "growth_test.txt", content.clone())
            .await
            .is_ok();
        let download_ok = upload_ok
            && download_file_from_all_nodes_with_timeout(
                &after2,
                "/growth_test.txt",
                Duration::from_secs(45),
            )
            .await
            .map(|d| d.iter().all(|b| b == &content))
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "All 5 nodes serve the upload".to_string(),
                passed: upload_ok && download_ok,
                detail: None,
            },
        );

        Ok(result)
    }
}
