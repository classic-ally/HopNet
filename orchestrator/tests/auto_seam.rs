use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};

use crate::tests::graceful_leave::view_state;
use crate::tests::persistence::{start_node, stop_node, wait_for_node_ready};
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check};

/// AUTO profile across the V_bft seam (RFC-CONSENSUS-002 S6).
///
/// A mesh grows to 7 validators under the AUTO default — crossing the seam
/// at v=6→7, where the effective profile flips from majority to BFT
/// (quorum 4 → 5). The observable: at v=7 killing THREE leaves 4 live,
/// below the BFT quorum of 5, so consensus stalls (a majority mesh would
/// keep committing with 4 of 7). Restoring resumes.
pub struct AutoSeam;

async fn seated_count(client: &Client, node: &NodeInfo, height: i64) -> usize {
    view_state(client, node, height)
        .await
        .map(|vs| vs.validators_at_height.len())
        .unwrap_or(0)
}

impl TestScenario for AutoSeam {
    fn name(&self) -> &'static str {
        "auto-seam"
    }

    fn description(&self) -> &'static str {
        "Mesh forms across the V_bft seam under AUTO; v=7 needs the BFT quorum"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        let client = Client::new();
        anyhow::ensure!(nodes.len() >= 7, "auto-seam expects a 7-node mesh");
        let docker = Docker::connect_with_local_defaults()?;

        println!("\nRunning auto-seam checks:");

        // 1. Formation crosses the seam: the mesh seats up to 7 under AUTO
        // (the v=6->7 crossing is a posture-legal lateral). create_mesh's
        // formation wait already ran; confirm v=7.
        let mut reached7 = false;
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            let tip = get_max_view(nodes).await.unwrap_or(0);
            if seated_count(&client, &nodes[0], tip as i64 + 1).await == 7 {
                reached7 = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Mesh forms to 7 validators across the seam (AUTO -> BFT)".to_string(),
                passed: reached7,
                detail: None,
            },
        );

        // 2. BFT threshold at v=7: kill 3 (quorum(7)=5, 4 live < 5) — no new
        // decided height for a window.
        let before = get_max_view(nodes).await.unwrap_or(0);
        for i in [6usize, 5, 4] {
            let _ = stop_node(&docker, mesh_id, nodes[i].node_id).await;
        }
        tokio::time::sleep(Duration::from_secs(25)).await;
        let survivors: Vec<NodeInfo> = nodes[..4].to_vec();
        let during = get_max_view(&survivors).await.unwrap_or(before);
        // Allow a small pre-kill in-flight advance, but it must not keep
        // committing across the window.
        let stalled = during <= before + 1;
        print_and_add_check(
            &mut result,
            Check {
                name: "v=7 stalls with 3 down (BFT quorum 5 > 4 live)".to_string(),
                passed: stalled,
                detail: Some(format!("{before} -> {during}")),
            },
        );

        // 3. Restore quorum: bring the three back, consensus resumes.
        for i in [4usize, 5, 6] {
            let _ = start_node(&docker, mesh_id, nodes[i].node_id).await;
            let _ = wait_for_node_ready(&nodes[i], Duration::from_secs(30)).await;
        }
        let mut resumed = false;
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if get_max_view(nodes).await.unwrap_or(0) > during {
                resumed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Progress resumes once the BFT quorum is restored".to_string(),
                passed: resumed,
                detail: None,
            },
        );

        Ok(result)
    }
}
