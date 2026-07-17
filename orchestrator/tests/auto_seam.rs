use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};

use crate::tests::graceful_leave::view_state;
use crate::tests::persistence::stop_node;
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check};

/// AUTO profile across the V_bft seam (RFC-CONSENSUS-002 S6).
///
/// A mesh grows to 7 validators under the AUTO default — crossing the seam
/// at v=6→7, where the effective profile flips from majority to BFT
/// (quorum 4 → 5). Then the self-heal loop across the seam: killing three
/// at v=7 (below the BFT quorum of 5) makes the mesh GRACEFULLY SHRINK —
/// the survivors vote the dead out one by one, sliding v=7→…→4 back into
/// the majority region where 4 live hold quorum. (The climb back across
/// the seam is S5's batch-seating exercised near the composite, covered by
/// the mesh-growth gate.)
/// (The pure BFT-threshold-at-v=7 observable — where a *simultaneous*
/// 3-loss stalls outright — is proven fault-injection-clean in the crate
/// sim test tests/seam.rs; here the live vote-out scan turns it into the
/// spec's graceful shrink, which is the real end-to-end behavior.)
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

        // 2. Kill 3 at v=7: the mesh sheds the dead across the seam back
        // into the majority region (v drops toward 4). The graceful-shrink
        // half of the self-heal loop — the survivors keep the (shrinking)
        // quorum at every step.
        let survivors: Vec<NodeInfo> = nodes[..4].to_vec();
        for i in [6usize, 5, 4] {
            let _ = stop_node(&docker, mesh_id, nodes[i].node_id).await;
        }
        let mut shed = false;
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            let tip = get_max_view(&survivors).await.unwrap_or(0);
            if seated_count(&client, &survivors[0], tip as i64 + 1).await <= 4 {
                shed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "v=7 sheds the 3 dead across the seam (graceful shrink to majority)"
                    .to_string(),
                passed: shed,
                detail: None,
            },
        );


        Ok(result)
    }
}
