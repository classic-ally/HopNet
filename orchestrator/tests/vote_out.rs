use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::Duration;

use crate::tests::files::{download_file_from_all_nodes_with_timeout, upload_file};
use crate::tests::graceful_leave::{view_state, wait_validator_count};
use crate::tests::persistence::{start_node, stop_node};
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check};

/// Vote-out after a kill (RFC-CONSENSUS-002 S4) — the core fault path.
///
/// A killed validator ages past the removal window on every survivor's
/// evidence; the proposer scan submits the vote-out; the survivors'
/// subjective guards attest; the set shrinks and consensus continues.
/// The victim restarts, syncs, sees its own voted_out departure, and the
/// reactivation retry loop re-seats it once its bright span clears the
/// S_min gate — no human action anywhere.
///
/// Seeded: probe_base=2, grace=1 (t_out cliff/fast = 5s/9s), s_full=6,
/// p_prove=6; majority profile (forced until S6 — BFT v=3 with a dead
/// node commits nothing).
pub struct VoteOutAfterKill;

impl TestScenario for VoteOutAfterKill {
    fn name(&self) -> &'static str {
        "vote-out-after-kill"
    }

    fn description(&self) -> &'static str {
        "Killed validator is voted out by the mesh, then auto-readmitted after restart"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        let client = Client::new();
        anyhow::ensure!(nodes.len() == 3, "vote-out-after-kill expects a 3-node mesh");
        let victim = &nodes[2];
        let survivors = [nodes[0].clone(), nodes[1].clone()];

        println!("\nRunning vote-out-after-kill checks:");

        // 1. Baseline.
        let baseline = wait_validator_count(&client, nodes, 3, None, Duration::from_secs(30))
            .await
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline: 3 validators on every node".to_string(),
                passed: baseline,
                detail: None,
            },
        );
        let tip0 = get_max_view(nodes).await.unwrap_or(0);
        let victim_id = view_state(&client, victim, tip0 as i64 + 1)
            .await
            .map(|vs| vs.node_id)
            .unwrap_or(-1);

        // 2. Kill the victim.
        let docker = Docker::connect_with_local_defaults()?;
        let stopped = stop_node(&docker, mesh_id, victim.node_id).await.is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Stop node {} (consensus id {victim_id})", victim.node_id),
                passed: stopped,
                detail: None,
            },
        );

        // 3. The mesh evicts it: survivors report v=2 without the victim.
        // Budget: ages past t_unresponsive (~5s) -> band Cliff -> t_out 5s
        // -> probes accrue -> proposer scan (cooldown <= 2.5s) -> commit.
        let evicted = wait_validator_count(
            &client,
            &survivors,
            2,
            Some(victim_id),
            Duration::from_secs(60),
        )
        .await
        .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Mesh votes the dead validator out (v=2 on survivors)".to_string(),
                passed: evicted,
                detail: None,
            },
        );
        let eviction_tip = get_max_view(&survivors).await.unwrap_or(0);

        // 4. Consensus continues at v=2.
        let content = b"vote-out: consensus continues".to_vec();
        let upload_ok = upload_file(&survivors[0], "/", "voteout_test.txt", content.clone())
            .await
            .is_ok();
        let download_ok = upload_ok
            && download_file_from_all_nodes_with_timeout(
                &survivors,
                "/voteout_test.txt",
                Duration::from_secs(30),
            )
            .await
            .map(|d| d.iter().all(|b| b == &content))
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Consensus continues at v=2 (upload + download)".to_string(),
                passed: upload_ok && download_ok,
                detail: None,
            },
        );

        // 5. Restart the victim; it tip-polls back to the chain.
        let restarted = start_node(&docker, mesh_id, victim.node_id).await.is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "Restart the victim".to_string(),
                passed: restarted,
                detail: None,
            },
        );

        // 6. Fresh JWT (keys regenerate on startup), then the
        // height-scoped departure kind: once the victim has synced past
        // the eviction height, its own view at that height must say
        // voted_out (stays true even after readmission).
        let mut victim = victim.clone();
        match crate::get_jwt_token(
            &docker,
            mesh_id,
            victim.node_id,
            crate::sys::ContainerRuntime::Docker,
        )
        .await
        {
            Ok(token) => victim.jwt_token = token,
            Err(e) => println!("  (fresh JWT failed: {e}; using stale token)"),
        }
        let mut kind_ok = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        while std::time::Instant::now() < deadline {
            if let Ok(vs) = view_state(&client, &victim, eviction_tip as i64 + 1).await {
                if vs.last_departure_kind.as_deref() == Some("voted_out") {
                    kind_ok = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Victim's own view records departure_kind = voted_out".to_string(),
                passed: kind_ok,
                detail: None,
            },
        );

        // 7. AUTO-readmission: the retry loop + S_min gate re-seat it with
        // no explicit activate call (retry base 8s; floor span 2s at the
        // Cliff; exposure-free under majority 2->3).
        let refreshed: Vec<NodeInfo> =
            vec![survivors[0].clone(), survivors[1].clone(), victim.clone()];
        let restored =
            wait_validator_count(&client, &refreshed, 3, None, Duration::from_secs(120))
                .await
                .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Auto-readmission restores v=3 (no explicit activate)".to_string(),
                passed: restored,
                detail: None,
            },
        );

        // 8. Full-mesh participation.
        let content2 = b"vote-out: readmitted and serving".to_vec();
        let upload2 = upload_file(&nodes[0], "/", "readmit_test.txt", content2.clone())
            .await
            .is_ok();
        let download2 = upload2
            && download_file_from_all_nodes_with_timeout(
                &refreshed,
                "/readmit_test.txt",
                Duration::from_secs(45),
            )
            .await
            .map(|d| d.iter().all(|b| b == &content2))
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Readmitted node participates (upload visible on all 3)".to_string(),
                passed: upload2 && download2,
                detail: None,
            },
        );

        Ok(result)
    }
}
