use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};

use crate::tests::evidence_observe::fetch_evidence;
use crate::tests::files::{download_file_from_all_nodes_with_timeout, upload_file};
use crate::tests::graceful_leave::view_state;
use crate::tests::mesh_growth::rebuild_nodes;
use crate::tests::persistence::stop_node;
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check};

/// AUTO profile across the V_bft seam (RFC-CONSENSUS-002 S6) — the crossing
/// watched, not just the endpoints.
///
/// The mesh forms in the majority region (v ∈ {5,6}: a 6-container mesh seats
/// 5 with a pooled spare, or 6). We then add a node so a pooled candidate
/// batch-seats UPWARD across the seam to v=7, where AUTO flips majority→BFT
/// and the effective quorum changes (3→5 or 4→5). The crossing loop reads the
/// quorum live from `/consensus/evidence` (`summary.quorum`), and a committed-
/// history walk proves the seating was ATOMIC (no height ever sat strictly
/// between the pre-seam count and 7). A file uploaded under majority is then
/// served by all 7 under BFT. Finally the graceful-shrink half of the
/// self-heal loop: kill 3 at v=7 (below BFT quorum 5) and watch the survivors
/// shed the dead back into the majority region.
///
/// (The pure BFT-threshold-at-v=7 observable — a *simultaneous* 3-loss
/// stalling outright — is proven fault-injection-clean in the crate sim test
/// tests/seam.rs; here the live vote-out scan turns it into the graceful
/// shrink, the real end-to-end behavior.)
pub struct AutoSeam;

/// `(seated count, effective quorum)` from the live evidence summary.
async fn v_and_quorum(client: &Client, node: &NodeInfo) -> Option<(u64, u64)> {
    let doc = fetch_evidence(client, node).await.ok()?;
    let v = doc["summary"]["v"].as_u64()?;
    let q = doc["summary"]["quorum"].as_u64()?;
    Some((v, q))
}

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
        "AUTO crosses the V_bft seam upward: quorum flips 3/4->5 live, data served across it"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        let client = Client::new();
        anyhow::ensure!(
            nodes.len() == 6,
            "auto-seam expects a 6-node mesh (it adds the 7th)"
        );
        let docker = Docker::connect_with_local_defaults()?;

        println!("\nRunning auto-seam checks:");

        // 1. Baseline: stabilize in the majority region. A 6-container AUTO
        // mesh seats 5 (pooled spare) or 6; poll until the seated count is
        // stable across 3 consecutive samples and in {5,6}.
        let mut v_pre = 0u64;
        let mut q_pre = 0u64;
        let mut stable = false;
        let mut last = 0u64;
        let mut streak = 0u32;
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if let Some((v, q)) = v_and_quorum(&client, &nodes[0]).await {
                if v == last && (v == 5 || v == 6) {
                    streak += 1;
                } else {
                    streak = 0;
                }
                last = v;
                if streak >= 2 {
                    v_pre = v;
                    q_pre = q;
                    stable = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline stable in majority region (v in {5,6})".to_string(),
                passed: stable,
                detail: Some(format!("v_pre={v_pre} quorum={q_pre}")),
            },
        );
        // Majority formula holds pre-seam.
        print_and_add_check(
            &mut result,
            Check {
                name: "Pre-seam quorum matches the majority formula (v/2+1)".to_string(),
                passed: stable && q_pre == v_pre / 2 + 1,
                detail: Some(format!(
                    "quorum({v_pre})={q_pre}, expected {}",
                    v_pre / 2 + 1
                )),
            },
        );
        if !stable {
            return Ok(result);
        }
        let h0 = get_max_view(nodes).await.unwrap_or(0);

        // 2. Upload a file UNDER the majority profile (served under BFT later).
        let content = b"auto-seam: uploaded under majority, served under BFT".to_vec();
        let upload_ok = upload_file(&nodes[0], "/", "seam_test.txt", content.clone())
            .await
            .is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload under the majority profile".to_string(),
                passed: upload_ok,
                detail: None,
            },
        );

        // 3. Add the 7th container.
        crate::add_nodes_to_mesh(&docker, mesh_id, 1, crate::sys::detect_runtime(&docker).await?).await?;
        let all7 = rebuild_nodes(&docker, mesh_id).await?;

        // 4. Crossing observer: poll the live quorum @1s until v=7,quorum=5.
        // Record every (v,quorum) sample; assert the flip and that quorum
        // tracked the active profile at every step.
        let mut reached_seam = false;
        let mut sub_seam_ok = true;
        let mut bad_at_7 = false;
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if let Some((v, q)) = v_and_quorum(&client, &nodes[0]).await {
                if (1..7).contains(&v) && q != v / 2 + 1 {
                    sub_seam_ok = false;
                }
                if v == 7 && q != 5 {
                    bad_at_7 = true;
                }
                if v == 7 && q == 5 {
                    reached_seam = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Seam crossed upward: v=7, quorum flipped to 5 (BFT) observed live"
                    .to_string(),
                passed: reached_seam,
                detail: Some(format!("from v_pre={v_pre}/quorum={q_pre}")),
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Quorum tracked the active profile at every sub-seam sample".to_string(),
                passed: sub_seam_ok && !bad_at_7,
                detail: None,
            },
        );

        // 5. Atomic crossing: walk committed heights h0..tip; every height's
        // validator count is either the pre-seam count or 7 — the batch never
        // sat at an intermediate size (e.g. 5->7 skips 6).
        let tip = get_max_view(&all7).await.unwrap_or(h0);
        let mut atomic = reached_seam;
        let mut intermediate: Option<usize> = None;
        for h in h0..=tip {
            let c = seated_count(&client, &all7[0], h as i64).await;
            if c != 0 && c != v_pre as usize && c != 7 {
                atomic = false;
                intermediate = Some(c);
                break;
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Atomic crossing: no committed height between the pre-seam count and 7"
                    .to_string(),
                passed: atomic,
                detail: intermediate.map(|c| format!("saw intermediate count {c}")),
            },
        );

        // 6. Serve the pre-seam file from all 7 across the seam.
        let served = download_file_from_all_nodes_with_timeout(
            &all7,
            "/seam_test.txt",
            Duration::from_secs(45),
        )
        .await
        .map(|d| d.iter().all(|b| b == &content))
        .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "File served by all 7 across the seam (majority upload, BFT read)"
                    .to_string(),
                passed: served,
                detail: None,
            },
        );

        // 7. Graceful shrink: kill the 3 highest-id containers (below BFT
        // quorum 5) and watch the survivors shed them back to the majority
        // region. Killed AFTER the download so holders survive step 6.
        let survivors: Vec<NodeInfo> = all7[..4].to_vec();
        let mut ids: Vec<u32> = all7.iter().map(|n| n.node_id).collect();
        ids.sort_unstable();
        for id in ids.iter().rev().take(3) {
            let _ = stop_node(&docker, mesh_id, *id).await;
        }
        let mut shed = false;
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            let t = get_max_view(&survivors).await.unwrap_or(0);
            if seated_count(&client, &survivors[0], t as i64 + 1).await <= 4 {
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

        result.details = format!(
            "Crossed the seam upward v_pre={v_pre}(q{q_pre})->7(q5), served data across it, shed back"
        );
        Ok(result)
    }
}
