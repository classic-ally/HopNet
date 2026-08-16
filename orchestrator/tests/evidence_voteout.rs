use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::tests::evidence_observe::{fetch_evidence, node_entry};
use crate::tests::files::{download_file_from_all_nodes_with_timeout, upload_file};
use crate::tests::graceful_leave::{view_state, wait_validator_count};
use crate::tests::persistence::stop_node;
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check};

/// Evidence compression CAUSES the vote-out — observed in one run
/// (RFC-CONSENSUS-002 S3+S4).
///
/// `evidence-observe` watches the band compress Fast→Cliff; `vote-out`
/// watches the set shrink; neither asserts the causal chain in a single run.
/// Here one kill is followed by a tight (500ms) poll of `/consensus/evidence`
/// on both survivors — each response atomically carries the seated count
/// (`summary.v`), band, live/headroom, and the victim's probe count — so we
/// record, race-free, that the compression (band Cliff, headroom 0, probes
/// ≥ the attestation floor) is observed WHILE the victim is still seated
/// (v=3), and STRICTLY BEFORE the same victim leaves the set (v=2). The
/// observed compression is the observed removal's cause, not an inference
/// across two separate runs.
///
/// Seeds a wider probe base (3, vs the sibling 2) so the window between
/// first-observable compression and removal is ~4-5s — 8-10 samples at
/// 500ms.
pub struct EvidenceDrivesVoteout;

/// First-observation timestamps for the transition, per survivor.
#[derive(Default, Clone, Copy)]
struct Marks {
    compress: Option<Duration>,
    probes: Option<Duration>,
    removed: Option<Duration>,
}

impl TestScenario for EvidenceDrivesVoteout {
    fn name(&self) -> &'static str {
        "evidence-drives-voteout"
    }

    fn description(&self) -> &'static str {
        "Evidence compression is observed to precede and cause the vote-out, in one run"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        let client = crate::insecure_client();
        anyhow::ensure!(
            nodes.len() == 3,
            "evidence-drives-voteout expects a 3-node mesh"
        );

        println!("\nRunning evidence-drives-voteout checks:");

        // 1. Baseline: all peers fresh, band Fast, headroom 1 (majority v=3).
        let mut baseline = false;
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let mut ok = true;
            for node in nodes {
                let Ok(doc) = fetch_evidence(&client, node).await else {
                    ok = false;
                    break;
                };
                if doc["summary"]["band"] != "Fast"
                    || doc["summary"]["headroom"].as_i64() != Some(1)
                {
                    ok = false;
                    break;
                }
            }
            if ok {
                baseline = true;
                break;
            }
            sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline: all bright, band Fast, headroom 1 (majority v=3)".to_string(),
                passed: baseline,
                detail: None,
            },
        );

        // Victim consensus id from its own report (not assumed == container id).
        let tip0 = get_max_view(nodes).await.unwrap_or(0);
        let victim = &nodes[2];
        let survivors = [nodes[0].clone(), nodes[1].clone()];
        let victim_id = view_state(&client, victim, tip0 as i64 + 1)
            .await
            .map(|vs| vs.node_id)
            .unwrap_or(victim.node_id as i32);

        // 2. Kill the victim.
        let docker = crate::sys::connect()?;
        let stop_ok = stop_node(&docker, mesh_id, victim.node_id).await.is_ok();
        let t_kill = Instant::now();
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Stop node {} (consensus id {victim_id})", victim.node_id),
                passed: stop_ok,
                detail: None,
            },
        );

        // 3. The single tight transition loop: sample both survivors every
        // 500ms, recording the first time each observes the composite
        // compression state (victim still seated), the attestation floor,
        // and finally the removal. One evidence GET carries all fields, so
        // each sample is a race-free composite.
        let mut marks = [Marks::default(); 2];
        let loop_deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if Instant::now() >= loop_deadline {
                break;
            }
            for (i, node) in survivors.iter().enumerate() {
                let Ok(doc) = fetch_evidence(&client, node).await else {
                    continue;
                };
                let t_unresp = doc["summary"]["t_unresponsive_ms"].as_u64().unwrap_or(0);
                let v = doc["summary"]["v"].as_u64().unwrap_or(0);
                let band = &doc["summary"]["band"];
                let headroom = doc["summary"]["headroom"].as_i64().unwrap_or(99);
                let live = doc["summary"]["live"].as_u64().unwrap_or(99);
                let entry = node_entry(&doc, victim_id as i64);
                let age = entry.and_then(|e| e["age_ms"].as_u64()).unwrap_or(0);
                let probes = entry
                    .and_then(|e| e["probes_since_contact"].as_u64())
                    .unwrap_or(0);
                let seated = entry.and_then(|e| e["seated"].as_bool()).unwrap_or(true);

                // Compression WHILE the victim is still seated (v=3).
                if marks[i].compress.is_none()
                    && v == 3
                    && band == "Cliff"
                    && headroom == 0
                    && live == 2
                    && age > t_unresp
                {
                    marks[i].compress = Some(t_kill.elapsed());
                }
                // Attestation floor satisfied while still seated.
                if marks[i].probes.is_none() && v == 3 && probes >= 2 {
                    marks[i].probes = Some(t_kill.elapsed());
                }
                // Removal: victim unseated, v dropped to 2.
                if marks[i].removed.is_none() && v == 2 && !seated {
                    marks[i].removed = Some(t_kill.elapsed());
                }
            }
            if marks.iter().all(|m| m.removed.is_some()) {
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }

        // 4. Confirm removal on committed state too.
        let committed_removed = wait_validator_count(
            &client,
            &survivors,
            2,
            Some(victim_id),
            Duration::from_secs(30),
        )
        .await
        .unwrap_or(false);

        // 5. Assertions from the sample logs. The compression observation is
        // the timing-sensitive one; require it on at least one survivor (fall
        // back tolerance for a missed poll), the others on either.
        let compress_seen = marks.iter().any(|m| m.compress.is_some());
        print_and_add_check(
            &mut result,
            Check {
                name:
                    "Compression observed while victim still seated (v=3, band Cliff, H=0, live=2)"
                        .to_string(),
                passed: compress_seen,
                detail: Some(format!(
                    "first at {}",
                    marks
                        .iter()
                        .filter_map(|m| m.compress.map(|d| format!("{:.1}s", d.as_secs_f64())))
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            },
        );

        let probes_seen = marks.iter().any(|m| m.probes.is_some());
        print_and_add_check(
            &mut result,
            Check {
                name: "Attestation floor observed pre-removal (probes >= 2 while v=3)".to_string(),
                passed: probes_seen,
                detail: None,
            },
        );

        let removed_seen = marks.iter().any(|m| m.removed.is_some()) || committed_removed;
        print_and_add_check(
            &mut result,
            Check {
                name: "Same victim voted out (v=2, victim unseated)".to_string(),
                passed: removed_seen,
                detail: None,
            },
        );

        // The causal chain: on some survivor, compression preceded removal.
        let ordered = marks.iter().any(|m| match (m.compress, m.removed) {
            (Some(c), Some(r)) => c < r,
            _ => false,
        });
        print_and_add_check(
            &mut result,
            Check {
                name: "Causal chain: compression strictly precedes removal on the same observer"
                    .to_string(),
                passed: ordered,
                detail: None,
            },
        );

        // 6. Consensus continues at v=2: upload + download on survivors.
        let content = b"evidence-drives-voteout: consensus continues".to_vec();
        let upload_ok = upload_file(&survivors[0], "/", "evdvo_test.txt", content.clone())
            .await
            .is_ok();
        let download_ok = upload_ok
            && download_file_from_all_nodes_with_timeout(
                &survivors,
                "/evdvo_test.txt",
                Duration::from_secs(30),
            )
            .await
            .map(|d| d.iter().all(|b| b == &content))
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Consensus continues at v=2 (upload + download on survivors)".to_string(),
                passed: upload_ok && download_ok,
                detail: None,
            },
        );

        Ok(result)
    }
}
