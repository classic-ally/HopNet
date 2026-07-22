use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::collections::HashSet;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{
    download_file_from_all_nodes_with_timeout, get_fragment_distribution,
    trigger_fragment_inventory_sync_all, upload_file, verify_all_identical,
    wait_for_fragment_distribution,
};
use crate::tests::graceful_leave::{view_state, wait_validator_count};
use crate::tests::persistence::stop_node;
use crate::tests::reencode::{trigger_metrics, trigger_tick, view_members};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

/// Three-timescale coupling: one fault drives BOTH planes, in order
/// (RFC-STORAGE-001 "Membership timescales"; RFC-CONSENSUS-002).
///
/// The headline coverage gap: no other gate watches a consensus membership
/// change and a storage rebalance in the SAME run. Here one kill triggers a
/// fast validator vote-out (minutes clock) while storage placement — which
/// keys off decay-tier membership, NOT the validator set — does NOT move at
/// vote-out time (the §408 decoupling, live), and only later does the victim
/// decay out of the storage view and its classes re-encode onto survivors.
///
/// Seeding puts a wide gap between the two clocks: consensus knobs match
/// `vote-out-after-kill` (vote-out ~8-15s post-kill), while the storage cold
/// tier is 90s — larger than the vote-out budget — so the ordering guard is
/// structural even if a background metrics round starts the absence clock at
/// kill time.
pub struct ThreeTimescales;

impl TestScenario for ThreeTimescales {
    fn name(&self) -> &'static str {
        "three-timescales"
    }

    fn description(&self) -> &'static str {
        "One kill: fast validator vote-out, storage placement unmoved, then slow decay + re-encode"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = Client::new();
        anyhow::ensure!(nodes.len() == 3, "three-timescales expects a 3-node mesh");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let filename = format!("test-3ts-{}.txt", timestamp);
        let contents = format!("HopNet three-timescale test {}", timestamp).into_bytes();
        let full_path = format!("/{}", filename);

        println!("\nRunning three-timescales checks:");

        // 1. Upload + distribute + settle inventory (lifted from reencode).
        let upload_ok = upload_file(&nodes[0], "/", &filename, contents.clone())
            .await
            .is_ok();
        let dist_ok = upload_ok
            && wait_for_fragment_distribution(&nodes[0], &full_path, Duration::from_secs(30))
                .await
                .is_ok();
        let _ = trigger_fragment_inventory_sync_all(nodes).await;
        trigger_metrics(&client, &nodes[0]).await;
        let mut settled = false;
        let settle_start = Instant::now();
        let mut settle_round = 0u32;
        while settle_start.elapsed() < Duration::from_secs(90) {
            if settle_round % 5 == 4 {
                let _ = trigger_fragment_inventory_sync_all(nodes).await;
            }
            settle_round += 1;
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
                name: "Upload, distribute, settle inventory".to_string(),
                passed: dist_ok && settled,
                detail: None,
            },
        );
        if !(dist_ok && settled) {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 2. Snapshot pre-kill state: victim's classes (as a storage holder
        // id) and its consensus id (fetched via its own view report — the
        // two id spaces are not assumed equal).
        let victim = &nodes[2];
        let survivors = &nodes[..2];
        let victim_storage_id = victim.node_id as i32;
        let pre = get_fragment_distribution(&nodes[0], &full_path).await?;
        let victim_classes: Vec<u32> = pre
            .fragments
            .iter()
            .filter(|f| f.nodes_with_fragment.contains(&victim_storage_id))
            .map(|f| f.local_index)
            .collect();
        let tip0 = crate::tests::get_max_view(nodes).await.unwrap_or(0);
        let victim_consensus_id = view_state(&client, victim, tip0 as i64 + 1)
            .await
            .map(|vs| vs.node_id)
            .unwrap_or(victim.node_id as i32);

        // 3. Kill the victim.
        let docker = Docker::connect_with_local_defaults()?;
        let stop_ok = stop_node(&docker, mesh_id, victim.node_id).await.is_ok();
        let t_kill = Instant::now();
        print_and_add_check(
            &mut result,
            Check {
                name: format!(
                    "Stop node {} (consensus id {victim_consensus_id}, held {} classes)",
                    victim.node_id,
                    victim_classes.len()
                ),
                passed: stop_ok && !victim_classes.is_empty(),
                detail: Some(format!("classes: {victim_classes:?}")),
            },
        );
        if !(stop_ok && !victim_classes.is_empty()) {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 4. FAST CLOCK: the mesh votes the dead validator out (v=2).
        let voted_out =
            wait_validator_count(&client, survivors, 2, Some(victim_consensus_id), Duration::from_secs(60))
                .await
                .unwrap_or(false);
        let t_voteout = t_kill.elapsed();
        print_and_add_check(
            &mut result,
            Check {
                name: "Fast clock: validator voted out (v=2 on survivors)".to_string(),
                passed: voted_out,
                detail: Some(format!("after {:.0}s", t_voteout.as_secs_f64())),
            },
        );

        // 5. DECOUPLING (the novel money): at vote-out time, the victim is
        // STILL a storage member on both survivors, and its fragment classes
        // are still attributed to it — placement keys off decay-tier
        // membership, not the validator set, so removing the validator moved
        // zero storage bytes. (Live counterpart of the crate's
        // consensus_contract "placement is profile-invariant".)
        let mut still_member = voted_out;
        for s in survivors {
            match view_members(&client, s).await {
                Ok(members) => {
                    if !members.contains(&victim_storage_id) {
                        still_member = false;
                    }
                }
                Err(_) => still_member = false,
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Decoupling: victim still a STORAGE member at vote-out time (both survivors)"
                    .to_string(),
                passed: still_member,
                detail: Some("validator removal moved no placement".to_string()),
            },
        );

        let mid = get_fragment_distribution(&nodes[0], &full_path).await?;
        let still_holder = mid
            .fragments
            .iter()
            .filter(|f| victim_classes.contains(&f.local_index))
            .all(|f| f.nodes_with_fragment.contains(&victim_storage_id));
        print_and_add_check(
            &mut result,
            Check {
                name: "Zero bytes moved on vote-out: victim still holds its classes (not regenerated)"
                    .to_string(),
                passed: still_holder,
                detail: None,
            },
        );

        // 6. SLOW CLOCK: drive metrics + ticks until the victim's sustained
        // absence outlives its (90s cold) decay tier and it leaves the
        // storage view. Sample the validator count meanwhile: it must stay 2
        // (no phantom re-seat of the dead node).
        let mut departed = false;
        let mut reseat_seen = false;
        let decay_start = Instant::now();
        while decay_start.elapsed() < Duration::from_secs(360) {
            trigger_metrics(&client, &nodes[0]).await;
            trigger_tick(&client, &nodes[0]).await;
            trigger_tick(&client, &nodes[1]).await;
            let tip = crate::tests::get_max_view(survivors).await.unwrap_or(0);
            if let Ok(vs) = view_state(&client, &survivors[0], tip as i64 + 1).await
                && vs.validators_at_height.len() != 2
            {
                reseat_seen = true;
            }
            if let Ok(members) = view_members(&client, &nodes[0]).await
                && !members.contains(&victim_storage_id)
            {
                departed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        let t_decay = t_kill.elapsed();
        print_and_add_check(
            &mut result,
            Check {
                name: "Slow clock: victim decays out of the storage view".to_string(),
                passed: departed,
                detail: Some(format!("after {:.0}s", t_decay.as_secs_f64())),
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Validator set stayed at 2 during decay (no phantom reseat)".to_string(),
                passed: !reseat_seen,
                detail: None,
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Ordering: vote-out preceded storage departure".to_string(),
                passed: departed && t_voteout < t_decay,
                detail: Some(format!(
                    "vote-out {:.0}s < storage departure {:.0}s",
                    t_voteout.as_secs_f64(),
                    t_decay.as_secs_f64()
                )),
            },
        );
        if !departed {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 7. Re-encode: every class the victim held gets a live holder among
        // the survivors.
        let survivor_ids: HashSet<i32> = survivors.iter().map(|n| n.node_id as i32).collect();
        let mut regenerated = false;
        let repair_start = Instant::now();
        while repair_start.elapsed() < Duration::from_secs(180) {
            trigger_tick(&client, &nodes[0]).await;
            trigger_tick(&client, &nodes[1]).await;
            let _ = trigger_fragment_inventory_sync_all(survivors).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            if let Ok(dist) = get_fragment_distribution(&nodes[0], &full_path).await
                && !dist.fragments.is_empty()
                && dist.fragments.iter().all(|f| {
                    f.nodes_with_fragment
                        .iter()
                        .any(|n| survivor_ids.contains(n))
                })
            {
                regenerated = true;
                break;
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Every victim class regenerated on survivors".to_string(),
                passed: regenerated,
                detail: Some(format!("after {:.0}s", repair_start.elapsed().as_secs_f64())),
            },
        );

        // 8. File still fully downloadable from survivors, byte-identical.
        let downloads_ok = match download_file_from_all_nodes_with_timeout(
            survivors,
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
                name: "File downloadable from survivors, byte-identical".to_string(),
                passed: downloads_ok,
                detail: None,
            },
        );

        result.details = format!(
            "One kill drove both planes: vote-out at {:.0}s, storage departure at {:.0}s; \
             {} classes re-encoded, data intact",
            t_voteout.as_secs_f64(),
            t_decay.as_secs_f64(),
            victim_classes.len()
        );
        result.duration = start.elapsed();
        Ok(result)
    }
}
