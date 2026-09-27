//! The RFC-STORAGE-003 cutover, rehearsed: a POPULATED mesh born on the
//! deployed release crosses into THIS build and the lifecycle's catch-up
//! drain converges on its own.
//!
//! This is the flip itself, in miniature. The mesh is born on the newest
//! released image (`ENFORCEMENT_OLD_RELEASE`, what production runs),
//! files are written and distributed under the old regime, some are
//! deleted, and the RFC-019 upgrade choreography carries the mesh over
//! the boundary: stage, seal, every node recreated onto this build. On
//! the far side the storage chain fast-forwards through the lifecycle
//! steps, the backfill enrolls every blob at the sentinel goal, the first
//! transition and the first declare page start the drain — and the
//! rehearsal watches the pane until `converged` holds everywhere, every
//! inventory row is disk-verified, every surviving file still downloads
//! byte-identical from every node, and state is coherent.
//!
//! Flags: `blobs=N` files written before the crossing (default 60; use
//! thousands to size the drain), `delete=M` of them deleted before the
//! crossing (default 5). The convergence deadline scales with N.
//!
//! Before running, load the old image:
//! `scripts/build-release-image.sh v<ENFORCEMENT_OLD_RELEASE>`.

use anyhow::Result;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::tests::files::{
    delete_file, download_file_from_all_nodes_with_timeout, upload_files_multi,
    wait_for_fragment_distribution,
};
use crate::tests::multi_user::fetch_state_snapshots;
use crate::tests::regenesis::{
    ENFORCEMENT_OLD_RELEASE, attest_and_freeze, coherence, decided_height,
    enforcement_crossing_target, get_json, ordinal_map, post_json, reauth_node,
    recreate_node_with_env, regenesis_status, wait_for_convergence, wait_sealed_everywhere,
};
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};

pub struct LifecycleCutoverDrain;

fn flag(flags: &[String], key: &str, default: usize) -> usize {
    flags
        .iter()
        .find_map(|f| f.strip_prefix(key).and_then(|v| v.strip_prefix('=')))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Deterministic per-file bytes: mostly small, every tenth a few hundred
/// KiB so the drain moves real fragments, never all the same size.
fn contents(i: usize) -> Vec<u8> {
    let len = if i % 10 == 9 {
        300 * 1024 + i
    } else {
        64 + i * 7
    };
    (0..len).map(|b| ((b * 31 + i * 17) % 251) as u8).collect()
}

fn lifecycle(view: &serde_json::Value) -> (bool, u64, u64, u64) {
    let l = &view["storage"]["lifecycle"];
    (
        l["converged"].as_bool().unwrap_or(false),
        l["owed"].as_u64().unwrap_or(u64::MAX),
        l["in_flight"].as_u64().unwrap_or(u64::MAX),
        l["confirmed"].as_u64().unwrap_or(0),
    )
}

impl TestScenario for LifecycleCutoverDrain {
    fn name(&self) -> &'static str {
        "lifecycle-cutover-drain"
    }

    fn description(&self) -> &'static str {
        "Populated mesh born on the deployed release crosses into THIS build; the RFC-STORAGE-003 catch-up drain converges (the flip, rehearsed)"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(
            nodes.len() == 3,
            "lifecycle-cutover-drain expects a 3-node mesh"
        );
        let docker = crate::sys::connect()?;
        let blobs = flag(flags, "blobs", 60);
        let deletes = flag(flags, "delete", 5).min(blobs);
        let target = enforcement_crossing_target();
        let crossed_env: &[(&str, &str)] = &[("HOPNET_UPGRADE_VERSION_OVERRIDE", target)];
        let new_image = format!("hopnet:{}", crate::naming::checkout_hash());

        println!("\nRunning lifecycle-cutover-drain checks (blobs={blobs}, delete={deletes}):");

        // 0. Born on the deployed release — a misloaded image would
        //    self-cross and prove nothing.
        let born_on = regenesis_status(&nodes[0]).await?["running_version"]
            .as_str()
            .unwrap_or("?")
            .to_string();
        print_and_add_check(
            &mut result,
            Check {
                name: format!(
                    "Mesh born on the deployed release image ({ENFORCEMENT_OLD_RELEASE})"
                ),
                passed: born_on == ENFORCEMENT_OLD_RELEASE,
                detail: Some(born_on.clone()),
            },
        );
        if born_on != ENFORCEMENT_OLD_RELEASE {
            return Ok(result);
        }

        // 1. Populate under the OLD regime: batches of files (one consensus
        //    view per batch), distributed by the old push pipeline, then a
        //    few deletions so orphans and inode-less blobs cross too.
        let started = Instant::now();
        let mut expected: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for batch in (0..blobs).collect::<Vec<_>>().chunks(25) {
            let files: Vec<(String, Vec<u8>)> = batch
                .iter()
                .map(|i| (format!("cutover-{i:05}.bin"), contents(*i)))
                .collect();
            for (name, bytes) in &files {
                expected.insert(format!("/{name}"), bytes.clone());
            }
            upload_files_multi(&nodes[0], "/", files).await?;
        }
        let mut distributed = 0usize;
        for path in expected.keys() {
            if wait_for_fragment_distribution(&nodes[0], path, Duration::from_secs(60))
                .await
                .is_ok()
            {
                distributed += 1;
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: format!("{blobs} files written and distributed under the old regime"),
                passed: distributed == blobs,
                detail: Some(format!(
                    "{distributed}/{blobs} distributed in {:.0}s",
                    started.elapsed().as_secs_f64()
                )),
            },
        );
        if distributed != blobs {
            return Ok(result);
        }
        let doomed: Vec<String> = expected.keys().take(deletes).cloned().collect();
        for path in &doomed {
            delete_file(&nodes[0], path).await?;
            expected.remove(path);
        }
        let pre_tip = decided_height(&nodes[0]).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: format!("{deletes} files deleted before the crossing"),
                passed: true,
                detail: Some(format!("{} survive; tip {pre_tip}", expected.len())),
            },
        );

        // 2. The upgrade choreography: stage this build, seal.
        if attest_and_freeze(&mut result, nodes, Some(target))
            .await?
            .is_none()
        {
            return Ok(result);
        }
        let seal_height = wait_sealed_everywhere(nodes).await?.unwrap_or(0);
        print_and_add_check(
            &mut result,
            Check {
                name: "Upgrade-target moratorium seals".to_string(),
                passed: seal_height > 0,
                detail: Some(format!("seal_height {seal_height}")),
            },
        );
        if seal_height == 0 {
            return Ok(result);
        }

        // 3. The binary swap: every node recreated onto THIS build on its
        //    surviving volume. Crossing evidence: phase normal, epoch 2,
        //    every module at its chain head (storage through the lifecycle
        //    steps).
        let heads: BTreeMap<String, u64> = hopnet::db::chains::chains()
            .iter()
            .map(|c| (c.module.to_string(), u64::from(c.head())))
            .collect();
        let crossing_started = Instant::now();
        let mut fresh_nodes = Vec::new();
        let mut crossings = Vec::new();
        for node in nodes {
            recreate_node_with_env(
                &docker,
                mesh_id,
                node.node_id,
                Some(&new_image),
                crossed_env,
            )
            .await?;
            let fresh = reauth_node(&docker, mesh_id, node).await?;
            let v = regenesis_status(&fresh).await?;
            crossings.push((
                node.node_id,
                v["phase"].as_str() == Some("normal"),
                v["epoch"].as_str() == Some("2"),
                ordinal_map(&v) == heads,
            ));
            fresh_nodes.push(fresh);
        }
        let all_crossed = crossings.iter().all(|(_, p, e, o)| *p && *e && *o);
        print_and_add_check(
            &mut result,
            Check {
                name: "Every node crosses: epoch 2, chains at head (storage through the lifecycle steps)"
                    .to_string(),
                passed: all_crossed,
                detail: Some(format!(
                    "(node, phase, epoch, ordinals): {crossings:?}; heads {heads:?}; {:.0}s",
                    crossing_started.elapsed().as_secs_f64()
                )),
            },
        );
        if !all_crossed {
            return Ok(result);
        }

        // 4. The drain. The backfill put every blob at the sentinel goal;
        //    the first transition and declare page start it; the pane's
        //    converged predicate ends it. Confirmation evidence is a row
        //    disk-verified within the recency window, and the old regime's
        //    rows carry no verified_height at all — so the drain cannot
        //    confirm anything until the first disk-truth sweep has run
        //    (the 30-minute cron in production; the runbook kicks it right
        //    after the flip, as this does). Then the fulfillment pass on
        //    the 5-minute tick confirms in batches. Deadline: two ticks'
        //    worth of cron slack plus a per-blob allowance; progress is
        //    sampled so a stall is diagnosable.
        for node in &fresh_nodes {
            let (status, body) =
                post_json(node, "/api/maintenance/fragment-inventory-self-check", None).await?;
            anyhow::ensure!(
                status == 200,
                "post-flip sweep on node {}: {status} {body}",
                node.node_id
            );
        }
        let deadline = Duration::from_secs(600 + 2 * blobs as u64);
        let drain_started = Instant::now();
        let mut last: Vec<(u32, bool, u64, u64, u64)> = Vec::new();
        let mut converged_at: Option<Duration> = None;
        let mut samples: Vec<String> = Vec::new();
        while drain_started.elapsed() < deadline {
            last.clear();
            for node in &fresh_nodes {
                let (c, owed, in_flight, confirmed) =
                    match get_json(node, "/api/views/network-resilience").await {
                        Ok(v) => lifecycle(&v),
                        Err(_) => (false, u64::MAX, u64::MAX, 0),
                    };
                last.push((node.node_id, c, owed, in_flight, confirmed));
            }
            if last
                .iter()
                .all(|(_, c, owed, in_flight, _)| *c && *owed == 0 && *in_flight == 0)
            {
                converged_at = Some(drain_started.elapsed());
                break;
            }
            if samples.len() < 12 {
                samples.push(format!(
                    "{:.0}s owed/in-flight {:?}",
                    drain_started.elapsed().as_secs_f64(),
                    last.iter()
                        .map(|(_, _, o, i, _)| (*o, *i))
                        .collect::<Vec<_>>()
                ));
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Catch-up drain converges on every node (owed 0, in flight 0, converged)"
                    .to_string(),
                passed: converged_at.is_some(),
                detail: Some(match converged_at {
                    Some(t) => format!(
                        "after {:.0}s; (node, converged, owed, in_flight, confirmed): {last:?}",
                        t.as_secs_f64()
                    ),
                    None => format!(
                        "NOT converged after {:.0}s; last {last:?}; samples {samples:?}",
                        deadline.as_secs_f64()
                    ),
                }),
            },
        );
        if converged_at.is_none() {
            return Ok(result);
        }

        // 5. Disk truth crossed too. Rows the drain's pulls created after
        //    the post-flip sweep are verified by the puller's prompt
        //    attestation, but a prompt attestation can land before the
        //    self-check that creates the rows (seen 2026-09-27: ~15% of the
        //    origin's rows unverified right after convergence), so the
        //    sweep — the 30-minute cron in production — is what settles
        //    every row. Run it once more, then assert.
        for node in &fresh_nodes {
            let (status, body) =
                post_json(node, "/api/maintenance/fragment-inventory-self-check", None).await?;
            anyhow::ensure!(
                status == 200,
                "sweep on node {}: {status} {body}",
                node.node_id
            );
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        let mut truth = Vec::new();
        for node in &fresh_nodes {
            let v = get_json(node, "/api/views/network-resilience").await?;
            let m = &v["storage"]["verification"]["mesh"];
            truth.push((
                node.node_id,
                m["fresh"].as_u64().unwrap_or(0),
                m["never"].as_u64().unwrap_or(u64::MAX),
                m["suspect"].as_u64().unwrap_or(u64::MAX),
            ));
        }
        let all_fresh = truth
            .iter()
            .all(|(_, fresh, never, suspect)| *fresh > 0 && *never == 0 && *suspect == 0);
        print_and_add_check(
            &mut result,
            Check {
                name: "Every inventory row is disk-verified after the drain, none suspect"
                    .to_string(),
                passed: all_fresh,
                detail: Some(format!("(node, fresh, never, suspect): {truth:?}")),
            },
        );

        // 6. The bytes crossed: every surviving file, from every node,
        //    byte-identical to what was written under the old regime.
        let mut intact = 0usize;
        let mut first_failure = None;
        for (path, bytes) in &expected {
            match download_file_from_all_nodes_with_timeout(
                &fresh_nodes,
                path,
                Duration::from_secs(30),
            )
            .await
            {
                Ok(downloads) if downloads.iter().all(|d| d == bytes) => intact += 1,
                Ok(_) => {
                    first_failure.get_or_insert(format!("{path}: content differs"));
                }
                Err(e) => {
                    first_failure.get_or_insert(format!("{path}: {e}"));
                }
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: format!(
                    "All {} surviving files download byte-identical from every node",
                    expected.len()
                ),
                passed: intact == expected.len(),
                detail: Some(match &first_failure {
                    Some(f) => format!("{intact}/{}; first failure: {f}", expected.len()),
                    None => format!("{intact}/{}", expected.len()),
                }),
            },
        );

        // 7. Coherent everywhere.
        let tip = decided_height(&fresh_nodes[0]).await.unwrap_or(seal_height);
        let (converged, heights) = wait_for_convergence(&fresh_nodes, tip, 120).await;
        let snapshots = fetch_state_snapshots(&fresh_nodes).await?;
        let (coherent, detail) = coherence(&snapshots);
        print_and_add_check(
            &mut result,
            Check {
                name: "Crossed mesh is coherent at one height".to_string(),
                passed: converged && coherent,
                detail: Some(format!("heights: {heights:?}, {detail}")),
            },
        );

        result.details = format!(
            "{} blobs crossed from {ENFORCEMENT_OLD_RELEASE}; drain converged in {:.0}s; {} files intact",
            blobs,
            converged_at.map(|t| t.as_secs_f64()).unwrap_or(0.0),
            intact
        );
        Ok(result)
    }
}
