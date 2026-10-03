use anyhow::{Context, Result};
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{download_file_with_timeout, get_fragment_distribution, upload_file};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

/// The default 20 GiB pull floor, over a probe that sees 1 GiB free.
const FLOOR: &str = "21474836480";
const FREE: &str = "1073741824";

/// A node below its pull floor takes on no copies but keeps serving, and
/// pulls its share once it is back above it (2026-10-03: the macbook filled
/// its disk pulling; RFC-STORAGE-003 pull floor). Node 2 of a 3-node mesh
/// is recreated seeing 1 GiB free under a 20 GiB floor (the floor is
/// clamped to the volume, so a huge floor alone no longer stages "full"
/// on a roomy host), a file is uploaded on node 0, node 1
/// pulls its classes while node 2 holds back (paused in its tick report,
/// the blob unconfirmed, the file still readable through it); node 2 is
/// then recreated without the override and the blob confirms.
pub struct PullFloorHoldsBack;

fn holders(dist: &crate::tests::files::FileFragmentDistribution, node: i32) -> usize {
    dist.fragments
        .iter()
        .filter(|f| f.nodes_with_fragment.contains(&node))
        .count()
}

async fn paused_since(client: &reqwest::Client, node: &NodeInfo) -> Option<i64> {
    let url = format!(
        "https://{}:{}/api/maintenance/policy-tick",
        node.ip_address, node.port
    );
    let report: serde_json::Value = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    report["planner"]["scheduler"]["space"]["paused_since"].as_i64()
}

impl TestScenario for PullFloorHoldsBack {
    fn name(&self) -> &'static str {
        "pull-floor-holds-back"
    }

    fn description(&self) -> &'static str {
        "A node below its pull floor serves but takes on no copies, then pulls its share once above it"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() >= 3, "needs 3 nodes");
        let client = crate::insecure_client();
        let docker = crate::sys::connect().context("docker connect")?;

        // 1. Node 2 comes back "full" for replica writes.
        super::regenesis::recreate_node_with_env(
            &docker,
            mesh_id,
            2,
            None,
            &[
                ("HOPNET_PULL_MIN_FREE_BYTES", FLOOR),
                ("HOPNET_PULL_TEST_FREE_BYTES", FREE),
            ],
        )
        .await
        .context("recreate node 2 below its pull floor")?;
        let node2 = super::regenesis::reauth_node(&docker, mesh_id, &nodes[2])
            .await
            .context("reauth node 2")?;

        // 2. Upload on node 0.
        let (path, filename, full_path) = ("/", "pull_floor.dat", "/pull_floor.dat");
        let contents: Vec<u8> = (0..2 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
        let content_hash = blake3::hash(&contents);
        upload_file(&nodes[0], path, filename, contents).await?;

        // 3. Node 1 pulls its share while node 2 holds back.
        let deadline = Instant::now() + Duration::from_secs(120);
        let (mut n1, mut n2, mut placed) = (0, 0, None);
        while Instant::now() < deadline {
            if let Ok(dist) = get_fragment_distribution(&nodes[0], full_path).await {
                (n1, n2, placed) = (holders(&dist, 1), holders(&dist, 2), dist.placement_height);
                if n1 > 0 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        // Give node 2 the same time again to (wrongly) pull anything.
        tokio::time::sleep(Duration::from_secs(20)).await;
        if let Ok(dist) = get_fragment_distribution(&nodes[0], full_path).await {
            (n1, n2, placed) = (holders(&dist, 1), holders(&dist, 2), dist.placement_height);
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Node 1 pulls its classes; node 2 takes on none".to_string(),
                passed: n1 > 0 && n2 == 0,
                detail: Some(format!("node 1 holds {n1}, node 2 holds {n2}")),
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "The blob stays unconfirmed while node 2 holds back".to_string(),
                passed: placed.is_none(),
                detail: placed.map(|h| format!("placed at {h}")),
            },
        );
        let paused = paused_since(&client, &node2).await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Node 2's tick report says it is holding back".to_string(),
                passed: paused.is_some(),
                detail: None,
            },
        );

        // 4. Node 2 still serves: the file reads through it.
        let read = download_file_with_timeout(&node2, full_path, Duration::from_secs(60)).await;
        let read_ok = read.as_ref().is_ok_and(|d| blake3::hash(d) == content_hash);
        print_and_add_check(
            &mut result,
            Check {
                name: "The file is readable through node 2 while it holds back".to_string(),
                passed: read_ok,
                detail: read.err().map(|e| format!("{e:?}")),
            },
        );

        // 5. Back above the floor: node 2 pulls its share and the blob confirms.
        super::regenesis::recreate_node_with_env(&docker, mesh_id, 2, None, &[])
            .await
            .context("recreate node 2 without the override")?;
        let _node2 = super::regenesis::reauth_node(&docker, mesh_id, &node2)
            .await
            .context("reauth node 2 again")?;
        let deadline = Instant::now() + Duration::from_secs(240);
        while Instant::now() < deadline {
            if let Ok(dist) = get_fragment_distribution(&nodes[0], full_path).await {
                (n2, placed) = (holders(&dist, 2), dist.placement_height);
                if placed.is_some() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Without the floor, node 2 pulls its share and the blob confirms".to_string(),
                passed: n2 > 0 && placed.is_some(),
                detail: Some(format!("node 2 holds {n2}, placement {placed:?}")),
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
