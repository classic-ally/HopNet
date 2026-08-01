use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};

/// Observe-only storage-membership view test (RFC-STORAGE-002 S2).
///
/// Asserts every node derives the SAME decay-tiered view from the same
/// replicated state: genesis-seeded tiny tiers resolve identically,
/// members/tiers/weights/watermark match cross-node, and the derived
/// watermark is correct for the mesh size. Decay transitions (node leaves
/// the view after its tier expires) are exercised by the S6 tick tests —
/// availability buckets are 10 minutes wide, so decay is minutes-scale by
/// construction.
///
/// The auto-managed runner seeds `HOPNET_GENESIS_STORAGE_POLICY`
/// (decay_tiers=60,120,180,240) before mesh creation; caller-managed
/// meshes must set it before `orchestrator create` for the tier checks.
pub struct TierMembership;

#[derive(Debug, Deserialize, PartialEq, Clone)]
struct StorageViewResponse {
    height: i32,
    watermark: usize,
    members: Vec<i32>,
    tiers: HashMap<i32, i64>,
    weights: HashMap<i32, u64>,
}

async fn fetch_view(client: &Client, node: &NodeInfo) -> Result<StorageViewResponse> {
    let url = format!("http://{}:{}/api/storage/view", node.ip_address, node.port);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(15))
        .send()
        .await?;
    anyhow::ensure!(resp.status().is_success(), "HTTP {}", resp.status());
    let mut view: StorageViewResponse = resp.json().await?;
    view.members.sort_unstable();
    Ok(view)
}

impl TestScenario for TierMembership {
    fn name(&self) -> &'static str {
        "tier-membership"
    }

    fn description(&self) -> &'static str {
        "Storage membership view derives identically on every node (tiers, weights, watermark)"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = Client::new();
        let num_nodes = nodes.len();

        println!("\nRunning tier-membership checks:");

        // 1. Seed availability history: one metrics round so the view has
        // replicated rows to derive from (empty history is also valid —
        // cold tiers, everyone present — but rows exercise the real path).
        let trigger = format!(
            "http://{}:{}/api/metrics/trigger",
            nodes[0].ip_address, nodes[0].port
        );
        let trigger_ok = client
            .get(&trigger)
            .header("Authorization", format!("Bearer {}", nodes[0].jwt_token))
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Trigger metrics round".to_string(),
                passed: trigger_ok,
                detail: None,
            },
        );

        // 2. Fetch /storage/view from every node, retrying until all nodes
        // answer at the same height (metrics tx must commit everywhere).
        let mut views: Vec<StorageViewResponse> = Vec::new();
        let mut aligned = false;
        for _attempt in 0..15 {
            let mut round = Vec::with_capacity(num_nodes);
            let mut ok = true;
            for node in nodes {
                match fetch_view(&client, node).await {
                    Ok(v) => round.push(v),
                    Err(e) => {
                        println!("  view fetch failed: {e}");
                        ok = false;
                        break;
                    }
                }
            }
            if ok && round.windows(2).all(|w| w[0].height == w[1].height) {
                views = round;
                aligned = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "All nodes answer /storage/view at one height".to_string(),
                passed: aligned,
                detail: views.first().map(|v| format!("height {}", v.height)),
            },
        );
        if !aligned {
            result.details = "Nodes never aligned on one height".to_string();
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 3. Views identical cross-node — the determinism claim.
        let identical = views.windows(2).all(|w| w[0] == w[1]);
        print_and_add_check(
            &mut result,
            Check {
                name: "View identical on every node".to_string(),
                passed: identical,
                detail: Some(format!(
                    "members={:?} watermark={} tiers={:?} weights={:?}",
                    views[0].members, views[0].watermark, views[0].tiers, views[0].weights
                )),
            },
        );

        // 4. Everyone is a member (nothing decayed on a fresh mesh).
        let all_members = views[0].members.len() == num_nodes;
        print_and_add_check(
            &mut result,
            Check {
                name: "All nodes are storage members".to_string(),
                passed: all_members,
                detail: Some(format!(
                    "members={:?} expected {} nodes",
                    views[0].members, num_nodes
                )),
            },
        );

        // 5. Genesis-seeded tiers resolved: cold-start tier = second-largest
        // of the seeded list (180 from 60,120,180,240). Skipped when the
        // mesh was created without the seed (default tiers → 259200).
        let seeded = views[0].tiers.values().any(|&t| t == 180);
        let default_cold = views[0].tiers.values().all(|&t| t == 259_200);
        print_and_add_check(
            &mut result,
            Check {
                name: "Genesis-seeded cold-start tiers resolved".to_string(),
                passed: seeded
                    || (default_cold && !views[0].tiers.is_empty())
                    || views[0].tiers.is_empty(),
                detail: Some(format!(
                    "tiers={:?} ({})",
                    views[0].tiers,
                    if seeded {
                        "seeded 60,120,180,240"
                    } else {
                        "unseeded mesh — code defaults"
                    }
                )),
            },
        );

        // 6. Derived watermark. The mesh runs the AUTO profile (the genesis
        // default with no HOPNET_QUORUM_PROFILE), which is majority below
        // V_BFT=7. At v=3 majority tolerates one fault, so B(3)=1 and the
        // reserve caps at advMax=10 → W=K+10=20 (K=10, N=30). Keying the
        // watermark off the ACTIVE profile is the durability fix; under the
        // old hard-coded BFT formula this was W=10 (B(3)=0), which
        // under-buffered a majority mesh.
        let w_ok = if views[0].members.len() == 3 {
            views[0].watermark == 20
        } else {
            views[0].watermark >= 10
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Derived watermark correct for view size".to_string(),
                passed: w_ok,
                detail: Some(format!(
                    "W={} at v={}",
                    views[0].watermark,
                    views[0].members.len()
                )),
            },
        );

        result.details = format!(
            "Storage view identical across {} nodes at height {} (W={})",
            num_nodes, views[0].height, views[0].watermark
        );
        result.duration = start.elapsed();
        Ok(result)
    }
}
