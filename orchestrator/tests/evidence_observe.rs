use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::tests::persistence::stop_node;
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};

/// Evidence-layer observation test (RFC-CONSENSUS-002 S3).
///
/// Every node's per-peer evidence goes bright through passive traffic and
/// deadline probes; killing a node drives its age past T_unresponsive on
/// every survivor, the live estimate drops, and the band compresses.
/// Genesis seeds a tiny probe ladder (probe_base=2, grace=1 ⇒
/// t_unresponsive fast = 5 s) and MAJORITY quorum — 3 nodes give H = 1
/// (Fast) so the kill produces an OBSERVABLE band shift to Cliff; under
/// default BFT quorum(3) = 3 the mesh would already sit at the Cliff.
pub struct EvidenceObserve;

pub(crate) async fn fetch_evidence(client: &Client, node: &NodeInfo) -> Result<serde_json::Value> {
    let url = format!(
        "http://{}:{}/consensus/evidence",
        node.ip_address, node.port
    );
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    anyhow::ensure!(resp.status().is_success(), "HTTP {}", resp.status());
    Ok(resp.json().await?)
}

pub(crate) fn node_entry<'a>(doc: &'a serde_json::Value, node_id: i64) -> Option<&'a serde_json::Value> {
    doc["nodes"]
        .as_array()?
        .iter()
        .find(|n| n["node_id"].as_i64() == Some(node_id) && n["self"] != true)
}

impl TestScenario for EvidenceObserve {
    fn name(&self) -> &'static str {
        "evidence-observe"
    }

    fn description(&self) -> &'static str {
        "Per-peer evidence goes bright, then a killed node ages out and the band compresses"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        let client = Client::new();
        anyhow::ensure!(nodes.len() == 3, "evidence-observe expects a 3-node mesh");

        println!("\nRunning evidence-observe checks:");

        // 1. Everyone bright on every observer: peers fresh (age below the
        // observer's own t_unresponsive), heights known, band Fast at H=1.
        let mut all_fresh = false;
        let mut band_ok = false;
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let mut fresh = true;
            let mut bands = true;
            for node in nodes {
                let Ok(doc) = fetch_evidence(&client, node).await else {
                    fresh = false;
                    break;
                };
                let t_unresp = doc["summary"]["t_unresponsive_ms"].as_u64().unwrap_or(0);
                let summary_ok = doc["summary"]["band"] == "Fast"
                    && doc["summary"]["headroom"].as_i64() == Some(1);
                bands = bands && summary_ok;
                for peer in nodes {
                    if peer.node_id == node.node_id {
                        continue;
                    }
                    let Some(entry) = node_entry(&doc, peer.node_id as i64) else {
                        fresh = false;
                        break;
                    };
                    let age_ok = entry["age_ms"].as_u64().unwrap_or(u64::MAX) < t_unresp;
                    let height_known = entry["last_known_height"].is_i64()
                        || entry["last_known_height"].is_u64();
                    if !age_ok || !height_known {
                        fresh = false;
                        break;
                    }
                }
                if !fresh {
                    break;
                }
            }
            if fresh && bands {
                all_fresh = true;
                band_ok = true;
                break;
            }
            sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "All peers bright on every observer (fresh age + known height)".to_string(),
                passed: all_fresh,
                detail: None,
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline band Fast at headroom 1 (majority, v=3)".to_string(),
                passed: band_ok,
                detail: None,
            },
        );

        // 2. Kill node 2.
        let docker = Docker::connect_with_local_defaults()?;
        let victim = &nodes[2];
        let stop_ok = stop_node(&docker, mesh_id, victim.node_id).await.is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Stop node {}", victim.node_id),
                passed: stop_ok,
                detail: None,
            },
        );

        // 3. Survivors: victim ages past t_unresponsive, live drops to 2,
        // headroom 0, band Cliff, and the deadline probes accrued the S4
        // attestation floor (>= 2 unanswered probes).
        let survivors = [&nodes[0], &nodes[1]];
        let mut aged_out = false;
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let mut all = true;
            for node in survivors {
                let Ok(doc) = fetch_evidence(&client, node).await else {
                    all = false;
                    break;
                };
                let t_unresp = doc["summary"]["t_unresponsive_ms"].as_u64().unwrap_or(0);
                let Some(entry) = node_entry(&doc, victim.node_id as i64) else {
                    all = false;
                    break;
                };
                let ok = entry["age_ms"].as_u64().unwrap_or(0) > t_unresp
                    && entry["probes_since_contact"].as_u64().unwrap_or(0) >= 2
                    && doc["summary"]["live"].as_u64() == Some(2)
                    && doc["summary"]["headroom"].as_i64() == Some(0)
                    && doc["summary"]["band"] == "Cliff";
                if !ok {
                    all = false;
                    break;
                }
            }
            if all {
                aged_out = true;
                break;
            }
            sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Survivors: victim aged out (probes >= 2), live=2, H=0, band Cliff"
                    .to_string(),
                passed: aged_out,
                detail: None,
            },
        );

        // 4. Independence: the survivors still see each other fresh.
        let mut mutual_fresh = true;
        for (a, b) in [(survivors[0], survivors[1]), (survivors[1], survivors[0])] {
            let Ok(doc) = fetch_evidence(&client, a).await else {
                mutual_fresh = false;
                break;
            };
            let t_unresp = doc["summary"]["t_unresponsive_ms"].as_u64().unwrap_or(0);
            let fresh = node_entry(&doc, b.node_id as i64)
                .and_then(|e| e["age_ms"].as_u64())
                .is_some_and(|age| age < t_unresp);
            mutual_fresh = mutual_fresh && fresh;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Survivors still fresh to each other (one death does not stain the living)"
                    .to_string(),
                passed: mutual_fresh,
                detail: None,
            },
        );

        Ok(result)
    }
}
