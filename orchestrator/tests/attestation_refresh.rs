use anyhow::{Context, Result};
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::upload_files_multi;
use crate::tests::lifecycle_cutover::lifecycle;
use crate::tests::regenesis::{decided_height, get_json, post_json};
use crate::tests::{Check, TestResult, TestScenario, decided_functions, print_and_add_check};

/// The wall-clock cap this test's mesh runs under
/// (`HOPNET_STORAGE_ATTEST_MAX_AGE_SECS`, set by `mesh_creation_env`):
/// short enough to wait out, long enough that a sweep right after another
/// finds every stamp fresh (a fragment's own cap sits up to a quarter below).
pub(crate) const CAP_SECS: u64 = 60;

/// The attestation refresh end to end: a node's sweep attests only the
/// stamps about to expire. Every node sweeps once to settle its stamps,
/// again at once and must find every present fragment fresh, then after
/// the wall-clock cap and must refresh them. Each run's own report is
/// checked, so the background walker's pages cannot muddy the counts.
/// The height threshold (thousands of heights) is out of a mesh test's
/// reach; `sweep_sim` covers it.
pub struct AttestationRefresh;

/// Deterministic per-file bytes, a few hundred KiB so every node holds
/// fragments.
fn contents(i: usize) -> Vec<u8> {
    (0..200 * 1024 + i * 4096)
        .map(|b| ((b * 13 + i * 29) % 251) as u8)
        .collect()
}

/// The sweep's counts from one self-check report.
#[derive(Debug, Clone, Copy)]
struct Counts {
    present: u64,
    flags: u64,
    age: u64,
    cap: u64,
    fresh: u64,
    refresh_pages: u64,
}

impl Counts {
    fn due(&self) -> u64 {
        self.flags + self.age + self.cap
    }
}

impl std::fmt::Display for Counts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} present: {}/{}/{} flags/age/cap, {} fresh, {} refresh pages",
            self.present, self.flags, self.age, self.cap, self.fresh, self.refresh_pages
        )
    }
}

async fn self_check(node: &NodeInfo) -> Result<Counts> {
    let (status, body) = post_json(node, "/api/maintenance/fragment-inventory-self-check", None)
        .await
        .with_context(|| format!("self-check on node {}", node.node_id))?;
    anyhow::ensure!(
        status == 200,
        "self-check on node {}: {status} {body}",
        node.node_id
    );
    let report: serde_json::Value = serde_json::from_str(&body)?;
    let n = |k: &str| report[k].as_u64().unwrap_or(0);
    Ok(Counts {
        present: n("present"),
        flags: n("attested_flags"),
        age: n("attested_age"),
        cap: n("attested_cap"),
        fresh: n("refresh_fresh"),
        refresh_pages: n("refresh_pages"),
    })
}

async fn self_check_all(nodes: &[NodeInfo]) -> Result<Vec<Counts>> {
    let tasks: Vec<_> = nodes
        .iter()
        .cloned()
        .map(|node| tokio::spawn(async move { self_check(&node).await }))
        .collect();
    let mut counts = Vec::with_capacity(tasks.len());
    for task in tasks {
        counts.push(task.await.map_err(|e| anyhow::anyhow!("join: {e}"))??);
    }
    Ok(counts)
}

fn per_node(counts: &[Counts]) -> String {
    counts
        .iter()
        .enumerate()
        .map(|(i, c)| format!("node {i}: {c}"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Decided `attest_fragments` transactions in `(from, to]`, read in
/// ranges the blocks route accepts.
async fn attestations_between(node: &NodeInfo, from: u64, to: u64) -> Result<usize> {
    let mut count = 0;
    let mut lo = from + 1;
    while lo <= to {
        let hi = (lo + 999).min(to);
        count += decided_functions(node, lo, hi)
            .await?
            .iter()
            .flat_map(|(_, functions)| functions)
            .filter(|f| f.as_str() == "attest_fragments")
            .count();
        lo = hi + 1;
    }
    Ok(count)
}

impl TestScenario for AttestationRefresh {
    fn name(&self) -> &'static str {
        "attestation-refresh"
    }

    fn description(&self) -> &'static str {
        "A sweep attests only stamps about to expire: none right after another sweep, all once past the wall-clock cap"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        // 1. Upload, then wait for the mesh to place and confirm it all.
        let files: Vec<(String, Vec<u8>)> = (0..12)
            .map(|i| (format!("refresh_{i:02}.dat"), contents(i)))
            .collect();
        upload_files_multi(&nodes[0], "/", files).await?;
        let deadline = Instant::now() + Duration::from_secs(240);
        let converged = loop {
            let mut all = true;
            for node in nodes {
                let view = get_json(node, "/api/views/network-resilience").await?;
                let (converged, owed, in_flight, _) = lifecycle(&view);
                all &= converged && owed == 0 && in_flight == 0;
            }
            if all || Instant::now() > deadline {
                break all;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Uploads placed and confirmed on every node".to_string(),
                passed: converged,
                detail: (!converged).then(|| "lifecycle did not converge in 240 s".to_string()),
            },
        );
        if !converged {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 2. Settle: whatever the pull path left unstamped gets its stamp.
        let settled = self_check_all(nodes).await?;
        let holds = settled.iter().all(|c| c.present > 0);
        print_and_add_check(
            &mut result,
            Check {
                name: "Every node sweeps fragments it holds".to_string(),
                passed: holds,
                detail: Some(per_node(&settled)),
            },
        );

        // 3. At once again: every stamp is seconds old, nothing is due.
        let again = self_check_all(nodes).await?;
        let quiet = again
            .iter()
            .all(|c| c.present > 0 && c.due() == 0 && c.fresh == c.present);
        print_and_add_check(
            &mut result,
            Check {
                name: "A sweep right after another attests nothing".to_string(),
                passed: quiet,
                detail: Some(per_node(&again)),
            },
        );

        // 4. Past the cap: every node refreshes, as refresh pages that
        //    decide on the chain.
        tokio::time::sleep(Duration::from_secs(CAP_SECS + 5)).await;
        let before = decided_height(&nodes[0]).await?;
        let capped = self_check_all(nodes).await?;
        let after = decided_height(&nodes[0]).await?;
        let refreshed = capped.iter().all(|c| c.cap > 0 && c.refresh_pages > 0);
        print_and_add_check(
            &mut result,
            Check {
                name: "A sweep past the wall-clock cap refreshes the stamps".to_string(),
                passed: refreshed,
                detail: Some(per_node(&capped)),
            },
        );
        let decided = attestations_between(&nodes[0], before, after).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "The refresh pages decide".to_string(),
                passed: decided > 0,
                detail: Some(format!(
                    "{decided} attest_fragments in heights {}..={after}",
                    before + 1
                )),
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
