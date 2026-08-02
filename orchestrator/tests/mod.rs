use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// Re-export NodeInfo from main for use in tests
pub use crate::NodeInfo;

// File operation helpers
pub mod files;

// Test implementations
mod consensus_queue;
mod db_pragma_bench;
mod device_tokens;
mod eviction;
mod documentprovider_write;
mod evidence_voteout;
mod file_upload;
mod fileprovider_device_token;
mod fragment_distribution;
pub(crate) mod evidence_observe;
mod auto_seam;
pub(crate) mod mesh_growth;
pub(crate) mod graceful_leave;
mod three_timescales;
mod vote_out;
mod fragment_health_check;
mod import;
mod iroh_ping;
mod iroh_reject_unknown;
mod malachite;
mod metrics;
mod multi_size_files;
pub(crate) mod multi_user;
mod orphan_cleanup;
mod performance;
pub(crate) mod persistence;
mod post_files_mixed;
mod post_files_shape;
mod range_download;
mod recents;
pub(crate) mod reencode;
pub(crate) mod regenesis;
mod sharing;
mod takeout;
mod tier_membership;
mod upload_and_confirm_placement;

/// Represents the result of a test scenario execution
#[derive(Debug)]
pub struct TestResult {
    pub passed: bool,
    pub details: String,
    pub checks: Vec<Check>,
    pub duration: Duration,
}

impl TestResult {
    pub fn new() -> Self {
        Self {
            passed: true,
            details: String::new(),
            checks: Vec::new(),
            duration: Duration::default(),
        }
    }

    pub fn add_check(&mut self, check: Check) {
        if !check.passed {
            self.passed = false;
        }
        self.checks.push(check);
    }
}

/// Represents a single validation check within a test
#[derive(Debug)]
pub struct Check {
    pub name: String,
    pub passed: bool,
    pub detail: Option<String>,
}

/// Helper to print and add a check in real-time (shared across all tests)
pub fn print_and_add_check(result: &mut TestResult, check: Check) {
    let status = if check.passed { "✅" } else { "❌" };
    print!("  {} {}", status, check.name);
    if let Some(detail) = &check.detail {
        print!(" - {}", detail);
    }
    println!();
    result.add_check(check);
}

/// Test scenario trait - all tests must implement this
pub trait TestScenario: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], flags: &[String]) -> Result<TestResult>;
}

/// Mesh-creation env for tests needing genesis-seeded config (applied by
/// the auto-managed runner BEFORE create_mesh; caller-managed meshes must
/// export these before `orchestrator create` for the same checks).
pub fn mesh_creation_env(test_name: &str) -> Vec<(&'static str, &'static str)> {
    match test_name {
        "consensus-bft-quorum-loss" => vec![
            ("HOPNET_QUORUM_PROFILE", "bft"),
            // 1->4 formation batch is exposed; seed a small span so the
            // 4-node BFT mesh forms.
            ("HOPNET_GENESIS_CONSENSUS_POLICY", "s_full=6;p_prove=6"),
        ],
        "auto-seam" => vec![
            ("HOPNET_GENESIS_CONSENSUS_POLICY", "probe_base=2;grace=1;s_full=6;p_prove=6"),
        ],
        "mesh-growth" => vec![
            ("HOPNET_GENESIS_CONSENSUS_POLICY", "probe_base=2;grace=1;s_full=6;p_prove=6"),
            // AUTO (default): majority below v=7 — the growth stays in the
            // majority region, no forcing.
        ],
        "vote-out-after-kill" => vec![
            (
                "HOPNET_GENESIS_CONSENSUS_POLICY",
                "probe_base=2;grace=1;s_full=6;p_prove=6",
            ),
            // AUTO (default): majority at v=3, so the kill leaves a live
            // quorum — no forcing needed.
        ],
        // Same seeding as vote-out: the 3-seat formation batch needs the
        // shortened spans. No grace override: the scenario detects the
        // seal by the exit code, never by racing HTTP against the exit.
        "regenesis-restart" => vec![(
            "HOPNET_GENESIS_CONSENSUS_POLICY",
            "probe_base=2;grace=1;s_full=6;p_prove=6",
        )],
        // Upgrade-target boundary: the staged override lets the mesh claim
        // 2026.8.1 staged (the start precondition) without a second image;
        // nodes then park awaiting-upgrade and are recreated one by one
        // with the running-version override — the "binary swap".
        "regenesis-awaiting-upgrade" => vec![
            (
                "HOPNET_GENESIS_CONSENSUS_POLICY",
                "probe_base=2;grace=1;s_full=6;p_prove=6",
            ),
            ("HOPNET_UPGRADE_STAGED_OVERRIDE", "2026.8.1"),
        ],
        // REGRESSION FIX (S4): the S_min gate makes the BFT rejoin seat
        // EXPOSED (quorum(3)-quorum(2)=1) => req_span = s_full; the
        // default 30 min would refuse the rejoin inside the test window.
        "graceful-leave" => vec![(
            "HOPNET_GENESIS_CONSENSUS_POLICY",
            "probe_base=2;grace=1;s_full=6",
        )],
        "evidence-observe" => vec![
            (
                "HOPNET_GENESIS_CONSENSUS_POLICY",
                // s_full/p_prove seeded so the 3-node mesh FORMS (the
                // formation batch is exposed) — a self-set policy drops the
                // global default seed.
                "probe_base=2;grace=1;s_full=6;p_prove=6",
            ),
            // AUTO (default) is majority at v=3: quorum 2, H=1 (Fast) ->
            // kill one -> H=0 (Cliff), an observable band shift.
        ],
        "tier-membership" => vec![(
            "HOPNET_GENESIS_STORAGE_POLICY",
            "decay_tiers=60,120,180,240",
        )],
        "re-encode-after-departure" => vec![
            (
                "HOPNET_GENESIS_STORAGE_POLICY",
                "decay_tiers=15,30,60,120;availability_step_secs=5",
            ),
            // AUTO (default) is majority at v=3: 2 of 3 alive keeps
            // committing metrics/self-check txs.
        ],
        // Both planes on one kill: consensus knobs match vote-out (fast
        // vote-out); storage cold tier = 90s (second-largest) — larger than
        // the vote-out budget, so the decoupling snapshot is race-immune even
        // if a background metrics round starts the absence clock at kill time.
        "three-timescales" => vec![
            (
                "HOPNET_GENESIS_CONSENSUS_POLICY",
                "probe_base=2;grace=1;s_full=6;p_prove=6",
            ),
            (
                "HOPNET_GENESIS_STORAGE_POLICY",
                "decay_tiers=15,30,90,180;availability_step_secs=5",
            ),
        ],
        // Wider probe base (3 vs the sibling 2) widens the observation
        // window between first-visible compression and removal to ~4-5s.
        "evidence-drives-voteout" => vec![(
            "HOPNET_GENESIS_CONSENSUS_POLICY",
            "probe_base=3;grace=1;s_full=6;p_prove=6",
        )],
        _ => vec![],
    }
}

/// Preferred auto-mesh node count for tests whose premise needs a specific
/// size (RFC-CONSENSUS-002: BFT meshes form 1->4->7; even-majority meshes
/// keep a pooled spare). None = use the CLI --auto-nodes default.
pub fn preferred_auto_nodes(test_name: &str) -> Option<u32> {
    match test_name {
        // 4-node BFT: forms via a batch of 3 (1->4); quorum(4)=3, so
        // killing 2 loses quorum.
        "consensus-bft-quorum-loss" => Some(4),
        // 6 nodes: forms in the majority region (seats 5 + pooled spare, or
        // 6); the test adds the 7th itself to watch the seam get crossed.
        "auto-seam" => Some(6),
        // Boundary scenarios are written against a 3-node mesh.
        "regenesis-restart" | "regenesis-awaiting-upgrade" => Some(3),
        _ => None,
    }
}

/// Run a test by name
pub async fn run_test_by_name(
    mesh_id: u32,
    name: &str,
    nodes: &[NodeInfo],
    flags: &[String],
) -> Result<TestResult> {
    match name {
        "file-upload-consistency" => {
            file_upload::FileUploadConsistency
                .run(mesh_id, nodes, flags)
                .await
        }
        "fragment-distribution" => {
            fragment_distribution::FragmentDistribution
                .run(mesh_id, nodes, flags)
                .await
        }
        "fragment-health-check" => {
            fragment_health_check::FragmentHealthCheck
                .run(mesh_id, nodes, flags)
                .await
        }
        "chunked-streaming-performance" => {
            performance::ChunkedStreamingPerformance
                .run(mesh_id, nodes, flags)
                .await
        }
        "multi-size-file-consistency" => {
            multi_size_files::MultiSizeFileConsistency
                .run(mesh_id, nodes, flags)
                .await
        }
        "restart-persistence" => {
            persistence::RestartPersistence
                .run(mesh_id, nodes, flags)
                .await
        }
        "device-token-consistency" => {
            device_tokens::DeviceTokenConsistency
                .run(mesh_id, nodes, flags)
                .await
        }
        "documentprovider-write-consistency" => {
            documentprovider_write::DocumentProviderWriteConsistency
                .run(mesh_id, nodes, flags)
                .await
        }
        "iroh-ping" => iroh_ping::IrohPing.run(mesh_id, nodes, flags).await,
        "iroh-reject-unknown" => {
            iroh_reject_unknown::IrohRejectUnknown
                .run(mesh_id, nodes, flags)
                .await
        }
        // Malachite protocol tests. The ones that expect progress with a
        // node down need `HOPNET_QUORUM_PROFILE=majority` set when the mesh
        // is created (auto-managed mode inherits the orchestrator's env).
        "consensus-leader-down" => {
            malachite::ConsensusLeaderDown
                .run(mesh_id, nodes, flags)
                .await
        }
        "consensus-lagging-catch-up" => {
            malachite::ConsensusLaggingCatchUp
                .run(mesh_id, nodes, flags)
                .await
        }
        "consensus-bft-quorum-loss" => {
            malachite::ConsensusBftQuorumLoss
                .run(mesh_id, nodes, flags)
                .await
        }
        "consensus-barrier-decide-window" => {
            malachite::ConsensusBarrierDecideWindow
                .run(mesh_id, nodes, flags)
                .await
        }
        "consensus-barrier-proposal-hold" => {
            malachite::ConsensusBarrierProposalHold
                .run(mesh_id, nodes, flags)
                .await
        }
        "metrics-collection" => metrics::MetricsCollection.run(mesh_id, nodes, flags).await,
        "eviction-under-pressure" => {
            eviction::EvictionUnderPressure
                .run(mesh_id, nodes, flags)
                .await
        }
        "re-encode-after-departure" => {
            reencode::ReencodeAfterDeparture
                .run(mesh_id, nodes, flags)
                .await
        }
        "auto-seam" => {
            auto_seam::AutoSeam.run(mesh_id, nodes, flags).await
        }
        "mesh-growth" => {
            mesh_growth::MeshGrowth.run(mesh_id, nodes, flags).await
        }
        "three-timescales" => {
            three_timescales::ThreeTimescales
                .run(mesh_id, nodes, flags)
                .await
        }
        "evidence-drives-voteout" => {
            evidence_voteout::EvidenceDrivesVoteout
                .run(mesh_id, nodes, flags)
                .await
        }
        "vote-out-after-kill" => {
            vote_out::VoteOutAfterKill
                .run(mesh_id, nodes, flags)
                .await
        }
        "regenesis-restart" => {
            regenesis::RegenesisRestart
                .run(mesh_id, nodes, flags)
                .await
        }
        "regenesis-awaiting-upgrade" => {
            regenesis::RegenesisAwaitingUpgrade
                .run(mesh_id, nodes, flags)
                .await
        }
        "evidence-observe" => {
            evidence_observe::EvidenceObserve
                .run(mesh_id, nodes, flags)
                .await
        }
        "graceful-leave" => {
            graceful_leave::GracefulLeave
                .run(mesh_id, nodes, flags)
                .await
        }
        "tier-membership" => {
            tier_membership::TierMembership
                .run(mesh_id, nodes, flags)
                .await
        }
        "multi-user-isolation" => {
            multi_user::MultiUserIsolation
                .run(mesh_id, nodes, flags)
                .await
        }
        "multi-user-sharing" => sharing::MultiUserSharing.run(mesh_id, nodes, flags).await,
        "multi-user-sharing-live-link" => {
            sharing::MultiUserSharingLiveLink
                .run(mesh_id, nodes, flags)
                .await
        }
        "consensus-queue-burst" => {
            consensus_queue::ConsensusQueueBurst
                .run(mesh_id, nodes, flags)
                .await
        }
        "consensus-queue-cross-node" => {
            consensus_queue::ConsensusQueueCrossNode
                .run(mesh_id, nodes, flags)
                .await
        }
        "consensus-queue-throughput" => {
            consensus_queue::ConsensusQueueThroughput
                .run(mesh_id, nodes, flags)
                .await
        }
        "orphan-cleanup" => {
            orphan_cleanup::OrphanCleanup
                .run(mesh_id, nodes, flags)
                .await
        }
        "device-token-session-bootstrap" => {
            fileprovider_device_token::DeviceTokenSessionBootstrap
                .run(mesh_id, nodes, flags)
                .await
        }
        "fileprovider-device-token-auth" => {
            fileprovider_device_token::FileProviderDeviceTokenAuth
                .run(mesh_id, nodes, flags)
                .await
        }
        "recents-ordering" => recents::RecentsOrdering.run(mesh_id, nodes, flags).await,
        "range-download" => {
            range_download::RangeDownload
                .run(mesh_id, nodes, flags)
                .await
        }
        "takeout-happy-path" => takeout::TakeoutHappyPath.run(mesh_id, nodes, flags).await,
        "import-create-active-conflict" => {
            import::ImportCreateActiveConflict
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-upload-happy-path" => {
            import::ImportUploadHappyPath
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-upload-version-rejected" => {
            import::ImportUploadVersionRejected
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-upload-missing-manifest" => {
            import::ImportUploadMissingManifest
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-upload-quota-exceeded" => {
            import::ImportUploadQuotaExceeded
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-extraction-happy-path" => {
            import::ImportExtractionHappyPath
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-extraction-hash-mismatch" => {
            import::ImportExtractionHashMismatch
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-creation-happy-path" => {
            import::ImportCreationHappyPath
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-creation-mixed-failure" => {
            import::ImportCreationMixedFailure
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-write-gate" => import::ImportWriteGate.run(mesh_id, nodes, flags).await,
        "import-status-counts" => import::ImportStatusCounts.run(mesh_id, nodes, flags).await,
        "import-unknown-projection-skipped" => {
            import::ImportUnknownProjectionSkipped
                .run(mesh_id, nodes, flags)
                .await
        }
        "import-resume-after-restart" => {
            import::ImportResumeAfterRestart
                .run(mesh_id, nodes, flags)
                .await
        }
        "post-files-consensus-shape" => {
            post_files_shape::PostFilesConsensusShape
                .run(mesh_id, nodes, flags)
                .await
        }
        "mixed-files-and-folders-one-request" => {
            post_files_mixed::PostFilesMixedFilesAndParents
                .run(mesh_id, nodes, flags)
                .await
        }
        "db-pragma-bench" => {
            db_pragma_bench::DbPragmaBench
                .run(mesh_id, nodes, flags)
                .await
        }
        "upload-and-confirm-placement" => {
            upload_and_confirm_placement::UploadAndConfirmPlacement
                .run(mesh_id, nodes, flags)
                .await
        }
        _ => Err(anyhow::anyhow!("Unknown test: {}", name)),
    }
}

/// List all available test names
pub fn list_test_names() -> Vec<&'static str> {
    vec![
        "file-upload-consistency",
        "fragment-distribution",
        "fragment-health-check",
        "multi-size-file-consistency",
        "chunked-streaming-performance",
        "restart-persistence",
        "graceful-leave",
        "evidence-observe",
        "vote-out-after-kill",
        "regenesis-restart",
        "regenesis-awaiting-upgrade",
        "mesh-growth",
        "auto-seam",
        "three-timescales",
        "evidence-drives-voteout",
        "device-token-consistency",
        "documentprovider-write-consistency",
        "iroh-ping",
        "iroh-reject-unknown",
        "consensus-leader-down",
        "consensus-lagging-catch-up",
        "consensus-bft-quorum-loss",
        "consensus-barrier-decide-window",
        "consensus-barrier-proposal-hold",
        "metrics-collection",
        "tier-membership",
        "eviction-under-pressure",
        "re-encode-after-departure",
        "multi-user-isolation",
        "multi-user-sharing",
        "multi-user-sharing-live-link",
        "consensus-queue-burst",
        "consensus-queue-cross-node",
        "consensus-queue-throughput",
        "orphan-cleanup",
        "device-token-session-bootstrap",
        "fileprovider-device-token-auth",
        "recents-ordering",
        "range-download",
        "takeout-happy-path",
        "import-create-active-conflict",
        "import-upload-happy-path",
        "import-upload-version-rejected",
        "import-upload-missing-manifest",
        "import-upload-quota-exceeded",
        "import-extraction-happy-path",
        "import-extraction-hash-mismatch",
        "import-creation-happy-path",
        "import-creation-mixed-failure",
        "import-write-gate",
        "import-status-counts",
        "import-unknown-projection-skipped",
        "import-resume-after-restart",
        "post-files-consensus-shape",
        "mixed-files-and-folders-one-request",
        "db-pragma-bench",
        "upload-and-confirm-placement",
    ]
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Get the maximum consensus view across all nodes
/// Polls all nodes in parallel and returns the highest view number
pub async fn get_max_view(nodes: &[NodeInfo]) -> Result<u64> {
    let client = Client::new();

    // Poll ALL nodes in parallel
    let mut tasks = Vec::new();
    for node in nodes {
        let client = client.clone();
        let node = node.clone();
        let task = tokio::spawn(async move {
            let url = format!("http://{}:{}/api/consensus", node.ip_address, node.port);
            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", node.jwt_token))
                .timeout(Duration::from_secs(3))
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() => resp
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|json| json["last_decided_height"].as_u64()),
                _ => None,
            }
        });
        tasks.push(task);
    }

    // Wait for all responses
    let mut results = Vec::new();
    for task in tasks {
        if let Ok(Some(view)) = task.await {
            results.push(view);
        }
    }

    if results.is_empty() {
        anyhow::bail!("Failed to get consensus view from any node");
    }

    Ok(results.into_iter().max().unwrap())
}

/// Wait for all nodes to reach at least a minimum consensus view
/// Polls all nodes in parallel to avoid timing issues where views could change
/// between sequential checks
pub async fn wait_for_minimum_view(
    nodes: &[NodeInfo],
    min_view: u64,
    timeout: Duration,
) -> Result<bool> {
    let start = Instant::now();
    let client = Client::new();

    loop {
        if start.elapsed() > timeout {
            return Ok(false);
        }

        // Poll ALL nodes in parallel
        let mut tasks = Vec::new();
        for node in nodes {
            let client = client.clone();
            let node = node.clone();
            let task = tokio::spawn(async move {
                let url = format!("http://{}:{}/api/consensus", node.ip_address, node.port);
                let response = client
                    .get(&url)
                    .header("Authorization", format!("Bearer {}", node.jwt_token))
                    .timeout(Duration::from_secs(2)) // Short timeout per request
                    .send()
                    .await;

                match response {
                    Ok(resp) if resp.status().is_success() => resp
                        .json::<serde_json::Value>()
                        .await
                        .ok()
                        .and_then(|json| json["last_decided_height"].as_u64()),
                    _ => None,
                }
            });
            tasks.push(task);
        }

        // Wait for all responses
        let mut results = Vec::new();
        for task in tasks {
            if let Ok(view_opt) = task.await {
                results.push(view_opt);
            }
        }

        // Check if all nodes reached minimum view
        let all_reached = results.len() == nodes.len()
            && results
                .iter()
                .all(|view_opt| view_opt.map(|v| v >= min_view).unwrap_or(false));

        if all_reached {
            return Ok(true);
        }

        sleep(Duration::from_millis(500)).await;
    }
}

// ============================================================================
// Test Command Handler
// ============================================================================

/// Handle the test subcommand - list or run tests on a mesh
pub async fn handle_test_command(
    docker: &Docker,
    mesh_id: u32,
    test: Option<&str>,
    list: bool,
    flags: &[String],
    runtime: crate::sys::ContainerRuntime,
) -> Result<()> {
    // Handle --list flag
    if list {
        let test_names = list_test_names();
        if test_names.is_empty() {
            println!("No tests registered yet.");
        } else {
            println!("Available tests:");
            for name in test_names {
                println!("  - {}", name);
            }
        }
        return Ok(());
    }

    // If no test specified and not listing, error
    let test_name =
        test.ok_or_else(|| anyhow::anyhow!("No test specified. Use --test <name> or --list"))?;

    println!("Running test '{}' on mesh {}", test_name, mesh_id);
    if !flags.is_empty() {
        println!("Flags: {}", flags.join(", "));
    }

    // Get node metadata for all nodes in the mesh
    let addresses = crate::get_external_addresses(docker, mesh_id, runtime).await?;

    if addresses.is_empty() {
        return Err(anyhow::anyhow!("No nodes found in mesh {}", mesh_id));
    }

    println!("Found {} nodes in mesh", addresses.len());

    // Get JWT tokens for all nodes in parallel
    let mut tasks = Vec::new();
    for (node_id, ip_address, port) in addresses {
        let docker = docker.clone();
        let task = tokio::spawn(async move {
            crate::get_jwt_token(&docker, mesh_id, node_id, runtime)
                .await
                .map(|jwt_token| NodeInfo {
                    node_id,
                    ip_address,
                    port: port as u32,
                    jwt_token,
                })
        });
        tasks.push(task);
    }

    // Collect results
    let mut nodes = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(node_info)) => nodes.push(node_info),
            Ok(Err(e)) => return Err(anyhow::anyhow!("Failed to get JWT token: {}", e)),
            Err(e) => return Err(anyhow::anyhow!("Task join failed: {}", e)),
        }
    }

    println!("Successfully authenticated with all {} nodes", nodes.len());

    // Run the test
    let result = run_test_by_name(mesh_id, test_name, &nodes, flags).await?;

    // Display test result summary
    println!("\n{}", "=".repeat(60));
    println!("Test Results for '{}'", test_name);
    println!("{}", "=".repeat(60));
    println!(
        "Status: {}",
        if result.passed { "PASSED" } else { "FAILED" }
    );
    println!("Duration: {:.2}s", result.duration.as_secs_f64());
    println!(
        "Checks: {} total ({} passed, {} failed)",
        result.checks.len(),
        result.checks.iter().filter(|c| c.passed).count(),
        result.checks.iter().filter(|c| !c.passed).count()
    );

    if !result.details.is_empty() {
        println!("\nDetails:");
        println!("{}", result.details);
    }

    println!();

    if result.passed {
        println!("Test PASSED");
        Ok(())
    } else {
        Err(anyhow::anyhow!("Test FAILED"))
    }
}
