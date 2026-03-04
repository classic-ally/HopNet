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
mod device_tokens;
mod documentprovider_write;
mod file_upload;
mod fragment_distribution;
mod fragment_health_check;
mod iroh_ping;
mod iroh_reject_unknown;
mod metrics;
mod multi_size_files;
mod performance;
pub(crate) mod persistence;
mod timeout_progression;
mod consensus_barriers;
pub(crate) mod multi_user;
mod sharing;
mod consensus_queue;
mod orphan_cleanup;
mod fileprovider_device_token;
mod recents;

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

/// Run a test by name
pub async fn run_test_by_name(mesh_id: u32, name: &str, nodes: &[NodeInfo], flags: &[String]) -> Result<TestResult> {
    match name {
        "file-upload-consistency" => {
            file_upload::FileUploadConsistency.run(mesh_id, nodes, flags).await
        }
        "fragment-distribution" => {
            fragment_distribution::FragmentDistribution.run(mesh_id, nodes, flags).await
        }
        "fragment-health-check" => {
            fragment_health_check::FragmentHealthCheck.run(mesh_id, nodes, flags).await
        }
        "chunked-streaming-performance" => {
            performance::ChunkedStreamingPerformance.run(mesh_id, nodes, flags).await
        }
        "multi-size-file-consistency" => {
            multi_size_files::MultiSizeFileConsistency.run(mesh_id, nodes, flags).await
        }
        "restart-persistence" => {
            persistence::RestartPersistence.run(mesh_id, nodes, flags).await
        }
        "device-token-consistency" => {
            device_tokens::DeviceTokenConsistency.run(mesh_id, nodes, flags).await
        }
        "documentprovider-write-consistency" => {
            documentprovider_write::DocumentProviderWriteConsistency.run(mesh_id, nodes, flags).await
        }
        "iroh-ping" => {
            iroh_ping::IrohPing.run(mesh_id, nodes, flags).await
        }
        "iroh-reject-unknown" => {
            iroh_reject_unknown::IrohRejectUnknown.run(mesh_id, nodes, flags).await
        }
        "timeout-progression" => {
            timeout_progression::TimeoutProgression.run(mesh_id, nodes, flags).await
        }
        "consensus-barrier-basic" => {
            consensus_barriers::ConsensusBarrierBasic.run(mesh_id, nodes, flags).await
        }
        "consensus-barrier-missed-ballot" => {
            consensus_barriers::ConsensusBarrierMissedBallot.run(mesh_id, nodes, flags).await
        }
        "consensus-barrier-tc-late" => {
            consensus_barriers::ConsensusBarrierTcLate.run(mesh_id, nodes, flags).await
        }
        "metrics-collection" => {
            metrics::MetricsCollection.run(mesh_id, nodes, flags).await
        }
        "multi-user-isolation" => {
            multi_user::MultiUserIsolation.run(mesh_id, nodes, flags).await
        }
        "multi-user-sharing" => {
            sharing::MultiUserSharing.run(mesh_id, nodes, flags).await
        }
        "multi-user-sharing-live-link" => {
            sharing::MultiUserSharingLiveLink.run(mesh_id, nodes, flags).await
        }
        "consensus-queue-burst" => {
            consensus_queue::ConsensusQueueBurst.run(mesh_id, nodes, flags).await
        }
        "consensus-queue-cross-node" => {
            consensus_queue::ConsensusQueueCrossNode.run(mesh_id, nodes, flags).await
        }
        "consensus-queue-throughput" => {
            consensus_queue::ConsensusQueueThroughput.run(mesh_id, nodes, flags).await
        }
        "orphan-cleanup" => {
            orphan_cleanup::OrphanCleanup.run(mesh_id, nodes, flags).await
        }
        "device-token-session-bootstrap" => {
            fileprovider_device_token::DeviceTokenSessionBootstrap.run(mesh_id, nodes, flags).await
        }
        "fileprovider-device-token-auth" => {
            fileprovider_device_token::FileProviderDeviceTokenAuth.run(mesh_id, nodes, flags).await
        }
        "recents-ordering" => {
            recents::RecentsOrdering.run(mesh_id, nodes, flags).await
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
        "device-token-consistency",
        "documentprovider-write-consistency",
        "iroh-ping",
        "iroh-reject-unknown",
        "timeout-progression",
        "consensus-barrier-basic",
        "consensus-barrier-missed-ballot",
        "consensus-barrier-tc-late",
        "metrics-collection",
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
            let url = format!("http://{}:{}/consensus", node.ip_address, node.port);
            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", node.jwt_token))
                .timeout(Duration::from_secs(3))
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    resp.json::<serde_json::Value>()
                        .await
                        .ok()
                        .and_then(|json| json["view"].as_u64())
                }
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
                let url = format!("http://{}:{}/consensus", node.ip_address, node.port);
                let response = client
                    .get(&url)
                    .header("Authorization", format!("Bearer {}", node.jwt_token))
                    .timeout(Duration::from_secs(2)) // Short timeout per request
                    .send()
                    .await;

                match response {
                    Ok(resp) if resp.status().is_success() => {
                        resp.json::<serde_json::Value>()
                            .await
                            .ok()
                            .and_then(|json| json["view"].as_u64())
                    }
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
            && results.iter().all(|view_opt| {
                view_opt.map(|v| v >= min_view).unwrap_or(false)
            });

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
    let test_name = test.ok_or_else(|| anyhow::anyhow!("No test specified. Use --test <name> or --list"))?;

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
            crate::get_jwt_token(&docker, mesh_id, node_id, runtime).await
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
    println!("Status: {}", if result.passed { "PASSED" } else { "FAILED" });
    println!("Duration: {:.2}s", result.duration.as_secs_f64());
    println!("Checks: {} total ({} passed, {} failed)",
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
