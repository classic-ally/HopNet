use anyhow::Result;
use bollard::Docker;
use hopnet_common::{StateSnapshot, TableHashInfo};
use std::collections::HashMap;

/// Cluster of nodes with identical table state at the same view
#[derive(Debug)]
pub struct TableCluster {
    pub nodes: Vec<u32>,
    pub view: i32,
    pub hash: String,
    pub row_count: usize,
    pub excluded_columns: Vec<String>,
}

/// Divergence report for a single table
#[derive(Debug)]
pub struct TableDivergenceReport {
    pub table_name: String,
    pub total_nodes: usize,
    pub clusters: Vec<TableCluster>,
}

impl TableDivergenceReport {
    /// Check if table is in consensus (all nodes at highest view have same hash)
    pub fn is_consensus(&self) -> bool {
        let max_view = self.clusters.iter().map(|c| c.view).max().unwrap_or(0);
        let clusters_at_max_view: Vec<_> = self
            .clusters
            .iter()
            .filter(|c| c.view == max_view)
            .collect();
        clusters_at_max_view.len() == 1
    }

    /// Get clusters at the highest view (the current consensus state)
    pub fn current_view_clusters(&self) -> Vec<&TableCluster> {
        let max_view = self.clusters.iter().map(|c| c.view).max().unwrap_or(0);
        self.clusters
            .iter()
            .filter(|c| c.view == max_view)
            .collect()
    }

    /// Get clusters at lower views (catching up)
    pub fn catching_up_clusters(&self) -> Vec<&TableCluster> {
        let max_view = self.clusters.iter().map(|c| c.view).max().unwrap_or(0);
        self.clusters.iter().filter(|c| c.view < max_view).collect()
    }

    /// Check if there's true divergence (multiple hashes at same view)
    pub fn has_divergence(&self) -> bool {
        !self.is_consensus()
    }
}

/// Overall divergence report for all tables
#[derive(Debug)]
pub struct DivergenceReport {
    pub mesh_id: u32,
    pub total_nodes: usize,
    pub view_range: (i32, i32),   // (min, max)
    pub height_range: (i32, i32), // (min, max)
    pub table_reports: Vec<TableDivergenceReport>,
}

impl DivergenceReport {
    pub fn consensus_tables(&self) -> Vec<&TableDivergenceReport> {
        self.table_reports
            .iter()
            .filter(|r| r.is_consensus())
            .collect()
    }

    pub fn divergent_tables(&self) -> Vec<&TableDivergenceReport> {
        self.table_reports
            .iter()
            .filter(|r| !r.is_consensus())
            .collect()
    }

    pub fn is_full_consensus(&self) -> bool {
        self.divergent_tables().is_empty()
    }
}

/// Analyze table state across nodes and build view-aware clusters
fn analyze_table(
    table_name: &str,
    node_snapshots: &[(u32, StateSnapshot)],
) -> TableDivergenceReport {
    // Group nodes by (view, hash) for this table
    let mut view_hash_to_nodes: HashMap<(i32, String), Vec<u32>> = HashMap::new();
    let mut hash_to_info: HashMap<String, &TableHashInfo> = HashMap::new();

    for (node_id, snapshot) in node_snapshots {
        if let Some(table_info) = snapshot.table_hashes.get(table_name) {
            let key = (snapshot.committed_view, table_info.hash.clone());
            view_hash_to_nodes.entry(key).or_default().push(*node_id);
            hash_to_info.insert(table_info.hash.clone(), table_info);
        }
    }

    // Build clusters
    let mut clusters: Vec<TableCluster> = view_hash_to_nodes
        .into_iter()
        .map(|((view, hash), nodes)| {
            let info = hash_to_info.get(&hash).expect("Hash must have info");
            TableCluster {
                nodes,
                view,
                hash: hash.clone(),
                row_count: info.row_count,
                excluded_columns: info.excluded_columns.clone(),
            }
        })
        .collect();

    // Sort clusters by view (highest first), then by size (largest first)
    clusters.sort_by(|a, b| {
        b.view
            .cmp(&a.view)
            .then_with(|| b.nodes.len().cmp(&a.nodes.len()))
    });

    TableDivergenceReport {
        table_name: table_name.to_string(),
        total_nodes: node_snapshots.len(),
        clusters,
    }
}

/// Build complete divergence report from node snapshots (pure function)
pub fn build_divergence_report(
    mesh_id: u32,
    node_snapshots: Vec<(u32, StateSnapshot)>,
) -> Result<DivergenceReport> {
    if node_snapshots.is_empty() {
        anyhow::bail!("No node snapshots provided");
    }

    // Calculate view and height ranges
    let views: Vec<i32> = node_snapshots
        .iter()
        .map(|(_, s)| s.committed_view)
        .collect();
    let heights: Vec<i32> = node_snapshots
        .iter()
        .map(|(_, s)| s.consensus_height)
        .collect();

    let view_range = (*views.iter().min().unwrap(), *views.iter().max().unwrap());
    let height_range = (
        *heights.iter().min().unwrap(),
        *heights.iter().max().unwrap(),
    );

    // Get all table names (from first node)
    let table_names: Vec<String> = node_snapshots[0].1.table_hashes.keys().cloned().collect();

    // Analyze each table
    let table_reports: Vec<TableDivergenceReport> = table_names
        .iter()
        .map(|table_name| analyze_table(table_name, &node_snapshots))
        .collect();

    Ok(DivergenceReport {
        mesh_id,
        total_nodes: node_snapshots.len(),
        view_range,
        height_range,
        table_reports,
    })
}

/// Print formatted divergence report to console
pub fn print_divergence_report(report: &DivergenceReport) {
    println!("State Divergence Report (Mesh {})", report.mesh_id);
    println!("═══════════════════════════════════════");
    println!(
        "Nodes: {} (views {}-{}, heights {}-{})",
        report.total_nodes,
        report.view_range.0,
        report.view_range.1,
        report.height_range.0,
        report.height_range.1
    );

    if report.view_range.0 != report.view_range.1 {
        println!("⚠️  View spread detected - some nodes may be catching up");
    }
    println!();

    if report.is_full_consensus() {
        println!(
            "✅ All nodes in consensus across {} tables",
            report.table_reports.len()
        );
        return;
    }

    let divergent = report.divergent_tables();
    let consensus = report.consensus_tables();

    // Distinguish true divergence from catch-up
    let true_divergence: Vec<_> = divergent
        .iter()
        .filter(|t| t.current_view_clusters().len() > 1)
        .collect();

    if !true_divergence.is_empty() {
        println!(
            "🚨 {} tables with DIVERGENCE at current view\n",
            true_divergence.len()
        );
    } else {
        println!(
            "⚙️  {} tables show view spread (nodes catching up)\n",
            divergent.len()
        );
    }

    if !consensus.is_empty() {
        println!("Consensus Tables ({}):", consensus.len());
        let table_names: Vec<_> = consensus.iter().map(|r| r.table_name.as_str()).collect();
        println!("  ✅ {}\n", table_names.join(", "));
    }

    if !true_divergence.is_empty() {
        println!("Tables with Divergence:\n");
        for table in true_divergence {
            print_table_divergence(table);
        }
    }

    let catch_up_only: Vec<_> = divergent
        .iter()
        .filter(|t| t.current_view_clusters().len() == 1)
        .collect();

    if !catch_up_only.is_empty() {
        println!("Tables with Catch-up in Progress:\n");
        for table in catch_up_only {
            print_table_divergence(table);
        }
    }
}

/// Print divergence details for a single table (view-aware)
fn print_table_divergence(table: &TableDivergenceReport) {
    let current_view_clusters = table.current_view_clusters();
    let catching_up = table.catching_up_clusters();

    let max_view = table.clusters.iter().map(|c| c.view).max().unwrap_or(0);
    let row_count = table.clusters[0].row_count;

    println!("  {} ({} rows)", table.table_name, row_count);

    if !table.clusters[0].excluded_columns.is_empty() {
        println!(
            "    (excludes: {})",
            table.clusters[0].excluded_columns.join(", ")
        );
    }

    // Show current view clusters (potential divergence)
    if current_view_clusters.len() > 1 {
        println!("    ⚠️  DIVERGENCE at view {}:", max_view);
        for (i, cluster) in current_view_clusters.iter().enumerate() {
            let label = (b'A' + i as u8) as char;
            println!(
                "      View {} Cluster {} ({} nodes): {:?}  hash: {}...",
                cluster.view,
                label,
                cluster.nodes.len(),
                cluster.nodes,
                &cluster.hash[..16]
            );
        }
    } else if let Some(cluster) = current_view_clusters.first() {
        println!(
            "    View {} ({} nodes): {:?}  hash: {}...  ✅ Consensus at view {}",
            cluster.view,
            cluster.nodes.len(),
            cluster.nodes,
            &cluster.hash[..16],
            cluster.view
        );
    }

    // Show catching up nodes
    if !catching_up.is_empty() {
        for cluster in catching_up {
            println!(
                "    View {} ({} nodes): {:?}  hash: {}...  ⚙️  Catching up",
                cluster.view,
                cluster.nodes.len(),
                cluster.nodes,
                &cluster.hash[..16]
            );
        }
    }

    println!();
}

/// Check for state divergence across nodes in a mesh
pub async fn check_divergence(
    docker: &Docker,
    mesh_id: u32,
    runtime: crate::sys::ContainerRuntime,
) -> Result<()> {
    println!("Checking state divergence for mesh {}...", mesh_id);

    // Get external addresses (runtime-aware: container IPs for Docker, localhost for Podman)
    let node_addresses = crate::get_external_addresses(docker, mesh_id, runtime).await?;

    if node_addresses.is_empty() {
        println!("No containers found for mesh {}.", mesh_id);
        return Ok(());
    }

    println!(
        "Found {} nodes, fetching JWT tokens...",
        node_addresses.len()
    );

    // Build NodeInfo for each node (includes JWT)
    let mut nodes: Vec<crate::NodeInfo> = Vec::new();
    for (node_id, ip_address, port) in node_addresses {
        match crate::get_jwt_token(docker, mesh_id, node_id, runtime).await {
            Ok(jwt_token) => {
                nodes.push(crate::NodeInfo {
                    node_id,
                    ip_address,
                    port: port as u32,
                    jwt_token,
                });
            }
            Err(e) => {
                eprintln!("⚠️  Failed to get JWT for node {}: {}", node_id, e);
            }
        }
    }

    if nodes.is_empty() {
        anyhow::bail!("Failed to authenticate with any nodes");
    }

    println!(
        "Authenticated with {} nodes, fetching state snapshots...",
        nodes.len()
    );

    // Fetch state snapshots from all nodes in parallel
    let mut snapshots = Vec::new();
    let mut fetch_handles = Vec::new();

    for node_info in nodes {
        let handle = tokio::spawn(async move {
            let response = crate::call_node_api(&node_info, "/api/debug/state", true).await?;
            if !response.status().is_success() {
                anyhow::bail!("HTTP {}", response.status());
            }
            let snapshot: hopnet_common::StateSnapshot = response.json().await?;
            Ok::<_, anyhow::Error>((node_info.node_id, snapshot))
        });
        fetch_handles.push(handle);
    }

    for handle in fetch_handles {
        match handle.await {
            Ok(Ok(snapshot)) => snapshots.push(snapshot),
            Ok(Err(e)) => eprintln!("⚠️  Failed to fetch snapshot: {}", e),
            Err(e) => eprintln!("⚠️  Task panicked: {}", e),
        }
    }

    if snapshots.is_empty() {
        anyhow::bail!("Failed to fetch any state snapshots");
    }

    println!(
        "Fetched {} snapshots, analyzing divergence...\n",
        snapshots.len()
    );

    // Build and display divergence report
    let report = build_divergence_report(mesh_id, snapshots)?;
    print_divergence_report(&report);

    Ok(())
}
