use anyhow::Result;
use bollard::Docker;
use hopnet_common::NodeStateReport;
use std::collections::{BTreeSet, HashMap};

/// Cluster of nodes with identical table content at the same height.
#[derive(Debug)]
pub struct TableCluster {
    pub nodes: Vec<u32>,
    pub height: u64,
    pub hash: String,
    pub row_count: u64,
    pub excluded_columns: Vec<String>,
}

/// Divergence report for a single covered table.
#[derive(Debug)]
pub struct TableDivergenceReport {
    pub section: String,
    pub table_name: String,
    pub total_nodes: usize,
    pub clusters: Vec<TableCluster>,
}

/// Sentinel hash for a node that reported at the cluster height but did
/// not report this table at all — a covered-set mismatch is divergence,
/// not absence.
const MISSING: &str = "<table not reported>";

impl TableDivergenceReport {
    /// Consensus = exactly one cluster among the nodes at the highest
    /// height; nodes below it are catching up, not divergent.
    pub fn is_consensus(&self) -> bool {
        self.current_height_clusters().len() == 1
    }

    /// Clusters at the highest height (the current consensus state).
    pub fn current_height_clusters(&self) -> Vec<&TableCluster> {
        let max_height = self.clusters.iter().map(|c| c.height).max().unwrap_or(0);
        self.clusters
            .iter()
            .filter(|c| c.height == max_height)
            .collect()
    }

    /// Clusters at lower heights (catching up).
    pub fn catching_up_clusters(&self) -> Vec<&TableCluster> {
        let max_height = self.clusters.iter().map(|c| c.height).max().unwrap_or(0);
        self.clusters
            .iter()
            .filter(|c| c.height < max_height)
            .collect()
    }

    pub fn has_divergence(&self) -> bool {
        !self.is_consensus()
    }
}

/// Overall divergence report across every covered table.
#[derive(Debug)]
pub struct DivergenceReport {
    pub mesh_id: u32,
    pub total_nodes: usize,
    pub height_range: (u64, u64), // (min, max)
    /// Top-hash clusters: (height, top_hash, nodes). One cluster at the
    /// max height is the one-line full-consensus check; the per-table
    /// reports exist to pinpoint what diverged when it isn't.
    pub top_clusters: Vec<(u64, String, Vec<u32>)>,
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

    /// One top-hash cluster at the max height AND every table in
    /// consensus. The top hash excludes DivergenceOnly tables (epoch
    /// history), so the per-table check is not redundant — it is what
    /// covers decided_blocks.
    pub fn is_full_consensus(&self) -> bool {
        let max_height = self.height_range.1;
        let tops_at_max = self
            .top_clusters
            .iter()
            .filter(|(h, _, _)| *h == max_height)
            .count();
        tops_at_max == 1 && self.divergent_tables().is_empty()
    }
}

/// Cluster one table across nodes by (height, hash). A node at a given
/// height that lacks the table entirely clusters under a sentinel hash —
/// differing covered sets are divergence in their own right.
fn analyze_table(
    section: &str,
    table_name: &str,
    node_reports: &[(u32, NodeStateReport)],
) -> TableDivergenceReport {
    struct Info {
        row_count: u64,
        excluded_columns: Vec<String>,
    }
    let mut cluster_nodes: HashMap<(u64, String), Vec<u32>> = HashMap::new();
    let mut hash_info: HashMap<String, Info> = HashMap::new();

    for (node_id, report) in node_reports {
        let table = report
            .manifest
            .sections
            .iter()
            .find(|s| s.name == section)
            .and_then(|s| s.tables.iter().find(|t| t.name == table_name));
        let hash = match table {
            Some(t) => {
                let hex = t.hash.to_hex();
                hash_info.entry(hex.clone()).or_insert(Info {
                    row_count: t.row_count,
                    excluded_columns: t.excluded_columns.clone(),
                });
                hex
            }
            None => MISSING.to_string(),
        };
        cluster_nodes
            .entry((report.consensus_height, hash))
            .or_default()
            .push(*node_id);
    }

    let mut clusters: Vec<TableCluster> = cluster_nodes
        .into_iter()
        .map(|((height, hash), nodes)| {
            let info = hash_info.get(&hash);
            TableCluster {
                nodes,
                height,
                row_count: info.map(|i| i.row_count).unwrap_or(0),
                excluded_columns: info.map(|i| i.excluded_columns.clone()).unwrap_or_default(),
                hash,
            }
        })
        .collect();

    // Highest height first, then largest cluster first.
    clusters.sort_by(|a, b| {
        b.height
            .cmp(&a.height)
            .then_with(|| b.nodes.len().cmp(&a.nodes.len()))
    });

    TableDivergenceReport {
        section: section.to_string(),
        table_name: table_name.to_string(),
        total_nodes: node_reports.len(),
        clusters,
    }
}

/// Build the complete divergence report from node manifests (pure
/// function). The table universe is the UNION across all nodes, so a
/// node reporting extra or missing tables is visible, not silently
/// skipped.
pub fn build_divergence_report(
    mesh_id: u32,
    node_reports: Vec<(u32, NodeStateReport)>,
) -> Result<DivergenceReport> {
    if node_reports.is_empty() {
        anyhow::bail!("No node state reports provided");
    }

    let heights: Vec<u64> = node_reports
        .iter()
        .map(|(_, r)| r.consensus_height)
        .collect();
    let height_range = (
        *heights.iter().min().unwrap(),
        *heights.iter().max().unwrap(),
    );

    let mut top_nodes: HashMap<(u64, String), Vec<u32>> = HashMap::new();
    for (node_id, report) in &node_reports {
        top_nodes
            .entry((report.consensus_height, report.manifest.top_hash.to_hex()))
            .or_default()
            .push(*node_id);
    }
    let mut top_clusters: Vec<(u64, String, Vec<u32>)> = top_nodes
        .into_iter()
        .map(|((height, hash), nodes)| (height, hash, nodes))
        .collect();
    top_clusters.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.2.len().cmp(&a.2.len())));

    // Union of (section, table) across all nodes, in first-seen order
    // per BTreeSet's deterministic ordering.
    let table_keys: BTreeSet<(String, String)> = node_reports
        .iter()
        .flat_map(|(_, r)| {
            r.manifest.sections.iter().flat_map(|s| {
                s.tables
                    .iter()
                    .map(|t| (s.name.clone(), t.name.clone()))
                    .collect::<Vec<_>>()
            })
        })
        .collect();

    let table_reports: Vec<TableDivergenceReport> = table_keys
        .iter()
        .map(|(section, table)| analyze_table(section, table, &node_reports))
        .collect();

    Ok(DivergenceReport {
        mesh_id,
        total_nodes: node_reports.len(),
        height_range,
        top_clusters,
        table_reports,
    })
}

/// Print formatted divergence report to console.
pub fn print_divergence_report(report: &DivergenceReport) {
    println!("State Divergence Report (Mesh {})", report.mesh_id);
    println!("═══════════════════════════════════════");
    println!(
        "Nodes: {} (heights {}-{})",
        report.total_nodes, report.height_range.0, report.height_range.1
    );
    for (height, hash, nodes) in &report.top_clusters {
        println!(
            "  top hash @ height {}: {}...  ({} nodes: {:?})",
            height,
            &hash[..16],
            nodes.len(),
            nodes
        );
    }

    if report.height_range.0 != report.height_range.1 {
        println!("⚠️  Height spread detected - some nodes may be catching up");
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

    println!(
        "🚨 {} tables with DIVERGENCE at current height\n",
        divergent.len()
    );

    if !consensus.is_empty() {
        println!("Consensus Tables ({}):", consensus.len());
        let table_names: Vec<_> = consensus.iter().map(|r| r.table_name.as_str()).collect();
        println!("  ✅ {}\n", table_names.join(", "));
    }

    if !divergent.is_empty() {
        println!("Tables with Divergence:\n");
        for table in &divergent {
            print_table_divergence(table);
        }
    }

    let catching_up: Vec<_> = report
        .table_reports
        .iter()
        .filter(|t| t.is_consensus() && !t.catching_up_clusters().is_empty())
        .collect();
    if !catching_up.is_empty() {
        println!("Tables with Catch-up in Progress:\n");
        for table in catching_up {
            print_table_divergence(table);
        }
    }
}

/// Print divergence details for a single table (height-aware).
fn print_table_divergence(table: &TableDivergenceReport) {
    let current = table.current_height_clusters();
    let catching_up = table.catching_up_clusters();

    let max_height = table.clusters.iter().map(|c| c.height).max().unwrap_or(0);
    let row_count = table.clusters[0].row_count;

    println!(
        "  {}/{} ({} rows)",
        table.section, table.table_name, row_count
    );

    if !table.clusters[0].excluded_columns.is_empty() {
        println!(
            "    (excludes: {})",
            table.clusters[0].excluded_columns.join(", ")
        );
    }

    if current.len() > 1 {
        println!("    ⚠️  DIVERGENCE at height {}:", max_height);
        for (i, cluster) in current.iter().enumerate() {
            let label = (b'A' + i as u8) as char;
            println!(
                "      Height {} Cluster {} ({} nodes): {:?}  hash: {}...",
                cluster.height,
                label,
                cluster.nodes.len(),
                cluster.nodes,
                &cluster.hash[..16.min(cluster.hash.len())]
            );
        }
    } else if let Some(cluster) = current.first() {
        println!(
            "    Height {} ({} nodes): {:?}  hash: {}...  ✅ Consensus",
            cluster.height,
            cluster.nodes.len(),
            cluster.nodes,
            &cluster.hash[..16.min(cluster.hash.len())]
        );
    }

    for cluster in catching_up {
        println!(
            "    Height {} ({} nodes): {:?}  hash: {}...  ⚙️  Catching up",
            cluster.height,
            cluster.nodes.len(),
            cluster.nodes,
            &cluster.hash[..16.min(cluster.hash.len())]
        );
    }

    println!();
}

/// Check for state divergence across nodes in a mesh. Errs when the mesh
/// is genuinely divergent (same height, different state) — this is the
/// standing post-test gate, so a divergent mesh fails the run instead of
/// being deleted as a pass.
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
        "Authenticated with {} nodes, fetching state manifests...",
        nodes.len()
    );

    // Fetch state manifests from all nodes in parallel
    let mut reports = Vec::new();
    let mut fetch_handles = Vec::new();

    for node_info in nodes {
        let handle = tokio::spawn(async move {
            let response = crate::call_node_api(&node_info, "/api/debug/state", true).await?;
            if !response.status().is_success() {
                anyhow::bail!("HTTP {}", response.status());
            }
            let report: NodeStateReport = response.json().await?;
            Ok::<_, anyhow::Error>((node_info.node_id, report))
        });
        fetch_handles.push(handle);
    }

    for handle in fetch_handles {
        match handle.await {
            Ok(Ok(report)) => reports.push(report),
            Ok(Err(e)) => eprintln!("⚠️  Failed to fetch state manifest: {}", e),
            Err(e) => eprintln!("⚠️  Task panicked: {}", e),
        }
    }

    if reports.is_empty() {
        anyhow::bail!("Failed to fetch any state manifests");
    }

    println!(
        "Fetched {} manifests, analyzing divergence...\n",
        reports.len()
    );

    let report = build_divergence_report(mesh_id, reports)?;
    print_divergence_report(&report);

    if !report.is_full_consensus() {
        anyhow::bail!(
            "state divergence detected across mesh {} (see report above)",
            mesh_id
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hopnet_common::Blake3Hash;
    use hopnet_common::snapshot::{SectionManifest, SnapshotManifest, TableManifest, TableRole};

    fn report_with(height: u64, seed: u8, tables: &[(&str, u8)]) -> NodeStateReport {
        NodeStateReport {
            consensus_height: height,
            manifest: SnapshotManifest {
                artifact_version: 1,
                top_hash: Blake3Hash::from_bytes([seed; 32]),
                sections: vec![SectionManifest {
                    name: "host".to_string(),
                    format_version: 1,
                    section_hash: Blake3Hash::from_bytes([seed; 32]),
                    tables: tables
                        .iter()
                        .map(|(name, h)| TableManifest {
                            name: name.to_string(),
                            role: TableRole::Exported,
                            row_count: 1,
                            hash: Blake3Hash::from_bytes([*h; 32]),
                            excluded_columns: vec![],
                        })
                        .collect(),
                }],
            },
        }
    }

    // Should: flag divergence when nodes at the same height report
    // different hashes. Should not: flag pure height spread (a node
    // catching up) as divergence.
    // Impact: the auto-managed post-test gate was unreachable for its
    // entire life — check_divergence returned Ok unconditionally, so a
    // divergent mesh was deleted as a pass. This pins the gate's logic.
    #[test]
    fn divergence_at_same_height_gates_but_catch_up_does_not() {
        // Same height, different table hash → divergent.
        let divergent = build_divergence_report(
            0,
            vec![
                (0, report_with(5, 1, &[("users", 10)])),
                (1, report_with(5, 2, &[("users", 20)])),
            ],
        )
        .unwrap();
        assert!(!divergent.is_full_consensus());
        assert_eq!(divergent.divergent_tables().len(), 1);

        // Height spread with identical state at the tip → catching up.
        let catch_up = build_divergence_report(
            0,
            vec![
                (0, report_with(5, 1, &[("users", 10)])),
                (1, report_with(5, 1, &[("users", 10)])),
                (2, report_with(3, 3, &[("users", 30)])),
            ],
        )
        .unwrap();
        assert!(catch_up.is_full_consensus());
        assert!(catch_up.divergent_tables().is_empty());
    }

    // Should: analyze the union of tables across nodes, so a table
    // missing from one node at the same height reports as divergence.
    // Impact: regression guard — the old checker took the table list
    // from node 0 only, making a node with a different covered set
    // invisible.
    #[test]
    fn table_universe_is_union_of_nodes() {
        let report = build_divergence_report(
            0,
            vec![
                (0, report_with(5, 1, &[("users", 10), ("nodes", 11)])),
                (1, report_with(5, 1, &[("users", 10)])),
            ],
        )
        .unwrap();
        assert!(!report.is_full_consensus());
        let divergent = report.divergent_tables();
        assert_eq!(divergent.len(), 1);
        assert_eq!(divergent[0].table_name, "nodes");
        assert!(
            divergent[0]
                .current_height_clusters()
                .iter()
                .any(|c| c.hash == MISSING)
        );
    }
}
