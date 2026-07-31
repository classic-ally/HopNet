use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_stream::StreamExt;

use crate::NodeInfo;

// ============================================================================
// File Upload Helpers
// ============================================================================

/// Upload a file to a specific node
///
/// # Arguments
/// * `node` - The node to upload to
/// * `path` - The directory path (e.g., "/documents")
/// * `filename` - The filename (e.g., "report.pdf")
/// * `contents` - The file contents as bytes (takes ownership to avoid copying)
pub async fn upload_file(
    node: &NodeInfo,
    path: &str,
    filename: &str,
    contents: Vec<u8>,
) -> Result<()> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/files", node.ip_address, node.port);

    let contents_len = contents.len();

    // Take ownership and move into multipart form without copying
    let part = reqwest::multipart::Part::bytes(contents).file_name(filename.to_string());

    // Create multipart form with path and file
    let form = reqwest::multipart::Form::new()
        .text("path", path.to_string())
        .part(format!("file_{}", contents_len), part);

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .multipart(form)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("Upload failed with status {}: {}", status, body);
    }

    Ok(())
}

/// Upload multiple files in a single multipart request to one node.
///
/// All files share the same parent path. Each file becomes a `file_<size>` part
/// alongside one `path` text part. Used to exercise the batched-inode atomicity
/// of `post_files`.
pub async fn upload_files_multi(
    node: &NodeInfo,
    path: &str,
    files: Vec<(String, Vec<u8>)>,
) -> Result<()> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/files", node.ip_address, node.port);

    let mut form = reqwest::multipart::Form::new().text("path", path.to_string());
    for (filename, contents) in files {
        let len = contents.len();
        let part = reqwest::multipart::Part::bytes(contents).file_name(filename);
        form = form.part(format!("file_{}", len), part);
    }

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .multipart(form)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("Multi-file upload failed with status {}: {}", status, body);
    }

    Ok(())
}

// ============================================================================
// File Modify Helpers
// ============================================================================

/// Modify (update content of) an existing file by inode_id
///
/// # Arguments
/// * `node` - The node to send the update to
/// * `inode_id` - The inode ID of the file to modify
/// * `new_contents` - The new file contents as bytes
pub async fn modify_file(node: &NodeInfo, inode_id: &str, new_contents: Vec<u8>) -> Result<()> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/files", node.ip_address, node.port);

    let contents_len = new_contents.len();

    let part = reqwest::multipart::Part::bytes(new_contents).file_name("content");

    let form = reqwest::multipart::Form::new()
        .text("inode_id", inode_id.to_string())
        .part(format!("file_{}", contents_len), part);

    let response = client
        .patch(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .multipart(form)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("Modify file failed with status {}: {}", status, body);
    }

    Ok(())
}

// ============================================================================
// File Download Helpers
// ============================================================================

/// Download a file from a specific node
///
/// # Arguments
/// * `node` - The node to download from
/// * `path` - The full file path (e.g., "/documents/report.pdf")
///
/// # Returns
/// The file contents as bytes
pub async fn download_file(node: &NodeInfo, path: &str) -> Result<Vec<u8>> {
    download_file_with_timeout(node, path, std::time::Duration::from_secs(120)).await
}

/// Download a file from a node with configurable timeout and retry logic
///
/// # Arguments
/// * `node` - The node to download from
/// * `path` - The full file path
/// * `timeout` - Timeout for the download request
///
/// # Returns
/// File contents as bytes
pub async fn download_file_with_timeout(
    node: &NodeInfo,
    path: &str,
    timeout: std::time::Duration,
) -> Result<Vec<u8>> {
    const MAX_RETRIES: u32 = 3;
    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        let client = Client::builder().timeout(timeout).build()?;

        // Strip leading slash if present for URL construction
        let path_trimmed = path.strip_prefix('/').unwrap_or(path);
        let url = format!(
            "http://{}:{}/api/files/{}",
            node.ip_address, node.port, path_trimmed
        );

        match client
            .get(&url)
            .header("Authorization", format!("Bearer {}", node.jwt_token))
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "No body".to_string());
                    last_error = Some(anyhow::anyhow!(
                        "Download failed with status {}: {}",
                        status,
                        body
                    ));
                    if attempt < MAX_RETRIES {
                        tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                            .await;
                        continue;
                    }
                } else {
                    match response.bytes().await {
                        Ok(bytes) => return Ok(bytes.to_vec()),
                        Err(e) => {
                            last_error =
                                Some(anyhow::anyhow!("Failed to read response body: {}", e));
                            if attempt < MAX_RETRIES {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    500 * attempt as u64,
                                ))
                                .await;
                                continue;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                last_error = Some(anyhow::anyhow!("Request failed: {}", e));
                if attempt < MAX_RETRIES {
                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                        .await;
                    continue;
                }
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("Download failed after {} retries", MAX_RETRIES)))
}

/// Download a file from all nodes in parallel
///
/// # Arguments
/// * `nodes` - All nodes to download from
/// * `path` - The full file path
///
/// # Returns
/// Vector of file contents from each node (in same order as nodes)
pub async fn download_file_from_all_nodes(nodes: &[NodeInfo], path: &str) -> Result<Vec<Vec<u8>>> {
    download_file_from_all_nodes_with_timeout(nodes, path, std::time::Duration::from_secs(120))
        .await
}

/// Download a file from all nodes in parallel with configurable timeout
///
/// # Arguments
/// * `nodes` - All nodes to download from
/// * `path` - The full file path
/// * `timeout` - Timeout for each download request
///
/// # Returns
/// Vector of file contents from each node (in same order as nodes)
pub async fn download_file_from_all_nodes_with_timeout(
    nodes: &[NodeInfo],
    path: &str,
    timeout: std::time::Duration,
) -> Result<Vec<Vec<u8>>> {
    let mut tasks = Vec::new();

    for node in nodes {
        let node = node.clone();
        let path = path.to_string();
        let task =
            tokio::spawn(async move { download_file_with_timeout(&node, &path, timeout).await });
        tasks.push(task);
    }

    let mut results = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(data)) => results.push(data),
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(anyhow::anyhow!("Task join failed: {}", e)),
        }
    }

    Ok(results)
}

/// Verify that all downloaded files are identical
///
/// # Arguments
/// * `data` - Vector of file contents from multiple nodes
///
/// # Returns
/// Ok if all identical, Err with details if any mismatch
pub fn verify_all_identical(data: &[Vec<u8>]) -> Result<()> {
    if data.is_empty() {
        return Err(anyhow::anyhow!("No data to verify"));
    }

    let first = &data[0];
    for (i, other) in data.iter().enumerate().skip(1) {
        if first != other {
            return Err(anyhow::anyhow!(
                "Data mismatch: node 0 ({} bytes) != node {} ({} bytes)",
                first.len(),
                i,
                other.len()
            ));
        }
    }

    Ok(())
}

// ============================================================================
// File Listing Helpers
// ============================================================================

/// List files in a directory on a specific node
///
/// # Arguments
/// * `node` - The node to query
/// * `path` - The directory path to list
///
/// # Returns
/// JSON array of file items
pub async fn list_files(node: &NodeInfo, path: &str) -> Result<serde_json::Value> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/files?path={}",
        node.ip_address, node.port, path
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("List files failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

/// List files from all nodes in parallel
///
/// # Arguments
/// * `nodes` - All nodes to query
/// * `path` - The directory path to list
///
/// # Returns
/// Vector of JSON responses from each node (in same order as nodes)
pub async fn list_files_from_all_nodes(
    nodes: &[NodeInfo],
    path: &str,
) -> Result<Vec<serde_json::Value>> {
    let mut tasks = Vec::new();

    for node in nodes {
        let node = node.clone();
        let path = path.to_string();
        let task = tokio::spawn(async move { list_files(&node, &path).await });
        tasks.push(task);
    }

    let mut results = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(data)) => results.push(data),
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(anyhow::anyhow!("Task join failed: {}", e)),
        }
    }

    Ok(results)
}

/// Verify that all file listings are strictly identical across nodes
///
/// This does a byte-for-byte JSON comparison, which includes comparing
/// file IDs, paths, sizes, and timestamps. Now that inode IDs are
/// consensus-coordinated, all metadata should be identical.
///
/// # Arguments
/// * `listings` - Vector of JSON file listings from multiple nodes
///
/// # Returns
/// Ok if all identical, Err with details if any mismatch
pub fn verify_listings_identical(listings: &[serde_json::Value]) -> Result<()> {
    if listings.is_empty() {
        return Err(anyhow::anyhow!("No listings to verify"));
    }

    let first = &listings[0];
    for (i, other) in listings.iter().enumerate().skip(1) {
        if first != other {
            return Err(anyhow::anyhow!(
                "Listing mismatch between node 0 and node {}\nNode 0: {}\nNode {}: {}",
                i,
                serde_json::to_string_pretty(first).unwrap_or_else(|_| "invalid json".to_string()),
                i,
                serde_json::to_string_pretty(other).unwrap_or_else(|_| "invalid json".to_string())
            ));
        }
    }

    Ok(())
}

// ============================================================================
// File Deletion Helpers
// ============================================================================

/// Delete a file on a specific node
///
/// # Arguments
/// * `node` - The node to delete from
/// * `path` - The full file path to delete
pub async fn delete_file(node: &NodeInfo, path: &str) -> Result<()> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/files?path={}",
        node.ip_address, node.port, path
    );

    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("Delete failed with status {}: {}", status, body);
    }

    Ok(())
}

// ============================================================================
// Fragment Distribution Helpers
// ============================================================================

/// Fragment information for distribution diagnostic (orchestrator types)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentInfo {
    pub chunk_number: u32,
    pub local_index: u32,
    pub fragment_id: String,
    pub fragment_hash: String,
    pub chunk_type: String,
    pub nodes_with_fragment: Vec<i32>,
}

/// Complete fragment distribution data for a file diagnostic (orchestrator types)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileFragmentDistribution {
    pub inode_id: String,
    pub data_block_id: String,
    pub file_size: u64,
    pub placement_height: Option<u64>,
    pub fragment_count: u32,
    pub original_count: u32,
    pub recovery_count: u32,
    pub fragments: Vec<FragmentInfo>,
}

/// Trigger fragment inventory self-check on a specific node
///
/// # Arguments
/// * `node` - The node to trigger self-check on
///
/// This triggers the manual fragment inventory sync via POST /maintenance/fragment-inventory-self-check
pub async fn trigger_fragment_inventory_sync(node: &NodeInfo) -> Result<()> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/maintenance/fragment-inventory-self-check",
        node.ip_address, node.port
    );

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!(
            "Fragment inventory sync failed with status {}: {}",
            status,
            body
        );
    }

    Ok(())
}

/// Trigger fragment inventory self-check on all nodes in parallel
///
/// # Arguments
/// * `nodes` - All nodes to trigger self-check on
pub async fn trigger_fragment_inventory_sync_all(nodes: &[NodeInfo]) -> Result<()> {
    let mut tasks = Vec::new();

    for node in nodes {
        let node = node.clone();
        let task = tokio::spawn(async move { trigger_fragment_inventory_sync(&node).await });
        tasks.push(task);
    }

    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(anyhow::anyhow!("Task join failed: {}", e)),
        }
    }

    Ok(())
}

/// Get fragment distribution data for a specific file from a node
///
/// # Arguments
/// * `node` - The node to query
/// * `path` - The unencrypted file path (e.g., "/documents/report.pdf")
///
/// # Returns
/// FileFragmentDistribution with fragment and node information
pub async fn get_fragment_distribution(
    node: &NodeInfo,
    path: &str,
) -> Result<FileFragmentDistribution> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/diagnostics/file-fragments?path={}",
        node.ip_address,
        node.port,
        urlencoding::encode(path)
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!(
            "Get fragment distribution failed with status {}: {}",
            status,
            body
        );
    }

    Ok(response.json().await?)
}

/// Wait for fragment distribution to complete (placement_height to be set)
///
/// # Arguments
/// * `node` - The node to query
/// * `path` - The unencrypted file path
/// * `timeout` - Maximum time to wait
///
/// # Returns
/// FileFragmentDistribution once placement_height is set, or timeout error
pub async fn wait_for_fragment_distribution(
    node: &NodeInfo,
    path: &str,
    timeout: std::time::Duration,
) -> Result<FileFragmentDistribution> {
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            anyhow::bail!("Timeout waiting for fragment distribution to complete");
        }

        match get_fragment_distribution(node, path).await {
            Ok(dist) if dist.placement_height.is_some() => {
                return Ok(dist);
            }
            Ok(_) => {
                // placement_height is still NULL, wait and retry
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => {
                // For 404 errors, the file metadata may not be queryable yet (especially for large files)
                // Retry instead of immediately failing
                let is_404 = e.to_string().contains("404");
                if is_404 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                } else {
                    // Propagate non-404 query errors immediately
                    return Err(e);
                }
            }
        }
    }
}

/// Calculate worst-case failure tolerance for a fragment distribution
///
/// Returns the maximum number of node failures the distribution can tolerate
/// while still being able to recover the file (need at least `required` fragments)
///
/// # Arguments
/// * `node_fragment_counts` - Map of node_id to fragment count
/// * `node_count` - Total number of nodes
/// * `required` - Minimum fragments needed for recovery (10 for Reed-Solomon)
///
/// # Returns
/// Maximum number of failures tolerable in worst case
fn calculate_failure_tolerance(
    node_fragment_counts: &HashMap<i32, usize>,
    node_count: usize,
    required: usize,
) -> usize {
    // Get counts sorted in descending order
    let mut counts: Vec<usize> = node_fragment_counts.values().copied().collect();

    // Pad with zeros for nodes that have no fragments
    while counts.len() < node_count {
        counts.push(0);
    }

    counts.sort_by(|a, b| b.cmp(a)); // Sort descending

    let total: usize = counts.iter().sum();

    // For each possible number of failures, check if we can survive worst case
    for num_failures in 0..node_count {
        // Worst case: the top `num_failures` nodes fail (they have the most fragments)
        let lost: usize = counts.iter().take(num_failures).sum();
        let remaining = total - lost;

        if remaining < required {
            // Can't survive this many failures
            return if num_failures > 0 {
                num_failures - 1
            } else {
                0
            };
        }
    }

    // Can survive all but the last node failing
    node_count - 1
}

/// Verify fragment redundancy properties for a file
///
/// Checks:
/// 1. 2:1 redundancy ratio (recovery_count == 2 * original_count)
/// 2. All fragments (fragment_count) are accounted for
/// 3. Each fragment is stored on at least one node
/// 4. Fragment distribution can tolerate at least consensus_tolerance failures
/// 5. placement_height is set (not NULL)
///
/// # Arguments
/// * `distribution` - The fragment distribution data
/// * `node_count` - Total number of nodes in the mesh
///
/// # Returns
/// Vector of (check_name, passed, detail_message) tuples
pub fn verify_fragment_redundancy(
    distribution: &FileFragmentDistribution,
    node_count: usize,
) -> Vec<(String, bool, String)> {
    let mut checks = Vec::new();

    // Check 1: 2:1 redundancy ratio
    let expected_recovery = distribution.original_count * 2;
    let redundancy_check = distribution.recovery_count == expected_recovery;
    checks.push((
        "2:1 redundancy ratio".to_string(),
        redundancy_check,
        format!(
            "original={} recovery={} (expected {})",
            distribution.original_count, distribution.recovery_count, expected_recovery
        ),
    ));

    // Check 2: Fragment count matches
    let total_fragments = distribution.original_count + distribution.recovery_count;
    let count_check = distribution.fragment_count == total_fragments;
    checks.push((
        "Fragment count matches".to_string(),
        count_check,
        format!(
            "fragment_count={} total={} (original + recovery)",
            distribution.fragment_count, total_fragments
        ),
    ));

    // Check 3: Each fragment is stored on at least one node
    let mut all_fragments_stored = true;
    let mut unstored_fragments = Vec::new();
    for fragment in &distribution.fragments {
        if fragment.nodes_with_fragment.is_empty() {
            all_fragments_stored = false;
            unstored_fragments.push((fragment.chunk_number, fragment.local_index));
        }
    }
    checks.push((
        "All fragments stored".to_string(),
        all_fragments_stored,
        if all_fragments_stored {
            format!(
                "All {} fragments stored on at least one node",
                distribution.fragments.len()
            )
        } else {
            format!(
                "Fragments not stored (chunk_number, local_index): {:?}",
                unstored_fragments
            )
        },
    ));

    // Check 4: Fragment distribution resilience (maximum failure tolerance)
    // Ensures that fragment distribution achieves maximum possible failure tolerance
    // given the network size and Reed-Solomon parameters (10 of 30)
    let mut node_fragment_counts: HashMap<i32, usize> = HashMap::new();
    for fragment in &distribution.fragments {
        for &node_id in &fragment.nodes_with_fragment {
            *node_fragment_counts.entry(node_id).or_insert(0) += 1;
        }
    }

    let max_fragments_per_node = node_fragment_counts.values().max().copied().unwrap_or(0);

    // Calculate worst-case failure tolerance: how many nodes can fail before we can't recover?
    // With Reed-Solomon (10 of 30), we need at least 10 fragments to remain
    let actual_tolerance = calculate_failure_tolerance(&node_fragment_counts, node_count, 10);

    // Calculate maximum possible failure tolerance based on network size
    // With 30 total fragments needing 10 to recover, we can lose up to 20 fragments
    // For smaller networks, must account for uneven distribution (worst case):
    //   - With uneven distribution, max fragments per node = ceil(30/n)
    //   - Can lose up to 20 fragments total (30 - 10 required)
    //   - Max failures = floor(20 / max_fragments_per_node)
    let max_possible_tolerance = if node_count >= 30 {
        // For 30+ nodes, the theoretical maximum is 20 failures
        // (can lose 20 out of 30 fragments and still have 10)
        20
    } else {
        // For smaller networks, calculate based on worst-case uneven distribution
        // Max fragments any single node can have with 30 total fragments
        let fragments_per_node_max = ((30.0 / node_count as f64).ceil()) as usize;
        // Can lose up to 20 fragments (30 - 10 required for reconstruction)
        let max_fragment_losses = 20;
        // In worst case, we lose nodes with the most fragments first
        max_fragment_losses / fragments_per_node_max
    };

    let balance_check = actual_tolerance >= max_possible_tolerance;

    // Create a detailed breakdown of distribution across nodes
    let mut node_counts: Vec<(i32, usize)> = node_fragment_counts
        .iter()
        .map(|(&node_id, &count)| (node_id, count))
        .collect();
    node_counts.sort_by_key(|(node_id, _)| *node_id);

    let distribution_detail = if node_counts.is_empty() {
        "No fragments found in inventory".to_string()
    } else {
        let counts_str = node_counts
            .iter()
            .map(|(node_id, count)| format!("node_{}={}", node_id, count))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "{} (tolerates {} failures, maximum possible ≥{})",
            counts_str, actual_tolerance, max_possible_tolerance
        )
    };

    checks.push((
        "Fragment distribution resilience".to_string(),
        balance_check,
        distribution_detail,
    ));

    // Check 5: placement_height is set
    let placement_check = distribution.placement_height.is_some();
    checks.push((
        "Placement height set".to_string(),
        placement_check,
        format!("placement_height={:?}", distribution.placement_height),
    ));

    checks
}

// ============================================================================
// Range Download Helpers
// ============================================================================

/// Response metadata from a download request
pub struct DownloadHeaders {
    pub status: u16,
    pub content_type: Option<String>,
    pub accept_ranges: Option<String>,
    pub content_length: Option<u64>,
    pub content_range: Option<String>,
}

/// Download a file and return response headers (full download, no Range header)
pub async fn download_file_headers(
    node: &NodeInfo,
    path: &str,
) -> Result<(Vec<u8>, DownloadHeaders)> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let path_trimmed = path.strip_prefix('/').unwrap_or(path);
    let url = format!(
        "http://{}:{}/api/files/{}",
        node.ip_address, node.port, path_trimmed
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    let headers = DownloadHeaders {
        status: response.status().as_u16(),
        content_type: response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        accept_ranges: response
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        content_length: response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok()),
        content_range: response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
    };

    let bytes = response.bytes().await?.to_vec();
    Ok((bytes, headers))
}

/// Download a file with a Range header and return status, headers, and body bytes
pub async fn download_file_range(
    node: &NodeInfo,
    path: &str,
    start: u64,
    end: Option<u64>,
) -> Result<(Vec<u8>, DownloadHeaders)> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let path_trimmed = path.strip_prefix('/').unwrap_or(path);
    let url = format!(
        "http://{}:{}/api/files/{}",
        node.ip_address, node.port, path_trimmed
    );

    let range_value = match end {
        Some(e) => format!("bytes={}-{}", start, e),
        None => format!("bytes={}-", start),
    };

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .header("Range", range_value)
        .send()
        .await?;

    let headers = DownloadHeaders {
        status: response.status().as_u16(),
        content_type: response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        accept_ranges: response
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        content_length: response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok()),
        content_range: response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
    };

    let bytes = response.bytes().await?.to_vec();
    Ok((bytes, headers))
}
