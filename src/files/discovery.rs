use crate::{
    NodeAuthInfo,
    types::{Blake3Hash, NodeConnectionInfo},
    files::placement::FragmentType,
    net::IrohTransport,
};
use ed25519_dalek::Signer;
use either::Either;

#[derive(Debug)]
pub enum DiscoveryError {
    Database(crate::db::DatabaseError),
    Network(String),
    NotFound,
}

impl From<crate::db::DatabaseError> for DiscoveryError {
    fn from(e: crate::db::DatabaseError) -> Self {
        DiscoveryError::Database(e)
    }
}

/// Try to fetch a fragment from a specific node
pub async fn try_fetch_from_node(
    fragment_hash: &Blake3Hash,
    node: &NodeConnectionInfo,
    auth: &NodeAuthInfo,
) -> Result<Vec<u8>, DiscoveryError> {
    let url = format!("http://{}:{}/fragments/{}", node.ip_address, node.port, fragment_hash.to_hex());
    
    // Create HTTP client with timeout
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|e| DiscoveryError::Network(format!("HTTP client error: {}", e)))?;
    
    // Sign empty body for GET request authentication (same pattern as metrics collector)
    let body = b"";
    let node_signature = auth.node_private_key.sign(body);
    let user_signature = auth.user_keys.private_key.sign(body);
    
    // Make authenticated request with cryptographic signatures
    let response = client
        .get(&url)
        .header("X-Node-ID", auth.node_id.to_string())
        .header("X-User-ID", auth.user_id.to_string())
        .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
        .header("X-User-Signature", hex::encode(user_signature.to_bytes()))
        .send()
        .await
        .map_err(|e| DiscoveryError::Network(format!("Request failed: {}", e)))?;
    
    if !response.status().is_success() {
        return Err(DiscoveryError::NotFound);
    }
    
    // Get fragment data
    let fragment_data = response
        .bytes()
        .await
        .map_err(|e| DiscoveryError::Network(format!("Failed to read response: {}", e)))?
        .to_vec();
    
    // Verify fragment hash
    let actual_hash = Blake3Hash::new(blake3::hash(&fragment_data));
    if actual_hash != *fragment_hash {
        return Err(DiscoveryError::Network("Fragment hash mismatch".to_string()));
    }
    
    tracing::debug!("Successfully fetched fragment {} from node {} ({}:{})",
                   fragment_hash.to_hex(), node.node_id, node.ip_address, node.port);

    Ok(fragment_data)
}

/// Ask a node if it has a fragment (health check over iroh).
/// Timeout is owned by check_fragment_health (500ms stream I/O + 10s connection budget).
pub async fn try_ask_node_for_fragment(
    fragment_hash: &Blake3Hash,
    node: &NodeConnectionInfo,
    iroh_transport: &IrohTransport,
) -> Result<bool, DiscoveryError> {
    let iroh_node_id = node.pubkey.to_iroh_node_id();
    match crate::files::rpc::check_fragment_health(iroh_transport, node.node_id, iroh_node_id, *fragment_hash).await {
        Ok(healthy) => {
            tracing::debug!("Health check for fragment {} on node {}: {}",
                fragment_hash.to_hex(), node.node_id,
                if healthy { "HAS" } else { "MISSING" });
            Ok(healthy)
        }
        Err(e) => Err(DiscoveryError::Network(e.to_string())),
    }
}

/// Main fragment discovery function with inventory-first pattern
///
/// Accepts either:
/// - Left(Vec<NodeConnectionInfo>): Direct node list for reactive discovery
/// - Right(Vec<NodeMetrics>): Node metrics (converted to NodeConnectionInfo for discovery)
pub async fn find_fragment(
    fragment_hash: &Blake3Hash,
    _fragment_type: FragmentType,
    nodes: Either<Vec<NodeConnectionInfo>, Vec<crate::db::metrics::NodeMetrics>>,
    auth: &NodeAuthInfo,
    iroh_transport: &IrohTransport,
    inventory_hint: Option<Vec<NodeConnectionInfo>>,
) -> Result<Vec<u8>, DiscoveryError> {
    // Phase 0: Try fragment inventory nodes first (PRIMARY lookup mechanism)
    if let Some(inventory_nodes) = inventory_hint {
        if !inventory_nodes.is_empty() {
            tracing::debug!("Trying {} inventory nodes for fragment {}",
                          inventory_nodes.len(), fragment_hash.to_hex());

            if let Ok(data) = try_reactive_discovery_and_fetch(fragment_hash, &inventory_nodes, auth, iroh_transport).await {
                tracing::debug!("Fragment {} found via inventory!", fragment_hash.to_hex());
                return Ok(data);
            }

            tracing::debug!("Inventory nodes failed, falling back to network-wide search");
        }
    }

    // Phase 1: Fallback to reactive discovery across all available nodes
    let discovery_nodes = match nodes {
        Either::Left(node_list) => node_list,
        Either::Right(node_metrics) => {
            node_metrics.into_iter()
                .map(|m| NodeConnectionInfo {
                    node_id: m.node_id,
                    ip_address: m.ip_address,
                    port: m.port,
                    pubkey: m.pubkey,
                })
                .collect()
        }
    };

    if !discovery_nodes.is_empty() {
        tracing::debug!("Trying reactive discovery across {} nodes", discovery_nodes.len());
        if let Ok(data) = try_reactive_discovery_and_fetch(fragment_hash, &discovery_nodes, auth, iroh_transport).await {
            return Ok(data);
        }
    }

    Err(DiscoveryError::NotFound)
}

/// Reactive discovery and fetch: health check over iroh + download over HTTP as nodes respond
async fn try_reactive_discovery_and_fetch(
    fragment_hash: &Blake3Hash,
    nodes: &[NodeConnectionInfo],
    auth: &NodeAuthInfo,
    iroh_transport: &IrohTransport,
) -> Result<Vec<u8>, DiscoveryError> {
    let (health_tx, mut health_rx) = tokio::sync::mpsc::unbounded_channel();
    let (download_tx, mut download_rx) = tokio::sync::mpsc::unbounded_channel();

    // Spawn health check tasks for all nodes (over iroh)
    for node in nodes {
        let tx = health_tx.clone();
        let fragment_hash = *fragment_hash;
        let node_info = node.clone();
        let transport = iroh_transport.clone();

        tokio::spawn(async move {
            let has_fragment = try_ask_node_for_fragment(
                &fragment_hash,
                &node_info,
                &transport,
            ).await.unwrap_or(false);

            if has_fragment {
                let _ = tx.send(node_info);
            }
        });
    }
    drop(health_tx);

    // Process results as they flow in
    loop {
        tokio::select! {
            // New node found with fragment - start download immediately (HTTP for now)
            Some(node_info) = health_rx.recv() => {
                tracing::debug!("Node {} reports having fragment, starting download", node_info.node_id);

                let tx = download_tx.clone();
                let fragment_hash = *fragment_hash;
                let auth_clone = auth.clone();

                tokio::spawn(async move {
                    let result = try_fetch_from_node(
                        &fragment_hash,
                        &node_info,
                        &auth_clone,
                    ).await;
                    let _ = tx.send(result);
                });
            }

            // Download completed
            Some(download_result) = download_rx.recv() => {
                match download_result {
                    Ok(data) => {
                        tracing::debug!("Successfully downloaded fragment {}", fragment_hash.to_hex());
                        return Ok(data);
                    }
                    Err(e) => {
                        tracing::debug!("Download failed: {:?}, continuing with other candidates", e);
                    }
                }
            }

            else => break,
        }
    }

    Err(DiscoveryError::NotFound)
}