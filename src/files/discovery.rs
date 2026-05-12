use crate::{
    files::placement::FragmentType,
    net::IrohTransport,
    types::{Blake3Hash, NodeConnectionInfo},
};
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

/// Try to fetch a fragment from a specific node over iroh
pub async fn try_fetch_from_node(
    fragment_hash: &Blake3Hash,
    node: &NodeConnectionInfo,
    iroh_transport: &IrohTransport,
) -> Result<Vec<u8>, DiscoveryError> {
    let iroh_node_id = node.pubkey.to_iroh_node_id();

    let fragment_data = crate::files::rpc::fetch_fragment(
        iroh_transport,
        node.node_id,
        iroh_node_id,
        *fragment_hash,
    )
    .await
    .map_err(|e| {
        use crate::net::transport::ProtocolError;
        match &e {
            crate::net::IrohError::Protocol(ProtocolError::PeerError(msg))
                if msg == "fragment not found" =>
            {
                DiscoveryError::NotFound
            }
            _ => DiscoveryError::Network(e.to_string()),
        }
    })?;

    // Defense in depth: verify hash even though server already verified
    let actual_hash = Blake3Hash::new(blake3::hash(&fragment_data));
    if actual_hash != *fragment_hash {
        return Err(DiscoveryError::Network(
            "Fragment hash mismatch".to_string(),
        ));
    }

    tracing::debug!(
        "Successfully fetched fragment {} from node {} via iroh",
        fragment_hash.to_hex(),
        node.node_id
    );

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
    match crate::files::rpc::check_fragment_health(
        iroh_transport,
        node.node_id,
        iroh_node_id,
        *fragment_hash,
    )
    .await
    {
        Ok(healthy) => {
            tracing::debug!(
                "Health check for fragment {} on node {}: {}",
                fragment_hash.to_hex(),
                node.node_id,
                if healthy { "HAS" } else { "MISSING" }
            );
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
    iroh_transport: &IrohTransport,
    inventory_hint: Option<Vec<NodeConnectionInfo>>,
) -> Result<Vec<u8>, DiscoveryError> {
    // Phase 0: Try fragment inventory nodes first (PRIMARY lookup mechanism)
    if let Some(inventory_nodes) = inventory_hint
        && !inventory_nodes.is_empty() {
            tracing::debug!(
                "Trying {} inventory nodes for fragment {}",
                inventory_nodes.len(),
                fragment_hash.to_hex()
            );

            if let Ok(data) =
                try_reactive_discovery_and_fetch(fragment_hash, &inventory_nodes, iroh_transport)
                    .await
            {
                tracing::debug!("Fragment {} found via inventory!", fragment_hash.to_hex());
                return Ok(data);
            }

            tracing::debug!("Inventory nodes failed, falling back to network-wide search");
        }

    // Phase 1: Fallback to reactive discovery across all available nodes
    let discovery_nodes = match nodes {
        Either::Left(node_list) => node_list,
        Either::Right(node_metrics) => node_metrics
            .into_iter()
            .map(|m| NodeConnectionInfo {
                node_id: m.node_id,
                pubkey: m.pubkey,
            })
            .collect(),
    };

    if !discovery_nodes.is_empty() {
        tracing::debug!(
            "Trying reactive discovery across {} nodes",
            discovery_nodes.len()
        );
        if let Ok(data) =
            try_reactive_discovery_and_fetch(fragment_hash, &discovery_nodes, iroh_transport).await
        {
            return Ok(data);
        }
    }

    Err(DiscoveryError::NotFound)
}

/// Reactive discovery and fetch: health check over iroh, then fetch over iroh as nodes respond
async fn try_reactive_discovery_and_fetch(
    fragment_hash: &Blake3Hash,
    nodes: &[NodeConnectionInfo],
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
            let has_fragment = try_ask_node_for_fragment(&fragment_hash, &node_info, &transport)
                .await
                .unwrap_or(false);

            if has_fragment {
                let _ = tx.send(node_info);
            }
        });
    }
    drop(health_tx);

    // Process results as they flow in.
    // Track in-flight downloads to detect when all work is exhausted — the original
    // download_tx keeps the channel alive, so we can't rely on `else` in select!.
    let mut health_done = false;
    let mut in_flight = 0usize;

    loop {
        if health_done && in_flight == 0 {
            break;
        }

        tokio::select! {
            // New node found with fragment - start download immediately over iroh
            result = health_rx.recv(), if !health_done => {
                match result {
                    Some(node_info) => {
                        tracing::debug!("Node {} reports having fragment, starting download", node_info.node_id);

                        let tx = download_tx.clone();
                        let fragment_hash = *fragment_hash;
                        let transport = iroh_transport.clone();
                        in_flight += 1;

                        tokio::spawn(async move {
                            let result = try_fetch_from_node(
                                &fragment_hash,
                                &node_info,
                                &transport,
                            ).await;
                            let _ = tx.send(result);
                        });
                    }
                    None => {
                        health_done = true;
                    }
                }
            }

            // Download completed
            Some(download_result) = download_rx.recv(), if in_flight > 0 => {
                in_flight -= 1;
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
        }
    }

    Err(DiscoveryError::NotFound)
}
