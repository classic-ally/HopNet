//! Host implementations of the hopnet-storage engine seams (RFC-014).
//!
//! One adapter over AppState implements `StateReader` (scoped pool checkouts
//! over replicated state), `TxSubmitter` (sign + consensus queue), and
//! `LocalStateSink` (write-gate drain channel). The `Transport` seam is no
//! longer host code (RFC-017 Stage 2): the substrate's own
//! `rpc::RpcTransport` implements it over comms — the host contributes only
//! the comms handle. The substrate never sees iroh, r2d2, or consensus
//! types — everything narrows to the seam vocabulary here.

use crate::AppState;
use crate::consensus::queue::ConsensusSubmitError;
use crate::db::write_gate::LocalStateUpdate;
use crate::types::Blake3Hash;
use hopnet_storage::BlobId;
use hopnet_storage::StorageError;
use hopnet_storage::engine::{EngineConfig, EngineHandle, Seams};
use hopnet_storage::rpc::RpcTransport;
use hopnet_storage::store::DistributableBlob;
use hopnet_storage::traits::{
    LocalStateSink, PeerRef, PlacementInputs, StateReader, StorageView, SubmitError, TxSubmitter,
};
use std::sync::Arc;

pub struct SubstrateHost {
    app_state: AppState,
}

impl SubstrateHost {
    pub fn new(app_state: AppState) -> Self {
        SubstrateHost { app_state }
    }
}

/// Spawn the storage distribution engine behind the host seams and install
/// its handle in AppState (idempotent; mirrors the malachite OnceCell).
/// Data-plane workers land on the CALLER's runtime (main — fragment sends);
/// the placement batcher lands on the consensus queue runtime.
pub fn spawn_storage_engine(app_state: &AppState) {
    if app_state.storage.get().is_some() {
        return;
    }
    let host = Arc::new(SubstrateHost::new(app_state.clone()));
    let handle = EngineHandle::spawn(
        Seams {
            transport: Arc::new(RpcTransport {
                rpc: app_state.comms.clone(),
            }),
            state: host.clone(),
            submitter: host.clone(),
            local_state: host,
        },
        EngineConfig {
            fragments_dir: app_state.fragments_dir.clone(),
        },
        tokio::runtime::Handle::current(),
        crate::consensus::queue::queue_rt().handle().clone(),
    );
    // Lost race = another spawner won; their engine is equivalent.
    let _ = app_state.storage.set(handle);
}

/// Seam bundle for the crate's get path (api::get) — one shared adapter
/// behind all three capabilities.
pub fn get_net(
    app_state: &AppState,
) -> hopnet_storage::api::GetNet<RpcTransport<hopnet_comms::IrohComms>, SubstrateHost, SubstrateHost>
{
    let host = Arc::new(SubstrateHost::new(app_state.clone()));
    hopnet_storage::api::GetNet {
        transport: Arc::new(RpcTransport {
            rpc: app_state.comms.clone(),
        }),
        state: host.clone(),
        local_state: host,
    }
}

impl StateReader for SubstrateHost {
    fn placement_inputs(&self) -> Result<PlacementInputs, StorageError> {
        // One scoped checkout for all placement inputs, dropped at return —
        // height, validators, and metrics read at the same instant so
        // placement is computed against one consistent state.
        let conn = self
            .app_state
            .db_pool
            .get()
            .map_err(|e| StorageError::Host(format!("pool checkout: {e}")))?;
        let height = crate::db::consensus::get_current_consensus_height(&conn)
            .map_err(|e| StorageError::Host(format!("current height: {e:?}")))?;
        let validators = crate::db::consensus::get_validators_with_conn(&conn, height)
            .map_err(|e| StorageError::Host(format!("validators: {e:?}")))?;
        let metrics = crate::db::metrics::get_all_node_metrics_with_conn(&conn, height)
            .map_err(|e| StorageError::Host(format!("node metrics: {e:?}")))?;
        Ok(PlacementInputs {
            height,
            validators: validators
                .into_iter()
                .map(|n| PeerRef {
                    node_id: n.node_id,
                    pubkey: n.pubkey.0.to_bytes(),
                })
                .collect(),
            metrics: metrics.into_iter().map(Into::into).collect(),
        })
    }

    fn placement_inputs_at(&self, height: i32) -> Result<PlacementInputs, StorageError> {
        let conn = self
            .app_state
            .db_pool
            .get()
            .map_err(|e| StorageError::Host(format!("pool checkout: {e}")))?;
        let validators = crate::db::consensus::get_validators_with_conn(&conn, height)
            .map_err(|e| StorageError::Host(format!("validators at {height}: {e:?}")))?;
        let metrics = crate::db::metrics::get_all_node_metrics_with_conn(&conn, height)
            .map_err(|e| StorageError::Host(format!("node metrics at {height}: {e:?}")))?;
        Ok(PlacementInputs {
            height,
            validators: validators
                .into_iter()
                .map(|n| PeerRef {
                    node_id: n.node_id,
                    pubkey: n.pubkey.0.to_bytes(),
                })
                .collect(),
            metrics: metrics.into_iter().map(Into::into).collect(),
        })
    }

    fn storage_view(&self) -> Result<StorageView, StorageError> {
        use hopnet_storage::membership;
        // One scoped checkout: height, mesh policy, node universe + weights
        // (via the anchored metrics scoring), and availability history all
        // read against one consistent state — the derivation below is pure,
        // so every node computes the same view from the same rows.
        let conn = self
            .app_state
            .db_pool
            .get()
            .map_err(|e| StorageError::Host(format!("pool checkout: {e}")))?;
        let height = crate::db::consensus::get_current_consensus_height(&conn)
            .map_err(|e| StorageError::Host(format!("current height: {e:?}")))?;
        let policy = hopnet_storage::store::read_policy(&conn)
            .map_err(|e| StorageError::Host(format!("storage policy: {e}")))?;
        let node_metrics = crate::db::metrics::get_all_node_metrics_with_conn(&conn, height)
            .map_err(|e| StorageError::Host(format!("node metrics: {e:?}")))?;
        let grid = crate::db::metrics::get_availability_history_with_conn(
            &conn,
            height,
            4320, // 30 days of buckets at the default step
            policy.availability_step_secs,
        )
        .map_err(|e| StorageError::Host(format!("availability history: {e:?}")))?;
        drop(conn);

        let cold_tier = policy.decay_tiers[policy.decay_tiers.len().saturating_sub(2)];
        let mut tiers = std::collections::HashMap::new();
        let mut absence = std::collections::HashMap::new();
        for (node_id, samples) in &grid.per_node {
            let spans = membership::offline_spans(samples, grid.step_secs);
            tiers.insert(
                *node_id,
                membership::derive_tier(
                    &spans,
                    &policy.decay_tiers,
                    membership::TIER_MIN_HISTORY,
                ),
            );
            absence.insert(
                *node_id,
                membership::current_absence(samples, grid.step_secs),
            );
        }

        // Node universe = every registered node (the metrics scoring query
        // is FROM nodes, so unmeasured nodes appear with defaults). Nodes
        // with no availability grid have absence 0 (presence bias) and the
        // cold tier.
        let node_ids: Vec<i32> = node_metrics.iter().map(|m| m.node_id).collect();
        let member_ids = membership::storage_members(&node_ids, &absence, &tiers, cold_tier);
        let online: Vec<i32> = node_ids
            .iter()
            .copied()
            .filter(|n| absence.get(n).copied().unwrap_or(0) == 0)
            .collect();
        let member_set: std::collections::HashSet<i32> = member_ids.iter().copied().collect();
        let watermark =
            membership::watermark_with(member_ids.len(), &policy.watermark_params());

        let mut members = Vec::with_capacity(member_ids.len());
        let mut weights = std::collections::HashMap::new();
        let mut rows = Vec::with_capacity(node_metrics.len());
        for m in node_metrics {
            let node_id = m.node_id;
            let pubkey = m.pubkey.0.to_bytes();
            let row: hopnet_storage::placement::MetricsRow = m.into();
            weights.insert(node_id, hopnet_storage::placement::quantized_weight(&row));
            if member_set.contains(&node_id) {
                members.push(PeerRef { node_id, pubkey });
            }
            rows.push(row);
        }

        Ok(StorageView {
            height,
            members,
            tiers,
            weights,
            watermark,
            online,
            metrics: rows,
        })
    }

    fn fragment_sources(
        &self,
        fragment_hashes: &[Blake3Hash],
    ) -> Result<std::collections::HashMap<Blake3Hash, Vec<PeerRef>>, StorageError> {
        let sources = crate::db::inventory::batch_query_fragment_inventory(
            self.app_state.db_pool.get(),
            fragment_hashes,
            None,
        )
        .map_err(|e| StorageError::Host(format!("fragment inventory: {e:?}")))?;
        Ok(sources
            .into_iter()
            .map(|(hash, nodes)| {
                (
                    hash,
                    nodes
                        .into_iter()
                        .map(|n| PeerRef {
                            node_id: n.node_id,
                            pubkey: n.pubkey.0.to_bytes(),
                        })
                        .collect(),
                )
            })
            .collect())
    }

    fn all_peers(&self) -> Result<Vec<PeerRef>, StorageError> {
        let my_node_id = self
            .app_state
            .get_node_id()
            .map_err(|_| StorageError::Host("node id not set".to_string()))?;
        let nodes = crate::db::nodes::get_all_nodes_as_connection_info(
            self.app_state.db_pool.get(),
            my_node_id,
        )
        .map_err(|e| StorageError::Host(format!("gossip nodes: {e:?}")))?;
        Ok(nodes
            .into_iter()
            .map(|n| PeerRef {
                node_id: n.node_id,
                pubkey: n.pubkey.0.to_bytes(),
            })
            .collect())
    }

    fn distributable_blob(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<DistributableBlob>, StorageError> {
        let conn = self
            .app_state
            .db_pool
            .get()
            .map_err(|e| StorageError::Host(format!("pool checkout: {e}")))?;
        hopnet_storage::store::get_distributable_blob(&conn, blob_id)
    }

    fn blob_manifest(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<hopnet_storage::store::BlobManifest>, StorageError> {
        let conn = self
            .app_state
            .db_pool
            .get()
            .map_err(|e| StorageError::Host(format!("pool checkout: {e}")))?;
        hopnet_storage::store::blob_manifest(&conn, blob_id)
    }

    fn local_node_id(&self) -> Option<i32> {
        self.app_state.node_id.get().copied()
    }
}

impl TxSubmitter for SubstrateHost {
    async fn submit(&self, function: &'static str, payload: Vec<u8>) -> Result<(), SubmitError> {
        // Sign at submit time — fresh nonce per attempt. Signing failure is
        // permanent from the engine's perspective (node identity missing).
        let transaction = crate::consensus::dispatch::create_signed_transaction(
            &self.app_state,
            function.to_string(),
            payload,
        )
        .map_err(|e| SubmitError::Rejected(format!("signing failed: {e:?}")))?;
        match self.app_state.consensus_queue.submit(transaction).await {
            Ok(()) => Ok(()),
            Err(ConsensusSubmitError::Rejected(reason)) => Err(SubmitError::Rejected(reason)),
            Err(e) => Err(SubmitError::Transient(format!("{e:?}"))),
        }
    }
}

impl LocalStateSink for SubstrateHost {
    fn mark_local(&self, fragment_hash: Blake3Hash) {
        if let Err(e) = self
            .app_state
            .local_state_tx
            .try_send(LocalStateUpdate::MarkLocal { fragment_hash })
        {
            tracing::warn!(
                "Local state queue full, dropping mark-local for {}: {}",
                fragment_hash.to_hex(),
                e
            );
        }
    }

    fn mark_remote_batch(&self, fragment_hashes: Vec<Blake3Hash>) {
        if let Err(e) = self
            .app_state
            .local_state_tx
            .try_send(LocalStateUpdate::MarkRemoteBatch { fragment_hashes })
        {
            tracing::warn!("Local state queue full, dropping mark-remote batch: {}", e);
        }
    }
}
