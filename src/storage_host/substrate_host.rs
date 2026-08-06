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

    fn placement_inputs_at(&self, height: u64) -> Result<PlacementInputs, StorageError> {
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
        // One scoped checkout: height, mesh policy, node universe + weights
        // (via the anchored metrics scoring), and availability history all
        // read against one consistent state — the derivation below is pure,
        // so every node computes the same view from the same rows.
        let conn = self
            .app_state
            .db_pool
            .get()
            .map_err(|e| StorageError::Host(format!("pool checkout: {e}")))?;
        storage_view_with_conn(&conn)
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

/// Derive the storage member view from an already-checked-out connection.
///
/// Extracted verbatim from `SubstrateHost::storage_view` so callers holding a
/// pool but no `AppState` — the snapshotter, and the read-only diagnostics
/// views — can reach the same derivation. This feeds `select_nodes_for_blob`,
/// so it is a behaviour-preserving move and nothing more.
pub fn storage_view_with_conn(conn: &rusqlite::Connection) -> Result<StorageView, StorageError> {
    use hopnet_storage::membership;

    let height = crate::db::consensus::get_current_consensus_height(conn)
        .map_err(|e| StorageError::Host(format!("current height: {e:?}")))?;
    // Active quorum profile (consensus_meta) — sizes the watermark fault
    // budget so a majority-profile mesh buffers the burst consensus
    // actually survives. Defaults to AUTO, matching the mesh default.
    let profile =
        hopnet_consensus::store::meta_get(conn, hopnet_consensus::store::META_QUORUM_PROFILE)
            .ok()
            .flatten()
            .and_then(|b| String::from_utf8(b).ok())
            .and_then(|s| hopnet_consensus::QuorumProfile::parse(&s))
            .unwrap_or(hopnet_consensus::QuorumProfile::Auto);
    let policy = hopnet_storage::store::read_policy(conn)
        .map_err(|e| StorageError::Host(format!("storage policy: {e}")))?;
    let node_metrics = crate::db::metrics::get_all_node_metrics_with_conn(conn, height)
        .map_err(|e| StorageError::Host(format!("node metrics: {e:?}")))?;
    let grid = crate::db::metrics::get_availability_history_with_conn(
        conn,
        height,
        4320, // 30 days of buckets at the default step
        policy.availability_step_secs,
    )
    .map_err(|e| StorageError::Host(format!("availability history: {e:?}")))?;

    // Map the DB rows to the host-agnostic kernel input, then derive the
    // view with a pure function (membership::derive_view) — same rows
    // yield the same view on every node. The validator set is NOT read
    // here: storage membership derives from availability, not from who
    // validates (RFC-STORAGE-001 three-timescale design).
    let nodes: Vec<membership::ViewNode> = node_metrics
        .into_iter()
        .map(|m| membership::ViewNode {
            node_id: m.node_id,
            pubkey: m.pubkey.0.to_bytes(),
            metrics: m.into(),
        })
        .collect();

    Ok(membership::derive_view(
        height,
        nodes,
        &grid.per_node,
        grid.step_secs,
        &policy,
        profile,
    ))
}
