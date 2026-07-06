//! Host implementations of the hopnet-storage engine seams (RFC-014).
//!
//! One adapter over AppState implements all four traits: `Transport` (iroh
//! fragment RPC), `StateReader` (scoped pool checkouts over replicated
//! state), `TxSubmitter` (sign + consensus queue), and `LocalStateSink`
//! (write-gate drain channel). The substrate never sees iroh, r2d2, or
//! consensus types — everything narrows to the seam vocabulary here.

use crate::AppState;
use crate::consensus::queue::ConsensusSubmitError;
use crate::db::write_gate::LocalStateUpdate;
use crate::types::Blake3Hash;
use hopnet_storage::BlobId;
use hopnet_storage::StorageError;
use hopnet_storage::engine::{EngineConfig, EngineHandle, Seams};
use hopnet_storage::store::DistributableBlob;
use hopnet_storage::traits::{
    LocalStateSink, PeerRef, PlacementInputs, StateReader, StoreResult, SubmitError, Transport,
    TransportError, TxSubmitter,
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
            transport: host.clone(),
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
) -> hopnet_storage::api::GetNet<SubstrateHost, SubstrateHost, SubstrateHost> {
    let host = Arc::new(SubstrateHost::new(app_state.clone()));
    hopnet_storage::api::GetNet {
        transport: host.clone(),
        state: host.clone(),
        local_state: host,
    }
}

fn map_iroh_error(e: crate::net::IrohError) -> TransportError {
    match e {
        crate::net::IrohError::Protocol(crate::net::transport::ProtocolError::PeerError(msg)) => {
            TransportError::Peer(msg)
        }
        other => TransportError::Transport(other.to_string()),
    }
}

fn peer_iroh_key(peer: &PeerRef) -> Result<iroh::PublicKey, TransportError> {
    iroh::PublicKey::from_bytes(&peer.pubkey)
        .map_err(|e| TransportError::Transport(format!("invalid peer pubkey: {e}")))
}

impl Transport for SubstrateHost {
    async fn store_fragment(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
        data: Vec<u8>,
    ) -> Result<StoreResult, TransportError> {
        let peer_key = peer_iroh_key(peer)?;
        let result = crate::files::rpc::store_fragment_remote(
            &self.app_state.iroh_transport,
            peer.node_id,
            peer_key,
            *fragment_hash,
            data,
        )
        .await
        .map_err(map_iroh_error)?;
        if result.success {
            Ok(StoreResult {
                already_existed: result.already_existed,
            })
        } else {
            // success=false shouldn't happen (errors come via
            // IrohResponse::Error) — classify as peer-side so the engine's
            // domain retry covers it.
            Err(TransportError::Peer(
                "fragment store returned success=false".to_string(),
            ))
        }
    }

    async fn fetch_fragment(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
    ) -> Result<Vec<u8>, TransportError> {
        let peer_key = peer_iroh_key(peer)?;
        crate::files::rpc::fetch_fragment(
            &self.app_state.iroh_transport,
            peer.node_id,
            peer_key,
            *fragment_hash,
        )
        .await
        .map_err(map_iroh_error)
    }

    async fn fragment_health(
        &self,
        peer: &PeerRef,
        fragment_hash: &Blake3Hash,
    ) -> Result<bool, TransportError> {
        let peer_key = peer_iroh_key(peer)?;
        crate::files::rpc::check_fragment_health(
            &self.app_state.iroh_transport,
            peer.node_id,
            peer_key,
            *fragment_hash,
        )
        .await
        .map_err(map_iroh_error)
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
