use crate::{
    db::DatabaseError,
    handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta},
    storage_host::db_apply::delete_orphaned_data_blocks_consensus,
};
use hopnet_storage::DeleteOrphanedDataBlocksPayload;

/// The storage substrate's consensus tx functions, registered from the
/// HOST (not hopnet-storage): the layering is projection → storage, so
/// storage can't see the handler seam; and delete_orphaned_data_blocks
/// consults the takeout gate, which storage could never depend on. The
/// boot tripwire asserts these alongside every manifest's tx_functions.
pub const TX_FUNCTIONS: &[&str] = &[
    "update_placement_heights",
    "delete_orphaned_data_blocks",
    "self_check_fragments",
];

pub struct UpdatePlacementHeightsHandler;

impl TransactionHandler for UpdatePlacementHeightsHandler {
    fn name(&self) -> &'static str {
        "update_placement_heights"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        // Storage-owned tx (RFC-014): payload type and apply both live in
        // the substrate crate; this shim only decodes and delegates.
        match bincode::serde::decode_from_slice::<Vec<hopnet_storage::PlacementUpdate>, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((updates, _)) => {
                let crate_updates: Vec<(hopnet_storage::BlobId, i32)> = updates
                    .into_iter()
                    .map(|u| (u.blob_id, u.placement_height))
                    .collect();
                hopnet_storage::store::apply_placement_commit(db_tx, &crate_updates).map_err(
                    |e| {
                        tracing::error!("apply_placement_commit failed: {e}");
                        DatabaseError::ProcessingError
                    },
                )?;
                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &UpdatePlacementHeightsHandler as &dyn TransactionHandler
}

pub struct DeleteOrphanedDataBlocksHandler;

impl TransactionHandler for DeleteOrphanedDataBlocksHandler {
    fn name(&self) -> &'static str {
        "delete_orphaned_data_blocks"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<DeleteOrphanedDataBlocksPayload, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((payload_data, _)) => {
                let deleted_fragment_hashes =
                    delete_orphaned_data_blocks_consensus(db_tx, payload_data.data_block_ids)?;

                // If executing, opportunistically delete local fragment files
                if execute && !deleted_fragment_hashes.is_empty() {
                    tracing::info!(
                        "Opportunistically cleaning up {} local fragment files",
                        deleted_fragment_hashes.len()
                    );

                    let mut successfully_deleted = 0;
                    for fragment_hash in &deleted_fragment_hashes {
                        match crate::storage_host::functions::delete_fragment(
                            ctx.fragments_dir,
                            fragment_hash,
                        ) {
                            Ok(()) => {
                                successfully_deleted += 1;
                                tracing::debug!(
                                    "Deleted local fragment file: {}",
                                    fragment_hash.to_hex()
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to delete local fragment file {}: {:?}",
                                    fragment_hash.to_hex(),
                                    e
                                );
                                // Continue with other deletions - this fragment will be caught by filesystem cleanup job
                            }
                        }
                    }

                    tracing::info!(
                        "Successfully deleted {}/{} local fragment files",
                        successfully_deleted,
                        deleted_fragment_hashes.len()
                    );
                }

                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &DeleteOrphanedDataBlocksHandler as &dyn TransactionHandler
}

pub struct SelfCheckFragmentsHandler;

impl TransactionHandler for SelfCheckFragmentsHandler {
    fn name(&self) -> &'static str {
        "self_check_fragments"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<hopnet_storage::SelfCheckFragments, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((report, _)) => {
                // Authorization: verify node can only submit attestations for itself
                if report.node_id != tx.submitter_node {
                    tracing::warn!(
                        "Authorization failed: node {} attempted to submit self-attestation for node {}",
                        tx.submitter_node,
                        report.node_id
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                // Apply the self-check updates using the inventory module
                crate::storage_host::db_apply::apply_self_check_updates(db_tx, &report)?;

                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &SelfCheckFragmentsHandler as &dyn TransactionHandler
}
