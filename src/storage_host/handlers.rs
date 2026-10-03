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
    hopnet_storage::lifecycle::DECLARE_TX_FN,
    hopnet_storage::lifecycle::CONFIRM_TX_FN,
    hopnet_storage::engine::policy::ATTEST_FN,
];

/// RFC-STORAGE-003 S5 disk-truth attestation: stamp the rows this node
/// verified on its own disk. A node may attest only for itself.
pub struct AttestFragmentsHandler;

impl TransactionHandler for AttestFragmentsHandler {
    fn name(&self) -> &'static str {
        hopnet_storage::engine::policy::ATTEST_FN
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (report, _) =
            bincode::serde::decode_from_slice::<hopnet_storage::FragmentAttestation, _>(
                tx.payload,
                bincode::config::standard(),
            )
            .map_err(|_| DatabaseError::InvalidPayload)?;
        if report.node_id != tx.submitter_node {
            tracing::warn!(
                "Authorization failed: node {} attempted to attest for node {}",
                tx.submitter_node,
                report.node_id
            );
            return Err(DatabaseError::AuthorizationError);
        }
        let stamped = hopnet_storage::store::apply_attestation(
            db_tx,
            report.node_id,
            report.height,
            ctx.height,
            &report.present,
            &report.suspect,
        )
        .map_err(storage_err("apply_attestation"))?;
        tracing::debug!(
            node = report.node_id,
            height = report.height,
            stamped,
            suspect = report.suspect.len(),
            "attest_fragments applied"
        );
        Ok(())
    }
}

inventory::submit! {
    &AttestFragmentsHandler as &dyn TransactionHandler
}

/// Storage apply errors → handler errors: SQLite contention stays
/// transient (validation must surface it as Undetermined, never a
/// verdict); anything else is a processing failure.
fn storage_err(what: &'static str) -> impl Fn(hopnet_storage::StorageError) -> DatabaseError {
    move |e| match e {
        hopnet_storage::StorageError::Transient(code) => DatabaseError::Transient(code),
        other => {
            tracing::error!("{what} failed: {other}");
            DatabaseError::ProcessingError
        }
    }
}

/// RFC-STORAGE-003 DeclarePlacementTarget: move blobs' goals forward.
/// Per-entry validation lives in the substrate (`lifecycle::apply_declare`);
/// invalid entries are skipped, never fail the block — only an undecodable
/// payload errors.
pub struct DeclarePlacementTargetHandler;

impl TransactionHandler for DeclarePlacementTargetHandler {
    fn name(&self) -> &'static str {
        hopnet_storage::lifecycle::DECLARE_TX_FN
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<
            hopnet_storage::DeclarePlacementTarget,
            _,
        >(tx.payload, bincode::config::standard())
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let outcome = hopnet_storage::lifecycle::apply_declare(db_tx, &payload, ctx.height)
            .map_err(storage_err("apply_declare"))?;
        tracing::debug!(
            height = ctx.height,
            applied = outcome.applied,
            skipped = outcome.skipped,
            "declare_placement_target applied"
        );
        // Pull duties derive at declare-apply (RFC-STORAGE-003 S3): wake
        // the reconciler for every moved goal — execute only, validation
        // must stay pure. Anyone's page applying is the staleness check
        // observed (S4): it re-arms this node's grace rung.
        if execute {
            crate::storage_host::staleness::stamp_observed();
            for blob_id in &outcome.applied_ids {
                ctx.work.schedule("storage.pull", blob_id.to_string());
            }
        }
        Ok(())
    }
}

inventory::submit! {
    &DeclarePlacementTargetHandler as &dyn TransactionHandler
}

/// RFC-STORAGE-003 ConfirmPlacement: stamp placement_height once the
/// attested evidence for the declared goal is complete. Same skip
/// semantics as declare.
pub struct ConfirmPlacementHandler;

impl TransactionHandler for ConfirmPlacementHandler {
    fn name(&self) -> &'static str {
        hopnet_storage::lifecycle::CONFIRM_TX_FN
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) =
            bincode::serde::decode_from_slice::<hopnet_storage::ConfirmPlacement, _>(
                tx.payload,
                bincode::config::standard(),
            )
            .map_err(|_| DatabaseError::InvalidPayload)?;
        let outcome = hopnet_storage::lifecycle::apply_confirm(db_tx, &payload, ctx.height)
            .map_err(storage_err("apply_confirm"))?;
        tracing::debug!(
            height = ctx.height,
            applied = outcome.applied,
            skipped = outcome.skipped,
            "confirm_placement applied"
        );
        Ok(())
    }
}

inventory::submit! {
    &ConfirmPlacementHandler as &dyn TransactionHandler
}

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
                let crate_updates: Vec<(hopnet_storage::BlobId, u64)> = updates
                    .into_iter()
                    .map(|u| (u.blob_id, u.placement_height))
                    .collect();
                hopnet_storage::store::apply_placement_commit(db_tx, &crate_updates)
                    .map_err(storage_err("apply_placement_commit"))?;
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
