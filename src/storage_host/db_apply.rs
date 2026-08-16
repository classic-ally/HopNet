//! Consensus-apply helpers for the storage handlers (RFC-016 Stage 6 —
//! moved verbatim from src/db/{inventory,fragments}.rs to live beside
//! their only callers). Both are host-side by design: the orphan GC
//! consults the takeout gate and the cross-crate reference-provider
//! registry, neither of which hopnet-storage can depend on.

use crate::db::{CustomUUID, DatabaseError};
use crate::reference_providers::DataBlockReferenceProvider;
use hopnet_storage::SelfCheckFragments;

/// Apply differential fragment inventory updates from a self-check report
/// Called by consensus middleware when processing SelfCheckFragments transactions
///
/// The execute flag controls whether to actually apply changes (true) or just validate (false)
pub fn apply_self_check_updates(
    db_tx: &rusqlite::Transaction,
    report: &SelfCheckFragments,
) -> Result<(), DatabaseError> {
    // Substrate-owned apply (RFC-014): count verification + remove /
    // re-height / add, in-crate.
    hopnet_storage::store::apply_self_check(
        db_tx,
        report.node_id,
        report.previous_count,
        report.self_verified_height,
        &report.fragments_added,
        &report.fragments_removed,
    )
    .map_err(|e| {
        tracing::error!("apply_self_check failed for node {}: {e}", report.node_id);
        DatabaseError::ProcessingError
    })
}

/// Delete orphaned data blocks and their associated fragment_hashes records
/// Uses explicit deletion (not CASCADE) for visibility and control
/// The execute parameter controls whether to actually delete or just validate
/// Returns fragment hashes that were deleted (for opportunistic local cleanup)
pub fn delete_orphaned_data_blocks_consensus(
    db_tx: &rusqlite::Transaction,
    data_block_ids: Vec<CustomUUID>,
) -> Result<Vec<crate::db::Blake3Hash>, DatabaseError> {
    if data_block_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Validation: Check for non-expired takeouts before proceeding
    // This prevents race conditions between pre-flight check and consensus execution
    if crate::db::takeout::has_active_takeout_tx(db_tx, None)? {
        tracing::error!("Validation failed: active takeout(s) in network prevent cleanup");
        return Err(DatabaseError::ConflictError);
    }

    // Validation: Check if the data blocks exist and are truly orphaned
    for data_block_id in &data_block_ids {
        // Verify the data block exists
        let exists: bool = db_tx
            .query_row(
                "SELECT COUNT(*) > 0 FROM data_blocks WHERE id = ?",
                rusqlite::params![data_block_id],
                |row| row.get(0),
            )
            .map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))?;

        if !exists {
            tracing::warn!("Data block {} does not exist, skipping", data_block_id);
            continue;
        }

        // Verify it's truly orphaned (no references from any provider)
        let data_block_id_str = data_block_id.to_string();
        for provider in inventory::iter::<&'static dyn DataBlockReferenceProvider> {
            if provider.references_data_block(db_tx, &data_block_id_str)? {
                tracing::error!(
                    "Data block {} referenced by {} provider",
                    data_block_id,
                    provider.name()
                );
                return Err(DatabaseError::ProcessingError);
            }
        }
    }

    // Substrate-owned deletes (RFC-014): fragment_hashes + blob_access +
    // data_blocks, child-first; returns locally-stored hashes for the
    // handler's post-commit file cleanup. Gates above are the host's.
    let deleted_fragment_hashes =
        hopnet_storage::store::apply_delete_orphaned(db_tx, &data_block_ids).map_err(|e| {
            tracing::error!("apply_delete_orphaned failed: {e}");
            DatabaseError::ProcessingError
        })?;

    Ok(deleted_fragment_hashes)
}
