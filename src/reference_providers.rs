use crate::db::DatabaseError;

pub trait DataBlockReferenceProvider: Send + Sync {
    /// Provider name for logging ("filesystem", "photos", etc.)
    fn name(&self) -> &'static str;

    /// Per-row check: does this provider reference the given data block?
    /// Used during consensus validation in delete_orphaned_data_blocks_consensus().
    fn references_data_block(
        &self,
        db_tx: &rusqlite::Transaction,
        data_block_id: &str,
    ) -> Result<bool, DatabaseError>;

    /// SQL subquery selecting all data_block_ids claimed by this provider.
    /// Used for bulk candidate identification in find_orphaned_data_blocks().
    /// Must return rows with a column named `data_block_id`.
    fn referenced_data_blocks_subquery(&self) -> &'static str;
}

inventory::collect!(&'static dyn DataBlockReferenceProvider);
