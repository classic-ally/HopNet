use crate::db::DatabaseError;
use crate::reference_providers::DataBlockReferenceProvider;

pub struct FilesystemReferenceProvider;

impl DataBlockReferenceProvider for FilesystemReferenceProvider {
    fn name(&self) -> &'static str {
        "filesystem"
    }

    fn references_data_block(
        &self,
        db_tx: &rusqlite::Transaction,
        data_block_id: &str,
    ) -> Result<bool, DatabaseError> {
        db_tx
            .query_row(
                "SELECT COUNT(*) > 0 FROM inodes WHERE data_id = ?",
                rusqlite::params![data_block_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::RecallError)
    }

    fn referenced_data_blocks_subquery(&self) -> &'static str {
        "SELECT DISTINCT data_id AS data_block_id FROM inodes WHERE data_id IS NOT NULL"
    }
}

inventory::submit! { &FilesystemReferenceProvider as &dyn DataBlockReferenceProvider }
