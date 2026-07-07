//! Drive GC reference provider (RFC-015, Stage D3).
//!
//! Moved verbatim from the host's `files::reference_provider` — declares
//! which data blocks (blobs) the drive's inodes still reference, so orphan
//! cleanup never collects a referenced blob. Registered cross-crate via the
//! hopnet-projection inventory registry.

use hopnet_projection::{DataBlockReferenceProvider, DatabaseError};

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
