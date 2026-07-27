//! Photo-row mutating functions for consensus apply (RFC-011 Phase 1).
//!
//! These run INSIDE the host's one-SQLite-transaction consensus apply —
//! the substrate half of every blob write (`apply_blob_insert`) executes
//! first, then the photos projection half, both on db_tx. Blob + first
//! reference are atomic, so mark-and-sweep never observes a zero-ref blob
//! (drive precedent: hopnet-drive/src/db/files.rs:210-227).

use crate::envelopes::{MetadataAccessEntry, PhotoAddEntry};
use hopnet_common::CustomUUID;
use hopnet_projection::DatabaseError;
use rusqlite::params;

/// Insert one photo-add entry (substrate half + projection half).
/// The handler calls this once per entry in the batch; the whole batch
/// shares the block tx and fails atomically.
pub fn insert_photo_entry(
    db_tx: &rusqlite::Transaction,
    entry: &PhotoAddEntry,
    fragments_dir: &str,
) -> Result<(), DatabaseError> {
    // --- Substrate half: register blobs + fragment metadata + per-blob
    //     key wraps. Must run before the photos rows reference the blob
    //     ids (FK integrity, PRAGMA foreign_keys = ON).
    for resource in &entry.resources {
        hopnet_storage::store::apply_blob_insert(
            db_tx,
            &resource.op,
            &hopnet_storage::store::ApplyCtx { fragments_dir },
        )
        .map_err(|e| {
            tracing::error!(
                "photo_add: apply_blob_insert failed for blob {} (resource_type {}): {e}",
                resource.op.blob_id,
                resource.resource_type,
            );
            DatabaseError::InsertError
        })?;
    }

    // --- Projection half: photos row (identity + encrypted metadata).
    db_tx.execute(
        "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            entry.photo_id,
            entry.library_id,
            entry.uploaded_by,
            entry.encrypted_metadata,
            entry.metadata_nonce,
        ],
    )
    .map_err(|e| {
        tracing::error!("photo_add: insert photos row {} failed: {e}", entry.photo_id);
        DatabaseError::InsertError
    })?;

    // photo_resources — one row per resource.
    for resource in &entry.resources {
        db_tx.execute(
            "INSERT INTO photo_resources (photo_id, resource_type, data_block_id)
             VALUES (?1, ?2, ?3)",
            params![
                entry.photo_id,
                resource.resource_type,
                resource.op.blob_id,
            ],
        )
        .map_err(|e| {
            tracing::error!(
                "photo_add: insert photo_resources row ({}, {}) failed: {e}",
                entry.photo_id,
                resource.resource_type,
            );
            DatabaseError::InsertError
        })?;
    }

    // photo_metadata_access — per-user metadata key wraps.
    for access in &entry.metadata_access {
        insert_metadata_access_row(db_tx, &entry.photo_id, access)?;
    }

    // photo_operations — add entry (operation_type = 0).
    insert_operation_row(
        db_tx,
        &entry.operation_id,
        entry.library_id.as_ref(),
        &entry.photo_id,
        0, // add
        entry.uploaded_by,
    )?;

    Ok(())
}

/// Soft-delete one photo (set tombstone, clear album/favorite rows, log
/// operation). photo_resources and photo_metadata_access rows are retained
/// for the 30-day recovery window — the PhotosReferenceProvider pins the
/// data blocks until the cleanup job removes the photo_resources rows.
pub fn soft_delete_photo(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
    deleted_by: i32,
    deleted_at: &str,
    library_id: Option<&CustomUUID>,
    operation_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    let rows = db_tx.execute(
        "UPDATE photos SET deleted_at = ?2, deleted_by = ?3
         WHERE id = ?1 AND deleted_at IS NULL",
        params![photo_id, deleted_at, deleted_by],
    )
    .map_err(|e| {
        tracing::error!("photo_delete: update photos row {} failed: {e}", photo_id);
        DatabaseError::InsertError
    })?;
    if rows == 0 {
        // Already tombstoned or not found — idempotent, not an error.
        // (The handler still logs the operation for audit.)
    }

    // Clear album entries + favorites (photo disappears from active views).
    db_tx.execute(
        "DELETE FROM photo_album_entries WHERE photo_id = ?1",
        params![photo_id],
    )
    .map_err(|e| {
        tracing::error!("photo_delete: clear album_entries for {} failed: {e}", photo_id);
        DatabaseError::InsertError
    })?;
    db_tx.execute(
        "DELETE FROM photo_favorites WHERE photo_id = ?1",
        params![photo_id],
    )
    .map_err(|e| {
        tracing::error!("photo_delete: clear favorites for {} failed: {e}", photo_id);
        DatabaseError::InsertError
    })?;

    // Log operation (type = 2 = delete).
    insert_operation_row(db_tx, operation_id, library_id, photo_id, 2, deleted_by)?;

    Ok(())
}

/// Restore a soft-deleted photo (clear tombstone, log operation).
pub fn restore_photo(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
    restored_by: i32,
    library_id: Option<&CustomUUID>,
    operation_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    let rows = db_tx.execute(
        "UPDATE photos SET deleted_at = NULL, deleted_by = NULL
         WHERE id = ?1 AND deleted_at IS NOT NULL",
        params![photo_id],
    )
    .map_err(|e| {
        tracing::error!("photo_restore: update photos row {} failed: {e}", photo_id);
        DatabaseError::InsertError
    })?;
    if rows == 0 {
        // Not tombstoned — caller should validate. If the photo was
        // hard-deleted (tombstone expired), the row is gone entirely.
        return Err(DatabaseError::NotFound);
    }

    // Log operation (type = 8 = restore).
    insert_operation_row(db_tx, operation_id, library_id, photo_id, 8, restored_by)?;

    Ok(())
}

// --- Private helpers ---

fn insert_metadata_access_row(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
    entry: &MetadataAccessEntry,
) -> Result<(), DatabaseError> {
    db_tx.execute(
        "INSERT INTO photo_metadata_access (photo_id, user_id, ephemeral_pubkey, encrypted_metadata_key)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            photo_id,
            entry.user_id,
            entry.ephemeral_pubkey,
            entry.encrypted_metadata_key,
        ],
    )
    .map_err(|e| {
        tracing::error!(
            "photo_add: insert photo_metadata_access ({}, user {}) failed: {e}",
            photo_id,
            entry.user_id,
        );
        DatabaseError::InsertError
    })?;
    Ok(())
}

fn insert_operation_row(
    db_tx: &rusqlite::Transaction,
    operation_id: &CustomUUID,
    library_id: Option<&CustomUUID>,
    photo_id: &CustomUUID,
    operation_type: i32,
    performed_by: i32,
) -> Result<(), DatabaseError> {
    db_tx.execute(
        "INSERT INTO photo_operations (id, library_id, photo_id, operation_type, performed_by)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            operation_id,
            library_id,
            photo_id,
            operation_type,
            performed_by,
        ],
    )
    .map_err(|e| {
        tracing::error!(
            "photo_add/delete/restore: insert photo_operations row {} failed: {e}",
            operation_id,
        );
        DatabaseError::InsertError
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelopes::{PhotoAddEntry, PhotoResourceOp};
    use hopnet_common::Blake3Hash;
    use rusqlite::Connection;

    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(
            "CREATE TABLE users (user_id INTEGER PRIMARY KEY, x25519_pubkey BLOB);",
        )
        .unwrap();
        hopnet_storage::store::install_schema(&conn).unwrap();
        crate::db::install_schema(&conn).unwrap();
        conn.execute(
            "INSERT INTO users (user_id, x25519_pubkey) VALUES (1, x'00')",
            [],
        )
        .unwrap();
        conn
    }

    fn make_blob_op(blob_id: CustomUUID) -> hopnet_storage::store::BlobInsertOp {
        hopnet_storage::store::BlobInsertOp {
            blob_id,
            integrity_hash: Blake3Hash::from_bytes([0xCC; 32]),
            added_bytes: 0,
            file_size: 100,
            fragments: vec![],
            access: vec![],
        }
    }

    /// insert_photo_entry writes the photo + resources + metadata_access
    /// + operation row, all in one tx.
    #[test]
    fn insert_personal_photo_succeeds() {
        let conn = fixture();
        let entry = PhotoAddEntry {
            photo_id: CustomUUID::retention_cutoff(0),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"enc_meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![PhotoResourceOp {
                resource_type: 0,
                op: make_blob_op(CustomUUID::retention_cutoff(1)),
            }],
            metadata_access: vec![crate::envelopes::MetadataAccessEntry {
                user_id: 1,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: CustomUUID::retention_cutoff(2),
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();

        // Verify rows exist.
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM photos", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let res_count: i32 = conn
            .query_row("SELECT COUNT(*) FROM photo_resources", [], |r| r.get(0))
            .unwrap();
        assert_eq!(res_count, 1);
        let op_count: i32 = conn
            .query_row("SELECT COUNT(*) FROM photo_operations", [], |r| r.get(0))
            .unwrap();
        assert_eq!(op_count, 1);
    }

    /// soft_delete_photo sets the tombstone and logs the operation.
    /// photo_resources stays — the GC provider pins the blob through the
    /// tombstone window.
    #[test]
    fn soft_delete_tombstones_and_logs() {
        let conn = fixture();
        // Insert first.
        let entry = PhotoAddEntry {
            photo_id: CustomUUID::retention_cutoff(0),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"enc_meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![PhotoResourceOp {
                resource_type: 0,
                op: make_blob_op(CustomUUID::retention_cutoff(1)),
            }],
            metadata_access: vec![crate::envelopes::MetadataAccessEntry {
                user_id: 1,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: CustomUUID::retention_cutoff(2),
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();

        // Delete.
        let tx = conn.unchecked_transaction().unwrap();
        soft_delete_photo(
            &tx,
            &entry.photo_id,
            1,             // deleted_by
            "2025-06-01T00:00:00Z",
            None,          // personal library
            &CustomUUID::retention_cutoff(3),
        )
        .unwrap();
        tx.commit().unwrap();

        // Verify tombstone.
        let deleted: Option<String> = conn
            .query_row(
                "SELECT deleted_at FROM photos WHERE id = ?1",
                rusqlite::params![entry.photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(deleted.is_some(), "deleted_at must be set after soft-delete");

        // photo_resources still present (pins blob for GC).
        let res_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM photo_resources WHERE photo_id = ?1",
                rusqlite::params![entry.photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(res_count, 1, "photo_resources retained during tombstone window");
    }

    /// restore clears the tombstone.
    #[test]
    fn restore_clears_tombstone() {
        let conn = fixture();
        // Insert + delete.
        let entry = PhotoAddEntry {
            photo_id: CustomUUID::retention_cutoff(0),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"enc_meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![PhotoResourceOp {
                resource_type: 0,
                op: make_blob_op(CustomUUID::retention_cutoff(1)),
            }],
            metadata_access: vec![crate::envelopes::MetadataAccessEntry {
                user_id: 1,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: CustomUUID::retention_cutoff(2),
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        soft_delete_photo(
            &tx,
            &entry.photo_id,
            1,
            "2025-06-01T00:00:00Z",
            None,
            &CustomUUID::retention_cutoff(3),
        )
        .unwrap();
        tx.commit().unwrap();

        // Restore.
        let tx = conn.unchecked_transaction().unwrap();
        restore_photo(
            &tx,
            &entry.photo_id,
            1,
            None,
            &CustomUUID::retention_cutoff(4),
        )
        .unwrap();
        tx.commit().unwrap();

        let deleted: Option<String> = conn
            .query_row(
                "SELECT deleted_at FROM photos WHERE id = ?1",
                rusqlite::params![entry.photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(deleted.is_none(), "deleted_at must be NULL after restore");
    }

    /// Double-delete is idempotent — first delete wins (tombstone + window
    /// start time). Second delete skips the UPDATE, still logs the op.
    #[test]
    fn double_delete_is_idempotent() {
        let conn = fixture();
        let entry = PhotoAddEntry {
            photo_id: CustomUUID::retention_cutoff(0),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"enc_meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![PhotoResourceOp {
                resource_type: 0,
                op: make_blob_op(CustomUUID::retention_cutoff(1)),
            }],
            metadata_access: vec![crate::envelopes::MetadataAccessEntry {
                user_id: 1,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: CustomUUID::retention_cutoff(2),
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();

        // First delete.
        let tx = conn.unchecked_transaction().unwrap();
        soft_delete_photo(
            &tx,
            &entry.photo_id,
            1,
            "2025-06-01T00:00:00Z",
            None,
            &CustomUUID::retention_cutoff(3),
        )
        .unwrap();
        tx.commit().unwrap();

        // Second delete — idempotent, no error.
        let tx = conn.unchecked_transaction().unwrap();
        soft_delete_photo(
            &tx,
            &entry.photo_id,
            1,
            "2025-07-01T00:00:00Z", // later timestamp — ignored
            None,
            &CustomUUID::retention_cutoff(4), // new operation id
        )
        .unwrap();
        tx.commit().unwrap();

        // First delete's timestamp wins — window starts from first tombstone.
        let deleted_at: String = conn
            .query_row(
                "SELECT deleted_at FROM photos WHERE id = ?1",
                rusqlite::params![entry.photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            deleted_at, "2025-06-01T00:00:00Z",
            "first delete timestamp preserved for consistent 30-day window"
        );

        // Both operation log entries exist.
        let op_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM photo_operations WHERE photo_id = ?1 AND operation_type = 2",
                rusqlite::params![entry.photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(op_count, 2, "both delete operations logged");
    }

    /// Restoring a non-deleted photo returns NotFound.
    #[test]
    fn restore_not_deleted_is_error() {
        let conn = fixture();
        let entry = PhotoAddEntry {
            photo_id: CustomUUID::retention_cutoff(0),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"enc_meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![PhotoResourceOp {
                resource_type: 0,
                op: make_blob_op(CustomUUID::retention_cutoff(1)),
            }],
            metadata_access: vec![crate::envelopes::MetadataAccessEntry {
                user_id: 1,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: CustomUUID::retention_cutoff(2),
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        let result = restore_photo(
            &tx,
            &entry.photo_id,
            1,
            None,
            &CustomUUID::retention_cutoff(3),
        );
        assert!(result.is_err(), "restore on active photo must fail");
    }
}
