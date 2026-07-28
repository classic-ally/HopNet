//! Photos consensus transaction handlers (RFC-011 Phase 1).
//!
//! Registration crosses the crate boundary via `inventory::submit!` (the
//! registry lives in hopnet-projection); the host's boot tripwire asserts
//! nothing was dropped at link time. DB mutations run in BOTH validate and
//! execute passes; side effects (notifier, work) fire only under execute.

use crate::db::photos::{delete_favorite, edit_photo_content, edit_photo_metadata, hard_delete_expired_photo, insert_favorite, insert_photo_entry, lookup_photo_owner, restore_photo, soft_delete_photo, undo_content_edit};
use crate::envelopes::{
    PhotoAddPayload, PhotoCleanupExpiredPayload, PhotoDeletePayload,
    PhotoEditContentPayload, PhotoEditMetadataPayload, PhotoFavoritePayload,
    PhotoRestorePayload, PhotoUndoPayload, PhotoUnfavoritePayload,
};
use hopnet_projection::{DatabaseError, HandlerCtx, HandlerResult, TransactionHandler, TxMeta};

/// Every consensus function this projection registers — the host boot
/// tripwire asserts these are present in its dispatch table (guards
/// against a linker dropping cross-crate inventory registrations).
pub const TX_FUNCTIONS: &[&str] = &[
    "photo_add",
    "photo_delete",
    "photo_restore",
    "photo_cleanup_expired",
    "photo_edit_content",
    "photo_edit_metadata",
    "photo_undo",
    "photo_favorite",
    "photo_unfavorite",
];

// --- photo_add ---

pub struct PhotoAddHandler;

impl TransactionHandler for PhotoAddHandler {
    fn name(&self) -> &'static str {
        "photo_add"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoAddPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        let user_id = tx.user_id.ok_or_else(|| {
            tracing::warn!("photo_add: requires user authentication");
            DatabaseError::AuthorizationError
        })?;

        for entry in &payload.entries {
            if entry.uploaded_by != user_id {
                tracing::warn!(
                    "photo_add: user {} attempted to add photo for {}",
                    user_id,
                    entry.uploaded_by,
                );
                return Err(DatabaseError::AuthorizationError);
            }
            // Phase 3: when create_shared_library lands, replace this
            // guard with a shared_library_members membership check.
            // Without it, any user could add into any library.
            if entry.library_id.is_some() {
                tracing::warn!(
                    "photo_add: shared libraries not yet supported (library_id={})",
                    entry.library_id.as_ref().unwrap(),
                );
                return Err(DatabaseError::InvalidPayload);
            }
            insert_photo_entry(db_tx, entry, ctx.fragments_dir)?;
        }

        Ok(())
    }
}

inventory::submit! {
    &PhotoAddHandler as &dyn TransactionHandler
}

// --- photo_delete ---

pub struct PhotoDeleteHandler;

impl TransactionHandler for PhotoDeleteHandler {
    fn name(&self) -> &'static str {
        "photo_delete"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoDeletePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        let user_id = tx.user_id.ok_or_else(|| {
            tracing::warn!("photo_delete: requires user authentication");
            DatabaseError::AuthorizationError
        })?;

        for entry in &payload.entries {
            // Look up the photo's owner + library scope.
            let row: Result<(i32, Option<hopnet_common::CustomUUID>), _> = db_tx
                .query_row(
                    "SELECT uploaded_by, library_id FROM photos WHERE id = ?1",
                    rusqlite::params![entry.photo_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                );
            let (uploaded_by, library_id) = match row {
                Ok(r) => r,
                Err(_) => {
                    // Photo not found — already deleted or never existed.
                    // Idempotent skip (drive precedent: DeleteFilesHandler
                    // at hopnet-drive/src/handlers.rs:214-222).
                    continue;
                }
            };

            // Authorization: the actor must be the uploader (personal
            // library). Phase 3 extends this to shared_library_members.
            if uploaded_by != user_id {
                tracing::warn!(
                    "photo_delete: user {} not authorized for photo {} (owned by {})",
                    user_id,
                    entry.photo_id,
                    uploaded_by,
                );
                return Err(DatabaseError::AuthorizationError);
            }

            let deleted_at = entry
                .operation_id
                .extract_timestamp()
                .map(|dt| dt.to_rfc3339())
                .ok_or_else(|| {
                    tracing::warn!(
                        "photo_delete: operation_id {} is not UUIDv7",
                        entry.operation_id,
                    );
                    DatabaseError::InvalidPayload
                })?;
            // TODO(Phase 3): operation_id is client-minted with no bound
            // against consensus time. A malicious shared-library member
            // could backdate it 30+ days to skip the recovery window for
            // other members. Validate the operation_id timestamp is within
            // a sane window of the consensus block timestamp (derivable
            // from tx metadata, consistent with dispatch.rs:101).

            soft_delete_photo(
                db_tx,
                &entry.photo_id,
                user_id,
                &deleted_at,
                library_id.as_ref(),
                &entry.operation_id,
            )?;
        }

        Ok(())
    }
}

inventory::submit! {
    &PhotoDeleteHandler as &dyn TransactionHandler
}

// --- photo_restore ---

pub struct PhotoRestoreHandler;

impl TransactionHandler for PhotoRestoreHandler {
    fn name(&self) -> &'static str {
        "photo_restore"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoRestorePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        let user_id = tx.user_id.ok_or_else(|| {
            tracing::warn!("photo_restore: requires user authentication");
            DatabaseError::AuthorizationError
        })?;

        for entry in &payload.entries {
            // Look up owner + library scope for authorization.
            let row: Result<(i32, Option<hopnet_common::CustomUUID>), _> = db_tx
                .query_row(
                    "SELECT uploaded_by, library_id FROM photos WHERE id = ?1",
                    rusqlite::params![entry.photo_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                );
            let (uploaded_by, library_id) = match row {
                Ok(r) => r,
                Err(_) => {
                    // Photo not found — already hard-deleted by cleanup,
                    // or never existed. Idempotent skip.
                    continue;
                }
            };

            if uploaded_by != user_id {
                tracing::warn!(
                    "photo_restore: user {} not authorized for photo {} (owned by {})",
                    user_id,
                    entry.photo_id,
                    uploaded_by,
                );
                return Err(DatabaseError::AuthorizationError);
            }

            restore_photo(
                db_tx,
                &entry.photo_id,
                user_id,
                library_id.as_ref(),
                &entry.operation_id,
            )?;
        }

        Ok(())
    }
}

inventory::submit! {
    &PhotoRestoreHandler as &dyn TransactionHandler
}

// --- photo_cleanup_expired ---

pub struct PhotoCleanupExpiredHandler;

impl TransactionHandler for PhotoCleanupExpiredHandler {
    fn name(&self) -> &'static str {
        "photo_cleanup_expired"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) =
            bincode::serde::decode_from_slice::<PhotoCleanupExpiredPayload, _>(
                tx.payload,
                bincode::config::standard(),
            )
            .map_err(|_| DatabaseError::InvalidPayload)?;

        // Node-signed only — a user-signed submission could hard-delete
        // another member's tombstoned photo within the recovery window.
        // TODO(Phase 3): scan_cutoff is payload data — a malicious or
        // skewed-clock node can submit a far-future cutoff to bypass the
        // 30-day window. Clamp against the consensus block timestamp when
        // dispatch exposes one. Symmetric with the operation_id backdating
        // TODO at photo_delete.
        if tx.user_id.is_some() {
            tracing::warn!(
                "photo_cleanup_expired: user-signed submissions rejected"
            );
            return Err(DatabaseError::AuthorizationError);
        }

        for photo_id in &payload.photo_ids {
            match hard_delete_expired_photo(db_tx, photo_id, &payload.scan_cutoff) {
                Ok(()) | Err(DatabaseError::NotFound) => {}
                Err(e) => {
                    tracing::error!(
                        "photo_cleanup_expired: hard-delete {} failed: {:?}",
                        photo_id,
                        e,
                    );
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

inventory::submit! {
    &PhotoCleanupExpiredHandler as &dyn TransactionHandler
}

// --- photo_edit_content ---

pub struct PhotoEditContentHandler;

impl TransactionHandler for PhotoEditContentHandler {
    fn name(&self) -> &'static str { "photo_edit_content" }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoEditContentPayload, _>(
            tx.payload, bincode::config::standard(),
        ).map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            if entry.resources.is_empty() {
                return Err(DatabaseError::InvalidPayload);
            }
            let owner = lookup_photo_owner(db_tx, &entry.photo_id)?
                .ok_or(DatabaseError::NotFound)?;
            if owner.0 != user_id {
                return Err(DatabaseError::AuthorizationError);
            }
            if owner.1.is_some() {
                // Tombstoned — reject edits.
                return Err(DatabaseError::ConflictError);
            }
            edit_photo_content(db_tx, entry, ctx.fragments_dir, user_id)?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoEditContentHandler as &dyn TransactionHandler }

// --- photo_edit_metadata ---

pub struct PhotoEditMetadataHandler;

impl TransactionHandler for PhotoEditMetadataHandler {
    fn name(&self) -> &'static str { "photo_edit_metadata" }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoEditMetadataPayload, _>(
            tx.payload, bincode::config::standard(),
        ).map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            let owner = lookup_photo_owner(db_tx, &entry.photo_id)?
                .ok_or(DatabaseError::NotFound)?;
            if owner.0 != user_id {
                return Err(DatabaseError::AuthorizationError);
            }
            if owner.1.is_some() {
                return Err(DatabaseError::ConflictError);
            }
            edit_photo_metadata(db_tx, entry, user_id)?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoEditMetadataHandler as &dyn TransactionHandler }

// --- photo_undo ---

pub struct PhotoUndoHandler;

impl TransactionHandler for PhotoUndoHandler {
    fn name(&self) -> &'static str { "photo_undo" }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoUndoPayload, _>(
            tx.payload, bincode::config::standard(),
        ).map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            let owner = lookup_photo_owner(db_tx, &entry.photo_id)?
                .ok_or(DatabaseError::NotFound)?;
            if owner.0 != user_id {
                return Err(DatabaseError::AuthorizationError);
            }
            if owner.1.is_some() {
                return Err(DatabaseError::ConflictError);
            }
            undo_content_edit(
                db_tx,
                &entry.photo_id,
                &entry.target_operation_id,
                &entry.operation_id,
                user_id,
            )?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoUndoHandler as &dyn TransactionHandler }

// --- photo_favorite ---

pub struct PhotoFavoriteHandler;

impl TransactionHandler for PhotoFavoriteHandler {
    fn name(&self) -> &'static str { "photo_favorite" }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoFavoritePayload, _>(
            tx.payload, bincode::config::standard(),
        ).map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            let Some(owner) = lookup_photo_owner(db_tx, &entry.photo_id)? else {
                continue; // hard-deleted — idempotent skip
            };
            if owner.1.is_some() {
                return Err(DatabaseError::ConflictError);
            }
            // Any user can favorite any photo — no ownership check needed
            // (photos.md:218 — favorites are per-user, user_id is the actor).
            insert_favorite(db_tx, &entry.photo_id, user_id)?;
            crate::db::photos::upsert_photo_changes(db_tx, &entry.photo_id)?;
            crate::db::photos::insert_operation_row(
                db_tx, &entry.operation_id, None, &entry.photo_id,
                6, user_id, None, None, None, None,
            )?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoFavoriteHandler as &dyn TransactionHandler }

// --- photo_unfavorite ---

pub struct PhotoUnfavoriteHandler;

impl TransactionHandler for PhotoUnfavoriteHandler {
    fn name(&self) -> &'static str { "photo_unfavorite" }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoUnfavoritePayload, _>(
            tx.payload, bincode::config::standard(),
        ).map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            // Skip missing or tombstoned photos. soft_delete_photo already
            // clears favorites, so the delete is a no-op, but rejecting
            // tombstoned avoids logging a spurious operation.
            let Some(owner) = lookup_photo_owner(db_tx, &entry.photo_id)? else {
                continue;
            };
            if owner.1.is_some() {
                continue;
            }
            delete_favorite(db_tx, &entry.photo_id, user_id)?;
            crate::db::photos::upsert_photo_changes(db_tx, &entry.photo_id)?;
            crate::db::photos::insert_operation_row(
                db_tx, &entry.operation_id, None, &entry.photo_id,
                7, user_id, None, None, None, None,
            )?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoUnfavoriteHandler as &dyn TransactionHandler }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelopes::{
        MetadataAccessEntry, PhotoAddEntry, PhotoCleanupExpiredPayload,
        PhotoDeleteEntry, PhotoEditContentEntry,
        PhotoFavoriteEntry, PhotoResourceOp, PhotoRestoreEntry, PhotoUnfavoriteEntry,
    };
    use hopnet_common::{Blake3Hash, CustomUUID};
    use hopnet_projection::Projection;
    use rusqlite::Connection;

    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(
            "CREATE TABLE users (user_id INTEGER PRIMARY KEY, x25519_pubkey BLOB);
             CREATE TABLE consensus_meta (key TEXT PRIMARY KEY, value BLOB);",
        )
        .unwrap();
        hopnet_storage::store::install_schema(&conn).unwrap();
        crate::db::install_schema(&conn).unwrap();
        conn.execute(
            "INSERT INTO users (user_id, x25519_pubkey) VALUES (1, x'00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO users (user_id, x25519_pubkey) VALUES (2, x'01')",
            [],
        )
        .unwrap();
        conn
    }

    fn ctx(fragments_dir: &str) -> HandlerCtx<'_> {
        HandlerCtx {
            fragments_dir,
            node_id: None,
            notifier: &NoopNotifier,
            work: &NoopScheduler,
        }
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

    struct NoopNotifier;
    impl hopnet_projection::ChangeNotifier for NoopNotifier {
        fn files_changed(&self) {}
    }

    struct NoopScheduler;
    impl hopnet_projection::WorkScheduler for NoopScheduler {
        fn schedule(&self, _subsystem: &str, _key: String) {}
    }

    // --- Helpers ---

    fn add_payload_bytes(
        photo_id: CustomUUID,
        uploaded_by: i32,
        blob_id: CustomUUID,
        op_id: CustomUUID,
        library_id: Option<CustomUUID>,
    ) -> Vec<u8> {
        let payload = PhotoAddPayload {
            entries: vec![PhotoAddEntry {
                photo_id,
                library_id,
                uploaded_by,
                encrypted_metadata: b"enc_meta".to_vec(),
                metadata_nonce: [0u8; 12],
                resources: vec![PhotoResourceOp {
                    resource_type: 0,
                    op: make_blob_op(blob_id),
                }],
                metadata_access: vec![MetadataAccessEntry {
                    user_id: uploaded_by,
                    ephemeral_pubkey: [0x42; 32],
                    encrypted_metadata_key: vec![0xFF; 48],
                }],
                operation_id: op_id,
            }],
        };
        bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap()
    }

    /// Run a handler's validate pass on a fresh tx. The tx is rolled back
    /// implicitly — production does validate in a SAVEPOINT.
    fn validate(
        conn: &Connection,
        handler: &dyn TransactionHandler,
        function: &str,
        payload: &[u8],
        user_id: Option<i32>,
    ) -> HandlerResult {
        let tx = conn.unchecked_transaction().unwrap();
        let meta = TxMeta {
            function,
            payload,
            submitter_node: 0,
            user_id,
        };
        handler.process(&meta, false, &ctx("/tmp/fragments"), &tx)
    }

    /// Run a handler's execute (apply) pass on a fresh tx and commit.
    fn apply(
        conn: &Connection,
        handler: &dyn TransactionHandler,
        function: &str,
        payload: &[u8],
        user_id: Option<i32>,
    ) {
        let tx = conn.unchecked_transaction().unwrap();
        let meta = TxMeta {
            function,
            payload,
            submitter_node: 0,
            user_id,
        };
        handler
            .process(&meta, true, &ctx("/tmp/fragments"), &tx)
            .unwrap();
        tx.commit().unwrap();
    }

    // --- Tests ---

    /// photo_add: validate passes cleanly, apply persists.
    #[test]
    fn photo_add_validate_then_apply() {
        let conn = fixture();
        let bytes = add_payload_bytes(
            CustomUUID::retention_cutoff(0),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        validate(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1)).unwrap();
        apply(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));

        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM photos", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    /// photo_add: uploaded_by must equal tx.user_id.
    #[test]
    fn photo_add_rejects_wrong_uploader() {
        let conn = fixture();
        let bytes = add_payload_bytes(
            CustomUUID::retention_cutoff(0),
            99,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        let result = validate(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));
        assert!(matches!(result, Err(DatabaseError::AuthorizationError)));
    }

    /// photo_add: non-NULL library_id with no shared_library row must fail.
    /// create_shared_library doesn't exist yet (Phase 3) — the FK catches it.
    #[test]
    fn photo_add_rejects_nonexistent_library() {
        let conn = fixture();
        let bytes = add_payload_bytes(
            CustomUUID::retention_cutoff(0),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            Some(CustomUUID::retention_cutoff(99)),
        );
        let result = validate(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));
        assert!(result.is_err(), "non-NULL library_id without library row must fail");
    }

    /// photo_delete: validates and tombstones in separate passes.
    #[test]
    fn photo_delete_tombstones() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let del_payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        validate(&conn, &PhotoDeleteHandler, "photo_delete", &del_bytes, Some(1)).unwrap();
        apply(&conn, &PhotoDeleteHandler, "photo_delete", &del_bytes, Some(1));

        let deleted_at: Option<String> = conn
            .query_row(
                "SELECT deleted_at FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(deleted_at.is_some());
    }

    /// photo_delete: non-owner must be rejected.
    #[test]
    fn photo_delete_rejects_non_owner() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let del_payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        let result = validate(&conn, &PhotoDeleteHandler, "photo_delete", &del_bytes, Some(2));
        assert!(matches!(result, Err(DatabaseError::AuthorizationError)));
    }

    /// photo_restore: active (not tombstoned) photo must fail.
    #[test]
    fn restore_active_photo_fails() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let restore_payload = PhotoRestorePayload {
            entries: vec![PhotoRestoreEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let restore_bytes =
            bincode::serde::encode_to_vec(&restore_payload, bincode::config::standard()).unwrap();
        let result = validate(&conn, &PhotoRestoreHandler, "photo_restore", &restore_bytes, Some(1));
        assert!(result.is_err(), "restore of active photo must fail");
    }

    /// Non-owner restore must be rejected (mirrors non-owner delete).
    #[test]
    fn restore_rejects_non_owner() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));
        let tx = conn.unchecked_transaction().unwrap();
        soft_delete_photo(
            &tx,
            &photo_id,
            1,
            "2025-06-01T00:00:00Z",
            None,
            &CustomUUID::retention_cutoff(3),
        )
        .unwrap();
        tx.commit().unwrap();

        let restore_payload = PhotoRestorePayload {
            entries: vec![PhotoRestoreEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(4),
            }],
        };
        let restore_bytes =
            bincode::serde::encode_to_vec(&restore_payload, bincode::config::standard()).unwrap();
        let result = validate(&conn, &PhotoRestoreHandler, "photo_restore", &restore_bytes, Some(2));
        assert!(matches!(result, Err(DatabaseError::AuthorizationError)));
    }

    /// Handler-written deleted_at must be parseable by SQLite's datetime()
    /// (the cleanup query at photos.md:382-385 uses datetime(deleted_at, '+30 days')).
    /// to_rfc3339() produces "...+00:00" or "...Z" — both valid for datetime().
    #[test]
    fn handler_written_deleted_at_is_sqlite_datetime_parseable() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let del_payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        apply(&conn, &PhotoDeleteHandler, "photo_delete", &del_bytes, Some(1));

        let parsed: Option<String> = conn
            .query_row(
                "SELECT datetime(deleted_at, '+30 days') FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            parsed.is_some(),
            "datetime(deleted_at, '+30 days') must be non-NULL for cleanup query"
        );
    }

    /// committed_blob_ids on PhotosProjection extracts all blob ids from
    /// a photo_add payload.
    #[test]
    fn committed_blob_ids_photo_add() {
        let payload = PhotoAddPayload {
            entries: vec![
                PhotoAddEntry {
                    photo_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                    library_id: None,
                    uploaded_by: 1,
                    encrypted_metadata: vec![],
                    metadata_nonce: [0u8; 12],
                    resources: vec![PhotoResourceOp {
                        resource_type: 0,
                        op: make_blob_op(
                            "00000000-0000-0000-0000-0000000000a1".parse().unwrap(),
                        ),
                    }],
                    metadata_access: vec![],
                    operation_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
                },
                PhotoAddEntry {
                    photo_id: "00000000-0000-0000-0000-000000000003".parse().unwrap(),
                    library_id: None,
                    uploaded_by: 1,
                    encrypted_metadata: vec![],
                    metadata_nonce: [0u8; 12],
                    resources: vec![
                        PhotoResourceOp {
                            resource_type: 0,
                            op: make_blob_op(
                                "00000000-0000-0000-0000-0000000000b1".parse().unwrap(),
                            ),
                        },
                        PhotoResourceOp {
                            resource_type: 2,
                            op: make_blob_op(
                                "00000000-0000-0000-0000-0000000000b2".parse().unwrap(),
                            ),
                        },
                    ],
                    metadata_access: vec![],
                    operation_id: "00000000-0000-0000-0000-000000000004".parse().unwrap(),
                },
            ],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();

        let proj = crate::PhotosProjection;
        let ids = proj.committed_blob_ids("photo_add", &encoded);

        assert_eq!(ids.len(), 3);
        assert_eq!(
            ids[0].to_string(),
            "00000000-0000-0000-0000-0000000000a1"
        );
        assert_eq!(
            ids[1].to_string(),
            "00000000-0000-0000-0000-0000000000b1"
        );
        assert_eq!(
            ids[2].to_string(),
            "00000000-0000-0000-0000-0000000000b2"
        );
    }

    /// committed_blob_ids returns empty for unknown functions + garbage.
    #[test]
    fn committed_blob_ids_unknown_or_garbage_is_empty() {
        let proj = crate::PhotosProjection;
        assert!(proj
            .committed_blob_ids("photo_delete", &[])
            .is_empty());
        assert!(proj
            .committed_blob_ids("photo_restore", &[])
            .is_empty());
        assert!(proj
            .committed_blob_ids("photo_edit_content", &[])
            .is_empty());
        assert!(proj
            .committed_blob_ids("photo_add", b"not valid bincode")
            .is_empty());
    }

    /// committed_blob_ids on PhotosProjection extracts blob ids from
    /// a photo_edit_content payload (both primary edit + thumbnails).
    #[test]
    fn committed_blob_ids_photo_edit_content() {
        let payload = crate::envelopes::PhotoEditContentPayload {
            entries: vec![PhotoEditContentEntry {
                photo_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                resources: vec![
                    PhotoResourceOp { resource_type: 1, op: make_blob_op("00000000-0000-0000-0000-000000000e01".parse().unwrap()) },
                    PhotoResourceOp { resource_type: 5, op: make_blob_op("00000000-0000-0000-0000-000000000e05".parse().unwrap()) },
                ],
                encrypted_metadata: None, metadata_nonce: None,
                operation_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
            }],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let ids = crate::PhotosProjection.committed_blob_ids("photo_edit_content", &encoded);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].to_string(), "00000000-0000-0000-0000-000000000e01");
        assert_eq!(ids[1].to_string(), "00000000-0000-0000-0000-000000000e05");
    }

    /// photo_add writes a photo_metadata_access row for the uploader.
    #[test]
    fn photo_add_writes_metadata_access_row() {
        let conn = fixture();
        let bytes = add_payload_bytes(
            CustomUUID::retention_cutoff(0),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));

        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM photo_metadata_access WHERE user_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "uploader must have a photo_metadata_access row");
    }

    /// photo_cleanup_expired hard-deletes a tombstoned photo beyond the
    /// 30-day window. The scan_cutoff rides the payload — all validators
    /// apply the same predicate.
    #[test]
    fn cleanup_expired_hard_deletes() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        // Soft-delete with a date 40 days ago (well beyond 30d window).
        let del_payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        apply(&conn, &PhotoDeleteHandler, "photo_delete", &del_bytes, Some(1));

        // Hard-set deleted_at to 40 days ago so the cutoff check passes.
        conn.execute(
            "UPDATE photos SET deleted_at = '2025-06-01T00:00:00Z' WHERE id = ?1",
            rusqlite::params![photo_id],
        )
        .unwrap();

        // Cleanup with a cutoff after the 30-day window.
        let cleanup_payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![photo_id.clone()],
            scan_cutoff: "2099-01-01T00:00:00Z".into(),
        };
        let cleanup_bytes =
            bincode::serde::encode_to_vec(&cleanup_payload, bincode::config::standard()).unwrap();
        validate(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        )
        .unwrap();
        apply(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        );

        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!exists, "expired photo must be hard-deleted");
    }

    /// cleanup skips a photo whose 30-day window hasn't elapsed yet.
    #[test]
    fn cleanup_skips_within_window() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        // Tombstone with a date only 5 days ago — still within the 30d window.
        conn.execute(
            "UPDATE photos SET deleted_at = '2026-07-23T00:00:00Z' WHERE id = ?1",
            rusqlite::params![photo_id],
        )
        .unwrap();

        // Cutoff also 5 days ago — window not elapsed.
        let cleanup_payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![photo_id.clone()],
            scan_cutoff: "2026-07-23T00:00:00Z".into(),
        };
        let cleanup_bytes =
            bincode::serde::encode_to_vec(&cleanup_payload, bincode::config::standard()).unwrap();
        apply(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        );

        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(exists, "within-window photo must survive cleanup");
    }

    /// cleanup idempotently skips a missing photo (another node's earlier
    /// tx already deleted it).
    #[test]
    fn cleanup_skips_missing_photo() {
        let conn = fixture();
        let cleanup_payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![CustomUUID::retention_cutoff(99)],
            scan_cutoff: "2099-01-01T00:00:00Z".into(),
        };
        let cleanup_bytes =
            bincode::serde::encode_to_vec(&cleanup_payload, bincode::config::standard()).unwrap();
        validate(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        )
        .unwrap();
        apply(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        );
        // No error, no side effects.
    }

    /// User-signed cleanup must be rejected.
    #[test]
    fn cleanup_rejects_user_signed() {
        let conn = fixture();
        let cleanup_payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![CustomUUID::retention_cutoff(0)],
            scan_cutoff: "2099-01-01T00:00:00Z".into(),
        };
        let cleanup_bytes =
            bincode::serde::encode_to_vec(&cleanup_payload, bincode::config::standard()).unwrap();
        let result = validate(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            Some(1),
        );
        assert!(matches!(result, Err(DatabaseError::AuthorizationError)));
    }

    // --- edit handler tests ---

    #[test]
    fn photo_edit_content_applies() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(photo_id.clone(), 1, CustomUUID::retention_cutoff(1), CustomUUID::retention_cutoff(2), None);
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let new_blob_id = CustomUUID::retention_cutoff(3);
        let edit_payload = crate::envelopes::PhotoEditContentPayload {
            entries: vec![PhotoEditContentEntry {
                photo_id: photo_id.clone(),
                resources: vec![PhotoResourceOp { resource_type: 1, op: make_blob_op(new_blob_id.clone()) }],
                encrypted_metadata: None, metadata_nonce: None,
                operation_id: CustomUUID::retention_cutoff(4),
            }],
        };
        let edit_bytes = bincode::serde::encode_to_vec(&edit_payload, bincode::config::standard()).unwrap();
        validate(&conn, &PhotoEditContentHandler, "photo_edit_content", &edit_bytes, Some(1)).unwrap();
        apply(&conn, &PhotoEditContentHandler, "photo_edit_content", &edit_bytes, Some(1));

        let res: String = conn.query_row("SELECT data_block_id FROM photo_resources WHERE photo_id=?1 AND resource_type=1", rusqlite::params![photo_id], |r| r.get(0)).unwrap();
        assert_eq!(res, new_blob_id.to_string());
    }

    #[test]
    fn photo_edit_content_rejects_tombstoned() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(photo_id.clone(), 1, CustomUUID::retention_cutoff(1), CustomUUID::retention_cutoff(2), None);
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));
        // Tombstone.
        let del_payload = crate::envelopes::PhotoDeletePayload { entries: vec![PhotoDeleteEntry { photo_id: photo_id.clone(), operation_id: CustomUUID::retention_cutoff(5) }] };
        let del_bytes = bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        apply(&conn, &PhotoDeleteHandler, "photo_delete", &del_bytes, Some(1));

        let edit_payload = crate::envelopes::PhotoEditContentPayload { entries: vec![PhotoEditContentEntry { photo_id: photo_id.clone(), resources: vec![PhotoResourceOp { resource_type: 1, op: make_blob_op(CustomUUID::retention_cutoff(6)) }], encrypted_metadata: None, metadata_nonce: None, operation_id: CustomUUID::retention_cutoff(7) }] };
        let edit_bytes = bincode::serde::encode_to_vec(&edit_payload, bincode::config::standard()).unwrap();
        let result = validate(&conn, &PhotoEditContentHandler, "photo_edit_content", &edit_bytes, Some(1));
        assert!(matches!(result, Err(DatabaseError::ConflictError)));
    }

    #[test]
    fn photo_favorite_inserts() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(photo_id.clone(), 1, CustomUUID::retention_cutoff(1), CustomUUID::retention_cutoff(2), None);
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let fav_payload = crate::envelopes::PhotoFavoritePayload { entries: vec![PhotoFavoriteEntry { photo_id: photo_id.clone(), operation_id: CustomUUID::retention_cutoff(3) }] };
        let fav_bytes = bincode::serde::encode_to_vec(&fav_payload, bincode::config::standard()).unwrap();
        apply(&conn, &PhotoFavoriteHandler, "photo_favorite", &fav_bytes, Some(1));

        let count: i32 = conn.query_row("SELECT COUNT(*) FROM photo_favorites WHERE photo_id=?1 AND user_id=1", rusqlite::params![photo_id], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn photo_favorite_rejects_tombstoned() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(photo_id.clone(), 1, CustomUUID::retention_cutoff(1), CustomUUID::retention_cutoff(2), None);
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));
        let del_payload = crate::envelopes::PhotoDeletePayload { entries: vec![PhotoDeleteEntry { photo_id: photo_id.clone(), operation_id: CustomUUID::retention_cutoff(4) }] };
        apply(&conn, &PhotoDeleteHandler, "photo_delete", &bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap(), Some(1));

        let fav_payload = crate::envelopes::PhotoFavoritePayload { entries: vec![PhotoFavoriteEntry { photo_id: photo_id.clone(), operation_id: CustomUUID::retention_cutoff(5) }] };
        let fav_bytes = bincode::serde::encode_to_vec(&fav_payload, bincode::config::standard()).unwrap();
        let result = validate(&conn, &PhotoFavoriteHandler, "photo_favorite", &fav_bytes, Some(1));
        assert!(matches!(result, Err(DatabaseError::ConflictError)));
    }

    #[test]
    fn photo_unfavorite_is_idempotent() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(photo_id.clone(), 1, CustomUUID::retention_cutoff(1), CustomUUID::retention_cutoff(2), None);
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let unfav_payload = crate::envelopes::PhotoUnfavoritePayload { entries: vec![PhotoUnfavoriteEntry { photo_id: photo_id.clone(), operation_id: CustomUUID::retention_cutoff(3) }] };
        let unfav_bytes = bincode::serde::encode_to_vec(&unfav_payload, bincode::config::standard()).unwrap();
        // Delete non-existent favorite (idempotent).
        validate(&conn, &PhotoUnfavoriteHandler, "photo_unfavorite", &unfav_bytes, Some(1)).unwrap();
        apply(&conn, &PhotoUnfavoriteHandler, "photo_unfavorite", &unfav_bytes, Some(1));
    }
}
