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
    db_tx
        .execute(
            "INSERT INTO photos
               (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce, cloud_fingerprint)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                entry.photo_id,
                entry.library_id,
                entry.uploaded_by,
                entry.encrypted_metadata,
                entry.metadata_nonce,
                entry.cloud_fingerprint.map(hex::encode),
            ],
        )
        .map_err(|e| {
            if let rusqlite::Error::SqliteFailure(err, _) = &e {
                if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE {
                    // Deterministic on every validator: the fingerprint's
                    // partial UNIQUE index is the dedupe backstop for
                    // admission races (the loser re-resolves and adopts).
                    tracing::warn!(
                        "photo_add: photos row {} rejected — cloud_fingerprint already committed",
                        entry.photo_id
                    );
                    return DatabaseError::InsertError;
                }
            }
            tracing::error!(
                "photo_add: insert photos row {} failed: {e}",
                entry.photo_id
            );
            DatabaseError::InsertError
        })?;

    // photo_resources — one row per resource.
    for resource in &entry.resources {
        db_tx
            .execute(
                "INSERT INTO photo_resources (photo_id, resource_type, data_block_id)
             VALUES (?1, ?2, ?3)",
                params![entry.photo_id, resource.resource_type, resource.op.blob_id,],
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
        None,
        None,
        None,
        None,
    )?;

    upsert_photo_changes(db_tx, &entry.photo_id)?;

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
    let rows = db_tx
        .execute(
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
    db_tx
        .execute(
            "DELETE FROM photo_album_entries WHERE photo_id = ?1",
            params![photo_id],
        )
        .map_err(|e| {
            tracing::error!(
                "photo_delete: clear album_entries for {} failed: {e}",
                photo_id
            );
            DatabaseError::InsertError
        })?;
    db_tx
        .execute(
            "DELETE FROM photo_favorites WHERE photo_id = ?1",
            params![photo_id],
        )
        .map_err(|e| {
            tracing::error!("photo_delete: clear favorites for {} failed: {e}", photo_id);
            DatabaseError::InsertError
        })?;

    // Log operation (type = 2 = delete).
    insert_operation_row(
        db_tx,
        operation_id,
        library_id,
        photo_id,
        2,
        deleted_by,
        None,
        None,
        None,
        None,
    )?;
    upsert_photo_changes(db_tx, photo_id)?;

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
    let rows = db_tx
        .execute(
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
    insert_operation_row(
        db_tx,
        operation_id,
        library_id,
        photo_id,
        8,
        restored_by,
        None,
        None,
        None,
        None,
    )?;
    upsert_photo_changes(db_tx, photo_id)?;

    Ok(())
}

/// Hard-delete a tombstoned photo and all its children (FK order:
/// photo_operations → photo_resources → photo_metadata_access →
/// photo_favorites → photo_album_entries → photos). Called by the
/// `photo_cleanup_expired` consensus handler; the wall-clock 30-day
/// expiry predicate runs only host-side in the scan query.
///
/// The `photo_operations` FK to `photos` (db/mod.rs:156) forces deletion
/// despite the spec's "retained indefinitely" claim — operation rows
/// must be deleted before the photos row, and retaining audit history of
/// a permanently-erased photo has no value.
///
/// Returns NotFound if the photo row doesn't exist (idempotent skip).
/// Skips silently if deleted_at IS NULL (active photo — another node's
/// restore beat this cleanup tx).
pub fn hard_delete_expired_photo(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
    scan_cutoff: &str,
) -> Result<(), DatabaseError> {
    let deleted_at: Option<String> = db_tx
        .query_row(
            "SELECT deleted_at FROM photos WHERE id = ?1",
            rusqlite::params![photo_id],
            |r| r.get(0),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
            _ => {
                tracing::error!("photo_cleanup: query photos row {} failed: {e}", photo_id,);
                DatabaseError::RecallError
            }
        })?;

    if deleted_at.is_none() {
        return Ok(()); // Active photo — restore beat cleanup.
    }

    // Validate the 30-day window deterministically. The scan_cutoff
    // rides the payload, so all validators apply the same predicate.
    // On replay, the cutoff is payload data, not wall-clock, and
    // `datetime()` is deterministic given the same operands. If the
    // window hasn't elapsed, skip — the next scan will catch it.
    let expired: bool = db_tx
        .query_row(
            "SELECT COUNT(*) > 0 FROM photos
             WHERE id = ?1
               AND deleted_at IS NOT NULL
               AND datetime(deleted_at, '+30 days') < datetime(?2)",
            rusqlite::params![photo_id, scan_cutoff],
            |r| r.get(0),
        )
        .map_err(|e| {
            tracing::error!("photo_cleanup: datetime check for {} failed: {e}", photo_id,);
            DatabaseError::RecallError
        })?;

    if !expired {
        return Ok(()); // Window not yet elapsed — still in recovery.
    }

    upsert_photo_changes(db_tx, photo_id)?;

    // Children first, then the photos row (PRAGMA foreign_keys = ON).
    db_tx
        .execute(
            "DELETE FROM photo_operations WHERE photo_id = ?1",
            rusqlite::params![photo_id],
        )
        .map_err(|e| {
            tracing::error!(
                "photo_cleanup: delete photo_operations for {} failed: {e}",
                photo_id
            );
            DatabaseError::InsertError
        })?;
    db_tx
        .execute(
            "DELETE FROM photo_resources WHERE photo_id = ?1",
            rusqlite::params![photo_id],
        )
        .map_err(|e| {
            tracing::error!(
                "photo_cleanup: delete photo_resources for {} failed: {e}",
                photo_id
            );
            DatabaseError::InsertError
        })?;
    db_tx
        .execute(
            "DELETE FROM photo_metadata_access WHERE photo_id = ?1",
            rusqlite::params![photo_id],
        )
        .map_err(|e| {
            tracing::error!(
                "photo_cleanup: delete photo_metadata_access for {} failed: {e}",
                photo_id
            );
            DatabaseError::InsertError
        })?;
    db_tx
        .execute(
            "DELETE FROM photo_favorites WHERE photo_id = ?1",
            rusqlite::params![photo_id],
        )
        .map_err(|e| {
            tracing::error!(
                "photo_cleanup: delete photo_favorites for {} failed: {e}",
                photo_id
            );
            DatabaseError::InsertError
        })?;
    db_tx
        .execute(
            "DELETE FROM photo_album_entries WHERE photo_id = ?1",
            rusqlite::params![photo_id],
        )
        .map_err(|e| {
            tracing::error!(
                "photo_cleanup: delete photo_album_entries for {} failed: {e}",
                photo_id
            );
            DatabaseError::InsertError
        })?;
    db_tx
        .execute(
            "DELETE FROM photos WHERE id = ?1",
            rusqlite::params![photo_id],
        )
        .map_err(|e| {
            tracing::error!("photo_cleanup: delete photos row {} failed: {e}", photo_id);
            DatabaseError::InsertError
        })?;

    Ok(())
}

/// Upsert the ingress responsibility holder for a user's personal scope.
/// Claim and transfer are the same operation — last committed claim wins.
pub fn upsert_ingress_responsibility(
    db_tx: &rusqlite::Transaction,
    user_id: i32,
    device_id: &CustomUUID,
    operation_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT INTO photo_ingress_responsibility (user_id, device_id, operation_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(user_id) DO UPDATE SET device_id = ?2, operation_id = ?3",
            params![user_id, device_id, operation_id],
        )
        .map_err(|e| {
            tracing::error!(
                "photo_ingress_claim: upsert responsibility (user {}, device {}) failed: {e}",
                user_id,
                device_id,
            );
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// Deterministic ownership check against the consensus-replicated
/// `device_tokens` table (host-owned; photos SQL may READ it — the
/// ownership boundary is code, not schema).
pub fn device_belongs_to_user(
    db_tx: &rusqlite::Transaction,
    device_id: &CustomUUID,
    user_id: i32,
) -> Result<bool, DatabaseError> {
    db_tx
        .query_row(
            "SELECT COUNT(*) > 0 FROM device_tokens WHERE id = ?1 AND user_id = ?2",
            params![device_id, user_id],
            |r| r.get(0),
        )
        .map_err(|e| {
            tracing::error!("device_belongs_to_user ({}, {}) failed: {e}", device_id, user_id);
            DatabaseError::RecallError
        })
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

pub fn insert_operation_row(
    db_tx: &rusqlite::Transaction,
    operation_id: &CustomUUID,
    library_id: Option<&CustomUUID>,
    photo_id: &CustomUUID,
    operation_type: i32,
    performed_by: i32,
    resource_type: Option<i32>,
    prior_data_block_id: Option<&CustomUUID>,
    new_data_block_id: Option<&CustomUUID>,
    operation_data: Option<&[u8]>,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT INTO photo_operations
           (id, library_id, photo_id, operation_type, resource_type,
            prior_data_block_id, new_data_block_id, operation_data, performed_by)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                operation_id,
                library_id,
                photo_id,
                operation_type,
                resource_type,
                prior_data_block_id,
                new_data_block_id,
                operation_data,
                performed_by,
            ],
        )
        .map_err(|e| {
            tracing::error!("insert photo_operations row {} failed: {e}", operation_id,);
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// UPSERT the `photo_changes` feed row for incremental sync. Called from
/// every handler that mutates a photo. The feed table has NO FK — the row
/// survives hard-delete so offline clients learn of the tombstone expiry.
pub fn upsert_photo_changes(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    let height = hopnet_projection::current_height(db_tx)? + 1;
    db_tx
        .execute(
            "INSERT OR REPLACE INTO photo_changes (photo_id, changed_at_height)
         VALUES (?1, ?2)",
            params![photo_id, height],
        )
        .map_err(|e| {
            tracing::error!("upsert photo_changes for {} failed: {e}", photo_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// Look up a photo's owner, tombstone state, and library scope. Returns
/// None if the photo doesn't exist. Used by mutation handlers for authz
/// (owner-or-library-member) + tombstone gating.
pub fn lookup_photo_authz(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
) -> Result<Option<(i32, Option<String>, Option<CustomUUID>)>, DatabaseError> {
    match db_tx.query_row(
        "SELECT uploaded_by, deleted_at, library_id FROM photos WHERE id = ?1",
        rusqlite::params![photo_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ) {
        Ok(row) => Ok(Some(row)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => {
            tracing::error!("lookup_photo_authz {} failed: {e}", photo_id);
            Err(DatabaseError::RecallError)
        }
    }
}

/// Resolve one resource of one photo to its backing data block. Returns
/// None if the photo has no resource of that type (or no photo at all).
///
/// Tombstone-agnostic by design: soft-deleted photos still serve content
/// (the recently-deleted UI needs thumbnails and the blobs stay pinned for
/// the 30-day window); hard-deleted rows are gone, so None -> 404 upstream.
///
/// `NotMember` is returned for a shared photo the reading user has no
/// membership for — the byte gate is wrap AND membership (the wrap alone
/// may be a pre-staged invitee grant or a kicked member's stale row).
#[derive(Debug, PartialEq, Eq)]
pub enum ResourceBlockLookup {
    Found(CustomUUID),
    NotFound,
    NotMember,
}

pub fn lookup_resource_block_authz(
    conn: &rusqlite::Connection,
    photo_id: &CustomUUID,
    resource_type: i32,
    user_id: i32,
) -> Result<ResourceBlockLookup, DatabaseError> {
    match conn.query_row(
        "SELECT r.data_block_id,
                (p.library_id IS NULL
                 OR EXISTS (SELECT 1 FROM shared_library_members m
                            WHERE m.library_id = p.library_id
                              AND m.user_id = ?3))
         FROM photo_resources r
         JOIN photos p ON p.id = r.photo_id
         WHERE r.photo_id = ?1 AND r.resource_type = ?2",
        params![photo_id, resource_type, user_id],
        |r| Ok((r.get::<_, CustomUUID>(0)?, r.get::<_, bool>(1)?)),
    ) {
        Ok((id, member)) => Ok(if member {
            ResourceBlockLookup::Found(id)
        } else {
            ResourceBlockLookup::NotMember
        }),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(ResourceBlockLookup::NotFound),
        Err(e) => {
            tracing::error!("lookup_resource_block {photo_id}/{resource_type} failed: {e}");
            Err(DatabaseError::RecallError)
        }
    }
}

/// Look up a user's blob_access wrap for a photo resource's data block.
/// None = no wrap for this user -> the caller treats it as forbidden; the
/// wrap is cryptographically required to decrypt, so its existence IS the
/// read grant.
pub fn get_blob_access_for_user(
    conn: &rusqlite::Connection,
    data_block_id: &CustomUUID,
    user_id: i32,
) -> Result<Option<hopnet_storage::BlobAccess>, DatabaseError> {
    // Projection half: user → pubkey; substrate half: pubkey-keyed wrap.
    // The users table is HOST-owned; photos SQL may READ it (same SQLite DB —
    // the ownership boundary is code, not schema), so this is a local lookup
    // rather than a call into the host's users module.
    use rusqlite::OptionalExtension;
    let pubkey: Option<[u8; 32]> = conn
        .query_row(
            "SELECT x25519_pubkey FROM users WHERE user_id = ?",
            params![user_id],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?
        .map(|blob| <[u8; 32]>::try_from(blob).map_err(|_| DatabaseError::RecallError))
        .transpose()?;
    let pubkey = match pubkey {
        Some(pubkey) => pubkey,
        None => return Ok(None),
    };
    hopnet_storage::store::get_blob_access(conn, data_block_id, &pubkey)
        .map_err(|_| DatabaseError::RecallError)
}

/// Content edit: replace a resource's blob + optionally update metadata.
/// Looks up the current data_block_id as prior (LWW contract). Returns
/// NotFound if the photo doesn't exist; the handler rejects tombstoned
/// photos before calling this.
pub fn edit_photo_content(
    db_tx: &rusqlite::Transaction,
    entry: &crate::envelopes::PhotoEditContentEntry,
    fragments_dir: &str,
    performed_by: i32,
) -> Result<(), DatabaseError> {
    // Substrate half: register new blobs (primary edit + thumbnails).
    for resource in &entry.resources {
        hopnet_storage::store::apply_blob_insert(
            db_tx,
            &resource.op,
            &hopnet_storage::store::ApplyCtx { fragments_dir },
        )
        .map_err(|e| {
            tracing::error!(
                "photo_edit_content: apply_blob_insert {} failed: {e}",
                resource.op.blob_id,
            );
            DatabaseError::InsertError
        })?;
    }

    // LWW: read current data_block_id from photo_resources. The resource
    // may not exist yet (first edit for a photo that only had original).
    let primary = &entry.resources[0];
    let prior: Option<CustomUUID> = db_tx
        .query_row(
            "SELECT data_block_id FROM photo_resources
             WHERE photo_id = ?1 AND resource_type = ?2",
            rusqlite::params![entry.photo_id, primary.resource_type],
            |r| r.get(0),
        )
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            _ => {
                tracing::error!(
                    "photo_edit_content: lookup prior for {} failed: {e}",
                    entry.photo_id,
                );
                Err(DatabaseError::RecallError)
            }
        })?;

    // Upsert all resources (primary edit + thumbnails).
    for resource in &entry.resources {
        db_tx
            .execute(
                "INSERT INTO photo_resources (photo_id, resource_type, data_block_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(photo_id, resource_type) DO UPDATE SET data_block_id = ?3",
                params![entry.photo_id, resource.resource_type, resource.op.blob_id],
            )
            .map_err(|e| {
                tracing::error!(
                    "photo_edit_content: upsert resource ({}, {}) failed: {e}",
                    entry.photo_id,
                    resource.resource_type,
                );
                DatabaseError::InsertError
            })?;
    }

    // Optional metadata update.
    if let (Some(meta), Some(nonce)) = (&entry.encrypted_metadata, &entry.metadata_nonce) {
        db_tx
            .execute(
                "UPDATE photos SET encrypted_metadata = ?1, metadata_nonce = ?2 WHERE id = ?3",
                params![meta, nonce, entry.photo_id],
            )
            .map_err(|e| {
                tracing::error!(
                    "photo_edit_content: update metadata {} failed: {e}",
                    entry.photo_id
                );
                DatabaseError::InsertError
            })?;
    }

    upsert_photo_changes(db_tx, &entry.photo_id)?;

    // Log operation — type=1 with prior/new.
    insert_operation_row(
        db_tx,
        &entry.operation_id,
        None, // library_id — denormalized, not computed here
        &entry.photo_id,
        1, // content_edit
        performed_by,
        Some(primary.resource_type),
        prior.as_ref(),
        Some(&primary.op.blob_id),
        None,
    )?;

    Ok(())
}

/// Metadata-only edit.
pub fn edit_photo_metadata(
    db_tx: &rusqlite::Transaction,
    entry: &crate::envelopes::PhotoEditMetadataEntry,
    performed_by: i32,
) -> Result<(), DatabaseError> {
    let prior_meta: Option<Vec<u8>> = db_tx
        .query_row(
            "SELECT encrypted_metadata FROM photos WHERE id = ?1",
            rusqlite::params![entry.photo_id],
            |r| r.get(0),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
            _ => {
                tracing::error!("edit_metadata lookup {} failed: {e}", entry.photo_id);
                DatabaseError::RecallError
            }
        })?;

    db_tx
        .execute(
            "UPDATE photos SET encrypted_metadata = ?1, metadata_nonce = ?2 WHERE id = ?3",
            params![
                entry.encrypted_metadata,
                entry.metadata_nonce,
                entry.photo_id
            ],
        )
        .map_err(|e| {
            tracing::error!("edit_metadata update {} failed: {e}", entry.photo_id);
            DatabaseError::InsertError
        })?;

    upsert_photo_changes(db_tx, &entry.photo_id)?;

    insert_operation_row(
        db_tx,
        &entry.operation_id,
        None,
        &entry.photo_id,
        3, // metadata_edit
        performed_by,
        None,
        None,
        None,
        Some(prior_meta.as_deref().unwrap_or(&[])), // log prior metadata as operation_data
    )?;

    Ok(())
}

/// Content undo: swap a resource back to its prior blob.
pub fn undo_content_edit(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
    target_operation_id: &CustomUUID,
    operation_id: &CustomUUID,
    performed_by: i32,
) -> Result<(), DatabaseError> {
    // Read the target operation — must be a content_edit with a prior.
    let (resource_type, prior_id, current_id): (i32, Option<CustomUUID>, Option<CustomUUID>) =
        db_tx
            .query_row(
                "SELECT resource_type, prior_data_block_id, new_data_block_id
             FROM photo_operations
             WHERE id = ?1 AND photo_id = ?2 AND operation_type = 1",
                rusqlite::params![target_operation_id, photo_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
                _ => {
                    tracing::error!("undo lookup op {} failed: {e}", target_operation_id);
                    DatabaseError::RecallError
                }
            })?;

    let prior_id = prior_id.ok_or_else(|| {
        tracing::warn!(
            "undo: op {} has no prior — nothing to revert",
            target_operation_id
        );
        DatabaseError::NotFound
    })?;

    // Verify the prior blob still exists (soft pointer — may have been
    // collected by the orphan sweep after 30d).
    let blob_exists: bool = db_tx
        .query_row(
            "SELECT COUNT(*) > 0 FROM data_blocks WHERE id = ?1",
            rusqlite::params![prior_id],
            |r| r.get(0),
        )
        .map_err(|e| {
            tracing::error!("undo blob-exists check {} failed: {e}", prior_id);
            DatabaseError::RecallError
        })?;
    if !blob_exists {
        tracing::warn!(
            "undo: prior blob {} for photo {} has been collected",
            prior_id,
            photo_id,
        );
        return Err(DatabaseError::NotFound);
    }

    // Swap back.
    db_tx
        .execute(
            "UPDATE photo_resources
         SET data_block_id = ?1
         WHERE photo_id = ?2 AND resource_type = ?3",
            params![prior_id, photo_id, resource_type],
        )
        .map_err(|e| {
            tracing::error!(
                "undo UPDATE photo_resources ({}, {}) failed: {e}",
                photo_id,
                resource_type
            );
            DatabaseError::InsertError
        })?;

    upsert_photo_changes(db_tx, photo_id)?;

    // Log undo as type=1 with prior/new swapped.
    insert_operation_row(
        db_tx,
        operation_id,
        None,
        photo_id,
        1, // content_edit — LWW chain stays contiguous
        performed_by,
        Some(resource_type),
        current_id.as_ref(),
        Some(&prior_id),
        None,
    )?;

    Ok(())
}

/// Insert a favorite row (INSERT OR IGNORE — PK collision is idempotent).
pub fn insert_favorite(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
    user_id: i32,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT OR IGNORE INTO photo_favorites (photo_id, user_id) VALUES (?1, ?2)",
            params![photo_id, user_id],
        )
        .map_err(|e| {
            tracing::error!("insert_favorite ({}, {}) failed: {e}", photo_id, user_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// Remove a favorite row (DELETE with 0 rows OK — idempotent).
pub fn delete_favorite(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
    user_id: i32,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "DELETE FROM photo_favorites WHERE photo_id = ?1 AND user_id = ?2",
            params![photo_id, user_id],
        )
        .map_err(|e| {
            tracing::error!("delete_favorite ({}, {}) failed: {e}", photo_id, user_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

// --- Read-only queries for sidecar sync ---

/// One row from the `photo_changes` incremental feed, joined with `photos`
/// and `photo_metadata_access`. Callers (host, dispatch) map these raw rows
/// into `hopnet_photos_core::dispatch::SyncBatch`.
pub struct ChangeRow {
    pub photo_id: CustomUUID,
    pub changed_at_height: i64,
    pub row_exists: bool,
    pub library_id: Option<CustomUUID>,
    pub uploaded_by: Option<i32>,
    pub encrypted_metadata: Option<Vec<u8>>,
    pub metadata_nonce: Option<[u8; 12]>,
    pub deleted_at: Option<String>,
    pub deleted_by: Option<i32>,
    pub eph_pubkey: Option<[u8; 32]>,
    pub enc_meta_key: Option<Vec<u8>>,
}

pub(crate) fn map_change_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChangeRow> {
    let nonce_blob: Option<Vec<u8>> = row.get(6)?;
    let eph_blob: Option<Vec<u8>> = row.get(9)?;
    Ok(ChangeRow {
        photo_id: row.get(0)?,
        changed_at_height: row.get(1)?,
        row_exists: row.get::<_, Option<CustomUUID>>(2)?.is_some(),
        library_id: row.get(3)?,
        uploaded_by: row.get(4)?,
        encrypted_metadata: row.get(5)?,
        metadata_nonce: nonce_blob.and_then(|b| b.try_into().ok()),
        deleted_at: row.get(7)?,
        deleted_by: row.get(8)?,
        eph_pubkey: eph_blob.and_then(|b| b.try_into().ok()),
        enc_meta_key: row.get(10)?,
    })
}

/// Query `photo_changes` since a given height, joined with photo row and
/// per-user metadata access. Returns rows ordered by `changed_at_height` ASC.
/// Capped at `limit` rows to avoid unbounded sync batches; callers paginate
/// by advancing `since_height` from the last returned `changed_at_height`.
pub fn query_changes(
    conn: &rusqlite::Connection,
    user_id: i32,
    since_height: u64,
    limit: u64,
) -> Result<Vec<ChangeRow>, DatabaseError> {
    let mut stmt = conn
        .prepare(
            "SELECT pc.photo_id, pc.changed_at_height,
                    p.id, p.library_id, p.uploaded_by, p.encrypted_metadata,
                    p.metadata_nonce, p.deleted_at, p.deleted_by,
                    pma.ephemeral_pubkey, pma.encrypted_metadata_key
             FROM photo_changes pc
             LEFT JOIN photos p ON p.id = pc.photo_id
             LEFT JOIN photo_metadata_access pma
                 ON pma.photo_id = pc.photo_id AND pma.user_id = ?1
             WHERE pc.changed_at_height > ?2
               -- Active photos: scoped to users with a metadata_access row.
               -- Hard-deleted photos (p.id IS NULL): the photo + access rows
               -- are gone (FK cascade in hard_delete_expired_photo), but the
               -- photo_changes row survives so offline clients learn of the
               -- tombstone expiry. photo_id alone (a UUIDv7 handle) carries
               -- no content — the encrypted metadata and resources were
               -- deleted.
               AND (pma.photo_id IS NOT NULL OR p.id IS NULL)
               -- Shared photos additionally require MEMBERSHIP: access-row
               -- existence alone is not the read grant for library photos
               -- (pre-staged invitee wraps must stay inert until accept,
               -- and a kicked member loses reads the instant the
               -- membership row dies). Personal photos (NULL library) keep
               -- existence-as-grant.
               AND (p.id IS NULL OR p.library_id IS NULL
                    OR EXISTS (SELECT 1 FROM shared_library_members m
                               WHERE m.library_id = p.library_id
                                 AND m.user_id = ?1))
             ORDER BY pc.changed_at_height ASC, pc.photo_id ASC
             LIMIT ?3",
        )
        .map_err(|e| {
            tracing::error!("query_changes prepare: {e}");
            DatabaseError::RecallError
        })?;

    let mut rows = stmt
        .query_map(
            params![user_id, since_height as i64, limit as i64],
            map_change_row,
        )
        .map_err(|e| {
            tracing::error!("query_changes: {e}");
            DatabaseError::RecallError
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            tracing::error!("query_changes collect: {e}");
            DatabaseError::RecallError
        })?;

    // Never split a consensus height: every mutation in one decided block
    // shares changed_at_height. Extend a capped batch through the complete
    // boundary height so a height-only cursor cannot skip tied rows.
    if rows.len() == limit as usize && !rows.is_empty() {
        let Some(boundary) = rows.last() else {
            return Ok(rows);
        };
        let boundary_height = boundary.changed_at_height;
        let boundary_photo_id = boundary.photo_id.clone();
        let mut boundary_stmt = conn
            .prepare(
                "SELECT pc.photo_id, pc.changed_at_height,
                        p.id, p.library_id, p.uploaded_by, p.encrypted_metadata,
                        p.metadata_nonce, p.deleted_at, p.deleted_by,
                        pma.ephemeral_pubkey, pma.encrypted_metadata_key
                 FROM photo_changes pc
                 LEFT JOIN photos p ON p.id = pc.photo_id
                 LEFT JOIN photo_metadata_access pma
                     ON pma.photo_id = pc.photo_id AND pma.user_id = ?1
                 WHERE pc.changed_at_height = ?2
                   AND pc.photo_id > ?3
                   AND (pma.photo_id IS NOT NULL OR p.id IS NULL)
                   -- Same membership gate as the main statement — a capped
                   -- batch must not leak shared rows at the boundary.
                   AND (p.id IS NULL OR p.library_id IS NULL
                        OR EXISTS (SELECT 1 FROM shared_library_members m
                                   WHERE m.library_id = p.library_id
                                     AND m.user_id = ?1))
                 ORDER BY pc.photo_id ASC",
            )
            .map_err(|e| {
                tracing::error!("query_changes boundary prepare: {e}");
                DatabaseError::RecallError
            })?;
        let boundary_rows = boundary_stmt
            .query_map(
                params![user_id, boundary_height, boundary_photo_id],
                map_change_row,
            )
            .map_err(|e| {
                tracing::error!("query_changes boundary: {e}");
                DatabaseError::RecallError
            })?;
        for row in boundary_rows {
            rows.push(row.map_err(|e| {
                tracing::error!("query_changes boundary row: {e}");
                DatabaseError::RecallError
            })?);
        }
    }

    Ok(rows)
}

/// Batch-fetch `photo_resources` for a set of photo IDs.
pub fn query_resources(
    conn: &rusqlite::Connection,
    photo_ids: &[&CustomUUID],
) -> Result<std::collections::HashMap<CustomUUID, Vec<(i32, CustomUUID)>>, DatabaseError> {
    use std::collections::HashMap;
    let mut map: HashMap<CustomUUID, Vec<(i32, CustomUUID)>> = HashMap::new();
    if photo_ids.is_empty() {
        return Ok(map);
    }
    for chunk in photo_ids.chunks(500) {
        let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            "SELECT photo_id, resource_type, data_block_id FROM photo_resources WHERE photo_id IN ({})",
            placeholders.join(",")
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            tracing::error!("query_resources prepare: {e}");
            DatabaseError::RecallError
        })?;
        let params: Vec<&dyn rusqlite::types::ToSql> = chunk
            .iter()
            .map(|id| *id as &dyn rusqlite::types::ToSql)
            .collect();
        let rows = stmt
            .query_map(params.as_slice(), |row| {
                Ok((
                    row.get::<_, CustomUUID>(0)?,
                    row.get::<_, i32>(1)?,
                    row.get::<_, CustomUUID>(2)?,
                ))
            })
            .map_err(|e| {
                tracing::error!("query_resources: {e}");
                DatabaseError::RecallError
            })?;
        for row in rows {
            let (pid, rt, bid) = row.map_err(|e| {
                tracing::error!("query_resources row: {e}");
                DatabaseError::RecallError
            })?;
            map.entry(pid).or_default().push((rt, bid));
        }
    }
    Ok(map)
}

/// Current decided consensus height, cast to u64. Pre-genesis reads as 0.
pub fn read_current_height(conn: &rusqlite::Connection) -> Result<u64, DatabaseError> {
    let h: i32 = hopnet_projection::current_height(conn)?;
    Ok(if h > 0 { h as u64 } else { 0 })
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
            cloud_fingerprint: None,
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
            cloud_fingerprint: None,
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();

        // Delete.
        let tx = conn.unchecked_transaction().unwrap();
        soft_delete_photo(
            &tx,
            &entry.photo_id,
            1, // deleted_by
            "2025-06-01T00:00:00Z",
            None, // personal library
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
        assert!(
            deleted.is_some(),
            "deleted_at must be set after soft-delete"
        );

        // photo_resources still present (pins blob for GC).
        let res_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM photo_resources WHERE photo_id = ?1",
                rusqlite::params![entry.photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            res_count, 1,
            "photo_resources retained during tombstone window"
        );
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
            cloud_fingerprint: None,
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
            cloud_fingerprint: None,
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
            cloud_fingerprint: None,
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

    /// hard_delete removes all six row types for a tombstoned photo,
    /// but leaves data_blocks + blob_access untouched (orphan sweep owns
    /// those).
    #[test]
    fn hard_delete_removes_all_child_rows() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let blob_id = CustomUUID::retention_cutoff(1);
        let entry = PhotoAddEntry {
            photo_id: photo_id.clone(),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"enc_meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![PhotoResourceOp {
                resource_type: 0,
                op: make_blob_op(blob_id.clone()),
            }],
            metadata_access: vec![crate::envelopes::MetadataAccessEntry {
                user_id: 1,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: CustomUUID::retention_cutoff(2),
            cloud_fingerprint: None,
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();

        // Soft-delete first.
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

        // Hard-delete.
        let tx = conn.unchecked_transaction().unwrap();
        hard_delete_expired_photo(&tx, &photo_id, "2099-01-01T00:00:00Z").unwrap();
        tx.commit().unwrap();

        // All projection rows gone.
        for (table, col) in [
            ("photos", "id"),
            ("photo_resources", "photo_id"),
            ("photo_metadata_access", "photo_id"),
            ("photo_favorites", "photo_id"),
            ("photo_album_entries", "photo_id"),
            ("photo_operations", "photo_id"),
        ] {
            let count: i32 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE {col} = ?1"),
                    rusqlite::params![photo_id],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, 0, "{table} rows must be empty after hard-delete");
        }
        // data_blocks row survives (orphan sweep owns it).
        let blob_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM data_blocks WHERE id = ?1",
                rusqlite::params![blob_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(blob_count, 1, "data_blocks row survives hard-delete");
    }

    /// Double hard-delete: second call returns NotFound, no side effects.
    #[test]
    fn hard_delete_missing_photo_is_notfound() {
        let conn = fixture();
        let tx = conn.unchecked_transaction().unwrap();
        let result = hard_delete_expired_photo(
            &tx,
            &CustomUUID::retention_cutoff(99),
            "2099-01-01T00:00:00Z",
        );
        assert!(matches!(result, Err(DatabaseError::NotFound)));
    }

    /// Active (non-tombstoned) photo is skipped silently.
    #[test]
    fn hard_delete_active_photo_is_noop() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let entry = PhotoAddEntry {
            photo_id: photo_id.clone(),
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
            cloud_fingerprint: None,
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        hard_delete_expired_photo(&tx, &photo_id, "2099-01-01T00:00:00Z").unwrap(); // no error
        tx.commit().unwrap();

        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(exists, "active photo must survive cleanup attempt");
    }

    // Impact: this is Design B's metadata read gate — access-row existence
    // alone must NOT surface shared photos. Pre-staged invitee wraps stay
    // inert until the accept commits a membership row, and a kicked
    // member's stale wraps go dark the instant the row dies.
    // Should: surface a shared photo to a member with a wrap.
    // Should not: surface it to a wrap-holding non-member (invitee
    // pre-accept, kicked member), in the main batch OR the boundary
    // extension.
    #[test]
    fn query_changes_gates_shared_photos_on_membership() {
        let conn = fixture();
        conn.execute("INSERT INTO users (user_id, x25519_pubkey) VALUES (2, x'02')", [])
            .unwrap();
        conn.execute(
            "INSERT INTO shared_libraries (id, encrypted_name, name_nonce)
             VALUES ('00000000-0000-0000-0000-0000000000f1', x'00', x'00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO shared_library_members (library_id, user_id) VALUES ('00000000-0000-0000-0000-0000000000f1', 1)",
            [],
        )
        .unwrap();
        let photo_id = CustomUUID::retention_cutoff(40);
        conn.execute(
            "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
             VALUES (?1, '00000000-0000-0000-0000-0000000000f1', 1, x'00', x'00')",
            rusqlite::params![photo_id],
        )
        .unwrap();
        // BOTH users hold metadata wraps — user 2 is a pre-staged invitee
        // (or a kicked member): wrap present, membership absent.
        for uid in [1, 2] {
            conn.execute(
                "INSERT INTO photo_metadata_access
                   (photo_id, user_id, ephemeral_pubkey, encrypted_metadata_key)
                 VALUES (?1, ?2, x'00', x'00')",
                rusqlite::params![photo_id, uid],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO photo_changes (photo_id, changed_at_height) VALUES (?1, 5)",
            rusqlite::params![photo_id],
        )
        .unwrap();

        let member_rows = query_changes(&conn, 1, 0, 500).unwrap();
        assert_eq!(member_rows.len(), 1, "member sees the shared photo");
        let outsider_rows = query_changes(&conn, 2, 0, 500).unwrap();
        assert!(
            outsider_rows.is_empty(),
            "wrap without membership must surface nothing"
        );

        // Boundary-extension path: cap the batch exactly at the boundary
        // height so the second statement runs, and pin the same gate there.
        let filler = CustomUUID::retention_cutoff(39); // sorts before photo_id
        conn.execute(
            "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
             VALUES (?1, NULL, 2, x'00', x'00')",
            rusqlite::params![filler],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photo_metadata_access
               (photo_id, user_id, ephemeral_pubkey, encrypted_metadata_key)
             VALUES (?1, 2, x'00', x'00')",
            rusqlite::params![filler],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photo_changes (photo_id, changed_at_height) VALUES (?1, 5)",
            rusqlite::params![filler],
        )
        .unwrap();
        let capped = query_changes(&conn, 2, 0, 1).unwrap();
        assert_eq!(
            capped.len(),
            1,
            "boundary extension must not leak the shared photo to a non-member"
        );
        assert_eq!(capped[0].photo_id, filler);
    }

    #[test]
    fn query_changes_extends_through_boundary_height() {
        let conn = fixture();
        for i in 0..600 {
            let photo_id = CustomUUID::new(None);
            conn.execute(
                "INSERT INTO photos
                    (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
                 VALUES (?1, NULL, 1, ?2, ?3)",
                rusqlite::params![photo_id, vec![i as u8], [0u8; 12]],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO photo_metadata_access
                    (photo_id, user_id, ephemeral_pubkey, encrypted_metadata_key)
                 VALUES (?1, 1, ?2, ?3)",
                rusqlite::params![photo_id, [1u8; 32], vec![2u8; 48]],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO photo_changes (photo_id, changed_at_height) VALUES (?1, 7)",
                rusqlite::params![photo_id],
            )
            .unwrap();
        }

        let rows = query_changes(&conn, 1, 0, 500).unwrap();
        assert_eq!(
            rows.len(),
            600,
            "a page must not split one consensus height"
        );
        assert!(rows.iter().all(|row| row.changed_at_height == 7));
    }

    #[test]
    fn photo_changes_stamps_next_apply_height() {
        let conn = fixture();
        let photo_id = CustomUUID::new(None);
        let tx = conn.unchecked_transaction().unwrap();
        upsert_photo_changes(&tx, &photo_id).unwrap();
        let height: i64 = tx
            .query_row(
                "SELECT changed_at_height FROM photo_changes WHERE photo_id = ?1",
                rusqlite::params![photo_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(height, 1, "pre-genesis apply height is current_height + 1");
    }

    // --- content-serving lookups ---

    fn insert_user_with_pubkey(
        conn: &Connection,
        user_id: i32,
        pubkey: &hopnet_storage::x25519_dalek::PublicKey,
    ) {
        conn.execute(
            "INSERT INTO users (user_id, x25519_pubkey) VALUES (?1, ?2)",
            rusqlite::params![user_id, pubkey.as_bytes().to_vec()],
        )
        .unwrap();
    }

    fn insert_photo_with_access(
        conn: &Connection,
        photo_id: &CustomUUID,
        blob_id: &CustomUUID,
        access: Vec<hopnet_storage::BlobAccess>,
    ) {
        let mut op = make_blob_op(blob_id.clone());
        op.access = access;
        let entry = PhotoAddEntry {
            photo_id: photo_id.clone(),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"enc_meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![PhotoResourceOp {
                resource_type: 0,
                op,
            }],
            metadata_access: vec![crate::envelopes::MetadataAccessEntry {
                user_id: 1,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: CustomUUID::new(None),
            cloud_fingerprint: None,
        };
        let tx = conn.unchecked_transaction().unwrap();
        insert_photo_entry(&tx, &entry, "/tmp/fragments").unwrap();
        tx.commit().unwrap();
    }

    // Should: resolve a declared (photo, resource type) pair to its data
    // block — any authenticated user for a personal photo.
    // Should not: report a data block for an undeclared type or unknown photo.
    #[test]
    fn lookup_resource_block_resolves_and_misses() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(10);
        let blob_id = CustomUUID::retention_cutoff(11);
        insert_photo_with_access(&conn, &photo_id, &blob_id, vec![]);

        assert_eq!(
            lookup_resource_block_authz(&conn, &photo_id, 0, 1).unwrap(),
            ResourceBlockLookup::Found(blob_id)
        );
        assert_eq!(
            lookup_resource_block_authz(&conn, &photo_id, 5, 1).unwrap(),
            ResourceBlockLookup::NotFound
        );
        assert_eq!(
            lookup_resource_block_authz(&conn, &CustomUUID::retention_cutoff(99), 0, 1).unwrap(),
            ResourceBlockLookup::NotFound
        );
    }

    // Impact: the byte gate for shared photos is wrap AND membership — a
    // pre-staged invitee wrap or a kicked member's stale row must never
    // serve bytes on its own.
    // Should: serve a member; refuse a non-member with NotMember (403
    // upstream, distinct from a 404).
    #[test]
    fn lookup_resource_block_gates_shared_on_membership() {
        let conn = fixture();
        conn.execute(
            "INSERT INTO shared_libraries (id, encrypted_name, name_nonce)
             VALUES ('lib1', x'00', x'00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO shared_library_members (library_id, user_id) VALUES ('lib1', 1)",
            [],
        )
        .unwrap();
        let photo_id = CustomUUID::retention_cutoff(30);
        let blob_id = CustomUUID::retention_cutoff(31);
        conn.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, file_size)
             VALUES (?1, x'00', 1, 0, 10)",
            params![blob_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
             VALUES (?1, 'lib1', 1, x'00', x'00')",
            params![photo_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photo_resources (photo_id, resource_type, data_block_id)
             VALUES (?1, 0, ?2)",
            params![photo_id, blob_id],
        )
        .unwrap();

        assert_eq!(
            lookup_resource_block_authz(&conn, &photo_id, 0, 1).unwrap(),
            ResourceBlockLookup::Found(blob_id)
        );
        assert_eq!(
            lookup_resource_block_authz(&conn, &photo_id, 0, 2).unwrap(),
            ResourceBlockLookup::NotMember,
            "non-member must be refused even though the resource exists"
        );
    }

    // Impact: the recently-deleted UI needs content during the 30-day window,
    // so a tombstone must not sever resource resolution.
    // Should: keep resolving a soft-deleted photo's resources.
    #[test]
    fn lookup_resource_block_survives_soft_delete() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(20);
        let blob_id = CustomUUID::retention_cutoff(21);
        insert_photo_with_access(&conn, &photo_id, &blob_id, vec![]);

        let tx = conn.unchecked_transaction().unwrap();
        soft_delete_photo(
            &tx,
            &photo_id,
            1,
            "2025-06-01T00:00:00Z",
            None,
            &CustomUUID::retention_cutoff(22),
        )
        .unwrap();
        tx.commit().unwrap();

        assert_eq!(
            lookup_resource_block_authz(&conn, &photo_id, 0, 1).unwrap(),
            ResourceBlockLookup::Found(blob_id)
        );
    }

    // Impact: the blob_access wrap is the sole read-authorization gate for
    // photo content — its presence IS the grant.
    // Should: return the access row only for a user holding a wrap for the blob.
    // Should not: return a row for a wrap-less user or an unknown user.
    #[test]
    fn blob_access_lookup_gates_on_wrap() {
        let conn = fixture();
        let granted_secret = hopnet_storage::x25519_dalek::StaticSecret::from([0xA1; 32]);
        let granted_pubkey = hopnet_storage::x25519_dalek::PublicKey::from(&granted_secret);
        let bystander_secret = hopnet_storage::x25519_dalek::StaticSecret::from([0xB2; 32]);
        let bystander_pubkey = hopnet_storage::x25519_dalek::PublicKey::from(&bystander_secret);
        insert_user_with_pubkey(&conn, 2, &granted_pubkey);
        insert_user_with_pubkey(&conn, 3, &bystander_pubkey);

        let photo_id = CustomUUID::retention_cutoff(30);
        let blob_id = CustomUUID::retention_cutoff(31);
        let per_blob_key = chacha20poly1305::Key::from([0x11; 32]);
        let wrap =
            hopnet_storage::crypto::wrap_blob_key(&blob_id, &granted_pubkey, &per_blob_key)
                .unwrap();
        insert_photo_with_access(&conn, &photo_id, &blob_id, vec![wrap]);

        assert!(get_blob_access_for_user(&conn, &blob_id, 2).unwrap().is_some());
        assert!(get_blob_access_for_user(&conn, &blob_id, 3).unwrap().is_none());
        assert!(get_blob_access_for_user(&conn, &blob_id, 99).unwrap().is_none());
    }

    // Should: recover the exact per-blob key by unwrapping the stored access
    // row with the recipient's private key.
    #[test]
    fn blob_access_round_trips_through_unwrap() {
        let conn = fixture();
        let secret = hopnet_storage::x25519_dalek::StaticSecret::from([0xC3; 32]);
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from(&secret);
        insert_user_with_pubkey(&conn, 2, &pubkey);

        let photo_id = CustomUUID::retention_cutoff(40);
        let blob_id = CustomUUID::retention_cutoff(41);
        let per_blob_key = chacha20poly1305::Key::from([0x77; 32]);
        let wrap = hopnet_storage::crypto::wrap_blob_key(&blob_id, &pubkey, &per_blob_key).unwrap();
        insert_photo_with_access(&conn, &photo_id, &blob_id, vec![wrap]);

        let access = get_blob_access_for_user(&conn, &blob_id, 2).unwrap().unwrap();
        let unwrapped = hopnet_storage::crypto::unwrap_blob_key(
            &access,
            &hopnet_storage::StaticRecipient(secret),
        )
        .unwrap();
        assert_eq!(unwrapped.as_slice(), per_blob_key.as_slice());
    }
}
