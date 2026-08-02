//! Drive file/inode DB operations (RFC-015).
//!
//! Moved verbatim from the host's `db::files`; the host re-exports this
//! module at its old path. The two `stored_locally` functions stayed
//! host-side (substrate seam) in the host's `db::fragments`.

use crate::model::{CustomDateTime, FileAccessData, Inode};
use crate::paths::decrypt_path;
use aes_siv::{siv::Aes256Siv, Key, Nonce};
use hopnet_common::height::height_to_db;
use hopnet_common::{CustomUUID, FileItem};
use hopnet_projection::DatabaseError;
use r2d2_sqlite::SqliteConnectionManager;

use rusqlite::{params, OptionalExtension, Transaction};
use std::str::FromStr;

/// Helper function to log ancestor folder modifications (extracted from log_modification)
fn log_ancestor_modifications(
    tx: &Transaction,
    path: &str,
    owner_id: i32,
    modification_height: u64,
) -> Result<(), DatabaseError> {
    let ancestors = get_all_ancestor_folders(tx, path, owner_id)?;
    for ancestor_id in &ancestors {
        tx.execute(
            "INSERT OR IGNORE INTO modification_log (inode_id, owner_id, old_parent_id, modified_at_height) VALUES (?, ?, ?, ?)",
            params![ancestor_id, owner_id, None::<CustomUUID>, height_to_db(modification_height)]
        ).map_err(|e| {
            tracing::error!("Failed to log ancestor modification for path {} ancestor {}: {:?}",
                           path, ancestor_id, e);
            DatabaseError::ProcessingError
        })?;
    }
    tracing::debug!(
        "Logged {} ancestor modifications for path: {}",
        ancestors.len(),
        path
    );
    Ok(())
}

pub fn get_files(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    path: String,
    owner_id: i32,
    key: &Key<Aes256Siv>,
    nonce: &Nonce,
) -> Result<Vec<FileItem>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // Updated query to include data_blocks join for file_size and timestamps
            let query = r#"
                SELECT
                    i.id,
                    i.path,
                    i.type,
                    i.data_id,
                    db.file_size,
                    uuid_extract_timestamp(i.id) as creation_date,
                    CASE
                        WHEN i.data_id IS NOT NULL THEN uuid_extract_timestamp(i.data_id)
                        ELSE NULL
                    END as modification_date,
                    COALESCE(
                        (SELECT COUNT(*) FROM shares s WHERE s.data_block_id = i.data_id AND s.user_id != ?)
                        +
                        (SELECT COUNT(*) FROM incoming_shares ist WHERE ist.data_block_id = i.data_id),
                        0
                    ) as shared_with_count
                FROM inodes i
                LEFT JOIN data_blocks db ON i.data_id = db.id
                WHERE i.path LIKE ? AND i.path NOT LIKE ? AND i.owner_id = ?
            "#;

            let mut stmt = db_lock
                .prepare(query)
                .map_err(|_| DatabaseError::RecallError)?;
            let like_path = format!("{}/%", path);
            let not_like_path = format!("{}/%", like_path);
            tracing::debug!(
                "Querying files with metadata: like_path: {}, not_like_path: {}",
                like_path,
                not_like_path
            );

            let files = stmt
                .query_map(
                    params![owner_id, like_path, not_like_path, owner_id],
                    |row| {
                        let id: CustomUUID = row.get(0)?;
                        let encrypted_path: String = row.get(1)?;
                        let decrypted_path = decrypt_path(encrypted_path, key, nonce)?;
                        let inode_type: hopnet_common::InodeType = row.get(2)?;
                        let _data_id: Option<CustomUUID> = row.get(3)?; // Not used in FileItem
                        let file_size = row.get::<_, Option<i64>>(4)?.map(|v| v as u64);
                        let creation_date: CustomDateTime = row.get(5)?;
                        let modification_date: Option<CustomDateTime> = row.get(6)?;
                        let shared_with_count: i64 = row.get(7)?;

                        // Convert our internal CustomUUID to common module's CustomUUID
                        let common_uuid = hopnet_common::CustomUUID::from_str(&id.to_string())
                            .map_err(|_| rusqlite::Error::InvalidQuery)?;

                        Ok(FileItem {
                            id: common_uuid,
                            path: decrypted_path,
                            inode_type,
                            file_size,
                            creation_date: *creation_date, // Dereference CustomDateTime to get DateTime<Utc>
                            modification_date: modification_date.map(|dt| *dt), // Dereference if present
                            shared_with_count: Some(shared_with_count as u32),
                        })
                    },
                )
                .map_err(|_| DatabaseError::ProcessingError)?;

            Ok(files
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?)
        }
        Err(e) => {
            tracing::error!("Database connection error in get_files: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn get_recent_files(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    owner_id: i32,
    limit: i32,
    key: &Key<Aes256Siv>,
    nonce: &Nonce,
) -> Result<Vec<FileItem>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let query = r#"
                SELECT
                    i.id,
                    i.path,
                    i.type,
                    i.data_id,
                    db.file_size,
                    uuid_extract_timestamp(i.id) as creation_date,
                    CASE
                        WHEN i.data_id IS NOT NULL THEN uuid_extract_timestamp(i.data_id)
                        ELSE NULL
                    END as modification_date,
                    COALESCE(
                        (SELECT COUNT(*) FROM shares s WHERE s.data_block_id = i.data_id AND s.user_id != ?)
                        +
                        (SELECT COUNT(*) FROM incoming_shares ist WHERE ist.data_block_id = i.data_id),
                        0
                    ) as shared_with_count
                FROM (
                    SELECT inode_id, MAX(modified_at_height) AS max_height
                    FROM modification_log
                    WHERE owner_id = ?
                    GROUP BY inode_id
                ) ml
                JOIN inodes i ON i.id = ml.inode_id AND i.owner_id = ? AND i.type = 0
                LEFT JOIN data_blocks db ON i.data_id = db.id
                ORDER BY ml.max_height DESC
                LIMIT ?
            "#;

            let mut stmt = db_lock
                .prepare(query)
                .map_err(|_| DatabaseError::RecallError)?;

            let files = stmt
                .query_map(params![owner_id, owner_id, owner_id, limit], |row| {
                    let id: CustomUUID = row.get(0)?;
                    let encrypted_path: String = row.get(1)?;
                    let decrypted_path = decrypt_path(encrypted_path, key, nonce)?;
                    let inode_type: hopnet_common::InodeType = row.get(2)?;
                    let _data_id: Option<CustomUUID> = row.get(3)?;
                    let file_size = row.get::<_, Option<i64>>(4)?.map(|v| v as u64);
                    let creation_date: CustomDateTime = row.get(5)?;
                    let modification_date: Option<CustomDateTime> = row.get(6)?;
                    let shared_with_count: i64 = row.get(7)?;

                    let common_uuid = hopnet_common::CustomUUID::from_str(&id.to_string())
                        .map_err(|_| rusqlite::Error::InvalidQuery)?;

                    Ok(FileItem {
                        id: common_uuid,
                        path: decrypted_path,
                        inode_type,
                        file_size,
                        creation_date: *creation_date,
                        modification_date: modification_date.map(|dt| *dt),
                        shared_with_count: Some(shared_with_count as u32),
                    })
                })
                .map_err(|_| DatabaseError::ProcessingError)?;

            Ok(files
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DatabaseError::ProcessingError)?)
        }
        Err(e) => {
            tracing::error!("Database connection error in get_recent_files: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn insert_files(
    db_tx: &rusqlite::Transaction,
    blob_ops: &[hopnet_storage::store::BlobInsertOp],
    inodes: Vec<Inode>,
    fragments_dir: &str,
    block_height: u64,
) -> Result<(), DatabaseError> {
    // Substrate half first: blobs must exist before inodes reference them.
    for op in blob_ops {
        hopnet_storage::store::apply_blob_insert(
            db_tx,
            op,
            &hopnet_storage::store::ApplyCtx { fragments_dir },
        )
        .map_err(|e| {
            tracing::error!("apply_blob_insert failed: id={} error={e}", op.blob_id);
            DatabaseError::InsertError
        })?;
    }

    // Parent folder inodes are now pre-generated by the submitting node and included
    // in the Vec<Inode> payload, so every node inserts identical folder UUIDs.

    let inode_count = inodes.len();

    // Modification heights stamp the DECIDING block (ctx.height), never
    // last_decided_height — the meta row lags the block being applied.
    let current_height = block_height;

    for inode in inodes {
        let data_id: Option<CustomUUID> = inode.data_id.clone();

        // Get the owner_id from the inode
        let owner_id = inode.owner.id();

        // Use inode ID from consensus payload for distributed consistency
        // This ensures all nodes have the same ID for the same file
        let inode_id = inode.id;

        // Insert into inodes table
        // Folders use INSERT OR IGNORE — concurrent transactions may pre-generate
        // the same parent folder, and the first one wins.
        let is_folder = matches!(inode.inode_type, hopnet_common::InodeType::Folder);
        if is_folder {
            let rows = db_tx.execute(
                "INSERT OR IGNORE INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, ?, ?)",
                params![inode_id, owner_id, inode.path, inode.inode_type, data_id]
            ).map_err(|e| {
                tracing::error!("Failed to insert folder inode: id={} owner_id={} path={:?} error={:?}", inode_id, owner_id, inode.path, e);
                DatabaseError::InsertError
            })?;
            if rows > 0 {
                log_modification(
                    db_tx,
                    inode_id,
                    owner_id,
                    None,
                    None,
                    Some(&inode.path),
                    current_height,
                )?;
            }
        } else {
            db_tx
                .execute(
                    "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, ?, ?)",
                    params![inode_id, owner_id, inode.path, inode.inode_type, data_id],
                )
                .map_err(|e| {
                    tracing::error!(
                        "Failed to insert inode: id={} owner_id={} path={:?} type={:?} error={:?}",
                        inode_id,
                        owner_id,
                        inode.path,
                        inode.inode_type,
                        e
                    );
                    DatabaseError::InsertError
                })?;
            log_modification(
                db_tx,
                inode_id,
                owner_id,
                None,
                None,
                Some(&inode.path),
                current_height,
            )?;
        }
    }

    tracing::debug!("Inserted {} files using shared transaction", inode_count);
    Ok(())
}

// Helper function to find missing parent directories
pub fn find_missing_parents(
    tx: &Transaction,
    new_paths: &[&str],
) -> Result<Vec<String>, DatabaseError> {
    if new_paths.is_empty() {
        return Ok(Vec::new());
    }

    // Decompose paths into all ancestor paths in Rust
    // (DuckDB array functions like string_split/unnest/LATERAL have no SQLite equivalent)
    let mut all_ancestors: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for path in new_paths {
        let trimmed = path.trim_start_matches('/');
        let parts: Vec<&str> = trimmed.split('/').collect();
        // Generate all ancestor paths (exclude the full path itself)
        for i in 1..parts.len() {
            let ancestor = format!("/{}", parts[..i].join("/"));
            all_ancestors.insert(ancestor);
        }
    }

    if all_ancestors.is_empty() {
        return Ok(Vec::new());
    }

    // Create temp table with ancestor paths
    tx.execute("CREATE TEMP TABLE temp_ancestor_paths (path TEXT)", [])
        .map_err(|_| DatabaseError::InsertError)?;

    for ancestor in &all_ancestors {
        tx.execute(
            "INSERT INTO temp_ancestor_paths VALUES (?)",
            params![ancestor],
        )
        .map_err(|_| DatabaseError::InsertError)?;
    }

    // Find ancestors that don't exist as inodes
    let mut stmt = tx
        .prepare(
            "SELECT DISTINCT tap.path
         FROM temp_ancestor_paths tap
         LEFT JOIN inodes i ON tap.path = i.path
         WHERE i.path IS NULL
         ORDER BY tap.path",
        )
        .map_err(|_| DatabaseError::ProcessingError)?;

    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|_| DatabaseError::ProcessingError)?;

    let mut missing_parents = Vec::new();
    for row in rows {
        missing_parents.push(row.map_err(|_| DatabaseError::ProcessingError)?);
    }

    // Clean up temp table
    tx.execute("DROP TABLE temp_ancestor_paths", [])
        .map_err(|_| DatabaseError::ProcessingError)?;

    Ok(missing_parents)
}

pub fn delete_files(
    db_tx: &rusqlite::Transaction,
    path: String,
    user_id: i32,
    block_height: u64,
) -> Result<(), DatabaseError> {
    let current_height = block_height;

    // Log ancestor folder modifications (reusing existing logic)
    log_ancestor_modifications(db_tx, &path, user_id, current_height)?;

    // Verify items exist before attempting deletion (separate from INSERT OR IGNORE
    // which may report 0 rows if modification_log already has entries at this height)
    let item_count: i32 = db_tx
        .query_row(
            "SELECT COUNT(*) FROM inodes WHERE (path = ? OR path LIKE ?) AND owner_id = ?",
            params![path, format!("{}/%", path), user_id],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;

    if item_count == 0 {
        return Err(DatabaseError::NotFound);
    }

    // Log deletion for ALL items that will be deleted (target + children) in a single SQL operation
    db_tx.execute(
        r#"
        INSERT OR IGNORE INTO modification_log (inode_id, owner_id, old_parent_id, modified_at_height)
        SELECT
            i.id,
            i.owner_id,
            p.id as old_parent_id,
            ? as modified_at_height
        FROM inodes i
        LEFT JOIN inodes p ON (
            p.owner_id = i.owner_id
            AND p.type = 1
            AND p.path = substr(i.path, 1, length(i.path) - length(reverse(substr(reverse(i.path), 1, INSTR(reverse(i.path), '/') - 1))) - 1)
            AND length(i.path) - length(replace(i.path, '/', '')) > 1
        )
        WHERE (i.path = ? OR i.path LIKE ?) AND i.owner_id = ?
        "#,
        params![height_to_db(current_height), path, format!("{}/%", path), user_id]
    ).map_err(|e| {
        tracing::error!("Failed to log modifications for deletion: {:?}", e);
        DatabaseError::ProcessingError
    })?;

    tracing::debug!(
        "Logged deletion of {} items at height {}",
        item_count,
        current_height
    );

    // Phase 2b: Clean up share memberships and pending outgoing shares for deleted files
    {
        let mut data_ids_stmt = db_tx.prepare(
            "SELECT DISTINCT data_id FROM inodes WHERE (path = ? OR path LIKE ?) AND owner_id = ? AND data_id IS NOT NULL"
        ).map_err(|_| DatabaseError::RecallError)?;
        let data_ids: Vec<CustomUUID> = data_ids_stmt
            .query_map(params![path, format!("{}/%", path), user_id], |row| {
                row.get::<_, CustomUUID>(0)
            })
            .map_err(|_| DatabaseError::ProcessingError)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| DatabaseError::ProcessingError)?;

        for data_block_id in &data_ids {
            crate::db::shares::remove_user_from_shares(db_tx, data_block_id, user_id)?;
            crate::db::shares::remove_sender_incoming_shares(db_tx, data_block_id, user_id)?;
        }
    }

    // Delete the file/folder and all its children (only for this user)
    db_tx
        .execute(
            "DELETE FROM inodes WHERE (path = ? OR path LIKE ?) AND owner_id = ?",
            params![path, format!("{}/%", path), user_id],
        )
        .map_err(|_| DatabaseError::ProcessingError)?;

    tracing::debug!(
        "Deleted files at path {} for user {} using shared transaction",
        path,
        user_id
    );
    Ok(())
}

// One argument per orthogonal aspect of a modify (rename, content, shares,
// height stamp); a params struct would just relocate the same names.
#[allow(clippy::too_many_arguments)]
pub fn modify_item(
    db_tx: &rusqlite::Transaction,
    user_id: i32,
    inode_id: CustomUUID,
    new_encrypted_path: Option<String>,
    // None = no content change; Some(None) = content cleared (data_id NULL);
    // Some(Some(op)) = new blob registered via the substrate apply.
    content_update: Option<Option<hopnet_storage::store::BlobInsertOp>>,
    incoming_share_updates: Option<Vec<crate::envelopes::IncomingShareUpdate>>,
    fragments_dir: &str,
    block_height: u64,
) -> Result<(), DatabaseError> {
    // Check if the item exists and get its type and current path using inode_id
    tracing::debug!(
        "modify_item: Querying inodes table for inode_id={} user_id={}",
        inode_id,
        user_id
    );
    let item_info: Option<(hopnet_common::InodeType, String)> = db_tx
        .query_row(
            "SELECT type, path FROM inodes WHERE id = ? AND owner_id = ?",
            params![inode_id, user_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    let (item_type, current_encrypted_path) = match item_info {
        Some((itype, path)) => (itype, path),
        None => {
            tracing::warn!(
                "modify_item: Item not found - inode_id: {}, user_id: {}",
                inode_id,
                user_id
            );
            return Err(DatabaseError::NotFound);
        }
    };

    // Capture old parent BEFORE any modifications
    let old_parent_id =
        get_parent_id(db_tx, &current_encrypted_path, user_id).unwrap_or_else(|e| {
            tracing::warn!(
                "Failed to get parent for path {}: {:?}",
                current_encrypted_path,
                e
            );
            None
        });

    if let Some(ref new_path) = new_encrypted_path {
        // Circular reference prevention for folders
        if item_type == hopnet_common::InodeType::Folder {
            // Check if new path would place folder inside itself or its descendants
            // This happens when the new path starts with the current folder's path
            if new_path.starts_with(&format!("{}/", current_encrypted_path))
                || new_path == &current_encrypted_path
            {
                tracing::warn!(
                    "Circular reference prevented: Cannot move folder '{}' into itself at '{}'",
                    current_encrypted_path,
                    new_path
                );
                return Err(DatabaseError::InvalidPayload); // Invalid operation - circular reference
            }
        }

        // Check if the new path already exists (exclude current inode to allow "move to same location")
        let new_exists: bool = db_tx
            .query_row(
                "SELECT COUNT(*) > 0 FROM inodes WHERE path = ? AND owner_id = ? AND id != ?",
                params![new_path, user_id, inode_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::RecallError)?;

        if new_exists {
            return Err(DatabaseError::ConflictError); // Path already occupied
        }

        let rows_updated = match item_type {
            hopnet_common::InodeType::File => {
                // For files: simple path update
                db_tx
                    .execute(
                        "UPDATE inodes SET path = ? WHERE path = ? AND owner_id = ?",
                        params![new_path, current_encrypted_path, user_id],
                    )
                    .map_err(|_| DatabaseError::ProcessingError)?
            }
            hopnet_common::InodeType::Folder => {
                // For folders: update the folder and all descendants
                // Use SQL string concatenation to update all child paths
                db_tx
                    .execute(
                        "UPDATE inodes
                     SET path = ? || substr(path, length(?) + 1)
                     WHERE owner_id = ?
                       AND (path = ? OR path LIKE ?)",
                        params![
                            new_path,
                            current_encrypted_path,
                            user_id,
                            current_encrypted_path,
                            format!("{}/%", current_encrypted_path)
                        ],
                    )
                    .map_err(|_| DatabaseError::ProcessingError)?
            }
        };

        tracing::debug!(
            "modify_item: Path change validation successful - {} row(s) would be updated",
            rows_updated
        );
    }

    // Phase 4b: Handle content updates if provided (works with or without
    // path changes). blob_op None ⇒ content cleared ⇒ data_id NULL.
    if let Some(update) = content_update {
        let new_data_id: Option<CustomUUID> = update.as_ref().map(|op| op.blob_id.clone());
        tracing::debug!(
            "modify_item: Updating content for inode_id={} to new_data_id={:?}",
            inode_id,
            new_data_id
        );

        // Read old data_id BEFORE updating (needed for share propagation)
        let old_data_id: Option<CustomUUID> = db_tx
            .query_row(
                "SELECT data_id FROM inodes WHERE id = ? AND owner_id = ?",
                params![inode_id, user_id],
                |row| row.get::<_, Option<CustomUUID>>(0),
            )
            .optional()
            .map_err(|_| DatabaseError::RecallError)?
            .unwrap_or(None);

        // Substrate half: register the new blob (skipped for cleared content)
        if let Some(ref op) = update {
            hopnet_storage::store::apply_blob_insert(
                db_tx,
                op,
                &hopnet_storage::store::ApplyCtx { fragments_dir },
            )
            .map_err(|e| {
                tracing::error!(
                    "modify_item: apply_blob_insert failed: id={} error={e}",
                    op.blob_id
                );
                DatabaseError::InsertError
            })?;
        }

        // Update inode to point to new data block (this changes the modification time via UUIDv7)
        tracing::debug!(
            "modify_item: Updating inode id={} to point to new data_id={:?} for user_id={}",
            inode_id,
            new_data_id,
            user_id
        );
        let rows_updated = db_tx
            .execute(
                "UPDATE inodes SET data_id = ? WHERE id = ? AND owner_id = ?",
                params![new_data_id, inode_id, user_id],
            )
            .map_err(|e| {
                tracing::error!(
                    "modify_item: Failed to update inode id={} data_id={:?} user_id={}: {:?}",
                    inode_id,
                    new_data_id,
                    user_id,
                    e
                );
                DatabaseError::ProcessingError
            })?;

        tracing::debug!(
            "modify_item: Updated {} inode rows to new data_id={:?}",
            rows_updated,
            new_data_id
        );

        tracing::info!(
            "Updated content for inode_id={} to data_id={:?}",
            inode_id,
            new_data_id
        );

        // Phase 2b: Share propagation — update other sharers' inodes and share tracking
        if let Some(old_data) = old_data_id {
            let sharers = crate::db::shares::get_sharers_for_data_block(db_tx, &old_data)?;
            if !sharers.is_empty() {
                tracing::debug!(
                    "modify_item: Propagating content update to {} sharers",
                    sharers.len()
                );

                // Update only current sharers' inodes to point to new data block
                // Must be scoped to shares table members to avoid updating unshared users
                let propagated = db_tx.execute(
                    "UPDATE inodes SET data_id = ? WHERE data_id = ? AND owner_id != ? AND owner_id IN (SELECT user_id FROM shares WHERE data_block_id = ?)",
                    params![new_data_id, old_data, user_id, old_data]
                ).map_err(|e| {
                    tracing::error!("modify_item: Failed to propagate data_id to other sharers: {:?}", e);
                    DatabaseError::ProcessingError
                })?;
                tracing::debug!(
                    "modify_item: Propagated content update to {} other sharers' inodes",
                    propagated
                );

                // Share bookkeeping that needs a real blob id — skipped for
                // cleared content (shares of an emptied file keep the NULL
                // inode reference set above; there is no key to re-wrap).
                if let Some(ref nid) = new_data_id {
                    // Log modification for each affected sharer's inode
                    let current_height = block_height;
                    let mut stmt = db_tx
                        .prepare(
                            "SELECT id, owner_id, path FROM inodes WHERE data_id = ? AND owner_id != ?",
                        )
                        .map_err(|_| DatabaseError::RecallError)?;
                    let affected: Vec<(CustomUUID, i32, String)> = stmt
                        .query_map(params![nid, user_id], |row| {
                            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                        })
                        .map_err(|_| DatabaseError::ProcessingError)?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| DatabaseError::ProcessingError)?;
                    for (affected_inode_id, affected_owner_id, affected_path) in &affected {
                        log_modification(
                            db_tx,
                            affected_inode_id.clone(),
                            *affected_owner_id,
                            None,
                            None,
                            Some(affected_path.as_str()),
                            current_height,
                        )?;
                    }

                    // Update shares table: all rows from old → new data_block_id
                    crate::db::shares::update_shares_data_block(db_tx, &old_data, nid)?;

                    // Update pending incoming_shares with pre-computed file_access blobs
                    if let Some(ref updates) = incoming_share_updates {
                        for update in updates {
                            crate::db::shares::update_incoming_share_data_block(
                                db_tx,
                                &update.incoming_share_id,
                                nid,
                                &update.new_file_access_blob,
                            )?;
                        }
                    }
                }
            }
        }
    }

    // Phase 4a: Metadata-only changes don't update modification time
    // The modification time comes from data_block.id UUIDv7 timestamp, which only changes with content

    // Log modification for FileProvider change tracking
    let current_height = block_height;
    // For moves: pass old path and new path. For content updates: only new path (current location).
    let old_path_ref = if new_encrypted_path.is_some() {
        Some(current_encrypted_path.as_str())
    } else {
        None
    };
    let new_path_ref = new_encrypted_path
        .as_deref()
        .or(Some(current_encrypted_path.as_str()));
    log_modification(
        db_tx,
        inode_id.clone(),
        user_id,
        old_parent_id,
        old_path_ref,
        new_path_ref,
        current_height,
    )?;

    tracing::debug!(
        "Modified item inode_id={} for user {} using shared transaction",
        inode_id,
        user_id
    );
    Ok(())
}

pub fn get_file_fragments(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    encrypted_path: String,
    user_id: i32,
) -> Result<FileAccessData, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // First check if file exists and whether it's empty (data_id is NULL)
            let (file_exists, is_empty) = db_lock
                .prepare(
                    "SELECT COUNT(*) > 0, COALESCE(MAX(data_id IS NULL), 0)
                 FROM inodes
                 WHERE path = ? AND type = 0",
                )
                .and_then(|mut stmt| {
                    stmt.query_row(params![encrypted_path.clone()], |row| {
                        Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?))
                    })
                })
                .map_err(|_| DatabaseError::RecallError)?;

            if !file_exists {
                return Err(DatabaseError::RecallError); // File doesn't exist
            }

            if is_empty {
                // Empty file: no fragments, no encryption
                tracing::debug!(
                    "get_file_fragments: empty file detected for path {}",
                    encrypted_path
                );
                return Ok(FileAccessData {
                    manifest: None,          // No fragments for empty files
                    file_access_entry: None, // No encryption for empty files
                    file_size: 0,
                });
            }

            // Projection half: resolve the inode's blob reference. The
            // substrate half (fragment layout + wrap row) is crate-owned.
            let data_block_id: CustomUUID = db_lock
                .query_row(
                    "SELECT data_id FROM inodes WHERE path = ? AND type = 0",
                    params![encrypted_path],
                    |row| row.get(0),
                )
                .map_err(|_| DatabaseError::RecallError)?;

            let manifest = hopnet_storage::store::blob_manifest(&db_lock, &data_block_id)
                .map_err(|e| {
                    tracing::error!("blob_manifest failed: {e}");
                    DatabaseError::ProcessingError
                })?
                .ok_or(DatabaseError::RecallError)?; // dangling blob reference

            // The user's blob_access wrap (resolved via their pubkey);
            // None = user might not have access.
            let file_access_entry =
                get_file_access(&db_lock, &data_block_id, user_id).unwrap_or(None);

            let file_size = manifest.file_size;
            Ok(FileAccessData {
                manifest: Some(manifest),
                file_access_entry,
                file_size,
            })
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Extract all ancestor folder IDs from a path for modification tracking
/// Returns list of ancestor folder IDs from immediate parent up to root
/// Example: "/a/b/c/file.txt" returns IDs for ["/a/b/c", "/a/b", "/a"] if they exist
fn get_all_ancestor_folders(
    tx: &Transaction,
    path: &str,
    owner_id: i32,
) -> Result<Vec<CustomUUID>, DatabaseError> {
    let mut stmt = tx
        .prepare(
            "SELECT id FROM inodes 
         WHERE owner_id = ? AND type = 1 AND ? LIKE path || '/%'
         ORDER BY LENGTH(path) DESC",
        )
        .map_err(|_| DatabaseError::ProcessingError)?;

    let rows = stmt
        .query_map(params![owner_id, path], |row| row.get::<_, CustomUUID>(0))
        .map_err(|_| DatabaseError::ProcessingError)?;

    let ancestors: Result<Vec<_>, _> = rows.collect();
    ancestors.map_err(|_| DatabaseError::ProcessingError)
}

/// Extract parent folder's inode_id from a path
/// Returns None for root level items or if parent folder doesn't exist
fn get_parent_id(
    tx: &Transaction,
    path: &str,
    owner_id: i32,
) -> Result<Option<CustomUUID>, DatabaseError> {
    let parent_path = match path.rfind('/') {
        Some(idx) if idx > 1 => &path[..idx], // Has parent (not root level)
        _ => return Ok(None),                 // Root level item, no parent
    };

    match tx.query_row(
        "SELECT id FROM inodes WHERE path = ? AND owner_id = ? AND type = 1",
        params![parent_path, owner_id],
        |row| row.get::<_, CustomUUID>(0),
    ) {
        Ok(parent_id) => Ok(Some(parent_id)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None), // Parent folder doesn't exist
        Err(e) => {
            tracing::error!("Error looking up parent folder for path {}: {:?}", path, e);
            Err(DatabaseError::ProcessingError)
        }
    }
}

/// Log a modification to the modification_log for FileProvider change tracking
/// Automatically logs all ancestor folders to ensure proper modification time cascading
pub fn log_modification(
    tx: &Transaction,
    inode_id: CustomUUID,
    owner_id: i32,
    old_parent_id: Option<CustomUUID>, // Parent BEFORE modification (None for new items)
    old_path: Option<&str>,            // Path BEFORE modification (for moves/deletes)
    new_path: Option<&str>,            // Path AFTER modification (for inserts/moves)
    modification_height: u64,
) -> Result<(), DatabaseError> {
    // Log the primary item modification
    tx.execute(
        "INSERT OR IGNORE INTO modification_log (inode_id, owner_id, old_parent_id, modified_at_height) VALUES (?, ?, ?, ?)",
        params![inode_id, owner_id, old_parent_id, height_to_db(modification_height)]
    ).map_err(|e| {
        tracing::error!("Failed to log modification for inode_id {} at height {}: {:?}",
                       inode_id, modification_height, e);
        DatabaseError::ProcessingError
    })?;

    // Log ancestor folders for old path (for deletes/moves)
    if let Some(old) = old_path {
        log_ancestor_modifications(tx, old, owner_id, modification_height)?;
    }

    // Log ancestor folders for new path (for inserts/moves)
    if let Some(new) = new_path {
        log_ancestor_modifications(tx, new, owner_id, modification_height)?;
    }

    tracing::debug!(
        "Logged modification for inode_id {} with old_parent_id {:?} at height {}",
        inode_id,
        old_parent_id,
        modification_height
    );
    Ok(())
}

/// Look up an inode by its UUID and owner. Returns (data_id, encrypted_path, inode_type).
pub fn get_inode_by_id(
    conn: &rusqlite::Connection,
    inode_id: &CustomUUID,
    owner_id: i32,
) -> Result<Option<(Option<CustomUUID>, String, hopnet_common::InodeType)>, DatabaseError> {
    conn.query_row(
        "SELECT data_id, path, type FROM inodes WHERE id = ? AND owner_id = ?",
        params![inode_id, owner_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
    .map_err(|_| DatabaseError::RecallError)
}

/// Look up a user's blob_access wrap for a specific blob (via their pubkey).
pub fn get_file_access(
    conn: &rusqlite::Connection,
    data_block_id: &CustomUUID,
    user_id: i32,
) -> Result<Option<hopnet_storage::BlobAccess>, DatabaseError> {
    // Projection half: user → pubkey; substrate half: pubkey-keyed wrap.
    // The users table is HOST-owned; drive SQL may READ it (same SQLite DB —
    // the ownership boundary is code, not schema), so this is a local lookup
    // rather than a call into the host's users module.
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

/// Total bytes of this user's drive content (file inodes joined to their
/// blobs). RFC-017 Stage 6: moved from host takeout-quota SQL — the drive
/// owns its sizing, the host sums across projections via the
/// `Projection::user_data_size_bytes` hook.
pub fn user_data_size(conn: &rusqlite::Connection, user_id: i32) -> Result<u64, DatabaseError> {
    let total_size: Option<i64> = conn
        .query_row(
            "SELECT COALESCE(SUM(db.file_size), 0) FROM inodes i
                 INNER JOIN data_blocks db ON i.data_id = db.id
                 WHERE i.owner_id = ? AND i.type = 0",
            rusqlite::params![user_id],
            |row| row.get(0),
        )
        .map_err(|_| DatabaseError::RecallError)?;

    Ok(total_size.unwrap_or(0) as u64)
}
