use super::*;
use aes_siv::{Key, Nonce, siv::Aes256Siv};
use either::Either;
use hopnet_common::FileItem;

use crate::files::functions::decrypt_path;

use rusqlite::{OptionalExtension, Transaction};
use std::str::FromStr;

/// Helper function to log ancestor folder modifications (extracted from log_modification)
fn log_ancestor_modifications(
    tx: &Transaction,
    path: &str,
    owner_id: i32,
    modification_height: i32,
) -> Result<(), DatabaseError> {
    let ancestors = get_all_ancestor_folders(tx, path, owner_id)?;
    for ancestor_id in &ancestors {
        tx.execute(
            "INSERT OR IGNORE INTO modification_log (inode_id, owner_id, old_parent_id, modified_at_height) VALUES (?, ?, ?, ?)",
            params![ancestor_id, owner_id, None::<CustomUUID>, modification_height]
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

    // Get consensus height once for modification logging
    let current_height = crate::db::consensus::get_current_consensus_height(db_tx)?;

    for inode in inodes {
        let data_id: Option<CustomUUID> = inode.data_id.clone();

        // Get the owner_id from the inode
        let owner_id = match inode.owner {
            either::Either::Left(user_id) => user_id,
            either::Either::Right(user) => user.user_id,
        };

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
pub(crate) fn find_missing_parents(
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
) -> Result<(), DatabaseError> {
    // Get current consensus height for modification tracking
    let current_height = crate::db::consensus::get_current_consensus_height(db_tx)?;

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
        params![current_height, path, format!("{}/%", path), user_id]
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

pub fn modify_item(
    db_tx: &rusqlite::Transaction,
    user_id: i32,
    inode_id: crate::db::CustomUUID,
    new_encrypted_path: Option<String>,
    // None = no content change; Some(None) = content cleared (data_id NULL);
    // Some(Some(op)) = new blob registered via the substrate apply.
    content_update: Option<Option<hopnet_storage::store::BlobInsertOp>>,
    incoming_share_updates: Option<Vec<crate::shares::types::IncomingShareUpdate>>,
    fragments_dir: &str,
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
                    let current_height =
                        crate::db::consensus::get_current_consensus_height(db_tx)?;
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
    let current_height = crate::db::consensus::get_current_consensus_height(db_tx)?;
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
) -> Result<crate::files::functions::FileAccessData, DatabaseError> {
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
                return Ok(crate::files::functions::FileAccessData {
                    file_reassembly_data: None, // No fragments for empty files
                    file_access_entry: None,    // No encryption for empty files
                    file_size: 0,
                });
            }

            // Non-empty file: query for fragments with reassembly info (now chunk-aware)
            let mut stmt = db_lock.prepare(
                "SELECT db.id, db.file_hash, db.added_bytes, db.placement_height, fh.chunk_number, fh.local_index, fh.fragment_id, fh.fragment_hash, fh.chunk_type, fh.stored_locally, db.file_size
                 FROM inodes i
                 JOIN data_blocks db ON i.data_id = db.id
                 JOIN fragment_hashes fh ON db.id = fh.data_block_id
                 WHERE i.path = ? AND i.type = 0
                 ORDER BY fh.chunk_number, fh.local_index"
            ).map_err(|_| DatabaseError::RecallError)?;

            let rows = stmt
                .query_map(params![encrypted_path], |row| {
                    let data_block_id: CustomUUID = row.get(0)?;
                    let file_hash: Blake3Hash = row.get(1)?;
                    let added_bytes: u8 = row.get(2)?;
                    let placement_height: Option<i32> = row.get(3)?;
                    let chunk_number: u32 = row.get(4)?;
                    let local_index: u32 = row.get(5)?;
                    let fragment_id: CustomUUID = row.get(6)?;
                    let fragment_hash: Blake3Hash = row.get(7)?;
                    let chunk_type: crate::db::ChunkType = row.get(8)?;
                    let stored_locally: bool = row.get(9)?;
                    let file_size: u64 = row.get::<_, i64>(10).unwrap_or(0) as u64;
                    Ok((
                        data_block_id,
                        file_hash,
                        added_bytes,
                        placement_height,
                        chunk_number,
                        local_index,
                        fragment_id,
                        fragment_hash,
                        chunk_type,
                        stored_locally,
                        file_size,
                    ))
                })
                .map_err(|_| DatabaseError::ProcessingError)?;

            let mut data_block_id: Option<CustomUUID> = None;
            let mut file_hash: Option<Blake3Hash> = None;
            let mut added_bytes: Option<u8> = None;
            let mut placement_height: Option<i32> = None;
            let mut db_file_size: u64 = 0;

            // Group fragments by chunk_number: chunk_number -> (original_frags, recovery_frags)
            let mut chunks_map: crate::files::functions::ReassemblyChunks =
                std::collections::HashMap::new();

            for row in rows {
                let (
                    d_block_id,
                    f_hash,
                    a_bytes,
                    p_height,
                    chunk_number,
                    local_index,
                    fragment_id,
                    fragment_hash,
                    chunk_type,
                    stored_locally,
                    f_size,
                ) = row.map_err(|_| DatabaseError::ProcessingError)?;

                if data_block_id.is_none() {
                    data_block_id = Some(d_block_id);
                    file_hash = Some(f_hash);
                    added_bytes = Some(a_bytes);
                    placement_height = p_height;
                    db_file_size = f_size;
                }

                // Get or create entry for this chunk
                let chunk_entry = chunks_map.entry(chunk_number).or_insert_with(|| {
                    (
                        std::collections::HashMap::new(),
                        std::collections::HashMap::new(),
                    )
                });

                match chunk_type {
                    crate::db::ChunkType::Original => {
                        chunk_entry.0.insert(
                            local_index as usize,
                            (fragment_hash, fragment_id, stored_locally),
                        );
                    }
                    crate::db::ChunkType::Recovery => {
                        chunk_entry.1.insert(
                            local_index as usize,
                            (fragment_hash, fragment_id, stored_locally),
                        );
                    }
                }
            }

            match (data_block_id, file_hash, added_bytes) {
                (Some(data_block_id), Some(file_hash), Some(added_bytes)) => {
                    // Get the user's blob_access wrap (resolved via their pubkey)
                    let file_access_entry = db_lock.prepare(
                        "SELECT ba.blob_id, ba.recipient_pubkey, ba.ephemeral_pubkey, ba.wrapped_key
                         FROM blob_access ba JOIN users u ON u.x25519_pubkey = ba.recipient_pubkey
                         WHERE ba.blob_id = ? AND u.user_id = ?"
                    ).and_then(|mut stmt| {
                        stmt.query_row(params![data_block_id, user_id], row_to_blob_access)
                    }).ok(); // Convert error to None - user might not have access

                    let file_reassembly_data = crate::files::functions::FileReassemblyData {
                        chunks: chunks_map,
                        added_bytes,
                        expected_file_hash: file_hash,
                        data_block_id,
                        per_file_key: None, // Will be set after decryption
                        placement_height,
                    };

                    Ok(crate::files::functions::FileAccessData {
                        file_reassembly_data: Some(file_reassembly_data), // Wrap in Some for non-empty files
                        file_access_entry,
                        file_size: db_file_size,
                    })
                }
                _ => Err(DatabaseError::RecallError), // File not found
            }
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

use serde::{Deserialize, Serialize};

/// Payload for placement height updates consensus transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementHeightUpdate {
    pub data_block_id: CustomUUID,
    pub placement_height: i32,
}

/// Update placement_height for data blocks after successful fragment distribution
/// Consensus-safe function with execute flag for validation/rollback support
pub fn update_placement_heights_batch(
    db_tx: &rusqlite::Transaction,
    updates: Vec<PlacementHeightUpdate>,
) -> Result<(), DatabaseError> {
    let updates_len = updates.len();
    let crate_updates: Vec<(hopnet_storage::BlobId, i32)> = updates
        .into_iter()
        .map(|u| (u.data_block_id, u.placement_height))
        .collect();
    hopnet_storage::store::apply_placement_commit(db_tx, &crate_updates).map_err(|e| {
        tracing::error!("apply_placement_commit failed: {e}");
        DatabaseError::ProcessingError
    })?;

    tracing::debug!(
        "Updated placement_height for {} data blocks using shared transaction",
        updates_len
    );
    Ok(())
}

/// Get a specific file if it needs distribution (placement_height = NULL and all fragments stored locally)
pub fn get_distributable_file(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    data_block_id: CustomUUID,
) -> Result<Option<DistributableFileData>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // Combined query: get file_hash + fragments together in one query
            // This ensures we only get file_hash when the file is actually distributable
            // If no rows match, we return Ok(None) to allow polling loop to retry
            let mut stmt = db_lock.prepare(
                "SELECT db.file_hash, fh.local_index, fh.fragment_hash, fh.chunk_type
                 FROM data_blocks db
                 JOIN fragment_hashes fh ON db.id = fh.data_block_id
                 WHERE db.id = ?
                   AND db.placement_height IS NULL
                   AND fh.stored_locally = TRUE
                   AND (SELECT COUNT(*) FROM fragment_hashes WHERE data_block_id = db.id AND stored_locally = TRUE) = db.fragment_count
                 ORDER BY fh.chunk_number, fh.local_index"
            ).map_err(|e| {
                tracing::error!("Failed to prepare distributable file query for data_block {}: {:?}", data_block_id, e);
                DatabaseError::RecallError
            })?;

            let mut file_hash: Option<crate::types::Blake3Hash> = None;
            let mut fragments = Vec::new();

            let rows = stmt
                .query_map([data_block_id.clone()], |row| {
                    let f_hash: crate::types::Blake3Hash = row.get(0)?;
                    let local_index: u32 = row.get(1)?;
                    let fragment_hash: crate::types::Blake3Hash = row.get(2)?;
                    let chunk_type: crate::db::ChunkType = row.get(3)?;

                    Ok((f_hash, local_index, fragment_hash, chunk_type))
                })
                .map_err(|_| DatabaseError::RecallError)?;

            for row_result in rows {
                let (f_hash, local_index, fragment_hash, chunk_type) =
                    row_result.map_err(|_| DatabaseError::ProcessingError)?;

                // Store file_hash from first row (same for all rows)
                if file_hash.is_none() {
                    file_hash = Some(f_hash);
                }

                let fragment_type = match chunk_type {
                    crate::db::ChunkType::Original => {
                        crate::files::placement::FragmentType::Original
                    }
                    crate::db::ChunkType::Recovery => {
                        crate::files::placement::FragmentType::Recovery
                    }
                };

                fragments.push((local_index as usize, fragment_hash, fragment_type));
            }

            if fragments.is_empty() {
                // File doesn't exist, isn't ready, or has already been distributed
                // Return Ok(None) to allow polling loop to retry
                Ok(None)
            } else {
                let file_hash = file_hash.expect("file_hash must be set if fragments exist");
                Ok(Some(DistributableFileData {
                    id: data_block_id,
                    file_hash,
                    fragment_hashes: fragments,
                }))
            }
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Batch-update local storage state for multiple fragments in a single transaction.
/// Acquires the write lock once instead of once per fragment, avoiding sustained
/// lock contention with consensus operations.
pub fn mark_fragments_local_state_batch(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    fragment_hashes: &[crate::types::Blake3Hash],
    stored_locally: bool,
) -> Result<usize, DatabaseError> {
    if fragment_hashes.is_empty() {
        return Ok(0);
    }
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|e| {
                tracing::error!(
                    "Failed to begin transaction for batch fragment update: {:?}",
                    e
                );
                DatabaseError::LockError
            })?;
            let mut total_rows = 0;
            {
                let mut stmt = tx
                    .prepare_cached(
                        "UPDATE fragment_hashes SET stored_locally = ? WHERE fragment_hash = ?",
                    )
                    .map_err(|e| {
                        tracing::error!(
                            "Failed to prepare batch fragment update statement: {:?}",
                            e
                        );
                        DatabaseError::ProcessingError
                    })?;
                for hash in fragment_hashes {
                    let rows = stmt.execute(params![stored_locally, hash]).map_err(|e| {
                        tracing::error!(
                            "Error updating stored_locally for fragment hash {}: {:?}",
                            hash,
                            e
                        );
                        DatabaseError::ProcessingError
                    })?;
                    total_rows += rows;
                }
            }
            crate::db::shared::commit_timed(tx).map_err(|e| {
                tracing::error!("Failed to commit batch fragment update: {:?}", e);
                DatabaseError::InsertError
            })?;
            let state_text = if stored_locally {
                "stored locally"
            } else {
                "not stored locally"
            };
            tracing::debug!(
                "Batch-marked {} fragment records ({} hashes) as {}",
                total_rows,
                fragment_hashes.len(),
                state_text
            );
            Ok(total_rows)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Data about a file ready for distribution
#[derive(Debug, Clone)]
pub struct DistributableFileData {
    pub id: CustomUUID,
    pub file_hash: crate::types::Blake3Hash,
    pub fragment_hashes: Vec<(
        usize,
        crate::types::Blake3Hash,
        crate::files::placement::FragmentType,
    )>,
}

/// Get count of fragments stored locally on this node
pub fn get_local_fragment_count(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<i64, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let count = db_lock
                .query_row(
                    "SELECT COUNT(*) FROM fragment_hashes WHERE stored_locally = TRUE",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|e| {
                    tracing::error!("Error querying local fragment count: {:?}", e);
                    DatabaseError::RecallError
                })?;

            tracing::debug!("Found {} fragments stored locally", count);
            Ok(count)
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
    modification_height: i32,
) -> Result<(), DatabaseError> {
    // Log the primary item modification
    tx.execute(
        "INSERT OR IGNORE INTO modification_log (inode_id, owner_id, old_parent_id, modified_at_height) VALUES (?, ?, ?, ?)",
        params![inode_id, owner_id, old_parent_id, modification_height]
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

/// Map a blob_access row (blob_id, recipient_pubkey, ephemeral_pubkey,
/// wrapped_key) into the substrate's BlobAccess.
pub(crate) fn row_to_blob_access(
    row: &rusqlite::Row<'_>,
) -> Result<crate::db::types::BlobAccess, rusqlite::Error> {
    let recipient: Vec<u8> = row.get(1)?;
    let ephemeral: Vec<u8> = row.get(2)?;
    let to_arr = |v: Vec<u8>, idx: usize| -> Result<[u8; 32], rusqlite::Error> {
        v.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Blob,
                "expected 32-byte X25519 key".into(),
            )
        })
    };
    Ok(crate::db::types::BlobAccess {
        blob_id: row.get(0)?,
        recipient_pubkey: to_arr(recipient, 1)?,
        ephemeral_pubkey: to_arr(ephemeral, 2)?,
        wrapped_key: row.get(3)?,
    })
}

/// Look up a user's blob_access wrap for a specific blob (via their pubkey).
pub fn get_file_access(
    conn: &rusqlite::Connection,
    data_block_id: &CustomUUID,
    user_id: i32,
) -> Result<Option<crate::db::types::BlobAccess>, DatabaseError> {
    conn.query_row(
        "SELECT ba.blob_id, ba.recipient_pubkey, ba.ephemeral_pubkey, ba.wrapped_key
         FROM blob_access ba JOIN users u ON u.x25519_pubkey = ba.recipient_pubkey
         WHERE ba.blob_id = ? AND u.user_id = ?",
        params![data_block_id, user_id],
        row_to_blob_access,
    ).optional().map_err(|_| DatabaseError::RecallError)
}
