use super::*;
use aes_siv::{siv::Aes256Siv, Key, Nonce};
use either::Either;
use hopnet_common::FileItem;

use crate::files::functions::decrypt_path;

use duckdb::{Transaction, OptionalExt};

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
    tracing::debug!("Logged {} ancestor modifications for path: {}", ancestors.len(), path);
    Ok(())
}

pub fn get_files(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    path: String,
    owner_id: i32,
    key: &Key<Aes256Siv>,
    nonce: &Nonce
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

            let mut stmt = db_lock.prepare(query).map_err(|_| DatabaseError::RecallError)?;
            let like_path = format!("{}/%", path);
            let not_like_path = format!("{}/%", like_path);
            tracing::debug!("Querying files with metadata: like_path: {}, not_like_path: {}", like_path, not_like_path);

            let files = stmt.query_map(params![owner_id, like_path, not_like_path, owner_id], |row| {
                let id: CustomUUID = row.get(0)?;
                let encrypted_path: String = row.get(1)?;
                let decrypted_path = decrypt_path(encrypted_path, key, nonce)?;
                let inode_type: hopnet_common::InodeType = row.get(2)?;
                let _data_id: Option<CustomUUID> = row.get(3)?; // Not used in FileItem
                let file_size: Option<u64> = row.get(4)?;
                let creation_date: CustomDateTime = row.get(5)?;
                let modification_date: Option<CustomDateTime> = row.get(6)?;
                let shared_with_count: i64 = row.get(7)?;

                // Convert our internal CustomUUID to common module's CustomUUID
                let common_uuid = hopnet_common::CustomUUID::from_str(&id.to_string())
                    .map_err(|_| duckdb::Error::InvalidQuery)?;

                Ok(FileItem {
                    id: common_uuid,
                    path: decrypted_path,
                    inode_type,
                    file_size,
                    creation_date: *creation_date, // Dereference CustomDateTime to get DateTime<Utc>
                    modification_date: modification_date.map(|dt| *dt), // Dereference if present
                    shared_with_count: Some(shared_with_count as u32),
                })
            }).map_err(|_| DatabaseError::ProcessingError)?;

            Ok(files.collect::<Result<Vec<_>, _>>().map_err(|_| DatabaseError::ProcessingError)?)
        }
        Err(e) => {
            tracing::error!("Database connection error in get_files: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn insert_files(
    db_tx: &duckdb::Transaction,
    inodes: Vec<Inode>,
) -> Result<(), DatabaseError> {
    // STEP 1: Collect all paths that need to be inserted
    let new_paths: Vec<String> = inodes.iter()
        .map(|inode| inode.path.clone())
        .collect();

    // STEP 2: Find all missing parent directories in one query
    let missing_parents = find_missing_parents(db_tx, &new_paths)?;

    // STEP 3: Insert missing parent directories first
    if !missing_parents.is_empty() {
        let owner_id_for_parents = match &inodes[0].owner {
            either::Either::Left(user_id) => *user_id,
            either::Either::Right(user) => user.user_id,
        };
        insert_parent_directories(db_tx, &missing_parents, owner_id_for_parents)?;
    }

    let inode_count = inodes.len();

    // Get consensus height once for modification logging
    let current_height = crate::db::consensus::get_current_consensus_height(db_tx)?;

    for inode in inodes {
        // Handle the data_id which can be Either<CustomUUID, DataRecord>
        let data_id: Option<CustomUUID> = match inode.data_id {
            Some(either::Either::Left(uuid)) => {
                // If it's already a UUID, use it directly
                Ok(Some(uuid))
            },
            Some(either::Either::Right(data_record)) => {
                // If it's a DataRecord, we need to insert it first
                let data_id = data_record.id;

                // Access fragment hashes directly (now Vec<FragmentHash>)
                let data = &data_record.data;

                // Insert into data_blocks table
                db_tx.execute(
                    "INSERT INTO data_blocks (id, modified_at, file_hash, fragment_count, added_bytes, placement_height, file_size) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params![
                        data_id,
                        data_record.modified_at,
                        data.hash,
                        data.fragments.len() as i32,
                        data.added_bytes,
                        None::<i32>,  // placement_height is NULL initially, set during fragment placement
                        data_record.file_size
                    ]
                ).map_err(|e| {
                    tracing::error!("Failed to insert data_block: id={} hash={:?} error={:?}", data_id, data.hash, e);
                    DatabaseError::InsertError
                })?;

                // Insert fragment hashes into fragment_hashes table
                for fragment in &data.fragments {
                    db_tx.execute(
                        "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        params![fragment.data_block_id, fragment.chunk_number, fragment.local_index, fragment.fragment_id, fragment.fragment_hash, fragment.chunk_type, fragment.stored_locally]
                    ).map_err(|e| {
                        tracing::error!("Failed to insert fragment_hash: data_block_id={} fragment_id={} error={:?}", fragment.data_block_id, fragment.fragment_id, e);
                        DatabaseError::InsertError
                    })?;
                }

                // Insert file access entries if present
                if let Some(ref file_access_entries) = data_record.file_access_entries {
                    for access_entry in file_access_entries {
                        db_tx.execute(
                            "INSERT INTO file_access (data_block_id, user_id, ephemeral_pubkey, encrypted_file_key) VALUES (?, ?, ?, ?)",
                            params![access_entry.data_block_id, access_entry.user_id, access_entry.ephemeral_pubkey, access_entry.encrypted_file_key]
                        ).map_err(|e| {
                            tracing::error!("Failed to insert file_access: data_block_id={} user_id={} error={:?}", access_entry.data_block_id, access_entry.user_id, e);
                            DatabaseError::InsertError
                        })?;
                    }
                }

                Ok(Some(data_id))
            },
            None => Ok(None)
        }?;

        // Get the owner_id from the inode
        let owner_id = match inode.owner {
            either::Either::Left(user_id) => user_id,
            either::Either::Right(user) => user.user_id,
        };

        // Use inode ID from consensus payload for distributed consistency
        // This ensures all nodes have the same ID for the same file
        let inode_id = inode.id;

        // Insert into inodes table
        db_tx.execute(
            "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, ?, ?)",
            params![
                inode_id,
                owner_id,
                inode.path,
                inode.inode_type,
                data_id
            ]
        ).map_err(|e| {
            tracing::error!("Failed to insert inode: id={} owner_id={} path={:?} type={:?} error={:?}", inode_id, owner_id, inode.path, inode.inode_type, e);
            DatabaseError::InsertError
        })?;

        // Log modification for FileProvider change tracking
        // New items have None as old_parent_id (didn't exist before)
        log_modification(db_tx, inode_id, owner_id, None, None, Some(&inode.path), current_height)?;
    }

    tracing::debug!("Inserted {} files using shared transaction", inode_count);
    Ok(())
}

// Helper function to find missing parent directories
pub(crate) fn find_missing_parents(
    tx: &Transaction,
    new_paths: &[String]
) -> Result<Vec<String>, DatabaseError> {
    if new_paths.is_empty() {
        return Ok(Vec::new());
    }
    
    // Create a temporary table with the new paths
    tx.execute(
        "CREATE TEMP TABLE temp_new_paths (path VARCHAR)",
        []
    ).map_err(|_| DatabaseError::InsertError)?;
    
    // Insert new paths into temp table
    for path in new_paths {
        tx.execute(
            "INSERT INTO temp_new_paths VALUES (?)",
            params![path]
        ).map_err(|_| DatabaseError::InsertError)?;
    }
    
    // Find all missing parent directories
    let query = r#"
        WITH RECURSIVE path_parents AS (
            -- Generate all required parent paths
            SELECT DISTINCT 
                '/' || array_to_string(
                    list_slice(
                        string_split(ltrim(tnp.path, '/'), '/'), 
                        1, 
                        i
                    ), 
                    '/'
                ) as parent_path
            FROM temp_new_paths tnp,
            LATERAL (
                SELECT unnest(
                    generate_series(1, array_length(string_split(ltrim(tnp.path, '/'), '/')) - 1)
                ) as i
            )
            WHERE array_length(string_split(ltrim(tnp.path, '/'), '/')) > 1
        )
        SELECT DISTINCT pp.parent_path
        FROM path_parents pp
        LEFT JOIN inodes i ON pp.parent_path = i.path
        WHERE i.path IS NULL
        ORDER BY pp.parent_path
    "#;
    
    let mut stmt = tx.prepare(query).map_err(|_| DatabaseError::ProcessingError)?;
    let rows = stmt.query_map([], |row| {
        Ok(row.get::<_, String>(0)?)
    }).map_err(|_| DatabaseError::ProcessingError)?;
    
    let mut missing_parents = Vec::new();
    for row in rows {
        missing_parents.push(row.map_err(|_| DatabaseError::ProcessingError)?);
    }
    
    // Clean up temp table
    tx.execute("DROP TABLE temp_new_paths", [])
        .map_err(|_| DatabaseError::ProcessingError)?;
    
    Ok(missing_parents)
}

// Helper function to insert parent directories
pub(crate) fn insert_parent_directories(
    tx: &Transaction,
    missing_parents: &[String],
    owner_id: i32,
) -> Result<(), DatabaseError> {
    // Insert each missing parent directory
    for parent_path in missing_parents {
        // Generate stable UUIDv7 for folder identity
        let folder_id = crate::db::CustomUUID::new(None);
        
        tx.execute(
            "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, 'folder', NULL)",
            params![
                folder_id,
                owner_id,
                parent_path
            ]
        ).map_err(|e| {
            tracing::error!("Failed to insert parent directory: owner_id={} path={:?} error={:?}", owner_id, parent_path, e);
            DatabaseError::InsertError
        })?;
    }
    
    Ok(())
}

pub fn delete_files(
    db_tx: &duckdb::Transaction,
    path: String,
    user_id: i32,
) -> Result<(), DatabaseError> {
    // Get current consensus height for modification tracking
    let current_height = crate::db::consensus::get_current_consensus_height(db_tx)?;

    // Log ancestor folder modifications (reusing existing logic)
    log_ancestor_modifications(db_tx, &path, user_id, current_height)?;

    // Log deletion for ALL items that will be deleted (target + children) in a single SQL operation
    let logged_count = db_tx.execute(
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
            AND p.type = 'folder'
            AND p.path = substr(i.path, 1, length(i.path) - length(reverse(substr(reverse(i.path), 1, strpos(reverse(i.path), '/') - 1))) - 1)
            AND length(i.path) - length(replace(i.path, '/', '')) > 1
        )
        WHERE (i.path = ? OR i.path LIKE ?) AND i.owner_id = ?
        "#,
        params![current_height, path, format!("{}/%", path), user_id]
    ).map_err(|e| {
        tracing::error!("Failed to log modifications for deletion: {:?}", e);
        DatabaseError::ProcessingError
    })?;

    if logged_count == 0 {
        return Err(DatabaseError::NotFound);
    }

    tracing::debug!("Logged deletion of {} items (including children and ancestors) at height {}", logged_count, current_height);

    // Phase 2b: Clean up share memberships and pending outgoing shares for deleted files
    {
        let mut data_ids_stmt = db_tx.prepare(
            "SELECT DISTINCT data_id FROM inodes WHERE (path = ? OR path LIKE ?) AND owner_id = ? AND data_id IS NOT NULL"
        ).map_err(|_| DatabaseError::RecallError)?;
        let data_ids: Vec<CustomUUID> = data_ids_stmt.query_map(
            params![path, format!("{}/%", path), user_id],
            |row| row.get::<_, CustomUUID>(0)
        ).map_err(|_| DatabaseError::ProcessingError)?
            .collect::<Result<Vec<_>, _>>().map_err(|_| DatabaseError::ProcessingError)?;

        for data_block_id in &data_ids {
            crate::db::shares::remove_user_from_shares(db_tx, data_block_id, user_id)?;
            crate::db::shares::remove_sender_incoming_shares(db_tx, data_block_id, user_id)?;
        }
    }

    // Delete the file/folder and all its children (only for this user)
    db_tx.execute(
        "DELETE FROM inodes WHERE (path = ? OR path LIKE ?) AND owner_id = ?",
        params![path, format!("{}/%", path), user_id]
    ).map_err(|_| DatabaseError::ProcessingError)?;

    tracing::debug!("Deleted files at path {} for user {} using shared transaction", path, user_id);
    Ok(())
}

pub fn modify_item(
    db_tx: &duckdb::Transaction,
    user_id: i32,
    inode_id: crate::db::CustomUUID,
    new_encrypted_path: Option<String>,
    new_data_block_id: Option<crate::db::CustomUUID>,
    new_data_record: Option<crate::db::DataRecord>,
    incoming_share_updates: Option<Vec<crate::shares::types::IncomingShareUpdate>>,
) -> Result<(), DatabaseError> {
    // Check if the item exists and get its type and current path using inode_id
    tracing::debug!("modify_item: Querying inodes table for inode_id={} user_id={}", inode_id, user_id);
    let item_info: Option<(hopnet_common::InodeType, String)> = db_tx.query_row(
        "SELECT type, path FROM inodes WHERE id = ? AND owner_id = ?",
        params![inode_id, user_id],
        |row| Ok((row.get(0)?, row.get(1)?))
    ).optional().map_err(|_| DatabaseError::RecallError)?;

    let (item_type, current_encrypted_path) = match item_info {
        Some((itype, path)) => (itype, path),
        None => {
            tracing::warn!("modify_item: Item not found - inode_id: {}, user_id: {}", inode_id, user_id);
            return Err(DatabaseError::NotFound);
        },
    };

    // Capture old parent BEFORE any modifications
    let old_parent_id = get_parent_id(db_tx, &current_encrypted_path, user_id)
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to get parent for path {}: {:?}", current_encrypted_path, e);
            None
        });

    if let Some(ref new_path) = new_encrypted_path {
        // Circular reference prevention for folders
        if item_type == hopnet_common::InodeType::Folder {
            // Check if new path would place folder inside itself or its descendants
            // This happens when the new path starts with the current folder's path
            if new_path.starts_with(&format!("{}/", current_encrypted_path)) || new_path == &current_encrypted_path {
                tracing::warn!("Circular reference prevented: Cannot move folder '{}' into itself at '{}'",
                             current_encrypted_path, new_path);
                return Err(DatabaseError::InvalidPayload);  // Invalid operation - circular reference
            }
        }

        // Check if the new path already exists (exclude current inode to allow "move to same location")
        let new_exists: bool = db_tx.query_row(
            "SELECT COUNT(*) > 0 FROM inodes WHERE path = ? AND owner_id = ? AND id != ?",
            params![new_path, user_id, inode_id],
            |row| row.get(0)
        ).map_err(|_| DatabaseError::RecallError)?;

        if new_exists {
            return Err(DatabaseError::ConflictError); // Path already occupied
        }

        let rows_updated = match item_type {
            hopnet_common::InodeType::File => {
                // For files: simple path update
                db_tx.execute(
                    "UPDATE inodes SET path = ? WHERE path = ? AND owner_id = ?",
                    params![new_path, current_encrypted_path, user_id]
                ).map_err(|_| DatabaseError::ProcessingError)?
            }
            hopnet_common::InodeType::Folder => {
                // For folders: update the folder and all descendants
                // Use SQL string concatenation to update all child paths
                db_tx.execute(
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
                    ]
                ).map_err(|_| DatabaseError::ProcessingError)?
            }
        };

        tracing::debug!("modify_item: Path change validation successful - {} row(s) would be updated", rows_updated);
    }

    // Phase 4b: Handle content updates if new data is provided (works with or without path changes)
    if let (Some(new_data_id), Some(data_record)) = (new_data_block_id, new_data_record) {
        tracing::debug!("modify_item: Updating content for inode_id={} to new_data_id={}", inode_id, new_data_id);

        // Read old data_id BEFORE updating (needed for share propagation)
        let old_data_id: Option<CustomUUID> = db_tx.query_row(
            "SELECT data_id FROM inodes WHERE id = ? AND owner_id = ?",
            params![inode_id, user_id],
            |row| row.get::<_, Option<CustomUUID>>(0)
        ).optional().map_err(|_| DatabaseError::RecallError)?
            .unwrap_or(None);

        // Insert new data_block
        tracing::debug!("modify_item: Inserting data_block with id={} hash={} file_size={} fragment_count={}",
                       data_record.id, data_record.data.hash.to_hex(), data_record.file_size, data_record.data.fragments.len());
        db_tx.execute(
            "INSERT INTO data_blocks (id, file_hash, file_size, fragment_count, added_bytes, placement_height) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                data_record.id,
                data_record.data.hash,
                data_record.file_size,
                data_record.data.fragments.len() as i32,
                data_record.data.added_bytes,
                None::<i32>  // placement_height is NULL initially, set during fragment placement
            ]
        ).map_err(|e| {
            tracing::error!("modify_item: Failed to insert data_block id={}: {:?}", data_record.id, e);
            DatabaseError::InsertError
        })?;

        // Insert fragment hashes for the new data block
        tracing::debug!("modify_item: Inserting {} fragment_hashes for data_block_id={}", data_record.data.fragments.len(), data_record.id);
        for (i, fragment) in data_record.data.fragments.iter().enumerate() {
            tracing::debug!("modify_item: Inserting fragment_hash [{}/{}] chunk={} local_idx={} id={} hash={}",
                            i + 1, data_record.data.fragments.len(), fragment.chunk_number, fragment.local_index, fragment.fragment_id, fragment.fragment_hash.to_hex());
            db_tx.execute(
                "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally) VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    data_record.id,
                    fragment.chunk_number,
                    fragment.local_index,
                    fragment.fragment_id,
                    fragment.fragment_hash,
                    fragment.chunk_type,
                    fragment.stored_locally
                ]
            ).map_err(|e| {
                tracing::error!("modify_item: Failed to insert fragment_hash chunk={} local_idx={} id={}: {:?}", fragment.chunk_number, fragment.local_index, fragment.fragment_id, e);
                DatabaseError::InsertError
            })?;
        }

        // Insert file access entries for the new data block
        if let Some(ref file_access_entries) = data_record.file_access_entries {
            tracing::debug!("modify_item: Inserting {} file_access entries for data_block_id={}", file_access_entries.len(), data_record.id);
            for (i, access_entry) in file_access_entries.iter().enumerate() {
                tracing::debug!("modify_item: Inserting file_access [{}/{}] data_block_id={} user_id={}",
                                i + 1, file_access_entries.len(), access_entry.data_block_id, access_entry.user_id);
                db_tx.execute(
                    "INSERT INTO file_access (data_block_id, user_id, ephemeral_pubkey, encrypted_file_key) VALUES (?, ?, ?, ?)",
                    params![access_entry.data_block_id, access_entry.user_id, access_entry.ephemeral_pubkey, access_entry.encrypted_file_key]
                ).map_err(|e| {
                    tracing::error!("modify_item: Failed to insert file_access data_block_id={} user_id={}: {:?}", access_entry.data_block_id, access_entry.user_id, e);
                    DatabaseError::InsertError
                })?;
            }
        } else {
            tracing::debug!("modify_item: No file_access entries to insert for data_block_id={}", data_record.id);
        }

        // Update inode to point to new data block (this changes the modification time via UUIDv7)
        tracing::debug!("modify_item: Updating inode id={} to point to new data_id={} for user_id={}", inode_id, new_data_id, user_id);
        let rows_updated = db_tx.execute(
            "UPDATE inodes SET data_id = ? WHERE id = ? AND owner_id = ?",
            params![new_data_id, inode_id, user_id]
        ).map_err(|e| {
            tracing::error!("modify_item: Failed to update inode id={} data_id={} user_id={}: {:?}", inode_id, new_data_id, user_id, e);
            DatabaseError::ProcessingError
        })?;

        tracing::debug!("modify_item: Updated {} inode rows to new data_id={}", rows_updated, new_data_id);

        tracing::info!("Updated content for inode_id={} to data_id={}", inode_id, new_data_id);

        // Phase 2b: Share propagation — update other sharers' inodes and share tracking
        if let Some(old_data) = old_data_id {
            let sharers = crate::db::shares::get_sharers_for_data_block(db_tx, &old_data)?;
            if !sharers.is_empty() {
                tracing::debug!("modify_item: Propagating content update to {} sharers", sharers.len());

                // Update only current sharers' inodes to point to new data block
                // Must be scoped to shares table members to avoid updating unshared users
                let propagated = db_tx.execute(
                    "UPDATE inodes SET data_id = ? WHERE data_id = ? AND owner_id != ? AND owner_id IN (SELECT user_id FROM shares WHERE data_block_id = ?)",
                    params![new_data_id, old_data, user_id, old_data]
                ).map_err(|e| {
                    tracing::error!("modify_item: Failed to propagate data_id to other sharers: {:?}", e);
                    DatabaseError::ProcessingError
                })?;
                tracing::debug!("modify_item: Propagated content update to {} other sharers' inodes", propagated);

                // Log modification for each affected sharer's inode
                let current_height = crate::db::consensus::get_current_consensus_height(db_tx)?;
                let mut stmt = db_tx.prepare(
                    "SELECT id, owner_id, path FROM inodes WHERE data_id = ? AND owner_id != ?"
                ).map_err(|_| DatabaseError::RecallError)?;
                let affected: Vec<(CustomUUID, i32, String)> = stmt.query_map(
                    params![new_data_id, user_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                ).map_err(|_| DatabaseError::ProcessingError)?
                    .collect::<Result<Vec<_>, _>>().map_err(|_| DatabaseError::ProcessingError)?;
                for (affected_inode_id, affected_owner_id, affected_path) in &affected {
                    log_modification(db_tx, affected_inode_id.clone(), *affected_owner_id, None, None, Some(affected_path.as_str()), current_height)?;
                }

                // Update shares table: all rows from old → new data_block_id
                crate::db::shares::update_shares_data_block(db_tx, &old_data, &new_data_id)?;

                // Update pending incoming_shares with pre-computed file_access blobs
                if let Some(ref updates) = incoming_share_updates {
                    for update in updates {
                        crate::db::shares::update_incoming_share_data_block(
                            db_tx, &update.incoming_share_id, &new_data_id, &update.new_file_access_blob,
                        )?;
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
    let old_path_ref = if new_encrypted_path.is_some() { Some(current_encrypted_path.as_str()) } else { None };
    let new_path_ref = new_encrypted_path.as_deref().or(Some(current_encrypted_path.as_str()));
    log_modification(db_tx, inode_id.clone(), user_id, old_parent_id, old_path_ref, new_path_ref, current_height)?;

    tracing::debug!("Modified item inode_id={} for user {} using shared transaction", inode_id, user_id);
    Ok(())
}

pub fn get_file_fragments(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    encrypted_path: String,
    user_id: i32,
) -> Result<crate::files::functions::FileAccessData, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // First check if file exists and whether it's empty (data_id is NULL)
            let (file_exists, is_empty) = db_lock.prepare(
                "SELECT COUNT(*) > 0, COALESCE(BOOL_OR(data_id IS NULL), false)
                 FROM inodes
                 WHERE path = ? AND type = 'file'"
            ).and_then(|mut stmt| {
                stmt.query_row(params![encrypted_path.clone()], |row| {
                    Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?))
                })
            }).map_err(|_| DatabaseError::RecallError)?;

            if !file_exists {
                return Err(DatabaseError::RecallError); // File doesn't exist
            }

            if is_empty {
                // Empty file: no fragments, no encryption
                tracing::debug!("get_file_fragments: empty file detected for path {}", encrypted_path);
                return Ok(crate::files::functions::FileAccessData {
                    file_reassembly_data: None,  // No fragments for empty files
                    file_access_entry: None,     // No encryption for empty files
                });
            }

            // Non-empty file: query for fragments with reassembly info (now chunk-aware)
            let mut stmt = db_lock.prepare(
                "SELECT db.id, db.file_hash, db.added_bytes, db.placement_height, fh.chunk_number, fh.local_index, fh.fragment_id, fh.fragment_hash, fh.chunk_type, fh.stored_locally
                 FROM inodes i
                 JOIN data_blocks db ON i.data_id = db.id
                 JOIN fragment_hashes fh ON db.id = fh.data_block_id
                 WHERE i.path = ? AND i.type = 'file'
                 ORDER BY fh.chunk_number, fh.local_index"
            ).map_err(|_| DatabaseError::RecallError)?;

            let rows = stmt.query_map(params![encrypted_path], |row| {
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
                Ok((data_block_id, file_hash, added_bytes, placement_height, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally))
            }).map_err(|_| DatabaseError::ProcessingError)?;

            let mut data_block_id: Option<CustomUUID> = None;
            let mut file_hash: Option<Blake3Hash> = None;
            let mut added_bytes: Option<u8> = None;
            let mut placement_height: Option<i32> = None;

            // Group fragments by chunk_number: chunk_number -> (original_frags, recovery_frags)
            let mut chunks_map: std::collections::HashMap<u32, (
                std::collections::HashMap<usize, (Blake3Hash, CustomUUID, bool)>,  // originals
                std::collections::HashMap<usize, (Blake3Hash, CustomUUID, bool)>   // recovery
            )> = std::collections::HashMap::new();

            for row in rows {
                let (d_block_id, f_hash, a_bytes, p_height, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally) = row.map_err(|_| DatabaseError::ProcessingError)?;

                if data_block_id.is_none() {
                    data_block_id = Some(d_block_id);
                    file_hash = Some(f_hash);
                    added_bytes = Some(a_bytes);
                    placement_height = p_height;
                }

                // Get or create entry for this chunk
                let chunk_entry = chunks_map.entry(chunk_number).or_insert_with(|| {
                    (std::collections::HashMap::new(), std::collections::HashMap::new())
                });

                match chunk_type {
                    crate::db::ChunkType::Original => {
                        chunk_entry.0.insert(local_index as usize, (fragment_hash, fragment_id, stored_locally));
                    }
                    crate::db::ChunkType::Recovery => {
                        chunk_entry.1.insert(local_index as usize, (fragment_hash, fragment_id, stored_locally));
                    }
                }
            }
            
            match (data_block_id, file_hash, added_bytes) {
                (Some(data_block_id), Some(file_hash), Some(added_bytes)) => {
                    // Get file_access entry for this user and file
                    let file_access_entry = db_lock.prepare(
                        "SELECT data_block_id, user_id, ephemeral_pubkey, encrypted_file_key FROM file_access WHERE data_block_id = ? AND user_id = ?"
                    ).and_then(|mut stmt| {
                        stmt.query_row(params![data_block_id, user_id], |row| {
                            Ok(crate::db::types::FileAccess {
                                data_block_id: row.get(0)?,
                                user_id: row.get(1)?,
                                ephemeral_pubkey: row.get(2)?,
                                encrypted_file_key: row.get(3)?,
                            })
                        })
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
                        file_reassembly_data: Some(file_reassembly_data),  // Wrap in Some for non-empty files
                        file_access_entry,
                    })
                },
                _ => Err(DatabaseError::RecallError), // File not found
            }
        }
        Err(_) => Err(DatabaseError::LockError)
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
    db_tx: &duckdb::Transaction,
    updates: Vec<PlacementHeightUpdate>,
) -> Result<(), DatabaseError> {
    let updates_len = updates.len();
    for update in updates {
        db_tx.execute(
            "UPDATE data_blocks SET placement_height = ? WHERE id = ?",
            params![update.placement_height, update.data_block_id]
        ).map_err(|e| {
            tracing::error!("Error updating placement_height for {:?}: {:?}", update.data_block_id, e);
            DatabaseError::ProcessingError
        })?;
    }

    tracing::debug!("Updated placement_height for {} data blocks using shared transaction", updates_len);
    Ok(())
}

/// Get a specific file if it needs distribution (placement_height = NULL and all fragments stored locally)
pub fn get_distributable_file(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
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

            let rows = stmt.query_map([data_block_id.clone()], |row| {
                let f_hash: crate::types::Blake3Hash = row.get(0)?;
                let local_index: u32 = row.get(1)?;
                let fragment_hash: crate::types::Blake3Hash = row.get(2)?;
                let chunk_type: crate::db::ChunkType = row.get(3)?;

                Ok((f_hash, local_index, fragment_hash, chunk_type))
            }).map_err(|_| DatabaseError::RecallError)?;

            for row_result in rows {
                let (f_hash, local_index, fragment_hash, chunk_type) = row_result.map_err(|_| DatabaseError::ProcessingError)?;

                // Store file_hash from first row (same for all rows)
                if file_hash.is_none() {
                    file_hash = Some(f_hash);
                }

                let fragment_type = match chunk_type {
                    crate::db::ChunkType::Original => crate::files::placement::FragmentType::Original,
                    crate::db::ChunkType::Recovery => crate::files::placement::FragmentType::Recovery,
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
        Err(_) => Err(DatabaseError::LockError)
    }
}

/// Update local storage state for a fragment by its hash
pub fn mark_fragment_local_state(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    fragment_hash: &crate::types::Blake3Hash,
    stored_locally: bool,
) -> Result<usize, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let rows_affected = db_lock.execute(
                "UPDATE fragment_hashes SET stored_locally = ? WHERE fragment_hash = ?",
                params![stored_locally, fragment_hash]
            ).map_err(|e| {
                tracing::error!("Error updating stored_locally for fragment hash {}: {:?}", fragment_hash, e);
                DatabaseError::ProcessingError
            })?;
            
            let state_text = if stored_locally { "stored locally" } else { "not stored locally" };
            tracing::debug!("Marked {} fragment records with hash {} as {}", rows_affected, fragment_hash, state_text);
            Ok(rows_affected)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

/// Update local storage state for a fragment by its hash (transaction version)
pub fn mark_fragment_local_state_tx(
    tx: &Transaction,
    fragment_hash: &crate::types::Blake3Hash,
    stored_locally: bool,
) -> Result<usize, DatabaseError> {
    let rows_affected = tx.execute(
        "UPDATE fragment_hashes SET stored_locally = ? WHERE fragment_hash = ?",
        params![stored_locally, fragment_hash]
    ).map_err(|e| {
        tracing::error!("Error updating stored_locally for fragment hash {}: {:?}", fragment_hash, e);
        DatabaseError::ProcessingError
    })?;

    let state_text = if stored_locally { "stored locally" } else { "not stored locally" };
    tracing::debug!("Marked {} fragment records with hash {} as {}", rows_affected, fragment_hash, state_text);
    Ok(rows_affected)
}

/// Data about a file ready for distribution
#[derive(Debug, Clone)]
pub struct DistributableFileData {
    pub id: CustomUUID,
    pub file_hash: crate::types::Blake3Hash,
    pub fragment_hashes: Vec<(usize, crate::types::Blake3Hash, crate::files::placement::FragmentType)>,
}

/// Get count of fragments stored locally on this node
pub fn get_local_fragment_count(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<i64, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let count = db_lock.query_row(
                "SELECT COUNT(*) FROM fragment_hashes WHERE stored_locally = TRUE",
                [],
                |row| row.get::<_, i64>(0)
            ).map_err(|e| {
                tracing::error!("Error querying local fragment count: {:?}", e);
                DatabaseError::RecallError
            })?;
            
            tracing::debug!("Found {} fragments stored locally", count);
            Ok(count)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

/// Extract all ancestor folder IDs from a path for modification tracking
/// Returns list of ancestor folder IDs from immediate parent up to root
/// Example: "/a/b/c/file.txt" returns IDs for ["/a/b/c", "/a/b", "/a"] if they exist
fn get_all_ancestor_folders(tx: &Transaction, path: &str, owner_id: i32) -> Result<Vec<CustomUUID>, DatabaseError> {
    let mut stmt = tx.prepare(
        "SELECT id FROM inodes 
         WHERE owner_id = ? AND type = 'folder' AND ? LIKE path || '/%'
         ORDER BY LENGTH(path) DESC"
    ).map_err(|_| DatabaseError::ProcessingError)?;
    
    let rows = stmt.query_map(params![owner_id, path], |row| {
        row.get::<_, CustomUUID>(0)
    }).map_err(|_| DatabaseError::ProcessingError)?;
    
    let ancestors: Result<Vec<_>, _> = rows.collect();
    ancestors.map_err(|_| DatabaseError::ProcessingError)
}

/// Extract parent folder's inode_id from a path
/// Returns None for root level items or if parent folder doesn't exist
fn get_parent_id(tx: &Transaction, path: &str, owner_id: i32) -> Result<Option<CustomUUID>, DatabaseError> {
    let parent_path = match path.rfind('/') {
        Some(idx) if idx > 1 => &path[..idx],  // Has parent (not root level)
        _ => return Ok(None),  // Root level item, no parent
    };
    
    match tx.query_row(
        "SELECT id FROM inodes WHERE path = ? AND owner_id = ? AND type = 'folder'",
        params![parent_path, owner_id],
        |row| row.get::<_, CustomUUID>(0)
    ) {
        Ok(parent_id) => Ok(Some(parent_id)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),  // Parent folder doesn't exist
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
    old_parent_id: Option<CustomUUID>,  // Parent BEFORE modification (None for new items)
    old_path: Option<&str>,  // Path BEFORE modification (for moves/deletes)
    new_path: Option<&str>,  // Path AFTER modification (for inserts/moves)
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
    
    tracing::debug!("Logged modification for inode_id {} with old_parent_id {:?} at height {}",
                   inode_id, old_parent_id, modification_height);
    Ok(())
}

/// Look up an inode by its UUID and owner. Returns (data_id, encrypted_path, inode_type).
pub fn get_inode_by_id(
    conn: &duckdb::Connection,
    inode_id: &CustomUUID,
    owner_id: i32,
) -> Result<Option<(Option<CustomUUID>, String, hopnet_common::InodeType)>, DatabaseError> {
    conn.query_row(
        "SELECT data_id, path, type FROM inodes WHERE id = ? AND owner_id = ?",
        params![inode_id, owner_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    ).optional().map_err(|_| DatabaseError::RecallError)
}

/// Look up a user's file_access entry for a specific data_block.
pub fn get_file_access(
    conn: &duckdb::Connection,
    data_block_id: &CustomUUID,
    user_id: i32,
) -> Result<Option<crate::db::types::FileAccess>, DatabaseError> {
    conn.query_row(
        "SELECT data_block_id, user_id, ephemeral_pubkey, encrypted_file_key FROM file_access WHERE data_block_id = ? AND user_id = ?",
        params![data_block_id, user_id],
        |row| Ok(crate::db::types::FileAccess {
            data_block_id: row.get(0)?,
            user_id: row.get(1)?,
            ephemeral_pubkey: row.get(2)?,
            encrypted_file_key: row.get(3)?,
        })
    ).optional().map_err(|_| DatabaseError::RecallError)
}
