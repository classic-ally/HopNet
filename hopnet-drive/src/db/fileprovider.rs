//! Drive FileProvider DB surface (RFC-015).
//!
//! Moved verbatim from the host's `db::fileprovider`; the host re-exports
//! this module at its old path.

use crate::model::CustomDateTime;
use crate::paths::decrypt_path;
use aes_siv::{Key, Nonce, siv::Aes256Siv};
use hopnet_common::height::{height_from_db, height_to_db};
use hopnet_common::{CustomUUID, InodeType};
use hopnet_projection::DatabaseError;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

/// Simple database result for FileProvider items
/// Raw data that gets serialized over HTTP to FileProvider binary
#[derive(Debug)]
pub struct FileProviderItemData {
    pub identifier: String,
    pub item_type: InodeType,
    pub filename: String,                                  // Decrypted filename
    pub parent_item_identifier: String,                    // Parent folder's item: identifier
    pub file_size: Option<u64>,                            // File size in bytes (None for folders)
    pub creation_date: Option<CustomDateTime>, // Timestamp extracted from UUIDv7 or folder creation
    pub content_modification_date: Option<CustomDateTime>, // Timestamp from modified_at column
    pub modification_height: Option<u64>,      // Consensus height when item was last modified
}

/// FileProvider enumeration result with consensus height for sync anchoring
#[derive(Debug)]
pub struct FileProviderEnumerateResult {
    pub items: Vec<FileProviderItemData>,
    pub current_consensus_height: u64,
    pub deleted_identifiers: Option<Vec<String>>, // Optional list of deleted item identifiers
}

/// Get folder contents for FileProvider directory enumeration
/// Returns raw data that FileProvider binary can convert to objc2 types
pub fn get_folder_contents(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    user_id: i32,
    parent_path_pattern: &str,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
    cursor: Option<&str>,
    limit: usize,
) -> Result<FileProviderEnumerateResult, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let query = r#"
                SELECT 
                    'item:' || CAST(i.id AS VARCHAR) as identifier,
                    i.type as item_type,
                    i.path as encrypted_path,
                    CASE 
                        WHEN length(i.path) - length(replace(i.path, '/', '')) = 1 THEN 'NSFileProviderRootContainerItemIdentifier'
                        ELSE COALESCE('item:' || CAST(parent.id AS VARCHAR), 'NSFileProviderRootContainerItemIdentifier')
                    END as parent_item_identifier,
                    db.file_size,
                    uuid_extract_timestamp(i.id) as creation_date,
                    CASE 
                        WHEN i.data_id IS NOT NULL THEN uuid_extract_timestamp(i.data_id)
                        WHEN i.type = 1 THEN (
                            SELECT MAX(uuid_extract_timestamp(COALESCE(child.data_id, child.id)))
                            FROM inodes child
                            WHERE child.owner_id = i.owner_id
                              AND child.path LIKE i.path || '/%'
                        )
                        ELSE NULL
                    END as content_modification_date,
                    ml.modified_at_height
                FROM inodes i
                LEFT JOIN data_blocks db ON i.data_id = db.id
                LEFT JOIN inodes parent ON (
                    parent.owner_id = i.owner_id
                    AND parent.type = 1
                    AND parent.path = substr(i.path, 1, length(i.path) - length(reverse(substr(reverse(i.path), 1, INSTR(reverse(i.path), '/') - 1))) - 1)
                    AND length(i.path) - length(replace(i.path, '/', '')) > 1
                )
                LEFT JOIN (
                    SELECT 
                        inode_id,
                        MAX(modified_at_height) as modified_at_height
                    FROM modification_log
                    WHERE owner_id = ?
                    GROUP BY inode_id
                ) ml ON i.id = ml.inode_id
                WHERE i.owner_id = ? AND i.path LIKE ? AND i.path NOT LIKE ?
                  AND (? IS NULL OR i.path > ?)
                ORDER BY i.type DESC, i.path ASC
                LIMIT ?
            "#;

            let mut stmt = db_lock
                .prepare(query)
                .map_err(|_| DatabaseError::ProcessingError)?;

            // Create not_like_pattern same as get_files
            let not_like_pattern = format!("{}/%", parent_path_pattern);

            let rows = stmt
                .query_map(
                    params![
                        user_id,
                        user_id,
                        parent_path_pattern,
                        not_like_pattern,
                        cursor,
                        cursor,
                        limit as i64
                    ],
                    |row| {
                        let identifier: String = row.get(0)?;
                        let item_type: InodeType = row.get(1)?; // Direct deserialization using FromSql
                        let encrypted_path: String = row.get(2)?; // path is VARCHAR in database
                        let parent_item_identifier: String = row.get(3)?; // Parent identifier from JOIN
                        let file_size = row.get::<_, Option<i64>>(4)?.map(|v| v as u64); // File size from data_blocks (NULL for folders)
                        let creation_date: Option<CustomDateTime> = row.get(5)?; // UUIDv7 timestamp or NULL for folders
                        let content_modification_date: Option<CustomDateTime> = row.get(6)?; // modified_at from data_blocks
                        let modification_height: Option<u64> = row.get::<_, Option<i64>>(7)?.map(height_from_db); // Consensus height when item was last modified

                        // Decrypt the full path using the same pattern as get_files
                        let decrypted_path = decrypt_path(encrypted_path, siv_key, siv_nonce)
                            .map_err(|_| {
                                rusqlite::Error::InvalidColumnType(
                                    2,
                                    "path_decryption".to_string(),
                                    rusqlite::types::Type::Text,
                                )
                            })?;

                        // Extract filename from path (last component after '/')
                        let filename = decrypted_path
                            .split('/')
                            .next_back()
                            .unwrap_or(&decrypted_path)
                            .to_string();

                        Ok(FileProviderItemData {
                            identifier,
                            item_type,
                            filename,
                            parent_item_identifier,
                            file_size,
                            creation_date,
                            content_modification_date,
                            modification_height,
                        })
                    },
                )
                .map_err(|_| DatabaseError::RecallError)?;

            let items: Result<Vec<FileProviderItemData>, _> = rows
                .map(|row_result| row_result.map_err(|_| DatabaseError::RecallError))
                .collect();

            let items = items?;

            // Current decided height (malachite schema — the legacy blocks
            // table died with the bespoke engine at Stage 5b)
            let current_consensus_height =
                crate::db::current_height(&db_lock)?;

            Ok(FileProviderEnumerateResult {
                items,
                current_consensus_height,
                deleted_identifiers: None, // Not needed for regular enumeration
            })
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get folder changes since a given consensus height for FileProvider incremental sync
/// Returns all folders in the path + files changed since the given consensus height
pub fn get_folder_changes_since_height(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    user_id: i32,
    encrypted_parent_path: &str,
    since_height: u64,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
) -> Result<FileProviderEnumerateResult, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // Check if this is a root query (empty encrypted path means root "/")
            let is_root = encrypted_parent_path.is_empty();

            // Common metadata selection logic
            let base_query = r#"
                SELECT DISTINCT
                    source.inode_id,
                    CASE WHEN i.id IS NOT NULL THEN 'exists' ELSE 'deleted' END as status,
                    -- Metadata fields only when item exists
                    CASE WHEN i.id IS NOT NULL THEN 'item:' || CAST(i.id AS VARCHAR) ELSE NULL END as identifier,
                    i.type as item_type,
                    i.path as encrypted_path,
                    CASE 
                        WHEN i.id IS NOT NULL AND length(i.path) - length(replace(i.path, '/', '')) = 1 THEN 'NSFileProviderRootContainerItemIdentifier'
                        WHEN i.id IS NOT NULL THEN COALESCE('item:' || CAST(parent.id AS VARCHAR), 'NSFileProviderRootContainerItemIdentifier')
                        ELSE NULL
                    END as parent_item_identifier,
                    db.file_size,
                    CASE WHEN i.id IS NOT NULL THEN uuid_extract_timestamp(i.id) ELSE NULL END as creation_date,
                    CASE 
                        WHEN i.data_id IS NOT NULL THEN uuid_extract_timestamp(i.data_id)
                        WHEN i.type = 1 THEN (
                            SELECT MAX(uuid_extract_timestamp(COALESCE(child.data_id, child.id)))
                            FROM inodes child
                            WHERE child.owner_id = i.owner_id
                              AND child.path LIKE i.path || '/%'
                        )
                        ELSE NULL
                    END as content_modification_date,
                    source.modified_at_height
                FROM ({source_query}) source
                LEFT JOIN inodes i ON source.inode_id = i.id
                LEFT JOIN data_blocks db ON i.data_id = db.id
                LEFT JOIN inodes parent ON (
                    parent.owner_id = i.owner_id
                    AND parent.type = 1
                    AND parent.path = substr(i.path, 1, length(i.path) - length(reverse(substr(reverse(i.path), 1, INSTR(reverse(i.path), '/') - 1))) - 1)
                    AND length(i.path) - length(replace(i.path, '/', '')) > 1
                )
                ORDER BY status DESC, i.type DESC, i.path ASC
            "#;

            let final_query = if is_root {
                // Root case: all modifications

                // tracing::debug!("Root query assembled: {}", root_query);
                base_query.replace("{source_query}", "SELECT DISTINCT ml.inode_id, ml.modified_at_height FROM modification_log ml WHERE ml.owner_id = ? AND ml.modified_at_height > ?")
            } else {
                // Specific folder case: items that were or are in this folder

                // tracing::debug!("Folder query assembled for path '{}': {}", encrypted_parent_path, folder_query);
                base_query.replace("{source_query}", r#"
                    WITH target_folder AS (
                        SELECT id FROM inodes WHERE owner_id = ? AND path = ? AND type = 1
                    ),
                    modified_items AS (
                        SELECT 
                            ml.inode_id,
                            ml.old_parent_id,
                            -- Get current parent if item exists
                            CASE WHEN i.id IS NOT NULL AND length(i.path) > 2 THEN (
                                SELECT p.id FROM inodes p
                                WHERE p.owner_id = i.owner_id AND p.type = 1
                                  AND p.path = substr(i.path, 1, length(i.path) - length(reverse(substr(reverse(i.path), 1, INSTR(reverse(i.path), '/') - 1))) - 1)
                            ) ELSE NULL END as current_parent_id
                        FROM modification_log ml
                        LEFT JOIN inodes i ON ml.inode_id = i.id AND i.owner_id = ml.owner_id
                        WHERE ml.owner_id = ? AND ml.modified_at_height > ?
                    )
                    SELECT DISTINCT mi.inode_id, ml.modified_at_height
                    FROM modified_items mi
                    LEFT JOIN modification_log ml ON mi.inode_id = ml.inode_id AND ml.owner_id = ?
                    WHERE (mi.old_parent_id IN (SELECT id FROM target_folder)
                       OR mi.current_parent_id IN (SELECT id FROM target_folder))
                       AND ml.modified_at_height > ?
                "#)
            };

            let mut stmt = db_lock
                .prepare(&final_query)
                .map_err(|_| DatabaseError::ProcessingError)?;

            // Tuple shape returned by the row mapper for this query:
            // (status, inode_id, identifier, item_type, encrypted_path,
            //  parent_item_identifier, file_size, creation_date,
            //  content_modification_date, modification_height).
            type EnumerateRow = (
                String,
                CustomUUID,
                Option<String>,
                Option<InodeType>,
                Option<String>,
                Option<String>,
                Option<u64>,
                Option<CustomDateTime>,
                Option<CustomDateTime>,
                Option<u64>,
            );

            // Define the closure once to avoid type mismatch
            let row_mapper = |row: &rusqlite::Row| -> Result<EnumerateRow, rusqlite::Error> {
                let inode_id: CustomUUID = row.get(0)?;
                let status: String = row.get(1)?;
                let identifier: Option<String> = row.get(2)?;
                let item_type: Option<InodeType> = row.get(3)?;
                let encrypted_path: Option<String> = row.get(4)?;
                let parent_item_identifier: Option<String> = row.get(5)?;
                let file_size = row.get::<_, Option<i64>>(6)?.map(|v| v as u64);
                let creation_date: Option<CustomDateTime> = row.get(7)?;
                let content_modification_date: Option<CustomDateTime> = row.get(8)?;
                let modification_height: Option<u64> = row.get::<_, Option<i64>>(9)?.map(height_from_db);

                Ok((
                    status,
                    inode_id,
                    identifier,
                    item_type,
                    encrypted_path,
                    parent_item_identifier,
                    file_size,
                    creation_date,
                    content_modification_date,
                    modification_height,
                ))
            };

            tracing::debug!(
                "Executing query with params - user_id: {}, since_height: {}, is_root: {}",
                user_id,
                since_height,
                is_root
            );

            let rows = if is_root {
                stmt.query_map(params![user_id, height_to_db(since_height)], row_mapper)
                    .map_err(|_| DatabaseError::RecallError)?
            } else {
                stmt.query_map(
                    params![
                        user_id,
                        encrypted_parent_path,
                        user_id,
                        height_to_db(since_height),
                        user_id,
                        height_to_db(since_height)
                    ],
                    row_mapper,
                )
                .map_err(|_| DatabaseError::RecallError)?
            };

            let mut items: Vec<FileProviderItemData> = Vec::new();
            let mut deleted_identifiers: Vec<String> = Vec::new();

            for row_result in rows {
                let (
                    status,
                    inode_id,
                    identifier,
                    item_type,
                    encrypted_path,
                    parent_item_identifier,
                    file_size,
                    creation_date,
                    content_modification_date,
                    modification_height,
                ) = row_result.map_err(|_| DatabaseError::RecallError)?;

                if status == "deleted" {
                    // Add to deleted_identifiers array
                    deleted_identifiers.push(format!("item:{}", inode_id));
                } else {
                    // Item exists - decrypt path and add to items
                    let encrypted_path = encrypted_path.ok_or(DatabaseError::ProcessingError)?;
                    let decrypted_path = decrypt_path(encrypted_path, siv_key, siv_nonce)
                        .map_err(|_| DatabaseError::ProcessingError)?;

                    let filename = decrypted_path
                        .split('/')
                        .next_back()
                        .unwrap_or(&decrypted_path)
                        .to_string();

                    items.push(FileProviderItemData {
                        identifier: identifier.ok_or(DatabaseError::ProcessingError)?,
                        item_type: item_type.ok_or(DatabaseError::ProcessingError)?,
                        filename,
                        parent_item_identifier: parent_item_identifier
                            .ok_or(DatabaseError::ProcessingError)?,
                        file_size,
                        creation_date,
                        content_modification_date,
                        modification_height,
                    });
                }
            }

            // Current decided height (malachite schema — same as
            // get_folder_contents)
            let current_consensus_height =
                crate::db::current_height(&db_lock)?;

            tracing::debug!(
                "Found {} changed items and {} deleted items since height {}",
                items.len(),
                deleted_identifiers.len(),
                since_height
            );

            Ok(FileProviderEnumerateResult {
                items,
                current_consensus_height,
                deleted_identifiers: Some(deleted_identifiers),
            })
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get the encrypted path, file size, timestamps, and type for any item by its inode_id
/// Used by FileProvider get_item endpoint to fetch complete metadata for files and folders
/// Tuple returned by [`get_item_metadata_by_inode_id`]:
/// `(encrypted_path, item_type, file_size, creation_date, content_modification_date, modification_height)`.
pub type InodeMetadata = (
    String,
    InodeType,
    Option<u64>,
    CustomDateTime,
    Option<CustomDateTime>,
    Option<u64>,
);

pub fn get_item_metadata_by_inode_id(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    inode_id: CustomUUID,
    user_id: i32,
) -> Result<InodeMetadata, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let query = r#"
                SELECT 
                    i.path, 
                    i.type,
                    db.file_size, 
                    uuid_extract_timestamp(i.id) as creation_date,
                    CASE 
                        WHEN i.data_id IS NOT NULL THEN uuid_extract_timestamp(i.data_id)
                        WHEN i.type = 1 THEN (
                            SELECT MAX(uuid_extract_timestamp(COALESCE(child.data_id, child.id)))
                            FROM inodes child
                            WHERE child.owner_id = i.owner_id
                              AND child.path LIKE i.path || '/%'
                        )
                        ELSE NULL
                    END as content_modification_date,
                    ml.modified_at_height
                FROM inodes i
                LEFT JOIN data_blocks db ON i.data_id = db.id
                LEFT JOIN (
                    SELECT 
                        inode_id,
                        MAX(modified_at_height) as modified_at_height
                    FROM modification_log
                    WHERE owner_id = ?
                    GROUP BY inode_id
                ) ml ON i.id = ml.inode_id
                WHERE i.id = ? AND i.owner_id = ?
                LIMIT 1
            "#;

            let mut stmt = db_lock
                .prepare(query)
                .map_err(|_| DatabaseError::ProcessingError)?;

            let result = stmt
                .query_row(params![user_id, inode_id, user_id], |row| {
                    let path: String = row.get(0)?;
                    let item_type: InodeType = row.get(1)?;
                    let file_size = row.get::<_, Option<i64>>(2)?.map(|v| v as u64); // NULL for folders
                    let creation_date: CustomDateTime = row.get(3)?; // UUIDv7 from inode.id (always exists)
                    let content_modification_date: Option<CustomDateTime> = row.get(4)?; // UUIDv7 from data_id (files only)
                    let modification_height: Option<u64> = row.get::<_, Option<i64>>(5)?.map(height_from_db); // Consensus height when item was last modified
                    Ok((
                        path,
                        item_type,
                        file_size,
                        creation_date,
                        content_modification_date,
                        modification_height,
                    ))
                })
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
                    _ => DatabaseError::RecallError,
                })?;

            Ok(result)
        }
        Err(e) => {
            tracing::error!(
                "Database connection error in get_item_metadata_by_inode_id: {:?}",
                e
            );
            Err(DatabaseError::LockError)
        }
    }
}

/// Get the encrypted path for a file by its data_id
/// Used by FileProvider delete endpoint to resolve file identifiers
pub fn get_file_path_by_data_id(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    data_id: CustomUUID,
    user_id: i32,
) -> Result<String, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let query = r#"
                SELECT path
                FROM inodes
                WHERE data_id = ? AND owner_id = ? AND type = 0
                LIMIT 1
            "#;

            let mut stmt = db_lock
                .prepare(query)
                .map_err(|_| DatabaseError::ProcessingError)?;

            let encrypted_path: String = stmt
                .query_row(params![data_id, user_id], |row| row.get(0))
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
                    _ => DatabaseError::RecallError,
                })?;

            Ok(encrypted_path)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get inode_id for a given encrypted path and user
/// Used by FileProvider to convert paths to unified item: identifiers
pub fn get_inode_id_by_path(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    encrypted_path: &str,
    user_id: i32,
) -> Result<CustomUUID, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let query = r#"
                SELECT id
                FROM inodes 
                WHERE path = ? AND owner_id = ?
                LIMIT 1
            "#;

            let mut stmt = db_lock
                .prepare(query)
                .map_err(|_| DatabaseError::ProcessingError)?;

            let inode_id: CustomUUID = stmt
                .query_row(params![encrypted_path, user_id], |row| row.get(0))
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
                    _ => DatabaseError::RecallError,
                })?;

            Ok(inode_id)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Check if a folder is empty (has no children)
/// Used by FileProvider delete endpoint to validate non-recursive folder deletion
pub fn is_folder_empty(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    encrypted_path: &str,
    user_id: i32,
) -> Result<bool, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // Check if there are any children under this path
            let query = r#"
                SELECT COUNT(*) 
                FROM inodes 
                WHERE path LIKE ? AND owner_id = ?
                LIMIT 1
            "#;

            let mut stmt = db_lock
                .prepare(query)
                .map_err(|_| DatabaseError::ProcessingError)?;

            // Pattern to match children: "path/%"
            let children_pattern = format!("{}/%", encrypted_path);

            let count: i64 = stmt
                .query_row(params![children_pattern, user_id], |row| row.get(0))
                .map_err(|_| DatabaseError::RecallError)?;

            Ok(count == 0)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}
