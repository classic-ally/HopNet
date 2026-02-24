use aes_siv::{siv::Aes256Siv, Key, Nonce};
use rusqlite::params;
use r2d2::PooledConnection;

use crate::db::{CustomUUID, CustomDateTime, SqliteConnectionManager};
use hopnet_common::db::InodeType;
use crate::files::functions::decrypt_path;
use hopnet_common::documentprovider::DocumentProviderItem;

use super::DatabaseError;

/// Get a single item by inode_id, returning a DocumentProviderItem
/// Uses a self-join to get parent_id in a single query
pub fn get_item(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    inode_id: &CustomUUID,
    user_id: i32,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
) -> Result<DocumentProviderItem, DatabaseError> {
    let query = r#"
        SELECT
            i.id,
            i.type,
            i.path,
            db.file_size,
            COALESCE(
                uuid_extract_timestamp(i.data_id),
                uuid_extract_timestamp(i.id)
            ) as last_modified,
            parent.id as parent_id
        FROM inodes i
        LEFT JOIN data_blocks db ON i.data_id = db.id
        LEFT JOIN inodes parent ON
            parent.path = substr(i.path, 1, length(i.path) - INSTR(reverse(i.path), '/'))
            AND parent.owner_id = i.owner_id
        WHERE i.id = ? AND i.owner_id = ?
        LIMIT 1
    "#;

    let mut stmt = db_lock.prepare(query).map_err(|_| DatabaseError::ProcessingError)?;

    let (id, item_type, encrypted_path, file_size, last_modified, parent_id): (
        CustomUUID,
        InodeType,
        String,
        Option<i64>,
        CustomDateTime,
        Option<CustomUUID>,
    ) = stmt
        .query_row(params![inode_id, user_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?))
        })
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
            _ => DatabaseError::RecallError,
        })?;

    // Decrypt path to get filename
    let decrypted_path = decrypt_path(encrypted_path, siv_key, siv_nonce)
        .map_err(|_| DatabaseError::ProcessingError)?;
    let name = decrypted_path
        .split('/')
        .last()
        .unwrap_or(&decrypted_path)
        .to_string();

    // Derive MIME type from filename
    let mime_type = match item_type {
        InodeType::Folder => "vnd.android.document/directory".to_string(),
        InodeType::File => mime_guess::from_path(&name)
            .first()
            .map(|m| m.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string()),
    };

    Ok(DocumentProviderItem {
        id,
        name,
        mime_type,
        size: file_size.unwrap_or(0),
        last_modified: last_modified.timestamp_millis(),
        parent_id,
    })
}

/// Get minimal metadata needed for file download: encrypted_path and type
/// Single table query, no joins - optimized for download hot path
pub fn get_download_metadata(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    inode_id: &CustomUUID,
    user_id: i32,
) -> Result<(String, InodeType), DatabaseError> {
    let query = r#"
        SELECT path, type
        FROM inodes
        WHERE id = ? AND owner_id = ?
        LIMIT 1
    "#;

    let mut stmt = db_lock.prepare(query).map_err(|_| DatabaseError::ProcessingError)?;

    stmt.query_row(params![inode_id, user_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, InodeType>(1)?))
    })
    .map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
        _ => DatabaseError::RecallError,
    })
}

/// Get encrypted path for a given inode_id
/// Takes an existing db lock to allow combining with other operations
pub fn get_path_by_inode_id(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    inode_id: &CustomUUID,
    user_id: i32,
) -> Result<String, DatabaseError> {
    let query = r#"
        SELECT path
        FROM inodes
        WHERE id = ? AND owner_id = ?
        LIMIT 1
    "#;

    let mut stmt = db_lock.prepare(query).map_err(|_| DatabaseError::ProcessingError)?;

    let path: String = stmt.query_row(
        params![inode_id, user_id],
        |row| row.get(0)
    ).map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
        _ => DatabaseError::RecallError,
    })?;

    Ok(path)
}

/// Get children of a folder, returning DocumentProviderItems directly
/// Takes an existing db lock and encrypted parent path
pub fn get_children(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    user_id: i32,
    encrypted_parent_path: &str,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
    parent_id: Option<CustomUUID>,
) -> Result<Vec<DocumentProviderItem>, DatabaseError> {
    let query = r#"
        SELECT
            i.id,
            i.type,
            i.path,
            db.file_size,
            COALESCE(
                uuid_extract_timestamp(i.data_id),
                uuid_extract_timestamp(i.id)
            ) as last_modified
        FROM inodes i
        LEFT JOIN data_blocks db ON i.data_id = db.id
        WHERE i.owner_id = ?
          AND i.path LIKE ?
          AND i.path NOT LIKE ?
        ORDER BY i.type DESC, i.path ASC
    "#;

    let like_pattern = format!("{}/%", encrypted_parent_path);
    let not_like_pattern = format!("{}/%/%", encrypted_parent_path);

    let mut stmt = db_lock.prepare(query).map_err(|e| {
        tracing::error!("get_children prepare failed: {:?}", e);
        DatabaseError::ProcessingError
    })?;

    let rows = stmt.query_map(
        params![user_id, like_pattern, not_like_pattern],
        |row| {
            let id: CustomUUID = row.get(0)?;
            let item_type: InodeType = row.get(1)?;
            let encrypted_path: String = row.get(2)?;
            let file_size: Option<i64> = row.get(3)?;
            let last_modified: crate::db::CustomDateTime = row.get(4)?;

            // Decrypt path to get filename
            let decrypted_path = decrypt_path(encrypted_path.clone(), siv_key, siv_nonce)
                .map_err(|e| {
                    tracing::error!("get_children path decryption failed for inode {}: {:?} (encrypted_path len={})", id, e, encrypted_path.len());
                    rusqlite::Error::InvalidColumnType(
                        2,
                        "path_decryption".to_string(),
                        rusqlite::types::Type::Text,
                    )
                })?;
            let name = decrypted_path.split('/').last().unwrap_or(&decrypted_path).to_string();

            // Derive MIME type from filename
            let mime_type = match item_type {
                InodeType::Folder => "vnd.android.document/directory".to_string(),
                InodeType::File => mime_guess::from_path(&name)
                    .first()
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
            };

            Ok(DocumentProviderItem {
                id,
                name,
                mime_type,
                size: file_size.unwrap_or(0),
                last_modified: last_modified.timestamp_millis(),
                parent_id: parent_id.clone(),
            })
        },
    ).map_err(|e| {
        tracing::error!("get_children query_map failed: {:?}", e);
        DatabaseError::RecallError
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(|e| {
        tracing::error!("get_children row collection failed: {:?}", e);
        DatabaseError::RecallError
    })
}
