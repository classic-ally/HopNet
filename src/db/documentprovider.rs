use aes_siv::{siv::Aes256Siv, Key, Nonce};
use duckdb::params;
use r2d2::PooledConnection;

use crate::db::{CustomUUID, DuckdbConnectionManager};
use hopnet_common::db::InodeType;
use crate::files::functions::decrypt_path;
use hopnet_common::documentprovider::DocumentProviderItem;

use super::DatabaseError;

/// Get encrypted path for a given inode_id
/// Takes an existing db lock to allow combining with other operations
pub fn get_path_by_inode_id(
    db_lock: &PooledConnection<DuckdbConnectionManager>,
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
        duckdb::Error::QueryReturnedNoRows => DatabaseError::NotFound,
        _ => DatabaseError::RecallError,
    })?;

    Ok(path)
}

/// Get children of a folder, returning DocumentProviderItems directly
/// Takes an existing db lock and encrypted parent path
pub fn get_children(
    db_lock: &PooledConnection<DuckdbConnectionManager>,
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

    let mut stmt = db_lock.prepare(query).map_err(|_| DatabaseError::ProcessingError)?;

    let rows = stmt.query_map(
        params![user_id, like_pattern, not_like_pattern],
        |row| {
            let id: CustomUUID = row.get(0)?;
            let item_type: InodeType = row.get(1)?;
            let encrypted_path: String = row.get(2)?;
            let file_size: Option<i64> = row.get(3)?;
            let last_modified: crate::db::CustomDateTime = row.get(4)?;

            // Decrypt path to get filename
            let decrypted_path = decrypt_path(encrypted_path, siv_key, siv_nonce)
                .map_err(|_| duckdb::Error::InvalidColumnType(
                    2,
                    "path_decryption".to_string(),
                    duckdb::types::Type::Text,
                ))?;
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
    ).map_err(|_| DatabaseError::RecallError)?;

    rows.collect::<Result<Vec<_>, _>>().map_err(|_| DatabaseError::RecallError)
}
