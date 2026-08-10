//! Mount-surface DB reads (RFC-018 S2).
//!
//! UUID-native rows for the Linux mount daemon: no sentinel identifier
//! strings, cursor pagination that compares and orders by the SAME column
//! (i.id — deliberately not the fileprovider cursor shape, which filters
//! on a different column than it orders by). UUIDv7 id strings sort
//! lexicographically in creation order, so `i.id > ?cursor ORDER BY i.id`
//! is a stable walk; readdir snapshots need stability, not any particular
//! ordering.

use aes_siv::{siv::Aes256Siv, Key, Nonce};
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

use crate::model::CustomDateTime;
use crate::paths::decrypt_path;
use hopnet_common::db::InodeType;
use hopnet_common::mount::MountItem;
use hopnet_common::CustomUUID;
use hopnet_projection::DatabaseError;

/// Shared SELECT list for item rows. Parent id comes from a self-join on
/// the everything-before-the-last-slash prefix; single-segment paths have
/// no matching parent row, so `parent.id` is NULL = root.
const ITEM_SELECT: &str = r#"
    SELECT
        i.id,
        i.type,
        i.path,
        db.file_size,
        i.data_id,
        uuid_extract_timestamp(i.id) as created,
        COALESCE(
            uuid_extract_timestamp(i.data_id),
            uuid_extract_timestamp(i.id)
        ) as modified,
        ml.modified_at_height,
        parent.id as parent_id
    FROM inodes i
    LEFT JOIN data_blocks db ON i.data_id = db.id
    LEFT JOIN (
        SELECT inode_id, MAX(modified_at_height) as modified_at_height
        FROM modification_log
        WHERE owner_id = ?1
        GROUP BY inode_id
    ) ml ON i.id = ml.inode_id
    LEFT JOIN inodes parent ON
        parent.path = substr(i.path, 1, length(i.path) - INSTR(reverse(i.path), '/'))
        AND parent.owner_id = i.owner_id
"#;

fn row_to_item(
    row: &rusqlite::Row<'_>,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
) -> rusqlite::Result<MountItem> {
    let id: CustomUUID = row.get(0)?;
    let item_type: InodeType = row.get(1)?;
    let encrypted_path: String = row.get(2)?;
    let file_size: Option<i64> = row.get(3)?;
    let data_id: Option<CustomUUID> = row.get(4)?;
    let created: CustomDateTime = row.get(5)?;
    let modified: CustomDateTime = row.get(6)?;
    let height: Option<u64> = row
        .get::<_, Option<i64>>(7)?
        .map(hopnet_common::height::height_from_db);
    let parent_id: Option<CustomUUID> = row.get(8)?;

    let decrypted_path = decrypt_path(encrypted_path, siv_key, siv_nonce).map_err(|_| {
        rusqlite::Error::InvalidColumnType(
            2,
            "path_decryption".to_string(),
            rusqlite::types::Type::Text,
        )
    })?;
    let name = decrypted_path
        .split('/')
        .next_back()
        .unwrap_or(&decrypted_path)
        .to_string();

    let size = match item_type {
        InodeType::File => Some(file_size.unwrap_or(0) as u64),
        InodeType::Folder => None,
    };

    Ok(MountItem {
        id: Some(id),
        parent_id,
        name,
        item_type,
        size,
        blob_id: data_id,
        created_ms: created.timestamp_millis(),
        modified_ms: Some(modified.timestamp_millis()),
        height,
    })
}

/// One page of a folder's children, resumable via last-seen-id cursor.
/// Returns up to `limit` items; the caller requests limit+1 to detect a
/// further page.
pub fn children_page(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    user_id: i32,
    encrypted_parent_path: &str,
    cursor: Option<&str>,
    limit: u32,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
) -> Result<Vec<MountItem>, DatabaseError> {
    let query = format!(
        "{ITEM_SELECT}
         WHERE i.owner_id = ?1
           AND i.path LIKE ?2
           AND i.path NOT LIKE ?3
           AND (?4 IS NULL OR i.id > ?4)
         ORDER BY i.id ASC
         LIMIT ?5"
    );

    let like_pattern = format!("{}/%", encrypted_parent_path);
    let not_like_pattern = format!("{}/%/%", encrypted_parent_path);

    let mut stmt = db_lock.prepare(&query).map_err(|e| {
        tracing::error!("children_page prepare failed: {e:?}");
        DatabaseError::ProcessingError
    })?;

    let rows = stmt
        .query_map(
            params![user_id, like_pattern, not_like_pattern, cursor, limit],
            |row| row_to_item(row, siv_key, siv_nonce),
        )
        .map_err(|e| {
            tracing::error!("children_page query failed: {e:?}");
            DatabaseError::RecallError
        })?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|_| DatabaseError::RecallError)
}

/// Single item by inode id.
pub fn item_by_id(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    user_id: i32,
    inode_id: &CustomUUID,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
) -> Result<MountItem, DatabaseError> {
    let query = format!("{ITEM_SELECT} WHERE i.id = ?2 AND i.owner_id = ?1 LIMIT 1");
    let mut stmt = db_lock
        .prepare(&query)
        .map_err(|_| DatabaseError::ProcessingError)?;
    stmt.query_row(params![user_id, inode_id], |row| {
        row_to_item(row, siv_key, siv_nonce)
    })
    .map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
        _ => DatabaseError::RecallError,
    })
}

/// Single item by exact encrypted path — the /lookup hot path. PK
/// (owner_id, path) makes this an index hit.
pub fn item_by_exact_path(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    user_id: i32,
    encrypted_path: &str,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
) -> Result<MountItem, DatabaseError> {
    let query = format!("{ITEM_SELECT} WHERE i.path = ?2 AND i.owner_id = ?1 LIMIT 1");
    let mut stmt = db_lock
        .prepare(&query)
        .map_err(|_| DatabaseError::ProcessingError)?;
    stmt.query_row(params![user_id, encrypted_path], |row| {
        row_to_item(row, siv_key, siv_nonce)
    })
    .map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => DatabaseError::NotFound,
        _ => DatabaseError::RecallError,
    })
}

/// Whole-tree changes strictly after `since_height`: latest state per
/// touched inode (grouped to MAX height), split into live items and
/// deleted ids.
pub fn changes_since(
    // &Connection (not the pooled guard) so callers can pass a Transaction
    // and read the delta + height anchor from one snapshot.
    db_lock: &rusqlite::Connection,
    user_id: i32,
    since_height: u64,
    siv_key: &Key<Aes256Siv>,
    siv_nonce: &Nonce,
) -> Result<(Vec<MountItem>, Vec<CustomUUID>), DatabaseError> {
    let query = r#"
        SELECT
            source.inode_id,
            i.id,
            i.type,
            i.path,
            db.file_size,
            i.data_id,
            CASE WHEN i.id IS NOT NULL THEN uuid_extract_timestamp(i.id) END as created,
            COALESCE(
                uuid_extract_timestamp(i.data_id),
                uuid_extract_timestamp(i.id)
            ) as modified,
            source.modified_at_height,
            parent.id as parent_id
        FROM (
            SELECT ml.inode_id, MAX(ml.modified_at_height) as modified_at_height
            FROM modification_log ml
            WHERE ml.owner_id = ?1 AND ml.modified_at_height > ?2
            GROUP BY ml.inode_id
        ) source
        LEFT JOIN inodes i ON source.inode_id = i.id AND i.owner_id = ?1
        LEFT JOIN data_blocks db ON i.data_id = db.id
        LEFT JOIN inodes parent ON
            parent.path = substr(i.path, 1, length(i.path) - INSTR(reverse(i.path), '/'))
            AND parent.owner_id = i.owner_id
    "#;

    let mut stmt = db_lock
        .prepare(query)
        .map_err(|_| DatabaseError::ProcessingError)?;

    let mut items = Vec::new();
    let mut deleted = Vec::new();

    let rows = stmt
        .query_map(
            params![user_id, hopnet_common::height::height_to_db(since_height)],
            |row| {
                let logged_id: CustomUUID = row.get(0)?;
                let live_id: Option<CustomUUID> = row.get(1)?;
                if live_id.is_none() {
                    return Ok(Err(logged_id));
                }
                let item_type: InodeType = row.get(2)?;
                let encrypted_path: String = row.get(3)?;
                let file_size: Option<i64> = row.get(4)?;
                let data_id: Option<CustomUUID> = row.get(5)?;
                let created: Option<CustomDateTime> = row.get(6)?;
                let modified: Option<CustomDateTime> = row.get(7)?;
                let height: Option<u64> = row
                    .get::<_, Option<i64>>(8)?
                    .map(hopnet_common::height::height_from_db);
                let parent_id: Option<CustomUUID> = row.get(9)?;

                let decrypted_path =
                    decrypt_path(encrypted_path, siv_key, siv_nonce).map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            3,
                            "path_decryption".to_string(),
                            rusqlite::types::Type::Text,
                        )
                    })?;
                let name = decrypted_path
                    .split('/')
                    .next_back()
                    .unwrap_or(&decrypted_path)
                    .to_string();

                let size = match item_type {
                    InodeType::File => Some(file_size.unwrap_or(0) as u64),
                    InodeType::Folder => None,
                };

                Ok(Ok(MountItem {
                    id: live_id,
                    parent_id,
                    name,
                    item_type,
                    size,
                    blob_id: data_id,
                    created_ms: created.map(|c| c.timestamp_millis()).unwrap_or_default(),
                    modified_ms: modified.map(|m| m.timestamp_millis()),
                    height,
                }))
            },
        )
        .map_err(|e| {
            tracing::error!("changes_since query failed: {e:?}");
            DatabaseError::RecallError
        })?;

    for row in rows {
        match row.map_err(|_| DatabaseError::RecallError)? {
            Ok(item) => items.push(item),
            Err(gone) => deleted.push(gone),
        }
    }

    Ok((items, deleted))
}
