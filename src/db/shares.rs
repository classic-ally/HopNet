use super::*;
use duckdb::OptionalExt;

/// Raw incoming share row from the database.
pub struct IncomingShareRow {
    pub id: CustomUUID,
    pub data_block_id: CustomUUID,
    pub sender_id: i32,
    pub recipient_id: i32,
    pub file_access: Vec<u8>,
    pub display_ephemeral_pubkey: Vec<u8>,
    pub encrypted_display_name: Vec<u8>,
}

/// Share member info for the detail view.
pub struct ShareMember {
    pub username: String,
    pub user_id: i32,
    pub status: String,
}

pub fn insert_incoming_share(
    db_tx: &duckdb::Transaction,
    id: CustomUUID,
    data_block_id: CustomUUID,
    sender_id: i32,
    recipient_id: i32,
    file_access: &[u8],
    display_ephemeral_pubkey: &[u8],
    encrypted_display_name: &[u8],
) -> Result<(), DatabaseError> {
    db_tx.execute(
        "INSERT INTO incoming_shares (id, data_block_id, sender_id, recipient_id, file_access, display_ephemeral_pubkey, encrypted_display_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![id, data_block_id, sender_id, recipient_id, file_access.to_vec(), display_ephemeral_pubkey.to_vec(), encrypted_display_name.to_vec()]
    ).map_err(|e| {
        tracing::error!("Failed to insert incoming_share {}: {:?}", id, e);
        DatabaseError::InsertError
    })?;
    Ok(())
}

pub fn get_incoming_shares_for_user(
    db_connection: Result<PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    recipient_id: i32,
) -> Result<Vec<(IncomingShareRow, String)>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT s.id, s.data_block_id, s.sender_id, s.recipient_id, s.file_access, s.display_ephemeral_pubkey, s.encrypted_display_name, u.username
                 FROM incoming_shares s
                 JOIN users u ON s.sender_id = u.user_id
                 WHERE s.recipient_id = ?
                 ORDER BY s.id"
            ).map_err(|_| DatabaseError::RecallError)?;

            let rows = stmt.query_map([recipient_id], |row| {
                Ok((
                    IncomingShareRow {
                        id: row.get(0)?,
                        data_block_id: row.get(1)?,
                        sender_id: row.get(2)?,
                        recipient_id: row.get(3)?,
                        file_access: row.get(4)?,
                        display_ephemeral_pubkey: row.get(5)?,
                        encrypted_display_name: row.get(6)?,
                    },
                    row.get::<_, String>(7)?,
                ))
            }).map_err(|_| DatabaseError::ProcessingError)?;

            rows.collect::<Result<Vec<_>, _>>().map_err(|_| DatabaseError::ProcessingError)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn get_incoming_share_count(
    db_connection: Result<PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    recipient_id: i32,
) -> Result<i64, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            db_lock.query_row(
                "SELECT COUNT(*) FROM incoming_shares WHERE recipient_id = ?",
                [recipient_id],
                |row| row.get(0)
            ).map_err(|_| DatabaseError::RecallError)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn get_incoming_share_by_id(
    db_tx: &duckdb::Transaction,
    share_id: &CustomUUID,
) -> Result<Option<IncomingShareRow>, DatabaseError> {
    db_tx.query_row(
        "SELECT id, data_block_id, sender_id, recipient_id, file_access, display_ephemeral_pubkey, encrypted_display_name FROM incoming_shares WHERE id = ?",
        params![share_id],
        |row| Ok(IncomingShareRow {
            id: row.get(0)?,
            data_block_id: row.get(1)?,
            sender_id: row.get(2)?,
            recipient_id: row.get(3)?,
            file_access: row.get(4)?,
            display_ephemeral_pubkey: row.get(5)?,
            encrypted_display_name: row.get(6)?,
        })
    ).optional().map_err(|_| DatabaseError::RecallError)
}

pub fn delete_incoming_share(
    db_tx: &duckdb::Transaction,
    share_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    let rows = db_tx.execute(
        "DELETE FROM incoming_shares WHERE id = ?",
        params![share_id]
    ).map_err(|e| {
        tracing::error!("Failed to delete incoming_share {}: {:?}", share_id, e);
        DatabaseError::ProcessingError
    })?;

    if rows == 0 {
        return Err(DatabaseError::NotFound);
    }
    Ok(())
}

pub fn insert_share_members(
    db_tx: &duckdb::Transaction,
    data_block_id: CustomUUID,
    user_ids: &[i32],
) -> Result<(), DatabaseError> {
    for &user_id in user_ids {
        db_tx.execute(
            "INSERT OR IGNORE INTO shares (data_block_id, user_id) VALUES (?, ?)",
            params![data_block_id, user_id]
        ).map_err(|e| {
            tracing::error!("Failed to insert share member data_block={} user={}: {:?}", data_block_id, user_id, e);
            DatabaseError::InsertError
        })?;
    }
    Ok(())
}

/// Check if a share already exists for this recipient+data_block (across both tables).
pub fn share_exists_for_recipient(
    db_tx: &duckdb::Transaction,
    data_block_id: &CustomUUID,
    recipient_id: i32,
) -> Result<bool, DatabaseError> {
    let exists: bool = db_tx.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM incoming_shares WHERE data_block_id = ? AND recipient_id = ?
            UNION ALL
            SELECT 1 FROM shares WHERE data_block_id = ? AND user_id = ?
        )",
        params![data_block_id, recipient_id, data_block_id, recipient_id],
        |row| row.get(0)
    ).map_err(|_| DatabaseError::RecallError)?;

    Ok(exists)
}

pub fn get_share_details(
    db_connection: Result<PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    data_block_id: &CustomUUID,
) -> Result<Vec<ShareMember>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT u.username, s.user_id, 'accepted' as status
                 FROM shares s
                 JOIN users u ON s.user_id = u.user_id
                 WHERE s.data_block_id = ?
                 UNION ALL
                 SELECT u.username, ist.recipient_id as user_id, 'pending' as status
                 FROM incoming_shares ist
                 JOIN users u ON ist.recipient_id = u.user_id
                 WHERE ist.data_block_id = ?"
            ).map_err(|_| DatabaseError::RecallError)?;

            let rows = stmt.query_map(params![data_block_id, data_block_id], |row| {
                Ok(ShareMember {
                    username: row.get(0)?,
                    user_id: row.get(1)?,
                    status: row.get(2)?,
                })
            }).map_err(|_| DatabaseError::ProcessingError)?;

            rows.collect::<Result<Vec<_>, _>>().map_err(|_| DatabaseError::ProcessingError)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}
