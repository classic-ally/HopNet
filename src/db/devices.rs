use crate::db::{Blake3Hash, CustomUUID, DatabaseError, SqliteConnectionManager};
use r2d2::PooledConnection;
use rusqlite::{Transaction, params};

/// Insert device token within a consensus transaction
pub fn insert_device_token_tx(
    db_tx: &Transaction,
    id: &CustomUUID,
    user_id: i32,
    api_key_hash: &Blake3Hash,
    encrypted_device_name: &str,
    wrapped_user_key: &[u8],
) -> Result<(), DatabaseError> {
    db_tx.execute(
        "INSERT INTO device_tokens (id, user_id, api_key_hash, encrypted_device_name, wrapped_user_key) VALUES (?, ?, ?, ?, ?)",
        params![id, user_id, api_key_hash, encrypted_device_name, wrapped_user_key],
    ).map_err(|e| {
        tracing::error!("Failed to insert device token: {:?}", e);
        DatabaseError::InsertError
    })?;
    Ok(())
}

/// Delete device token within a consensus transaction (revocation)
/// Idempotent: succeeds even if device doesn't exist or doesn't belong to user
pub fn delete_device_token_tx(
    db_tx: &Transaction,
    device_id: &CustomUUID,
    user_id: i32,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "DELETE FROM device_tokens WHERE id = ? AND user_id = ?",
            params![device_id, user_id],
        )
        .map_err(|e| {
            tracing::error!("Failed to delete device token: {:?}", e);
            DatabaseError::ProcessingError
        })?;
    Ok(())
}

/// Device token record for auth verification
pub struct DeviceTokenRecord {
    pub id: CustomUUID,
    pub user_id: i32,
    pub api_key_hash: Blake3Hash,
    pub wrapped_user_key: Vec<u8>,
}

/// Get device by ID for auth verification (primary key lookup, O(log n))
pub fn get_device_by_id(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    device_id: &CustomUUID,
) -> Result<Option<DeviceTokenRecord>, DatabaseError> {
    let result = db_lock.query_row(
        "SELECT id, user_id, api_key_hash, wrapped_user_key FROM device_tokens WHERE id = ?",
        params![device_id],
        |row| {
            Ok(DeviceTokenRecord {
                id: row.get(0)?,
                user_id: row.get(1)?,
                api_key_hash: row.get(2)?,
                wrapped_user_key: row.get(3)?,
            })
        },
    );

    match result {
        Ok(record) => Ok(Some(record)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => {
            tracing::error!("Failed to get device by id: {:?}", e);
            Err(DatabaseError::RecallError)
        }
    }
}

/// Device info for listing (includes encrypted name for decryption by caller)
pub struct DeviceListRecord {
    pub id: CustomUUID,
    pub encrypted_device_name: String,
}

/// List devices for a user (for management UI)
/// Returns encrypted device names - caller must decrypt with user's SIV key
pub fn get_devices_for_user(
    db_lock: &PooledConnection<SqliteConnectionManager>,
    user_id: i32,
) -> Result<Vec<DeviceListRecord>, DatabaseError> {
    let mut stmt = db_lock
        .prepare(
            "SELECT id, encrypted_device_name FROM device_tokens WHERE user_id = ? ORDER BY id",
        )
        .map_err(|e| {
            tracing::error!("Failed to prepare device list query: {:?}", e);
            DatabaseError::RecallError
        })?;

    let rows = stmt
        .query_map(params![user_id], |row| {
            Ok(DeviceListRecord {
                id: row.get(0)?,
                encrypted_device_name: row.get(1)?,
            })
        })
        .map_err(|e| {
            tracing::error!("Failed to query devices: {:?}", e);
            DatabaseError::RecallError
        })?;

    let mut devices = Vec::new();
    for row in rows {
        devices.push(row.map_err(|e| {
            tracing::error!("Failed to read device row: {:?}", e);
            DatabaseError::RecallError
        })?);
    }

    Ok(devices)
}
