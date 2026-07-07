//! Narrow READ helpers against the HOST-owned `users` table.
//!
//! The users table is host-owned; drive SQL may READ it (same SQLite DB —
//! the ownership boundary is code, not schema; precedent:
//! `db::files::get_file_access`). These helpers replicate the host's
//! `db::users` / `db::types::blob_access_for_user` semantics exactly:
//! prepare/query failures map to `RecallError`, pool checkout failures to
//! `LockError`, wrap failures to `ProcessingError`.

use hopnet_common::CustomUUID;
use hopnet_projection::DatabaseError;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::OptionalExtension;
use x25519_dalek::PublicKey as X25519PublicKey;

/// The recipient fields the share flow needs: id + X25519 pubkey.
pub struct RecipientUser {
    pub user_id: i32,
    pub x25519_pubkey: X25519PublicKey,
}

fn pubkey_from_blob(blob: Vec<u8>) -> Result<X25519PublicKey, DatabaseError> {
    let bytes = <[u8; 32]>::try_from(blob).map_err(|_| DatabaseError::RecallError)?;
    Ok(X25519PublicKey::from(bytes))
}

/// Look up a recipient by username (share flow). `Ok(None)` = no such user.
pub fn get_recipient_by_username(
    conn: &rusqlite::Connection,
    username: &str,
) -> Result<Option<RecipientUser>, DatabaseError> {
    let row: Option<(i32, Vec<u8>)> = conn
        .query_row(
            "SELECT user_id, x25519_pubkey FROM users WHERE username = ?",
            rusqlite::params![username],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    match row {
        Some((user_id, blob)) => Ok(Some(RecipientUser {
            user_id,
            x25519_pubkey: pubkey_from_blob(blob)?,
        })),
        None => Ok(None),
    }
}

/// Whether a user row exists (upload flow's pre-check).
pub fn user_exists(
    db_connection: Result<PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    user_id: i32,
) -> Result<bool, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let row: Option<i32> = db_lock
                .query_row(
                    "SELECT user_id FROM users WHERE user_id = ?",
                    rusqlite::params![user_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|_| DatabaseError::RecallError)?;
            Ok(row.is_some())
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Wrap the per-blob key to a user's X25519 pubkey — resolves the user's
/// pubkey from the DB, then delegates to the substrate's v1 wrap. Mirrors
/// the host's `db::types::blob_access_for_user_with_conn`.
pub fn blob_access_for_user_with_conn(
    conn: &rusqlite::Connection,
    blob_id: CustomUUID,
    user_id: i32,
    per_blob_key: &chacha20poly1305::Key,
) -> Result<hopnet_storage::BlobAccess, DatabaseError> {
    let blob: Option<Vec<u8>> = conn
        .query_row(
            "SELECT x25519_pubkey FROM users WHERE user_id = ?",
            rusqlite::params![user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    let pubkey = match blob {
        Some(blob) => pubkey_from_blob(blob)?,
        None => return Err(DatabaseError::RecallError), // User not found
    };
    hopnet_storage::crypto::wrap_blob_key(&blob_id, &pubkey, per_blob_key)
        .map_err(|_| DatabaseError::ProcessingError)
}

pub fn blob_access_for_user(
    db_connection: Result<PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    blob_id: CustomUUID,
    user_id: i32,
    per_blob_key: &chacha20poly1305::Key,
) -> Result<hopnet_storage::BlobAccess, DatabaseError> {
    match db_connection {
        Ok(db_lock) => blob_access_for_user_with_conn(&db_lock, blob_id, user_id, per_blob_key),
        Err(_) => Err(DatabaseError::LockError),
    }
}
