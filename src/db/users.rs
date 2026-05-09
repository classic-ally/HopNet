use super::*;
use crate::types::User;

pub fn get_users(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<Vec<User>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare("SELECT * FROM users").map_err(|_| DatabaseError::RecallError)?;
            let results = stmt.query_map([], |row| {
                Ok(User {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    pubkey: row.get(2)?,
                    x25519_pubkey: row.get(3)?,
                    encrypted_privkey: row.get(4)?,
                    key_salt: row.get(5)?,
                    first_name: row.get(6)?,
                    last_name: row.get(7)?,
                    avatar: row.get(8)?,
                    onboarding_flags: row.get(9)?,
                })
            });

            match results {
                Ok(users) => Ok(users.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?),
                Err(e) => {
                    tracing::error!("Error querying users: {:?}", e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            tracing::error!("Database connection error in get_users: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn get_user_by_username(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    username: String,
) -> Result<Option<User>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT * FROM users WHERE username = ?"
            ).map_err(|_|DatabaseError::RecallError)?;

            let mut rows = stmt.query(&[&username]).map_err(|_|DatabaseError::RecallError)?;

            if let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
                let user = User {
                    user_id: row.get(0).map_err(|_| DatabaseError::RecallError)?,
                    username: row.get(1).map_err(|_| DatabaseError::RecallError)?,
                    pubkey: row.get(2).map_err(|_| DatabaseError::RecallError)?,
                    x25519_pubkey: row.get(3).map_err(|_| DatabaseError::RecallError)?,
                    encrypted_privkey: row.get(4).map_err(|_| DatabaseError::RecallError)?,
                    key_salt: row.get(5).map_err(|_| DatabaseError::RecallError)?,
                    first_name: row.get(6).map_err(|_| DatabaseError::RecallError)?,
                    last_name: row.get(7).map_err(|_| DatabaseError::RecallError)?,
                    avatar: row.get(8).map_err(|_| DatabaseError::RecallError)?,
                    onboarding_flags: row.get(9).map_err(|_| DatabaseError::RecallError)?,
                };
                return Ok(Some(user))
            } else {
                return Ok(None)
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_user_by_userid_conn(
    conn: &rusqlite::Connection,
    userid: i32,
) -> Result<Option<User>, DatabaseError> {
    let mut stmt = conn.prepare(
        "SELECT * FROM users WHERE user_id = ?"
    ).map_err(|_| DatabaseError::RecallError)?;

    let mut rows = stmt.query(&[&userid]).map_err(|_| DatabaseError::RecallError)?;

    if let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
        let user = User {
            user_id: row.get(0).map_err(|_| DatabaseError::RecallError)?,
            username: row.get(1).map_err(|_| DatabaseError::RecallError)?,
            pubkey: row.get(2).map_err(|_| DatabaseError::RecallError)?,
            x25519_pubkey: row.get(3).map_err(|_| DatabaseError::RecallError)?,
            encrypted_privkey: row.get(4).map_err(|_| DatabaseError::RecallError)?,
            key_salt: row.get(5).map_err(|_| DatabaseError::RecallError)?,
            first_name: row.get(6).map_err(|_| DatabaseError::RecallError)?,
            last_name: row.get(7).map_err(|_| DatabaseError::RecallError)?,
            avatar: row.get(8).map_err(|_| DatabaseError::RecallError)?,
            onboarding_flags: row.get(9).map_err(|_| DatabaseError::RecallError)?,
        };
        Ok(Some(user))
    } else {
        Ok(None)
    }
}

pub fn get_user_by_userid(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    userid: i32,
) -> Result<Option<User>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => get_user_by_userid_conn(&db_lock, userid),
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Core user insertion logic - operates within provided transaction for atomicity
/// Returns the assigned user_id
pub fn insert_user_tx(
    tx: &rusqlite::Transaction,
    user: User,
) -> Result<i32, DatabaseError> {
    let next_id = tx.query_row(
        "SELECT next_id FROM sequences WHERE name = 'users'",
        [],
        |row| row.get::<_, i32>(0)
    ).map_err(|_| DatabaseError::RecallError)?;

    tx.execute(
        "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt) VALUES (?, ?, ?, ?, ?, ?)",
        params![next_id, user.username, user.pubkey, user.x25519_pubkey, user.encrypted_privkey, user.key_salt]
    ).map_err(|_| DatabaseError::InsertError)?;

    // Update the sequence for next user
    tx.execute(
        "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'users'",
        []
    ).map_err(|_| DatabaseError::InsertError)?;

    Ok(next_id)
}

/// Update user profile fields within a transaction.
/// For each field: None = no change, Some(None) = clear, Some(Some(v)) = set.
/// Uses CASE WHEN to distinguish "no change" from "set to NULL".
pub fn update_user_profile_tx(
    tx: &rusqlite::Transaction,
    user_id: i32,
    first_name: Option<Option<&str>>,
    last_name: Option<Option<&str>>,
    avatar: Option<Option<&[u8]>>,
) -> Result<(), DatabaseError> {
    let fn_changing = first_name.is_some();
    let fn_val: Option<&str> = first_name.flatten();

    let ln_changing = last_name.is_some();
    let ln_val: Option<&str> = last_name.flatten();

    let av_changing = avatar.is_some();
    let av_val: Option<&[u8]> = avatar.flatten();

    tx.execute(
        "UPDATE users SET \
            first_name = CASE WHEN ? THEN ? ELSE first_name END, \
            last_name  = CASE WHEN ? THEN ? ELSE last_name END, \
            avatar     = CASE WHEN ? THEN ? ELSE avatar END \
         WHERE user_id = ?",
        params![fn_changing, fn_val, ln_changing, ln_val, av_changing, av_val, user_id]
    ).map_err(|_| DatabaseError::InsertError)?;

    Ok(())
}

/// Apply onboarding-flag bitfield update within a consensus transaction.
/// `set_flags` are OR'd in; `clear_flags` are AND-NOT'd. Idempotent.
pub fn update_user_onboarding_tx(
    tx: &rusqlite::Transaction,
    payload: &crate::users::types::UpdateUserOnboardingPayload,
) -> Result<(), DatabaseError> {
    tx.execute(
        "UPDATE users SET onboarding_flags = (onboarding_flags | ?) & ~? WHERE user_id = ?",
        params![payload.set_flags, payload.clear_flags, payload.user_id],
    ).map_err(|_| DatabaseError::InsertError)?;
    Ok(())
}

/// Wrapper that manages connection and transaction - for backward compatibility
pub fn insert_user(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    user: User,
    execute: bool,
) -> Result<(), DatabaseError> {

    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            let user_id = insert_user_tx(&tx, user)?;

            // Commit or rollback based on execute flag
            if execute {
                crate::db::shared::commit_timed(tx).map_err(|_| DatabaseError::InsertError)?;
                tracing::info!("Successfully inserted user {}", user_id);
            } else {
                tx.rollback().map_err(|_| DatabaseError::LockError)?;
                tracing::debug!("User {} insertion validated successfully (rolled back)", user_id);
            }

            Ok(())
        },
        Err(_) => Err(DatabaseError::LockError),
    }
}
