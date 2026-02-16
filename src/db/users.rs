use super::*;
use crate::types::User;

pub fn get_users(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
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
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
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
    conn: &duckdb::Connection,
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
        };
        Ok(Some(user))
    } else {
        Ok(None)
    }
}

pub fn get_user_by_userid(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
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
    tx: &duckdb::Transaction,
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

/// Wrapper that manages connection and transaction - for backward compatibility
pub fn insert_user(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    user: User,
    execute: bool,
) -> Result<(), DatabaseError> {

    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            let user_id = insert_user_tx(&tx, user)?;

            // Commit or rollback based on execute flag
            if execute {
                tx.commit().map_err(|_| DatabaseError::InsertError)?;
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
