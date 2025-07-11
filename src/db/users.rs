use duckdb::DuckdbConnectionManager;

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
                    password: row.get(2)?,
                    pubkey: row.get(3)?,
                    x25519_pubkey: row.get(4)?
                })
            });

            match results {
                Ok(users) => Ok(users.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?),
                Err(e) => {
                    dbg!(e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            dbg!(e);
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
                    password: row.get(2).map_err(|_| DatabaseError::RecallError)?,
                    pubkey: row.get(3).map_err(|_| DatabaseError::RecallError)?,
                    x25519_pubkey: row.get(4).map_err(|_| DatabaseError::RecallError)?
                };
                return Ok(Some(user))
            } else {
                return Ok(None)
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_user_by_userid(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    userid: i32,
) -> Result<Option<User>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT * FROM users WHERE user_id = ?"
            ).map_err(|_|DatabaseError::RecallError)?;

            let mut rows = stmt.query(&[&userid]).map_err(|_|DatabaseError::RecallError)?;

            if let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
                let user = User {
                    user_id: row.get(0).map_err(|_| DatabaseError::RecallError)?,
                    username: row.get(1).map_err(|_| DatabaseError::RecallError)?,
                    password: row.get(2).map_err(|_| DatabaseError::RecallError)?,
                    pubkey: row.get(3).map_err(|_| DatabaseError::RecallError)?,
                    x25519_pubkey: row.get(4).map_err(|_| DatabaseError::RecallError)?
                };
                return Ok(Some(user))
            } else {
                return Ok(None)
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn insert_user(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    mut user: User,
) -> Result<(), DatabaseError> {

    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;
            let next_id = tx.query_row(
                "SELECT next_id FROM sequences WHERE name = 'users'",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;

            let password_hash = user.password_hash().map_err(|_| DatabaseError::ProcessingError)?;

            tx.execute(
                "INSERT INTO users (user_id, username, password_hash, pubkey, x25519_pubkey) VALUES (?, ?, ?, ?, ?)",
                params![next_id, user.username, password_hash, user.pubkey, user.x25519_pubkey]
            ).map_err(|_| DatabaseError::InsertError)?;
            
            // Update the sequence for next user
            tx.execute(
                "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'users'",
                []
            ).map_err(|_| DatabaseError::InsertError)?;
            
            // Commit the transaction
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            
            dbg!("Successfully inserted user");
            Ok(())
        },
        Err(_) => Err(DatabaseError::LockError),
    }
}
