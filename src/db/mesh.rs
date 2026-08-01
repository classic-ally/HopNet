//! Mesh-wide keypair state (RFC-014 "all users" access primitive).
//!
//! `mesh_key` holds the single mesh X25519 pubkey; `mesh_key_access` holds
//! the privkey wrapped to each member's pubkey. Rows are written ONLY by
//! transaction handlers (genesis / insert_user) so joining nodes reproduce
//! them through ordinary replay.

use crate::db::DatabaseError;
use hopnet_storage::MeshKeyGrant;
use rusqlite::{OptionalExtension, params};

pub fn insert_mesh_key_tx(
    db_tx: &rusqlite::Transaction,
    pubkey: &[u8; 32],
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT INTO mesh_key (internal_id, pubkey, key_version) VALUES (1, ?, 1)",
            params![pubkey.to_vec()],
        )
        .map_err(|e| {
            tracing::error!("Failed to insert mesh_key: {:?}", e);
            DatabaseError::InsertError
        })?;
    Ok(())
}

pub fn insert_mesh_grant_tx(
    db_tx: &rusqlite::Transaction,
    grant: &MeshKeyGrant,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT OR REPLACE INTO mesh_key_access (recipient_pubkey, ephemeral_pubkey, wrapped_privkey) VALUES (?, ?, ?)",
            params![
                grant.recipient_pubkey.to_vec(),
                grant.ephemeral_pubkey.to_vec(),
                grant.wrapped_privkey
            ],
        )
        .map_err(|e| {
            tracing::error!("Failed to insert mesh_key_access grant: {:?}", e);
            DatabaseError::InsertError
        })?;
    Ok(())
}

pub fn get_mesh_pubkey(conn: &rusqlite::Connection) -> Result<Option<[u8; 32]>, DatabaseError> {
    let blob: Option<Vec<u8>> = conn
        .query_row(
            "SELECT pubkey FROM mesh_key WHERE internal_id = 1",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;
    match blob {
        None => Ok(None),
        Some(v) => v
            .try_into()
            .map(Some)
            .map_err(|_| DatabaseError::RecallError),
    }
}

/// Fetch the mesh-key grant for a member pubkey (their unwrap capability).
pub fn get_mesh_grant(
    conn: &rusqlite::Connection,
    recipient_pubkey: &[u8; 32],
) -> Result<Option<MeshKeyGrant>, DatabaseError> {
    conn.query_row(
        "SELECT recipient_pubkey, ephemeral_pubkey, wrapped_privkey FROM mesh_key_access WHERE recipient_pubkey = ?",
        params![recipient_pubkey.to_vec()],
        |row| {
            let rec: Vec<u8> = row.get(0)?;
            let eph: Vec<u8> = row.get(1)?;
            let to_arr = |v: Vec<u8>, idx: usize| -> Result<[u8; 32], rusqlite::Error> {
                v.try_into().map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        idx,
                        rusqlite::types::Type::Blob,
                        "expected 32-byte X25519 key".into(),
                    )
                })
            };
            Ok(MeshKeyGrant {
                recipient_pubkey: to_arr(rec, 0)?,
                ephemeral_pubkey: to_arr(eph, 1)?,
                wrapped_privkey: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(|_| DatabaseError::RecallError)
}
