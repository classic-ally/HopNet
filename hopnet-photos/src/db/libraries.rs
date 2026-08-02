//! Shared-library membership DB layer (RFC-011 Phase 3).
//!
//! Two halves: consensus-apply mutations (run inside the handler's block
//! transaction, like `db::photos`) and read-side delta/backfill queries
//! for the convergence worker and the sidecar sync worker.
//!
//! Idempotency is chosen per re-delivery semantics: creations are plain
//! INSERTs (a duplicate is a deterministic ConflictError on every
//! validator), access grants are `INSERT OR IGNORE` (first committed
//! wrap wins — racing convergence workers must not churn a usable
//! wrap), and revocations are idempotent DELETEs.

use crate::envelopes::{LibraryBlobGrant, LibraryMetadataGrant};
use hopnet_common::CustomUUID;
use hopnet_projection::DatabaseError;
use rusqlite::{OptionalExtension, params};

// --- Mutations (consensus apply) ---

/// Insert the library row. Duplicate id → ConflictError (deterministic).
pub fn insert_library(
    db_tx: &rusqlite::Transaction,
    library_id: &CustomUUID,
    encrypted_name: &[u8],
    name_nonce: &[u8; 12],
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT INTO shared_libraries (id, encrypted_name, name_nonce)
             VALUES (?1, ?2, ?3)",
            params![library_id, encrypted_name, name_nonce],
        )
        .map_err(|e| {
            if is_unique_violation(&e) {
                tracing::warn!("create_shared_library: {} already exists", library_id);
                return DatabaseError::ConflictError;
            }
            tracing::error!("insert shared_libraries {} failed: {e}", library_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

pub fn insert_member(
    db_tx: &rusqlite::Transaction,
    library_id: &CustomUUID,
    user_id: i32,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT INTO shared_library_members (library_id, user_id) VALUES (?1, ?2)",
            params![library_id, user_id],
        )
        .map_err(|e| {
            if is_unique_violation(&e) {
                return DatabaseError::ConflictError;
            }
            tracing::error!("insert member ({}, {user_id}) failed: {e}", library_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

pub fn delete_member(
    db_tx: &rusqlite::Transaction,
    library_id: &CustomUUID,
    user_id: i32,
) -> Result<bool, DatabaseError> {
    let n = db_tx
        .execute(
            "DELETE FROM shared_library_members WHERE library_id = ?1 AND user_id = ?2",
            params![library_id, user_id],
        )
        .map_err(|e| {
            tracing::error!("delete member ({}, {user_id}) failed: {e}", library_id);
            DatabaseError::InsertError
        })?;
    Ok(n > 0)
}

pub fn is_member(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
    user_id: i32,
) -> Result<bool, DatabaseError> {
    exists_query(
        conn,
        "SELECT 1 FROM shared_library_members WHERE library_id = ?1 AND user_id = ?2",
        library_id,
        user_id,
    )
}

/// OR IGNORE: accept re-delivery and the invite-wrap-already-promoted
/// case are both no-ops.
pub fn insert_library_key(
    db_tx: &rusqlite::Transaction,
    library_id: &CustomUUID,
    user_id: i32,
    ephemeral_pubkey: &[u8; 32],
    wrapped_key: &[u8],
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT OR IGNORE INTO shared_library_keys
               (library_id, user_id, ephemeral_pubkey, wrapped_key)
             VALUES (?1, ?2, ?3, ?4)",
            params![library_id, user_id, ephemeral_pubkey, wrapped_key],
        )
        .map_err(|e| {
            tracing::error!("insert library key ({}, {user_id}) failed: {e}", library_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

pub fn delete_library_key(
    db_tx: &rusqlite::Transaction,
    library_id: &CustomUUID,
    user_id: i32,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "DELETE FROM shared_library_keys WHERE library_id = ?1 AND user_id = ?2",
            params![library_id, user_id],
        )
        .map_err(|e| {
            tracing::error!("delete library key ({}, {user_id}) failed: {e}", library_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// Plain INSERT — a duplicate invite is a deterministic ConflictError.
#[allow(clippy::too_many_arguments)]
pub fn insert_invite(
    db_tx: &rusqlite::Transaction,
    library_id: &CustomUUID,
    user_id: i32,
    invited_by: i32,
    operation_id: &CustomUUID,
    ephemeral_pubkey: &[u8; 32],
    wrapped_key: &[u8],
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT INTO shared_library_invites
               (library_id, user_id, invited_by, operation_id, ephemeral_pubkey, wrapped_key)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                library_id,
                user_id,
                invited_by,
                operation_id,
                ephemeral_pubkey,
                wrapped_key
            ],
        )
        .map_err(|e| {
            if is_unique_violation(&e) {
                return DatabaseError::ConflictError;
            }
            tracing::error!("insert invite ({}, {user_id}) failed: {e}", library_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// `(ephemeral_pubkey, wrapped_key)` — one X25519 key wrap as stored on a row.
pub type KeyWrap = ([u8; 32], Vec<u8>);

/// The invitee's library-key wrap parked on the invite row.
pub fn get_invite_wrap(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
    user_id: i32,
) -> Result<Option<KeyWrap>, DatabaseError> {
    conn.query_row(
        "SELECT ephemeral_pubkey, wrapped_key FROM shared_library_invites
         WHERE library_id = ?1 AND user_id = ?2",
        params![library_id, user_id],
        |r| {
            let eph: Vec<u8> = r.get(0)?;
            let wrapped: Vec<u8> = r.get(1)?;
            Ok((eph, wrapped))
        },
    )
    .optional()
    .map_err(|e| {
        tracing::error!("get invite wrap ({}, {user_id}) failed: {e}", library_id);
        DatabaseError::RecallError
    })?
    .map(|(eph, wrapped)| {
        let eph: [u8; 32] = eph.try_into().map_err(|_| {
            tracing::error!("invite wrap ({library_id}, {user_id}): malformed ephemeral pubkey");
            DatabaseError::RecallError
        })?;
        Ok((eph, wrapped))
    })
    .transpose()
}

pub fn delete_invite(
    db_tx: &rusqlite::Transaction,
    library_id: &CustomUUID,
    user_id: i32,
) -> Result<bool, DatabaseError> {
    let n = db_tx
        .execute(
            "DELETE FROM shared_library_invites WHERE library_id = ?1 AND user_id = ?2",
            params![library_id, user_id],
        )
        .map_err(|e| {
            tracing::error!("delete invite ({}, {user_id}) failed: {e}", library_id);
            DatabaseError::InsertError
        })?;
    Ok(n > 0)
}

pub fn is_invitee(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
    user_id: i32,
) -> Result<bool, DatabaseError> {
    exists_query(
        conn,
        "SELECT 1 FROM shared_library_invites WHERE library_id = ?1 AND user_id = ?2",
        library_id,
        user_id,
    )
}

/// OR IGNORE — first committed wrap wins; racing workers are harmless.
pub fn insert_metadata_access_grant(
    db_tx: &rusqlite::Transaction,
    grant: &LibraryMetadataGrant,
    user_id: i32,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT OR IGNORE INTO photo_metadata_access
               (photo_id, user_id, ephemeral_pubkey, encrypted_metadata_key)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                grant.photo_id,
                user_id,
                grant.ephemeral_pubkey,
                grant.encrypted_metadata_key
            ],
        )
        .map_err(|e| {
            tracing::error!(
                "grant metadata access ({}, {user_id}) failed: {e}",
                grant.photo_id
            );
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// OR IGNORE into the substrate's pubkey-keyed `blob_access`. The
/// recipient pubkey comes from the target's `users` row (resolved by the
/// handler), never from the wire.
pub fn insert_blob_access_grant(
    db_tx: &rusqlite::Transaction,
    grant: &LibraryBlobGrant,
    recipient_pubkey: &[u8; 32],
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT OR IGNORE INTO blob_access
               (blob_id, recipient_pubkey, ephemeral_pubkey, wrapped_key)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                grant.data_block_id,
                recipient_pubkey,
                grant.ephemeral_pubkey,
                grant.wrapped_key
            ],
        )
        .map_err(|e| {
            tracing::error!("grant blob access ({}) failed: {e}", grant.data_block_id);
            DatabaseError::InsertError
        })?;
    Ok(())
}

pub fn delete_metadata_access(
    db_tx: &rusqlite::Transaction,
    photo_id: &CustomUUID,
    user_id: i32,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "DELETE FROM photo_metadata_access WHERE photo_id = ?1 AND user_id = ?2",
            params![photo_id, user_id],
        )
        .map_err(|e| {
            tracing::error!("revoke metadata access ({photo_id}, {user_id}) failed: {e}");
            DatabaseError::InsertError
        })?;
    Ok(())
}

pub fn delete_blob_access(
    db_tx: &rusqlite::Transaction,
    data_block_id: &CustomUUID,
    recipient_pubkey: &[u8; 32],
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "DELETE FROM blob_access WHERE blob_id = ?1 AND recipient_pubkey = ?2",
            params![data_block_id, recipient_pubkey],
        )
        .map_err(|e| {
            tracing::error!("revoke blob access ({data_block_id}) failed: {e}");
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// UPSERT the per-user view-change signal: "your visibility into this
/// library changed at height h." Same height convention as
/// `photo_changes` (`current_height + 1` — the block being applied).
pub fn upsert_view_change(
    db_tx: &rusqlite::Transaction,
    user_id: i32,
    library_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    let height = hopnet_projection::current_height(db_tx)? + 1;
    db_tx
        .execute(
            "INSERT OR REPLACE INTO photo_view_changes (user_id, library_id, changed_at_height)
             VALUES (?1, ?2, ?3)",
            params![user_id, library_id, hopnet_common::height::height_to_db(height)],
        )
        .map_err(|e| {
            tracing::error!("upsert view change ({user_id}, {library_id}) failed: {e}");
            DatabaseError::InsertError
        })?;
    Ok(())
}

pub fn delete_view_change(
    db_tx: &rusqlite::Transaction,
    user_id: i32,
    library_id: &CustomUUID,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "DELETE FROM photo_view_changes WHERE user_id = ?1 AND library_id = ?2",
            params![user_id, library_id],
        )
        .map_err(|e| {
            tracing::error!("delete view change ({user_id}, {library_id}) failed: {e}");
            DatabaseError::InsertError
        })?;
    Ok(())
}

/// Target user's X25519 pubkey from the host-owned `users` table (photos
/// SQL may READ host tables — the ownership boundary is code, not
/// schema). None = unknown user.
pub fn user_x25519_pubkey(
    conn: &rusqlite::Connection,
    user_id: i32,
) -> Result<Option<[u8; 32]>, DatabaseError> {
    conn.query_row(
        "SELECT x25519_pubkey FROM users WHERE user_id = ?1",
        params![user_id],
        |r| r.get::<_, Vec<u8>>(0),
    )
    .optional()
    .map_err(|e| {
        tracing::error!("user pubkey lookup {user_id} failed: {e}");
        DatabaseError::RecallError
    })?
    .map(|blob| {
        <[u8; 32]>::try_from(blob).map_err(|_| {
            tracing::error!("user {user_id}: malformed x25519_pubkey");
            DatabaseError::RecallError
        })
    })
    .transpose()
}

// --- Grant-validation helpers (handler-side, deterministic) ---

/// A live (untombstoned) photo belonging to this library. Tombstoned
/// photos are never granted — invitees don't inherit the recovery window.
pub fn photo_in_library_live(
    conn: &rusqlite::Connection,
    photo_id: &CustomUUID,
    library_id: &CustomUUID,
) -> Result<bool, DatabaseError> {
    exists_query(
        conn,
        "SELECT 1 FROM photos WHERE id = ?1 AND library_id = ?2 AND deleted_at IS NULL",
        photo_id,
        library_id,
    )
}

/// The data block backs some resource of a live photo in this library.
pub fn block_in_library(
    conn: &rusqlite::Connection,
    data_block_id: &CustomUUID,
    library_id: &CustomUUID,
) -> Result<bool, DatabaseError> {
    exists_query(
        conn,
        "SELECT 1 FROM photo_resources r
         JOIN photos p ON p.id = r.photo_id
         WHERE r.data_block_id = ?1 AND p.library_id = ?2 AND p.deleted_at IS NULL",
        data_block_id,
        library_id,
    )
}

// --- Delta queries (convergence worker) ---

/// Libraries the user belongs to.
pub fn libraries_for_member(
    conn: &rusqlite::Connection,
    user_id: i32,
) -> Result<Vec<CustomUUID>, DatabaseError> {
    collect_query(
        conn,
        "SELECT library_id FROM shared_library_members WHERE user_id = ?1 ORDER BY library_id",
        params![user_id],
        |r| r.get(0),
    )
}

/// The assertion set: members ∪ pending invitees, with pubkeys — who
/// SHOULD hold access rows for this library's photos.
pub fn assertion_targets(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
) -> Result<Vec<(i32, [u8; 32])>, DatabaseError> {
    let rows: Vec<(i32, Vec<u8>)> = collect_query(
        conn,
        "SELECT t.user_id, u.x25519_pubkey FROM (
             SELECT user_id FROM shared_library_members WHERE library_id = ?1
             UNION
             SELECT user_id FROM shared_library_invites WHERE library_id = ?1
         ) t JOIN users u ON u.user_id = t.user_id
         ORDER BY t.user_id",
        params![library_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    rows.into_iter()
        .map(|(uid, blob)| {
            let pk = <[u8; 32]>::try_from(blob).map_err(|_| {
                tracing::error!("assertion target {uid}: malformed x25519_pubkey");
                DatabaseError::RecallError
            })?;
            Ok((uid, pk))
        })
        .collect()
}

/// Live library photos the target has no metadata wrap for yet.
pub fn missing_metadata_grants(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
    target_user: i32,
    limit: u32,
) -> Result<Vec<CustomUUID>, DatabaseError> {
    collect_query(
        conn,
        "SELECT p.id FROM photos p
         WHERE p.library_id = ?1 AND p.deleted_at IS NULL
           AND NOT EXISTS (SELECT 1 FROM photo_metadata_access a
                           WHERE a.photo_id = p.id AND a.user_id = ?2)
         ORDER BY p.id LIMIT ?3",
        params![library_id, target_user, limit],
        |r| r.get(0),
    )
}

/// (photo_id, data_block_id) pairs of live library resources the target
/// pubkey has no blob wrap for yet.
pub fn missing_blob_grants(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
    target_pubkey: &[u8; 32],
    limit: u32,
) -> Result<Vec<(CustomUUID, CustomUUID)>, DatabaseError> {
    collect_query(
        conn,
        "SELECT r.photo_id, r.data_block_id FROM photo_resources r
         JOIN photos p ON p.id = r.photo_id
         WHERE p.library_id = ?1 AND p.deleted_at IS NULL
           AND NOT EXISTS (SELECT 1 FROM blob_access b
                           WHERE b.blob_id = r.data_block_id
                             AND b.recipient_pubkey = ?2)
         ORDER BY r.photo_id, r.resource_type LIMIT ?3",
        params![library_id, target_pubkey, limit],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
}

/// Users holding metadata-access rows on this library's photos while
/// being neither member nor invitee — the revoke set.
pub fn stale_access_users(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
) -> Result<Vec<i32>, DatabaseError> {
    collect_query(
        conn,
        "SELECT DISTINCT a.user_id FROM photo_metadata_access a
         JOIN photos p ON p.id = a.photo_id
         WHERE p.library_id = ?1
           AND a.user_id NOT IN
               (SELECT user_id FROM shared_library_members WHERE library_id = ?1)
           AND a.user_id NOT IN
               (SELECT user_id FROM shared_library_invites WHERE library_id = ?1)
         ORDER BY a.user_id",
        params![library_id],
        |r| r.get(0),
    )
}

/// The concrete rows to revoke for one stale user: photo ids with a
/// metadata wrap, and this library's data blocks the user's pubkey holds
/// a blob wrap for.
pub fn stale_access_rows(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
    user_id: i32,
    user_pubkey: &[u8; 32],
    limit: u32,
) -> Result<(Vec<CustomUUID>, Vec<CustomUUID>), DatabaseError> {
    let photos = collect_query(
        conn,
        "SELECT p.id FROM photos p
         JOIN photo_metadata_access a ON a.photo_id = p.id AND a.user_id = ?2
         WHERE p.library_id = ?1
         ORDER BY p.id LIMIT ?3",
        params![library_id, user_id, limit],
        |r| r.get(0),
    )?;
    let blocks = collect_query(
        conn,
        "SELECT DISTINCT r.data_block_id FROM photo_resources r
         JOIN photos p ON p.id = r.photo_id
         JOIN blob_access b ON b.blob_id = r.data_block_id AND b.recipient_pubkey = ?2
         WHERE p.library_id = ?1
         ORDER BY r.data_block_id LIMIT ?3",
        params![library_id, user_pubkey, limit],
        |r| r.get(0),
    )?;
    Ok((photos, blocks))
}

/// A photo's metadata-key wrap, tagged with the photo it belongs to.
pub type PhotoMetadataWrap = (CustomUUID, [u8; 32], Vec<u8>);

/// The worker's own unwrap inputs: its metadata-access rows for a photo
/// set. Photos the caller has no wrap for are absent from the result
/// (another member's worker covers them).
pub fn own_metadata_wraps(
    conn: &rusqlite::Connection,
    user_id: i32,
    photo_ids: &[CustomUUID],
) -> Result<Vec<PhotoMetadataWrap>, DatabaseError> {
    let mut out = Vec::with_capacity(photo_ids.len());
    let mut stmt = conn
        .prepare(
            "SELECT ephemeral_pubkey, encrypted_metadata_key FROM photo_metadata_access
             WHERE photo_id = ?1 AND user_id = ?2",
        )
        .map_err(|e| {
            tracing::error!("own_metadata_wraps prepare: {e}");
            DatabaseError::RecallError
        })?;
    for photo_id in photo_ids {
        let row: Option<(Vec<u8>, Vec<u8>)> = stmt
            .query_row(params![photo_id, user_id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .map_err(|e| {
                tracing::error!("own_metadata_wraps {photo_id}: {e}");
                DatabaseError::RecallError
            })?;
        if let Some((eph, wrapped)) = row {
            let eph: [u8; 32] = eph.try_into().map_err(|_| {
                tracing::error!("own wrap {photo_id}: malformed ephemeral pubkey");
                DatabaseError::RecallError
            })?;
            out.push((photo_id.clone(), eph, wrapped));
        }
    }
    Ok(out)
}

// --- Backfill queries (sidecar sync worker) ---

/// Per-user view-change signals: which libraries changed for this user,
/// and at what height. Consumed by the sync worker's membership-diff
/// pre-phase; a height above the sidecar's stored value triggers a
/// targeted re-backfill.
pub fn read_view_changes(
    conn: &rusqlite::Connection,
    user_id: i32,
) -> Result<Vec<(CustomUUID, i64)>, DatabaseError> {
    collect_query(
        conn,
        "SELECT library_id, changed_at_height FROM photo_view_changes WHERE user_id = ?1",
        params![user_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
}

/// Paged library backfill for a joined member: live photos of the library
/// the user holds a metadata wrap for, in `ChangeRow` shape so the
/// sidecar's ordinary batch apply consumes it unchanged. Keyset-paged by
/// photo id; `changed_at_height` carries 0 (the backfill is outside the
/// height cursor — concurrent genuine changes arrive via photo_changes).
pub fn library_photos_for_member(
    conn: &rusqlite::Connection,
    library_id: &CustomUUID,
    user_id: i32,
    after_photo_id: Option<&CustomUUID>,
    limit: u32,
) -> Result<Vec<super::photos::ChangeRow>, DatabaseError> {
    let after = after_photo_id.map(|id| id.to_string()).unwrap_or_default();
    let mut stmt = conn
        .prepare(
            "SELECT p.id, 0,
                    p.id, p.library_id, p.uploaded_by, p.encrypted_metadata,
                    p.metadata_nonce, p.deleted_at, p.deleted_by,
                    pma.ephemeral_pubkey, pma.encrypted_metadata_key
             FROM photos p
             JOIN photo_metadata_access pma
                 ON pma.photo_id = p.id AND pma.user_id = ?2
             WHERE p.library_id = ?1 AND p.id > ?3
             ORDER BY p.id ASC LIMIT ?4",
        )
        .map_err(|e| {
            tracing::error!("library_photos_for_member prepare: {e}");
            DatabaseError::RecallError
        })?;
    let rows = stmt
        .query_map(
            params![library_id, user_id, after, limit],
            super::photos::map_change_row,
        )
        .map_err(|e| {
            tracing::error!("library_photos_for_member: {e}");
            DatabaseError::RecallError
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            tracing::error!("library_photos_for_member collect: {e}");
            DatabaseError::RecallError
        })?;
    Ok(rows)
}

// --- Private helpers ---

fn is_unique_violation(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(err, _)
            if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                || err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
    )
}

fn exists_query(
    conn: &rusqlite::Connection,
    sql: &str,
    a: impl rusqlite::ToSql,
    b: impl rusqlite::ToSql,
) -> Result<bool, DatabaseError> {
    conn.query_row(sql, params![a, b], |_| Ok(()))
        .optional()
        .map(|r| r.is_some())
        .map_err(|e| {
            tracing::error!("exists query failed: {e}");
            DatabaseError::RecallError
        })
}

fn collect_query<T>(
    conn: &rusqlite::Connection,
    sql: &str,
    params: impl rusqlite::Params,
    map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>, DatabaseError> {
    let mut stmt = conn.prepare(sql).map_err(|e| {
        tracing::error!("collect prepare: {e}");
        DatabaseError::RecallError
    })?;
    let rows = stmt
        .query_map(params, map)
        .map_err(|e| {
            tracing::error!("collect query: {e}");
            DatabaseError::RecallError
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            tracing::error!("collect rows: {e}");
            DatabaseError::RecallError
        })?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE users (user_id INTEGER PRIMARY KEY, x25519_pubkey BLOB);
             CREATE TABLE consensus_meta (key TEXT PRIMARY KEY, value BLOB);",
        )
        .unwrap();
        hopnet_storage::store::install_schema(&conn).unwrap();
        super::super::install_schema(&conn).unwrap();
        for uid in 1..=3 {
            conn.execute(
                "INSERT INTO users (user_id, x25519_pubkey) VALUES (?1, ?2)",
                params![uid, vec![uid as u8; 32]],
            )
            .unwrap();
        }
        conn
    }

    fn add_library(conn: &rusqlite::Connection, id: &str) {
        conn.execute(
            "INSERT INTO shared_libraries (id, encrypted_name, name_nonce)
             VALUES (?1, x'00', x'00')",
            [id],
        )
        .unwrap();
    }

    fn add_photo(conn: &rusqlite::Connection, id: &str, lib: &str, deleted: bool) {
        conn.execute(
            "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce, deleted_at)
             VALUES (?1, ?2, 1, x'00', x'00', ?3)",
            params![id, lib, deleted.then_some("2026-01-01T00:00:00Z")],
        )
        .unwrap();
    }

    fn add_resource(conn: &rusqlite::Connection, photo: &str, block: &str) {
        conn.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, file_size)
             VALUES (?1, x'00', 1, 0, 10)",
            [block],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photo_resources (photo_id, resource_type, data_block_id)
             VALUES (?1, 0, ?2)",
            params![photo, block],
        )
        .unwrap();
    }

    fn add_meta_access(conn: &rusqlite::Connection, photo: &str, user: i32) {
        conn.execute(
            "INSERT INTO photo_metadata_access (photo_id, user_id, ephemeral_pubkey, encrypted_metadata_key)
             VALUES (?1, ?2, x'00', x'00')",
            params![photo, user],
        )
        .unwrap();
    }

    const LIB: &str = "00000000-0000-0000-0000-0000000000a1";
    const P1: &str = "00000000-0000-0000-0000-0000000000b1";
    const P2: &str = "00000000-0000-0000-0000-0000000000b2";

    // Should: include members and pending invitees, with their pubkeys,
    // in the assertion set; exclude everyone else.
    // Impact: this set IS the convergence target — an over-broad set
    // grants access to strangers, an under-broad one starves invitees.
    #[test]
    fn assertion_targets_are_members_union_invitees() {
        let conn = fixture();
        add_library(&conn, LIB);
        let lib: CustomUUID = LIB.parse().unwrap();
        conn.execute(
            "INSERT INTO shared_library_members (library_id, user_id) VALUES (?1, 1)",
            params![lib],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO shared_library_invites
               (library_id, user_id, invited_by, operation_id, ephemeral_pubkey, wrapped_key)
             VALUES (?1, 2, 1, 'op', x'00', x'00')",
            params![lib],
        )
        .unwrap();

        let targets = assertion_targets(&conn, &lib).unwrap();
        assert_eq!(
            targets,
            vec![(1, [1u8; 32]), (2, [2u8; 32])],
            "member + invitee, pubkeys from users, user 3 excluded"
        );
    }

    // Should: list live photos the target lacks a metadata wrap for.
    // Should not: grant tombstoned photos to anyone, or re-list photos
    // the target already has a wrap for.
    #[test]
    fn missing_metadata_grants_excludes_tombstones_and_covered() {
        let conn = fixture();
        add_library(&conn, LIB);
        let lib: CustomUUID = LIB.parse().unwrap();
        add_photo(&conn, P1, LIB, false);
        add_photo(&conn, P2, LIB, false);
        add_photo(&conn, "00000000-0000-0000-0000-0000000000b3", LIB, true); // tombstoned
        add_meta_access(&conn, P1, 2); // already covered for user 2

        let missing = missing_metadata_grants(&conn, &lib, 2, 500).unwrap();
        assert_eq!(missing, vec![P2.parse::<CustomUUID>().unwrap()]);
    }

    // Should: cap the delta at the batch limit.
    #[test]
    fn missing_grants_respect_batch_limit() {
        let conn = fixture();
        add_library(&conn, LIB);
        let lib: CustomUUID = LIB.parse().unwrap();
        for i in 0..5 {
            add_photo(
                &conn,
                &format!("00000000-0000-0000-0000-0000000000c{i}"),
                LIB,
                false,
            );
        }
        assert_eq!(missing_metadata_grants(&conn, &lib, 2, 3).unwrap().len(), 3);
    }

    // Should: report users holding access rows who are neither member nor
    // invitee, with the exact rows to revoke.
    // Impact: the revoke half of convergence — a member or invitee
    // appearing here would be stealth-revoked by their own peers' workers.
    #[test]
    fn stale_access_users_excludes_members_and_invitees() {
        let conn = fixture();
        add_library(&conn, LIB);
        let lib: CustomUUID = LIB.parse().unwrap();
        add_photo(&conn, P1, LIB, false);
        add_resource(&conn, P1, "00000000-0000-0000-0000-0000000000e1");
        conn.execute(
            "INSERT INTO shared_library_members (library_id, user_id) VALUES (?1, 1)",
            params![lib],
        )
        .unwrap();
        add_meta_access(&conn, P1, 1); // member — must not appear
        add_meta_access(&conn, P1, 3); // kicked user — stale
        conn.execute(
            "INSERT INTO blob_access (blob_id, recipient_pubkey, ephemeral_pubkey, wrapped_key)
             VALUES (?1, ?2, x'00', x'00')",
            params![
                "00000000-0000-0000-0000-0000000000e1"
                    .parse::<CustomUUID>()
                    .unwrap(),
                [3u8; 32]
            ],
        )
        .unwrap();

        assert_eq!(stale_access_users(&conn, &lib).unwrap(), vec![3]);
        let (photos, blocks) = stale_access_rows(&conn, &lib, 3, &[3u8; 32], 500).unwrap();
        assert_eq!(photos, vec![P1.parse::<CustomUUID>().unwrap()]);
        assert_eq!(
            blocks,
            vec![
                "00000000-0000-0000-0000-0000000000e1"
                    .parse::<CustomUUID>()
                    .unwrap()
            ]
        );
    }

    // Should: page the member backfill by photo id and only return photos
    // the user holds a wrap for.
    // Impact: this is the join-time rematerialization source — the sidecar
    // hydrates a library from it without touching the photo_changes cursor.
    #[test]
    fn library_backfill_pages_and_requires_wrap() {
        let conn = fixture();
        add_library(&conn, LIB);
        let lib: CustomUUID = LIB.parse().unwrap();
        add_photo(&conn, P1, LIB, false);
        add_photo(&conn, P2, LIB, false);
        add_meta_access(&conn, P1, 2);
        add_meta_access(&conn, P2, 2);
        add_photo(&conn, "00000000-0000-0000-0000-0000000000b3", LIB, false); // no wrap yet

        let page1 = library_photos_for_member(&conn, &lib, 2, None, 1).unwrap();
        assert_eq!(page1.len(), 1);
        assert_eq!(page1[0].photo_id, P1.parse::<CustomUUID>().unwrap());
        let page2 =
            library_photos_for_member(&conn, &lib, 2, Some(&page1[0].photo_id), 500).unwrap();
        assert_eq!(page2.len(), 1, "unwrapped photo must be absent");
        assert_eq!(page2[0].photo_id, P2.parse::<CustomUUID>().unwrap());
    }

    // Should: report the latest view-change height per library and allow
    // the remove path to clear it.
    #[test]
    fn view_change_upsert_read_delete_round_trip() {
        let conn = fixture();
        add_library(&conn, LIB);
        let lib: CustomUUID = LIB.parse().unwrap();
        conn.execute(
            "INSERT INTO consensus_meta (key, value) VALUES ('height', ?1)",
            params![7i64.to_le_bytes().to_vec()],
        )
        .ok(); // height source varies; fall back to manual insert below
        let tx = conn.unchecked_transaction().unwrap();
        // Bypass current_height coupling: insert directly at a known height.
        tx.execute(
            "INSERT OR REPLACE INTO photo_view_changes (user_id, library_id, changed_at_height)
             VALUES (2, ?1, 41)",
            params![lib],
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            read_view_changes(&conn, 2).unwrap(),
            vec![(lib.clone(), 41)]
        );
        let tx = conn.unchecked_transaction().unwrap();
        delete_view_change(&tx, 2, &lib).unwrap();
        tx.commit().unwrap();
        assert!(read_view_changes(&conn, 2).unwrap().is_empty());
    }

    // Should: gate grant validation on library membership of the photo and
    // its tombstone state.
    #[test]
    fn grant_validation_helpers() {
        let conn = fixture();
        add_library(&conn, LIB);
        add_library(&conn, "00000000-0000-0000-0000-0000000000a2");
        let lib: CustomUUID = LIB.parse().unwrap();
        let other: CustomUUID = "00000000-0000-0000-0000-0000000000a2".parse().unwrap();
        add_photo(&conn, P1, LIB, false);
        add_photo(&conn, P2, LIB, true);
        add_resource(&conn, P1, "00000000-0000-0000-0000-0000000000e1");
        let p1: CustomUUID = P1.parse().unwrap();
        let p2: CustomUUID = P2.parse().unwrap();
        let blk: CustomUUID = "00000000-0000-0000-0000-0000000000e1".parse().unwrap();

        assert!(photo_in_library_live(&conn, &p1, &lib).unwrap());
        assert!(
            !photo_in_library_live(&conn, &p2, &lib).unwrap(),
            "tombstoned"
        );
        assert!(
            !photo_in_library_live(&conn, &p1, &other).unwrap(),
            "wrong library"
        );
        assert!(block_in_library(&conn, &blk, &lib).unwrap());
        assert!(!block_in_library(&conn, &blk, &other).unwrap());
    }
}
