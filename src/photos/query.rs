use hopnet_photos_core::dispatch::{
    EncryptedPhotoState, LibraryMember, LibraryMembership, PhotoChange, SyncBatch,
};

use hopnet_photos::db::photos;

/// Batch size for sync queries — well below SQLITE_MAX_VARIABLE_NUMBER.
pub(crate) const SYNC_BATCH_LIMIT: u64 = 500;

fn select_high_water_mark(
    row_count: usize,
    last_height: Option<i64>,
    since_height: u64,
    chain_height: u64,
) -> u64 {
    if row_count as u64 >= SYNC_BATCH_LIMIT {
        last_height
            .map(|height| height as u64)
            .unwrap_or(since_height)
    } else {
        chain_height
    }
}

pub fn read_photo_changes(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
    since_height: u64,
) -> Result<SyncBatch, String> {
    let conn = pool.get().map_err(|e| format!("pool: {e}"))?;

    let chain_height =
        photos::read_current_height(&conn).map_err(|e| format!("current_height: {e:?}"))?;

    let rows = photos::query_changes(&conn, user_id, since_height, SYNC_BATCH_LIMIT)
        .map_err(|e| format!("query_changes: {e:?}"))?;

    let existing_ids: Vec<&hopnet_common::CustomUUID> = rows
        .iter()
        .filter(|r| r.row_exists)
        .map(|r| &r.photo_id)
        .collect();

    let resources = photos::query_resources(&conn, &existing_ids)
        .map_err(|e| format!("query_resources: {e:?}"))?;

    // query_changes may extend past the nominal limit to finish every row at
    // the boundary height. Such a batch is still truncated with respect to
    // later heights, so it must advance only to the last consumed height.
    let high_water_mark = select_high_water_mark(
        rows.len(),
        rows.last().map(|row| row.changed_at_height),
        since_height,
        chain_height,
    );

    let changes: Vec<PhotoChange> = rows
        .into_iter()
        .map(|r| {
            let state = if r.row_exists {
                let uploaded_by = r.uploaded_by.unwrap_or_else(|| {
                    tracing::warn!(
                        photo_id = %r.photo_id,
                        "change row missing uploaded_by; using 0"
                    );
                    0
                });
                let encrypted_metadata = r.encrypted_metadata.unwrap_or_else(|| {
                    tracing::warn!(
                        photo_id = %r.photo_id,
                        "change row missing encrypted_metadata; using empty"
                    );
                    Vec::new()
                });
                let metadata_nonce = r.metadata_nonce.unwrap_or_else(|| {
                    tracing::warn!(
                        photo_id = %r.photo_id,
                        "change row missing metadata_nonce; using zero nonce"
                    );
                    [0u8; 12]
                });
                Some(EncryptedPhotoState {
                    library_id: r.library_id,
                    uploaded_by,
                    encrypted_metadata,
                    metadata_nonce,
                    deleted_at: r.deleted_at,
                    deleted_by: r.deleted_by,
                    ephemeral_pubkey: r.eph_pubkey,
                    encrypted_metadata_key: r.enc_meta_key,
                    resources: resources.get(&r.photo_id).cloned().unwrap_or_default(),
                })
            } else {
                None
            };
            PhotoChange {
                photo_id: r.photo_id,
                changed_at_height: r.changed_at_height as u64,
                state,
            }
        })
        .collect();

    Ok(SyncBatch {
        changes,
        high_water_mark,
    })
}

fn pubkey_from_blob(blob: Vec<u8>) -> Result<hopnet_storage::x25519_dalek::PublicKey, String> {
    let arr: [u8; 32] = blob
        .try_into()
        .map_err(|_| "x25519_pubkey not 32 bytes".to_string())?;
    Ok(hopnet_storage::x25519_dalek::PublicKey::from(arr))
}

/// Recipients for a publish by `user_id`. `library_id == None` is the
/// personal library: exactly the acting user. For shared libraries the
/// member set comes from `shared_library_members`; an unknown library yields
/// an empty set, which the publisher rejects as NoRecipients.
pub fn read_library_membership(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
    library_id: Option<hopnet_common::CustomUUID>,
) -> Result<LibraryMembership, String> {
    let conn = pool.get().map_err(|e| format!("pool: {e}"))?;

    let members = match &library_id {
        None => {
            let blob: Vec<u8> = conn
                .query_row(
                    "SELECT x25519_pubkey FROM users WHERE user_id = ?",
                    rusqlite::params![user_id],
                    |row| row.get(0),
                )
                .map_err(|e| format!("uploader pubkey: {e:?}"))?;
            vec![LibraryMember {
                user_id,
                pubkey: pubkey_from_blob(blob)?,
            }]
        }
        Some(lib) => {
            let mut stmt = conn
                .prepare(
                    "SELECT m.user_id, u.x25519_pubkey
                     FROM shared_library_members m
                     JOIN users u ON u.user_id = m.user_id
                     WHERE m.library_id = ?
                     ORDER BY m.user_id",
                )
                .map_err(|e| format!("prepare members: {e:?}"))?;
            let rows = stmt
                .query_map(rusqlite::params![lib], |row| {
                    Ok((row.get::<_, i32>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|e| format!("members: {e:?}"))?;
            let mut members = Vec::new();
            for row in rows {
                let (uid, blob) = row.map_err(|e| format!("member row: {e:?}"))?;
                members.push(LibraryMember {
                    user_id: uid,
                    pubkey: pubkey_from_blob(blob)?,
                });
            }
            members
        }
    };

    Ok(LibraryMembership {
        uploaded_by: user_id,
        members,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extended_boundary_batch_does_not_skip_later_heights() {
        assert_eq!(select_high_water_mark(510, Some(7), 0, 10), 7);
    }

    #[test]
    fn complete_batch_advances_to_chain_tip() {
        assert_eq!(select_high_water_mark(499, Some(7), 0, 10), 10);
    }
}
