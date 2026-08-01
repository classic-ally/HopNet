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

    let changes = rows_to_changes(&conn, rows)?;

    Ok(SyncBatch {
        changes,
        high_water_mark,
    })
}

/// Map DB change rows into wire `PhotoChange`s, batch-fetching resources.
/// Shared by the cursor sync and the library-backfill path.
fn rows_to_changes(
    conn: &rusqlite::Connection,
    rows: Vec<hopnet_photos::db::photos::ChangeRow>,
) -> Result<Vec<PhotoChange>, String> {
    let existing_ids: Vec<&hopnet_common::CustomUUID> = rows
        .iter()
        .filter(|r| r.row_exists)
        .map(|r| &r.photo_id)
        .collect();
    let resources = photos::query_resources(conn, &existing_ids)
        .map_err(|e| format!("query_resources: {e:?}"))?;

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

    Ok(changes)
}

/// The sync worker's membership-diff inputs: the user's current library
/// memberships and their per-library view-change signals.
pub fn read_membership_state(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
) -> Result<
    (
        Vec<hopnet_common::CustomUUID>,
        Vec<(hopnet_common::CustomUUID, i64)>,
    ),
    String,
> {
    let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    let memberships = hopnet_photos::db::libraries::libraries_for_member(&conn, user_id)
        .map_err(|e| format!("memberships: {e:?}"))?;
    let signals = hopnet_photos::db::libraries::read_view_changes(&conn, user_id)
        .map_err(|e| format!("view changes: {e:?}"))?;
    Ok((memberships, signals))
}

/// One page of the join-time library backfill: photos of `library_id` the
/// user holds a metadata wrap for, in `PhotoChange` shape (heights carry
/// 0 — the backfill lives outside the cursor). Returns the page plus the
/// last photo id for keyset continuation; a short page ends the loop.
pub fn read_library_backfill(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
    library_id: &hopnet_common::CustomUUID,
    after: Option<&hopnet_common::CustomUUID>,
) -> Result<(Vec<PhotoChange>, Option<hopnet_common::CustomUUID>), String> {
    let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    let rows = hopnet_photos::db::libraries::library_photos_for_member(
        &conn,
        library_id,
        user_id,
        after,
        SYNC_BATCH_LIMIT as u32,
    )
    .map_err(|e| format!("library backfill: {e:?}"))?;
    let last = rows.last().map(|r| r.photo_id.clone());
    let changes = rows_to_changes(&conn, rows)?;
    Ok((changes, last))
}

/// Everything the content route needs to serve one resource: the blob id
/// (ETag), the caller's key wrap, and the manifest (fragments + file_size).
pub struct ResourceGrant {
    pub data_block_id: hopnet_common::CustomUUID,
    pub access: hopnet_storage::BlobAccess,
    pub manifest: hopnet_storage::store::BlobManifest,
}

#[derive(Debug)]
pub enum ResourceGrantError {
    /// Photo or resource type doesn't exist (or was hard-deleted).
    NotFound,
    /// No blob_access wrap for this user — the wrap is the read grant.
    Forbidden,
    Internal(String),
}

impl From<ResourceGrantError> for axum::http::StatusCode {
    fn from(e: ResourceGrantError) -> Self {
        match e {
            ResourceGrantError::NotFound => axum::http::StatusCode::NOT_FOUND,
            ResourceGrantError::Forbidden => axum::http::StatusCode::FORBIDDEN,
            ResourceGrantError::Internal(msg) => {
                tracing::error!("resource grant failed: {msg}");
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

/// Resolve + authorize one photo resource for `user_id`. Sync (rusqlite);
/// callers run it under spawn_blocking. 404-vs-403 reveals photo existence
/// to authenticated users — acceptable, ids are unguessable UUIDv7.
pub fn read_resource_grant(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
    photo_id: &hopnet_common::CustomUUID,
    kind: hopnet_photos_core::asset::ResourceKind,
) -> Result<ResourceGrant, ResourceGrantError> {
    let conn = pool
        .get()
        .map_err(|e| ResourceGrantError::Internal(format!("pool: {e}")))?;

    let data_block_id = match photos::lookup_resource_block_authz(
        &conn,
        photo_id,
        kind.as_wire(),
        user_id,
    )
    .map_err(|e| ResourceGrantError::Internal(format!("resource lookup: {e:?}")))?
    {
        photos::ResourceBlockLookup::Found(id) => id,
        photos::ResourceBlockLookup::NotFound => return Err(ResourceGrantError::NotFound),
        // Shared photo, no membership: the wrap row alone (pre-staged
        // invitee grant, or a kicked member's not-yet-revoked row) must
        // not serve bytes.
        photos::ResourceBlockLookup::NotMember => return Err(ResourceGrantError::Forbidden),
    };

    let access = photos::get_blob_access_for_user(&conn, &data_block_id, user_id)
        .map_err(|e| ResourceGrantError::Internal(format!("access lookup: {e:?}")))?
        .ok_or(ResourceGrantError::Forbidden)?;

    // Asset validation rejects zero-byte resources, so a resource row whose
    // blob has no manifest is corruption, not a client-visible 404.
    let manifest = hopnet_storage::store::blob_manifest(&conn, &data_block_id)
        .map_err(|e| ResourceGrantError::Internal(format!("manifest: {e:?}")))?
        .ok_or_else(|| {
            ResourceGrantError::Internal(format!(
                "resource row references unknown blob {data_block_id}"
            ))
        })?;

    Ok(ResourceGrant {
        data_block_id,
        access,
        manifest,
    })
}

/// Committed-state probe for the thin-client confirm-then-retry contract
/// (publisher idempotency: after an ambiguous submit failure the client MUST
/// query committed state before re-submitting the same photo_id). Reads the
/// CONSENSUS `photos` table deliberately — the per-user sidecar requires
/// opt-in enablement (428 on the read routes), which a headless daemon must
/// not depend on. Returns `Some(uploaded_by)` only when the row exists AND
/// belongs to `user_id`; anything else is None (no existence leak).
pub fn read_photo_committed(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
    photo_id: &hopnet_common::CustomUUID,
) -> Result<Option<i32>, String> {
    use rusqlite::OptionalExtension;
    let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    let uploaded_by: Option<i32> = conn
        .query_row(
            "SELECT uploaded_by FROM photos WHERE id = ?",
            rusqlite::params![photo_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| format!("photo lookup: {e:?}"))?;
    Ok(uploaded_by.filter(|&owner| owner == user_id))
}

/// By-fingerprint committed lookup for the thin-client resolve route
/// (cross-device dedupe / remote adoption). Same contract shape as
/// [`read_photo_committed`]: ownership filtered Rust-side, and deliberately
/// NO deleted_at filter — a tombstoned row still holds its fingerprint in
/// the partial UNIQUE index until hard-delete, so a re-publish would fail
/// deterministically anyway; resolving it lets the daemon adopt instead of
/// burning retries. Personal scope only in v1 (`library_id IS NULL`,
/// matching the fp_personal partial index).
pub fn read_photo_by_fingerprint(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
    fingerprint_hex: &str,
) -> Result<Option<hopnet_common::CustomUUID>, String> {
    use rusqlite::OptionalExtension;
    let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    let row: Option<(hopnet_common::CustomUUID, i32)> = conn
        .query_row(
            "SELECT id, uploaded_by FROM photos
             WHERE cloud_fingerprint = ? AND library_id IS NULL",
            rusqlite::params![fingerprint_hex],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|e| format!("fingerprint lookup: {e:?}"))?;
    Ok(row.and_then(|(id, owner)| (owner == user_id).then_some(id)))
}

/// Current ingress responsibility holder for a user's personal scope.
pub fn read_ingress_responsibility(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
) -> Result<Option<hopnet_common::CustomUUID>, String> {
    use rusqlite::OptionalExtension;
    let conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    conn.query_row(
        "SELECT device_id FROM photo_ingress_responsibility WHERE user_id = ?",
        rusqlite::params![user_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(|e| format!("responsibility lookup: {e:?}"))
}

fn pubkey_from_blob(blob: Vec<u8>) -> Result<hopnet_storage::x25519_dalek::PublicKey, String> {
    let arr: [u8; 32] = blob
        .try_into()
        .map_err(|_| "x25519_pubkey not 32 bytes".to_string())?;
    Ok(hopnet_storage::x25519_dalek::PublicKey::from(arr))
}

/// Recipients for a publish by `user_id`. `library_id == None` is the
/// personal library: exactly the acting user. For shared libraries the
/// recipient set is members UNION pending invitees — publish-time fan-out
/// covers invitees so the convergence worker only backfills, never races
/// new adds (the live-link lesson from drive). The caller must belong to
/// the library (member or invitee); membership lists are real data now.
/// An unknown library yields an empty set, which the publisher rejects as
/// NoRecipients.
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
            let caller_belongs: bool = conn
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM shared_library_members
                                    WHERE library_id = ?1 AND user_id = ?2)
                         OR EXISTS (SELECT 1 FROM shared_library_invites
                                    WHERE library_id = ?1 AND user_id = ?2)",
                    rusqlite::params![lib, user_id],
                    |row| row.get(0),
                )
                .map_err(|e| format!("caller membership: {e:?}"))?;
            if !caller_belongs {
                return Err(format!("user {user_id} is not a member of library {lib}"));
            }
            let mut stmt = conn
                .prepare(
                    "SELECT t.user_id, u.x25519_pubkey FROM (
                         SELECT user_id FROM shared_library_members WHERE library_id = ?1
                         UNION
                         SELECT user_id FROM shared_library_invites WHERE library_id = ?1
                     ) t JOIN users u ON u.user_id = t.user_id
                     ORDER BY t.user_id",
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

    fn test_pool() -> r2d2::Pool<r2d2_sqlite::SqliteConnectionManager> {
        let manager = r2d2_sqlite::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        pool
    }

    fn insert_user(
        pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
        user_id: i32,
        pubkey: &hopnet_storage::x25519_dalek::PublicKey,
    ) {
        pool.get()
            .unwrap()
            .execute(
                "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, \
                 encrypted_privkey, key_salt) VALUES (?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    user_id,
                    format!("user{user_id}"),
                    vec![0u8; 32],
                    pubkey.as_bytes().to_vec(),
                    vec![0u8; 32],
                    vec![0u8; 16],
                ],
            )
            .unwrap();
    }

    // Impact: pins the full write-to-read contract across the storage put,
    // the consensus apply, and the grant layer — bytes published for a user
    // must come back byte-identical through the serving path.
    // Should: resolve, authorize, unwrap, and stream back exactly the
    // published plaintext, and honor inclusive byte ranges.
    // Should not: grant access to a wrap-less user, an unpublished resource
    // kind, or an unknown photo.
    #[tokio::test(flavor = "multi_thread")]
    async fn publisher_to_serve_round_trip() {
        use hopnet_photos_core::asset::ResourceKind;
        use std::io::Cursor;
        use tokio_stream::StreamExt;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let fragments_dir = temp_dir.path().to_str().unwrap().to_string();
        let pool = test_pool();

        let owner_secret = hopnet_storage::x25519_dalek::StaticSecret::from([0xD4; 32]);
        let owner_pubkey = hopnet_storage::x25519_dalek::PublicKey::from(&owner_secret);
        insert_user(&pool, 7, &owner_pubkey);
        let bystander_pubkey = hopnet_storage::x25519_dalek::PublicKey::from(
            &hopnet_storage::x25519_dalek::StaticSecret::from([0xE5; 32]),
        );
        insert_user(&pool, 8, &bystander_pubkey);

        let plaintext: Vec<u8> = (0..100 * 1024).map(|i| (i % 251) as u8).collect();
        let blob_id = hopnet_common::CustomUUID::new(None);
        let per_blob_key = chacha20poly1305::Key::from([0x55; 32]);
        let mut blob_op = crate::storage_host::routes::process_uploaded_file(
            Cursor::new(plaintext.clone()),
            plaintext.len(),
            blob_id.clone(),
            &per_blob_key,
            &fragments_dir,
        )
        .await
        .unwrap();
        blob_op.access = vec![
            hopnet_storage::crypto::wrap_blob_key(&blob_id, &owner_pubkey, &per_blob_key).unwrap(),
        ];

        let photo_id = hopnet_common::CustomUUID::new(None);
        let entry = hopnet_photos::envelopes::PhotoAddEntry {
            photo_id: photo_id.clone(),
            library_id: None,
            uploaded_by: 7,
            encrypted_metadata: b"meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![hopnet_photos::envelopes::PhotoResourceOp {
                resource_type: ResourceKind::Original.as_wire(),
                op: blob_op,
            }],
            metadata_access: vec![hopnet_photos::envelopes::MetadataAccessEntry {
                user_id: 7,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: hopnet_common::CustomUUID::new(None),
            cloud_fingerprint: None,
        };
        {
            let conn = pool.get().unwrap();
            let tx = conn.unchecked_transaction().unwrap();
            hopnet_photos::db::photos::insert_photo_entry(&tx, &entry, &fragments_dir).unwrap();
            crate::db::shared::commit_timed(tx).unwrap();
        }

        let grant = read_resource_grant(&pool, 7, &photo_id, ResourceKind::Original)
            .ok()
            .expect("owner grant");
        let key = hopnet_storage::crypto::unwrap_blob_key(
            &grant.access,
            &hopnet_storage::StaticRecipient(owner_secret),
        )
        .unwrap();
        assert_eq!(key.as_slice(), per_blob_key.as_slice());

        let stream =
            hopnet_storage::api::get_local(fragments_dir.clone(), grant.manifest, Some(key), None);
        tokio::pin!(stream);
        let mut out = Vec::with_capacity(plaintext.len());
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(out, plaintext);

        let grant = read_resource_grant(&pool, 7, &photo_id, ResourceKind::Original)
            .ok()
            .expect("owner grant for range read");
        let stream = hopnet_storage::api::get_local(
            fragments_dir.clone(),
            grant.manifest,
            Some(per_blob_key),
            Some((10, 99)),
        );
        tokio::pin!(stream);
        let mut ranged = Vec::new();
        while let Some(chunk) = stream.next().await {
            ranged.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(ranged, &plaintext[10..=99]);

        let err = read_resource_grant(&pool, 8, &photo_id, ResourceKind::Original)
            .err()
            .expect("wrap-less user must be refused");
        assert!(matches!(err, ResourceGrantError::Forbidden));
        let err = read_resource_grant(&pool, 7, &photo_id, ResourceKind::ThumbnailSmall)
            .err()
            .expect("unpublished kind must miss");
        assert!(matches!(err, ResourceGrantError::NotFound));
        let err = read_resource_grant(
            &pool,
            7,
            &hopnet_common::CustomUUID::new(None),
            ResourceKind::Original,
        )
        .err()
        .expect("unknown photo must miss");
        assert!(matches!(err, ResourceGrantError::NotFound));
    }

    fn insert_photo_row(
        pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
        photo_id: &hopnet_common::CustomUUID,
        uploaded_by: i32,
    ) {
        pool.get()
            .unwrap()
            .execute(
                "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, \
                 metadata_nonce) VALUES (?, NULL, ?, ?, ?)",
                rusqlite::params![photo_id, uploaded_by, vec![0u8; 4], vec![0u8; 12]],
            )
            .unwrap();
    }

    // Should: report the committed photo's owner when the caller uploaded it.
    #[test]
    fn committed_probe_reports_own_photo() {
        let pool = test_pool();
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from(
            &hopnet_storage::x25519_dalek::StaticSecret::from([0x11; 32]),
        );
        insert_user(&pool, 7, &pubkey);
        let photo_id = hopnet_common::CustomUUID::new(None);
        insert_photo_row(&pool, &photo_id, 7);

        assert_eq!(read_photo_committed(&pool, 7, &photo_id), Ok(Some(7)));
    }

    // Impact: the probe's None is the daemon's "safe to retry the same
    // photo_id" signal in the confirm-then-retry contract — reporting another
    // user's photo would both leak existence and wrongly mark a foreign photo
    // as the caller's own published work.
    // Should not: report a photo uploaded by a different user, or a photo
    // that never committed.
    #[test]
    fn committed_probe_hides_foreign_and_absent_photos() {
        let pool = test_pool();
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from(
            &hopnet_storage::x25519_dalek::StaticSecret::from([0x22; 32]),
        );
        insert_user(&pool, 7, &pubkey);
        insert_user(&pool, 8, &pubkey);
        let photo_id = hopnet_common::CustomUUID::new(None);
        insert_photo_row(&pool, &photo_id, 8);

        assert_eq!(read_photo_committed(&pool, 7, &photo_id), Ok(None));
        assert_eq!(
            read_photo_committed(&pool, 7, &hopnet_common::CustomUUID::new(None)),
            Ok(None)
        );
    }

    fn insert_photo_row_with_fp(
        pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
        photo_id: &hopnet_common::CustomUUID,
        uploaded_by: i32,
        fingerprint_hex: &str,
    ) {
        pool.get()
            .unwrap()
            .execute(
                "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, \
                 metadata_nonce, cloud_fingerprint) VALUES (?, NULL, ?, ?, ?, ?)",
                rusqlite::params![
                    photo_id,
                    uploaded_by,
                    vec![0u8; 4],
                    vec![0u8; 12],
                    fingerprint_hex
                ],
            )
            .unwrap();
    }

    // Should: resolve a fingerprint to the caller's committed photo_id.
    // Should not: report a photo whose fingerprint was committed by a
    // different user, or a fingerprint that never committed.
    #[test]
    fn fingerprint_probe_matches_and_filters_owner() {
        let pool = test_pool();
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from(
            &hopnet_storage::x25519_dalek::StaticSecret::from([0x33; 32]),
        );
        insert_user(&pool, 7, &pubkey);
        insert_user(&pool, 8, &pubkey);
        let mine = hopnet_common::CustomUUID::new(None);
        let theirs = hopnet_common::CustomUUID::new(None);
        insert_photo_row_with_fp(&pool, &mine, 7, "aa11");
        insert_photo_row_with_fp(&pool, &theirs, 8, "bb22");

        assert_eq!(read_photo_by_fingerprint(&pool, 7, "aa11"), Ok(Some(mine)));
        assert_eq!(read_photo_by_fingerprint(&pool, 7, "bb22"), Ok(None));
        assert_eq!(read_photo_by_fingerprint(&pool, 7, "cc33"), Ok(None));
    }

    // Impact: the tombstoned row still owns its fingerprint in the partial
    // UNIQUE index, so a re-publish would fail deterministically — resolving
    // it lets the daemon adopt instead of burning its retry budget.
    // Should: keep resolving a soft-deleted photo by fingerprint.
    #[test]
    fn fingerprint_probe_resolves_tombstoned_photo() {
        let pool = test_pool();
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from(
            &hopnet_storage::x25519_dalek::StaticSecret::from([0x44; 32]),
        );
        insert_user(&pool, 7, &pubkey);
        let photo_id = hopnet_common::CustomUUID::new(None);
        insert_photo_row_with_fp(&pool, &photo_id, 7, "dd44");
        pool.get()
            .unwrap()
            .execute(
                "UPDATE photos SET deleted_at = '2026-01-01T00:00:00Z', deleted_by = 7 WHERE id = ?",
                rusqlite::params![photo_id],
            )
            .unwrap();

        assert_eq!(
            read_photo_by_fingerprint(&pool, 7, "dd44"),
            Ok(Some(photo_id))
        );
    }

    // Should: report the responsibility holder once claimed, and None for a
    // user with no claim.
    #[test]
    fn responsibility_read_reports_holder() {
        let pool = test_pool();
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from(
            &hopnet_storage::x25519_dalek::StaticSecret::from([0x55; 32]),
        );
        insert_user(&pool, 7, &pubkey);
        let device_id = hopnet_common::CustomUUID::new(None);
        {
            let conn = pool.get().unwrap();
            conn.execute(
                "INSERT INTO device_tokens (id, user_id, api_key_hash, encrypted_device_name, \
                 wrapped_user_key) VALUES (?, 7, ?, 'enc', ?)",
                rusqlite::params![device_id, vec![0u8; 32], vec![0u8; 32]],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO photo_ingress_responsibility (user_id, device_id, operation_id) \
                 VALUES (7, ?, ?)",
                rusqlite::params![device_id, hopnet_common::CustomUUID::new(None)],
            )
            .unwrap();
        }

        assert_eq!(read_ingress_responsibility(&pool, 7), Ok(Some(device_id)));
        assert_eq!(read_ingress_responsibility(&pool, 8), Ok(None));
    }
}
