#[cfg(feature = "sidecar")]
use crate::crypto::{decrypt_metadata, unwrap_metadata_key};
#[cfg(feature = "sidecar")]
use crate::dispatch::{EncryptedPhotoState, PhotoChange, SyncBatch};
#[cfg(feature = "sidecar")]
use crate::error::PhotosCoreError;
#[cfg(feature = "sidecar")]
use crate::metadata::PhotoMetadata;
#[cfg(feature = "sidecar")]
use hopnet_common::CustomUUID;
#[cfg(feature = "sidecar")]
use hopnet_storage::RecipientKey;
#[cfg(feature = "sidecar")]
use rusqlite::Connection;
#[cfg(feature = "sidecar")]
use std::path::PathBuf;

/// Photo gallery row — decrypted metadata from `photo_index`.
#[cfg(feature = "sidecar")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct PhotoRow {
    pub photo_id: CustomUUID,
    pub library_id: Option<CustomUUID>,
    pub date_taken: Option<String>,
    pub upload_date: Option<String>,
    pub media_type: i32,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub orientation: Option<i32>,
    pub duration_ms: Option<i32>,
    pub camera_make: Option<String>,
    pub camera_model: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub group_id: Option<String>,
    pub group_type: Option<i32>,
    pub group_index: Option<i32>,
    pub is_group_pick: i32,
    pub deleted_at: Option<String>,
    pub expires_at: Option<String>,
    pub undecryptable: bool,
    /// `(resource_type, data_block_id)` pairs from `photo_resources_cache`,
    /// ordered by type. Same wire shape as `EncryptedPhotoState::resources`.
    /// The frontend uses these to pick a display kind and to key its content
    /// cache by blob id (content edits swap the blob under the same URL).
    pub resources: Vec<(i32, CustomUUID)>,
}

/// Install the sidecar's local (non-consensus) tables. Idempotent — uses
/// `IF NOT EXISTS` throughout because the local DB persists across app
/// restarts, unlike the consensus DB which installs once at cold start.
#[cfg(feature = "sidecar")]
pub fn install_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS photo_index (
            photo_id         TEXT PRIMARY KEY,
            library_id       TEXT,
            -- Temporal
            date_taken       TEXT,
            upload_date      TEXT,
            -- Media
            media_type       INTEGER NOT NULL,
            width            INTEGER,
            height           INTEGER,
            orientation      INTEGER,
            duration_ms      INTEGER,
            -- Camera
            camera_make      TEXT,
            camera_model     TEXT,
            -- Location
            latitude         REAL,
            longitude        REAL,
            -- Grouping (from encrypted_metadata, not consensus)
            group_id         TEXT,
            group_type       INTEGER,
            group_index      INTEGER,
            is_group_pick    INTEGER NOT NULL DEFAULT 0,
            -- Soft-delete (mirrored from consensus)
            deleted_at       TEXT,
            deleted_by       INTEGER,
            expires_at       TEXT,
            -- Sync
            synced_at_height INTEGER NOT NULL,
            undecryptable    INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_sidecar_date
            ON photo_index(date_taken);
        CREATE INDEX IF NOT EXISTS idx_sidecar_library
            ON photo_index(library_id);
        CREATE INDEX IF NOT EXISTS idx_sidecar_media
            ON photo_index(media_type);
        CREATE INDEX IF NOT EXISTS idx_sidecar_location
            ON photo_index(latitude, longitude);
        CREATE INDEX IF NOT EXISTS idx_sidecar_camera
            ON photo_index(camera_make, camera_model);
        CREATE INDEX IF NOT EXISTS idx_sidecar_group
            ON photo_index(group_id) WHERE group_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_sidecar_active
            ON photo_index(date_taken) WHERE deleted_at IS NULL;
        CREATE INDEX IF NOT EXISTS idx_sidecar_recently_deleted
            ON photo_index(expires_at) WHERE deleted_at IS NOT NULL;

        CREATE TABLE IF NOT EXISTS photo_resources_cache (
            photo_id         TEXT NOT NULL,
            resource_type    INTEGER NOT NULL,
            data_block_id    TEXT NOT NULL,
            PRIMARY KEY (photo_id, resource_type)
        );
        CREATE INDEX IF NOT EXISTS idx_resources_cache_data_block
            ON photo_resources_cache(data_block_id);

        CREATE TABLE IF NOT EXISTS sidecar_meta (
            key   TEXT PRIMARY KEY,
            value BLOB NOT NULL
        );
        ",
    )
}

/// Active gallery — photos where `deleted_at IS NULL` and decryption
/// succeeded. ORDER BY date_taken DESC, photo_id DESC for stable pagination.
#[cfg(feature = "sidecar")]
pub fn list_active(
    conn: &Connection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PhotoRow>, PhotosCoreError> {
    let mut stmt = conn.prepare(
        "SELECT photo_id, library_id, date_taken, upload_date, media_type,
                width, height, orientation, duration_ms,
                camera_make, camera_model, latitude, longitude,
                group_id, group_type, group_index, is_group_pick,
                deleted_at, expires_at, undecryptable
         FROM photo_index
         WHERE deleted_at IS NULL AND undecryptable = 0
         ORDER BY date_taken DESC, photo_id DESC
         LIMIT ? OFFSET ?",
    )?;
    let mut rows = map_rows(stmt.query_map(rusqlite::params![limit, offset], map_row)?)?;
    attach_resources(conn, &mut rows)?;
    Ok(rows)
}

/// Recently deleted — photos within the 30-day recovery window.
#[cfg(feature = "sidecar")]
pub fn list_recently_deleted(
    conn: &Connection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PhotoRow>, PhotosCoreError> {
    let mut stmt = conn.prepare(
        "SELECT photo_id, library_id, date_taken, upload_date, media_type,
                width, height, orientation, duration_ms,
                camera_make, camera_model, latitude, longitude,
                group_id, group_type, group_index, is_group_pick,
                deleted_at, expires_at, undecryptable
         FROM photo_index
         WHERE deleted_at IS NOT NULL
           AND undecryptable = 0
           AND (expires_at IS NULL OR expires_at > datetime('now'))
         ORDER BY expires_at ASC, photo_id ASC
         LIMIT ? OFFSET ?",
    )?;
    let mut rows = map_rows(stmt.query_map(rusqlite::params![limit, offset], map_row)?)?;
    attach_resources(conn, &mut rows)?;
    Ok(rows)
}

/// Single photo lookup by id.
#[cfg(feature = "sidecar")]
pub fn get_photo(
    conn: &Connection,
    photo_id: &CustomUUID,
) -> Result<Option<PhotoRow>, PhotosCoreError> {
    let mut stmt = conn.prepare(
        "SELECT photo_id, library_id, date_taken, upload_date, media_type,
                width, height, orientation, duration_ms,
                camera_make, camera_model, latitude, longitude,
                group_id, group_type, group_index, is_group_pick,
                deleted_at, expires_at, undecryptable
         FROM photo_index WHERE photo_id = ?",
    )?;
    let mut rows = map_rows(stmt.query_map(rusqlite::params![photo_id], map_row)?)?;
    attach_resources(conn, &mut rows)?;
    Ok(rows.pop())
}

#[cfg(feature = "sidecar")]
fn map_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<PhotoRow> {
    Ok(PhotoRow {
        photo_id: r.get::<_, CustomUUID>(0)?,
        library_id: r.get(1)?,
        date_taken: r.get(2)?,
        upload_date: r.get(3)?,
        media_type: r.get(4)?,
        width: r.get(5)?,
        height: r.get(6)?,
        orientation: r.get(7)?,
        duration_ms: r.get(8)?,
        camera_make: r.get(9)?,
        camera_model: r.get(10)?,
        latitude: r.get(11)?,
        longitude: r.get(12)?,
        group_id: r.get(13)?,
        group_type: r.get(14)?,
        group_index: r.get(15)?,
        is_group_pick: r.get(16)?,
        deleted_at: r.get(17)?,
        expires_at: r.get(18)?,
        undecryptable: r.get::<_, i32>(19)? != 0,
        resources: Vec::new(),
    })
}

#[cfg(feature = "sidecar")]
fn map_rows(
    iter: impl Iterator<Item = rusqlite::Result<PhotoRow>>,
) -> Result<Vec<PhotoRow>, PhotosCoreError> {
    iter.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Batch read of `photo_resources_cache` for a set of photos. 500-id chunks
/// keep the IN list well below SQLITE_MAX_VARIABLE_NUMBER.
#[cfg(feature = "sidecar")]
pub fn resources_for(
    conn: &Connection,
    photo_ids: &[&CustomUUID],
) -> Result<std::collections::HashMap<CustomUUID, Vec<(i32, CustomUUID)>>, PhotosCoreError> {
    let mut out: std::collections::HashMap<CustomUUID, Vec<(i32, CustomUUID)>> =
        std::collections::HashMap::new();
    for chunk in photo_ids.chunks(500) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT photo_id, resource_type, data_block_id
             FROM photo_resources_cache
             WHERE photo_id IN ({placeholders})
             ORDER BY photo_id, resource_type"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |r| {
            Ok((
                r.get::<_, CustomUUID>(0)?,
                r.get::<_, i32>(1)?,
                r.get::<_, CustomUUID>(2)?,
            ))
        })?;
        for row in rows {
            let (photo_id, resource_type, data_block_id) = row?;
            out.entry(photo_id)
                .or_default()
                .push((resource_type, data_block_id));
        }
    }
    Ok(out)
}

/// Fills `PhotoRow::resources` for rows returned by the list/get queries.
#[cfg(feature = "sidecar")]
fn attach_resources(conn: &Connection, rows: &mut [PhotoRow]) -> Result<(), PhotosCoreError> {
    let ids: Vec<&CustomUUID> = rows.iter().map(|r| &r.photo_id).collect();
    let mut by_photo = resources_for(conn, &ids)?;
    for row in rows.iter_mut() {
        if let Some(resources) = by_photo.remove(&row.photo_id) {
            row.resources = resources;
        }
    }
    Ok(())
}

// --- SidecarDb ---

/// The local sidecar database holding decrypted photo metadata for gallery
/// queries. Parametric over `R: RecipientKey` — the host supplies the
/// user's X25519 static secret at open time; it's used per-photo during
/// sync to unwrap `photo_metadata_access` wraps.
#[cfg(feature = "sidecar")]
pub struct SidecarDb<R: RecipientKey> {
    pub(crate) conn: Connection,
    reader: R,
    _path: PathBuf,
}

#[cfg(feature = "sidecar")]
impl<R: RecipientKey> SidecarDb<R> {
    pub fn open<P: AsRef<std::path::Path>>(path: P, reader: R) -> Result<Self, PhotosCoreError> {
        let conn = Connection::open(path.as_ref())?;
        hopnet_common::db_impl::register_uuid_extract_timestamp(&conn)
            .map_err(|e| PhotosCoreError::Dispatch(format!("register functions: {e}")))?;
        install_schema(&conn)?;
        Ok(Self {
            conn,
            reader,
            _path: path.as_ref().to_path_buf(),
        })
    }

    pub fn open_in_memory(reader: R) -> Result<Self, PhotosCoreError> {
        let conn = Connection::open_in_memory()?;
        hopnet_common::db_impl::register_uuid_extract_timestamp(&conn)
            .map_err(|e| PhotosCoreError::Dispatch(format!("register functions: {e}")))?;
        install_schema(&conn)?;
        Ok(Self {
            conn,
            reader,
            _path: PathBuf::from(":memory:"),
        })
    }

    pub fn cursor(&self) -> Result<u64, PhotosCoreError> {
        let value: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT value FROM sidecar_meta WHERE key = 'cursor'",
                [],
                |r| r.get(0),
            )
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })?;
        match value {
            Some(v) if v.len() == 8 => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&v);
                Ok(u64::from_be_bytes(bytes))
            }
            _ => Ok(0),
        }
    }

    /// Hydrate from genesis — apply a full-inventory batch (typically
    /// obtained via `dispatch.fetch_photos_since(0)`) and persist the
    /// cursor. Thin wrapper over `sync_from_batch`.
    pub fn hydrate(&self, batch: SyncBatch) -> Result<(), PhotosCoreError> {
        self.sync_from_batch(batch)
    }

    /// Apply a sync batch (obtained from the dispatch) and persist the
    /// new cursor. The caller handles the async dispatch call; the sidecar
    /// only applies the data.
    pub fn sync_from_batch(&self, batch: SyncBatch) -> Result<(), PhotosCoreError> {
        let new_cursor = batch.high_water_mark;
        let tx = self.conn.unchecked_transaction()?;
        for change in &batch.changes {
            self.apply_change(&tx, change)?;
        }
        Self::set_cursor(&tx, new_cursor)?;
        tx.commit()?;
        Ok(())
    }

    fn apply_change(&self, conn: &Connection, change: &PhotoChange) -> Result<(), PhotosCoreError> {
        match &change.state {
            None => {
                conn.execute(
                    "DELETE FROM photo_index WHERE photo_id = ?",
                    rusqlite::params![change.photo_id],
                )?;
                conn.execute(
                    "DELETE FROM photo_resources_cache WHERE photo_id = ?",
                    rusqlite::params![change.photo_id],
                )?;
            }
            Some(state) => {
                self.upsert_photo(conn, &change.photo_id, change.changed_at_height, state)?;
            }
        }
        Ok(())
    }

    fn upsert_photo(
        &self,
        conn: &Connection,
        photo_id: &CustomUUID,
        changed_at_height: u64,
        state: &EncryptedPhotoState,
    ) -> Result<(), PhotosCoreError> {
        let meta = match (&state.ephemeral_pubkey, &state.encrypted_metadata_key) {
            (Some(eph), Some(wrapped)) => {
                match unwrap_metadata_key(photo_id, eph, wrapped, &self.reader)
                    .and_then(|key| {
                        decrypt_metadata(&key, &state.metadata_nonce, &state.encrypted_metadata)
                            .map_err(Into::into)
                    })
                    .and_then(|pt| PhotoMetadata::from_json(&pt).map_err(Into::into))
                {
                    Ok(m) => Some(m),
                    Err(_e) => {
                        // Metadata decrypt failed — record as undecryptable
                        // until a later change or explicit re-sync retries it.
                        None
                    }
                }
            }
            _ => None,
        };

        let undecryptable = if meta.is_some() { 0 } else { 1 };
        conn.execute(
            "INSERT OR REPLACE INTO photo_index (
                photo_id, library_id, date_taken, upload_date, media_type,
                width, height, orientation, duration_ms,
                camera_make, camera_model, latitude, longitude,
                group_id, group_type, group_index, is_group_pick,
                deleted_at, deleted_by, expires_at,
                synced_at_height, undecryptable
            ) VALUES (
                ?1,  ?2,  ?3,  datetime(uuid_extract_timestamp(?1)/1000, 'unixepoch'),
                ?4,  ?5,  ?6,  ?7,  ?8,
                ?9,  ?10, ?11, ?12,
                ?13, ?14, ?15, ?16,
                ?17, ?18,
                CASE WHEN ?17 IS NOT NULL THEN datetime(?17, '+30 days') ELSE NULL END,
                ?19, ?20
            )",
            rusqlite::params![
                photo_id,
                state.library_id,
                meta.as_ref().map(|m| m.date_taken.as_str()),
                meta.as_ref().map_or(0, |m| m.media_type),
                meta.as_ref().and_then(|m| m.width),
                meta.as_ref().and_then(|m| m.height),
                meta.as_ref().and_then(|m| m.orientation),
                meta.as_ref().and_then(|m| m.duration_ms),
                meta.as_ref().and_then(|m| m.camera_make.as_deref()),
                meta.as_ref().and_then(|m| m.camera_model.as_deref()),
                meta.as_ref().and_then(|m| m.latitude),
                meta.as_ref().and_then(|m| m.longitude),
                meta.as_ref().and_then(|m| m.group_id.as_deref()),
                meta.as_ref().and_then(|m| m.group_type),
                meta.as_ref().and_then(|m| m.group_index),
                meta.as_ref().map_or(0, |m| m.is_group_pick.unwrap_or(0)),
                state.deleted_at,
                state.deleted_by,
                changed_at_height as i64,
                undecryptable,
            ],
        )?;

        // Rebuild resource cache: delete all then insert current set.
        conn.execute(
            "DELETE FROM photo_resources_cache WHERE photo_id = ?",
            rusqlite::params![photo_id],
        )?;
        for (rt, block_id) in &state.resources {
            conn.execute(
                "INSERT INTO photo_resources_cache (photo_id, resource_type, data_block_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![photo_id, rt, block_id],
            )?;
        }

        Ok(())
    }

    pub fn get_photo(&self, photo_id: &CustomUUID) -> Result<Option<PhotoRow>, PhotosCoreError> {
        get_photo(&self.conn, photo_id)
    }

    pub fn list_active(&self, limit: i64, offset: i64) -> Result<Vec<PhotoRow>, PhotosCoreError> {
        list_active(&self.conn, limit, offset)
    }

    pub fn list_recently_deleted(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<PhotoRow>, PhotosCoreError> {
        list_recently_deleted(&self.conn, limit, offset)
    }

    fn set_cursor(conn: &Connection, cursor: u64) -> Result<(), PhotosCoreError> {
        conn.execute(
            "INSERT OR REPLACE INTO sidecar_meta (key, value) VALUES ('cursor', ?)",
            rusqlite::params![cursor.to_be_bytes().to_vec()],
        )?;
        Ok(())
    }
}

#[cfg(all(test, feature = "sidecar"))]
mod tests {
    use super::*;
    use crate::crypto::{encrypt_metadata, generate_metadata_key, wrap_metadata_key};
    use crate::metadata::PhotoMetadata;
    use hopnet_common::CustomUUID;
    use hopnet_storage::StaticRecipient;

    fn new_reader() -> StaticRecipient {
        StaticRecipient(hopnet_storage::x25519_dalek::StaticSecret::from([0xAB; 32]))
    }

    fn make_encrypted_state(
        photo_id: &CustomUUID,
        reader: &StaticRecipient,
        deleted_at: Option<&str>,
    ) -> EncryptedPhotoState {
        let meta = PhotoMetadata {
            date_taken: "2025-06-01T12:00:00Z".into(),
            media_type: 0,
            width: Some(1920),
            height: Some(1080),
            ..Default::default()
        };
        let key = generate_metadata_key();
        let (encrypted, nonce) = encrypt_metadata(&key, &meta.to_json().unwrap()).unwrap();
        let (eph, wrapped) = wrap_metadata_key(photo_id, &reader.pubkey(), &key).unwrap();
        EncryptedPhotoState {
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: encrypted,
            metadata_nonce: nonce,
            deleted_at: deleted_at.map(|s| s.to_string()),
            deleted_by: if deleted_at.is_some() { Some(1) } else { None },
            ephemeral_pubkey: Some(eph),
            encrypted_metadata_key: Some(wrapped),
            resources: vec![(0, CustomUUID::retention_cutoff(100))],
        }
    }

    #[test]
    fn open_and_install_schema() {
        let _db = SidecarDb::open_in_memory(new_reader()).unwrap();
    }

    #[test]
    fn hydrate_populates_index() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let photo_id = CustomUUID::retention_cutoff(0);
        let reader = new_reader();
        let state = make_encrypted_state(&photo_id, &reader, None);

        db.hydrate(SyncBatch {
            changes: vec![PhotoChange {
                photo_id: photo_id.clone(),
                changed_at_height: 1,
                state: Some(state),
            }],
            high_water_mark: 1,
        })
        .unwrap();

        let rows = db.list_active(10, 0).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].date_taken.as_deref(), Some("2025-06-01T12:00:00Z"));
        assert_eq!(rows[0].media_type, 0);
        assert_eq!(db.cursor().unwrap(), 1);
    }

    #[test]
    fn hard_delete_removes_row() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let photo_id = CustomUUID::retention_cutoff(1);
        let reader = new_reader();
        let state = make_encrypted_state(&photo_id, &reader, None);

        db.hydrate(SyncBatch {
            changes: vec![PhotoChange {
                photo_id: photo_id.clone(),
                changed_at_height: 1,
                state: Some(state),
            }],
            high_water_mark: 1,
        })
        .unwrap();

        db.sync_from_batch(SyncBatch {
            changes: vec![PhotoChange {
                photo_id: photo_id.clone(),
                changed_at_height: 2,
                state: None,
            }],
            high_water_mark: 2,
        })
        .unwrap();

        assert!(db.list_active(10, 0).unwrap().is_empty());
        assert_eq!(db.cursor().unwrap(), 2);
    }

    #[test]
    fn undecryptable_photo_gets_marker() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let photo_id = CustomUUID::retention_cutoff(2);

        db.hydrate(SyncBatch {
            changes: vec![PhotoChange {
                photo_id: photo_id.clone(),
                changed_at_height: 1,
                state: Some(EncryptedPhotoState {
                    library_id: None,
                    uploaded_by: 1,
                    encrypted_metadata: vec![],
                    metadata_nonce: [0u8; 12],
                    deleted_at: None,
                    deleted_by: None,
                    ephemeral_pubkey: None,
                    encrypted_metadata_key: None,
                    resources: vec![],
                }),
            }],
            high_water_mark: 1,
        })
        .unwrap();

        assert!(db.list_active(10, 0).unwrap().is_empty());
        let photo = db.get_photo(&photo_id).unwrap().unwrap();
        assert!(photo.undecryptable);
    }

    #[test]
    fn cursor_survives_row_deletion() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let photo_id = CustomUUID::retention_cutoff(3);
        let reader = new_reader();
        let state = make_encrypted_state(&photo_id, &reader, None);

        db.hydrate(SyncBatch {
            changes: vec![PhotoChange {
                photo_id,
                changed_at_height: 5,
                state: Some(state),
            }],
            high_water_mark: 5,
        })
        .unwrap();
        db.conn.execute("DELETE FROM photo_index", []).unwrap();
        assert_eq!(db.cursor().unwrap(), 5);
    }

    #[test]
    fn recently_deleted_shows_tombstoned() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let photo_id = CustomUUID::retention_cutoff(4);
        let reader = new_reader();
        let state = make_encrypted_state(&photo_id, &reader, Some("2099-01-01T00:00:00Z"));

        db.hydrate(SyncBatch {
            changes: vec![PhotoChange {
                photo_id,
                changed_at_height: 1,
                state: Some(state),
            }],
            high_water_mark: 1,
        })
        .unwrap();
        let deleted = db.list_recently_deleted(10, 0).unwrap();
        assert_eq!(deleted.len(), 1);
        assert!(deleted[0].expires_at.is_some());
    }

    /// Mixed batch: one decryptable, one with plausible crypto material
    /// that fails decrypt (wrong key). Both must produce rows; the failed
    /// one marked undecryptable; the batch must not abort.
    #[test]
    fn mixed_batch_decrypt_failure_is_non_fatal() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        let photo_ok = CustomUUID::retention_cutoff(0);
        let photo_bad = CustomUUID::retention_cutoff(1);
        let state_ok = make_encrypted_state(&photo_ok, &reader, None);

        // Bogus crypto: valid ephemeral length + valid wrapped-key length,
        // but encrypted under a different (random) key — decrypt will fail.
        let bad_state = EncryptedPhotoState {
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"garbage ciphertext".to_vec(),
            metadata_nonce: [0xA1; 12],
            deleted_at: None,
            deleted_by: None,
            ephemeral_pubkey: Some([0x42; 32]),
            encrypted_metadata_key: Some(vec![0xFF; 48]),
            resources: vec![],
        };

        db.hydrate(SyncBatch {
            changes: vec![
                PhotoChange {
                    photo_id: photo_ok,
                    changed_at_height: 1,
                    state: Some(state_ok),
                },
                PhotoChange {
                    photo_id: photo_bad.clone(),
                    changed_at_height: 1,
                    state: Some(bad_state),
                },
            ],
            high_water_mark: 1,
        })
        .unwrap();

        let rows = db.list_active(10, 0).unwrap();
        assert_eq!(rows.len(), 1, "only decryptable photo in active gallery");
        let bad = db.get_photo(&photo_bad).unwrap().unwrap();
        assert!(
            bad.undecryptable,
            "decrypt-failure photo marked undecryptable"
        );
        assert_eq!(
            db.cursor().unwrap(),
            1,
            "cursor advanced despite decrypt failure"
        );
    }

    // Should: expose each photo's synced resource pairs, ordered by type, on
    // both gallery and detail rows.
    #[test]
    fn gallery_rows_carry_resources() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let photo_id = CustomUUID::retention_cutoff(50);
        let reader = new_reader();
        let original_blob = CustomUUID::retention_cutoff(51);
        let thumb_blob = CustomUUID::retention_cutoff(52);
        let mut state = make_encrypted_state(&photo_id, &reader, None);
        state.resources = vec![(5, thumb_blob.clone()), (0, original_blob.clone())];

        db.hydrate(SyncBatch {
            changes: vec![PhotoChange {
                photo_id: photo_id.clone(),
                changed_at_height: 1,
                state: Some(state),
            }],
            high_water_mark: 1,
        })
        .unwrap();

        let expected = vec![(0, original_blob), (5, thumb_blob)];
        let rows = db.list_active(10, 0).unwrap();
        assert_eq!(rows[0].resources, expected);
        let detail = db.get_photo(&photo_id).unwrap().unwrap();
        assert_eq!(detail.resources, expected);
    }

    // Impact: stale blob ids resurfacing in payloads after a hard delete
    // would point clients at content that no longer exists.
    // Should not: keep resource-cache rows for a hard-deleted photo.
    #[test]
    fn resources_cleared_on_hard_delete() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let photo_id = CustomUUID::retention_cutoff(60);
        let reader = new_reader();
        let state = make_encrypted_state(&photo_id, &reader, None);

        db.hydrate(SyncBatch {
            changes: vec![PhotoChange {
                photo_id: photo_id.clone(),
                changed_at_height: 1,
                state: Some(state),
            }],
            high_water_mark: 1,
        })
        .unwrap();
        assert!(!resources_for(&db.conn, &[&photo_id]).unwrap().is_empty());

        db.sync_from_batch(SyncBatch {
            changes: vec![PhotoChange {
                photo_id: photo_id.clone(),
                changed_at_height: 2,
                state: None,
            }],
            high_water_mark: 2,
        })
        .unwrap();
        assert!(resources_for(&db.conn, &[&photo_id]).unwrap().is_empty());
    }

    // Should: return an empty map for an empty photo set without erroring.
    #[test]
    fn resources_for_empty_input_is_empty() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        assert!(resources_for(&db.conn, &[]).unwrap().is_empty());
    }
}
