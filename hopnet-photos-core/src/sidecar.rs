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

        -- Hydrated shared libraries: which libraries this sidecar has
        -- backfilled, and the last consensus photo_view_changes height
        -- consumed for each. Joining/late-grant rematerialization keys on
        -- this instead of the photo_changes cursor — a membership change
        -- alters the VIEW, not the photos, so the global cursor never
        -- moves for it. IF NOT EXISTS keeps existing sidecars compatible.
        CREATE TABLE IF NOT EXISTS sidecar_libraries (
            library_id       TEXT PRIMARY KEY,
            last_view_height INTEGER NOT NULL
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
    attach_resources(conn, rows.iter_mut())?;
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
    attach_resources(conn, rows.iter_mut())?;
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
    attach_resources(conn, rows.iter_mut())?;
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
fn attach_resources<'a>(
    conn: &Connection,
    rows: impl Iterator<Item = &'a mut PhotoRow>,
) -> Result<(), PhotosCoreError> {
    let mut rows: Vec<&'a mut PhotoRow> = rows.collect();
    let ids: Vec<&CustomUUID> = rows.iter().map(|r| &r.photo_id).collect();
    let mut by_photo = resources_for(conn, &ids)?;
    for row in rows.iter_mut() {
        if let Some(resources) = by_photo.remove(&row.photo_id) {
            row.resources = resources;
        }
    }
    Ok(())
}

// --- keyset browse page + histogram ---

/// Media-type filter pushed into the browse SQL. Each `Some` flag appends a
/// fixed literal clause; `favorite` is absent until the sidecar grows a
/// favorites column (Phase 4).
#[cfg(feature = "sidecar")]
#[derive(Debug, Clone, Default)]
pub struct MediaFilter {
    pub video: Option<bool>,
    pub live: Option<bool>,
    pub raw: Option<bool>,
}

#[cfg(feature = "sidecar")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageDir {
    Older,
    Newer,
}

/// One browse-page row: `PhotoRow` plus the server-computed sort key the
/// client echoes back verbatim in keyset cursors. A wrapper (not a PhotoRow
/// field) so the existing gallery/detail payloads stay byte-identical.
#[cfg(feature = "sidecar")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct PhotoPageItem {
    pub sort_ms: i64,
    #[serde(flatten)]
    pub row: PhotoRow,
}

/// One month of the browse timeline, for the histogram rail.
#[cfg(feature = "sidecar")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct MonthBucket {
    pub month: String,
    pub count: i64,
}

#[cfg(feature = "sidecar")]
fn media_clauses(filter: &MediaFilter) -> String {
    let mut sql = String::new();
    for (flag, media_type) in [(filter.video, 1), (filter.live, 2), (filter.raw, 3)] {
        match flag {
            Some(true) => sql.push_str(&format!(" AND media_type = {media_type}")),
            Some(false) => sql.push_str(&format!(" AND media_type != {media_type}")),
            None => {}
        }
    }
    sql
}

/// The active set with a computed keyset sort key. Second-precision
/// truncation is fine: ties break on photo_id, and clients echo this exact
/// value back in cursors. IFNULL guards unparseable date strings (can't
/// happen in practice — upload_date is machine-generated).
#[cfg(feature = "sidecar")]
const PAGE_INNER_SELECT: &str = "SELECT photo_id, library_id, date_taken, upload_date, media_type,
            width, height, orientation, duration_ms,
            camera_make, camera_model, latitude, longitude,
            group_id, group_type, group_index, is_group_pick,
            deleted_at, expires_at, undecryptable,
            IFNULL(CAST(strftime('%s', COALESCE(date_taken, upload_date)) AS INTEGER) * 1000, 0)
                AS sort_ms
     FROM photo_index
     WHERE deleted_at IS NULL AND undecryptable = 0";

/// One keyset page of the browse timeline. `cursor` is the decoded
/// `(sort_ms, photo_id)` edge; the photo_id may be `""` for a month-boundary
/// anchor (TEXT comparison puts the boundary strictly between months).
/// Returns `(items, has_more)`; items are ALWAYS newest-first — for
/// `Newer` the block nearest the cursor is fetched ascending and reversed,
/// so the client can prepend it verbatim.
#[cfg(feature = "sidecar")]
pub fn list_page(
    conn: &Connection,
    cursor: Option<(i64, String)>,
    dir: PageDir,
    filter: &MediaFilter,
    limit: i64,
) -> Result<(Vec<PhotoPageItem>, bool), PhotosCoreError> {
    let (predicate, order) = match dir {
        PageDir::Older => (
            "(?1 IS NULL OR sort_ms < ?1 OR (sort_ms = ?1 AND photo_id < ?2))",
            "DESC",
        ),
        // Newer without a cursor is a caller error; ?1 = NULL then matches
        // nothing, which is the safe degenerate outcome.
        PageDir::Newer => ("(sort_ms > ?1 OR (sort_ms = ?1 AND photo_id > ?2))", "ASC"),
    };
    let sql = format!(
        "SELECT * FROM ({inner}{media}) WHERE {predicate}
         ORDER BY sort_ms {order}, photo_id {order}
         LIMIT ?3",
        inner = PAGE_INNER_SELECT,
        media = media_clauses(filter),
    );

    let (cursor_ms, cursor_id) = match &cursor {
        Some((ms, id)) => (Some(*ms), Some(id.clone())),
        None => (None, None),
    };
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        rusqlite::params![cursor_ms, cursor_id, limit + 1],
        |r| {
            Ok(PhotoPageItem {
                sort_ms: r.get(20)?,
                row: map_row(r)?,
            })
        },
    )?;
    let mut items = rows.collect::<Result<Vec<_>, _>>()?;

    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    if matches!(dir, PageDir::Newer) {
        items.reverse();
    }
    attach_resources(conn, items.iter_mut().map(|i| &mut i.row))?;
    Ok((items, has_more))
}

/// UTC month buckets of the active set, newest month first, honoring the
/// same media filters as the browse page. Months align with the client's
/// `toISOString().slice(0, 7)` / `Date.UTC` boundary math.
#[cfg(feature = "sidecar")]
pub fn month_histogram(
    conn: &Connection,
    filter: &MediaFilter,
) -> Result<Vec<MonthBucket>, PhotosCoreError> {
    let sql = format!(
        "SELECT strftime('%Y-%m', COALESCE(date_taken, upload_date)) AS month, COUNT(*) AS count
         FROM photo_index
         WHERE deleted_at IS NULL AND undecryptable = 0{media}
         GROUP BY month
         HAVING month IS NOT NULL
         ORDER BY month DESC",
        media = media_clauses(filter),
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |r| {
        Ok(MonthBucket {
            month: r.get(0)?,
            count: r.get(1)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
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

    /// Apply a library-backfill page WITHOUT touching the height cursor.
    /// Backfill rows sit outside the photo_changes feed (joining a library
    /// changes the view, not the photos); concurrent genuine changes still
    /// arrive via the ordinary cursor path, and upserts are idempotent
    /// either way.
    pub fn apply_backfill(&self, changes: &[PhotoChange]) -> Result<(), PhotosCoreError> {
        let tx = self.conn.unchecked_transaction()?;
        for change in changes {
            self.apply_change(&tx, change)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Hydrated shared libraries with the last consumed view height.
    pub fn library_states(&self) -> Result<Vec<(CustomUUID, i64)>, PhotosCoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT library_id, last_view_height FROM sidecar_libraries")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn set_library_state(
        &self,
        library_id: &CustomUUID,
        last_view_height: i64,
    ) -> Result<(), PhotosCoreError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO sidecar_libraries (library_id, last_view_height)
             VALUES (?1, ?2)",
            rusqlite::params![library_id, last_view_height],
        )?;
        Ok(())
    }

    /// Drop every local row belonging to a library the user left or was
    /// removed from — the client-side half of membership revocation (the
    /// mesh-side read gate closed the moment the membership row died).
    pub fn purge_library(&self, library_id: &CustomUUID) -> Result<(), PhotosCoreError> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM photo_resources_cache WHERE photo_id IN
               (SELECT photo_id FROM photo_index WHERE library_id = ?1)",
            rusqlite::params![library_id],
        )?;
        tx.execute(
            "DELETE FROM photo_index WHERE library_id = ?1",
            rusqlite::params![library_id],
        )?;
        tx.execute(
            "DELETE FROM sidecar_libraries WHERE library_id = ?1",
            rusqlite::params![library_id],
        )?;
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

    pub fn list_page(
        &self,
        cursor: Option<(i64, String)>,
        dir: PageDir,
        filter: &MediaFilter,
        limit: i64,
    ) -> Result<(Vec<PhotoPageItem>, bool), PhotosCoreError> {
        list_page(&self.conn, cursor, dir, filter, limit)
    }

    pub fn month_histogram(&self, filter: &MediaFilter) -> Result<Vec<MonthBucket>, PhotosCoreError> {
        month_histogram(&self.conn, filter)
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

    // --- keyset browse page + histogram ---

    fn make_state_with(
        photo_id: &CustomUUID,
        reader: &StaticRecipient,
        date_taken: &str,
        media_type: i32,
        resources: Vec<(i32, CustomUUID)>,
    ) -> EncryptedPhotoState {
        let meta = PhotoMetadata {
            date_taken: date_taken.into(),
            media_type,
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
            deleted_at: None,
            deleted_by: None,
            ephemeral_pubkey: Some(eph),
            encrypted_metadata_key: Some(wrapped),
            resources,
        }
    }

    fn hydrate_all(
        db: &SidecarDb<StaticRecipient>,
        states: Vec<(CustomUUID, EncryptedPhotoState)>,
    ) {
        db.hydrate(SyncBatch {
            changes: states
                .into_iter()
                .map(|(photo_id, state)| PhotoChange {
                    photo_id,
                    changed_at_height: 1,
                    state: Some(state),
                })
                .collect(),
            high_water_mark: 1,
        })
        .unwrap();
    }

    fn dated_photos(
        reader: &StaticRecipient,
        specs: &[(i64, &str)],
    ) -> Vec<(CustomUUID, EncryptedPhotoState)> {
        specs
            .iter()
            .map(|(seed, date)| {
                let id = CustomUUID::retention_cutoff(*seed);
                let state = make_state_with(&id, reader, date, 0, vec![]);
                (id, state)
            })
            .collect()
    }

    fn page_ids(items: &[PhotoPageItem]) -> Vec<CustomUUID> {
        items.iter().map(|i| i.row.photo_id.clone()).collect()
    }

    fn edge_cursor(item: &PhotoPageItem) -> (i64, String) {
        (item.sort_ms, item.row.photo_id.to_string())
    }

    // Should: return the first page newest-first, flipping has_more exactly
    // when more rows exist beyond the limit.
    #[test]
    fn page_orders_newest_first_with_has_more() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        hydrate_all(
            &db,
            dated_photos(
                &reader,
                &[
                    (1, "2025-01-01T00:00:00Z"),
                    (2, "2025-01-02T00:00:00Z"),
                    (3, "2025-01-03T00:00:00Z"),
                    (4, "2025-01-04T00:00:00Z"),
                    (5, "2025-01-05T00:00:00Z"),
                ],
            ),
        );

        let (items, has_more) =
            list_page(&db.conn, None, PageDir::Older, &MediaFilter::default(), 3).unwrap();
        assert!(has_more);
        assert_eq!(items.len(), 3);
        let dates: Vec<_> = items
            .iter()
            .map(|i| i.row.date_taken.clone().unwrap())
            .collect();
        assert_eq!(
            dates,
            vec![
                "2025-01-05T00:00:00Z",
                "2025-01-04T00:00:00Z",
                "2025-01-03T00:00:00Z"
            ]
        );

        let (all, has_more) =
            list_page(&db.conn, None, PageDir::Older, &MediaFilter::default(), 5).unwrap();
        assert!(!has_more);
        assert_eq!(all.len(), 5);
    }

    // Impact: a duplicate or skipped photo at a page seam silently corrupts
    // the browse window.
    // Should: visit every photo exactly once when walking pages by edge
    // cursor, including photos sharing one date_taken.
    #[test]
    fn pages_continue_without_overlap_or_gap() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        hydrate_all(
            &db,
            dated_photos(
                &reader,
                &[
                    (1, "2025-06-01T12:00:00Z"),
                    (2, "2025-06-01T12:00:00Z"),
                    (3, "2025-06-01T12:00:00Z"),
                    (4, "2025-06-02T00:00:00Z"),
                    (5, "2025-06-03T00:00:00Z"),
                    (6, "2025-06-04T00:00:00Z"),
                    (7, "2025-06-05T00:00:00Z"),
                ],
            ),
        );

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let (items, has_more) =
                list_page(&db.conn, cursor.clone(), PageDir::Older, &MediaFilter::default(), 2)
                    .unwrap();
            seen.extend(page_ids(&items));
            if !has_more {
                break;
            }
            cursor = Some(edge_cursor(items.last().unwrap()));
        }

        assert_eq!(seen.len(), 7, "every photo visited exactly once");
        let unique: std::collections::HashSet<_> = seen.iter().collect();
        assert_eq!(unique.len(), 7, "no duplicates across page seams");
    }

    // Impact: the client prepends dir=newer blocks verbatim; wrong ordering
    // corrupts the window silently.
    // Should: return the nearest-newer block newest-first.
    #[test]
    fn newer_pages_arrive_newest_first() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        hydrate_all(
            &db,
            dated_photos(
                &reader,
                &[
                    (1, "2025-02-01T00:00:00Z"),
                    (2, "2025-02-02T00:00:00Z"),
                    (3, "2025-02-03T00:00:00Z"),
                    (4, "2025-02-04T00:00:00Z"),
                    (5, "2025-02-05T00:00:00Z"),
                ],
            ),
        );
        let (all, _) =
            list_page(&db.conn, None, PageDir::Older, &MediaFilter::default(), 5).unwrap();

        // Anchor at the second-oldest item; the two nearest-newer photos are
        // all[1] and all[2], and the block must arrive in that (DESC) order.
        let (items, has_more) = list_page(
            &db.conn,
            Some(edge_cursor(&all[3])),
            PageDir::Newer,
            &MediaFilter::default(),
            2,
        )
        .unwrap();
        assert!(has_more, "all[0] remains beyond the newer block");
        assert_eq!(page_ids(&items), vec![
            all[1].row.photo_id.clone(),
            all[2].row.photo_id.clone()
        ]);
    }

    // Impact: histogram month jumps land on the wrong month if the boundary
    // falls on the wrong side of the anchor.
    // Should: split exactly at a month boundary for an empty-photo_id anchor
    // cursor — older admits the anchor month and older, newer everything
    // above, including a photo at exactly the boundary instant.
    #[test]
    fn boundary_cursor_splits_months() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        hydrate_all(
            &db,
            dated_photos(
                &reader,
                &[
                    (1, "2025-05-10T00:00:00Z"),
                    (2, "2025-05-20T00:00:00Z"),
                    (3, "2025-06-01T00:00:00Z"), // exactly at the boundary
                    (4, "2025-06-15T00:00:00Z"),
                ],
            ),
        );
        // First ms of June 2025 UTC — the anchor for jumping to May.
        let boundary = (1748736000i64) * 1000;
        let anchor = Some((boundary, String::new()));

        let (older, _) =
            list_page(&db.conn, anchor.clone(), PageDir::Older, &MediaFilter::default(), 10)
                .unwrap();
        let older_dates: Vec<_> = older
            .iter()
            .map(|i| i.row.date_taken.clone().unwrap())
            .collect();
        assert_eq!(older_dates, vec!["2025-05-20T00:00:00Z", "2025-05-10T00:00:00Z"]);

        let (newer, _) =
            list_page(&db.conn, anchor, PageDir::Newer, &MediaFilter::default(), 10).unwrap();
        let newer_dates: Vec<_> = newer
            .iter()
            .map(|i| i.row.date_taken.clone().unwrap())
            .collect();
        assert_eq!(newer_dates, vec!["2025-06-15T00:00:00Z", "2025-06-01T00:00:00Z"]);
    }

    // Should: push media filters into SQL — video only/exclude, live only,
    // and raw only each select the right media types.
    #[test]
    fn media_filters_push_into_sql() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        let mut states = Vec::new();
        for (seed, media_type) in [(1, 0), (2, 1), (3, 2), (4, 3)] {
            let id = CustomUUID::retention_cutoff(seed);
            let date = format!("2025-03-0{seed}T00:00:00Z");
            states.push((id.clone(), make_state_with(&id, &reader, &date, media_type, vec![])));
        }
        hydrate_all(&db, states);

        let types_for = |filter: MediaFilter| -> Vec<i32> {
            let (items, _) = list_page(&db.conn, None, PageDir::Older, &filter, 10).unwrap();
            let mut t: Vec<i32> = items.iter().map(|i| i.row.media_type).collect();
            t.sort();
            t
        };

        assert_eq!(types_for(MediaFilter { video: Some(true), ..Default::default() }), vec![1]);
        assert_eq!(
            types_for(MediaFilter { video: Some(false), ..Default::default() }),
            vec![0, 2, 3]
        );
        assert_eq!(types_for(MediaFilter { live: Some(true), ..Default::default() }), vec![2]);
        assert_eq!(types_for(MediaFilter { raw: Some(true), ..Default::default() }), vec![3]);
    }

    // Should not: include soft-deleted or undecryptable photos in any page.
    #[test]
    fn page_excludes_tombstones_and_undecryptable() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        let active = CustomUUID::retention_cutoff(1);
        let deleted = CustomUUID::retention_cutoff(2);
        let opaque = CustomUUID::retention_cutoff(3);

        let mut deleted_state =
            make_state_with(&deleted, &reader, "2025-04-02T00:00:00Z", 0, vec![]);
        deleted_state.deleted_at = Some("2025-04-10T00:00:00Z".into());
        deleted_state.deleted_by = Some(1);
        let mut opaque_state =
            make_state_with(&opaque, &reader, "2025-04-03T00:00:00Z", 0, vec![]);
        opaque_state.ephemeral_pubkey = None;
        opaque_state.encrypted_metadata_key = None;

        hydrate_all(
            &db,
            vec![
                (
                    active.clone(),
                    make_state_with(&active, &reader, "2025-04-01T00:00:00Z", 0, vec![]),
                ),
                (deleted, deleted_state),
                (opaque, opaque_state),
            ],
        );

        let (items, _) =
            list_page(&db.conn, None, PageDir::Older, &MediaFilter::default(), 10).unwrap();
        assert_eq!(page_ids(&items), vec![active]);
    }

    // Should: attach resource pairs to page items, same shape as list_active.
    #[test]
    fn page_items_carry_resources() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        let id = CustomUUID::retention_cutoff(1);
        let original = CustomUUID::retention_cutoff(10);
        let thumb = CustomUUID::retention_cutoff(11);
        hydrate_all(
            &db,
            vec![(
                id.clone(),
                make_state_with(
                    &id,
                    &reader,
                    "2025-07-01T00:00:00Z",
                    0,
                    vec![(5, thumb.clone()), (0, original.clone())],
                ),
            )],
        );

        let (items, _) =
            list_page(&db.conn, None, PageDir::Older, &MediaFilter::default(), 10).unwrap();
        assert_eq!(items[0].row.resources, vec![(0, original), (5, thumb)]);
    }

    // Should: bucket the histogram by UTC month of the sort date, newest
    // month first, honoring the same media filters.
    // Should not: count soft-deleted photos in histogram buckets.
    #[test]
    fn histogram_buckets_by_utc_month() {
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let reader = new_reader();
        let mut states = dated_photos(
            &reader,
            &[
                (1, "2025-05-05T00:00:00Z"),
                (2, "2025-05-25T00:00:00Z"),
                (3, "2025-06-10T00:00:00Z"),
                (4, "2025-06-20T00:00:00Z"),
            ],
        );
        // A June video (filterable) and a deleted May photo (never counted).
        let video = CustomUUID::retention_cutoff(5);
        states.push((
            video.clone(),
            make_state_with(&video, &reader, "2025-06-25T00:00:00Z", 1, vec![]),
        ));
        let tombstoned = CustomUUID::retention_cutoff(6);
        let mut tomb_state =
            make_state_with(&tombstoned, &reader, "2025-05-30T00:00:00Z", 0, vec![]);
        tomb_state.deleted_at = Some("2025-06-01T00:00:00Z".into());
        tomb_state.deleted_by = Some(1);
        states.push((tombstoned, tomb_state));
        hydrate_all(&db, states);

        let buckets = month_histogram(&db.conn, &MediaFilter::default()).unwrap();
        let flat: Vec<_> = buckets.iter().map(|b| (b.month.as_str(), b.count)).collect();
        assert_eq!(flat, vec![("2025-06", 3), ("2025-05", 2)]);

        let no_video = month_histogram(
            &db.conn,
            &MediaFilter { video: Some(false), ..Default::default() },
        )
        .unwrap();
        let flat: Vec<_> = no_video.iter().map(|b| (b.month.as_str(), b.count)).collect();
        assert_eq!(flat, vec![("2025-06", 2), ("2025-05", 2)]);
    }

    // Impact: the backfill is the join-time rematerialization path — if it
    // moved the photo_changes cursor, a joiner would skip genuine changes
    // that landed between the backfill snapshot and their next sync.
    // Should: apply backfill rows into the index without touching the
    // cursor, and be idempotent on re-apply.
    #[test]
    fn backfill_applies_without_moving_cursor() {
        let reader = new_reader();
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        db.sync_from_batch(SyncBatch {
            changes: vec![],
            high_water_mark: 41,
        })
        .unwrap();

        let photo_id = CustomUUID::retention_cutoff(1);
        let lib = CustomUUID::retention_cutoff(2);
        let mut state = make_encrypted_state(&photo_id, &reader, None);
        state.library_id = Some(lib.clone());
        let changes = vec![PhotoChange {
            photo_id: photo_id.clone(),
            changed_at_height: 0, // backfill rows live outside the cursor
            state: Some(state),
        }];
        db.apply_backfill(&changes).unwrap();
        db.apply_backfill(&changes).unwrap(); // idempotent

        assert_eq!(db.cursor().unwrap(), 41, "cursor untouched by backfill");
        assert!(db.get_photo(&photo_id).unwrap().is_some());
    }

    // Should: track hydrated libraries with their last view height, purge
    // a departed library's rows (index + resource cache + state), and
    // leave other libraries' rows alone.
    // Impact: purge is the client half of kick/leave — the mesh read gate
    // closes instantly, and this is what stops the local sidecar from
    // showing a library the user no longer belongs to.
    #[test]
    fn library_state_and_purge_round_trip() {
        let reader = new_reader();
        let db = SidecarDb::open_in_memory(new_reader()).unwrap();
        let lib_a = CustomUUID::retention_cutoff(10);
        let lib_b = CustomUUID::retention_cutoff(11);

        for (i, lib) in [(20, &lib_a), (21, &lib_b)] {
            let photo_id = CustomUUID::retention_cutoff(i);
            let mut state = make_encrypted_state(&photo_id, &reader, None);
            state.library_id = Some((*lib).clone());
            db.apply_backfill(&[PhotoChange {
                photo_id,
                changed_at_height: 0,
                state: Some(state),
            }])
            .unwrap();
        }
        db.set_library_state(&lib_a, 7).unwrap();
        db.set_library_state(&lib_b, 9).unwrap();
        assert_eq!(
            db.library_states().unwrap().len(),
            2,
            "both libraries tracked"
        );

        db.purge_library(&lib_a).unwrap();
        let states = db.library_states().unwrap();
        assert_eq!(states, vec![(lib_b, 9)]);
        assert!(
            db.get_photo(&CustomUUID::retention_cutoff(20)).unwrap().is_none(),
            "purged library's photo gone"
        );
        assert!(
            db.get_photo(&CustomUUID::retention_cutoff(21)).unwrap().is_some(),
            "other library's photo untouched"
        );
    }
}
