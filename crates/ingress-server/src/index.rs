//! The read index: a server-owned SQLite mirror of the sidecar tree.
//!
//! The daemon's `state.db` lives on another host and its snapshots rotate;
//! the sidecar tree is the complete, durable, replicated record. We index
//! *it* — reusing `ingress_core::sidecar_io::walk_sidecars` +
//! `Sidecar::from_json` (the same primitives `recover.rs` uses, so the
//! reconstruction path is exercised on every boot and cannot silently rot).
//!
//! The index is a disposable cache: a `PRAGMA user_version` mismatch drops and
//! rebuilds it (no migrations — nothing here is authoritative). Refresh is
//! incremental by file mtime; deletions are caught by a parse-free membership
//! sweep.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};

use ingress_core::descriptor::MediaType;
use ingress_core::paths::BlobPaths;
use ingress_core::sidecar_io::walk_sidecars;
use ingress_core::{LibraryId, Sidecar};

use crate::config::Config;
use crate::dto::{
    Cursor, LibrarySummary, ListDir, MonthBucket, PhotoDetail, PhotoFilter, PhotoPage,
    PhotoSummary, ResourceInfo,
};

/// Bump to invalidate every index DB (drop + full rebuild on next open).
const SCHEMA_VERSION: i64 = 1;

/// The `PRAGMA user_version` write, as a static literal (sqlx 0.9 only accepts
/// `&'static str` SQL). The assertion forces this to be updated in lockstep
/// with [`SCHEMA_VERSION`] — a bump that forgets it fails to compile.
const SET_USER_VERSION: &str = "PRAGMA user_version = 1";
const _: () = assert!(SCHEMA_VERSION == 1);

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS photos (
    photo_id         TEXT PRIMARY KEY,
    library_id       TEXT NOT NULL,
    cloud_id         TEXT,
    captured_at      TEXT,
    captured_at_ms   INTEGER,
    ingested_at      TEXT NOT NULL,
    sort_ms          INTEGER NOT NULL,
    deleted_at       TEXT,
    media_type       TEXT NOT NULL,
    media_subtypes   TEXT NOT NULL DEFAULT '[]',
    pixel_width      INTEGER,
    pixel_height     INTEGER,
    orientation      INTEGER,
    duration_ms      INTEGER,
    camera_make      TEXT,
    camera_model     TEXT,
    lat              REAL,
    lon              REAL,
    favorite         INTEGER NOT NULL DEFAULT 0,
    group_id         TEXT,
    group_type       TEXT,
    group_index      INTEGER,
    group_is_pick    INTEGER,
    sidecar_path     TEXT NOT NULL,
    sidecar_mtime_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_photos_browse
    ON photos (library_id, sort_ms DESC, photo_id DESC)
    WHERE deleted_at IS NULL;
CREATE TABLE IF NOT EXISTS resources (
    photo_id      TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    ext           TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    PRIMARY KEY (photo_id, resource_type),
    FOREIGN KEY (photo_id) REFERENCES photos(photo_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_resources_photo ON resources (photo_id);
CREATE TABLE IF NOT EXISTS index_meta (
    library_id   TEXT PRIMARY KEY,
    last_scan_ns INTEGER NOT NULL
);";

struct IndexedLibrary {
    library_id: LibraryId,
    display_name: String,
    shared: bool,
    blob_root: PathBuf,
    sidecar_root: PathBuf,
}

/// A resolved resource: its owning library plus the blob coordinates needed to
/// build the on-disk path via `BlobPaths::blob_path`.
#[derive(Debug, Clone)]
pub struct ResourceLocation {
    pub library_id: String,
    pub content_hash: String,
    pub ext: String,
    pub size_bytes: i64,
}

/// Per-scan counters (logging).
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct IndexStats {
    pub scanned: u64,
    pub parsed: u64,
    pub removed: u64,
    pub photos: u64,
    pub resources: u64,
}

pub struct Index {
    pool: SqlitePool,
    libraries: Vec<IndexedLibrary>,
}

/// Append the `library_id IN (...)` membership clause. Callers guarantee a
/// non-empty, pre-authorized set (routes 403 before this). An empty slice
/// still yields valid SQL (`IN ()` via a FALSE literal) returning no rows.
fn push_library_set(qb: &mut QueryBuilder<Sqlite>, library_ids: &[String]) {
    if library_ids.is_empty() {
        qb.push("0");
        return;
    }
    qb.push("library_id IN (");
    let mut sep = qb.separated(", ");
    for id in library_ids {
        sep.push_bind(id.clone());
    }
    qb.push(")");
}

/// Append `PhotoFilter`'s WHERE clauses. Shared by `list_photos` and
/// `month_histogram` so the rail can never disagree with the grid.
fn push_filter(qb: &mut QueryBuilder<Sqlite>, filter: &PhotoFilter) {
    if let Some(video) = filter.video {
        qb.push(if video {
            " AND media_type = 'video'"
        } else {
            " AND media_type != 'video'"
        });
    }
    if let Some(live) = filter.live {
        qb.push(if live {
            " AND media_type = 'live_photo'"
        } else {
            " AND media_type != 'live_photo'"
        });
    }
    if let Some(raw) = filter.raw {
        // Ground truth is the resource row, not media_subtypes: RAF originals
        // land as a `raw_alternate` resource alongside the JPEG.
        qb.push(if raw { " AND EXISTS" } else { " AND NOT EXISTS" })
            .push(
                " (SELECT 1 FROM resources r WHERE r.photo_id = photos.photo_id \
                 AND r.resource_type = 'raw_alternate')",
            );
    }
    if let Some(fav) = filter.favorite {
        qb.push(" AND favorite = ").push_bind(fav as i64);
    }
}

impl Index {
    /// Open (create if missing) the index DB and reconcile its schema. Mirrors
    /// `StateStore::open`'s PRAGMA template, but allows several connections
    /// (WAL: the refresh writer plus concurrent readers).
    pub async fn open(config: &Config) -> anyhow::Result<Arc<Self>> {
        let options = SqliteConnectOptions::new()
            .filename(&config.index_db)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_millis(5000));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        ensure_schema(&pool).await?;

        let libraries = config
            .libraries
            .iter()
            .map(|l| IndexedLibrary {
                library_id: LibraryId::parse(&l.library_id).expect("validated in Config::load"),
                display_name: l.display_name.clone(),
                shared: l.shared,
                blob_root: l.blob_root.clone(),
                sidecar_root: l.sidecar_root.clone(),
            })
            .collect();
        Ok(Arc::new(Self { pool, libraries }))
    }

    /// Full parse of every sidecar (boot / rebuild).
    pub async fn build(&self) -> anyhow::Result<IndexStats> {
        self.scan(true).await
    }

    /// Incremental pass — re-parse only sidecars newer than the high-water.
    pub async fn refresh(&self) -> anyhow::Result<IndexStats> {
        self.scan(false).await
    }

    async fn scan(&self, full: bool) -> anyhow::Result<IndexStats> {
        let mut stats = IndexStats::default();
        for lib in &self.libraries {
            let lid = lib.library_id.as_str();
            let high_water: i64 = if full {
                0
            } else {
                sqlx::query_scalar("SELECT last_scan_ns FROM index_meta WHERE library_id = ?")
                    .bind(lid)
                    .fetch_optional(&self.pool)
                    .await?
                    .unwrap_or(0)
            };

            let paths = match walk_sidecars(&lib.sidecar_root) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(library = lid, root = %lib.sidecar_root.display(), ?e,
                        "sidecar root unreadable; skipping this pass");
                    continue;
                }
            };

            let mut present: HashSet<String> = HashSet::with_capacity(paths.len());
            let mut max_seen = high_water;
            for path in &paths {
                stats.scanned += 1;
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    present.insert(stem.to_string());
                }
                let mtime = match mtime_ns(path) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!(path = %path.display(), ?e, "stat failed; skipping");
                        continue;
                    }
                };
                max_seen = max_seen.max(mtime);
                if mtime > high_water {
                    match read_and_upsert(&self.pool, lid, path, mtime).await {
                        Ok(()) => stats.parsed += 1,
                        Err(e) => {
                            tracing::warn!(path = %path.display(), ?e, "skipping bad sidecar")
                        }
                    }
                }
            }

            stats.removed += sweep_absent(&self.pool, lid, &present).await?;
            sqlx::query(
                "INSERT INTO index_meta(library_id, last_scan_ns) VALUES(?, ?) \
                 ON CONFLICT(library_id) DO UPDATE SET last_scan_ns = excluded.last_scan_ns",
            )
            .bind(lid)
            .bind(max_seen)
            .execute(&self.pool)
            .await?;
        }
        let (photos, resources) = table_counts(&self.pool).await?;
        stats.photos = photos;
        stats.resources = resources;
        Ok(stats)
    }

    // --- queries (consumed by the REST slice) ------------------------------

    pub async fn libraries(&self) -> anyhow::Result<Vec<LibrarySummary>> {
        let mut out = Vec::with_capacity(self.libraries.len());
        for lib in &self.libraries {
            let row = sqlx::query(
                "SELECT COUNT(*) AS total, \
                 COUNT(*) FILTER (WHERE media_type = 'video') AS videos \
                 FROM photos WHERE library_id = ? AND deleted_at IS NULL",
            )
            .bind(lib.library_id.as_str())
            .fetch_one(&self.pool)
            .await?;
            let total: i64 = row.get("total");
            let videos: i64 = row.get("videos");
            out.push(LibrarySummary {
                library_id: lib.library_id.as_str().to_string(),
                display_name: lib.display_name.clone(),
                shared: lib.shared,
                photo_count: total - videos,
                video_count: videos,
            });
        }
        Ok(out)
    }

    /// List across one or more libraries, fused into a single timeline. The
    /// keyset cursor is on `(sort_ms, photo_id)` — a total order independent
    /// of library, so fusion needs no per-library sub-cursors.
    ///
    /// `dir` walks the timeline relative to the cursor: `Older` (default)
    /// pages downward, `Newer` fetches the items just above it (ASC under the
    /// hood, reversed before return). Items are ALWAYS newest-first either
    /// way. `Newer` with no cursor degenerates to the top of the timeline.
    pub async fn list_photos(
        &self,
        library_ids: &[String],
        cursor: Option<Cursor>,
        limit: u32,
        filter: &PhotoFilter,
        dir: ListDir,
    ) -> anyhow::Result<PhotoPage> {
        let limit = limit.clamp(1, 500);
        let newer = dir == ListDir::Newer && cursor.is_some();
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
            "SELECT photo_id, library_id, captured_at, media_type, pixel_width, pixel_height, \
             orientation, duration_ms, favorite, media_subtypes, group_id, group_type, sort_ms \
             FROM photos WHERE deleted_at IS NULL AND ",
        );
        push_library_set(&mut qb, library_ids);
        push_filter(&mut qb, filter);
        // Keyset on (sort_ms, photo_id), expanded from the row-value tuple
        // form so the bind order is explicit. Comparison + scan order flip
        // together with the direction.
        if let Some(c) = &cursor {
            let (cmp1, cmp2) = if newer { (">", ">") } else { ("<", "<") };
            qb.push(" AND (sort_ms ")
                .push(cmp1)
                .push(" ")
                .push_bind(c.sort_ms)
                .push(" OR (sort_ms = ")
                .push_bind(c.sort_ms)
                .push(" AND photo_id ")
                .push(cmp2)
                .push(" ")
                .push_bind(c.photo_id.clone())
                .push("))");
        }
        qb.push(if newer {
            " ORDER BY sort_ms ASC, photo_id ASC LIMIT "
        } else {
            " ORDER BY sort_ms DESC, photo_id DESC LIMIT "
        })
        .push_bind(limit as i64 + 1);

        let rows = qb.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > limit as usize;
        let kept = &rows[..rows.len().min(limit as usize)];

        let mut items: Vec<PhotoSummary> = kept
            .iter()
            .map(|r| {
                let media_type: String = r.get("media_type");
                let subtypes: String = r.get("media_subtypes");
                PhotoSummary {
                    photo_id: r.get("photo_id"),
                    library_id: r.get("library_id"),
                    sort_ms: r.get("sort_ms"),
                    captured_at: r.get("captured_at"),
                    is_live_photo: media_type == "live_photo",
                    media_type,
                    pixel_width: r.get("pixel_width"),
                    pixel_height: r.get("pixel_height"),
                    orientation: r.get("orientation"),
                    duration_ms: r.get("duration_ms"),
                    favorite: r.get::<i64, _>("favorite") != 0,
                    media_subtypes: serde_json::from_str(&subtypes).unwrap_or_default(),
                    group_id: r.get("group_id"),
                    group_type: r.get("group_type"),
                }
            })
            .collect();
        // Newer pages scan ASC — restore the invariant order for the client.
        if newer {
            items.reverse();
        }

        // Continuation in the SAME direction: the scan-order last row kept
        // (for Newer that's the newest item, i.e. items[0] post-reverse).
        let next_cursor = if has_more {
            kept.last().map(|r| {
                Cursor {
                    sort_ms: r.get("sort_ms"),
                    photo_id: r.get("photo_id"),
                }
                .to_token()
            })
        } else {
            None
        };
        Ok(PhotoPage { items, next_cursor })
    }

    /// Photo counts per calendar month over `sort_ms`, newest month first,
    /// honoring the same filters as `list_photos` so the histogram rail always
    /// mirrors what the grid can actually reach.
    pub async fn month_histogram(
        &self,
        library_ids: &[String],
        filter: &PhotoFilter,
    ) -> anyhow::Result<Vec<MonthBucket>> {
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
            "SELECT strftime('%Y-%m', sort_ms / 1000, 'unixepoch') AS month, COUNT(*) AS n \
             FROM photos WHERE deleted_at IS NULL AND ",
        );
        push_library_set(&mut qb, library_ids);
        push_filter(&mut qb, filter);
        qb.push(" GROUP BY month ORDER BY month DESC");
        let rows = qb.build().fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|r| MonthBucket {
                month: r.get("month"),
                count: r.get("n"),
            })
            .collect())
    }

    pub async fn photo_detail(&self, photo_id: &str) -> anyhow::Result<Option<PhotoDetail>> {
        let Some(r) = sqlx::query("SELECT * FROM photos WHERE photo_id = ?")
            .bind(photo_id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let subtypes: String = r.get("media_subtypes");
        let resources = sqlx::query(
            "SELECT resource_type, content_hash, ext, size_bytes FROM resources \
             WHERE photo_id = ? ORDER BY resource_type",
        )
        .bind(photo_id)
        .fetch_all(&self.pool)
        .await?
        .iter()
        .map(|rr| ResourceInfo {
            resource_type: rr.get("resource_type"),
            content_hash: rr.get("content_hash"),
            ext: rr.get("ext"),
            size_bytes: rr.get("size_bytes"),
        })
        .collect();

        Ok(Some(PhotoDetail {
            photo_id: r.get("photo_id"),
            library_id: r.get("library_id"),
            cloud_id: r.get("cloud_id"),
            captured_at: r.get("captured_at"),
            ingested_at: r.get("ingested_at"),
            media_type: r.get("media_type"),
            media_subtypes: serde_json::from_str(&subtypes).unwrap_or_default(),
            pixel_width: r.get("pixel_width"),
            pixel_height: r.get("pixel_height"),
            orientation: r.get("orientation"),
            duration_ms: r.get("duration_ms"),
            camera_make: r.get("camera_make"),
            camera_model: r.get("camera_model"),
            lat: r.get("lat"),
            lon: r.get("lon"),
            favorite: r.get::<i64, _>("favorite") != 0,
            group_id: r.get("group_id"),
            group_type: r.get("group_type"),
            group_index: r.get("group_index"),
            group_is_pick: r.get::<Option<i64>, _>("group_is_pick").map(|v| v != 0),
            resources,
        }))
    }

    /// Resolve a photo's resource to its owning library + blob coordinates. The
    /// join also gates cross-library `photo_id` enumeration (the library comes
    /// from the row, not the caller). `Ok(None)` when the photo or the named
    /// resource is absent or the photo is tombstoned → the route maps that to 404.
    pub async fn resource_blob(
        &self,
        photo_id: &str,
        resource_type: &str,
    ) -> anyhow::Result<Option<ResourceLocation>> {
        let row = sqlx::query(
            "SELECT p.library_id AS library_id, r.content_hash AS content_hash, \
             r.ext AS ext, r.size_bytes AS size_bytes \
             FROM resources r JOIN photos p ON p.photo_id = r.photo_id \
             WHERE r.photo_id = ? AND r.resource_type = ? AND p.deleted_at IS NULL",
        )
        .bind(photo_id)
        .bind(resource_type)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| ResourceLocation {
            library_id: r.get("library_id"),
            content_hash: r.get("content_hash"),
            ext: r.get("ext"),
            size_bytes: r.get("size_bytes"),
        }))
    }

    /// Blob-path resolver for the Renderer slice: `<blob_root>/blobs/aa/bb/<hash>.<ext>`.
    pub fn blob_paths(&self, library_id: &str) -> Option<BlobPaths> {
        self.libraries
            .iter()
            .find(|l| l.library_id.as_str() == library_id)
            .map(|l| BlobPaths::new(&l.blob_root))
    }
}

// --- schema + scan helpers -------------------------------------------------

async fn ensure_schema(pool: &SqlitePool) -> anyhow::Result<()> {
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(pool)
        .await?;
    if version != SCHEMA_VERSION {
        // Disposable cache: drop and rebuild from the sidecar tree.
        sqlx::raw_sql(
            "DROP TABLE IF EXISTS resources; \
             DROP TABLE IF EXISTS photos; \
             DROP TABLE IF EXISTS index_meta;",
        )
        .execute(pool)
        .await?;
    }
    sqlx::raw_sql(SCHEMA).execute(pool).await?;
    sqlx::raw_sql(SET_USER_VERSION).execute(pool).await?;
    Ok(())
}

/// Parse one sidecar and upsert its photo row + resource set in a transaction.
async fn read_and_upsert(
    pool: &SqlitePool,
    library_id: &str,
    path: &Path,
    mtime_ns: i64,
) -> anyhow::Result<()> {
    let json = std::fs::read_to_string(path)?;
    let doc = Sidecar::from_json(&json)?;

    let media_type = match doc.media_type {
        MediaType::Image => "image",
        MediaType::Video => "video",
        MediaType::LivePhoto => "live_photo",
    };
    let captured_at = doc.captured_at.map(|t| t.to_rfc3339());
    let captured_ms = doc.captured_at.map(|t| t.timestamp_millis());
    let ingested_at = doc.ingested_at.to_rfc3339();
    let sort_ms = captured_ms.unwrap_or_else(|| doc.ingested_at.timestamp_millis());
    let deleted_at = doc.deleted_at.map(|t| t.to_rfc3339());
    let subtypes = serde_json::to_string(&doc.media_subtypes)?;
    let (cam_make, cam_model) = doc
        .camera
        .as_ref()
        .map(|c| (c.make.clone(), c.model.clone()))
        .unwrap_or((None, None));
    let (lat, lon) = doc
        .location
        .as_ref()
        .map(|l| (Some(l.lat), Some(l.lon)))
        .unwrap_or((None, None));
    let group_id = doc.group.as_ref().map(|g| g.id.clone());
    let group_type = doc.group.as_ref().map(|g| g.group_type.clone());
    let group_index = doc.group.as_ref().and_then(|g| g.index);
    let group_is_pick = doc.group.as_ref().map(|g| g.is_pick as i64);

    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO photos (photo_id, library_id, cloud_id, captured_at, captured_at_ms, \
         ingested_at, sort_ms, deleted_at, media_type, media_subtypes, pixel_width, pixel_height, \
         orientation, duration_ms, camera_make, camera_model, lat, lon, favorite, group_id, \
         group_type, group_index, group_is_pick, sidecar_path, sidecar_mtime_ns) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
         ON CONFLICT(photo_id) DO UPDATE SET \
         library_id=excluded.library_id, cloud_id=excluded.cloud_id, \
         captured_at=excluded.captured_at, captured_at_ms=excluded.captured_at_ms, \
         ingested_at=excluded.ingested_at, sort_ms=excluded.sort_ms, \
         deleted_at=excluded.deleted_at, media_type=excluded.media_type, \
         media_subtypes=excluded.media_subtypes, pixel_width=excluded.pixel_width, \
         pixel_height=excluded.pixel_height, orientation=excluded.orientation, \
         duration_ms=excluded.duration_ms, camera_make=excluded.camera_make, \
         camera_model=excluded.camera_model, lat=excluded.lat, lon=excluded.lon, \
         favorite=excluded.favorite, group_id=excluded.group_id, group_type=excluded.group_type, \
         group_index=excluded.group_index, group_is_pick=excluded.group_is_pick, \
         sidecar_path=excluded.sidecar_path, sidecar_mtime_ns=excluded.sidecar_mtime_ns",
    )
    .bind(doc.photo_id.as_str())
    .bind(library_id)
    .bind(doc.cloud_id.as_deref())
    .bind(captured_at)
    .bind(captured_ms)
    .bind(ingested_at)
    .bind(sort_ms)
    .bind(deleted_at)
    .bind(media_type)
    .bind(subtypes)
    .bind(doc.pixel_width.map(|v| v as i64))
    .bind(doc.pixel_height.map(|v| v as i64))
    .bind(doc.orientation.map(|v| v as i64))
    .bind(doc.duration_ms.map(|v| v as i64))
    .bind(cam_make)
    .bind(cam_model)
    .bind(lat)
    .bind(lon)
    .bind(doc.favorite as i64)
    .bind(group_id)
    .bind(group_type)
    .bind(group_index)
    .bind(group_is_pick)
    .bind(path.to_string_lossy().into_owned())
    .bind(mtime_ns)
    .execute(&mut *tx)
    .await?;

    // Resource set can change across edits — replace wholesale.
    sqlx::query("DELETE FROM resources WHERE photo_id = ?")
        .bind(doc.photo_id.as_str())
        .execute(&mut *tx)
        .await?;
    for res in &doc.resources {
        sqlx::query(
            "INSERT INTO resources (photo_id, resource_type, content_hash, ext, size_bytes) \
             VALUES (?,?,?,?,?)",
        )
        .bind(doc.photo_id.as_str())
        .bind(&res.resource_type)
        .bind(&res.content_hash)
        .bind(&res.ext)
        .bind(res.size_bytes)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Drop rows whose sidecar file is gone (parse-free; deletions cascade to
/// `resources`). Fetching the library's ids and diffing in Rust avoids a huge
/// `NOT IN (...)` bind; deletions are rare (library move / hard delete).
async fn sweep_absent(
    pool: &SqlitePool,
    library_id: &str,
    present: &HashSet<String>,
) -> anyhow::Result<u64> {
    let existing: Vec<String> =
        sqlx::query_scalar("SELECT photo_id FROM photos WHERE library_id = ?")
            .bind(library_id)
            .fetch_all(pool)
            .await?;
    let mut removed = 0u64;
    for id in existing {
        if !present.contains(&id) {
            sqlx::query("DELETE FROM photos WHERE photo_id = ?")
                .bind(&id)
                .execute(pool)
                .await?;
            removed += 1;
        }
    }
    Ok(removed)
}

async fn table_counts(pool: &SqlitePool) -> anyhow::Result<(u64, u64)> {
    let photos: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM photos")
        .fetch_one(pool)
        .await?;
    let resources: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resources")
        .fetch_one(pool)
        .await?;
    Ok((photos as u64, resources as u64))
}

fn mtime_ns(path: &Path) -> anyhow::Result<i64> {
    let meta = std::fs::metadata(path)?;
    let dur = meta
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Ok(dur.as_nanos() as i64)
}
