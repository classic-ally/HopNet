//! Sidecar-tree index behavior (build, incremental refresh, keyset queries).
//! Pure Rust: fixtures are real sidecar documents written through
//! `ingress_core::sidecar_io::write_sidecar_local`, so the YYYY/MM bucketing
//! and JSON round-trip match production exactly.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use ingress_core::descriptor::MediaType;
use ingress_core::sidecar::{SIDECAR_SCHEMA_V1, Sidecar, SidecarGroup, SidecarResource};
use ingress_core::sidecar_io::write_sidecar_local;
use ingress_core::{LibraryId, PhotoId};

use ingress_server::config::{Config, LibraryEntry};
use ingress_server::dto::{Cursor, PhotoFilter};
use ingress_server::index::Index;

// ------------------------------------------------------------------ fixtures

struct Rig {
    _tmp: tempfile::TempDir,
    sidecar_root: std::path::PathBuf,
    index_db: std::path::PathBuf,
    library_id: String,
}

impl Rig {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let sidecar_root = tmp.path().join("sidecars");
        let index_db = tmp.path().join("index.db");
        std::fs::create_dir_all(&sidecar_root).unwrap();
        Rig {
            _tmp: tmp,
            sidecar_root,
            index_db,
            library_id: "test_lib".to_string(),
        }
    }

    fn config(&self) -> Config {
        Config {
            bind: "127.0.0.1:0".parse().unwrap(),
            cache_dir: self.sidecar_root.join("cache"),
            index_db: self.index_db.clone(),
            refresh_interval_secs: 60,
            libraries: vec![LibraryEntry {
                library_id: self.library_id.clone(),
                display_name: "Test".to_string(),
                blob_root: self.sidecar_root.clone(),
                sidecar_root: self.sidecar_root.clone(),
            }],
            oidc: None,
        }
    }

    async fn open(&self) -> Arc<Index> {
        Index::open(&self.config()).await.unwrap()
    }

    /// Write a sidecar into this rig's library tree, then stamp its mtime so
    /// incremental refresh is deterministic.
    fn write(&self, sidecar: &Sidecar, mtime_secs: u64) {
        let path = write_sidecar_local(&self.sidecar_root, sidecar).unwrap();
        set_mtime(&path, mtime_secs);
    }
}

fn set_mtime(path: &Path, epoch_secs: u64) {
    let t = UNIX_EPOCH + Duration::from_secs(epoch_secs);
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(t)
        .unwrap();
}

fn res(rt: &str, hash: &str, ext: &str) -> SidecarResource {
    SidecarResource {
        resource_type: rt.to_string(),
        content_hash: hash.to_string(),
        ext: ext.to_string(),
        size_bytes: 1234,
    }
}

/// A minimal image sidecar; `captured` is an RFC3339 string (or None → the
/// ingested_at fallback drives ordering).
fn sidecar(
    photo_id: &str,
    library: &str,
    captured: Option<&str>,
    media: MediaType,
    favorite: bool,
    resources: Vec<SidecarResource>,
) -> Sidecar {
    Sidecar {
        schema: SIDECAR_SCHEMA_V1.to_string(),
        photo_id: PhotoId::from_string(photo_id),
        library_id: LibraryId::new(library),
        cloud_id: None,
        ingested_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        deleted_at: None,
        captured_at: captured.map(|s| DateTime::parse_from_rfc3339(s).unwrap()),
        media_type: media,
        media_subtypes: Vec::new(),
        pixel_width: Some(4032),
        pixel_height: Some(3024),
        orientation: Some(1),
        duration_ms: None,
        camera: None,
        location: None,
        favorite,
        group: None,
        resources,
    }
}

// --------------------------------------------------------------------- tests

// Impact: the index row-count is the viewer's ground truth for "what exists".
// Should: index every sidecar across libraries; photos + resources counts match.
#[tokio::test]
async fn build_index_row_counts() {
    let rig = Rig::new();
    for i in 0..5 {
        rig.write(
            &sidecar(
                &format!("p{i}"),
                &rig.library_id,
                Some("2020-06-01T12:00:00Z"),
                MediaType::Image,
                false,
                vec![res("original", &format!("h{i}"), "jpg")],
            ),
            1000,
        );
    }
    let index = rig.open().await;
    let stats = index.build().await.unwrap();
    assert_eq!(stats.photos, 5);
    assert_eq!(stats.resources, 5);
    assert_eq!(stats.parsed, 5);
}

// Impact: a dropped resource means a photo can't render its bytes.
// Should: round-trip all display-bearing fields, incl. the paired_video resource.
// Should not: lose the second resource of a live photo.
#[tokio::test]
async fn build_index_maps_all_fields() {
    let rig = Rig::new();
    let mut sc = sidecar(
        "live1",
        &rig.library_id,
        Some("2019-08-14T16:22:03+02:00"),
        MediaType::LivePhoto,
        true,
        vec![
            res("original", "abc", "heic"),
            res("paired_video", "def", "mov"),
        ],
    );
    sc.camera = Some(ingress_core::descriptor::Camera {
        make: Some("Apple".into()),
        model: Some("iPhone 15 Pro".into()),
    });
    sc.location = Some(ingress_core::descriptor::Location {
        lat: 45.5017,
        lon: -73.5673,
    });
    sc.group = Some(SidecarGroup {
        id: "g1".into(),
        group_type: "burst".into(),
        index: Some(3),
        is_pick: true,
    });
    rig.write(&sc, 1000);

    let index = rig.open().await;
    index.build().await.unwrap();
    let d = index.photo_detail("live1").await.unwrap().expect("present");
    assert_eq!(d.media_type, "live_photo");
    assert!(d.favorite);
    assert_eq!(d.camera_model.as_deref(), Some("iPhone 15 Pro"));
    assert_eq!(d.lat, Some(45.5017));
    assert_eq!(d.group_type.as_deref(), Some("burst"));
    assert_eq!(d.group_is_pick, Some(true));
    assert_eq!(d.resources.len(), 2);
    assert!(
        d.resources
            .iter()
            .any(|r| r.resource_type == "paired_video")
    );
}

// Impact: refresh must not re-parse 36k files each tick.
// Should: only the newly-written sidecar is parsed on refresh.
#[tokio::test]
async fn incremental_refresh_picks_up_new_sidecar() {
    let rig = Rig::new();
    rig.write(
        &sidecar(
            "old",
            &rig.library_id,
            Some("2020-01-01T00:00:00Z"),
            MediaType::Image,
            false,
            vec![],
        ),
        1000,
    );
    let index = rig.open().await;
    index.build().await.unwrap();

    rig.write(
        &sidecar(
            "new",
            &rig.library_id,
            Some("2021-01-01T00:00:00Z"),
            MediaType::Image,
            false,
            vec![],
        ),
        2000, // strictly newer mtime
    );
    let stats = index.refresh().await.unwrap();
    assert_eq!(stats.parsed, 1, "only the new sidecar re-parsed");
    assert!(index.photo_detail("new").await.unwrap().is_some());
}

// Should: a refresh with no filesystem change re-parses nothing.
#[tokio::test]
async fn incremental_refresh_ignores_unchanged() {
    let rig = Rig::new();
    rig.write(
        &sidecar(
            "p",
            &rig.library_id,
            Some("2020-01-01T00:00:00Z"),
            MediaType::Image,
            false,
            vec![],
        ),
        1000,
    );
    let index = rig.open().await;
    index.build().await.unwrap();
    let stats = index.refresh().await.unwrap();
    assert_eq!(stats.parsed, 0);
}

// Impact: a tombstone must disappear from the browse view.
// Should: an in-place rewrite with deleted_at set drops the photo from listings.
#[tokio::test]
async fn refresh_updates_tombstone() {
    let rig = Rig::new();
    let mut sc = sidecar(
        "t",
        &rig.library_id,
        Some("2020-01-01T00:00:00Z"),
        MediaType::Image,
        false,
        vec![],
    );
    rig.write(&sc, 1000);
    let index = rig.open().await;
    index.build().await.unwrap();

    sc.deleted_at = Some(Utc::now());
    rig.write(&sc, 2000); // rewrite in place, newer mtime
    index.refresh().await.unwrap();

    let page = index
        .list_photos(&rig.library_id, None, 50, &PhotoFilter::default())
        .await
        .unwrap();
    assert!(page.items.iter().all(|p| p.photo_id != "t"));
}

// Impact: a deleted sidecar (library move) must not linger in the index.
// Should: the membership sweep removes a row whose file vanished.
#[tokio::test]
async fn refresh_removes_deleted_sidecar() {
    let rig = Rig::new();
    let sc = sidecar(
        "gone",
        &rig.library_id,
        Some("2020-01-01T00:00:00Z"),
        MediaType::Image,
        false,
        vec![],
    );
    let path = write_sidecar_local(&rig.sidecar_root, &sc).unwrap();
    set_mtime(&path, 1000);
    let index = rig.open().await;
    index.build().await.unwrap();

    std::fs::remove_file(&path).unwrap();
    let stats = index.refresh().await.unwrap();
    assert_eq!(stats.removed, 1);
    assert!(index.photo_detail("gone").await.unwrap().is_none());
}

// Impact: cursor correctness is the API's core guarantee.
// Should: paging by next_cursor yields every photo exactly once, DESC, no dupes.
// Should not: emit a next_cursor on the final page.
#[tokio::test]
async fn keyset_pagination_returns_each_photo_once() {
    let rig = Rig::new();
    for i in 0..25 {
        // distinct captured times → deterministic DESC order
        let captured = format!("2020-01-{:02}T00:00:00Z", i + 1);
        rig.write(
            &sidecar(
                &format!("p{i:02}"),
                &rig.library_id,
                Some(&captured),
                MediaType::Image,
                false,
                vec![],
            ),
            1000,
        );
    }
    let index = rig.open().await;
    index.build().await.unwrap();

    let mut seen = Vec::new();
    let mut cursor: Option<Cursor> = None;
    loop {
        let page = index
            .list_photos(&rig.library_id, cursor.clone(), 10, &PhotoFilter::default())
            .await
            .unwrap();
        seen.extend(page.items.iter().map(|p| p.photo_id.clone()));
        match page.next_cursor {
            Some(tok) => cursor = Some(Cursor::from_token(&tok).unwrap()),
            None => break,
        }
    }
    assert_eq!(seen.len(), 25);
    let unique: std::collections::HashSet<_> = seen.iter().collect();
    assert_eq!(unique.len(), 25, "no duplicates across pages");
    // DESC by captured time → p24 first, p00 last.
    assert_eq!(seen.first().unwrap(), "p24");
    assert_eq!(seen.last().unwrap(), "p00");
}

// Should: photos with no captured_at still list, ordered by the ingested_at fallback.
#[tokio::test]
async fn list_photos_orders_null_captured_by_ingested() {
    let rig = Rig::new();
    rig.write(
        &sidecar(
            "nocap",
            &rig.library_id,
            None,
            MediaType::Image,
            false,
            vec![],
        ),
        1000,
    );
    let index = rig.open().await;
    index.build().await.unwrap();
    let page = index
        .list_photos(&rig.library_id, None, 50, &PhotoFilter::default())
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].photo_id, "nocap");
}

// Should: media_type + favorite filters narrow results.
#[tokio::test]
async fn list_photos_filters_media_type_and_favorite() {
    let rig = Rig::new();
    rig.write(
        &sidecar(
            "img",
            &rig.library_id,
            Some("2020-01-01T00:00:00Z"),
            MediaType::Image,
            false,
            vec![],
        ),
        1000,
    );
    rig.write(
        &sidecar(
            "vid",
            &rig.library_id,
            Some("2020-01-02T00:00:00Z"),
            MediaType::Video,
            false,
            vec![],
        ),
        1000,
    );
    rig.write(
        &sidecar(
            "fav",
            &rig.library_id,
            Some("2020-01-03T00:00:00Z"),
            MediaType::Image,
            true,
            vec![],
        ),
        1000,
    );
    let index = rig.open().await;
    index.build().await.unwrap();

    let videos = index
        .list_photos(
            &rig.library_id,
            None,
            50,
            &PhotoFilter {
                media_type: Some("video".into()),
                favorite: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(videos.items.len(), 1);
    assert_eq!(videos.items[0].photo_id, "vid");

    let favs = index
        .list_photos(
            &rig.library_id,
            None,
            50,
            &PhotoFilter {
                media_type: None,
                favorite: Some(true),
            },
        )
        .await
        .unwrap();
    assert_eq!(favs.items.len(), 1);
    assert_eq!(favs.items[0].photo_id, "fav");
}

// Should: an unknown photo_id resolves to Ok(None), never an error/panic.
#[tokio::test]
async fn photo_detail_absent_returns_none() {
    let rig = Rig::new();
    let index = rig.open().await;
    index.build().await.unwrap();
    assert!(index.photo_detail("nope").await.unwrap().is_none());
}

// Should: a tombstoned photo is excluded from the library count.
#[tokio::test]
async fn libraries_counts_exclude_deleted() {
    let rig = Rig::new();
    rig.write(
        &sidecar(
            "live",
            &rig.library_id,
            Some("2020-01-01T00:00:00Z"),
            MediaType::Image,
            false,
            vec![],
        ),
        1000,
    );
    let mut dead = sidecar(
        "dead",
        &rig.library_id,
        Some("2020-01-02T00:00:00Z"),
        MediaType::Image,
        false,
        vec![],
    );
    dead.deleted_at = Some(Utc::now());
    rig.write(&dead, 1000);
    let index = rig.open().await;
    index.build().await.unwrap();
    let libs = index.libraries().await.unwrap();
    assert_eq!(libs.len(), 1);
    assert_eq!(libs[0].count, 1, "tombstone excluded");
}

// Should: reopening and rebuilding is idempotent (upsert PK dedup, no doubling).
#[tokio::test]
async fn reopen_and_rebuild_is_idempotent() {
    let rig = Rig::new();
    for i in 0..3 {
        rig.write(
            &sidecar(
                &format!("p{i}"),
                &rig.library_id,
                Some("2020-01-01T00:00:00Z"),
                MediaType::Image,
                false,
                vec![],
            ),
            1000,
        );
    }
    let first = rig.open().await.build().await.unwrap();
    let second = rig.open().await.build().await.unwrap();
    assert_eq!(first.photos, second.photos);
    assert_eq!(second.photos, 3);
}

// Should: config parses libraries. Should not: accept an invalid library_id.
#[test]
fn config_load_rejects_bad_library_id() {
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("good.toml");
    std::fs::write(
        &good,
        "bind = \"0.0.0.0:8080\"\ncache_dir = \"/c\"\nindex_db = \"/i.db\"\n\
         [[libraries]]\nlibrary_id = \"ok_lib\"\ndisplay_name = \"X\"\n\
         blob_root = \"/b\"\nsidecar_root = \"/s\"\n",
    )
    .unwrap();
    assert!(Config::load(&good).is_ok());

    let bad = dir.path().join("bad.toml");
    std::fs::write(
        &bad,
        "bind = \"0.0.0.0:8080\"\ncache_dir = \"/c\"\nindex_db = \"/i.db\"\n\
         [[libraries]]\nlibrary_id = \"Bad/Id\"\ndisplay_name = \"X\"\n\
         blob_root = \"/b\"\nsidecar_root = \"/s\"\n",
    )
    .unwrap();
    assert!(Config::load(&bad).is_err());
}
