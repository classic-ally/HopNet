//! Migration behavior.

use ingress_core::StateStore;
use ingress_core::fixtures::store_with_personal;

// Impact: state.db lives across many daemon versions pre-1.0; migration
// idempotence is what lets every open() run the migrator unconditionally.
// Should: create all five tables and their indexes on a fresh database.
#[tokio::test]
async fn fresh_migrate_creates_schema() {
    let (store, _) = store_with_personal().await;

    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '_sqlx%' ORDER BY name",
    )
    .fetch_all(store.raw_pool())
    .await
    .unwrap();
    let names: Vec<&str> = tables.iter().map(|(n,)| n.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "blobs",
            "ingest_log",
            "libraries",
            "photo_resources",
            "photos"
        ]
    );

    let indexes: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_%' ORDER BY name",
    )
    .fetch_all(store.raw_pool())
    .await
    .unwrap();
    let names: Vec<&str> = indexes.iter().map(|(n,)| n.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "idx_ingest_log_photo",
            "idx_photo_resources_hash",
            "idx_photos_deleted",
            "idx_photos_group",
            "idx_photos_library",
            "idx_photos_pending",
            "idx_photos_unpublished",
        ]
    );
}

// Should: carry the consensus_photo_id adoption column on a migrated photos
// table, NULL by default (self-published identity).
#[tokio::test]
async fn consensus_adoption_column_exists_and_defaults_null() {
    let (store, _) = store_with_personal().await;
    sqlx::query(
        "INSERT INTO photos (photo_id, library_id, discovered_at) VALUES ('p1', 'personal', ?)",
    )
    .bind(chrono::Utc::now())
    .execute(store.raw_pool())
    .await
    .unwrap();
    let (adopted,): (Option<String>,) =
        sqlx::query_as("SELECT consensus_photo_id FROM photos WHERE photo_id = 'p1'")
            .fetch_one(store.raw_pool())
            .await
            .unwrap();
    assert!(adopted.is_none());
}

// Should: re-run the migrator on an already-migrated database as a no-op.
#[tokio::test]
async fn migrate_is_idempotent() {
    let dir = std::env::temp_dir().join(format!("ingress-core-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let first = StateStore::open(&path).await.expect("first open");
    drop(first);
    let second = StateStore::open(&path)
        .await
        .expect("second open (re-migrate)");
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(second.raw_pool())
        .await
        .unwrap();
    assert_eq!(n, 5);

    std::fs::remove_dir_all(&dir).ok();
}

// Impact: existing archives (real ones on the macbook) only get thumbnails
// through this backfill — the reconciliation scan probes unchanged photos
// Done and never re-delivers descriptors, so without it types 5/6 would
// never exist for pre-rendition photos.
// Should: mint pending 5/6 rows for materialized library-bound photos and
// clear materialized_at (drain-eligible); re-running the SQL is a no-op.
// Should not: touch tombstoned, unmapped-scope, or local_id-less photos;
// should not reset published_at.
#[tokio::test]
async fn thumbnail_backfill_requeues_materialized_photos() {
    use chrono::Utc;
    use ingress_core::fixtures::store_with_personal;

    let (store, _lib) = store_with_personal().await;
    let now = Utc::now();

    // Four photos: eligible+published, tombstoned, unmapped, unattached.
    let mk = |id: &str| (id.to_string(), format!("cloud-{id}"), format!("local-{id}"));
    for (id, cloud, local) in [mk("eligible"), mk("dead"), mk("unmapped"), mk("recovered")] {
        sqlx::query(
            "INSERT INTO photos (photo_id, library_id, cloud_id, local_id, discovered_at, \
             materialized_at, published_at, deleted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(if id == "unmapped" { None } else { Some("personal") })
        .bind(&cloud)
        .bind(if id == "recovered" { None } else { Some(local) })
        .bind(now)
        .bind(now)
        .bind(if id == "eligible" { Some(now) } else { None })
        .bind(if id == "dead" { Some(now) } else { None })
        .execute(store.raw_pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO photo_resources (photo_id, resource_type, content_hash, ext, \
             size_bytes, written_at) VALUES (?, 0, 'aa11', 'heic', 10, ?)",
        )
        .bind(&id)
        .bind(now)
        .execute(store.raw_pool())
        .await
        .unwrap();
    }

    let sql = include_str!("../migrations/1785513600_thumbnail_backfill.sql");
    for _ in 0..2 {
        // Twice: idempotence.
        sqlx::raw_sql(sql).execute(store.raw_pool()).await.unwrap();
    }

    let thumbs: Vec<(String, i64)> = sqlx::query_as(
        "SELECT photo_id, resource_type FROM photo_resources WHERE resource_type IN (5, 6) \
         ORDER BY photo_id, resource_type",
    )
    .fetch_all(store.raw_pool())
    .await
    .unwrap();
    assert_eq!(
        thumbs,
        vec![("eligible".to_string(), 5), ("eligible".to_string(), 6)],
        "only the eligible photo gains thumbnail rows"
    );

    let (materialized, published): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT materialized_at, published_at FROM photos WHERE photo_id = 'eligible'",
    )
    .fetch_one(store.raw_pool())
    .await
    .unwrap();
    assert!(materialized.is_none(), "re-queued for drain");
    assert!(published.is_some(), "published_at untouched");
}
