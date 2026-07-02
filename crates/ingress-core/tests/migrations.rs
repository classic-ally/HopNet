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
        ]
    );
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
    assert_eq!(n, 1);

    std::fs::remove_dir_all(&dir).ok();
}
