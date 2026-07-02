//! Library configuration commands (spec §Storage root configuration,
//! Phase 6): generated immutable ids, scope binding, display-name rename,
//! retention edits — all under the exclusive run lock.

use ingress_core::libconfig::{
    AddLibraryOptions, add_library, bind_scope, rename_library, set_retention,
};
use ingress_core::model::ICLOUD_SHARED_LIBRARY_BINDING;
use ingress_core::paths::DataDir;
use ingress_core::{LibraryId, LibraryScope, StateStore};

async fn rig(tmp: &std::path::Path) -> (StateStore, DataDir) {
    let store = StateStore::open(&tmp.join("state.db")).await.unwrap();
    let data_dir = DataDir::new(tmp.join("data"));
    std::fs::create_dir_all(data_dir.root()).unwrap();
    (store, data_dir)
}

fn opts(tmp: &std::path::Path, scope: LibraryScope) -> AddLibraryOptions {
    AddLibraryOptions {
        id: None,
        display_name: None,
        blob_root: tmp.join("blobs").to_string_lossy().into_owned(),
        sidecar_root_remote: None,
        scope,
        retention_days: 30,
    }
}

// Impact: the add command mints the id every other artifact keys on; wrong
// defaults or accepted-but-invalid input become permanent (ids are
// immutable by design).
// Should: generate a parseable two-word id, default the display name by
// scope, set the shared marker, and surface the no-remote warning.
// Should not: accept a relative blob root or negative retention.
#[tokio::test]
async fn add_generates_id_and_validates() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir) = rig(tmp.path()).await;

    let added = add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Personal))
        .await
        .unwrap();
    assert!(LibraryId::parse(added.config.library_id.as_str()).is_ok());
    assert!(added.config.library_id.as_str().contains('_'));
    assert_eq!(added.config.display_name, "Personal Library");
    assert!(added.config.scope_binding.is_none());
    assert!(added.warn_no_remote, "no remote root => loud warning flag");

    let mut shared_opts = opts(tmp.path(), LibraryScope::Shared);
    shared_opts.sidecar_root_remote =
        Some(tmp.path().join("remote").to_string_lossy().into_owned());
    let shared = add_library(&store, &data_dir, &shared_opts).await.unwrap();
    assert_eq!(
        shared.config.scope_binding.as_deref(),
        Some(ICLOUD_SHARED_LIBRARY_BINDING)
    );
    assert!(!shared.warn_no_remote);
    assert_eq!(store.log_events("library_added").await.unwrap().len(), 2);

    let mut relative = opts(tmp.path(), LibraryScope::Personal);
    relative.blob_root = "relative/path".into();
    assert!(add_library(&store, &data_dir, &relative).await.is_err());

    let mut negative = opts(tmp.path(), LibraryScope::Personal);
    negative.retention_days = -1;
    assert!(add_library(&store, &data_dir, &negative).await.is_err());
}

// Impact: two NULL-scope rows make personal routing arbitrary (LIMIT 1);
// two shared markers are impossible per the UNIQUE constraint but must
// fail with a message, not a raw constraint error.
// Should: refuse a second personal, a second shared, and a duplicate --id.
#[tokio::test]
async fn add_enforces_singleton_invariants() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir) = rig(tmp.path()).await;

    add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Personal))
        .await
        .unwrap();
    let err = add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Personal))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("personal library already exists"));

    add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Shared))
        .await
        .unwrap();
    let err = add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Shared))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already bound"));

    // Duplicate explicit id.
    let mut with_id = opts(tmp.path(), LibraryScope::Shared);
    with_id.id = Some(store.libraries().await.unwrap()[0].library_id.clone());
    let err = add_library(&store, &data_dir, &with_id).await.unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

// Impact: bind is how a scope moves between libraries; unbinding the wrong
// way silently creates a personal-routing ambiguity that misfiles photos.
// Should: move the shared marker, refuse a conflicting bind with a
// friendly message, refuse an unbind that creates a second NULL-scope row.
#[tokio::test]
async fn bind_moves_marker_and_guards_ambiguity() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir) = rig(tmp.path()).await;
    let personal = add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Personal))
        .await
        .unwrap()
        .config
        .library_id;
    let shared = add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Shared))
        .await
        .unwrap()
        .config
        .library_id;

    // Binding shared onto the personal library while another holds it: refused.
    let err = bind_scope(&store, &data_dir, &personal, Some(LibraryScope::Shared))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already bound to"));

    // Unbinding shared: refused (personal NULL-scope row exists).
    let err = bind_scope(&store, &data_dir, &shared, None)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("second personal-routing candidate")
    );

    // Rebinding the current holder is a no-op-shaped success.
    bind_scope(&store, &data_dir, &shared, Some(LibraryScope::Shared))
        .await
        .unwrap();
    assert_eq!(store.log_events("library_bound").await.unwrap().len(), 1);

    // Unknown library.
    let err = bind_scope(
        &store,
        &data_dir,
        &LibraryId::new("no_such"),
        Some(LibraryScope::Shared),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("no library"));
}

// Impact: rename must touch ONLY the mutable label — the id is embedded in
// every sidecar document and path; retention gates irreversible deletes.
// Should: update display_name / retention_days and log old→new.
// Should not: change the library_id.
#[tokio::test]
async fn rename_and_retention_update_and_log() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir) = rig(tmp.path()).await;
    let id = add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Personal))
        .await
        .unwrap()
        .config
        .library_id;

    rename_library(&store, &data_dir, &id, "Allison's Archive")
        .await
        .unwrap();
    set_retention(&store, &data_dir, &id, 60).await.unwrap();

    let lib = store.library(&id).await.unwrap().unwrap();
    assert_eq!(lib.display_name, "Allison's Archive");
    assert_eq!(lib.retention_days, 60);
    assert_eq!(lib.library_id, id, "id immutable");

    let renamed = store.log_events("library_renamed").await.unwrap();
    assert_eq!(renamed.len(), 1);
    assert!(
        renamed[0]
            .detail
            .as_ref()
            .unwrap()
            .contains("Personal Library")
    );
    let retention = store.log_events("retention_changed").await.unwrap();
    assert_eq!(retention.len(), 1);

    assert!(set_retention(&store, &data_dir, &id, -5).await.is_err());
    assert!(
        rename_library(&store, &data_dir, &LibraryId::new("no_such"), "X")
            .await
            .is_err()
    );
}

// Impact: config writes racing a live daemon would fight its transactions
// and its view of the libraries table mid-drain.
// Should: refuse every write command while a live-pid lock is held.
#[tokio::test]
async fn writes_refused_while_daemon_lock_held() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_dir) = rig(tmp.path()).await;
    let id = add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Personal))
        .await
        .unwrap()
        .config
        .library_id;

    let lock_path = data_dir.root().join("drain.lock");
    std::fs::write(&lock_path, std::process::id().to_string()).unwrap();

    assert!(
        add_library(&store, &data_dir, &opts(tmp.path(), LibraryScope::Shared))
            .await
            .is_err()
    );
    assert!(
        bind_scope(&store, &data_dir, &id, Some(LibraryScope::Shared))
            .await
            .is_err()
    );
    assert!(rename_library(&store, &data_dir, &id, "X").await.is_err());
    assert!(set_retention(&store, &data_dir, &id, 10).await.is_err());
    assert!(lock_path.is_file(), "foreign lock not consumed");
}
