//! Core-vs-mock tests: the daemon-core-alone layer from RFC-018's testing
//! model. No kernel, no node — MountCore drives the mock transport, and
//! assertions cover both what the core returns and what it emits across
//! the boundary (call recording).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::attrs::DEFAULT_TTL;
use crate::idmap::ROOT_INO;
use crate::mock::{CallRecord, MockHandle, MockTransport};
use crate::transport::{ItemId, ItemKind, NodeTransport};
use crate::vfs::{CoreError, Invalidation, MountCore};
use crate::watch::{KernelInvalidator, Watcher};

fn setup() -> (Arc<MountCore>, MockHandle) {
    let (transport, handle) = MockTransport::new();
    (
        Arc::new(MountCore::new(transport, DEFAULT_TTL)),
        handle,
    )
}

// Should: resolve a child of the root by name and hand back a stable
// inode number with the item's kind and size.
#[tokio::test]
async fn lookup_resolves_child_by_name() {
    let (core, handle) = setup();
    handle.add_file(ItemId::Root, "README.md", 2048);

    let node = core.lookup(ROOT_INO, "README.md").await.unwrap();
    assert_eq!(node.item.name, "README.md");
    assert_eq!(node.item.kind, ItemKind::File { size: 2048 });

    let again = core.lookup(ROOT_INO, "README.md").await.unwrap();
    assert_eq!(node.ino, again.ino);
}

// Should: report NotFound (ENOENT at the adapter) for a name that does
// not exist under the parent.
#[tokio::test]
async fn lookup_missing_is_not_found() {
    let (core, _handle) = setup();
    match core.lookup(ROOT_INO, "ghost.txt").await {
        Err(CoreError::NotFound) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

// Should: resolve nested paths component-by-component, the way the
// kernel issues lookups.
#[tokio::test]
async fn lookup_resolves_nested_components() {
    let (core, handle) = setup();
    let docs = handle.add_folder(ItemId::Root, "Documents");
    let rfcs = handle.add_folder(docs, "RFCs");
    handle.add_file(rfcs, "rfc-018.md", 12_400);

    let docs_node = core.lookup(ROOT_INO, "Documents").await.unwrap();
    let rfcs_node = core.lookup(docs_node.ino, "RFCs").await.unwrap();
    let file_node = core.lookup(rfcs_node.ino, "rfc-018.md").await.unwrap();
    assert_eq!(file_node.item.name, "rfc-018.md");
}

// Should: serve getattr for the root inode without any prior lookup —
// the kernel stats the mountpoint first.
#[tokio::test]
async fn getattr_root_works_cold() {
    let (core, _handle) = setup();
    let node = core.getattr(ROOT_INO).await.unwrap();
    assert_eq!(node.ino, ROOT_INO);
    assert_eq!(node.item.kind, ItemKind::Folder);
}

// Should: list a directory completely even when the transport paginates,
// walking every enumerate page at opendir.
// Impact: a missed page would silently hide files from ls — worse than
// an error.
#[tokio::test]
async fn readdir_walks_all_pages() {
    let (core, handle) = setup();
    for i in 0..10 {
        handle.add_file(ItemId::Root, &format!("file-{i:02}.txt"), 100);
    }
    handle.set_page_size(3);

    let fh = core.opendir(ROOT_INO).await.unwrap();
    let entries = core.dir_entries(fh).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();

    assert_eq!(entries.len(), 12, "10 files + . + ..");
    assert_eq!(names[0], ".");
    assert_eq!(names[1], "..");
    for i in 0..10 {
        assert!(names.contains(&format!("file-{i:02}.txt").as_str()));
    }
}

// Should: keep an open directory handle's listing immutable while the
// tree changes underneath it, and observe the change on a fresh opendir.
// Impact: POSIX readdir semantics — entries must not skip or duplicate
// mid-listing when a sibling is created/removed between getdents calls.
#[tokio::test]
async fn open_dir_snapshot_is_immutable_under_mutation() {
    let (core, handle) = setup();
    handle.add_file(ItemId::Root, "a.txt", 1);
    let doomed = handle.add_file(ItemId::Root, "b.txt", 1);

    let fh = core.opendir(ROOT_INO).await.unwrap();
    handle.remove(&doomed);
    handle.add_file(ItemId::Root, "c.txt", 1);

    let snapshot = core.dir_entries(fh).unwrap();
    let names: Vec<&str> = snapshot.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"b.txt"), "snapshot keeps removed entry");
    assert!(!names.contains(&"c.txt"), "snapshot excludes later addition");

    let fh2 = core.opendir(ROOT_INO).await.unwrap();
    let fresh = core.dir_entries(fh2).unwrap();
    let names: Vec<&str> = fresh.iter().map(|e| e.name.as_str()).collect();
    assert!(!names.contains(&"b.txt"));
    assert!(names.contains(&"c.txt"));
}

// Should: reject opendir on a file with NotADirectory.
#[tokio::test]
async fn opendir_on_file_is_not_a_directory() {
    let (core, handle) = setup();
    handle.add_file(ItemId::Root, "plain.txt", 5);
    let node = core.lookup(ROOT_INO, "plain.txt").await.unwrap();
    match core.opendir(node.ino).await {
        Err(CoreError::NotADirectory) => {}
        other => panic!("expected NotADirectory, got {other:?}"),
    }
}

// Should: report StaleHandle (EBADF) for a released directory handle.
#[tokio::test]
async fn released_dir_handle_is_stale() {
    let (core, _handle) = setup();
    let fh = core.opendir(ROOT_INO).await.unwrap();
    core.releasedir(fh);
    match core.dir_entries(fh) {
        Err(CoreError::StaleHandle) => {}
        other => panic!("expected StaleHandle, got {other:?}"),
    }
}

// Should: serve getattr from the attr cache after a lookup warmed it.
// Should not: re-fetch the item from the node within the TTL.
// Impact: the cache-until-poked policy (RFC-018) — every stat must not
// become a node round-trip.
#[tokio::test]
async fn getattr_within_ttl_emits_no_transport_call() {
    let (core, handle) = setup();
    handle.add_file(ItemId::Root, "cached.txt", 9);
    let node = core.lookup(ROOT_INO, "cached.txt").await.unwrap();

    handle.clear_calls();
    core.getattr(node.ino).await.unwrap();
    assert!(
        handle.calls().is_empty(),
        "warm getattr must be cache-served, saw {:?}",
        handle.calls()
    );
}

// Should: go back to the node after invalidation (the S4 poke path calls
// exactly this entry point).
#[tokio::test]
async fn invalidate_forces_refetch() {
    let (core, handle) = setup();
    handle.add_file(ItemId::Root, "poked.txt", 9);
    let node = core.lookup(ROOT_INO, "poked.txt").await.unwrap();

    core.invalidate(&node.item.id);
    handle.clear_calls();
    core.getattr(node.ino).await.unwrap();
    assert_eq!(
        handle.calls(),
        vec![CallRecord::Item {
            id: node.item.id.clone()
        }]
    );
}

// Should: expire cached attrs after the TTL backstop even with no poke.
#[tokio::test]
async fn ttl_expiry_forces_refetch() {
    let (transport, handle) = MockTransport::new();
    let core = MountCore::new(transport, Duration::from_millis(10));
    handle.add_file(ItemId::Root, "stale.txt", 9);
    let node = core.lookup(ROOT_INO, "stale.txt").await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.clear_calls();
    core.getattr(node.ino).await.unwrap();
    assert!(!handle.calls().is_empty(), "expired attr must re-fetch");
}

// ---------- S4: pokes, deltas, kernel invalidation ----------

/// Records the kernel busts the watch loop fires, for assertion.
#[derive(Default)]
struct RecordingInvalidator {
    seen: Mutex<Vec<Invalidation>>,
}
impl KernelInvalidator for RecordingInvalidator {
    fn inval_entry(&self, parent_ino: u64, name: &str) {
        self.seen.lock().unwrap().push(Invalidation::Entry {
            parent_ino,
            name: name.to_string(),
        });
    }
    fn inval_inode(&self, ino: u64) {
        self.seen
            .lock()
            .unwrap()
            .push(Invalidation::Inode { ino });
    }
}
impl RecordingInvalidator {
    fn snapshot(&self) -> Vec<Invalidation> {
        self.seen.lock().unwrap().clone()
    }
}

fn changes_calls(handle: &MockHandle) -> Vec<i64> {
    handle
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            CallRecord::Changes { since } => Some(since),
            _ => None,
        })
        .collect()
}

async fn wait_until(deadline_ms: u64, mut cond: impl FnMut() -> bool) -> bool {
    for _ in 0..(deadline_ms / 10) {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

/// Spawn a watcher over the given core+mock, wait for the watch
/// connection and the post-connect sync.
async fn spawn_watcher(
    core: Arc<MountCore>,
    transport: Arc<MockTransport>,
    handle: &MockHandle,
) -> Arc<RecordingInvalidator> {
    let invalidator = Arc::new(RecordingInvalidator::default());
    tokio::spawn(
        Watcher::new(core, transport as Arc<dyn NodeTransport>, invalidator.clone()).run(),
    );
    assert!(
        wait_until(2000, || handle.watch_connected() && !changes_calls(handle).is_empty()).await,
        "watcher never connected + synced"
    );
    invalidator
}

// Should: follow a poke with exactly one changes(anchor) sync.
// Should not: sync again without a further poke.
// Impact: this is the freshness mechanism — the TTLs are only backstops.
#[tokio::test]
async fn poke_triggers_exactly_one_sync() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    spawn_watcher(core, transport, &handle).await;
    let baseline = changes_calls(&handle).len();

    handle.add_file(ItemId::Root, "new.txt", 5);
    handle.poke();
    assert!(
        wait_until(2000, || changes_calls(&handle).len() == baseline + 1).await,
        "expected one sync after poke, saw {:?}",
        changes_calls(&handle)
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        changes_calls(&handle).len(),
        baseline + 1,
        "no further sync without a further poke"
    );
}

// Should: coalesce a burst of pokes into a single sync.
#[tokio::test]
async fn poke_burst_coalesces_into_one_sync() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    spawn_watcher(core, transport, &handle).await;
    let baseline = changes_calls(&handle).len();

    for _ in 0..5 {
        handle.poke();
    }
    assert!(wait_until(2000, || changes_calls(&handle).len() > baseline).await);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        changes_calls(&handle).len(),
        baseline + 1,
        "burst must coalesce to one sync"
    );
}

// Should: refresh the attr cache from a delta so a subsequent stat is
// served fresh with no transport round-trip, and bust the kernel's inode
// + parent-entry caches for items the kernel has seen.
#[tokio::test]
async fn remote_modify_refreshes_cache_and_busts_kernel() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    handle.add_file(ItemId::Root, "doc.txt", 100);
    let node = core.lookup(ROOT_INO, "doc.txt").await.unwrap();

    let invalidator = spawn_watcher(core.clone(), transport, &handle).await;

    handle.update_file_size(&node.item.id, 999);
    handle.poke();

    assert!(
        wait_until(2000, || {
            invalidator
                .snapshot()
                .contains(&Invalidation::Inode { ino: node.ino })
        })
        .await,
        "kernel inode bust for a known item"
    );
    assert!(invalidator.snapshot().contains(&Invalidation::Entry {
        parent_ino: ROOT_INO,
        name: "doc.txt".to_string()
    }));

    handle.clear_calls();
    let fresh = core.getattr(node.ino).await.unwrap();
    assert_eq!(fresh.item.kind, ItemKind::File { size: 999 });
    assert!(
        handle.calls().is_empty(),
        "delta-refreshed attr must be cache-served"
    );
}

// Should not: mint inode numbers (or fire kernel busts) for changed items
// the kernel has never looked up.
#[tokio::test]
async fn unseen_items_get_no_kernel_invalidation() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    let invalidator = spawn_watcher(core, transport, &handle).await;

    handle.add_file(ItemId::Root, "never-seen.txt", 1);
    handle.poke();
    let baseline_calls = changes_calls(&handle).len();
    assert!(wait_until(2000, || changes_calls(&handle).len() >= baseline_calls).await);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        invalidator.snapshot().is_empty(),
        "no kernel state exists for unseen items, got {:?}",
        invalidator.snapshot()
    );
}

// Should: bust both the old and new parent entries on a remote rename.
// Impact: a stale dentry under the old name is a phantom file — worse
// than staleness, it's wrong namespace.
#[tokio::test]
async fn remote_rename_busts_old_and_new_entries() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    let docs = handle.add_folder(ItemId::Root, "Documents");
    handle.add_file(docs.clone(), "old-name.txt", 5);

    let docs_node = core.lookup(ROOT_INO, "Documents").await.unwrap();
    let file_node = core.lookup(docs_node.ino, "old-name.txt").await.unwrap();
    let invalidator = spawn_watcher(core.clone(), transport, &handle).await;

    handle.rename(&file_node.item.id, ItemId::Root, "new-name.txt");
    handle.poke();

    assert!(
        wait_until(2000, || {
            let seen = invalidator.snapshot();
            seen.contains(&Invalidation::Entry {
                parent_ino: docs_node.ino,
                name: "old-name.txt".to_string(),
            }) && seen.contains(&Invalidation::Entry {
                parent_ino: ROOT_INO,
                name: "new-name.txt".to_string(),
            }) && seen.contains(&Invalidation::Inode { ino: file_node.ino })
        })
        .await,
        "rename must bust old entry, new entry, and the inode: {:?}",
        invalidator.snapshot()
    );
}

// Should: on remote delete, bust the parent entry and inode and forget
// the cached attrs — a subsequent getattr reports NotFound.
#[tokio::test]
async fn remote_delete_busts_and_forgets() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    handle.add_file(ItemId::Root, "doomed.txt", 5);
    let node = core.lookup(ROOT_INO, "doomed.txt").await.unwrap();
    let invalidator = spawn_watcher(core.clone(), transport, &handle).await;

    handle.remove(&node.item.id);
    handle.poke();

    assert!(
        wait_until(2000, || {
            let seen = invalidator.snapshot();
            seen.contains(&Invalidation::Entry {
                parent_ino: ROOT_INO,
                name: "doomed.txt".to_string(),
            }) && seen.contains(&Invalidation::Inode { ino: node.ino })
        })
        .await
    );
    match core.getattr(node.ino).await {
        Err(CoreError::NotFound) => {}
        other => panic!("deleted item must be NotFound, got {other:?}"),
    }
}

// Should: reconnect after a dropped watch connection and resync from the
// anchor, so mutations made during the gap propagate.
// Should not: leave a divergence window — the post-reconnect sync happens
// before any new poke is needed.
// Impact: the RFC's no-divergence-on-reconnect MUST.
#[tokio::test]
async fn disconnect_resyncs_without_divergence() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    handle.add_file(ItemId::Root, "gap.txt", 10);
    let node = core.lookup(ROOT_INO, "gap.txt").await.unwrap();
    let invalidator = spawn_watcher(core.clone(), transport, &handle).await;

    handle.drop_watch();
    // Mutation while no watch connection exists — no poke is possible.
    handle.update_file_size(&node.item.id, 777);

    assert!(
        wait_until(3000, || {
            invalidator
                .snapshot()
                .contains(&Invalidation::Inode { ino: node.ino })
        })
        .await,
        "gap mutation must propagate via post-reconnect sync"
    );
    let fresh = core.getattr(node.ino).await.unwrap();
    assert_eq!(fresh.item.kind, ItemKind::File { size: 777 });
}

// Should: pass strictly non-decreasing anchors to changes() across syncs.
// Impact: a regressing anchor re-applies old deltas; a skipping anchor
// loses changes silently.
#[tokio::test]
async fn sync_anchor_is_monotonic() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    spawn_watcher(core, transport, &handle).await;

    for i in 0..3 {
        handle.add_file(ItemId::Root, &format!("m{i}.txt"), 1);
        handle.poke();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let seen = changes_calls(&handle);
    assert!(seen.len() >= 3);
    // First call is the ANCHOR_INIT sentinel; later anchors are real
    // heights and must never decrease.
    for pair in seen[1..].windows(2) {
        assert!(pair[1] >= pair[0], "anchor regressed: {seen:?}");
    }
}

// ---------- S5: content reads through the sparse cache ----------

use crate::cache::{CacheConfig, CacheManager, EvictionPolicy};

/// Core + mock + attached cache with tiny segments and a deterministic
/// byte-cap policy. The tempdir guard must outlive the core.
fn setup_cached(
    segment_size: u64,
    policy: EvictionPolicy,
) -> (Arc<MountCore>, MockHandle, tempfile::TempDir) {
    let (transport, handle) = MockTransport::new();
    let dir = tempfile::tempdir().expect("cache tempdir");
    let cache = Arc::new(
        CacheManager::new(
            CacheConfig {
                root: dir.path().join("content"),
                segment_size,
                policy,
            },
            transport.clone(),
        )
        .expect("cache"),
    );
    let core = Arc::new(MountCore::new(transport, DEFAULT_TTL).with_cache(cache));
    (core, handle, dir)
}

fn read_blob_calls(handle: &MockHandle) -> Vec<(u64, u64)> {
    handle
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            CallRecord::ReadBlob { offset, len, .. } => Some((offset, len)),
            _ => None,
        })
        .collect()
}

// Should: serve exact bytes across segment boundaries, clamp at EOF, and
// return empty for zero-length and past-EOF reads.
#[tokio::test]
async fn cache_reads_exact_bytes_across_segments() {
    let (core, handle, _dir) = setup_cached(16, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    let content: Vec<u8> = (0..=99u8).collect();
    handle.add_file_with_content(ItemId::Root, "data.bin", &content);
    let node = core.lookup(ROOT_INO, "data.bin").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();

    assert_eq!(core.read(fh, 0, 100).await.unwrap(), content);
    assert_eq!(core.read(fh, 10, 20).await.unwrap(), &content[10..30]);
    assert_eq!(core.read(fh, 90, 50).await.unwrap(), &content[90..]);
    assert_eq!(core.read(fh, 100, 10).await.unwrap(), Vec::<u8>::new());
    assert_eq!(core.read(fh, 5, 0).await.unwrap(), Vec::<u8>::new());
}

// Should: serve empty files as immediate EOF with no blob fetch.
#[tokio::test]
async fn empty_file_reads_eof_without_fetch() {
    let (core, handle, _dir) = setup_cached(16, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    handle.add_file(ItemId::Root, "empty.txt", 0);
    let node = core.lookup(ROOT_INO, "empty.txt").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();
    assert_eq!(core.read(fh, 0, 4096).await.unwrap(), Vec::<u8>::new());
    assert!(read_blob_calls(&handle).is_empty());
}

// Should: coalesce concurrent reads of one segment into exactly one
// transport fetch, with every reader getting the bytes.
// Impact: without single-flight, a kernel readahead burst multiplies a
// 40 MB chunk reconstruction by the number of outstanding reads.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reads_single_flight_one_fetch() {
    let (core, handle, _dir) = setup_cached(64, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    let content = vec![7u8; 64];
    handle.add_file_with_content(ItemId::Root, "hot.bin", &content);
    let node = core.lookup(ROOT_INO, "hot.bin").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();

    handle.clear_calls();
    handle.hold_fetches();
    let mut tasks = Vec::new();
    for i in 0..8u64 {
        let core = core.clone();
        tasks.push(tokio::spawn(async move {
            core.read(fh, (i * 8) % 64, 8).await
        }));
    }
    // Give every reader time to reach the segment gate.
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.release_fetches();

    for task in tasks {
        let bytes = task.await.unwrap().unwrap();
        assert!(bytes.iter().all(|b| *b == 7));
    }
    assert_eq!(
        read_blob_calls(&handle).len(),
        1,
        "one segment, eight readers, one fetch"
    );
}

// Should: keep serving the blob captured at open() after a remote content
// replacement, while a fresh open serves the new content.
// Impact: POSIX open-handle semantics — without the snapshot, a reader
// gets the first half of one version and the second half of another.
#[tokio::test]
async fn snapshot_at_open_survives_remote_replace() {
    let (core, handle, _dir) = setup_cached(16, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    let (id, _blob) = handle.add_file_with_content(ItemId::Root, "doc.txt", b"version one!");
    let node = core.lookup(ROOT_INO, "doc.txt").await.unwrap();
    let old_fh = core.open(node.ino).await.unwrap();
    assert_eq!(core.read(old_fh, 0, 64).await.unwrap(), b"version one!");

    handle.update_file_content(&id, b"VERSION TWO IS LONGER");
    core.invalidate(&id); // the S4 poke path in miniature

    assert_eq!(
        core.read(old_fh, 0, 64).await.unwrap(),
        b"version one!",
        "open handle keeps its snapshot"
    );

    let new_fh = core.open(node.ino).await.unwrap();
    assert_eq!(core.read(new_fh, 0, 64).await.unwrap(), b"VERSION TWO IS LONGER");
}

// Should: hold cached bytes at or under the cap while walking a file
// larger than the cache, and refetch (correctly) a segment that was
// evicted behind the read head.
// Should not: ever change the cache file's logical size while punching.
// Impact: the rolling-window behavior — huge files stream through a
// bounded cache.
#[tokio::test]
async fn eviction_rolls_window_and_refetches() {
    let seg = 16u64;
    let cap = 3 * seg;
    let (core, handle, dir) = setup_cached(seg, EvictionPolicy::MaxBytes { bytes: cap });
    let content: Vec<u8> = (0..160u32).map(|i| (i % 251) as u8).collect();
    let (_, blob) = handle.add_file_with_content(ItemId::Root, "big.bin", &content);
    let node = core.lookup(ROOT_INO, "big.bin").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();

    for offset in (0..160).step_by(16) {
        let bytes = core.read(fh, offset as u64, seg).await.unwrap();
        assert_eq!(bytes, &content[offset..offset + 16]);
        assert!(
            core_cache_bytes(&core) <= cap,
            "cache exceeded cap: {} > {cap}",
            core_cache_bytes(&core)
        );
    }

    handle.clear_calls();
    let bytes = core.read(fh, 0, seg).await.unwrap();
    assert_eq!(bytes, &content[0..16]);
    assert_eq!(
        read_blob_calls(&handle),
        vec![(0, 16)],
        "evicted head segment must refetch"
    );

    let cache_file = dir.path().join("content").join(blob.to_string());
    assert_eq!(
        std::fs::metadata(&cache_file).unwrap().len(),
        160,
        "punching must never change logical size"
    );
}

// Should: fail all concurrent readers of a segment promptly when the
// fetch fails — no hangs, no zombie waiters.
#[tokio::test(flavor = "multi_thread")]
async fn fetch_failure_fails_all_waiters() {
    let (core, handle, _dir) = setup_cached(64, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    let (_, blob) = handle.add_file_with_content(ItemId::Root, "doomed.bin", &[1u8; 64]);
    let node = core.lookup(ROOT_INO, "doomed.bin").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();

    handle.hold_fetches();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let core = core.clone();
        tasks.push(tokio::spawn(async move { core.read(fh, 0, 8).await }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.drop_blob(&blob);
    handle.release_fetches();

    for task in tasks {
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("reader must not hang")
            .unwrap();
        assert!(result.is_err(), "reader of a vanished blob must error");
    }
}

fn core_cache_bytes(core: &MountCore) -> u64 {
    core.cache_stats().unwrap_or(0)
}

// Should: emit exactly one lookup per component and one enumerate walk
// per opendir for an `ls`-shaped sequence — nothing more.
// Impact: the boundary-emission contract; accidental N+1 patterns against
// the node show up here first.
#[tokio::test]
async fn ls_shaped_sequence_emits_expected_calls() {
    let (core, handle) = setup();
    let docs = handle.add_folder(ItemId::Root, "Documents");
    handle.add_file(docs.clone(), "Notes.txt", 640);

    handle.clear_calls();
    let node = core.lookup(ROOT_INO, "Documents").await.unwrap();
    let fh = core.opendir(node.ino).await.unwrap();
    let _ = core.dir_entries(fh).unwrap();
    core.releasedir(fh);

    assert_eq!(
        handle.calls(),
        vec![
            CallRecord::Lookup {
                parent: ItemId::Root,
                name: "Documents".to_string()
            },
            CallRecord::Enumerate {
                parent: docs.clone(),
                cursor: None
            },
        ],
        "lookup warms the attr cache, so opendir's getattr must not hit \
         the node, and a single-page listing is one enumerate"
    );
}
