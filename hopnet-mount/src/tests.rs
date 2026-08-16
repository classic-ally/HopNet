//! Core-vs-mock tests: the daemon-core-alone layer from RFC-018's testing
//! model. No kernel, no node — MountCore drives the mock transport, and
//! assertions cover both what the core returns and what it emits across
//! the boundary (call recording).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::attrs::DEFAULT_TTL;
use crate::idmap::ROOT_INO;
use crate::mock::{CallRecord, MockHandle, MockTransport};
use crate::transport::{Height, ItemId, ItemKind, NodeTransport};
use crate::vfs::{CoreError, Invalidation, MountCore};
use crate::watch::{KernelInvalidator, Watcher};

fn setup() -> (Arc<MountCore>, MockHandle) {
    let (transport, handle) = MockTransport::new();
    (Arc::new(MountCore::new(transport, DEFAULT_TTL)), handle)
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
    assert!(
        !names.contains(&"c.txt"),
        "snapshot excludes later addition"
    );

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
        self.seen.lock().unwrap().push(Invalidation::Inode { ino });
    }
}
impl RecordingInvalidator {
    fn snapshot(&self) -> Vec<Invalidation> {
        self.seen.lock().unwrap().clone()
    }
}

fn changes_calls(handle: &MockHandle) -> Vec<Height> {
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
        Watcher::new(
            core,
            transport as Arc<dyn NodeTransport>,
            invalidator.clone(),
        )
        .run(),
    );
    assert!(
        wait_until(2000, || handle.watch_connected()
            && !changes_calls(handle).is_empty())
        .await,
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
        tasks.push(tokio::spawn(
            async move { core.read(fh, (i * 8) % 64, 8).await },
        ));
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
    assert_eq!(
        core.read(new_fh, 0, 64).await.unwrap(),
        b"VERSION TWO IS LONGER"
    );
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

// ---------- S7: writes — staging, copy-up, upload tiers, recovery ----------

use crate::staging::Staging;
use hopnet_common::CustomUUID;

/// Core with cache AND staging attached (the full write-capable daemon
/// core). Returns the staging dir path for on-disk assertions.
fn setup_writable() -> (Arc<MountCore>, MockHandle, tempfile::TempDir) {
    let (transport, handle) = MockTransport::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(
        CacheManager::new(
            CacheConfig {
                root: dir.path().join("content"),
                segment_size: 64,
                policy: EvictionPolicy::MaxBytes { bytes: 1 << 20 },
            },
            transport.clone(),
        )
        .expect("cache"),
    );
    let staging = Arc::new(Staging::new(dir.path().join("staging")).expect("staging"));
    let core = Arc::new(
        MountCore::new(transport, DEFAULT_TTL)
            .with_cache(cache)
            .with_staging(staging),
    );
    (core, handle, dir)
}

fn upload_records(handle: &MockHandle) -> Vec<Vec<u8>> {
    handle
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            CallRecord::UpdateContent { bytes, .. } => Some(bytes),
            _ => None,
        })
        .collect()
}

fn staging_pairs(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir.join("staging"))
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("data"))
                .count()
        })
        .unwrap_or(0)
}

// Should: create a folder strictly — visible via lookup before mkdir
// returns to the caller.
#[tokio::test]
async fn mkdir_is_strict_and_visible() {
    let (core, _handle, _dir) = setup_writable();
    let node = core.mkdir(ROOT_INO, "NewDir").await.unwrap();
    assert_eq!(node.item.name, "NewDir");
    let found = core.lookup(ROOT_INO, "NewDir").await.unwrap();
    assert_eq!(found.ino, node.ino);

    match core.mkdir(ROOT_INO, "NewDir").await {
        Err(CoreError::AlreadyExists) => {}
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

// Should: upload exactly the staged bytes on release and delete the
// staging pair after success.
// Impact: the write-back tier — bytes the kernel handed us MUST reach
// the node byte-identical, and durable staging must not leak.
#[tokio::test]
async fn create_write_release_uploads_exact_bytes() {
    let (core, handle, dir) = setup_writable();
    let (node, fh) = core.create(ROOT_INO, "out.txt").await.unwrap();
    assert_eq!(
        node.item.kind,
        ItemKind::File { size: 0 },
        "created empty on node"
    );

    core.write(fh, 0, b"hello ").await.unwrap();
    core.write(fh, 6, b"staged world").await.unwrap();
    assert_eq!(
        staging_pairs(dir.path()),
        1,
        "dirty session has a staging pair"
    );

    core.release(fh);
    assert!(
        wait_until(3000, || upload_records(&handle)
            == vec![b"hello staged world".to_vec()])
        .await,
        "release must upload the exact staged bytes, got {:?}",
        upload_records(&handle)
    );
    assert!(
        wait_until(2000, || staging_pairs(dir.path()) == 0).await,
        "staging pair must be deleted after successful upload"
    );
}

// Should: preserve untouched regions when partially overwriting an
// existing file (whole-file copy-up before the first write).
// Impact: without copy-up, a 4-byte edit would upload a 4-byte file.
#[tokio::test]
async fn copy_up_preserves_untouched_bytes() {
    let (core, handle, _dir) = setup_writable();
    handle.add_file_with_content(ItemId::Root, "base.txt", b"0123456789");
    let node = core.lookup(ROOT_INO, "base.txt").await.unwrap();

    let fh = core.open_rw(node.ino, false).await.unwrap();
    core.write(fh, 2, b"abcd").await.unwrap();
    core.release(fh);

    assert!(
        wait_until(3000, || upload_records(&handle)
            == vec![b"01abcd6789".to_vec()])
        .await,
        "copy-up must preserve untouched bytes, got {:?}",
        upload_records(&handle)
    );
}

// Should: serve reads from staging on a dirty handle and report the
// staged size via the getattr overlay before any upload happens.
#[tokio::test]
async fn dirty_handles_read_their_own_writes() {
    let (core, handle, _dir) = setup_writable();
    handle.add_file_with_content(ItemId::Root, "doc.txt", b"original");
    let node = core.lookup(ROOT_INO, "doc.txt").await.unwrap();

    let fh = core.open_rw(node.ino, false).await.unwrap();
    core.write(fh, 0, b"REWRITTEN LONGER").await.unwrap();

    assert_eq!(core.read(fh, 0, 64).await.unwrap(), b"REWRITTEN LONGER");
    assert_eq!(core.staged_size(node.ino).await, Some(16));
    assert!(upload_records(&handle).is_empty(), "nothing uploaded yet");
    core.release(fh);
}

// Should: make fsync block until the upload is recorded while release
// alone returns immediately with the upload still parked.
// Impact: the two-tier durability contract — only fsync promises
// persistence; close must not freeze apps on big uploads.
#[tokio::test(flavor = "multi_thread")]
async fn fsync_blocks_release_does_not() {
    let (core, handle, _dir) = setup_writable();
    let (_, fh) = core.create(ROOT_INO, "tiered.txt").await.unwrap();
    core.write(fh, 0, b"tier test").await.unwrap();

    handle.hold_uploads();

    // fsync with uploads held must NOT complete.
    let fsync_core = core.clone();
    let fsync_task = tokio::spawn(async move { fsync_core.fsync(fh).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !fsync_task.is_finished(),
        "fsync must block while upload is parked"
    );

    handle.release_uploads();
    tokio::time::timeout(Duration::from_secs(5), fsync_task)
        .await
        .expect("fsync completes after release of the gate")
        .unwrap()
        .unwrap();
    assert_eq!(upload_records(&handle).len(), 1);

    // A clean release after fsync uploads nothing further.
    core.release(fh);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        upload_records(&handle).len(),
        1,
        "clean close re-uploads nothing"
    );
}

// Should: stage a truncate and upload the shortened content.
#[tokio::test]
async fn truncate_stages_and_uploads() {
    let (core, handle, _dir) = setup_writable();
    handle.add_file_with_content(ItemId::Root, "trunc.txt", b"0123456789");
    let node = core.lookup(ROOT_INO, "trunc.txt").await.unwrap();

    let fh = core.open_rw(node.ino, false).await.unwrap();
    core.truncate(fh, 4).await.unwrap();
    core.release(fh);

    assert!(
        wait_until(3000, || upload_records(&handle) == vec![b"0123".to_vec()]).await,
        "truncated content must upload, got {:?}",
        upload_records(&handle)
    );
}

// Should: rename strictly (old gone, new resolvable) and refuse rmdir of
// a non-empty folder with NotEmpty.
#[tokio::test]
async fn rename_and_rmdir_semantics() {
    let (core, handle, _dir) = setup_writable();
    let folder = core.mkdir(ROOT_INO, "Dir").await.unwrap();
    handle.add_file_with_content(ItemId::Root, "a.txt", b"x");
    core.lookup(ROOT_INO, "a.txt").await.unwrap();

    core.rename(ROOT_INO, "a.txt", folder.ino, "b.txt")
        .await
        .unwrap();
    match core.lookup(ROOT_INO, "a.txt").await {
        Err(CoreError::NotFound) => {}
        other => panic!("old name must be gone, got {other:?}"),
    }
    let moved = core.lookup(folder.ino, "b.txt").await.unwrap();
    assert_eq!(moved.item.name, "b.txt");

    match core.remove(ROOT_INO, "Dir", true).await {
        Err(CoreError::NotEmpty) => {}
        other => panic!("expected NotEmpty, got {other:?}"),
    }
    core.remove(folder.ino, "b.txt", false).await.unwrap();
    core.remove(ROOT_INO, "Dir", true).await.unwrap();
    match core.lookup(ROOT_INO, "Dir").await {
        Err(CoreError::NotFound) => {}
        other => panic!("deleted dir must be gone, got {other:?}"),
    }
}

// Should: detect a remote modification that raced a local write session,
// bump the conflict gauge, and proceed last-writer-wins.
// Impact: silent clobbering would hide exactly the events issue #26's
// rollback exists for; the gauge is the paper trail.
#[tokio::test]
async fn conflict_is_detected_and_lww_proceeds() {
    let (core, handle, _dir) = setup_writable();
    let (id, _) = handle.add_file_with_content(ItemId::Root, "shared.txt", b"base");
    let node = core.lookup(ROOT_INO, "shared.txt").await.unwrap();

    let fh = core.open_rw(node.ino, false).await.unwrap();
    core.write(fh, 0, b"mine").await.unwrap();

    // Remote writer lands first.
    handle.update_file_content(&id, b"theirs");

    core.fsync(fh).await.unwrap();
    assert_eq!(core.conflicts(), 1, "conflict gauge must bump");
    let uploads = upload_records(&handle);
    assert_eq!(uploads.last().unwrap(), b"mine", "LWW: our content wins");
    core.release(fh);
}

// Should: re-upload staging pairs left by a previous run, and park pairs
// whose inode no longer exists under orphaned/ without deleting bytes.
// Impact: the durability story for crash-before-upload — user data may
// never be lost silently.
#[tokio::test]
async fn recovery_uploads_leftover_staging() {
    let (core, handle, dir) = setup_writable();
    let (id, _) = handle.add_file_with_content(ItemId::Root, "recover.txt", b"old");
    let ItemId::Inode(inode_uuid) = id.clone() else {
        panic!()
    };

    // Simulate a previous run: staging pair on disk, daemon died.
    let staging = Staging::new(dir.path().join("staging")).unwrap();
    let staged = staging
        .begin(crate::staging::StagedMeta {
            inode_id: inode_uuid,
            base_height: 1,
        })
        .unwrap();
    staged
        .write_at(0, b"recovered content".to_vec())
        .await
        .unwrap();

    // A second pair whose inode is gone.
    let ghost = staging
        .begin(crate::staging::StagedMeta {
            inode_id: CustomUUID::new(None),
            base_height: 1,
        })
        .unwrap();
    ghost.write_at(0, b"orphan bytes".to_vec()).await.unwrap();

    core.recover().await;

    let uploads = upload_records(&handle);
    assert_eq!(uploads, vec![b"recovered content".to_vec()]);
    assert_eq!(staging_pairs(dir.path()), 0, "recovered pair cleaned up");
    let orphaned: Vec<_> = std::fs::read_dir(dir.path().join("staging/orphaned"))
        .unwrap()
        .flatten()
        .collect();
    assert_eq!(orphaned.len(), 2, "ghost pair parked, not deleted");
}

// Should: keep the staging pair when the upload fails and succeed on a
// later retry.
#[tokio::test(flavor = "multi_thread")]
async fn upload_failure_retains_staging_then_retries() {
    let (core, handle, dir) = setup_writable();
    let (_, fh) = core.create(ROOT_INO, "flaky.txt").await.unwrap();
    core.write(fh, 0, b"eventually").await.unwrap();

    handle.set_upload_fail(true);
    core.release(fh);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        staging_pairs(dir.path()),
        1,
        "failed upload must retain staging"
    );

    handle.set_upload_fail(false);
    // The background retry loop (1s backoff) picks it up.
    assert!(
        wait_until(5000, || upload_records(&handle)
            .last()
            .is_some_and(|b| b == b"eventually"))
        .await,
        "retry must eventually upload"
    );
    assert!(wait_until(2000, || staging_pairs(dir.path()) == 0).await);
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

// ---------------------------------------------------------------- S8 —
// statfs: TTL-cached mesh numbers that never turn df into an error.

// Should: serve repeated statfs reads within the TTL from cache with a
// single transport call.
#[tokio::test]
async fn statfs_within_ttl_is_cache_served() {
    let (core, handle) = setup();
    handle.set_statfs(Some(crate::transport::StatfsInfo {
        total_bytes: 1000,
        used_bytes: 250,
    }));

    let first = core.statfs().await;
    let second = core.statfs().await;
    assert_eq!(first.total_bytes, 1000);
    assert_eq!(second, first);
    assert_eq!(
        handle
            .calls()
            .iter()
            .filter(|c| **c == CallRecord::Statfs)
            .count(),
        1,
        "second read within the TTL must not hit the node"
    );
}

// Impact: file managers poll statfs continuously; a node blip must
// degrade to stale numbers, not to an erroring drive.
// Should: keep serving the last-known numbers when the node becomes
// unreachable.
// Should not: report zeros once real numbers have been seen.
#[tokio::test(start_paused = true)]
async fn statfs_serves_last_known_on_transport_failure() {
    let (core, handle) = setup();
    handle.set_statfs(Some(crate::transport::StatfsInfo {
        total_bytes: 4096,
        used_bytes: 1024,
    }));
    let first = core.statfs().await;
    assert_eq!(first.used_bytes, 1024);

    handle.set_statfs(None);
    tokio::time::advance(std::time::Duration::from_secs(20)).await;
    let stale = core.statfs().await;
    assert_eq!(stale, first, "transport failure must serve last-known");
}

// Should: report zeros before the first successful fetch rather than
// erroring the mount.
#[tokio::test]
async fn statfs_before_first_success_is_zeros() {
    let (core, handle) = setup();
    handle.set_statfs(None);
    let info = core.statfs().await;
    assert_eq!((info.total_bytes, info.used_bytes), (0, 0));
}

// Should: refetch once the TTL lapses and pick up fresh numbers.
#[tokio::test(start_paused = true)]
async fn statfs_refetches_after_ttl() {
    let (core, handle) = setup();
    handle.set_statfs(Some(crate::transport::StatfsInfo {
        total_bytes: 1000,
        used_bytes: 100,
    }));
    assert_eq!(core.statfs().await.used_bytes, 100);

    handle.set_statfs(Some(crate::transport::StatfsInfo {
        total_bytes: 1000,
        used_bytes: 900,
    }));
    tokio::time::advance(std::time::Duration::from_secs(20)).await;
    assert_eq!(
        core.statfs().await.used_bytes,
        900,
        "expired cache must refetch"
    );
}

// ---------------------------------------------------------------- S9 —
// passthrough eligibility + eviction pinning (kernel-free layer; the
// privileged stack smoke proves the kernel half).

// Impact: handing the kernel a backing fd for an incomplete blob would
// serve hole zeros to page faults with no daemon in the loop.
// Should: refuse a backing until every segment is present.
// Should: hand out a backing once the whole blob has been read.
#[tokio::test]
async fn backing_appears_only_when_blob_complete() {
    let (core, handle, _dir) = setup_cached(16, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    let content: Vec<u8> = (0..48u8).collect(); // 3 segments
    handle.add_file_with_content(ItemId::Root, "part.bin", &content);
    let node = core.lookup(ROOT_INO, "part.bin").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();

    core.read(fh, 0, 16).await.unwrap();
    assert!(
        core.backing_for(fh).is_none(),
        "one of three segments must not qualify"
    );

    core.read(fh, 16, 32).await.unwrap();
    assert!(
        core.backing_for(fh).is_some(),
        "fully-read blob must hand out a backing"
    );
}

// Should: refuse a backing while any segment fetch is in flight.
#[tokio::test(flavor = "multi_thread")]
async fn backing_refused_mid_fetch() {
    let (core, handle, _dir) = setup_cached(64, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    let content = vec![3u8; 64];
    handle.add_file_with_content(ItemId::Root, "slow.bin", &content);
    let node = core.lookup(ROOT_INO, "slow.bin").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();

    handle.hold_fetches();
    let reader = {
        let core = core.clone();
        tokio::spawn(async move { core.read(fh, 0, 64).await })
    };
    assert!(
        wait_until(2000, || !read_blob_calls(&handle).is_empty()).await,
        "fetch should have started"
    );
    assert!(
        core.backing_for(fh).is_none(),
        "in-flight fetch must block the backing"
    );
    handle.release_fetches();
    reader.await.unwrap().unwrap();
    assert!(core.backing_for(fh).is_some());
}

// Should: serve the blob's exact bytes through the cloned backing fd —
// what the kernel will read is what the daemon would have served.
#[tokio::test]
async fn backing_fd_reads_exact_bytes() {
    let (core, handle, _dir) = setup_cached(16, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    let content: Vec<u8> = (0..=99u8).collect();
    handle.add_file_with_content(ItemId::Root, "data.bin", &content);
    let node = core.lookup(ROOT_INO, "data.bin").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();
    core.read(fh, 0, 100).await.unwrap();

    let backing = core.backing_for(fh).expect("complete blob");
    let mut bytes = vec![0u8; 100];
    use std::os::unix::fs::FileExt;
    backing.file.read_exact_at(&mut bytes, 0).expect("pread");
    assert_eq!(bytes, content);
}

// Impact: the kernel reads a backing file at offsets the daemon never
// sees; a hole punched under it silently yields zeros. The pin is the
// only thing standing between eviction and that corruption.
// Should: keep every pinned segment on disk under eviction pressure.
// Should: evict the blob again once the pin is dropped.
#[tokio::test]
async fn pin_blocks_eviction_until_dropped() {
    // Cap = A(2 segs) + B(4 segs) exactly; C then forces eviction.
    let (core, handle, _dir) = setup_cached(16, EvictionPolicy::MaxBytes { bytes: 96 });
    let a: Vec<u8> = (0..32u8).collect();
    let b = vec![9u8; 64];
    let c = vec![5u8; 32];
    handle.add_file_with_content(ItemId::Root, "a.bin", &a);
    handle.add_file_with_content(ItemId::Root, "b.bin", &b);
    handle.add_file_with_content(ItemId::Root, "c.bin", &c);

    let a_node = core.lookup(ROOT_INO, "a.bin").await.unwrap();
    let a_fh = core.open(a_node.ino).await.unwrap();
    core.read(a_fh, 0, 32).await.unwrap();
    let backing = core.backing_for(a_fh).expect("A complete");

    let b_node = core.lookup(ROOT_INO, "b.bin").await.unwrap();
    let b_fh = core.open(b_node.ino).await.unwrap();
    core.read(b_fh, 0, 64).await.unwrap();

    // A survived B's fill: re-reading A needs no refetch.
    handle.clear_calls();
    assert_eq!(core.read(a_fh, 0, 32).await.unwrap(), a);
    assert!(
        read_blob_calls(&handle).is_empty(),
        "pinned A must still be fully cached"
    );

    // Pin dropped: C's pressure may now punch A. Re-warm B first so A
    // is unambiguously the coldest (the earlier A re-read bumped it).
    drop(backing);
    core.read(b_fh, 0, 64).await.unwrap();
    let c_node = core.lookup(ROOT_INO, "c.bin").await.unwrap();
    let c_fh = core.open(c_node.ino).await.unwrap();
    core.read(c_fh, 0, 32).await.unwrap();
    assert!(
        core.backing_for(a_fh).is_none(),
        "unpinned A should have lost segments to C's fill"
    );
}

// Impact: a regression here is not wrong bytes but a hung daemon —
// ensure_room looping forever over unevictable blobs.
// Should: complete fetches past the byte cap when every cached blob is
// pinned, rather than livelocking in eviction.
#[tokio::test]
async fn all_pinned_cache_overflows_loudly_instead_of_livelocking() {
    let (core, handle, _dir) = setup_cached(16, EvictionPolicy::MaxBytes { bytes: 16 });
    let a = vec![1u8; 16];
    let b = vec![2u8; 16];
    handle.add_file_with_content(ItemId::Root, "a.bin", &a);
    handle.add_file_with_content(ItemId::Root, "b.bin", &b);

    let a_node = core.lookup(ROOT_INO, "a.bin").await.unwrap();
    let a_fh = core.open(a_node.ino).await.unwrap();
    core.read(a_fh, 0, 16).await.unwrap();
    let _pin = core.backing_for(a_fh).expect("A complete");

    let b_node = core.lookup(ROOT_INO, "b.bin").await.unwrap();
    let b_fh = core.open(b_node.ino).await.unwrap();
    assert_eq!(
        core.read(b_fh, 0, 16).await.unwrap(),
        b,
        "fetch must proceed past the cap when nothing is evictable"
    );
}

// Should not: offer a backing for a dirty handle (staged bytes must
// stay daemon-served) or for an empty file (no blob to back).
#[tokio::test]
async fn backing_refused_for_dirty_and_empty_handles() {
    let (core, handle, _dir) = setup_writable();
    handle.add_file_with_content(ItemId::Root, "doc.txt", b"0123456789");
    handle.add_file(ItemId::Root, "empty.txt", 0);

    let doc = core.lookup(ROOT_INO, "doc.txt").await.unwrap();
    let doc_fh = core.open_rw(doc.ino, false).await.unwrap();
    core.read(doc_fh, 0, 10).await.unwrap(); // hydrate via copy-up path? no — clean until write
    core.write(doc_fh, 0, b"XX").await.unwrap();
    assert!(
        core.backing_for(doc_fh).is_none(),
        "dirty handle must never expose a backing"
    );

    let empty = core.lookup(ROOT_INO, "empty.txt").await.unwrap();
    let empty_fh = core.open(empty.ino).await.unwrap();
    assert!(core.backing_for(empty_fh).is_none());
}

// Should: count daemon-mediated reads (the passthrough proof signal).
#[tokio::test]
async fn read_calls_counter_tracks_daemon_reads() {
    let (core, handle, _dir) = setup_cached(16, EvictionPolicy::MaxBytes { bytes: 1 << 20 });
    handle.add_file_with_content(ItemId::Root, "n.bin", b"abcdef");
    let node = core.lookup(ROOT_INO, "n.bin").await.unwrap();
    let fh = core.open(node.ino).await.unwrap();

    let before = core.read_calls();
    core.read(fh, 0, 6).await.unwrap();
    core.read(fh, 0, 6).await.unwrap();
    assert_eq!(core.read_calls(), before + 2);
}

// Impact: the client version header (RFC-022 S4) sends this code; a
// non-CalVer token would make the mount unable to pass any versioned
// surface, so failing at build/test time beats failing at every request.
// Should: parse the crate's own version token as CalVer and round-trip
// it through format_code.
#[test]
fn own_version_is_calver() {
    let code = crate::version_code();
    assert!(hopnet_common::version::code_is_valid(code));
    assert_eq!(
        hopnet_common::version::format_code(code),
        env!("CARGO_PKG_VERSION")
    );
}

// Impact: the upgrade wrapper (RFC-023) interrogates staged binaries by
// running --min-node and parsing trimmed stdout as one token; any
// decoration here would silently break candidate selection.
// Should: print exactly the bare CalVer token that round-trips back to
// MIN_NODE.
#[test]
fn min_node_display_is_a_bare_parseable_token() {
    let display = crate::min_node_display();
    assert_eq!(
        hopnet_common::version::parse_code(&display),
        Some(crate::MIN_NODE)
    );
    assert!(!display.contains(char::is_whitespace), "{display:?}");
}

// Should: accept a node at or above MIN_NODE and refuse older ones,
// naming the node as the remedy in both refusal forms.
// Should not: accept a version-less (pre-RFC-022) node — zero is not
// "unknown, assume fine", it is "too old to say".
#[test]
fn node_version_check_matrix() {
    use crate::transport::{Health, HealthReport};
    let report = |node_version| HealthReport {
        status: Health::Ready,
        node_version,
    };
    assert!(crate::check_node_version(&report(crate::MIN_NODE)).is_ok());
    assert!(crate::check_node_version(&report(crate::MIN_NODE + 1)).is_ok());
    let older = crate::check_node_version(&report(20250101)).unwrap_err();
    assert!(older.contains("upgrade the node"), "{older}");
    let unversioned = crate::check_node_version(&report(0)).unwrap_err();
    assert!(unversioned.contains("pre-RFC-022"), "{unversioned}");
    assert!(unversioned.contains("upgrade the node"), "{unversioned}");
}

// Impact: the typed variant is what separates "hold until upgraded"
// from ordinary transport noise — if a gate refusal ever degrades to a
// generic Protocol error again, the daemon would retry forever instead
// of surfacing the standardized upgrade-required state.
// Should: surface a scripted gate refusal as the typed UpgradeRequired
// on the watch, changes, and health paths, and report the scripted
// node version through the health report once cleared.
#[tokio::test]
async fn scripted_gate_refusal_is_typed_and_clearable() {
    use crate::transport::{Health, NodeTransport, TransportError};
    let (transport, handle) = MockTransport::new();
    handle.set_upgrade_required(Some((20990100, 20990100)));

    for err in [
        transport.watch().await.err().unwrap(),
        transport.changes(0).await.err().unwrap(),
        transport.health().await.err().unwrap(),
    ] {
        assert!(
            matches!(
                err,
                TransportError::UpgradeRequired {
                    min_client: 20990100,
                    ..
                }
            ),
            "{err}"
        );
    }

    handle.set_upgrade_required(None);
    handle.set_node_version(20990101);
    let report = transport.health().await.unwrap();
    assert_eq!(report.status, Health::Ready);
    assert_eq!(report.node_version, 20990101);
}

// ---------- RFC-023 S2: the activation coupling ----------

#[derive(Default)]
struct RecordingCoupling {
    entered: std::sync::atomic::AtomicUsize,
    still_held: std::sync::atomic::AtomicUsize,
}

impl crate::watch::UpgradeCoupling for RecordingCoupling {
    fn entered(&self) {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn still_held(&self) {
        self.still_held
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl RecordingCoupling {
    fn counts(&self) -> (usize, usize) {
        (
            self.entered.load(std::sync::atomic::Ordering::SeqCst),
            self.still_held.load(std::sync::atomic::Ordering::SeqCst),
        )
    }
}

// Impact: entered() is what turns a dark 426'd mount into an upgrade
// attempt; firing more than once per entry would pile up nix builds.
// Should: fire entered() exactly once when the hold begins and
// still_held() on subsequent refusals while it persists.
// Should not: fire either hook again after the hold clears, until a
// NEW hold begins — the clear re-arms the one-shot.
#[tokio::test]
async fn coupling_fires_once_per_hold_and_rearms_after_clear() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    let coupling = Arc::new(RecordingCoupling::default());
    let invalidator = Arc::new(RecordingInvalidator::default());
    tokio::spawn(
        Watcher::new(
            core,
            transport as Arc<dyn NodeTransport>,
            invalidator.clone(),
        )
        .with_upgrade_coupling(coupling.clone())
        .run(),
    );
    assert!(
        wait_until(2000, || handle.watch_connected()
            && !changes_calls(&handle).is_empty())
        .await,
        "watcher never connected + synced"
    );

    handle.set_upgrade_required(Some((20990100, 20990101)));
    handle.poke();
    assert!(
        wait_until(2000, || coupling.counts().0 == 1).await,
        "entered() never fired: {:?}",
        coupling.counts()
    );

    handle.poke();
    assert!(
        wait_until(2000, || coupling.counts().1 >= 1).await,
        "still_held() never fired: {:?}",
        coupling.counts()
    );
    assert_eq!(coupling.counts().0, 1, "entered() must stay one-shot");

    // Clear, then force the reconnect that re-arms the one-shot (the
    // flag clears only at a successful watch() connect).
    let syncs_before = changes_calls(&handle).len();
    handle.set_upgrade_required(None);
    handle.drop_watch();
    assert!(
        wait_until(2000, || changes_calls(&handle).len() > syncs_before).await,
        "watcher never resynced after the clear"
    );
    let (entered, held) = coupling.counts();
    assert_eq!(entered, 1, "no hook may fire outside a hold");

    handle.set_upgrade_required(Some((20990100, 20990101)));
    handle.poke();
    assert!(
        wait_until(2000, || coupling.counts().0 == 2).await,
        "a NEW hold must re-fire entered(): {:?}",
        coupling.counts()
    );
    let _ = held;
}

// Should: run the watch loop identically when no coupling is attached
// (unmanaged installs) — the hold stays a log-only state and the loop
// resyncs after the gate clears.
#[tokio::test]
async fn watcher_without_coupling_holds_quietly() {
    let (transport, handle) = MockTransport::new();
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
    spawn_watcher(core, transport, &handle).await;

    handle.set_upgrade_required(Some((20990100, 20990101)));
    handle.poke();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let syncs_before = changes_calls(&handle).len();
    handle.set_upgrade_required(None);
    handle.drop_watch();
    assert!(
        wait_until(2000, || changes_calls(&handle).len() > syncs_before).await,
        "watcher must resync once the gate clears"
    );
}
