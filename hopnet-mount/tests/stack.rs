//! Stack contract tests (RFC-018 S3): HttpTransport against a real node.
//!
//! Spawns `target/debug/hopnet` (ephemeral DB, test mode, random port) —
//! run `cargo build` first so the binary exists. Not run in CI (workspace
//! CI is `--lib --bins`); dev-run like tests/fileprovider_integration.rs.
//! The `#[ignore]` smoke test additionally performs a real FUSE mount.

#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use hopnet_mount::http_transport::HttpTransport;
use hopnet_mount::transport::{Health, Item, ItemId, ItemKind, NodeTransport, TransportError};

struct NodeGuard {
    child: Child,
    port: u16,
}

impl NodeGuard {
    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for NodeGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

fn hopnet_binary() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.join("../target/debug/hopnet")
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn boot_node() -> NodeGuard {
    let binary = hopnet_binary();
    assert!(
        binary.exists(),
        "target/debug/hopnet missing — run `cargo build` before this test"
    );
    let port = free_port();
    let child = Command::new(&binary)
        .env("HOPNET_EPHEMERAL_DB", "1")
        .env("HOPNET_HTTP_PORT", port.to_string())
        .env("HOPNET_TEST_MODE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn hopnet");
    let guard = NodeGuard { child, port };

    let health_url = format!("{}/api/integrations/mount/health", guard.base());
    for _ in 0..75 {
        if reqwest::get(&health_url).await.is_ok() {
            return guard;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("node did not come up on port {port}");
}

/// Runs /setup, mints a device token via the test route, and returns
/// (api_key, jwt) for transport auth and JWT seeding respectively.
async fn provision(node: &NodeGuard) -> (String, String) {
    let client = reqwest::Client::new();

    let setup: serde_json::Value = client
        .post(format!("{}/api/setup", node.base()))
        .json(&serde_json::json!({"username": "stack-test", "node_name": "stack-node"}))
        .send()
        .await
        .expect("setup request")
        .json()
        .await
        .expect("setup json");
    let passphrase = setup["passphrase"].as_str().expect("passphrase").to_string();

    // The test route registers a device via consensus; retry until decided.
    let mut api_key = None;
    for _ in 0..50 {
        let response = client
            .get(format!("{}/api/integrations/fileprovider/test", node.base()))
            .send()
            .await
            .expect("test route");
        if response.status().is_success() {
            let body: serde_json::Value = response.json().await.expect("test json");
            if let Some(key) = body["api_key"].as_str() {
                api_key = Some(key.to_string());
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let api_key = api_key.expect("device token mint");

    let login: serde_json::Value = client
        .post(format!("{}/api/login", node.base()))
        .json(&serde_json::json!({"username": "stack-test", "passphrase": passphrase}))
        .send()
        .await
        .expect("login request")
        .json()
        .await
        .expect("login json");
    let jwt = login["token"].as_str().expect("jwt").to_string();

    (api_key, jwt)
}

/// Upload `contents` as `name` under plaintext `parent_plain` (folders are
/// created implicitly), then wait until the transport observes the file by
/// resolving the parent chain component-by-component (consensus decides
/// asynchronously).
async fn seed_file(
    node: &NodeGuard,
    jwt: &str,
    transport: &HttpTransport,
    parent_plain: &str,
    name: &str,
    contents: &[u8],
) -> Item {
    let part = reqwest::multipart::Part::bytes(contents.to_vec())
        .file_name(name.to_string());
    let form = reqwest::multipart::Form::new()
        .text("path", parent_plain.to_string())
        .part(format!("file_{}", contents.len()), part);
    let status = reqwest::Client::new()
        .post(format!("{}/api/files", node.base()))
        .bearer_auth(jwt)
        .multipart(form)
        .send()
        .await
        .expect("upload")
        .status();
    assert!(status.is_success(), "upload failed: {status}");

    'poll: for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut parent = ItemId::Root;
        for segment in parent_plain.split('/').filter(|s| !s.is_empty()) {
            match transport
                .lookup(parent.clone(), segment.to_string())
                .await
                .expect("segment lookup during seed")
            {
                Some(folder) => parent = folder.id,
                None => continue 'poll,
            }
        }
        if let Some(item) = transport
            .lookup(parent, name.to_string())
            .await
            .expect("file lookup during seed")
        {
            return item;
        }
    }
    panic!("seeded file {name} never appeared");
}

// Should: complete the full read contract against a live node — health,
// synthesized root, empty enumerate, seeded folder + file visible through
// lookup/enumerate with correct kind/size/blob, URL-encoding-hostile
// names resolving, misses as Ok(None), bad credentials as Unauthorized,
// and a killed node as Unavailable rather than a hang.
// Impact: this is the S2/S3 wire-contract seam — the mock can't catch
// serialization or encoding mismatches; only this test does.
#[tokio::test]
async fn http_transport_full_read_contract() {
    let mut node = boot_node().await;
    let (api_key, jwt) = provision(&node).await;
    let transport = HttpTransport::new(&node.base(), &api_key).unwrap();

    assert_eq!(transport.health().await.unwrap(), Health::Ready);

    let root = transport.item(ItemId::Root).await.unwrap().unwrap();
    assert_eq!(root.id, ItemId::Root);
    assert_eq!(root.kind, ItemKind::Folder);

    let empty = transport.enumerate(ItemId::Root, None).await.unwrap();
    assert!(empty.items.is_empty());
    assert!(empty.next.is_none());

    // Name chosen to break naive query building: space, ampersand, percent.
    let hostile_name = "report & notes 100%.txt";
    seed_file(
        &node,
        &jwt,
        &transport,
        "/Documents",
        hostile_name,
        b"stack contract content",
    )
    .await;
    let docs = transport
        .lookup(ItemId::Root, "Documents".to_string())
        .await
        .unwrap()
        .expect("Documents folder");
    assert_eq!(docs.kind, ItemKind::Folder);
    let file = transport
        .lookup(docs.id.clone(), hostile_name.to_string())
        .await
        .unwrap()
        .expect("hostile-name file resolves through URL encoding");
    assert_eq!(
        file.kind,
        ItemKind::File {
            size: b"stack contract content".len() as u64
        }
    );
    assert!(file.blob.is_some(), "non-empty file must carry its blob id");
    assert_eq!(file.parent, docs.id);

    let listing = transport.enumerate(docs.id.clone(), None).await.unwrap();
    let names: Vec<&str> = listing.items.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names, vec![hostile_name]);

    assert!(
        transport
            .lookup(ItemId::Root, "ghost.txt".to_string())
            .await
            .unwrap()
            .is_none(),
        "miss is Ok(None), not an error"
    );

    // S5: ranged blob reads through the transport (exercises the S2
    // Range header path end to end — real RS reconstruction under it).
    let blob = file.blob.clone().expect("seeded file has a blob");
    let full = transport
        .read_blob(blob.clone(), 0, b"stack contract content".len() as u64)
        .await
        .unwrap();
    assert_eq!(full, b"stack contract content");
    let slice = transport.read_blob(blob.clone(), 6, 8).await.unwrap();
    assert_eq!(slice, b"contract");
    let past_eof = transport.read_blob(blob, 500, 8).await.unwrap();
    assert!(past_eof.is_empty(), "past-EOF range read must be empty");

    // Well-formed token (uuid.secret_hex) for a device that doesn't exist —
    // a malformed one is rejected as 400 before auth even runs.
    let unknown_device = format!(
        "{}.{}",
        hopnet_common::CustomUUID::new(None),
        "00".repeat(32)
    );
    let bad = HttpTransport::new(&node.base(), &unknown_device).unwrap();
    match bad.item(ItemId::Root).await {
        Err(TransportError::Unauthorized) => {}
        other => panic!("expected Unauthorized, got {other:?}"),
    }

    node.kill();
    match transport.item(ItemId::Root).await {
        Err(TransportError::Unavailable(_)) => {}
        other => panic!("expected Unavailable after node death, got {other:?}"),
    }
}

// Should: deliver a poke over a live /watch connection when a mutation
// commits, interleaved with server heartbeats, and expose the mutation
// through changes(anchor).
// Impact: proves HostNotifier's platform-neutral broadcast arm actually
// fires on Linux — the poke path's reason to exist.
#[tokio::test]
async fn watch_pokes_flow_end_to_end() {
    use hopnet_mount::transport::WatchEvent;
    use tokio_stream::StreamExt;

    let node = boot_node().await;
    let (api_key, jwt) = provision(&node).await;
    let transport = HttpTransport::new(&node.base(), &api_key).unwrap();

    let anchor = transport
        .changes(i32::MAX as i64)
        .await
        .expect("anchor init")
        .height;

    let mut stream = transport.watch().await.expect("watch connect");

    // Consume events in the background; record what arrives.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_writer = seen.clone();
    let consumer = tokio::spawn(async move {
        while let Some(event) = stream.next().await {
            seen_writer.lock().unwrap().push(event);
        }
    });

    seed_file(&node, &jwt, &transport, "/Watched", "ping.txt", b"poke me").await;

    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    let mut got_poke = false;
    let mut got_heartbeat = false;
    while std::time::Instant::now() < deadline && !(got_poke && got_heartbeat) {
        {
            let seen = seen.lock().unwrap();
            got_poke = seen.contains(&WatchEvent::Poke);
            got_heartbeat = seen.contains(&WatchEvent::Heartbeat);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(got_poke, "no poke observed after committed mutation");
    assert!(got_heartbeat, "no heartbeat within 25s (keepalive is 15s)");
    consumer.abort();

    let delta = transport.changes(anchor).await.expect("delta");
    let names: Vec<&str> = delta.items.iter().map(|i| i.name.as_str()).collect();
    assert!(
        names.contains(&"ping.txt") && names.contains(&"Watched"),
        "changes since pre-mutation anchor must include the new file+folder, got {names:?}"
    );
    assert!(delta.height > anchor);
}

// Should: return from every mount mutation only after the transaction is
// applied on this node — an IMMEDIATE follow-up read (no sleeps, no
// polling) observes the effect. Create folder, create file with content,
// rename, delete — the full mkdir && cd contract over real consensus.
// Impact: S6's reason to exist; before it, 2xx preceded local apply.
#[tokio::test]
async fn mutations_are_strict_read_your_writes() {
    let node = boot_node().await;
    let (api_key, _jwt) = provision(&node).await;
    let transport = HttpTransport::new(&node.base(), &api_key).unwrap();
    let client = reqwest::Client::new();
    let base = format!("{}/api/integrations/mount", node.base());

    // Create folder — strict.
    let form = reqwest::multipart::Form::new().text("folder_name", "StrictDir");
    let response: serde_json::Value = client
        .post(format!("{base}/create"))
        .bearer_auth(&api_key)
        .multipart(form)
        .send()
        .await
        .expect("create folder")
        .json()
        .await
        .expect("create response json");
    let folder_id = response["item"]["id"].as_str().expect("folder id").to_string();
    let create_height = response["height"].as_i64().expect("height");
    assert!(create_height > 0);

    // IMMEDIATELY visible — no sleeps.
    let found = transport
        .lookup(ItemId::Root, "StrictDir".to_string())
        .await
        .unwrap()
        .expect("folder visible in the same breath as the 201");
    let ItemId::Inode(found_uuid) = &found.id else {
        panic!("folder resolved to root");
    };
    assert_eq!(found_uuid.to_string(), folder_id);

    // Create file with content under it — strict; content immediately
    // readable through real RS reconstruction.
    let content = b"strict content";
    let part = reqwest::multipart::Part::bytes(content.to_vec()).file_name("s6.txt");
    let form = reqwest::multipart::Form::new()
        .text("parent_id", folder_id.clone())
        .part(format!("file_{}", content.len()), part);
    let response: serde_json::Value = client
        .post(format!("{base}/create"))
        .bearer_auth(&api_key)
        .multipart(form)
        .send()
        .await
        .expect("create file")
        .json()
        .await
        .expect("file response json");
    let blob_id: hopnet_common::CustomUUID = response["item"]["blob_id"]
        .as_str()
        .expect("blob id")
        .parse()
        .unwrap();
    let bytes = transport
        .read_blob(blob_id, 0, content.len() as u64)
        .await
        .expect("immediate content read");
    assert_eq!(bytes, content);

    let file_id = response["item"]["id"].as_str().unwrap().to_string();

    // Rename — strict.
    let response = client
        .patch(format!("{base}/modify"))
        .bearer_auth(&api_key)
        .json(&serde_json::json!({ "id": file_id, "new_name": "renamed.txt" }))
        .send()
        .await
        .expect("modify");
    assert!(response.status().is_success());
    let folder_item_id = found.id.clone();
    assert!(
        transport
            .lookup(folder_item_id.clone(), "renamed.txt".to_string())
            .await
            .unwrap()
            .is_some(),
        "new name immediately resolvable"
    );
    assert!(
        transport
            .lookup(folder_item_id.clone(), "s6.txt".to_string())
            .await
            .unwrap()
            .is_none(),
        "old name immediately gone"
    );

    // Delete recursively — strict.
    let response = client
        .delete(format!("{base}/delete"))
        .bearer_auth(&api_key)
        .json(&serde_json::json!({ "id": folder_id, "recursive": true }))
        .send()
        .await
        .expect("delete");
    assert!(response.status().is_success());
    assert!(
        transport
            .lookup(ItemId::Root, "StrictDir".to_string())
            .await
            .unwrap()
            .is_none(),
        "deleted folder immediately gone"
    );
}

// Should: expose a live node's tree through a real kernel mount — ls and
// stat via std::fs observe the seeded namespace with sane metadata — and
// reflect a REMOTE content modification in stat within seconds (the
// inval_inode path busting the kernel's 60s attr cache), not after TTL.
// (Closes S1's smoke-test gap; #[ignore] because it needs /dev/fuse.)
#[tokio::test]
#[ignore = "requires /dev/fuse and a built hopnet binary"]
async fn fuse_mount_smoke_against_live_node() {
    use hopnet_mount::attrs::DEFAULT_TTL;
    use hopnet_mount::fuse::HopFs;
    use hopnet_mount::vfs::MountCore;
    use std::sync::Arc;

    let node = boot_node().await;
    let (api_key, jwt) = provision(&node).await;
    let transport = Arc::new(HttpTransport::new(&node.base(), &api_key).unwrap());

    seed_file(
        &node,
        &jwt,
        &transport,
        "/Docs",
        "hello.txt",
        b"mount smoke",
    )
    .await;

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        hopnet_mount::cache::CacheManager::new(
            hopnet_mount::cache::CacheConfig {
                root: cache_dir.path().join("content"),
                segment_size: hopnet_mount::cache::DEFAULT_SEGMENT_SIZE,
                policy: hopnet_mount::cache::EvictionPolicy::MaxBytes { bytes: 1 << 30 },
            },
            transport.clone() as Arc<dyn NodeTransport>,
        )
        .unwrap(),
    );
    let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL).with_cache(cache));
    let fs = HopFs::new(core.clone(), tokio::runtime::Handle::current());
    let mountpoint = tempfile::tempdir().unwrap();
    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("hopnet-test".to_string()),
    ];
    let session = fuser::spawn_mount(fs, mountpoint.path(), &config).unwrap();

    // S4: watch loop with the real kernel invalidator.
    let invalidator = Arc::new(hopnet_mount::fuse::FuserInvalidator(session.notifier()));
    let watcher = tokio::spawn(
        hopnet_mount::watch::Watcher::new(
            core,
            transport.clone() as Arc<dyn NodeTransport>,
            invalidator,
        )
        .run(),
    );

    // std::fs is blocking; keep it off the runtime threads the FUSE
    // callbacks need.
    let dir = mountpoint.path().to_path_buf();
    let observed = tokio::task::spawn_blocking({
        let dir = dir.clone();
        move || {
            let names: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            let meta = std::fs::metadata(dir.join("Docs/hello.txt")).unwrap();
            (names, meta.len(), meta.is_file())
        }
    })
    .await
    .unwrap();

    assert_eq!(observed.0, vec!["Docs".to_string()]);
    assert_eq!(observed.1, 11);
    assert!(observed.2);

    // The stat above put hello.txt's attrs in the KERNEL cache (60s TTL).
    // Modify content remotely; the poke → inval_inode path must make stat
    // reflect the new size within seconds, not after the TTL.
    let docs = transport
        .lookup(ItemId::Root, "Docs".to_string())
        .await
        .unwrap()
        .unwrap();
    let file = transport
        .lookup(docs.id.clone(), "hello.txt".to_string())
        .await
        .unwrap()
        .unwrap();
    let ItemId::Inode(inode_uuid) = &file.id else {
        panic!("file id")
    };
    let new_content = b"mount smoke, now considerably longer".to_vec();
    let part = reqwest::multipart::Part::bytes(new_content.clone()).file_name("hello.txt");
    let form = reqwest::multipart::Form::new()
        .text("inode_id", inode_uuid.to_string())
        .part(format!("file_{}", new_content.len()), part);
    let status = reqwest::Client::new()
        .patch(format!("{}/api/files", node.base()))
        .bearer_auth(&jwt)
        .multipart(form)
        .send()
        .await
        .expect("patch")
        .status();
    assert!(status.is_success(), "content patch failed: {status}");

    let target = new_content.len() as u64;
    let fresh = tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let len = std::fs::metadata(dir.join("Docs/hello.txt")).unwrap().len();
            if len == target {
                return true;
            }
            if std::time::Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    })
    .await
    .unwrap();
    assert!(
        fresh,
        "stat must reflect remote modify within 10s (kernel TTL is 60s) — inval_inode failed"
    );

    // S5: content reads through the kernel — whole file and a mid-file
    // slice must match what the node stores (real RS reconstruction).
    let file_path = mountpoint.path().join("Docs/hello.txt");
    let (whole, slice, held_content) = tokio::task::spawn_blocking({
        let file_path = file_path.clone();
        let expected = new_content.clone();
        move || {
            use std::io::{Read, Seek, SeekFrom};
            let whole = std::fs::read(&file_path).unwrap();

            let mut f = std::fs::File::open(&file_path).unwrap();
            f.seek(SeekFrom::Start(6)).unwrap();
            let mut slice = vec![0u8; 5];
            f.read_exact(&mut slice).unwrap();

            // Snapshot-at-open proof: hold this fd across a remote
            // replace; it must keep serving the CURRENT (second) version.
            let mut held = std::fs::File::open(&file_path).unwrap();
            // Touch it so the open definitely reached the daemon.
            let mut first = [0u8; 1];
            held.read_exact(&mut first).unwrap();
            assert_eq!(first[0], expected[0]);
            (whole, slice, held)
        }
    })
    .await
    .map(|(w, s, h)| (w, s, h))
    .unwrap();
    assert_eq!(whole, new_content);
    assert_eq!(slice, &new_content[6..11]);

    // Remote replace with a third version.
    let third = b"THIRD VERSION".to_vec();
    let part = reqwest::multipart::Part::bytes(third.clone()).file_name("hello.txt");
    let form = reqwest::multipart::Form::new()
        .text("inode_id", inode_uuid.to_string())
        .part(format!("file_{}", third.len()), part);
    assert!(
        reqwest::Client::new()
            .patch(format!("{}/api/files", node.base()))
            .bearer_auth(&jwt)
            .multipart(form)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );

    let expected_second = new_content.clone();
    let third_clone = third.clone();
    let snapshot_ok = tokio::task::spawn_blocking(move || {
        use std::io::{Read, Seek, SeekFrom};
        let mut held = held_content;
        // Wait for the fresh-open path to see the third version.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let fresh = std::fs::read(&file_path).unwrap();
            if fresh == third_clone {
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err(format!("fresh open never saw third version, got {fresh:?}"));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        // The held fd must read a CONSISTENT version — either its
        // daemon-layer snapshot (second) or, via the shared per-inode
        // page cache, the third; never a torn mix. (Per-fd isolation is
        // deliberately not promised — see OpenFile's scope note.)
        held.seek(SeekFrom::Start(0)).unwrap();
        let mut observed = Vec::new();
        held.read_to_end(&mut observed).unwrap();
        if observed != expected_second && observed != third_clone {
            return Err(format!("held fd tore across versions: {observed:?}"));
        }
        Ok(())
    })
    .await
    .unwrap();
    snapshot_ok.expect("held fd version consistency");

    watcher.abort();
    drop(session);
}
