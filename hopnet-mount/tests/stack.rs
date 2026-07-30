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

// Should: expose a live node's tree through a real kernel mount — ls and
// stat via std::fs observe the seeded namespace with sane metadata.
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

    let core = Arc::new(MountCore::new(transport, DEFAULT_TTL));
    let fs = HopFs::new(core, tokio::runtime::Handle::current());
    let mountpoint = tempfile::tempdir().unwrap();
    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("hopnet-test".to_string()),
    ];
    let session = fuser::spawn_mount(fs, mountpoint.path(), &config).unwrap();

    // std::fs is blocking; keep it off the runtime threads the FUSE
    // callbacks need.
    let dir = mountpoint.path().to_path_buf();
    let observed = tokio::task::spawn_blocking(move || {
        let names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        let meta = std::fs::metadata(dir.join("Docs/hello.txt")).unwrap();
        (names, meta.len(), meta.is_file())
    })
    .await
    .unwrap();

    assert_eq!(observed.0, vec!["Docs".to_string()]);
    assert_eq!(observed.1, 11);
    assert!(observed.2);

    drop(session);
}
