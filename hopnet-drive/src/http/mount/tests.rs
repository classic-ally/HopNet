//! In-crate HTTP tests for the mount surface (RFC-018 S2) — the first
//! axum-level route tests in the workspace.
//!
//! Environment: file-backed tempdir SQLite pool (host `users` DDL +
//! consensus + storage + drive schemas), a hand-built `HostCapabilities`
//! with fixture stubs for every seam the read routes touch, and
//! `tower::ServiceExt::oneshot` against the real router with an
//! `Extension(user_id)` layer standing in for the host's device-token
//! middleware.

use std::sync::Arc;

use aes_siv::{Key, Nonce, siv::Aes256Siv};
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::{Extension, Router};
use tower::ServiceExt;

use crate::host::DriveState;
use crate::paths::encrypt_path;
use hopnet_common::CustomUUID;
use hopnet_common::mount::{MountChangesResponse, MountEnumerateResponse, MountItem};
use hopnet_projection::host::{
    BlobStreamer, BoxFuture, ByteStream, SessionAccess, SessionError, TxGateway, TxSpec,
    TxSubmitError, UserSession, WriteAdmission, WriteCheckError,
};

const USER_ID: i32 = 1;
const OTHER_USER_ID: i32 = 2;

// ---------- deterministic fixture keys ----------

fn siv_fixture() -> (Key<Aes256Siv>, Nonce) {
    let mut key_bytes = [0u8; 64];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet-drive mount tests siv");
    hasher.update(&[7u8; 32]);
    hasher.finalize_xof().fill(&mut key_bytes);
    let mut nonce_bytes = [0u8; 16];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet-drive mount tests nonce");
    hasher.update(&[7u8; 32]);
    hasher.finalize_xof().fill(&mut nonce_bytes);
    (
        Key::<Aes256Siv>::from(key_bytes),
        Nonce::from(nonce_bytes),
    )
}

fn x25519_fixture() -> x25519_dalek::StaticSecret {
    x25519_dalek::StaticSecret::from([3u8; 32])
}

// ---------- seam stubs ----------

struct FixtureSessions;
impl SessionAccess for FixtureSessions {
    fn user_session(&self, _user_id: i32) -> BoxFuture<'_, Result<UserSession, SessionError>> {
        Box::pin(async {
            let (siv_key, siv_nonce) = siv_fixture();
            Ok(UserSession {
                siv_key,
                siv_nonce,
                x25519_privkey: x25519_fixture(),
            })
        })
    }
}

struct NoTx;
impl TxGateway for NoTx {
    fn submit_batch(&self, txs: Vec<TxSpec>) -> BoxFuture<'_, Vec<Result<(), TxSubmitError>>> {
        Box::pin(async move { txs.iter().map(|_| Err(TxSubmitError::Submit)).collect() })
    }
}

struct AllowWrites;
impl WriteAdmission for AllowWrites {
    fn check_write(&self, _user_id: i32) -> BoxFuture<'_, Result<(), WriteCheckError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Serves a canned byte buffer, honoring the requested range — download
/// tests exercise Range/auth/header behaviour, not Reed-Solomon.
struct CannedBlob(Vec<u8>);
impl BlobStreamer for CannedBlob {
    fn stream(
        &self,
        _manifest: hopnet_storage::store::BlobManifest,
        _per_blob_key: Option<chacha20poly1305::Key>,
        range: Option<(u64, u64)>,
    ) -> ByteStream {
        let bytes = match range {
            Some((start, end)) => self.0[start as usize..=(end as usize).min(self.0.len() - 1)]
                .to_vec(),
            None => self.0.clone(),
        };
        Box::pin(tokio_stream::once(Ok(bytes::Bytes::from(bytes))))
    }
}

// ---------- environment ----------

#[derive(Debug)]
struct TestInit;
impl r2d2::CustomizeConnection<rusqlite::Connection, rusqlite::Error> for TestInit {
    fn on_acquire(&self, conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        register_sql_functions(conn)
    }
}

/// The two SQL helpers drive read queries need; production registration
/// lives host-side (src/db/shared.rs) and is unreachable from this crate.
fn register_sql_functions(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.create_scalar_function(
        "uuid_extract_timestamp",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let uuid_str: Option<String> = ctx.get(0)?;
            match uuid_str {
                None => Ok(None),
                Some(s) => {
                    let hex_only: String = s.replace('-', "");
                    if hex_only.len() < 12 {
                        return Ok(Some(0i64));
                    }
                    match i64::from_str_radix(&hex_only[..12], 16) {
                        Ok(millis) => Ok(Some(millis)),
                        Err(_) => Ok(Some(0i64)),
                    }
                }
            }
        },
    )?;
    conn.create_scalar_function(
        "reverse",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let s: String = ctx.get(0)?;
            Ok(s.chars().rev().collect::<String>())
        },
    )
}

struct TestEnv {
    state: DriveState,
    siv_key: Key<Aes256Siv>,
    siv_nonce: Nonce,
    // Tempdirs live as long as the env.
    _db_dir: tempfile::TempDir,
    _fragments_dir: tempfile::TempDir,
}

fn setup_env(blob_bytes: Vec<u8>) -> TestEnv {
    let db_dir = tempfile::tempdir().expect("db tempdir");
    let fragments_dir = tempfile::tempdir().expect("fragments tempdir");

    let manager = r2d2_sqlite::SqliteConnectionManager::file(db_dir.path().join("test.db"));
    let pool = r2d2::Pool::builder()
        .max_size(4)
        .connection_customizer(Box::new(TestInit))
        .build(manager)
        .expect("pool");

    {
        let conn = pool.get().unwrap();
        conn.execute_batch(
            "CREATE TABLE users (
                 user_id INTEGER PRIMARY KEY,
                 username TEXT NOT NULL,
                 x25519_pubkey BLOB
             );
             CREATE TABLE nodes (node_id INTEGER PRIMARY KEY);",
        )
        .unwrap();
        hopnet_consensus::store::install_schema(&conn).unwrap();
        hopnet_storage::store::install_schema(&conn).unwrap();
        crate::db::install_schema(&conn).unwrap();

        let pubkey = x25519_dalek::PublicKey::from(&x25519_fixture());
        conn.execute(
            "INSERT INTO users (user_id, username, x25519_pubkey) VALUES (?, 'alice', ?)",
            rusqlite::params![USER_ID, pubkey.as_bytes().as_slice()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO users (user_id, username, x25519_pubkey) VALUES (?, 'mallory', X'00')",
            rusqlite::params![OTHER_USER_ID],
        )
        .unwrap();
    }

    let (siv_key, siv_nonce) = siv_fixture();
    let state = DriveState {
        db_pool: pool,
        fragments_dir: fragments_dir.path().to_string_lossy().into_owned(),
        test_mode: true,
        node_id: Arc::new(once_cell::sync::OnceCell::from(0)),
        sessions: Arc::new(FixtureSessions),
        txs: Arc::new(NoTx),
        blobs: Arc::new(CannedBlob(blob_bytes)),
        notify: Arc::new(hopnet_projection::NullNotifier),
        write_admission: Arc::new(AllowWrites),
    };

    TestEnv {
        state,
        siv_key,
        siv_nonce,
        _db_dir: db_dir,
        _fragments_dir: fragments_dir,
    }
}

impl TestEnv {
    fn app(&self) -> Router {
        super::router::<()>(self.state.clone()).layer(Extension(USER_ID))
    }

    async fn enc(&self, plain_path: &str) -> String {
        encrypt_path(plain_path.to_string(), &self.siv_key, &self.siv_nonce)
            .await
            .unwrap()
    }

    async fn add_folder(&self, plain_path: &str) -> CustomUUID {
        self.add_folder_for(USER_ID, plain_path).await
    }

    async fn add_folder_for(&self, owner: i32, plain_path: &str) -> CustomUUID {
        let id = CustomUUID::new(None);
        let path = self.enc(plain_path).await;
        let conn = self.state.db_pool.get().unwrap();
        conn.execute(
            "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, 1, NULL)",
            rusqlite::params![id, owner, path],
        )
        .unwrap();
        id
    }

    /// A file inode; when `size` is Some a backing blob row, one fragment
    /// row (blob_manifest requires it), and an access wrap for USER_ID are
    /// created. None = empty file (data_id NULL).
    async fn add_file(&self, plain_path: &str, size: Option<u64>) -> (CustomUUID, Option<CustomUUID>) {
        let inode_id = CustomUUID::new(None);
        let path = self.enc(plain_path).await;
        let conn = self.state.db_pool.get().unwrap();

        let blob_id = match size {
            None => None,
            Some(size) => {
                let blob_id = CustomUUID::new(None);
                let hash32 = vec![0u8; 32];
                conn.execute(
                    "INSERT INTO data_blocks (id, modified_at, file_hash, fragment_count, added_bytes, placement_height, file_size)
                     VALUES (?, NULL, ?, 1, 0, NULL, ?)",
                    rusqlite::params![blob_id, hash32, size as i64],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally)
                     VALUES (?, 0, 0, ?, ?, 0, 1)",
                    rusqlite::params![blob_id, CustomUUID::new(None), hash32],
                )
                .unwrap();

                let per_blob_key = chacha20poly1305::Key::from([9u8; 32]);
                let access = hopnet_storage::crypto::wrap_blob_key(
                    &blob_id,
                    &x25519_dalek::PublicKey::from(&x25519_fixture()),
                    &per_blob_key,
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO blob_access (blob_id, recipient_pubkey, ephemeral_pubkey, wrapped_key) VALUES (?, ?, ?, ?)",
                    rusqlite::params![
                        access.blob_id,
                        access.recipient_pubkey.as_slice(),
                        access.ephemeral_pubkey.as_slice(),
                        access.wrapped_key
                    ],
                )
                .unwrap();
                Some(blob_id)
            }
        };

        conn.execute(
            "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, 0, ?)",
            rusqlite::params![inode_id, USER_ID, path, blob_id],
        )
        .unwrap();
        (inode_id, blob_id)
    }

    fn log_modification(&self, inode_id: &CustomUUID, height: i32) {
        let conn = self.state.db_pool.get().unwrap();
        conn.execute(
            "INSERT INTO modification_log (inode_id, owner_id, old_parent_id, modified_at_height)
             VALUES (?, ?, NULL, ?)",
            rusqlite::params![inode_id, USER_ID, height],
        )
        .unwrap();
    }
}

async fn get_json<T: serde::de::DeserializeOwned>(app: &Router, uri: &str) -> (StatusCode, Option<T>) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let parsed = serde_json::from_slice::<T>(&bytes).ok();
    (status, parsed)
}

async fn get_raw(app: &Router, uri: &str, range: Option<&str>) -> axum::http::Response<Body> {
    let mut builder = Request::builder().uri(uri);
    if let Some(range) = range {
        builder = builder.header(header::RANGE, range);
    }
    app.clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn body_bytes(response: axum::http::Response<Body>) -> bytes::Bytes {
    axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap()
}

/// Broadcast-backed notifier standing in for the host's (S4): the test
/// holds the sender, the /watch route subscribes.
struct BroadcastNotifier {
    tx: tokio::sync::broadcast::Sender<()>,
}
impl hopnet_projection::ChangeNotifier for BroadcastNotifier {
    fn files_changed(&self) {
        let _ = self.tx.send(());
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.tx.subscribe()
    }
}

fn setup_env_with_watch(blob_bytes: Vec<u8>) -> (TestEnv, tokio::sync::broadcast::Sender<()>) {
    let mut env = setup_env(blob_bytes);
    let tx = tokio::sync::broadcast::channel(16).0;
    env.state.notify = Arc::new(BroadcastNotifier { tx: tx.clone() });
    (env, tx)
}

// ---------- watch ----------

// Should: deliver an SSE event for every poke sent on the notifier
// channel while the connection is open.
// Impact: the poke path is the daemon's only low-latency change signal —
// a dropped poke means staleness until the TTL backstop.
#[tokio::test]
async fn watch_streams_pokes_as_sse_events() {
    let (env, tx) = setup_env_with_watch(vec![]);
    let app = env.app();

    let response = app
        .clone()
        .oneshot(Request::builder().uri("/watch").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );

    let mut body = response.into_body().into_data_stream();
    use tokio_stream::StreamExt;

    tx.send(()).unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), body.next())
        .await
        .expect("poke frame within timeout")
        .expect("stream open")
        .expect("frame ok");
    let text = String::from_utf8_lossy(&frame).into_owned();
    assert!(text.contains("data:"), "expected SSE data frame, got {text:?}");

    tx.send(()).unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), body.next())
        .await
        .expect("second poke frame")
        .expect("stream open")
        .expect("frame ok");
    assert!(String::from_utf8_lossy(&frame).contains("data:"));
}

// Should: end the SSE stream when the notifier channel is closed —
// NullNotifier's subscribe hands out a dead receiver, so the stream ends
// immediately instead of hanging.
#[tokio::test]
async fn watch_ends_on_closed_channel() {
    let env = setup_env(vec![]);
    let app = env.app();

    let response = app
        .clone()
        .oneshot(Request::builder().uri("/watch").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    use tokio_stream::StreamExt;

    let end = tokio::time::timeout(std::time::Duration::from_secs(5), body.next())
        .await
        .expect("stream should end promptly");
    assert!(end.is_none(), "expected immediate end-of-stream on dead channel");
}

// ---------- enumerate ----------

// Should: list the children of the root and of a nested folder, carrying
// name, kind, size, and blob id on file rows.
#[tokio::test]
async fn enumerate_lists_children_with_metadata() {
    let env = setup_env(vec![]);
    env.add_folder("/Documents").await;
    let (_, blob) = env.add_file("/Documents/notes.txt", Some(640)).await;
    let app = env.app();

    let (status, root) = get_json::<MountEnumerateResponse>(&app, "/enumerate").await;
    assert_eq!(status, StatusCode::OK);
    let root = root.unwrap();
    assert_eq!(root.items.len(), 1);
    assert_eq!(root.items[0].name, "Documents");
    assert!(root.items[0].size.is_none());

    let docs_id = root.items[0].id.clone().unwrap();
    let (status, docs) = get_json::<MountEnumerateResponse>(
        &app,
        &format!("/enumerate?parent_id={docs_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let docs = docs.unwrap();
    assert_eq!(docs.items.len(), 1);
    assert_eq!(docs.items[0].name, "notes.txt");
    assert_eq!(docs.items[0].size, Some(640));
    assert_eq!(docs.items[0].blob_id, blob);
    assert_eq!(docs.items[0].parent_id, Some(docs_id));
}

// Should not: leak another user's inodes into an enumeration.
// Impact: cross-user listing would defeat per-user path encryption — the
// scoping is owner_id in SQL, and this is the regression guard on it.
#[tokio::test]
async fn enumerate_does_not_leak_other_users() {
    let env = setup_env(vec![]);
    env.add_folder("/Mine").await;
    env.add_folder_for(OTHER_USER_ID, "/Theirs").await;
    let app = env.app();

    let (_, root) = get_json::<MountEnumerateResponse>(&app, "/enumerate").await;
    let names: Vec<String> = root.unwrap().items.into_iter().map(|i| i.name).collect();
    assert_eq!(names, vec!["Mine"]);
}

// Should: walk a large directory across multiple cursor pages without
// duplicating or skipping any child, and resume correctly from each
// returned cursor.
// Impact: guards the cursor-column class of bug (filtering on a different
// column than the ordering) — a mismatch silently drops or repeats files.
#[tokio::test]
async fn enumerate_cursor_pages_are_stable_and_complete() {
    let env = setup_env(vec![]);
    // 2.5 pages worth.
    for i in 0..250 {
        env.add_file(&format!("/f{i:03}.txt"), None).await;
    }
    let app = env.app();

    let mut seen = std::collections::HashSet::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let uri = match &cursor {
            None => "/enumerate".to_string(),
            Some(c) => format!("/enumerate?cursor={c}"),
        };
        let (status, page) = get_json::<MountEnumerateResponse>(&app, &uri).await;
        assert_eq!(status, StatusCode::OK);
        let page = page.unwrap();
        pages += 1;
        for item in &page.items {
            assert!(
                seen.insert(item.id.clone().unwrap().to_string()),
                "duplicate item across pages: {}",
                item.name
            );
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(seen.len(), 250);
    assert_eq!(pages, 3);
}

// Should: keep already-returned pages valid when new children appear
// mid-walk — resumed pages never re-serve or skip ids at or before the
// cursor.
#[tokio::test]
async fn enumerate_insertion_mid_walk_does_not_duplicate() {
    let env = setup_env(vec![]);
    for i in 0..120 {
        env.add_file(&format!("/a{i:03}.txt"), None).await;
    }
    let app = env.app();

    let (_, first) = get_json::<MountEnumerateResponse>(&app, "/enumerate").await;
    let first = first.unwrap();
    let cursor = first.next_cursor.clone().unwrap();
    let first_ids: std::collections::HashSet<String> = first
        .items
        .iter()
        .map(|i| i.id.clone().unwrap().to_string())
        .collect();

    // New arrivals sort after existing UUIDv7 ids, so the in-flight walk
    // picks them up rather than shifting earlier entries.
    env.add_file("/zz-new.txt", None).await;

    let (_, second) =
        get_json::<MountEnumerateResponse>(&app, &format!("/enumerate?cursor={cursor}")).await;
    let second = second.unwrap();
    for item in &second.items {
        assert!(
            !first_ids.contains(&item.id.clone().unwrap().to_string()),
            "id re-served after cursor: {}",
            item.name
        );
    }
    let names: Vec<&str> = second.items.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"zz-new.txt"));
}

// ---------- lookup ----------

// Should: resolve a child by plaintext name in the root and in a nested
// folder, via encrypted exact-path match.
#[tokio::test]
async fn lookup_resolves_by_name() {
    let env = setup_env(vec![]);
    let docs = env.add_folder("/Documents").await;
    env.add_file("/Documents/notes.txt", Some(9)).await;
    let app = env.app();

    let (status, item) = get_json::<MountItem>(&app, "/lookup?name=Documents").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(item.unwrap().id, Some(docs.clone()));

    let (status, item) =
        get_json::<MountItem>(&app, &format!("/lookup?parent_id={docs}&name=notes.txt")).await;
    assert_eq!(status, StatusCode::OK);
    let item = item.unwrap();
    assert_eq!(item.name, "notes.txt");
    assert_eq!(item.size, Some(9));
}

// Should: return 404 for a name that does not exist under the parent.
#[tokio::test]
async fn lookup_miss_is_404() {
    let env = setup_env(vec![]);
    let app = env.app();
    let (status, _) = get_json::<MountItem>(&app, "/lookup?name=ghost.txt").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// Should not: match an inode whose stored path was not produced by the
// user's SIV encryption — the lookup encrypts before comparing.
// Impact: guards the trust boundary — plaintext names must never reach
// SQL comparison directly.
#[tokio::test]
async fn lookup_compares_encrypted_not_plaintext() {
    let env = setup_env(vec![]);
    let conn = env.state.db_pool.get().unwrap();
    conn.execute(
        "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, '/plain.txt', 0, NULL)",
        rusqlite::params![CustomUUID::new(None), USER_ID],
    )
    .unwrap();
    drop(conn);
    let app = env.app();

    let (status, _) = get_json::<MountItem>(&app, "/lookup?name=plain.txt").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------- item ----------

// Should: synthesize the root item (no id, no parent, folder) with the
// current consensus height, without any inode row existing for it.
#[tokio::test]
async fn item_root_is_synthesized() {
    let env = setup_env(vec![]);
    let app = env.app();
    let (status, root) = get_json::<MountItem>(&app, "/item").await;
    assert_eq!(status, StatusCode::OK);
    let root = root.unwrap();
    assert!(root.id.is_none());
    assert!(root.parent_id.is_none());
    assert_eq!(root.height, Some(0), "pre-genesis current height is 0");
}

// Should: return full metadata for a file by id — size, blob id, dates,
// and parent linkage back to its folder.
#[tokio::test]
async fn item_by_id_carries_metadata() {
    let env = setup_env(vec![]);
    let docs = env.add_folder("/Documents").await;
    let (file_id, blob) = env.add_file("/Documents/notes.txt", Some(640)).await;
    env.log_modification(&file_id, 4);
    let app = env.app();

    let (status, item) = get_json::<MountItem>(&app, &format!("/item?id={file_id}")).await;
    assert_eq!(status, StatusCode::OK);
    let item = item.unwrap();
    assert_eq!(item.id, Some(file_id));
    assert_eq!(item.parent_id, Some(docs));
    assert_eq!(item.size, Some(640));
    assert_eq!(item.blob_id, blob);
    assert_eq!(item.height, Some(4));
    assert!(item.created_ms > 0);
}

// ---------- changes ----------

// Should: report creations since the anchor as live items, deletions as
// deleted ids, and filter rows at or below since_height out.
// Impact: this feed is the daemon's only source of remote-change truth —
// missed rows mean silent divergence between kernel cache and node state.
#[tokio::test]
async fn changes_reports_live_and_deleted_since_anchor() {
    let env = setup_env(vec![]);
    let (old_file, _) = env.add_file("/old.txt", None).await;
    env.log_modification(&old_file, 2);
    let (new_file, _) = env.add_file("/new.txt", None).await;
    env.log_modification(&new_file, 5);

    // A deleted inode: log rows exist, inode row does not.
    let ghost = CustomUUID::new(None);
    env.log_modification(&ghost, 6);

    let app = env.app();
    let (status, changes) =
        get_json::<MountChangesResponse>(&app, "/changes?since_height=3").await;
    assert_eq!(status, StatusCode::OK);
    let changes = changes.unwrap();

    let names: Vec<&str> = changes.items.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names, vec!["new.txt"], "height-2 row must be filtered out");
    assert_eq!(changes.deleted_ids, vec![ghost]);
}

// ---------- download ----------

// Should: stream the whole blob with 200, Content-Length, and
// Accept-Ranges when no Range header is sent.
#[tokio::test]
async fn download_full_body() {
    let content = b"hello mount surface".to_vec();
    let env = setup_env(content.clone());
    let (_, blob) = env.add_file("/hello.txt", Some(content.len() as u64)).await;
    let app = env.app();

    let response = get_raw(&app, &format!("/download?blob_id={}", blob.unwrap()), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::ACCEPT_RANGES).unwrap(),
        "bytes"
    );
    assert_eq!(
        response.headers().get(header::CONTENT_LENGTH).unwrap(),
        &content.len().to_string()
    );
    assert_eq!(body_bytes(response).await.as_ref(), content.as_slice());
}

// Should: serve a byte range as 206 with Content-Range and the sliced
// body — the daemon's segment fetches depend on exact range semantics.
#[tokio::test]
async fn download_range_is_partial_content() {
    let content = b"0123456789".to_vec();
    let env = setup_env(content.clone());
    let (_, blob) = env.add_file("/digits.txt", Some(content.len() as u64)).await;
    let app = env.app();

    let response = get_raw(
        &app,
        &format!("/download?blob_id={}", blob.unwrap()),
        Some("bytes=2-5"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers().get(header::CONTENT_RANGE).unwrap(),
        "bytes 2-5/10"
    );
    assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "4");
    assert_eq!(body_bytes(response).await.as_ref(), b"2345");
}

// Should: reject a range starting at or past EOF with 416 and the
// `bytes */N` total-size form.
#[tokio::test]
async fn download_range_past_eof_is_416() {
    let content = b"tiny".to_vec();
    let env = setup_env(content.clone());
    let (_, blob) = env.add_file("/tiny.txt", Some(content.len() as u64)).await;
    let app = env.app();

    let response = get_raw(
        &app,
        &format!("/download?blob_id={}", blob.unwrap()),
        Some("bytes=100-"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        response.headers().get(header::CONTENT_RANGE).unwrap(),
        "bytes */4"
    );
}

// Should not: serve a blob the requesting user holds no access wrap for.
// Impact: the blob_access row IS the authorization model for downloads —
// nothing else gates blob-addressed reads.
#[tokio::test]
async fn download_without_access_wrap_is_403() {
    let content = b"secret".to_vec();
    let env = setup_env(content.clone());
    let (_, blob) = env.add_file("/secret.txt", Some(content.len() as u64)).await;
    let blob = blob.unwrap();
    let conn = env.state.db_pool.get().unwrap();
    conn.execute(
        "DELETE FROM blob_access WHERE blob_id = ?",
        rusqlite::params![blob],
    )
    .unwrap();
    drop(conn);
    let app = env.app();

    let response = get_raw(&app, &format!("/download?blob_id={blob}"), None).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

// Should: return 404 for a blob id that does not exist.
#[tokio::test]
async fn download_unknown_blob_is_404() {
    let env = setup_env(vec![]);
    let app = env.app();
    let response = get_raw(
        &app,
        &format!("/download?blob_id={}", CustomUUID::new(None)),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
