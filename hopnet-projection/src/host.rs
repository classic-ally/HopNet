//! Generic host capabilities (RFC-015 Stage D5a).
//!
//! The host-side seams ANY projection or projection-adjacent service
//! (hopnet-drive, hopnet-takeout, photos, …) may consume: per-user session
//! key material ([`SessionAccess`]) and consensus transaction submission
//! with host-side signing ([`TxGateway`]). Moved down from
//! `hopnet_drive::host` so services like takeout can use them without
//! depending on a specific projection; drive re-exports them.
//!
//! dyn-object style (boxed futures) deliberately: these hang off axum/task
//! state structs and cross one box per REQUEST, never per byte.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The key material a user's session grants a projection: path SIV keys and
/// the derived X25519 private key for blob-key unwrap. The host derives
/// all of it — ed25519 identity keys never cross this seam.
pub struct UserSession {
    pub siv_key: aes_siv::Key<aes_siv::siv::Aes256Siv>,
    pub siv_nonce: aes_siv::Nonce,
    pub x25519_privkey: x25519_dalek::StaticSecret,
}

#[derive(Debug)]
pub enum SessionError {
    /// Session exists but expired — HTTP 401.
    Unauthorized,
    /// No session cached (user must log in) — HTTP 428.
    PreconditionRequired,
}

pub trait SessionAccess: Send + Sync {
    fn user_session(&self, user_id: i32) -> BoxFuture<'_, Result<UserSession, SessionError>>;
}

/// Who signs a consensus transaction. Signing stays host-side: `Node` uses
/// the node identity key, `User` resolves that user's session key.
#[derive(Debug, Clone, Copy)]
pub enum TxSigner {
    Node,
    User(i32),
}

/// One transaction to sign-and-submit.
pub struct TxSpec {
    pub function: &'static str,
    pub payload: Vec<u8>,
    pub signer: TxSigner,
}

#[derive(Debug)]
pub enum TxSubmitError {
    /// Signing failed (missing session / node identity).
    Signing,
    /// Business-logic rejection (permanent) with the engine's reason —
    /// distinct so the shares routes can keep their 409 mapping.
    Rejected(String),
    /// Consensus wait timed out — the outcome is UNKNOWN (the tx may
    /// still commit later). Strict-consistency routes map this to 504,
    /// never to success. (RFC-018 S6; previously flattened into Submit.)
    Timeout,
    /// Consensus failed the transaction (queue full / internal).
    Submit,
    /// Mesh-global admission is closed (regenesis moratorium, RFC-019
    /// S5) — retryable, unlike Rejected. Routes map it to 503; the
    /// string is a serialized host-side refusal body.
    Unavailable(String),
}

pub trait TxGateway: Send + Sync {
    /// Sign and submit a batch as ONE consensus submission (per-entry
    /// results). The drive's upload flow batches a user-signed
    /// insert_files with a node-signed self-check attestation.
    fn submit_batch(&self, txs: Vec<TxSpec>) -> BoxFuture<'_, Vec<Result<(), TxSubmitError>>>;

    /// Like submit_batch, but success carries the decided height and is
    /// only reported once the transaction is decided AND applied on THIS
    /// node (RFC-018 S6). The height is an upper bound on the applying
    /// block: reads anchored at it observe the transaction's effects.
    fn submit_batch_decided(
        &self,
        txs: Vec<TxSpec>,
    ) -> BoxFuture<'_, Vec<Result<u64, TxSubmitError>>>;

    /// Convenience: single transaction.
    fn submit(&self, tx: TxSpec) -> BoxFuture<'_, Result<(), TxSubmitError>> {
        let fut = self.submit_batch(vec![tx]);
        Box::pin(async move {
            fut.await
                .into_iter()
                .next()
                .unwrap_or(Err(TxSubmitError::Submit))
        })
    }

    /// Convenience: single transaction, strict (decided + applied here).
    fn submit_decided(&self, tx: TxSpec) -> BoxFuture<'_, Result<u64, TxSubmitError>> {
        let fut = self.submit_batch_decided(vec![tx]);
        Box::pin(async move {
            fut.await
                .into_iter()
                .next()
                .unwrap_or(Err(TxSubmitError::Submit))
        })
    }
}

pub type ByteStream = Pin<
    Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, hopnet_storage::StorageError>> + Send>,
>;

/// Type-erases hopnet_storage::api::get + the host's seam bundle (the
/// generic GetNet can't cross a dyn boundary). Host impl = api::get over
/// its SubstrateHost seams. Moved down from hopnet_drive::host at RFC-016
/// Stage 1 — any projection streams blobs, not just the drive.
pub trait BlobStreamer: Send + Sync {
    fn stream(
        &self,
        manifest: hopnet_storage::store::BlobManifest,
        per_blob_key: Option<chacha20poly1305::Key>,
        range: Option<(u64, u64)>,
    ) -> ByteStream;
}

#[derive(Debug)]
pub struct WriteDenied {
    /// Human-readable reason (import in progress, …) — maps to HTTP 409.
    pub reason: String,
}

#[derive(Debug)]
pub enum WriteCheckError {
    /// Writes are gated for this user — HTTP 409 (empty body, matching the
    /// host's takeout import gate).
    Denied(WriteDenied),
    /// The check itself failed host-side (DB error) — HTTP 500.
    Internal,
}

/// Write admission for projection mutations (the takeout import gate
/// today). The host is the composition root: its impl may consult any
/// service (takeout's per-user import flag); projections only ever see
/// this trait. Moved down from hopnet_drive::host at RFC-016 Stage 1.
pub trait WriteAdmission: Send + Sync {
    fn check_write(&self, user_id: i32) -> BoxFuture<'_, Result<(), WriteCheckError>>;
}

/// The full host capability bundle a projection builds its axum state
/// from (RFC-016 Stage 2): concrete DB access (projections own their SQL)
/// plus every host seam. Field set is DriveState's, verbatim — drive's
/// `DriveState` is now an alias of this.
#[derive(Clone)]
pub struct HostCapabilities {
    pub db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    pub fragments_dir: String,
    pub test_mode: bool,
    pub node_id: std::sync::Arc<once_cell::sync::OnceCell<i32>>,
    pub sessions: std::sync::Arc<dyn SessionAccess>,
    pub txs: std::sync::Arc<dyn TxGateway>,
    pub blobs: std::sync::Arc<dyn BlobStreamer>,
    pub notify: std::sync::Arc<dyn crate::ChangeNotifier>,
    pub write_admission: std::sync::Arc<dyn WriteAdmission>,
}

impl HostCapabilities {
    pub fn node_id(&self) -> Option<i32> {
        self.node_id.get().copied()
    }
}

/// The host ingest: `api::put_with` under the upload-ledger hook, and the
/// clean-up when it fails. Every site that writes fragments before their
/// `fragment_hashes` rows exist goes through here (the drive's
/// `process_uploaded_file`, the photos `Submitter`). Each batch of
/// fragments is ledgered as this node's own upload before its files are
/// written, so the sweep holds the rowless files instead of deleting them
/// as orphans (consensus-bugs 20). A put that fails part-way — the client
/// disconnected, a read error, no space, a ledger write refused — would
/// otherwise leave its first chunks held for the whole retention, so the
/// blob is abandoned: its holds released and its files unlinked, each
/// re-checked for a row and another upload's hold first. The put's error
/// is returned either way; the client retries.
pub async fn put_own_upload<R: tokio::io::AsyncRead + Unpin>(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: &str,
    blob_id: hopnet_storage::BlobId,
    source: R,
    file_size: usize,
    per_blob_key: &chacha20poly1305::Key,
) -> Result<hopnet_storage::api::PutOutcome, hopnet_storage::StorageError> {
    let mut guard = UploadGuard::new(pool.clone(), fragments_dir.to_owned(), blob_id.clone());
    let outcome = hopnet_storage::api::put_with(
        source,
        file_size,
        blob_id.clone(),
        per_blob_key,
        fragments_dir,
        upload_ledger_hook(
            pool.clone(),
            fragments_dir.to_owned(),
            blob_id.clone(),
            guard.abandoned.clone(),
        ),
    )
    .await;
    match &outcome {
        Ok(_) => guard.settled(),
        Err(_) => {
            // Abandon here, awaited, so the caller's error response follows
            // the clean-up; the guard then has nothing left to do.
            guard.abandoned.store(true, Ordering::SeqCst);
            match abandon_own_upload(pool.clone(), fragments_dir.to_owned(), blob_id.clone()).await
            {
                Ok(unlinked) => tracing::info!(%blob_id, unlinked, "abandoned a failed upload"),
                Err(e) => tracing::warn!(
                    %blob_id,
                    "failed upload not abandoned, held until the retention: {e}"
                ),
            }
            guard.settled();
        }
    }
    outcome
}

/// Blob ids with a put in flight in this process — the exact signal that
/// an upload is live, which no timestamp heuristic gives (a slow client can
/// take longer than the orphan grace per 40 MB chunk). Registered for the
/// life of `put_own_upload`'s guard; a put never outlives the process, so
/// after a restart nothing is live and every hold is purgeable.
static LIVE_UPLOADS: Mutex<Option<HashSet<hopnet_storage::BlobId>>> = Mutex::new(None);

fn live_uploads() -> std::sync::MutexGuard<'static, Option<HashSet<hopnet_storage::BlobId>>> {
    LIVE_UPLOADS.lock().unwrap_or_else(|p| p.into_inner())
}

/// Is a put for `blob_id` in flight in this process?
pub fn upload_is_live(blob_id: &hopnet_storage::BlobId) -> bool {
    live_uploads()
        .as_ref()
        .is_some_and(|live| live.contains(blob_id))
}

/// A blob's registration as a live upload, for its holder's lifetime.
pub struct LiveUpload(hopnet_storage::BlobId);

impl LiveUpload {
    pub fn register(blob_id: hopnet_storage::BlobId) -> Self {
        live_uploads()
            .get_or_insert_with(HashSet::new)
            .insert(blob_id.clone());
        LiveUpload(blob_id)
    }
}

impl Drop for LiveUpload {
    fn drop(&mut self) {
        if let Some(live) = live_uploads().as_mut() {
            live.remove(&self.0);
        }
    }
}

/// What outlives a dropped `put_own_upload` future. A client disconnect
/// makes hyper drop the handler mid-stream: the future's `Err` path never
/// runs, while the detached chunk task keeps encoding. Unless `settled`
/// (the put returned), dropping the guard marks the upload abandoned — the
/// hook refuses the detached task's next batch — and schedules the abandon
/// on the blocking pool (inline when no runtime is at hand).
struct UploadGuard {
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: String,
    blob_id: hopnet_storage::BlobId,
    abandoned: Arc<AtomicBool>,
    settled: bool,
    _live: LiveUpload,
}

impl UploadGuard {
    fn new(
        pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
        fragments_dir: String,
        blob_id: hopnet_storage::BlobId,
    ) -> Self {
        UploadGuard {
            pool,
            fragments_dir,
            _live: LiveUpload::register(blob_id.clone()),
            blob_id,
            abandoned: Arc::new(AtomicBool::new(false)),
            settled: false,
        }
    }

    fn settled(&mut self) {
        self.settled = true;
    }
}

impl Drop for UploadGuard {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        self.abandoned.store(true, Ordering::SeqCst);
        let pool = self.pool.clone();
        let dir = std::mem::take(&mut self.fragments_dir);
        let blob_id = self.blob_id.clone();
        tracing::info!(%blob_id, "upload dropped mid-stream; abandoning");
        let abandon = move || match abandon_own_upload_blocking(&pool, &dir, &blob_id) {
            Ok(unlinked) => tracing::info!(%blob_id, unlinked, "abandoned a dropped upload"),
            Err(e) => tracing::warn!(
                %blob_id,
                "dropped upload not abandoned, held until the retention: {e}"
            ),
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(abandon);
            }
            Err(_) => abandon(),
        }
    }
}

/// Release a failed put's ledger holds and unlink its fragments that have
/// no `fragment_hashes` row and no other upload's hold, on the blocking
/// pool, in batches of `UNLINK_BATCH` write transactions. Returns the
/// files unlinked.
pub async fn abandon_own_upload(
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: String,
    blob_id: hopnet_storage::BlobId,
) -> Result<usize, String> {
    tokio::task::spawn_blocking(move || {
        abandon_own_upload_blocking(&pool, &fragments_dir, &blob_id)
    })
    .await
    .map_err(|e| format!("abandon task: {e}"))?
}

/// Rowless files unlinked per write transaction, by the host's sweep and
/// purge and by an abandoned put: small, because the IMMEDIATE transaction
/// holds the database's write lock that consensus apply also needs
/// (busy_timeout 5 s), and a cold disk's unlinks are not quick.
pub const UNLINK_BATCH: usize = 32;

/// `stat` outside the lock, then inside one write transaction drop
/// `release`'s own holds on the batch, if given, and re-check and unlink
/// it. Dropping the holds first matters to an abandoned upload's leftover
/// batch: its own entries would otherwise make the re-check keep every
/// file.
fn unlink_batch(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: &str,
    release: Option<&hopnet_storage::BlobId>,
    batch: &[hopnet_storage::Blake3Hash],
) -> Result<hopnet_storage::store::DeletedFragments, LedgerError> {
    let sized = hopnet_storage::store::stat_fragments(fragments_dir, batch);
    let mut conn = pool
        .get()
        .map_err(|e| LedgerError::Transient(format!("pool: {e}")))?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(|e| ledger_sqlite_error(e, "tx"))?;
    if let Some(blob_id) = release {
        hopnet_storage::store::release_local_upload_hashes(&tx, blob_id, batch)
            .map_err(|e| ledger_sqlite_error(e, "release batch"))?;
    }
    let gone = hopnet_storage::store::delete_unclaimed_fragments(&tx, fragments_dir, &sized)
        .map_err(|e| ledger_sqlite_error(e, "unlink re-check"))?;
    crate::dbstats::commit_timed(tx).map_err(|e| ledger_sqlite_error(e, "unlink commit"))?;
    Ok(gone)
}

/// Drop every ledger entry of `blob_id` in one write transaction, and
/// return their hashes.
fn release_blob(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    blob_id: &hopnet_storage::BlobId,
) -> Result<Vec<hopnet_storage::Blake3Hash>, LedgerError> {
    let mut conn = pool
        .get()
        .map_err(|e| LedgerError::Transient(format!("pool: {e}")))?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(|e| ledger_sqlite_error(e, "tx"))?;
    let candidates = hopnet_storage::store::release_local_uploads(&tx, blob_id)
        .map_err(|e| ledger_sqlite_error(e, "release"))?;
    crate::dbstats::commit_timed(tx).map_err(|e| ledger_sqlite_error(e, "release commit"))?;
    Ok(candidates)
}

/// Drop `blob_id`'s ledger entries for one batch in one transaction.
fn release_batch(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    blob_id: &hopnet_storage::BlobId,
    batch: &[hopnet_storage::Blake3Hash],
) -> Result<usize, LedgerError> {
    let mut conn = pool
        .get()
        .map_err(|e| LedgerError::Transient(format!("pool: {e}")))?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(|e| ledger_sqlite_error(e, "tx"))?;
    let released = hopnet_storage::store::release_local_upload_hashes(&tx, blob_id, batch)
        .map_err(|e| ledger_sqlite_error(e, "release batch"))?;
    crate::dbstats::commit_timed(tx).map_err(|e| ledger_sqlite_error(e, "release commit"))?;
    Ok(released)
}

fn abandon_own_upload_blocking(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: &str,
    blob_id: &hopnet_storage::BlobId,
) -> Result<usize, String> {
    abandon_own_upload_with(
        pool,
        fragments_dir,
        blob_id,
        LEDGER_RETRY_BUDGET,
        std::thread::sleep,
    )
}

/// `abandon_own_upload_blocking` with the retry budget and `sleep`
/// injected. The release and each unlink batch retry transient failures
/// under the same budget as a ledger write, so a moment of write-lock
/// contention does not leave a failed put held for the whole retention.
fn abandon_own_upload_with(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: &str,
    blob_id: &hopnet_storage::BlobId,
    budget: std::time::Duration,
    mut sleep: impl FnMut(std::time::Duration),
) -> Result<usize, String> {
    let candidates = retry_transient(|| release_blob(pool, blob_id), budget, &mut sleep)
        .map_err(|e| e.to_string())?;
    let mut unlinked = 0;
    for batch in candidates.chunks(UNLINK_BATCH) {
        unlinked += retry_transient(
            || unlink_batch(pool, fragments_dir, None, batch),
            budget,
            &mut sleep,
        )
        .map_err(|e| e.to_string())?
        .hashes
        .len();
    }
    Ok(unlinked)
}

/// A ledger write's failure, by whether waiting could help.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerError {
    /// A pool checkout timed out or SQLite was busy/locked: another writer
    /// held the database for a moment. Retried under `LEDGER_RETRY_BUDGET`.
    Transient(String),
    /// Anything else; fails the put at once.
    Fatal(String),
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedgerError::Transient(e) => write!(f, "transient: {e}"),
            LedgerError::Fatal(e) => write!(f, "{e}"),
        }
    }
}

fn ledger_sqlite_error(e: rusqlite::Error, what: &str) -> LedgerError {
    match e.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DatabaseBusy) | Some(rusqlite::ErrorCode::DatabaseLocked) => {
            LedgerError::Transient(format!("{what}: {e}"))
        }
        _ => LedgerError::Fatal(format!("{what}: {e}")),
    }
}

/// How long one ledger batch keeps retrying transient failures before the
/// put fails. A multi-GB put must not be thrown away because a pool
/// checkout (2 s) or a write lock (5 s) was missed once.
pub const LEDGER_RETRY_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);
const LEDGER_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(50);
const LEDGER_RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(5);

/// Run `op` until it succeeds or fails fatally, sleeping between transient
/// failures with exponential backoff (50 ms doubling, capped at 5 s) until
/// `budget` is spent. `sleep` is injected so the policy is testable.
pub fn retry_transient<T>(
    mut op: impl FnMut() -> Result<T, LedgerError>,
    budget: std::time::Duration,
    mut sleep: impl FnMut(std::time::Duration),
) -> Result<T, LedgerError> {
    let mut spent = std::time::Duration::ZERO;
    let mut wait = LEDGER_RETRY_FIRST;
    loop {
        match op() {
            Err(LedgerError::Transient(e)) if spent < budget => {
                let wait_now = wait.min(budget - spent);
                tracing::debug!("ledger write busy, retrying in {wait_now:?}: {e}");
                sleep(wait_now);
                spent += wait_now;
                wait = (wait * 2).min(LEDGER_RETRY_CAP);
            }
            other => return other,
        }
    }
}

/// The `api::put_with` hook for a host ingest: ledgers each batch of
/// fragments as this node's own upload before their files are written
/// (`put_own_upload`), retrying transient failures under
/// `LEDGER_RETRY_BUDGET` (it runs on a blocking thread). Once the upload
/// is `abandoned` (the future was dropped) it refuses the batch, so the
/// detached chunk task stops ledgering and writing, and releases and
/// unlinks the one batch it let through last — the abandon may have run
/// before those files landed, and their own holds must go first or the
/// unlink's re-check would keep every file. A batch whose ledger write was
/// still retrying when the upload was abandoned is refused too, after its
/// fresh holds are released: the abandon may have released the blob before
/// that write committed. A ledger failure aborts the put, so the client
/// retries rather than proceeding with files nothing protects.
pub fn upload_ledger_hook(
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: String,
    blob_id: hopnet_storage::BlobId,
    abandoned: Arc<AtomicBool>,
) -> impl FnMut(&[hopnet_storage::Blake3Hash]) -> Result<(), hopnet_storage::StorageError> + Send + 'static
{
    let record_pool = pool.clone();
    let record_blob = blob_id.clone();
    upload_ledger_hook_with(pool, fragments_dir, blob_id, abandoned, move |hashes| {
        retry_transient(
            || record_own_upload(&record_pool, &record_blob, hashes),
            LEDGER_RETRY_BUDGET,
            std::thread::sleep,
        )
    })
}

/// `upload_ledger_hook` with the ledger write (`record`, retries included)
/// injected.
fn upload_ledger_hook_with(
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: String,
    blob_id: hopnet_storage::BlobId,
    abandoned: Arc<AtomicBool>,
    mut record: impl FnMut(&[hopnet_storage::Blake3Hash]) -> Result<usize, LedgerError> + Send + 'static,
) -> impl FnMut(&[hopnet_storage::Blake3Hash]) -> Result<(), hopnet_storage::StorageError> + Send + 'static
{
    let refused = || {
        Err(hopnet_storage::StorageError::Host(
            "upload abandoned".into(),
        ))
    };
    let mut last_batch: Vec<hopnet_storage::Blake3Hash> = Vec::new();
    move |hashes| {
        if abandoned.load(Ordering::SeqCst) {
            if !last_batch.is_empty() {
                let unlinked = retry_transient(
                    || unlink_batch(&pool, &fragments_dir, Some(&blob_id), &last_batch),
                    LEDGER_RETRY_BUDGET,
                    std::thread::sleep,
                );
                if let Err(e) = unlinked {
                    tracing::warn!(%blob_id, "abandoned upload's last batch not unlinked: {e}");
                }
            }
            last_batch.clear();
            return refused();
        }
        record(hashes).map_err(|e| hopnet_storage::StorageError::Host(e.to_string()))?;
        if abandoned.load(Ordering::SeqCst) {
            // Abandoned while the write was in flight: the abandon's
            // release may have run before it committed. Nothing of this
            // batch is written yet, so dropping its holds is enough.
            let released = retry_transient(
                || release_batch(&pool, &blob_id, hashes),
                LEDGER_RETRY_BUDGET,
                std::thread::sleep,
            );
            if let Err(e) = released {
                tracing::warn!(%blob_id, "abandoned upload's last holds not released: {e}");
            }
            last_batch.clear();
            return refused();
        }
        last_batch.clear();
        last_batch.extend_from_slice(hashes);
        Ok(())
    }
}

/// One ledger write for a batch of an ingest's fragments, in one
/// transaction committed here (`commit_timed`).
pub fn record_own_upload<'a>(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    blob_id: &hopnet_storage::BlobId,
    fragment_hashes: impl IntoIterator<Item = &'a hopnet_storage::Blake3Hash>,
) -> Result<usize, LedgerError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // A pool checkout fails only by timing out: transient by definition.
    let mut conn = pool
        .get()
        .map_err(|e| LedgerError::Transient(format!("pool: {e}")))?;
    let tx = conn
        .transaction()
        .map_err(|e| ledger_sqlite_error(e, "tx"))?;
    let written = hopnet_storage::store::record_local_uploads(&tx, blob_id, fragment_hashes, now)
        .map_err(|e| ledger_sqlite_error(e, "upload ledger"))?;
    crate::dbstats::commit_timed(tx).map_err(|e| ledger_sqlite_error(e, "upload ledger commit"))?;
    Ok(written)
}

/// Per-user write gate. Returns 409 Conflict (empty body) on any request
/// hitting a route this layer is attached to while the host's write
/// admission denies the authenticated user (an active takeout import
/// today). Reads `user_id` from request extensions populated upstream by
/// the host's auth middleware. Missing user → 401, check failure → 500,
/// denied → 409.
///
/// Attachment is explicit per route (or per write-only sub-router) — the
/// middleware itself does no method discrimination. Routes that should
/// bypass the gate simply don't have the layer applied.
pub async fn write_gate(
    axum::extract::State(state): axum::extract::State<HostCapabilities>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    use axum::http::StatusCode;

    let user_id = req
        .extensions()
        .get::<i32>()
        .copied()
        .ok_or(StatusCode::UNAUTHORIZED)?;

    match state.write_admission.check_write(user_id).await {
        Ok(()) => Ok(next.run(req).await),
        Err(WriteCheckError::Denied(_)) => Err(StatusCode::CONFLICT),
        Err(WriteCheckError::Internal) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A plaintext source that streams `good` zero bytes and then fails,
    /// the way a client disconnect does.
    struct Disconnects {
        left: usize,
    }

    impl tokio::io::AsyncRead for Disconnects {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.left == 0 {
                return Poll::Ready(Err(std::io::Error::other("client went away")));
            }
            let n = self.left.min(buf.remaining());
            buf.put_slice(&vec![0u8; n]);
            self.left -= n;
            Poll::Ready(Ok(()))
        }
    }

    fn storage_pool() -> r2d2::Pool<r2d2_sqlite::SqliteConnectionManager> {
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .build(r2d2_sqlite::SqliteConnectionManager::memory())
            .unwrap();
        let chain = &hopnet_storage::store::CHAIN;
        hopnet_common::chain::replay(&pool.get().unwrap(), chain, chain.head()).unwrap();
        pool
    }

    // Impact: a put that fails after its first chunk (client disconnect,
    // read error, no space, a later ledger write refused) has thirty
    // ledgered files on disk that nothing will ever give a row; left
    // alone they would be held for the whole retention.
    // Should: on a failed put, release the blob's holds and unlink its
    // rowless files, and still return the put's own error.
    // Should not: leave a ledger entry or a fragment file behind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_put_that_fails_part_way_abandons_its_ledgered_fragments() {
        let dir = tempfile::tempdir().unwrap();
        let frags = dir.path().join("fragments").to_string_lossy().into_owned();
        let pool = storage_pool();
        let blob_id = hopnet_storage::BlobId::new(None);
        let key: chacha20poly1305::Key = [0x42u8; 32].into();
        // One full chunk streams and is encoded, ledgered and written;
        // the source fails inside the second.
        let one_chunk = hopnet_storage::rs::CHUNK_SIZE;
        let err = put_own_upload(
            &pool,
            &frags,
            blob_id.clone(),
            Disconnects {
                left: one_chunk + 1024,
            },
            one_chunk + 4096,
            &key,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, hopnet_storage::StorageError::Read(_)),
            "{err:?}"
        );

        let conn = pool.get().unwrap();
        assert!(
            hopnet_storage::store::held_local_uploads(&conn)
                .unwrap()
                .is_empty(),
            "no hold survives the failed put"
        );
        assert!(!upload_is_live(&blob_id), "the put is over");
        assert!(
            hopnet_storage::fragstore::scan_fragments_detailed(&frags)
                .unwrap()
                .is_empty(),
            "no fragment file survives the failed put"
        );
    }

    /// A source that streams one full chunk and then never resolves, the
    /// way a stalled client does.
    struct Stalls {
        left: usize,
    }

    impl tokio::io::AsyncRead for Stalls {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.left == 0 {
                return Poll::Pending;
            }
            let n = self.left.min(buf.remaining());
            buf.put_slice(&vec![0u8; n]);
            self.left -= n;
            Poll::Ready(Ok(()))
        }
    }

    // Impact: a client disconnect makes hyper drop the handler future; the
    // put's Err path never runs, the detached chunk task keeps writing, and
    // thirty ledgered files per chunk would be held for the retention.
    // Should: register the blob as live while the put runs, and on the
    // future being dropped mid-stream release its holds, unlink its files
    // and deregister it, without the caller doing anything.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_put_future_abandons_its_upload() {
        let dir = tempfile::tempdir().unwrap();
        let frags = dir.path().join("fragments").to_string_lossy().into_owned();
        let pool = storage_pool();
        let blob_id = hopnet_storage::BlobId::new(None);
        let key: chacha20poly1305::Key = [0x42u8; 32].into();
        let one_chunk = hopnet_storage::rs::CHUNK_SIZE;

        let put = put_own_upload(
            &pool,
            &frags,
            blob_id.clone(),
            Stalls {
                left: one_chunk + 1024,
            },
            one_chunk + 4096,
            &key,
        );
        // The first chunk is encoded, ledgered and written; the source then
        // stalls and the timeout drops the future.
        let dropped = tokio::time::timeout(std::time::Duration::from_secs(20), put).await;
        assert!(dropped.is_err(), "the put must not finish on its own");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let held = hopnet_storage::store::held_local_uploads(&pool.get().unwrap()).unwrap();
            let files = hopnet_storage::fragstore::scan_fragments_detailed(&frags).unwrap();
            if held.is_empty() && files.is_empty() && !upload_is_live(&blob_id) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "still held: {} entries, {} files on disk, live: {}",
                held.len(),
                files.len(),
                upload_is_live(&blob_id)
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// `count` distinct fragments tagged `tag`: a hash (first byte `tag`,
    /// second the index) and a payload. The store does not check one
    /// against the other.
    fn batch_of(tag: u8, count: u8) -> Vec<(hopnet_storage::Blake3Hash, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = tag;
                bytes[1] = i;
                let data = format!("fragment {i} of batch {tag}").into_bytes();
                (hopnet_storage::Blake3Hash::from_bytes(bytes), data)
            })
            .collect()
    }

    fn hashes_of(
        batch: &[(hopnet_storage::Blake3Hash, Vec<u8>)],
    ) -> Vec<hopnet_storage::Blake3Hash> {
        batch.iter().map(|(hash, _)| *hash).collect()
    }

    fn write_files(frags: &str, batch: &[(hopnet_storage::Blake3Hash, Vec<u8>)]) {
        for (hash, data) in batch {
            hopnet_storage::fragstore::store_fragment(frags, hash, data.clone()).unwrap();
        }
    }

    // Impact: a batch's ledger write can retry for up to a minute. If the
    // put is dropped meanwhile, the abandon's release can run before that
    // write commits; letting the batch through would then write files whose
    // fresh holds keep them for the whole retention.
    // Should: refuse a batch whose upload was abandoned while its ledger
    // write was in flight, dropping the holds that write left.
    // Should not: leave a ledger entry or a fragment file of that batch.
    #[test]
    fn a_batch_abandoned_during_its_ledger_write_leaves_nothing_held() {
        let dir = tempfile::tempdir().unwrap();
        let frags = dir.path().join("fragments").to_string_lossy().into_owned();
        let pool = storage_pool();
        let blob_id = hopnet_storage::BlobId::new(None);
        let abandoned = Arc::new(AtomicBool::new(false));
        let batch = batch_of(0x10, 3);

        let recorder = {
            let pool = pool.clone();
            let frags = frags.clone();
            let blob_id = blob_id.clone();
            let abandoned = abandoned.clone();
            move |hashes: &[hopnet_storage::Blake3Hash]| {
                // The put is dropped and its abandon runs while this write
                // is still retrying; the write commits afterwards.
                abandoned.store(true, Ordering::SeqCst);
                abandon_own_upload_blocking(&pool, &frags, &blob_id).unwrap();
                record_own_upload(&pool, &blob_id, hashes)
            }
        };
        let mut hook = upload_ledger_hook_with(
            pool.clone(),
            frags.clone(),
            blob_id.clone(),
            abandoned,
            recorder,
        );
        let accepted = hook(&hashes_of(&batch));
        if accepted.is_ok() {
            // What the chunk task does with a batch the hook lets through.
            write_files(&frags, &batch);
        }

        assert!(accepted.is_err(), "the abandoned upload's batch is refused");
        assert!(
            hopnet_storage::store::held_local_uploads(&pool.get().unwrap())
                .unwrap()
                .is_empty(),
            "no hold of the refused batch survives"
        );
        assert!(
            hopnet_storage::fragstore::scan_fragments_detailed(&frags)
                .unwrap_or_default()
                .is_empty(),
            "no file of the refused batch is written"
        );
    }

    // Impact: when the abandon's own pass runs before the last batch's
    // files land, the next hook call is the only clean-up of that batch,
    // and its own holds would make every unlink re-check keep the file.
    // Should: on the first call after the upload is abandoned, release the
    // last accepted batch's holds and unlink its files.
    #[test]
    fn an_abandoned_upload_releases_its_last_batch_before_unlinking_it() {
        let dir = tempfile::tempdir().unwrap();
        let frags = dir.path().join("fragments").to_string_lossy().into_owned();
        let pool = storage_pool();
        let blob_id = hopnet_storage::BlobId::new(None);
        let abandoned = Arc::new(AtomicBool::new(false));
        let mut hook = upload_ledger_hook(
            pool.clone(),
            frags.clone(),
            blob_id.clone(),
            abandoned.clone(),
        );

        let first = batch_of(0x20, 4);
        hook(&hashes_of(&first)).unwrap();
        write_files(&frags, &first);
        abandoned.store(true, Ordering::SeqCst);
        assert!(hook(&hashes_of(&batch_of(0x21, 2))).is_err());

        assert!(
            hopnet_storage::store::held_local_uploads(&pool.get().unwrap())
                .unwrap()
                .is_empty(),
            "the last batch's holds are released"
        );
        assert!(
            hopnet_storage::fragstore::scan_fragments_detailed(&frags)
                .unwrap()
                .is_empty(),
            "the last batch's files are unlinked"
        );
    }

    // Impact: the abandon competes with consensus apply for the write
    // lock; giving up on the first SQLITE_BUSY would leave a failed put's
    // files held for the whole retention.
    // Should: retry the abandon's release and unlinks through a transient
    // busy database, and still release every hold and unlink every file.
    #[test]
    fn an_abandon_retries_through_a_busy_database() {
        let dir = tempfile::tempdir().unwrap();
        let frags = dir.path().join("fragments").to_string_lossy().into_owned();
        let db_path = dir.path().join("node.db");
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .build(r2d2_sqlite::SqliteConnectionManager::file(&db_path))
            .unwrap();
        {
            let conn = pool.get().unwrap();
            let chain = &hopnet_storage::store::CHAIN;
            hopnet_common::chain::replay(&conn, chain, chain.head()).unwrap();
            // Give up on a lock fast, so the test does not wait out the
            // default busy_timeout.
            conn.execute_batch("PRAGMA busy_timeout = 50;").unwrap();
        }
        let blob_id = hopnet_storage::BlobId::new(None);
        let batch = batch_of(0x30, 5);
        record_own_upload(&pool, &blob_id, &hashes_of(&batch)).unwrap();
        write_files(&frags, &batch);

        // Another writer holds the database until the first retry's wait.
        let locker = rusqlite::Connection::open(&db_path).unwrap();
        locker.execute_batch("BEGIN IMMEDIATE;").unwrap();
        let mut locker = Some(locker);
        let mut waits = 0;
        let unlinked =
            abandon_own_upload_with(&pool, &frags, &blob_id, LEDGER_RETRY_BUDGET, |_| {
                waits += 1;
                if let Some(locker) = locker.take() {
                    locker.execute_batch("ROLLBACK;").unwrap();
                }
            });

        assert_eq!(unlinked, Ok(5));
        assert_eq!(waits, 1, "one transient busy, one retry");
        assert!(
            hopnet_storage::store::held_local_uploads(&pool.get().unwrap())
                .unwrap()
                .is_empty()
        );
        assert!(hopnet_storage::fragstore::scan_fragments_detailed(&frags)
            .unwrap()
            .is_empty());
    }

    // Impact: each ledger batch needs a pool checkout and the write lock; a
    // single missed checkout or SQLITE_BUSY would otherwise throw away a
    // multi-GB put that was minutes in.
    // Should: retry a transiently failing write with growing backoff until
    // it succeeds, within the budget.
    // Should: give up once the budget is spent, and fail a fatal error at
    // once without sleeping.
    #[test]
    fn ledger_writes_retry_transient_failures_with_backoff() {
        let mut failures_left = 3;
        let mut slept = Vec::new();
        let written = retry_transient(
            || {
                if failures_left > 0 {
                    failures_left -= 1;
                    Err(LedgerError::Transient("busy".into()))
                } else {
                    Ok(10usize)
                }
            },
            LEDGER_RETRY_BUDGET,
            |wait| slept.push(wait),
        );
        assert_eq!(written, Ok(10));
        assert_eq!(
            slept,
            vec![
                std::time::Duration::from_millis(50),
                std::time::Duration::from_millis(100),
                std::time::Duration::from_millis(200),
            ]
        );

        let mut slept = std::time::Duration::ZERO;
        let exhausted = retry_transient(
            || Err::<(), _>(LedgerError::Transient("busy".into())),
            std::time::Duration::from_secs(1),
            |wait| slept += wait,
        );
        assert!(matches!(exhausted, Err(LedgerError::Transient(_))));
        assert_eq!(
            slept,
            std::time::Duration::from_secs(1),
            "the whole budget, no more"
        );

        let mut sleeps = 0;
        let fatal = retry_transient(
            || Err::<(), _>(LedgerError::Fatal("constraint".into())),
            LEDGER_RETRY_BUDGET,
            |_| sleeps += 1,
        );
        assert_eq!(fatal, Err(LedgerError::Fatal("constraint".into())));
        assert_eq!(sleeps, 0);
    }

    // Should: classify a pool checkout failure and SQLITE_BUSY/LOCKED as
    // transient, and any other SQLite error as fatal.
    #[test]
    fn ledger_errors_are_transient_only_when_waiting_could_help() {
        let busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        );
        assert!(matches!(
            ledger_sqlite_error(busy, "tx"),
            LedgerError::Transient(_)
        ));
        let locked = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_LOCKED),
            None,
        );
        assert!(matches!(
            ledger_sqlite_error(locked, "tx"),
            LedgerError::Transient(_)
        ));
        let constraint = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
            None,
        );
        assert!(matches!(
            ledger_sqlite_error(constraint, "tx"),
            LedgerError::Fatal(_)
        ));
        assert!(matches!(
            ledger_sqlite_error(rusqlite::Error::QueryReturnedNoRows, "tx"),
            LedgerError::Fatal(_)
        ));
    }
}
