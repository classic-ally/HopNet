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

use std::future::Future;
use std::pin::Pin;

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
    let outcome = hopnet_storage::api::put_with(
        source,
        file_size,
        blob_id.clone(),
        per_blob_key,
        fragments_dir,
        upload_ledger_hook(pool.clone(), blob_id.clone()),
    )
    .await;
    if outcome.is_err() {
        match abandon_own_upload(pool.clone(), fragments_dir.to_owned(), blob_id.clone()).await {
            Ok(unlinked) => tracing::info!(%blob_id, unlinked, "abandoned a failed upload"),
            Err(e) => {
                tracing::warn!(%blob_id, "failed upload not abandoned, held until the retention: {e}")
            }
        }
    }
    outcome
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

/// Rowless files unlinked per write transaction (the host's sweep and
/// purge use the same bound).
pub const UNLINK_BATCH: usize = 256;

fn abandon_own_upload_blocking(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    fragments_dir: &str,
    blob_id: &hopnet_storage::BlobId,
) -> Result<usize, String> {
    let mut conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    let candidates = {
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| format!("tx: {e}"))?;
        let candidates = hopnet_storage::store::release_local_uploads(&tx, blob_id)
            .map_err(|e| format!("release: {e}"))?;
        crate::dbstats::commit_timed(tx).map_err(|e| format!("release commit: {e}"))?;
        candidates
    };
    let mut unlinked = 0;
    for batch in candidates.chunks(UNLINK_BATCH) {
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| format!("tx: {e}"))?;
        unlinked += hopnet_storage::store::delete_unclaimed_fragments(&tx, fragments_dir, batch)
            .map_err(|e| format!("unlink re-check: {e}"))?
            .hashes
            .len();
        crate::dbstats::commit_timed(tx).map_err(|e| format!("unlink commit: {e}"))?;
    }
    Ok(unlinked)
}

/// The `api::put_with` hook for a host ingest: ledgers each batch of
/// fragments as this node's own upload before their files are written
/// (`put_own_upload`). A ledger failure aborts the put, so the client
/// retries rather than proceeding with files nothing protects.
pub fn upload_ledger_hook(
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    blob_id: hopnet_storage::BlobId,
) -> impl FnMut(&[hopnet_storage::Blake3Hash]) -> Result<(), hopnet_storage::StorageError> + Send + 'static
{
    move |hashes| {
        record_own_upload(&pool, &blob_id, hashes)
            .map(|_| ())
            .map_err(hopnet_storage::StorageError::Host)
    }
}

/// One ledger write for a batch of an ingest's fragments, in one
/// transaction committed here (`commit_timed`).
pub fn record_own_upload<'a>(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    blob_id: &hopnet_storage::BlobId,
    fragment_hashes: impl IntoIterator<Item = &'a hopnet_storage::Blake3Hash>,
) -> Result<usize, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut conn = pool.get().map_err(|e| format!("pool: {e}"))?;
    let tx = conn.transaction().map_err(|e| format!("tx: {e}"))?;
    let written = hopnet_storage::store::record_local_uploads(&tx, blob_id, fragment_hashes, now)
        .map_err(|e| format!("upload ledger: {e}"))?;
    crate::dbstats::commit_timed(tx).map_err(|e| format!("upload ledger commit: {e}"))?;
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
        assert_eq!(
            hopnet_storage::store::newest_local_upload(&conn, &blob_id).unwrap(),
            None
        );
        assert!(
            hopnet_storage::fragstore::scan_fragments_detailed(&frags)
                .unwrap()
                .is_empty(),
            "no fragment file survives the failed put"
        );
    }
}
