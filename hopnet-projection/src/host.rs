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
    /// Consensus failed the transaction (timeout / queue full / internal).
    Submit,
}

pub trait TxGateway: Send + Sync {
    /// Sign and submit a batch as ONE consensus submission (per-entry
    /// results). The drive's upload flow batches a user-signed
    /// insert_files with a node-signed self-check attestation.
    fn submit_batch(&self, txs: Vec<TxSpec>) -> BoxFuture<'_, Vec<Result<(), TxSubmitError>>>;

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
