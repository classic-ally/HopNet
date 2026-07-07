//! Host seams for the drive's HTTP/business surface (RFC-015).
//!
//! The drive's routers and upload/download flows reach the host through
//! five capabilities: per-user session keys (`SessionAccess`), consensus
//! transaction submission with host-side signing (`TxGateway`), blob
//! reconstruction streaming (`BlobStreamer` — type-erases the storage
//! crate's generic `api::get`), post-apply change signaling (re-used
//! `ChangeNotifier` from hopnet-projection), and import write gating
//! (`WriteAdmission`). One host adapter implements them all (pattern:
//! the main crate's SubstrateHost for the RFC-014 seams).
//!
//! dyn-object style (boxed futures) deliberately: these hang off an axum
//! state struct and cross one box per REQUEST, never per byte — the
//! download body itself is already a boxed stream.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type ByteStream = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<bytes::Bytes, hopnet_storage::StorageError>>
            + Send,
    >,
>;

/// The key material a user's session grants the drive: path SIV keys and
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
    fn submit_batch(
        &self,
        txs: Vec<TxSpec>,
    ) -> BoxFuture<'_, Vec<Result<(), TxSubmitError>>>;

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

/// Type-erases hopnet_storage::api::get + the host's seam bundle (the
/// generic GetNet can't cross a dyn boundary). Host impl = api::get over
/// its SubstrateHost seams.
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

/// Write admission for drive mutations (the takeout import gate today;
/// generalizes to a host-owned per-user flag at Stage D5).
pub trait WriteAdmission: Send + Sync {
    fn check_write(&self, user_id: i32) -> BoxFuture<'_, Result<(), WriteCheckError>>;
}

/// The drive's axum state: concrete DB access (the drive owns its SQL)
/// plus the five host seams.
#[derive(Clone)]
pub struct DriveState {
    pub db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    pub fragments_dir: String,
    pub test_mode: bool,
    pub node_id: Arc<once_cell::sync::OnceCell<i32>>,
    pub sessions: Arc<dyn SessionAccess>,
    pub txs: Arc<dyn TxGateway>,
    pub blobs: Arc<dyn BlobStreamer>,
    pub notify: Arc<dyn hopnet_projection::ChangeNotifier>,
    pub write_admission: Arc<dyn WriteAdmission>,
}

impl DriveState {
    pub fn node_id(&self) -> Option<i32> {
        self.node_id.get().copied()
    }
}
