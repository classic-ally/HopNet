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
    Box<
        dyn tokio_stream::Stream<Item = Result<bytes::Bytes, hopnet_storage::StorageError>>
            + Send,
    >,
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
