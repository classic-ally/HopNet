//! `HttpDispatch`: `PhotoDispatch` over the node's thin-client routes.
//!
//! Every request carries the RFC-012 device token as a Bearer credential;
//! the node's device-token middleware bootstraps the session that signs
//! transactions and derives `uploaded_by`. Resource bytes stream — the
//! publisher's `ExactLen`-wrapped reader becomes a chunked request body,
//! and the node re-enforces the declared length inline.
//!
//! Unreachability (connect/timeout/HTTP 503 shedding) is folded into
//! `PhotosCoreError::Dispatch` messages with [`UNREACHABLE_PREFIX`], which
//! `flow` matches to classify park-vs-retry. Stringly, but both ends live
//! in this crate and it avoids a second probe round-trip per failure.

use hopnet_common::CustomUUID;
use hopnet_photos_core::PhotosCoreError;
use hopnet_photos_core::dispatch::{
    LibraryMembership, PhotoDispatch, SyncBatch, UploadedDataBlock,
};

pub(crate) const UNREACHABLE_PREFIX: &str = "node-unreachable: ";

/// Small-request timeout. Uploads get NO total timeout (a multi-GB original
/// can legitimately stream for many minutes); the transaction route gets its
/// own, longer than the node's 120s block-until-decided consensus wait.
const SMALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const TX_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

/// Outcome of the committed-state confirm probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitProbe {
    Committed,
    NotCommitted,
    /// Transport-level failure or shedding — park class.
    Unreachable(String),
    /// Unexpected HTTP status — transient class.
    Failed(String),
}

/// Wire shape of the node's `POST /api/photos/client/resolve` response.
#[derive(Debug, serde::Deserialize)]
pub struct ResolveResponseWire {
    pub responsibility: String,
    pub entries: Vec<ResolveEntryWire>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ResolveEntryWire {
    pub cloud_id: String,
    pub fingerprint: String,
    pub photo_id: Option<String>,
}

pub struct HttpDispatch {
    client: reqwest::Client,
    /// Node base URL WITHOUT `/api` (seeder convention), no trailing slash.
    base_url: String,
    device_token: String,
}

impl HttpDispatch {
    pub fn new(base_url: &str, device_token: &str) -> Result<Self, String> {
        // RFC-022: every request carries this build's identity. The
        // ingress crates sit outside the main workspace and cannot
        // inherit its version; hopnet-common is path-depped from the
        // same checkout, so ITS compile-time token names the snapshot
        // these bytes were built from.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            hopnet_common::compat::CLIENT_VERSION_HEADER,
            reqwest::header::HeaderValue::from(hopnet_common::version::common_version_code()),
        );
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .default_headers(headers)
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            device_token: device_token.to_string(),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/photos/client{path}", self.base_url)
    }

    fn unreachable(e: &reqwest::Error) -> bool {
        e.is_connect() || e.is_timeout()
    }

    fn transport_err(e: reqwest::Error) -> PhotosCoreError {
        if Self::unreachable(&e) {
            PhotosCoreError::Dispatch(format!("{UNREACHABLE_PREFIX}{e}"))
        } else {
            PhotosCoreError::Dispatch(format!("transport: {e}"))
        }
    }

    /// Convert a non-success response into the classified Dispatch error.
    async fn status_err(response: reqwest::Response) -> PhotosCoreError {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let body = body.trim();
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            // The node's shed gates own the retry (Retry-After) — park class.
            PhotosCoreError::Dispatch(format!("{UNREACHABLE_PREFIX}node shedding load (503)"))
        } else if status == reqwest::StatusCode::UPGRADE_REQUIRED {
            // RFC-022 gate refusal: retrying cannot help — park with the
            // versions named so the operator knows the remedy.
            let detail =
                serde_json::from_str::<hopnet_common::compat::UpgradeRequiredResponse>(body)
                    .map(|b| {
                        format!(
                            "node {} requires client >= {}",
                            hopnet_common::version::format_code(b.node_version),
                            hopnet_common::version::format_code(b.min_client),
                        )
                    })
                    .unwrap_or_else(|_| format!("unparsed 426 body: {body}"));
            PhotosCoreError::Dispatch(format!(
                "{UNREACHABLE_PREFIX}client too old ({detail}) — upgrade the ingress daemon"
            ))
        } else {
            PhotosCoreError::Dispatch(format!("http {status}: {body}"))
        }
    }

    /// Resolve pre-pass (`POST /resolve`): cloud_ids → fingerprints +
    /// committed ids + responsibility standing, scoped to one publish
    /// target (`library_id` None = personal partition). Not part of the
    /// `PhotoDispatch` trait — publish-flow only.
    pub async fn resolve_cloud_ids(
        &self,
        library_id: Option<&str>,
        cloud_ids: &[String],
    ) -> Result<ResolveResponseWire, PhotosCoreError> {
        let response = self
            .client
            .post(self.url("/resolve"))
            .bearer_auth(&self.device_token)
            .timeout(SMALL_TIMEOUT)
            .json(&serde_json::json!({ "cloud_ids": cloud_ids, "library_id": library_id }))
            .send()
            .await
            .map_err(Self::transport_err)?;
        if !response.status().is_success() {
            return Err(Self::status_err(response).await);
        }
        response
            .json::<ResolveResponseWire>()
            .await
            .map_err(|e| PhotosCoreError::Dispatch(format!("resolve response: {e}")))
    }

    /// Confirm probe (`GET /committed/{photo_id}`) for the idempotency
    /// contract. Not part of the `PhotoDispatch` trait — publish-flow only.
    pub async fn check_committed(&self, photo_id: &str) -> CommitProbe {
        let result = self
            .client
            .get(self.url(&format!("/committed/{photo_id}")))
            .bearer_auth(&self.device_token)
            .timeout(SMALL_TIMEOUT)
            .send()
            .await;
        match result {
            Ok(r) if r.status().is_success() => CommitProbe::Committed,
            Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => CommitProbe::NotCommitted,
            Ok(r) if r.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE => {
                CommitProbe::Unreachable("node shedding load (503)".into())
            }
            Ok(r) => CommitProbe::Failed(format!("confirm probe: http {}", r.status())),
            Err(e) if Self::unreachable(&e) => CommitProbe::Unreachable(e.to_string()),
            Err(e) => CommitProbe::Failed(format!("confirm probe: {e}")),
        }
    }
}

#[async_trait::async_trait]
impl PhotoDispatch for HttpDispatch {
    async fn submit_transaction(
        &self,
        tx_type: &str,
        payload_bytes: Vec<u8>,
    ) -> Result<(), PhotosCoreError> {
        let response = self
            .client
            .post(self.url("/transaction"))
            .bearer_auth(&self.device_token)
            .timeout(TX_TIMEOUT)
            .json(&serde_json::json!({ "tx_type": tx_type, "payload": payload_bytes }))
            .send()
            .await
            .map_err(Self::transport_err)?;
        if !response.status().is_success() {
            return Err(Self::status_err(response).await);
        }
        Ok(())
    }

    async fn fetch_photos_since(&self, _height: u64) -> Result<SyncBatch, PhotosCoreError> {
        // Publishing never syncs; the daemon's gallery is the node itself.
        Err(PhotosCoreError::Dispatch(
            "sync is unsupported on the ingress dispatch".into(),
        ))
    }

    async fn upload_data_block(
        &self,
        blob_id: hopnet_storage::BlobId,
        source: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        file_size: usize,
        per_blob_key: chacha20poly1305::Key,
    ) -> Result<UploadedDataBlock, PhotosCoreError> {
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(source));
        let response = self
            .client
            .post(self.url(&format!("/data-block/{blob_id}")))
            .bearer_auth(&self.device_token)
            .header("x-hopnet-blob-key", hex::encode(per_blob_key))
            .header("x-hopnet-file-size", file_size.to_string())
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await
            .map_err(Self::transport_err)?;
        if !response.status().is_success() {
            return Err(Self::status_err(response).await);
        }
        response
            .json::<UploadedDataBlock>()
            .await
            .map_err(|e| PhotosCoreError::Dispatch(format!("upload response: {e}")))
    }

    async fn fetch_library_members(
        &self,
        library_id: Option<CustomUUID>,
    ) -> Result<LibraryMembership, PhotosCoreError> {
        let mut request = self
            .client
            .get(self.url("/membership"))
            .bearer_auth(&self.device_token)
            .timeout(SMALL_TIMEOUT);
        if let Some(id) = &library_id {
            request = request.query(&[("library_id", id.to_string())]);
        }
        let response = request.send().await.map_err(Self::transport_err)?;
        if !response.status().is_success() {
            return Err(Self::status_err(response).await);
        }
        response
            .json::<LibraryMembership>()
            .await
            .map_err(|e| PhotosCoreError::Dispatch(format!("membership response: {e}")))
    }
}
