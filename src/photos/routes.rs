use axum::{
    Router,
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::derive_x25519_privkey_from_user;
use hopnet_storage::crypto::StaticRecipient;

use super::dispatch_local::Submitter;
use hopnet_photos_core::dispatch::PhotoDispatch;

/// Images + client-side thumbnails; video ingest (larger) is deferred.
const PHOTO_INGEST_BODY_LIMIT: usize = 64 * 1024 * 1024;

pub fn router<S: Clone + Send + Sync + 'static>(app_state: AppState) -> Router<S> {
    let reads_and_tx = Router::new()
        .route("/photos/sidecar/status", get(get_sidecar_status))
        .route("/photos/sidecar/enable", post(post_sidecar_enable))
        .route("/photos/sidecar/disable", post(post_sidecar_disable))
        .route("/photos/sidecar/reinit", post(post_sidecar_reinit))
        .route("/photos/gallery", get(get_gallery))
        .route("/photos/page", get(get_photo_page))
        .route("/photos/histogram", get(get_photo_histogram))
        .route("/photos/{id}", get(get_photo))
        .route("/photos/{id}/resource/{type}", get(get_photo_resource))
        .route("/photos/recently-deleted", get(get_recently_deleted))
        .route("/photos/transaction", post(post_transaction))
        .route("/photos/sync", get(get_sync_feed))
        // Ingress responsibility management (JWT only, deliberately: a
        // daemon can never claim for itself — see DEVICE_TX_FUNCTIONS).
        .route("/photos/ingress/claim", post(post_ingress_claim))
        .route(
            "/photos/ingress/responsibility",
            get(get_ingress_responsibility),
        );
    // Separate sub-router so the raised body limit applies only to ingest
    // (the rest of the surface keeps axum's 2 MB default).
    let ingest = Router::new()
        .route("/photos", post(post_photo_ingest))
        .layer(axum::extract::DefaultBodyLimit::max(PHOTO_INGEST_BODY_LIMIT));
    reads_and_tx.merge(ingest).with_state(app_state)
}

/// Blob uploads carry one resource per request; drive-sized ceiling
/// (hopnet-drive/src/http/files.rs) so multi-GB video originals stream through.
const DATA_BLOCK_BODY_LIMIT: usize = 5000 * 1_000_000;

/// Thin-client dispatch surface (photos.md §Upload Flow): the byte-transport
/// half of `PhotoDispatch` for clients that run the publisher locally and
/// reach the node over HTTP (the macOS photo-ingress daemon today). Mounted
/// at `/api/photos/client/*` under DEVICE-TOKEN auth (RFC-012) — the
/// middleware's bootstrapped session is what lets `submit_transaction` sign
/// and derives `uploaded_by`; callers never supply an identity.
pub fn device_router<S: Clone + Send + Sync + 'static>(app_state: AppState) -> Router<S> {
    let small = Router::new()
        .route("/membership", get(get_client_membership))
        .route("/transaction", post(post_client_transaction))
        .route("/committed/{photo_id}", get(get_client_committed))
        .route("/resolve", post(post_client_resolve));
    let upload = Router::new()
        .route("/data-block/{blob_id}", post(post_client_data_block))
        .layer(axum::extract::DefaultBodyLimit::max(DATA_BLOCK_BODY_LIMIT));
    small.merge(upload).with_state(app_state)
}

#[derive(Serialize)]
struct SidecarStatus {
    enabled: bool,
    cursor: Option<u64>,
    file_on_disk: bool,
}

async fn get_sidecar_status(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<Json<SidecarStatus>, StatusCode> {
    let file_on_disk = super::sidecar_db_path(uid).exists();

    let ph = &state.photos_host;
    let enabled = ph.is_enabled(uid).await;
    let cursor = if enabled {
        if let Some(db) = ph.get_db(uid).await {
            tokio::task::spawn_blocking(move || db.blocking_lock().cursor().ok())
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        } else {
            None
        }
    } else {
        None
    };
    Ok(Json(SidecarStatus {
        enabled,
        cursor,
        file_on_disk,
    }))
}

async fn post_sidecar_enable(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<StatusCode, (StatusCode, String)> {
    if state.photos_host.is_enabled(uid).await {
        return Ok(StatusCode::OK);
    }
    let session = state
        .get_session(uid)
        .await
        .map_err(|s| (s, "no session".into()))?;
    let x25519 = derive_x25519_privkey_from_user(&session.user_keys.private_key);
    let recipient = StaticRecipient(x25519);
    state
        .photos_host
        .enable(uid, recipient, state.db_pool.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(StatusCode::OK)
}

async fn post_sidecar_disable(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> StatusCode {
    state.photos_host.disable(uid).await;
    StatusCode::OK
}

async fn post_sidecar_reinit(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<StatusCode, (StatusCode, String)> {
    let session = state
        .get_session(uid)
        .await
        .map_err(|s| (s, "no session".into()))?;
    let x25519 = derive_x25519_privkey_from_user(&session.user_keys.private_key);
    let recipient = StaticRecipient(x25519);
    state
        .photos_host
        .reinit(uid, recipient, state.db_pool.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct Pagination {
    limit: Option<i64>,
    offset: Option<i64>,
}

fn clamp_page(q: &Pagination) -> (i64, i64) {
    (
        q.limit.unwrap_or(100).clamp(1, 200),
        q.offset.unwrap_or(0).max(0),
    )
}

async fn get_gallery(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<Pagination>,
) -> Result<impl IntoResponse, StatusCode> {
    let (limit, offset) = clamp_page(&q);
    let db = state
        .photos_host
        .get_db(uid)
        .await
        .ok_or(StatusCode::PRECONDITION_REQUIRED)?;
    let rows = tokio::task::spawn_blocking(move || db.blocking_lock().list_active(limit, offset))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows))
}

async fn get_photo(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Path(id): axum::extract::Path<hopnet_common::CustomUUID>,
) -> Result<Json<hopnet_photos_core::sidecar::PhotoRow>, StatusCode> {
    let db = state
        .photos_host
        .get_db(uid)
        .await
        .ok_or(StatusCode::PRECONDITION_REQUIRED)?;
    tokio::task::spawn_blocking(move || db.blocking_lock().get_photo(&id))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}

async fn get_recently_deleted(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<Pagination>,
) -> Result<impl IntoResponse, StatusCode> {
    let (limit, offset) = clamp_page(&q);
    let db = state
        .photos_host
        .get_db(uid)
        .await
        .ok_or(StatusCode::PRECONDITION_REQUIRED)?;
    let rows = tokio::task::spawn_blocking(move || {
        db.blocking_lock().list_recently_deleted(limit, offset)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows))
}

#[derive(Serialize)]
struct IngestResponse {
    photo_id: String,
    operation_id: String,
}

fn map_publish_error(e: hopnet_photos_core::PhotosCoreError) -> (StatusCode, String) {
    use hopnet_photos_core::PhotosCoreError as E;
    match &e {
        E::InvalidAsset(_) | E::InvalidPublishRequest(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
        }
        E::PartialPublish {
            photo_id,
            uploaded_blob_ids,
            ..
        } => {
            tracing::error!(
                %photo_id,
                ?uploaded_blob_ids,
                "partial photo publish; uploaded blobs are reconciliation candidates"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Manual photo ingest (multipart): first part `asset` = JSON PhotoAsset
/// descriptor, then one part per declared resource named by its ResourceKind
/// ("original", "thumbnail_small", ...). The publisher validates the
/// name<->descriptor bijection and exact byte lengths, so mismatches surface
/// as 422s.
///
/// Multipart parts arrive sequentially and each must be drained before
/// next_field(), but publish_photo_add wants all byte sources upfront — so
/// each resource part is buffered in memory (bounded by
/// PHOTO_INGEST_BODY_LIMIT). Streaming multi-resource ingest is future work;
/// it matters once video resources land.
///
/// Idempotency: a fresh UUIDv7 photo_id is minted per request, so a
/// duplicate POST creates a duplicate photo. asset.source (SourceIdentity)
/// never reaches consensus — the publisher encrypts metadata only — so
/// server-side dedup would need new persistence (future work).
///
/// No write gating: the photos surface is ungated today (drive's write_gate
/// needs HostCapabilities, which these routes don't hold). Align when photos
/// moves to projection mounts.
async fn post_photo_ingest(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    mut multipart: axum::extract::Multipart,
) -> Result<(StatusCode, Json<IngestResponse>), (StatusCode, String)> {
    use hopnet_photos_core::asset::{PhotoAsset, ResourceKind};
    use hopnet_photos_core::publisher::{ByteSource, PublishRequest, publish_photo_add};

    let first = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart: {e}")))?
        .ok_or((StatusCode::BAD_REQUEST, "empty multipart body".to_string()))?;
    if first.name() != Some("asset") {
        return Err((
            StatusCode::BAD_REQUEST,
            "first part must be `asset`".to_string(),
        ));
    }
    let asset: PhotoAsset = serde_json::from_str(
        &first
            .text()
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("asset part: {e}")))?,
    )
    .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, format!("asset descriptor: {e}")))?;

    let mut byte_sources: Vec<(ResourceKind, ByteSource)> = Vec::new();
    while let Some(part) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart: {e}")))?
    {
        let name = part
            .name()
            .ok_or((
                StatusCode::UNPROCESSABLE_ENTITY,
                "unnamed resource part".to_string(),
            ))?
            .to_string();
        let kind = ResourceKind::from_name(&name).ok_or((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("unknown resource kind `{name}`"),
        ))?;
        let bytes = part
            .bytes()
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("resource `{name}`: {e}")))?;
        byte_sources.push((
            kind,
            ByteSource::Stream(Box::new(std::io::Cursor::new(bytes))),
        ));
    }

    let photo_id = hopnet_common::CustomUUID::new(None);
    let sub = Submitter::new(std::sync::Arc::new(state), uid);
    let outcome = publish_photo_add(
        &sub,
        PublishRequest {
            asset: &asset,
            photo_id,
            library_id: None, // personal library only until Phase 3
            byte_sources,
            // Browser/desktop uploads carry no PhotoKit identity; import-time
            // fingerprints for non-PhotoKit paths are a recorded deferral.
            cloud_fingerprint: None,
        },
    )
    .await
    .map_err(map_publish_error)?;

    Ok((
        StatusCode::CREATED,
        Json(IngestResponse {
            photo_id: outcome.photo_id.to_string(),
            operation_id: outcome.operation_id.to_string(),
        }),
    ))
}

#[derive(Deserialize)]
struct TransactionBody {
    tx_type: String,
    payload: Vec<u8>,
}

/// Shared by the JWT route and the device-token thin-client route — the
/// contract (USER_TX_FUNCTIONS gate, block-until-decided submit) is identical.
async fn submit_photos_transaction(
    state: AppState,
    uid: i32,
    body: TransactionBody,
) -> Result<StatusCode, (StatusCode, String)> {
    if !hopnet_photos::handlers::USER_TX_FUNCTIONS.contains(&body.tx_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("unsupported photos tx_type: {}", body.tx_type),
        ));
    }
    let sub = Submitter::new(std::sync::Arc::new(state), uid);
    sub.submit_transaction(&body.tx_type, body.payload)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::OK)
}

async fn post_transaction(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    Json(body): Json<TransactionBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    submit_photos_transaction(state, uid, body).await
}

#[derive(Deserialize)]
struct IngressClaimBody {
    device_id: hopnet_common::CustomUUID,
}

/// Claim (or transfer) ingress responsibility to one of the caller's
/// devices. JWT-authenticated and JSON-bodied on purpose: the GUI enable
/// flow and a plain curl both need it without bincode-encoding a payload
/// client-side — the route builds and submits the consensus tx itself.
/// The handler re-validates device ownership deterministically; the check
/// here just gives the caller a friendly 4xx instead of a rejected tx.
async fn post_ingress_claim(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    Json(body): Json<IngressClaimBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    let owned = {
        let db = state
            .db_pool
            .get()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        crate::db::devices::get_device_by_id(&db, &body.device_id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}")))?
            .is_some_and(|d| d.user_id == uid)
    };
    if !owned {
        return Err((
            StatusCode::NOT_FOUND,
            "device not found for this user".into(),
        ));
    }

    let payload = hopnet_photos::envelopes::PhotoIngressClaimPayload {
        device_id: body.device_id,
        operation_id: hopnet_common::CustomUUID::new(None),
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    submit_photos_transaction(
        state,
        uid,
        TransactionBody {
            tx_type: "photo_ingress_claim".into(),
            payload: encoded,
        },
    )
    .await
}

#[derive(Serialize)]
struct IngressResponsibilityResponse {
    device_id: Option<String>,
}

async fn get_ingress_responsibility(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<Json<IngressResponsibilityResponse>, (StatusCode, String)> {
    let pool = state.db_pool.clone();
    let holder =
        tokio::task::spawn_blocking(move || super::query::read_ingress_responsibility(&pool, uid))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(IngressResponsibilityResponse {
        device_id: holder.map(|d| d.to_string()),
    }))
}

#[derive(Deserialize)]
struct SyncQuery {
    since: Option<u64>,
}

async fn get_sync_feed(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<SyncQuery>,
) -> Result<Json<hopnet_photos_core::dispatch::SyncBatch>, (StatusCode, String)> {
    let since = q.since.unwrap_or(0);
    let pool = state.db_pool.clone();
    tokio::task::spawn_blocking(move || super::query::read_photo_changes(&pool, uid, since))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))
}

// ---------------------------------------------------------------------------
// Thin-client dispatch handlers (`/api/photos/client/*`, device-token auth).
// Wire mirrors of the PhotoDispatch upload pipe for clients that run the
// publisher locally (the macOS photo-ingress daemon) and reach the node over
// HTTP with an RFC-012 device token.
// ---------------------------------------------------------------------------

const BLOB_KEY_HEADER: &str = "x-hopnet-blob-key";
const FILE_SIZE_HEADER: &str = "x-hopnet-file-size";

fn parse_blob_key_header(
    headers: &axum::http::HeaderMap,
) -> Result<chacha20poly1305::Key, String> {
    let raw = headers
        .get(BLOB_KEY_HEADER)
        .ok_or_else(|| format!("missing {BLOB_KEY_HEADER} header"))?
        .to_str()
        .map_err(|_| format!("{BLOB_KEY_HEADER} is not valid ASCII"))?;
    let bytes = hex::decode(raw).map_err(|_| format!("{BLOB_KEY_HEADER} is not valid hex"))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("{BLOB_KEY_HEADER} must be 32 bytes (64 hex chars)"))?;
    Ok(chacha20poly1305::Key::from(arr))
}

/// The declared plaintext length travels as a header because streaming
/// clients send chunked transfer encoding (no Content-Length).
fn parse_file_size_header(headers: &axum::http::HeaderMap) -> Result<usize, String> {
    let size: u64 = headers
        .get(FILE_SIZE_HEADER)
        .ok_or_else(|| format!("missing {FILE_SIZE_HEADER} header"))?
        .to_str()
        .map_err(|_| format!("{FILE_SIZE_HEADER} is not valid ASCII"))?
        .parse()
        .map_err(|_| format!("{FILE_SIZE_HEADER} is not a decimal byte count"))?;
    if size == 0 {
        return Err(format!("{FILE_SIZE_HEADER} must be > 0"));
    }
    usize::try_from(size).map_err(|_| format!("{FILE_SIZE_HEADER} exceeds platform limits"))
}

/// Typed payload inside the io::Error `ExactBody` raises, recovered by
/// downcast after the put fails (same pattern as the publisher's ExactLen).
#[derive(Debug)]
struct BodyLenError {
    expected: u64,
    /// `Some(actual)` = body ended early; `None` = body exceeds the header.
    actual: Option<u64>,
}

impl std::fmt::Display for BodyLenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.actual {
            Some(actual) => write!(f, "body ended at {actual} of {} bytes", self.expected),
            None => write!(f, "body exceeds declared {} bytes", self.expected),
        }
    }
}

impl std::error::Error for BodyLenError {}

/// Enforces the declared byte length on the streamed body INLINE. api::put
/// reads to EOF and never cross-checks `file_size` — without this wrapper a
/// network-truncated body would be encoded short and returned as a valid-
/// looking data block.
struct ExactBody<R> {
    inner: R,
    expected: u64,
    consumed: u64,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ExactBody<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();

        if this.consumed == this.expected {
            // Probe one extra read: clean EOF passes, any byte is too long.
            let mut probe_storage = [0u8; 1];
            let mut probe = tokio::io::ReadBuf::new(&mut probe_storage);
            std::task::ready!(std::pin::Pin::new(&mut this.inner).poll_read(cx, &mut probe))?;
            return std::task::Poll::Ready(if probe.filled().is_empty() {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    BodyLenError {
                        expected: this.expected,
                        actual: None,
                    },
                ))
            });
        }

        let remaining = usize::try_from(this.expected - this.consumed)
            .unwrap_or(usize::MAX)
            .min(buf.remaining());
        let mut sub = buf.take(remaining);
        std::task::ready!(std::pin::Pin::new(&mut this.inner).poll_read(cx, &mut sub))?;
        let n = sub.filled().len();
        if n == 0 {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                BodyLenError {
                    expected: this.expected,
                    actual: Some(this.consumed),
                },
            )));
        }
        // SAFETY: `sub` borrows `buf`'s unfilled region, so its first `n`
        // bytes are now initialized (standard tokio `Take` pattern).
        unsafe { buf.assume_init(n) };
        buf.advance(n);
        this.consumed += n as u64;
        std::task::Poll::Ready(Ok(()))
    }
}

/// Upload failures split three ways: a length mismatch our wrapper caught is
/// the client's declaration being wrong (422), any other read error is the
/// client's stream breaking (400), and everything else is the substrate (500).
fn map_upload_error(e: hopnet_photos_core::PhotosCoreError) -> (StatusCode, String) {
    if let hopnet_photos_core::PhotosCoreError::Storage(hopnet_storage::StorageError::Read(
        ref io_err,
    )) = e
    {
        if let Some(b) = io_err.get_ref().and_then(|b| b.downcast_ref::<BodyLenError>()) {
            return (StatusCode::UNPROCESSABLE_ENTITY, b.to_string());
        }
        return (StatusCode::BAD_REQUEST, format!("body read: {io_err}"));
    }
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

#[derive(Deserialize)]
struct MembershipQuery {
    library_id: Option<hopnet_common::CustomUUID>,
}

/// Recipients of a publish; `uploaded_by` derives from the authenticated
/// device's user, never from the caller.
async fn get_client_membership(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<MembershipQuery>,
) -> Result<Json<hopnet_photos_core::dispatch::LibraryMembership>, (StatusCode, String)> {
    let sub = Submitter::new(std::sync::Arc::new(state), uid);
    sub.fetch_library_members(q.library_id)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// Streaming wire mirror of `PhotoDispatch::upload_data_block`: raw
/// octet-stream body of exactly `X-Hopnet-File-Size` plaintext bytes, plus
/// the client-minted per-blob key in `X-Hopnet-Blob-Key` (64 hex chars).
/// Encryption is node-side by design (photos.md §Upload Flow): key custody
/// stays client-side while the encrypt/RS pipeline stays in one impl.
///
/// The blob_id is client-minted. A colliding or replayed id only produces
/// orphaned fragments (the substrate's orphan sweep owns them) — a blob
/// becomes reachable only via a committed photo_add manifest, and duplicate
/// photo ids are rejected by the proposer preflight.
async fn post_client_data_block(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Path(blob_id): axum::extract::Path<hopnet_common::CustomUUID>,
    headers: axum::http::HeaderMap,
    body: axum::body::Body,
) -> Result<
    (
        StatusCode,
        Json<hopnet_photos_core::dispatch::UploadedDataBlock>,
    ),
    (StatusCode, String),
> {
    use tokio_stream::StreamExt;

    let key = parse_blob_key_header(&headers).map_err(|m| (StatusCode::BAD_REQUEST, m))?;
    let file_size = parse_file_size_header(&headers).map_err(|m| (StatusCode::BAD_REQUEST, m))?;

    let reader = tokio_util::io::StreamReader::new(
        body.into_data_stream()
            .map(|r| r.map_err(std::io::Error::other)),
    );
    let source = ExactBody {
        inner: reader,
        expected: file_size as u64,
        consumed: 0,
    };

    let sub = Submitter::new(std::sync::Arc::new(state), uid);
    let uploaded = sub
        .upload_data_block(blob_id, Box::new(source), file_size, key)
        .await
        .map_err(map_upload_error)?;
    Ok((StatusCode::CREATED, Json(uploaded)))
}

/// Device-route mutations are double-gated beyond the shared contract:
/// (a) tx_type must be device-submittable (no self-claims), (b) the authed
/// device must hold ingress responsibility. The 403 body is machine-
/// parseable (`ingress_not_responsible:{other|unclaimed}`) — a well-behaved
/// daemon parks on the resolve pre-pass and never hits this; it exists as
/// the admission backstop, with the fingerprint UNIQUE pair behind it.
async fn post_client_transaction(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    Extension(device): Extension<crate::devices::auth::AuthedDevice>,
    Json(body): Json<TransactionBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    if !hopnet_photos::handlers::DEVICE_TX_FUNCTIONS.contains(&body.tx_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("tx_type not device-submittable: {}", body.tx_type),
        ));
    }

    let pool = state.db_pool.clone();
    let holder =
        tokio::task::spawn_blocking(move || super::query::read_ingress_responsibility(&pool, uid))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    match holder {
        Some(h) if h == device.0 => {}
        Some(_) => {
            return Err((
                StatusCode::FORBIDDEN,
                "ingress_not_responsible:other".into(),
            ));
        }
        None => {
            return Err((
                StatusCode::FORBIDDEN,
                "ingress_not_responsible:unclaimed".into(),
            ));
        }
    }

    submit_photos_transaction(state, uid, body).await
}

/// Resolve batch size ceiling. Must stay >= the ingress publisher's claim
/// batch (PublishConfig::default().batch — a couple dozen) so one publish
/// pass never needs a chunked resolve.
const RESOLVE_MAX_IDS: usize = 500;

#[derive(Deserialize)]
struct ResolveBody {
    cloud_ids: Vec<String>,
}

#[derive(Serialize)]
struct ResolveEntryResponse {
    cloud_id: String,
    fingerprint: String,
    photo_id: Option<String>,
}

#[derive(Serialize)]
struct ResolveResponse {
    /// The caller's standing: "holder" | "other" | "unclaimed".
    responsibility: &'static str,
    entries: Vec<ResolveEntryResponse>,
}

/// Pre-publish identity resolution for the thin client. The daemon holds
/// no user key material, so the fingerprint HMAC is computed here from the
/// device-auth session's user key; the response pairs each fingerprint
/// with any already-committed photo_id (→ the daemon adopts instead of
/// re-uploading) plus the caller's responsibility standing. Read-only —
/// deliberately NOT gated on responsibility, so non-holder devices can
/// still adopt (that's what makes handoff a cheap sweep).
async fn post_client_resolve(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    Extension(device): Extension<crate::devices::auth::AuthedDevice>,
    Json(body): Json<ResolveBody>,
) -> Result<Json<ResolveResponse>, (StatusCode, String)> {
    if body.cloud_ids.len() > RESOLVE_MAX_IDS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("too many cloud_ids (max {RESOLVE_MAX_IDS})"),
        ));
    }

    let session = state
        .get_session(uid)
        .await
        .map_err(|s| (s, "no session".into()))?;
    let fp_key = crate::auth::derive_photo_fingerprint_key(&session.user_keys.private_key);

    let pool = state.db_pool.clone();
    tokio::task::spawn_blocking(move || {
        let holder = super::query::read_ingress_responsibility(&pool, uid)?;
        let responsibility = match &holder {
            Some(h) if *h == device.0 => "holder",
            Some(_) => "other",
            None => "unclaimed",
        };

        let mut entries = Vec::with_capacity(body.cloud_ids.len());
        for cloud_id in body.cloud_ids {
            let fingerprint = hex::encode(blake3::keyed_hash(&fp_key, cloud_id.as_bytes()).as_bytes());
            let photo_id = super::query::read_photo_by_fingerprint(&pool, uid, &fingerprint)?;
            entries.push(ResolveEntryResponse {
                cloud_id,
                fingerprint,
                photo_id: photo_id.map(|id| id.to_string()),
            });
        }
        Ok(ResolveResponse {
            responsibility,
            entries,
        })
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map(Json)
    .map_err(|e: String| (StatusCode::INTERNAL_SERVER_ERROR, e))
}

#[derive(Serialize)]
struct CommittedResponse {
    photo_id: String,
    uploaded_by: i32,
}

/// Confirm probe for the publisher's idempotency contract: after an
/// ambiguous submit failure the client asks here before retrying. 404 ⇒ not
/// committed for this user, re-submitting the same photo_id is safe; 200 ⇒ a
/// previous attempt committed, mark published and never re-submit.
async fn get_client_committed(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Path(photo_id): axum::extract::Path<hopnet_common::CustomUUID>,
) -> Result<Json<CommittedResponse>, StatusCode> {
    let pool = state.db_pool.clone();
    let id = photo_id.clone();
    let uploaded_by =
        tokio::task::spawn_blocking(move || super::query::read_photo_committed(&pool, uid, &id))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(CommittedResponse {
        photo_id: photo_id.to_string(),
        uploaded_by,
    }))
}

#[derive(Deserialize)]
struct PageQuery {
    /// base64url(no-pad) of `sort_ms:photo_id`; photo_id may be empty for a
    /// month-boundary anchor.
    cursor: Option<String>,
    /// "older" (default) | "newer". Newer requires a cursor.
    dir: Option<String>,
    limit: Option<i64>,
    video: Option<bool>,
    live: Option<bool>,
    raw: Option<bool>,
}

#[derive(Serialize)]
struct PhotoPage {
    items: Vec<hopnet_photos_core::sidecar::PhotoPageItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

fn encode_cursor(sort_ms: i64, photo_id: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{sort_ms}:{photo_id}"))
}

fn decode_cursor(cursor: &str) -> Option<(i64, String)> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let (ms, photo_id) = text.split_once(':')?;
    Some((ms.parse().ok()?, photo_id.to_string()))
}

async fn get_photo_page(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<PageQuery>,
) -> Result<Json<PhotoPage>, StatusCode> {
    use hopnet_photos_core::sidecar::{MediaFilter, PageDir};

    let cursor = match &q.cursor {
        Some(raw) => Some(decode_cursor(raw).ok_or(StatusCode::BAD_REQUEST)?),
        None => None,
    };
    let dir = match q.dir.as_deref() {
        None | Some("older") => PageDir::Older,
        Some("newer") if cursor.is_some() => PageDir::Newer,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let limit = q.limit.unwrap_or(100).clamp(1, 200);
    let filter = MediaFilter {
        video: q.video,
        live: q.live,
        raw: q.raw,
    };

    let db = state
        .photos_host
        .get_db(uid)
        .await
        .ok_or(StatusCode::PRECONDITION_REQUIRED)?;
    let (items, has_more) = tokio::task::spawn_blocking(move || {
        db.blocking_lock().list_page(cursor, dir, &filter, limit)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // The client only checks presence, but hand back a real edge cursor.
    let next_cursor = has_more
        .then(|| items.last())
        .flatten()
        .map(|last| encode_cursor(last.sort_ms, &last.row.photo_id.to_string()));
    Ok(Json(PhotoPage { items, next_cursor }))
}

async fn get_photo_histogram(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Query(q): axum::extract::Query<PageQuery>,
) -> Result<Json<Vec<hopnet_photos_core::sidecar::MonthBucket>>, StatusCode> {
    use hopnet_photos_core::sidecar::MediaFilter;
    let filter = MediaFilter {
        video: q.video,
        live: q.live,
        raw: q.raw,
    };
    let db = state
        .photos_host
        .get_db(uid)
        .await
        .ok_or(StatusCode::PRECONDITION_REQUIRED)?;
    tokio::task::spawn_blocking(move || db.blocking_lock().month_histogram(&filter))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        .map(Json)
}

/// Blob content is immutable per data_block_id, hence `immutable`. A content
/// edit swaps the blob UNDER the same (photo_id, resource_type) URL, so
/// clients must key caches by data_block_id (the gallery payload's
/// `resources` field carries it); the ETag is the revalidation fallback.
const RESOURCE_CACHE_CONTROL: &str = "private, immutable, max-age=31536000";

/// `bytes=START-END` / `bytes=START-` only. Multi-range and suffix ranges
/// (`bytes=-N`) are treated as no-range (full response) — same semantics as
/// the drive's download route.
fn parse_range(headers: &axum::http::HeaderMap) -> Option<(u64, Option<u64>)> {
    let value = headers.get(axum::http::header::RANGE)?.to_str().ok()?;
    let spec = value.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let mut parts = spec.splitn(2, '-');
    let start: u64 = parts.next()?.parse().ok()?;
    let end = match parts.next() {
        Some("") | None => None,
        Some(e) => Some(e.parse().ok()?),
    };
    Some((start, end))
}

fn if_none_match_matches(headers: &axum::http::HeaderMap, etag: &str) -> bool {
    let Some(value) = headers
        .get(axum::http::header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    value.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == etag
    })
}

/// Serves decrypted resource bytes. Content-Type is application/octet-stream
/// — no MIME is stored anywhere; the client assigns the type when building
/// its object URL (FilePreview precedent). Range reads skip the whole-blob
/// keyed integrity verify (per-fragment blake3 still verifies every read);
/// on full reads the keyed verify only completes after the last chunk, so a
/// mismatch or missing fragment truncates an already-200 response — same
/// behavior as the drive route.
async fn get_photo_resource(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    axum::extract::Path((photo_id, kind_name)): axum::extract::Path<(
        hopnet_common::CustomUUID,
        String,
    )>,
    headers: axum::http::HeaderMap,
) -> Result<axum::response::Response<axum::body::Body>, StatusCode> {
    use axum::body::Body;
    use axum::http::header;

    let kind =
        hopnet_photos_core::asset::ResourceKind::from_name(&kind_name).ok_or(StatusCode::BAD_REQUEST)?;
    let requested_range = parse_range(&headers);

    let pool = state.db_pool.clone();
    let grant_photo_id = photo_id.clone();
    let grant = tokio::task::spawn_blocking(move || {
        super::query::read_resource_grant(&pool, uid, &grant_photo_id, kind)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(StatusCode::from)?;

    // Revalidation short-circuit: after authz (a 304 to an unauthorized
    // caller would leak existence), before key unwrap and storage work.
    let etag = format!("\"{}\"", grant.data_block_id);
    if if_none_match_matches(&headers, &etag) {
        return axum::response::Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, &etag)
            .header(header::CACHE_CONTROL, RESOURCE_CACHE_CONTROL)
            .body(Body::empty())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
    }

    let session = state.get_session(uid).await?;
    let x25519 = derive_x25519_privkey_from_user(&session.user_keys.private_key);
    let per_blob_key =
        hopnet_storage::crypto::unwrap_blob_key(&grant.access, &StaticRecipient(x25519))
            .map_err(|_| StatusCode::FORBIDDEN)?;

    let file_size = grant.manifest.file_size;
    let resolved = match requested_range {
        Some((start, _)) if start >= file_size => {
            return axum::response::Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{file_size}"))
                .body(Body::empty())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
        }
        Some((start, end_opt)) => {
            let end = end_opt.unwrap_or(file_size - 1).min(file_size - 1);
            Some((start, end))
        }
        None => None,
    };

    let stream = hopnet_storage::api::get(
        Some(crate::storage_host::substrate_host::get_net(&state)),
        state.fragments_dir.clone(),
        grant.manifest,
        Some(per_blob_key),
        resolved,
    );

    let builder = axum::response::Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, &etag)
        .header(header::CACHE_CONTROL, RESOURCE_CACHE_CONTROL);
    let builder = match resolved {
        Some((start, end)) => builder
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_LENGTH, end - start + 1)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{file_size}"),
            ),
        None => builder
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, file_size),
    };
    builder
        .body(Body::from_stream(stream))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    fn headers_with(name: axum::http::HeaderName, value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(name, value.parse().unwrap());
        headers
    }

    // Should: parse single start-end and open-ended byte ranges.
    #[test]
    fn parse_range_accepts_single_ranges() {
        let h = headers_with(axum::http::header::RANGE, "bytes=0-99");
        assert_eq!(parse_range(&h), Some((0, Some(99))));
        let h = headers_with(axum::http::header::RANGE, "bytes=100-");
        assert_eq!(parse_range(&h), Some((100, None)));
    }

    // Impact: malformed or unsupported Range headers must degrade to a full
    // 200 response, never to wrong bytes or an error.
    // Should not: parse multi-range, suffix-range, or junk specs.
    #[test]
    fn parse_range_degrades_unsupported_specs_to_full_response() {
        for value in ["bytes=0-99,200-299", "bytes=-100", "bytes=abc-", "items=0-99", ""] {
            let h = headers_with(axum::http::header::RANGE, value);
            assert_eq!(parse_range(&h), None, "spec {value:?} must degrade");
        }
        assert_eq!(parse_range(&HeaderMap::new()), None);
    }

    // Should: match the exact ETag, an ETag inside a list, a weak-prefixed
    // ETag, and the `*` wildcard.
    #[test]
    fn if_none_match_matches_per_rfc9110() {
        let etag = "\"abc\"";
        for value in ["\"abc\"", "\"zzz\", \"abc\"", "W/\"abc\"", "*"] {
            let h = headers_with(axum::http::header::IF_NONE_MATCH, value);
            assert!(if_none_match_matches(&h, etag), "{value:?} must match");
        }
    }

    // Should not: report a match for a different ETag or an absent header.
    #[test]
    fn if_none_match_misses_on_mismatch() {
        let h = headers_with(axum::http::header::IF_NONE_MATCH, "\"other\"");
        assert!(!if_none_match_matches(&h, "\"abc\""));
        assert!(!if_none_match_matches(&HeaderMap::new(), "\"abc\""));
    }

    // Should: round-trip keyset cursors, including the empty-photo_id
    // month-boundary form.
    #[test]
    fn cursor_round_trips() {
        let encoded = encode_cursor(1748736000000, "0198f3a2-aaaa-bbbb-cccc-dddddddddddd");
        assert_eq!(
            decode_cursor(&encoded),
            Some((
                1748736000000,
                "0198f3a2-aaaa-bbbb-cccc-dddddddddddd".to_string()
            ))
        );
        let boundary = encode_cursor(1748736000000, "");
        assert_eq!(decode_cursor(&boundary), Some((1748736000000, String::new())));
        let negative = encode_cursor(-5, "id");
        assert_eq!(decode_cursor(&negative), Some((-5, "id".to_string())));
    }

    // Should not: decode malformed cursors — bad base64, missing separator,
    // or a non-numeric sort key.
    #[test]
    fn cursor_rejects_malformed() {
        use base64::Engine;
        let b64 = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s);
        assert_eq!(decode_cursor("!!!not-base64!!!"), None);
        assert_eq!(decode_cursor(&b64("no-separator")), None);
        assert_eq!(decode_cursor(&b64("abc:id")), None);
        assert_eq!(decode_cursor(&b64(":id")), None);
        assert_eq!(decode_cursor(""), None);
    }

    // Impact: a client sending a bad descriptor must get an actionable 4xx,
    // never a retryable-looking 500.
    // Should: map InvalidAsset and InvalidPublishRequest to 422 carrying the
    // message; Dispatch and PartialPublish to 500.
    #[test]
    fn publish_errors_map_to_client_or_server_status() {
        use hopnet_photos_core::{PhotosCoreError, PublishValidationError};

        let (status, message) = map_publish_error(PhotosCoreError::InvalidAsset(
            hopnet_photos_core::AssetValidationError::NoResources,
        ));
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(message.contains("no resources"), "message: {message}");

        let (status, _) = map_publish_error(PhotosCoreError::InvalidPublishRequest(
            PublishValidationError::NoRecipients,
        ));
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        let (status, _) =
            map_publish_error(PhotosCoreError::Dispatch("queue unavailable".into()));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

        let (status, _) = map_publish_error(PhotosCoreError::PartialPublish {
            photo_id: hopnet_common::CustomUUID::retention_cutoff(1),
            uploaded_blob_ids: vec![hopnet_common::CustomUUID::retention_cutoff(2)],
            source: Box::new(PhotosCoreError::Dispatch("submit failed".into())),
        });
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Should: serialize photo_id and operation_id as strings — the exact
    // contract the seeder parses.
    #[test]
    fn ingest_response_serializes_ids() {
        let response = IngestResponse {
            photo_id: "0198aaaa-bbbb-7ccc-8ddd-eeeeffff0000".into(),
            operation_id: "0198aaaa-bbbb-7ccc-8ddd-eeeeffff0001".into(),
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json["photo_id"],
            serde_json::json!("0198aaaa-bbbb-7ccc-8ddd-eeeeffff0000")
        );
        assert_eq!(
            json["operation_id"],
            serde_json::json!("0198aaaa-bbbb-7ccc-8ddd-eeeeffff0001")
        );
    }

    fn key_headers(value: &str) -> HeaderMap {
        headers_with(
            axum::http::HeaderName::from_static(BLOB_KEY_HEADER),
            value,
        )
    }

    // Impact: a mis-parsed key would encrypt the blob under the wrong key —
    // the manifest commits fine but every later read fails integrity.
    // Should: accept exactly 64 hex chars and recover the 32 key bytes.
    #[test]
    fn blob_key_header_parses_64_hex_chars() {
        let hex_key = "ab".repeat(32);
        let key = parse_blob_key_header(&key_headers(&hex_key)).unwrap();
        assert_eq!(key.as_slice(), &[0xABu8; 32]);
    }

    // Should not: accept a missing, short, overlong, or non-hex key header.
    #[test]
    fn blob_key_header_rejects_malformed() {
        assert!(parse_blob_key_header(&HeaderMap::new()).is_err());
        for value in [
            &"ab".repeat(31),          // 62 chars: too short
            &"ab".repeat(33),          // 66 chars: too long
            &format!("{}q", "ab".repeat(31)), // 63 chars: odd length
            &format!("zz{}", "ab".repeat(31)), // non-hex
        ] {
            assert!(
                parse_blob_key_header(&key_headers(value)).is_err(),
                "value {value:?} must be rejected"
            );
        }
    }

    // Should: parse a decimal byte count from the file-size header.
    // Should not: accept zero, non-numeric, or missing sizes.
    #[test]
    fn file_size_header_requires_positive_decimal() {
        let name = axum::http::HeaderName::from_static(FILE_SIZE_HEADER);
        let h = headers_with(name.clone(), "1048576");
        assert_eq!(parse_file_size_header(&h), Ok(1_048_576));

        assert!(parse_file_size_header(&HeaderMap::new()).is_err());
        for value in ["0", "-1", "abc", "1.5", ""] {
            let h = headers_with(name.clone(), value);
            assert!(
                parse_file_size_header(&h).is_err(),
                "value {value:?} must be rejected"
            );
        }
    }

    // Should: pass through a body of exactly the declared length.
    #[tokio::test]
    async fn exact_body_passes_exact_length() {
        use tokio::io::AsyncReadExt;
        let mut reader = ExactBody {
            inner: std::io::Cursor::new(vec![7u8; 1000]),
            expected: 1000,
            consumed: 0,
        };
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, vec![7u8; 1000]);
    }

    // Impact: api::put reads to EOF without cross-checking file_size, so a
    // network-truncated body would otherwise be encoded short and returned
    // as a valid-looking data block whose manifest consensus then commits.
    // Should: error mid-read when the body ends before the declared length.
    #[tokio::test]
    async fn exact_body_rejects_truncated_body() {
        use tokio::io::AsyncReadExt;
        let mut reader = ExactBody {
            inner: std::io::Cursor::new(vec![7u8; 400]),
            expected: 1000,
            consumed: 0,
        };
        let mut out = Vec::new();
        let err = reader.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        let inner = err.get_ref().unwrap().downcast_ref::<BodyLenError>().unwrap();
        assert_eq!(inner.expected, 1000);
        assert_eq!(inner.actual, Some(400));
    }

    // Should not: accept a body longer than the declared length.
    #[tokio::test]
    async fn exact_body_rejects_overlong_body() {
        use tokio::io::AsyncReadExt;
        let mut reader = ExactBody {
            inner: std::io::Cursor::new(vec![7u8; 1001]),
            expected: 1000,
            consumed: 0,
        };
        let mut out = Vec::new();
        let err = reader.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let inner = err.get_ref().unwrap().downcast_ref::<BodyLenError>().unwrap();
        assert_eq!(inner.actual, None);
    }

    // Impact: the daemon keys its retry classification on these statuses — a
    // length mismatch surfacing as 500 would look retryable forever.
    // Should: map a declared-length violation to 422, other body-read
    // failures to 400, and substrate errors to 500.
    #[test]
    fn upload_errors_split_client_and_server_status() {
        let len_err = hopnet_photos_core::PhotosCoreError::Storage(
            hopnet_storage::StorageError::Read(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                BodyLenError {
                    expected: 10,
                    actual: Some(4),
                },
            )),
        );
        let (status, message) = map_upload_error(len_err);
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(message.contains("4 of 10"), "message: {message}");

        let broken_stream = hopnet_photos_core::PhotosCoreError::Storage(
            hopnet_storage::StorageError::Read(std::io::Error::other("connection reset")),
        );
        let (status, _) = map_upload_error(broken_stream);
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = map_upload_error(hopnet_photos_core::PhotosCoreError::Dispatch(
            "engine".into(),
        ));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }
}
