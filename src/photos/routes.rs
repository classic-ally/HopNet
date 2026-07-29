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
        .route("/photos/sync", get(get_sync_feed));
    // Separate sub-router so the raised body limit applies only to ingest
    // (the rest of the surface keeps axum's 2 MB default).
    let ingest = Router::new()
        .route("/photos", post(post_photo_ingest))
        .layer(axum::extract::DefaultBodyLimit::max(PHOTO_INGEST_BODY_LIMIT));
    reads_and_tx.merge(ingest).with_state(app_state)
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

async fn post_transaction(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
    Json(body): Json<TransactionBody>,
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
}
