//! HTTP surface: metadata JSON + raw resource passthrough, all behind the
//! OIDC session + per-library group authorization.

use std::sync::Arc;

use axum::Router;
use axum::extract::{FromRef, Path, Query, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use tower::ServiceExt as _;
use tower_http::services::ServeFile;

use crate::auth::{self, AccessRules, AuthContext, AuthUser, SessionUser, can_access};
use crate::dto::{Cursor, LibrarySummary, MonthBucket, PhotoDetail, PhotoFilter, PhotoPage};
use crate::index::Index;
use crate::render::{self, Variant};

#[derive(Clone)]
pub struct AppState {
    pub index: Arc<Index>,
    pub auth: Arc<AuthContext>,
    pub rules: Arc<AccessRules>,
    pub cache_dir: Arc<std::path::PathBuf>,
}

// Sub-extractors so handlers can take the narrow state they need.
impl FromRef<AppState> for Arc<AuthContext> {
    fn from_ref(s: &AppState) -> Self {
        s.auth.clone()
    }
}
impl FromRef<AppState> for Arc<Index> {
    fn from_ref(s: &AppState) -> Self {
        s.index.clone()
    }
}

/// Handler error → HTTP status. Internal details are logged, never sent.
pub enum AppError {
    Unauthorized,
    Forbidden,
    NotFound,
    Internal(anyhow::Error),
}

impl AppError {
    pub fn internal<E: Into<anyhow::Error>>(e: E) -> Self {
        Self::Internal(e.into())
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            AppError::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            AppError::NotFound => (StatusCode::NOT_FOUND, "not found"),
            AppError::Internal(e) => {
                tracing::error!(?e, "request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
            }
        };
        (code, msg).into_response()
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/auth/login", get(auth::login))
        .route("/auth/callback", get(auth::callback))
        .route("/auth/logout", get(auth::logout))
        .route("/api/libraries", get(list_libraries))
        .route("/api/photos", get(list_photos))
        .route("/api/photos/histogram", get(histogram))
        .route("/api/photos/{photo_id}", get(photo_detail))
        .route("/api/photos/{photo_id}/thumb", get(thumb))
        .route("/api/photos/{photo_id}/display", get(display))
        .route(
            "/api/photos/{photo_id}/resource/{resource_type}",
            get(resource),
        )
        .with_state(state)
}

async fn health(State(index): State<Arc<Index>>) -> String {
    match index.libraries().await {
        Ok(libs) => format!("ok ({} libraries)", libs.len()),
        Err(e) => format!("degraded: {e}"),
    }
}

async fn list_libraries(
    State(st): State<AppState>,
    AuthUser(user): AuthUser,
) -> Result<Json<Vec<LibrarySummary>>, AppError> {
    let all = st.index.libraries().await?;
    Ok(Json(
        all.into_iter()
            .filter(|l| can_access(&st.rules, &user, &l.library_id))
            .collect(),
    ))
}

#[derive(serde::Deserialize)]
struct PhotosQuery {
    /// One or more library ids, comma-separated (ids are `[a-z0-9_]`, so the
    /// comma is unambiguous). Multiple libraries fuse into one timeline.
    library: String,
    cursor: Option<String>,
    limit: Option<u32>,
    // Tri-state filters: absent = any, true = only, false = exclude.
    video: Option<bool>,
    live: Option<bool>,
    raw: Option<bool>,
    favorite: Option<bool>,
}

impl PhotosQuery {
    /// Split + authorize the library set. Every requested library must be
    /// accessible — a fused request never silently drops a forbidden one.
    fn libraries(&self, st: &AppState, user: &SessionUser) -> Result<Vec<String>, AppError> {
        let libs: Vec<String> = self
            .library
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if libs.is_empty() {
            return Err(AppError::Forbidden);
        }
        for lib in &libs {
            if !can_access(&st.rules, user, lib) {
                return Err(AppError::Forbidden);
            }
        }
        Ok(libs)
    }

    fn filter(&self) -> PhotoFilter {
        PhotoFilter {
            video: self.video,
            live: self.live,
            raw: self.raw,
            favorite: self.favorite,
        }
    }
}

async fn list_photos(
    State(st): State<AppState>,
    AuthUser(user): AuthUser,
    Query(q): Query<PhotosQuery>,
) -> Result<Json<PhotoPage>, AppError> {
    let libs = q.libraries(&st, &user)?;
    // A malformed cursor degrades to page 1, never a 500.
    let cursor = q.cursor.as_deref().and_then(Cursor::from_token);
    let page = st
        .index
        .list_photos(&libs, cursor, q.limit.unwrap_or(100), &q.filter())
        .await?;
    Ok(Json(page))
}

async fn histogram(
    State(st): State<AppState>,
    AuthUser(user): AuthUser,
    Query(q): Query<PhotosQuery>,
) -> Result<Json<Vec<MonthBucket>>, AppError> {
    let libs = q.libraries(&st, &user)?;
    Ok(Json(st.index.month_histogram(&libs, &q.filter()).await?))
}

async fn photo_detail(
    State(st): State<AppState>,
    AuthUser(user): AuthUser,
    Path(photo_id): Path<String>,
) -> Result<Json<PhotoDetail>, AppError> {
    // Fetch first, then authorize on the row's library: a nonexistent id is a
    // clean 404; a real id in a forbidden library is a 403.
    let detail = st
        .index
        .photo_detail(&photo_id)
        .await?
        .ok_or(AppError::NotFound)?;
    if !can_access(&st.rules, &user, &detail.library_id) {
        return Err(AppError::Forbidden);
    }
    Ok(Json(detail))
}

async fn thumb(
    State(st): State<AppState>,
    AuthUser(user): AuthUser,
    Path(photo_id): Path<String>,
    req: Request,
) -> Result<Response, AppError> {
    rendition(&st, &user, &photo_id, Variant::Thumb, req).await
}

async fn display(
    State(st): State<AppState>,
    AuthUser(user): AuthUser,
    Path(photo_id): Path<String>,
    req: Request,
) -> Result<Response, AppError> {
    rendition(&st, &user, &photo_id, Variant::Display, req).await
}

/// Serve a cached JPEG rendition of a photo's `original` resource (a video
/// original yields a poster frame). Cached by content_hash → immutable.
async fn rendition(
    st: &AppState,
    user: &SessionUser,
    photo_id: &str,
    variant: Variant,
    req: Request,
) -> Result<Response, AppError> {
    let loc = st
        .index
        .resource_blob(photo_id, "original")
        .await?
        .ok_or(AppError::NotFound)?;
    if !can_access(&st.rules, user, &loc.library_id) {
        return Err(AppError::Forbidden);
    }
    let bp = st
        .index
        .blob_paths(&loc.library_id)
        .ok_or(AppError::NotFound)?;
    let hash = ingress_core::ContentHash::from_hex(loc.content_hash.clone());
    let blob = bp.blob_path(&hash, &loc.ext);
    let cached =
        render::render_to_cache(&st.cache_dir, &loc.content_hash, &blob, &loc.ext, variant)
            .await
            .map_err(AppError::internal)?;

    let mut resp = ServeFile::new(&cached)
        .oneshot(req)
        .await
        .map_err(|e| AppError::internal(anyhow::anyhow!("serve rendition: {e}")))?
        .map(axum::body::Body::new);
    // Renditions are content-addressed → immutable, cache aggressively.
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    Ok(resp)
}

/// Raw blob bytes with HTTP range support (video streaming + download). No
/// decode — `ServeFile` implements the full range contract over a streamed
/// body, correct for 50 MB+ blobs.
async fn resource(
    State(st): State<AppState>,
    AuthUser(user): AuthUser,
    Path((photo_id, resource_type)): Path<(String, String)>,
    req: Request,
) -> Result<Response, AppError> {
    let loc = st
        .index
        .resource_blob(&photo_id, &resource_type)
        .await?
        .ok_or(AppError::NotFound)?;
    if !can_access(&st.rules, &user, &loc.library_id) {
        return Err(AppError::Forbidden);
    }
    let bp = st
        .index
        .blob_paths(&loc.library_id)
        .ok_or(AppError::NotFound)?;
    let hash = ingress_core::ContentHash::from_hex(loc.content_hash);
    let path = bp.blob_path(&hash, &loc.ext);

    // ServeFile forwards the request's Range/If-Range headers and answers with
    // 206 + Content-Range as appropriate; content-type is guessed from the ext.
    let mut resp = ServeFile::new(&path)
        .oneshot(req)
        .await
        .map_err(|e| AppError::internal(anyhow::anyhow!("serve blob: {e}")))?
        .map(axum::body::Body::new);
    resp.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    Ok(resp)
}
