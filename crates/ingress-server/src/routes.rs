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

use crate::auth::{self, AccessRules, AuthContext, AuthUser, can_access};
use crate::dto::{Cursor, LibrarySummary, PhotoDetail, PhotoFilter, PhotoPage};
use crate::index::Index;

#[derive(Clone)]
pub struct AppState {
    pub index: Arc<Index>,
    pub auth: Arc<AuthContext>,
    pub rules: Arc<AccessRules>,
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
        .route("/api/photos/{photo_id}", get(photo_detail))
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
    library: String,
    cursor: Option<String>,
    limit: Option<u32>,
    media_type: Option<String>,
    favorite: Option<bool>,
}

async fn list_photos(
    State(st): State<AppState>,
    AuthUser(user): AuthUser,
    Query(q): Query<PhotosQuery>,
) -> Result<Json<PhotoPage>, AppError> {
    if !can_access(&st.rules, &user, &q.library) {
        return Err(AppError::Forbidden);
    }
    // A malformed cursor degrades to page 1, never a 500.
    let cursor = q.cursor.as_deref().and_then(Cursor::from_token);
    let filter = PhotoFilter {
        media_type: q.media_type,
        favorite: q.favorite,
    };
    let page = st
        .index
        .list_photos(&q.library, cursor, q.limit.unwrap_or(100), &filter)
        .await?;
    Ok(Json(page))
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
