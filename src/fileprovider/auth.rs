use axum::{
    extract::{Request, State},
    response::{Response, IntoResponse},
    body::Body,
    http::{StatusCode, header},
    middleware::Next,
};

use crate::AppState;

pub struct FileProviderAuthError {
    message: String,
    status_code: StatusCode,
}

impl IntoResponse for FileProviderAuthError {
    fn into_response(self) -> Response<Body> {
        (self.status_code, self.message).into_response()
    }
}

/// Middleware for FileProvider API key authentication
/// Validates scoped API key for FileProvider endpoints only
pub async fn fileprovider_auth_middleware(
    State(app_state): State<AppState>,
    req: Request, 
    next: Next
) -> Result<Response<Body>, FileProviderAuthError> {
    let auth_header = req.headers().get(header::AUTHORIZATION);
    
    let auth_header = match auth_header {
        Some(header) => header.to_str().map_err(|_| FileProviderAuthError {
            message: "Invalid authorization header".to_string(),
            status_code: StatusCode::BAD_REQUEST
        })?,
        None => return Err(FileProviderAuthError {
            message: "Missing authorization header".to_string(),
            status_code: StatusCode::UNAUTHORIZED
        }),
    };

    // Parse Bearer token
    let mut header_parts = auth_header.split_whitespace();
    let (bearer, token) = (header_parts.next(), header_parts.next());

    if bearer != Some("Bearer") || token.is_none() {
        return Err(FileProviderAuthError {
            message: "Invalid authorization format. Expected 'Bearer <token>'".to_string(),
            status_code: StatusCode::BAD_REQUEST
        });
    }

    let provided_token = token.unwrap();

    // Validate the provided token against the FileProvider API key from AppState
    if provided_token != app_state.fileprovider_api_key {
        return Err(FileProviderAuthError {
            message: "Invalid FileProvider API key".to_string(),
            status_code: StatusCode::UNAUTHORIZED
        });
    }

    // API key is valid - FileProvider extension is authenticated
    // The key is scoped to only FileProvider operations by route nesting

    Ok(next.run(req).await)
}