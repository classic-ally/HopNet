use crate::db::{self, Blake3Hash, CustomUUID, devices::get_device_by_id};
use crate::{AppState, UserKeys, auth};
use axum::{
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

pub struct DeviceTokenAuthError {
    message: String,
    status_code: StatusCode,
}

impl IntoResponse for DeviceTokenAuthError {
    fn into_response(self) -> Response<Body> {
        (self.status_code, self.message).into_response()
    }
}

/// Middleware for device token authentication (used by DocumentProvider, FileProvider, etc.)
/// Token format: {device_id}.{secret}
/// Validates against consensus-replicated device_tokens table
pub async fn device_token_auth_middleware(
    State(app_state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response<Body>, DeviceTokenAuthError> {
    let auth_header = req.headers().get(header::AUTHORIZATION);

    let auth_header = match auth_header {
        Some(header) => header.to_str().map_err(|_| DeviceTokenAuthError {
            message: "Invalid authorization header".to_string(),
            status_code: StatusCode::BAD_REQUEST,
        })?,
        None => {
            return Err(DeviceTokenAuthError {
                message: "Missing authorization header".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        }
    };

    // Parse Bearer token
    let mut header_parts = auth_header.split_whitespace();
    let token = match (header_parts.next(), header_parts.next()) {
        (Some("Bearer"), Some(token)) => token,
        _ => {
            return Err(DeviceTokenAuthError {
                message: "Invalid authorization format. Expected 'Bearer <token>'".to_string(),
                status_code: StatusCode::BAD_REQUEST,
            });
        }
    };

    // Parse token format: {device_id}.{secret}
    let (device_id, secret) = parse_device_token(token).map_err(|_| DeviceTokenAuthError {
        message: "Invalid token format".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    })?;

    // Look up device by ID (primary key lookup, O(log n))
    let db_lock = app_state.db_pool.get().map_err(|_| DeviceTokenAuthError {
        message: "Database connection error".to_string(),
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    let device = get_device_by_id(&db_lock, &device_id).map_err(|_| DeviceTokenAuthError {
        message: "Database error".to_string(),
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    let device = match device {
        Some(d) => d,
        None => {
            return Err(DeviceTokenAuthError {
                message: "Invalid device token".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        }
    };

    // Verify secret hash
    let secret_hash = Blake3Hash::new(blake3::hash(secret.as_bytes()));
    if secret_hash != device.api_key_hash {
        return Err(DeviceTokenAuthError {
            message: "Invalid device token".to_string(),
            status_code: StatusCode::UNAUTHORIZED,
        });
    }

    // Bootstrap session if needed (cheap read-lock check first)
    let needs_session = {
        let store = app_state.session_store.read().await;
        match store.get(&device.user_id) {
            Some(entry) if entry.expires_at > chrono::Utc::now() => false,
            _ => true,
        }
    };

    if needs_session {
        let secret_bytes = hex::decode(&secret).map_err(|_| DeviceTokenAuthError {
            message: "Invalid device secret encoding".to_string(),
            status_code: StatusCode::UNAUTHORIZED,
        })?;

        let privkey = auth::unwrap_user_key_from_device(&secret_bytes, &device.wrapped_user_key)
            .map_err(|_| DeviceTokenAuthError {
                message: "Failed to unwrap device key".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            })?;

        let pubkey = db::PubKey(privkey.verifying_key());
        let (siv_key, siv_nonce) = auth::derive_siv_key_from_user(&privkey, "file_path");
        let user_keys = UserKeys {
            private_key: privkey,
            public_key: pubkey,
        };
        let expires_at = chrono::Utc::now() + chrono::Duration::minutes(5);
        let session = auth::SessionEntry {
            user_keys,
            siv_key,
            siv_nonce,
            expires_at,
        };

        let inserted = {
            let mut store = app_state.session_store.write().await;
            // Re-check under write lock to avoid overwriting a longer-lived session
            match store.get(&device.user_id) {
                Some(entry) if entry.expires_at > chrono::Utc::now() => false,
                _ => {
                    store.insert(device.user_id, session);
                    true
                }
            }
        };
        if inserted {
            tokio::spawn(crate::takeout::jobs::maybe_resume_for_user(
                app_state.clone(),
                device.user_id,
            ));
        }
    }

    // Insert user_id into request extensions for downstream handlers
    req.extensions_mut().insert(device.user_id);

    Ok(next.run(req).await)
}

/// Parse device token in format {device_id}.{secret}
fn parse_device_token(token: &str) -> Result<(CustomUUID, String), ()> {
    let dot_pos = token.find('.').ok_or(())?;

    let device_id_str = &token[..dot_pos];
    let secret = &token[dot_pos + 1..];

    if secret.is_empty() {
        return Err(());
    }

    let device_id = CustomUUID::from_str(device_id_str).map_err(|_| ())?;

    Ok((device_id, secret.to_string()))
}
