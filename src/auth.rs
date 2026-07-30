use aes_siv::{Key, Nonce, siv::Aes256Siv};
use argon2::{Algorithm, Argon2 as Argon2Raw, Params, Version};
use axum::http;
use axum::{
    Extension, Json,
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use chacha20poly1305::{
    ChaCha20Poly1305,
    aead::{Aead, KeyInit},
};
use chrono::{Duration, TimeDelta, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, decode, encode};
use jsonwebtoken::{TokenData, Validation};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::db;
use crate::db::types::XPubKey;
use crate::{AppState, PrivKey};

#[derive(Clone, Debug)]
pub struct SessionEntry {
    pub user_keys: crate::UserKeys,
    pub siv_key: Key<Aes256Siv>,
    pub siv_nonce: Nonce,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

pub type SessionStore = RwLock<HashMap<i32, SessionEntry>>;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    exp: usize,  // expiry of our token
    iss: String, // issuer is nodeID that issued it
    uid: String, // userid id userID it's valid for
}

#[derive(Serialize, Deserialize)]
pub struct SignInData {
    pub username: String,
    pub passphrase: String,
    pub remember_me: Option<bool>,
}

#[derive(Serialize)]
pub struct SignInResponse {
    pub token: String,
}

pub struct AuthError {
    message: String,
    status_code: StatusCode,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response<Body> {
        (self.status_code, self.message).into_response()
    }
}

// generate the random key, rolled every startup
pub fn generate_jwt_key() -> (EncodingKey, DecodingKey) {
    let mut rng = rand::rng();
    let secret: Vec<u8> = (0..16).map(|_| rng.random_range(0..=255)).collect();
    let encodingkey = EncodingKey::from_secret(secret.as_ref());
    let decodingkey = DecodingKey::from_secret(secret.as_ref());
    (encodingkey, decodingkey)
}

// middleware for validation
pub async fn auth_middleware(
    State(app_state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response<Body>, AuthError> {
    let auth_header = req.headers_mut().get(http::header::AUTHORIZATION);

    let auth_header = match auth_header {
        Some(header) => header.to_str().map_err(|_| AuthError {
            message: "Empty header is not allowed".to_string(),
            status_code: StatusCode::FORBIDDEN,
        })?,
        None => {
            return Err(AuthError {
                message: "Please add the JWT token to the header".to_string(),
                status_code: StatusCode::FORBIDDEN,
            });
        }
    };

    let mut header = auth_header.split_whitespace();

    let (_bearer, token) = (header.next(), header.next());

    let token_data = match decode_jwt(token.unwrap().to_string(), app_state.decoding_key) {
        Ok(data) => data,
        Err(_) => {
            return Err(AuthError {
                message: "Unable to decode token".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        }
    };

    // check user exists in db (what if deleted?)
    let uid: i32 = token_data.claims.uid.parse().map_err(|_| AuthError {
        message: "Malformed JWT".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    })?;
    match db::users::get_user_by_userid(app_state.db_pool.get(), uid) {
        Ok(Some(_user)) => {
            // store the user ID in request extensions
            req.extensions_mut().insert(uid);
            Ok(next.run(req).await) // future can check user perms here
        }
        Ok(None) => Err(AuthError {
            message: "User does not exist".to_string(),
            status_code: StatusCode::UNAUTHORIZED,
        }),
        Err(_) => Err(AuthError {
            message: "Error checking user database".to_string(),
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
        }),
    }
}

fn decode_jwt(jwt_token: String, key: DecodingKey) -> Result<TokenData<Claims>, StatusCode> {
    decode(&jwt_token, &key, &Validation::default()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn encode_jwt(iss: String, uid: String, key: EncodingKey) -> Result<String, StatusCode> {
    let now = Utc::now();
    let expire: TimeDelta = Duration::hours(1);
    let exp: usize = (now + expire).timestamp() as usize;
    let claim = Claims { exp, iss, uid };

    encode(&Header::default(), &claim, &key).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn sign_in(
    State(app_state): State<AppState>,
    Json(user_data): Json<SignInData>,
) -> Result<Json<SignInResponse>, AuthError> {
    let remember_me = user_data.remember_me.unwrap_or(false);
    let duration_hours: i64 = if remember_me { 24 } else { 1 };

    let node_id = app_state.get_node_id().map_err(|_| AuthError {
        message: "Node not initialized".into(),
        status_code: StatusCode::SERVICE_UNAVAILABLE,
    })?;

    // Look up user
    let db_user = db::users::get_user_by_username(app_state.db_pool.get(), user_data.username)
        .map_err(|_| AuthError {
            message: "Database error".into(),
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
        })?
        .ok_or(AuthError {
            message: "Invalid credentials".into(),
            status_code: StatusCode::UNAUTHORIZED,
        })?;

    // Unwrap private key — this IS the authentication (3-5s, 1 GiB Argon2id)
    // If the passphrase is wrong, ChaCha20-Poly1305 decryption fails
    let encrypted_privkey = db_user.encrypted_privkey.clone();
    let key_salt = db_user.key_salt.clone();
    let passphrase = crate::passphrase::normalize_passphrase(&user_data.passphrase);
    let privkey = tokio::task::spawn_blocking(move || {
        unwrap_user_privkey(&encrypted_privkey, &key_salt, &passphrase).map_err(|e| e.to_string())
    })
    .await
    .map_err(|_| AuthError {
        message: "Internal error".into(),
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
    })?
    .map_err(|_| AuthError {
        message: "Invalid credentials".into(),
        status_code: StatusCode::UNAUTHORIZED,
    })?;

    // Derive all key material
    let pubkey = crate::db::PubKey(privkey.verifying_key());
    let (siv_key, siv_nonce) = derive_siv_key_from_user(&privkey, "file_path");

    // Save bytes before privkey is moved into UserKeys
    #[cfg(all(target_os = "macos", feature = "gui", not(debug_assertions)))]
    let privkey_bytes = privkey.0.to_bytes();

    let user_keys = crate::UserKeys {
        private_key: privkey,
        public_key: pubkey,
    };

    // Build session entry
    let expires_at = Utc::now() + Duration::hours(duration_hours);
    let session = SessionEntry {
        user_keys,
        siv_key,
        siv_nonce,
        expires_at,
    };

    // Insert into session store
    {
        let mut store = app_state.session_store.write().await;
        store.insert(db_user.user_id, session);
    }

    // Resume any stranded import owned by this node for this user.
    tokio::spawn(hopnet_takeout::jobs::maybe_resume_for_user(
        crate::takeout_host::takeout_state(&app_state),
        db_user.user_id,
    ));

    // Store in keychain for auto-login (GUI mode, owner only)
    #[cfg(all(target_os = "macos", feature = "gui", not(debug_assertions)))]
    {
        if let Ok(owner_id) = app_state.get_user_id() {
            if db_user.user_id == owner_id {
                let _ = crate::fileprovider::keychain::store_session_key(
                    crate::fileprovider::keychain::KeychainEnvironment::Production,
                    db_user.user_id,
                    &privkey_bytes,
                );

                if let Err(e) = crate::devices::routes::ensure_fileprovider_device_token(
                    &app_state,
                    db_user.user_id,
                )
                .await
                {
                    tracing::warn!("Failed to ensure FileProvider device token: {:?}", e);
                }

                if let Err(e) = crate::devices::routes::ensure_photo_ingress_device_token(
                    &app_state,
                    db_user.user_id,
                )
                .await
                {
                    tracing::warn!("Failed to ensure photo-ingress device token: {:?}", e);
                }
            }
        }
    }

    // Encode JWT with matching duration
    let token = encode_jwt_with_duration(
        node_id.to_string(),
        db_user.user_id.to_string(),
        app_state.encoding_key,
        duration_hours,
    )?;

    Ok(Json(SignInResponse { token }))
}

pub fn encode_jwt_with_duration(
    iss: String,
    uid: String,
    key: EncodingKey,
    hours: i64,
) -> Result<String, AuthError> {
    let now = Utc::now();
    let exp = (now + Duration::hours(hours)).timestamp() as usize;
    let claim = Claims { exp, iss, uid };
    encode(&Header::default(), &claim, &key).map_err(|_| AuthError {
        message: "JWT encoding failed".into(),
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
    })
}

/// Logout endpoint. Removes session from store.
/// In GUI mode, the owner's keychain-loaded session is preserved for auto-login.
pub async fn sign_out(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> StatusCode {
    // In GUI mode, protect the owner's session (loaded from keychain for auto-login)
    #[cfg(feature = "gui")]
    {
        if let Ok(owner_id) = app_state.get_user_id() {
            if uid == owner_id {
                return StatusCode::OK;
            }
        }
    }

    app_state.session_store.write().await.remove(&uid);
    app_state.photos_host.shutdown(uid).await;
    StatusCode::OK
}

/// Derives SIV key and nonce from user's private key using Blake3 key derivation
pub fn derive_siv_key_from_user(user_privkey: &PrivKey, context: &str) -> (Key<Aes256Siv>, Nonce) {
    // Use the user's private key bytes as input key material
    let ikm = user_privkey.to_bytes();

    // Derive SIV key (64 bytes for AES-256-SIV) using XOF for custom length
    let mut siv_key_bytes = [0u8; 64];
    let mut hasher = blake3::Hasher::new_derive_key(&format!("hopnet {} siv_key", context));
    hasher.update(&ikm);
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut siv_key_bytes);
    let siv_key = Key::<Aes256Siv>::from(siv_key_bytes);

    // Derive SIV nonce (16 bytes) using XOF for custom length
    let mut siv_nonce_bytes = [0u8; 16];
    let mut hasher = blake3::Hasher::new_derive_key(&format!("hopnet {} siv_nonce", context));
    hasher.update(&ikm);
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut siv_nonce_bytes);
    let siv_nonce = Nonce::from(siv_nonce_bytes);

    (siv_key, siv_nonce)
}

/// Derives X25519 public key from user's Ed25519 private key using Blake3 key derivation
pub fn derive_x25519_pubkey_from_user(user_privkey: &PrivKey) -> XPubKey {
    // Use the user's private key bytes as input key material
    let ikm = user_privkey.to_bytes();

    // Derive X25519 secret key (32 bytes) using Blake3 for deterministic key derivation
    let mut x25519_secret_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet x25519_secret");
    hasher.update(&ikm);
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut x25519_secret_bytes);

    // Create X25519 static secret and derive public key
    let x25519_secret = x25519_dalek::StaticSecret::from(x25519_secret_bytes);
    let x25519_pubkey = x25519_dalek::PublicKey::from(&x25519_secret);
    XPubKey::from(x25519_pubkey)
}

/// Derives X25519 private key from user's Ed25519 private key using Blake3 key derivation
pub fn derive_x25519_privkey_from_user(user_privkey: &PrivKey) -> x25519_dalek::StaticSecret {
    // Use the user's private key bytes as input key material
    let ikm = user_privkey.to_bytes();

    // Derive X25519 secret key (32 bytes) using Blake3 for deterministic key derivation
    let mut x25519_secret_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet x25519_secret");
    hasher.update(&ikm);
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut x25519_secret_bytes);

    // Create X25519 static secret
    x25519_dalek::StaticSecret::from(x25519_secret_bytes)
}

/// Derives the per-user photo fingerprint key from the user's Ed25519
/// private key. Fingerprints are `blake3::keyed_hash(&key, cloud_id)` over
/// the asset's stable cross-device identifier (PHCloudIdentifier), computed
/// node-side in the resolve route — keyed so replicated state carries no
/// unkeyed function of the identifier (RFC-014 confirmation-oracle rule).
///
/// The context string is FROZEN: changing it orphans every committed
/// cloud_fingerprint (dedupe silently stops matching existing rows).
pub fn derive_photo_fingerprint_key(user_privkey: &PrivKey) -> [u8; 32] {
    let mut key = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet photos cloud fingerprint v1");
    hasher.update(&user_privkey.to_bytes());
    hasher.finalize_xof().fill(&mut key);
    key
}

/// Wrap a user private key with a password using Argon2id + ChaCha20-Poly1305.
/// Returns (nonce || ciphertext, salt).
pub fn wrap_user_privkey(
    privkey: &PrivKey,
    password: &str,
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    // Generate 16-byte random salt
    let mut salt = [0u8; 16];
    let mut rng = rand::rng();
    rng.fill(&mut salt);

    // Derive 32-byte wrapping key using Argon2id (1 GiB memory, 2 iterations, 1 parallelism)
    let params = Params::new(1_048_576, 2, 1, Some(32))?;
    let argon2 = Argon2Raw::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key_bytes = [0u8; 32];
    argon2
        .hash_password_into(password.as_bytes(), &salt, &mut key_bytes)
        .map_err(|e| format!("Argon2 key derivation failed: {}", e))?;

    // Generate 12-byte random nonce
    let mut nonce_bytes = [0u8; 12];
    rng.fill(&mut nonce_bytes);
    let nonce = chacha20poly1305::Nonce::from(nonce_bytes);

    // Encrypt the private key
    let cipher = ChaCha20Poly1305::new(chacha20poly1305::Key::from_slice(&key_bytes));
    let ciphertext = cipher
        .encrypt(&nonce, privkey.0.to_bytes().as_slice())
        .map_err(|e| format!("Encryption failed: {:?}", e))?;

    // Return nonce || ciphertext, and salt
    let mut encrypted = Vec::with_capacity(12 + ciphertext.len());
    encrypted.extend_from_slice(&nonce_bytes);
    encrypted.extend_from_slice(&ciphertext);

    Ok((encrypted, salt.to_vec()))
}

/// Unwrap a password-wrapped user private key.
/// encrypted_privkey is nonce (12 bytes) || ciphertext.
pub fn unwrap_user_privkey(
    encrypted_privkey: &[u8],
    key_salt: &[u8],
    password: &str,
) -> Result<PrivKey, Box<dyn std::error::Error>> {
    if encrypted_privkey.len() < 12 {
        return Err("Encrypted private key too short".into());
    }

    let (nonce_bytes, ciphertext) = encrypted_privkey.split_at(12);
    let nonce = chacha20poly1305::Nonce::from_slice(nonce_bytes);

    // Derive wrapping key with same Argon2id parameters
    let params = Params::new(1_048_576, 2, 1, Some(32))?;
    let argon2 = Argon2Raw::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key_bytes = [0u8; 32];
    argon2
        .hash_password_into(password.as_bytes(), key_salt, &mut key_bytes)
        .map_err(|e| format!("Argon2 key derivation failed: {}", e))?;

    // Decrypt
    let cipher = ChaCha20Poly1305::new(chacha20poly1305::Key::from_slice(&key_bytes));
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| format!("Decryption failed: {:?}", e))?;

    if plaintext.len() != 32 {
        return Err("Decrypted key is not 32 bytes".into());
    }

    let mut key_arr = [0u8; 32];
    key_arr.copy_from_slice(&plaintext);
    Ok(PrivKey(ed25519_dalek::SigningKey::from_bytes(&key_arr)))
}

/// Wrap a user private key for storage in a device token.
/// Uses Blake3 key derivation (device secret has 256-bit entropy, no stretching needed)
/// followed by ChaCha20-Poly1305 encryption.
/// Returns nonce (12) || ciphertext (48) = 60 bytes.
pub fn wrap_user_key_for_device(
    device_secret: &[u8],
    privkey: &PrivKey,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Derive 32-byte wrapping key from device secret using Blake3
    let mut key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet-device-key-wrap-v1");
    hasher.update(device_secret);
    hasher.finalize_xof().fill(&mut key_bytes);

    // Encrypt with ChaCha20-Poly1305 using random 12-byte nonce
    let mut nonce_bytes = [0u8; 12];
    rand::rng().fill(&mut nonce_bytes);
    let nonce = chacha20poly1305::Nonce::from(nonce_bytes);

    let cipher = ChaCha20Poly1305::new(chacha20poly1305::Key::from_slice(&key_bytes));
    let ciphertext = cipher
        .encrypt(&nonce, privkey.0.to_bytes().as_slice())
        .map_err(|e| format!("Device key wrap encryption failed: {:?}", e))?;

    let mut wrapped = Vec::with_capacity(12 + ciphertext.len());
    wrapped.extend_from_slice(&nonce_bytes);
    wrapped.extend_from_slice(&ciphertext);

    Ok(wrapped)
}

/// Unwrap a user private key from a device token's wrapped blob.
/// Input: nonce (12) || ciphertext (48) = 60 bytes.
/// SIV key and nonce are re-derived from the unwrapped privkey via derive_siv_key_from_user.
pub fn unwrap_user_key_from_device(
    device_secret: &[u8],
    wrapped: &[u8],
) -> Result<PrivKey, Box<dyn std::error::Error>> {
    if wrapped.len() < 12 {
        return Err("Wrapped device key too short".into());
    }

    let (nonce_bytes, ciphertext) = wrapped.split_at(12);
    let nonce = chacha20poly1305::Nonce::from_slice(nonce_bytes);

    // Derive wrapping key (same Blake3 derivation)
    let mut key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet-device-key-wrap-v1");
    hasher.update(device_secret);
    hasher.finalize_xof().fill(&mut key_bytes);

    let cipher = ChaCha20Poly1305::new(chacha20poly1305::Key::from_slice(&key_bytes));
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| format!("Device key unwrap decryption failed: {:?}", e))?;

    if plaintext.len() != 32 {
        return Err(format!(
            "Decrypted device key is {} bytes, expected 32",
            plaintext.len()
        )
        .into());
    }

    let mut key_arr = [0u8; 32];
    key_arr.copy_from_slice(&plaintext);
    Ok(PrivKey(ed25519_dalek::SigningKey::from_bytes(&key_arr)))
}

/// Unwrap a per-blob key via the substrate's v1 wrap, using the session
/// user's X25519 privkey as the RecipientKey capability. The wrap format is
/// crate-private to hopnet-storage — this is the only unwrap path.
pub fn decrypt_wrapped_file_key(
    blob_access: &crate::db::types::BlobAccess,
    user_x25519_privkey: &x25519_dalek::StaticSecret,
) -> Result<chacha20poly1305::Key, Box<dyn std::error::Error>> {
    let reader = hopnet_storage::crypto::StaticRecipient(user_x25519_privkey.clone());
    hopnet_storage::crypto::unwrap_blob_key(blob_access, &reader)
        .map_err(|e| format!("Unwrap failed: {e}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    #[test]
    fn test_wrap_unwrap_round_trip() {
        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let privkey = PrivKey(signing_key);
        let password = "test-password-123";

        let (encrypted, salt) = wrap_user_privkey(&privkey, password).unwrap();
        let recovered = unwrap_user_privkey(&encrypted, &salt, password).unwrap();

        assert_eq!(privkey.0.to_bytes(), recovered.0.to_bytes());
    }

    #[test]
    fn test_unwrap_wrong_password() {
        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let privkey = PrivKey(signing_key);

        let (encrypted, salt) = wrap_user_privkey(&privkey, "correct-password").unwrap();
        let result = unwrap_user_privkey(&encrypted, &salt, "wrong-password");

        assert!(result.is_err());
    }

    #[test]
    fn test_wrap_unwrap_with_generated_passphrase() {
        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let privkey = PrivKey(signing_key);
        let passphrase = crate::passphrase::generate_passphrase();

        let (encrypted, salt) = wrap_user_privkey(&privkey, &passphrase).unwrap();
        let recovered = unwrap_user_privkey(&encrypted, &salt, &passphrase).unwrap();

        assert_eq!(privkey.0.to_bytes(), recovered.0.to_bytes());
    }

    #[test]
    fn test_wrap_unwrap_with_normalized_passphrase() {
        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let privkey = PrivKey(signing_key);
        let passphrase = crate::passphrase::generate_passphrase();

        let (encrypted, salt) = wrap_user_privkey(&privkey, &passphrase).unwrap();

        // Mangle passphrase: uppercase + extra spaces
        let mangled = passphrase.to_uppercase().replace(' ', "   ");
        let normalized = crate::passphrase::normalize_passphrase(&mangled);
        let recovered = unwrap_user_privkey(&encrypted, &salt, &normalized).unwrap();

        assert_eq!(privkey.0.to_bytes(), recovered.0.to_bytes());
    }

    #[test]
    fn test_unwrap_corrupted_ciphertext() {
        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let privkey = PrivKey(signing_key);
        let password = "test-password";

        let (mut encrypted, salt) = wrap_user_privkey(&privkey, password).unwrap();

        // Corrupt a byte in the ciphertext (after the 12-byte nonce)
        if encrypted.len() > 14 {
            encrypted[14] ^= 0xff;
        }

        let result = unwrap_user_privkey(&encrypted, &salt, password);
        assert!(result.is_err());
    }

    #[test]
    fn test_device_key_wrap_unwrap_round_trip() {
        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let privkey = PrivKey(signing_key);
        let secret = [0xABu8; 32];

        let wrapped = wrap_user_key_for_device(&secret, &privkey).unwrap();
        assert_eq!(wrapped.len(), 60); // 12 nonce + 32 plaintext + 16 tag

        let recovered = unwrap_user_key_from_device(&secret, &wrapped).unwrap();
        assert_eq!(privkey.0.to_bytes(), recovered.0.to_bytes());
    }

    #[test]
    fn test_device_key_unwrap_wrong_secret() {
        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let privkey = PrivKey(signing_key);

        let wrapped = wrap_user_key_for_device(&[0xAAu8; 32], &privkey).unwrap();
        let result = unwrap_user_key_from_device(&[0xBBu8; 32], &wrapped);
        assert!(result.is_err());
    }

    #[test]
    fn test_device_key_unwrap_corrupted_ciphertext() {
        let signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let privkey = PrivKey(signing_key);
        let secret = [0xCCu8; 32];

        let mut wrapped = wrap_user_key_for_device(&secret, &privkey).unwrap();
        wrapped[14] ^= 0xff;

        let result = unwrap_user_key_from_device(&secret, &wrapped);
        assert!(result.is_err());
    }
}
