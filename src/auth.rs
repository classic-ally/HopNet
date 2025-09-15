use axum::http;
use chrono::{TimeDelta, Utc, Duration};
use jsonwebtoken::{TokenData, Validation};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, encode, decode};
use axum::{
    extract::{Request, State},
    response::{Response, IntoResponse},
    body::Body,
    Json,
    http::StatusCode,
    middleware::Next,
};
use serde::{Serialize, Deserialize};
use rand::Rng;
use aes_siv::{siv::Aes256Siv, Key, Nonce};
use chacha20poly1305::{ChaCha20Poly1305, aead::{Aead, KeyInit}};

use crate::db;
use crate::{AppState, PrivKey};
use crate::db::types::XPubKey;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    exp: usize,         // expiry of our token
    iss: String,        // issuer is nodeID that issued it
    uid: String,        // userid id userID it's valid for
}

#[derive(Serialize, Deserialize)]
pub struct SignInData {
    pub username: String,
    pub password: String,
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
    return (encodingkey, decodingkey);
}

// generate a secure API key for FileProvider, rolled every startup
pub fn generate_fileprovider_api_key() -> String {
    let mut rng = rand::rng();
    let key_bytes: Vec<u8> = (0..32).map(|_| rng.random_range(0..=255)).collect();
    hex::encode(key_bytes)
}

// middleware for validation
pub async fn auth_middleware(
    State(app_state): State<AppState>,
    mut req: Request, 
    next: Next
) -> Result<Response<Body>, AuthError> {
    let auth_header = req.headers_mut().get(http::header::AUTHORIZATION);
    
    let auth_header = match auth_header {
        Some(header) => header.to_str().map_err(|_| AuthError {
            message: "Empty header is not allowed".to_string(),
            status_code: StatusCode::FORBIDDEN
        })?,
        None => return Err(AuthError {
            message: "Please add the JWT token to the header".to_string(),
            status_code: StatusCode::FORBIDDEN
        }),
    };

    let mut header = auth_header.split_whitespace();

    let (_bearer, token) = (header.next(), header.next());

    let token_data = match decode_jwt(token.unwrap().to_string(), app_state.decoding_key) {
        Ok(data) => data,
        Err(_) => return Err(AuthError {
            message: "Unable to decode token".to_string(),
            status_code: StatusCode::UNAUTHORIZED
        }),
    };

    // check user exists in db (what if deleted?)
    let uid: i32 = token_data.claims.uid.parse().map_err(|_| AuthError{ message: "Malformed JWT".to_string(), status_code: StatusCode::BAD_REQUEST })?;
    match db::users::get_user_by_userid(app_state.db_pool.get(), uid) {
        Ok(Some(_user)) => {
            // store the user ID in request extensions
            req.extensions_mut().insert(uid);
            return Ok(next.run(req).await) // future can check user perms here
        },
        Ok(None) => return Err(AuthError { message: "User does not exist".to_string(), status_code: StatusCode::UNAUTHORIZED }),
        Err(_) => return Err(AuthError { message: "Error checking user database".to_string(), status_code: StatusCode::INTERNAL_SERVER_ERROR })
    };
    

}

fn decode_jwt(jwt_token: String, key: DecodingKey) -> Result<TokenData<Claims>, StatusCode> {
    return decode(&jwt_token, &key, &Validation::default()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
}

fn encode_jwt(iss: String, uid: String, key: EncodingKey) -> Result<String, StatusCode> {
    let now = Utc::now();
    let expire: TimeDelta = Duration::hours(1);
    let exp: usize = (now + expire).timestamp() as usize;
    let claim = Claims {
        exp,
        iss,
        uid
    };

    return encode(
        &Header::default(),
        &claim,
        &key
    ).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
}

pub async fn sign_in(
    State(app_state): State<AppState>,
    Json(user_data): Json<SignInData>
) -> Result<Json<String>, StatusCode> {
    // get user by username from db
    match db::users::get_user_by_username(app_state.db_pool.get(), user_data.username) {
        Ok(Some(mut db_user)) => {
            // verify user password against hash
            match db_user.verify_password(user_data.password.as_bytes()) {
                Ok(true) => return Ok(Json(encode_jwt('1'.to_string(), db_user.user_id.to_string(), app_state.encoding_key)?)), // return a JWT
                Ok(false) => return Err(StatusCode::UNAUTHORIZED),      // bad password
                Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR) // some error
            }
        }
        Ok(None) => return Err(StatusCode::UNAUTHORIZED),               // no user exists
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR)         // some error
    }
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

/// Decrypt a wrapped per-file key using X25519 ECDH and ChaCha20-Poly1305
pub fn decrypt_wrapped_file_key(
    file_access: &crate::db::types::FileAccess,
    user_x25519_privkey: &x25519_dalek::StaticSecret,
) -> Result<chacha20poly1305::Key, Box<dyn std::error::Error>> {
    // Perform ECDH with ephemeral public key
    let shared_secret = user_x25519_privkey.diffie_hellman(file_access.ephemeral_pubkey.as_x25519());
    
    // Derive ChaCha20Poly1305 key from shared secret using Blake3
    let mut wrap_key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet key_wrap");
    hasher.update(shared_secret.as_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut wrap_key_bytes);
    let wrap_key = chacha20poly1305::Key::from(wrap_key_bytes);
    
    // Derive deterministic nonce from data_block_id + user_id + ephemeral_pubkey
    let mut nonce_bytes = [0u8; 12];
    let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet wrap_nonce");
    nonce_hasher.update(file_access.data_block_id.as_bytes());
    nonce_hasher.update(&file_access.user_id.to_le_bytes());
    nonce_hasher.update(file_access.ephemeral_pubkey.as_bytes());
    nonce_hasher.finalize_xof().fill(&mut nonce_bytes);
    let wrap_nonce = chacha20poly1305::Nonce::from(nonce_bytes);
    
    // Decrypt the per-file key
    let wrap_cipher = ChaCha20Poly1305::new(&wrap_key);
    let decrypted_file_key = wrap_cipher.decrypt(&wrap_nonce, file_access.encrypted_file_key.as_slice())
        .map_err(|e| format!("Decryption failed: {:?}", e))?;
    
    if decrypted_file_key.len() != 32 {
        return Err("Invalid decrypted key length".into());
    }
    
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&decrypted_file_key);
    Ok(chacha20poly1305::Key::from(key_bytes))
}