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

use crate::db;
use crate::{AppState, PrivKey};

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
    match db::users::get_user_by_userid(&app_state.db, uid) {
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
    match db::users::get_user_by_username(&app_state.db, user_data.username) {
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