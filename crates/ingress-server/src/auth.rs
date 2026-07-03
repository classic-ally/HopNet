//! pocket-id OIDC (BFF pattern): confidential client, server-side session,
//! per-library group authorization.
//!
//! The browser never sees tokens — the auth-code + PKCE exchange happens
//! server-side and the result lands in an httpOnly session cookie. The token's
//! `groups` claim (pocket-id, via the `groups` scope) drives which libraries a
//! user may see.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{FromRequestParts, Query, State};
use axum::http::request::Parts;
use axum::response::Redirect;
use openidconnect::core::{
    CoreAuthDisplay, CoreAuthPrompt, CoreAuthenticationFlow, CoreErrorResponseType,
    CoreGenderClaim, CoreJsonWebKey, CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm,
    CoreProviderMetadata, CoreRevocableToken, CoreRevocationErrorResponse,
    CoreTokenIntrospectionResponse, CoreTokenType,
};
use openidconnect::{
    AdditionalClaims, AuthorizationCode, Client, ClientId, ClientSecret, CsrfToken,
    EmptyExtraTokenFields, EndpointMaybeSet, EndpointNotSet, EndpointSet, IdTokenFields, IssuerUrl,
    Nonce, OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope,
    StandardErrorResponse, StandardTokenResponse, TokenResponse,
};
use serde::{Deserialize, Serialize};
use tower_sessions::Session;

use crate::config::OidcConfig;
use crate::routes::AppError;

// --- custom claims: pocket-id's `groups` -----------------------------------

/// Additional claims we read beyond the OIDC standard set. pocket-id emits
/// `groups` (a string array) when the `groups` scope is granted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupsClaims {
    #[serde(default)]
    pub groups: Option<Vec<String>>,
}
impl AdditionalClaims for GroupsClaims {}

// Mirror of `core::CoreIdTokenFields` / `CoreTokenResponse`, swapping the empty
// additional-claims slot for `GroupsClaims`.
type GroupsIdTokenFields = IdTokenFields<
    GroupsClaims,
    EmptyExtraTokenFields,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
>;
type GroupsTokenResponse = StandardTokenResponse<GroupsIdTokenFields, CoreTokenType>;

/// Mirror of `core::CoreClient` with `GroupsClaims`/`GroupsTokenResponse`, in
/// the endpoint-typestate produced by `from_provider_metadata` (verified
/// against openidconnect 4.0.1: auth = Set, token/userinfo = MaybeSet).
pub type OidcClient = Client<
    GroupsClaims,
    CoreAuthDisplay,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJsonWebKey,
    CoreAuthPrompt,
    StandardErrorResponse<CoreErrorResponseType>,
    GroupsTokenResponse,
    CoreTokenIntrospectionResponse,
    CoreRevocableToken,
    CoreRevocationErrorResponse,
    EndpointSet,      // auth
    EndpointNotSet,   // device
    EndpointNotSet,   // introspection
    EndpointNotSet,   // revocation
    EndpointMaybeSet, // token
    EndpointMaybeSet, // userinfo
>;

/// Shared OIDC state (in the app state).
pub struct AuthContext {
    pub client: OidcClient,
    pub http: reqwest::Client,
    pub oidc: OidcConfig,
}

/// Discover the provider, build the confidential client. Called once at startup.
pub async fn build_auth(cfg: &OidcConfig) -> anyhow::Result<AuthContext> {
    // Never follow redirects on token/userinfo calls (SSRF hardening).
    let http = reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let issuer = IssuerUrl::new(cfg.issuer.clone())?;
    let meta = CoreProviderMetadata::discover_async(issuer, &http).await?;
    let client = OidcClient::from_provider_metadata(
        meta,
        ClientId::new(cfg.client_id.clone()),
        Some(ClientSecret::new(cfg.client_secret()?)),
    )
    .set_redirect_uri(RedirectUrl::new(cfg.redirect_uri.clone())?);
    Ok(AuthContext {
        client,
        http,
        oidc: cfg.clone(),
    })
}

// --- session shapes --------------------------------------------------------

/// The authenticated user, stored in the session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionUser {
    pub sub: String,
    pub username: Option<String>,
    pub email: Option<String>,
    pub groups: Vec<String>,
}

/// Pre-login CSRF/PKCE/nonce stash (secrets as their String form).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthFlow {
    pkce_verifier: String,
    csrf_state: String,
    nonce: String,
}

const SESSION_USER_KEY: &str = "user";
const SESSION_FLOW_KEY: &str = "auth_flow";

// --- handlers --------------------------------------------------------------

/// `GET /auth/login` — build the auth URL (PKCE + CSRF + nonce), stash the
/// secrets in the session, 302 to the provider.
pub async fn login(
    State(auth): State<Arc<AuthContext>>,
    session: Session,
) -> Result<Redirect, AppError> {
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let (auth_url, csrf_state, nonce) = auth
        .client
        .authorize_url(
            CoreAuthenticationFlow::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        .add_scope(Scope::new("profile".to_string()))
        .add_scope(Scope::new("email".to_string()))
        .add_scope(Scope::new("groups".to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    session
        .insert(
            SESSION_FLOW_KEY,
            AuthFlow {
                pkce_verifier: pkce_verifier.secret().clone(),
                csrf_state: csrf_state.secret().clone(),
                nonce: nonce.secret().clone(),
            },
        )
        .await
        .map_err(AppError::internal)?;
    Ok(Redirect::to(auth_url.as_str()))
}

#[derive(Deserialize)]
pub struct CallbackQuery {
    code: String,
    state: String,
}

/// `GET /auth/callback` — verify state, exchange code (PKCE), verify the ID
/// token nonce, extract claims + groups, establish the session.
pub async fn callback(
    State(auth): State<Arc<AuthContext>>,
    session: Session,
    Query(q): Query<CallbackQuery>,
) -> Result<Redirect, AppError> {
    let flow: AuthFlow = session
        .get(SESSION_FLOW_KEY)
        .await
        .map_err(AppError::internal)?
        .ok_or(AppError::Unauthorized)?;
    if flow.csrf_state != q.state {
        return Err(AppError::Unauthorized); // CSRF mismatch
    }
    let _ = session.remove::<AuthFlow>(SESSION_FLOW_KEY).await;

    let token_response = auth
        .client
        .exchange_code(AuthorizationCode::new(q.code))
        .map_err(AppError::internal)?
        .set_pkce_verifier(PkceCodeVerifier::new(flow.pkce_verifier))
        .request_async(&auth.http)
        .await
        .map_err(AppError::internal)?;

    let id_token = token_response.id_token().ok_or(AppError::Unauthorized)?;
    let verifier = auth.client.id_token_verifier();
    let nonce = Nonce::new(flow.nonce);
    // `claims()` performs the security-critical validation: JWS signature
    // against the provider JWKS + nonce replay check.
    let claims = id_token
        .claims(&verifier, &nonce)
        .map_err(AppError::internal)?;

    // groups: prefer the ID token; fall back to UserInfo if absent/empty.
    let mut groups = claims
        .additional_claims()
        .groups
        .clone()
        .unwrap_or_default();
    if groups.is_empty()
        && let Ok(info) = auth
            .client
            .user_info(token_response.access_token().clone(), None)
            .map_err(AppError::internal)?
            .request_async(&auth.http)
            .await
    {
        let info: openidconnect::UserInfoClaims<GroupsClaims, CoreGenderClaim> = info;
        groups = info.additional_claims().groups.clone().unwrap_or_default();
    }

    let user = SessionUser {
        sub: claims.subject().to_string(),
        username: claims.preferred_username().map(|u| u.as_str().to_string()),
        email: claims.email().map(|e| e.as_str().to_string()),
        groups,
    };
    tracing::info!(sub = %user.sub, groups = ?user.groups, "login ok");

    session.cycle_id().await.map_err(AppError::internal)?; // session-fixation defense
    session
        .insert(SESSION_USER_KEY, user)
        .await
        .map_err(AppError::internal)?;
    Ok(Redirect::to("/"))
}

/// `GET /auth/logout` — clear the server-side session and cookie.
pub async fn logout(State(auth): State<Arc<AuthContext>>, session: Session) -> Redirect {
    let _ = session.flush().await;
    let dest = auth
        .oidc
        .post_logout_redirect_uri
        .clone()
        .unwrap_or_else(|| "/".to_string());
    Redirect::to(&dest)
}

// --- extractor + authorization ---------------------------------------------

/// Extracts the authenticated user; a missing/expired session is a 401 (the
/// frontend turns that into a redirect to `/auth/login`).
pub struct AuthUser(pub SessionUser);

impl<S: Send + Sync> FromRequestParts<S> for AuthUser {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let session = Session::from_request_parts(parts, state)
            .await
            .map_err(|_| AppError::Unauthorized)?;
        let user: Option<SessionUser> = session
            .get(SESSION_USER_KEY)
            .await
            .map_err(AppError::internal)?;
        user.map(AuthUser).ok_or(AppError::Unauthorized)
    }
}

/// library_id → allowed groups (built from config `LibraryAccess`).
pub type AccessRules = HashMap<String, Vec<String>>;

/// A user may access a library iff their groups intersect its allowed set. An
/// unlisted library denies by default.
pub fn can_access(rules: &AccessRules, user: &SessionUser, library_id: &str) -> bool {
    match rules.get(library_id) {
        Some(allowed) => allowed.iter().any(|g| user.groups.iter().any(|ug| ug == g)),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(groups: &[&str]) -> SessionUser {
        SessionUser {
            sub: "u1".into(),
            username: None,
            email: None,
            groups: groups.iter().map(|s| s.to_string()).collect(),
        }
    }

    // Impact: a wrong `false` locks users out; a wrong `true` leaks another
    // user's library — this is the whole authorization boundary.
    // Should: grant only when the user's groups intersect the library's set.
    // Should not: grant on empty user groups, an unlisted library, or an
    // empty allow-list.
    #[test]
    fn access_intersection_truth_table() {
        let mut rules = AccessRules::new();
        rules.insert("shared".into(), vec!["shared_photo_library".into()]);
        rules.insert("personal".into(), vec!["allisons_photo_library".into()]);
        rules.insert("locked".into(), vec![]); // no group can reach it

        let allison = user(&["allisons_photo_library", "shared_photo_library"]);
        let guest = user(&["shared_photo_library"]);
        let nobody = user(&[]);

        assert!(can_access(&rules, &allison, "personal"));
        assert!(can_access(&rules, &allison, "shared"));
        assert!(can_access(&rules, &guest, "shared"));
        assert!(!can_access(&rules, &guest, "personal")); // not in the group
        assert!(!can_access(&rules, &nobody, "shared")); // no groups
        assert!(!can_access(&rules, &allison, "locked")); // empty allow-list
        assert!(!can_access(&rules, &allison, "unlisted")); // deny by default
    }
}
