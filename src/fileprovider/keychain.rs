//! Keychain operations for HopNet FileProvider configuration
//! Stores API key and base URL securely for FileProvider extension access

#[cfg(target_os = "macos")]
use security_framework::base::Error as SecurityError;
#[cfg(target_os = "macos")]
use security_framework::os::macos::keychain::SecKeychain;
#[cfg(target_os = "macos")]
use security_framework::os::macos::passwords::find_generic_password;
use std::error::Error;
use std::fmt;
use tracing::{error, info, warn};

/// Keychain service names
const HOPNET_SERVICE: &str = "com.hopnet.desktop.fileprovider";
const HOPNET_TEST_SERVICE: &str = "com.hopnet.desktop.fileprovider.test";
const SESSION_SERVICE: &str = "com.hopnet.desktop.session";
const SESSION_TEST_SERVICE: &str = "com.hopnet.desktop.session.test";
/// Photo-ingress daemon credentials (read Swift-side by
/// `PhotoIngressKit/PublishCredentials.swift`).
const PHOTO_INGRESS_SERVICE: &str = "com.hopnet.desktop.photo-ingress";

/// Keychain account names
const API_KEY_ACCOUNT: &str = "api_key";
const BASE_URL_ACCOUNT: &str = "base_url";
const SESSION_PRIVKEY_ACCOUNT: &str = "owner_privkey";
const SESSION_USERID_ACCOUNT: &str = "owner_user_id";
/// Photo-ingress library provisioning (enablement flow): the daemon's
/// startup auto-bind reads these alongside the credentials.
const BLOB_ROOT_ACCOUNT: &str = "blob_root";
const SIDECAR_ROOT_REMOTE_ACCOUNT: &str = "sidecar_root_remote";

/// Keychain environment configuration
#[derive(Debug, Clone, Copy)]
pub enum KeychainEnvironment {
    Production,
    Test,
}

impl KeychainEnvironment {
    fn service_name(&self) -> &'static str {
        match self {
            KeychainEnvironment::Production => HOPNET_SERVICE,
            KeychainEnvironment::Test => HOPNET_TEST_SERVICE,
        }
    }

    fn session_service_name(&self) -> &'static str {
        match self {
            KeychainEnvironment::Production => SESSION_SERVICE,
            KeychainEnvironment::Test => SESSION_TEST_SERVICE,
        }
    }
}

#[derive(Debug)]
pub enum KeychainError {
    #[cfg(target_os = "macos")]
    SecurityFramework(SecurityError),
    ItemNotFound,
    InvalidData,
}

impl fmt::Display for KeychainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(target_os = "macos")]
            KeychainError::SecurityFramework(e) => write!(f, "Security framework error: {}", e),
            KeychainError::ItemNotFound => write!(f, "Keychain item not found"),
            KeychainError::InvalidData => write!(f, "Invalid keychain data"),
        }
    }
}

impl Error for KeychainError {}

#[cfg(target_os = "macos")]
impl From<SecurityError> for KeychainError {
    fn from(err: SecurityError) -> Self {
        KeychainError::SecurityFramework(err)
    }
}

/// FileProvider configuration stored in keychain
#[derive(Debug, Clone)]
pub struct FileProviderConfig {
    pub api_key: String,
    pub base_url: String,
}

impl FileProviderConfig {
    pub fn new(api_key: String, base_url: String) -> Self {
        Self { api_key, base_url }
    }
}

/// Store FileProvider configuration in the keychain
#[cfg(target_os = "macos")]
pub fn store_config(
    config: &FileProviderConfig,
    env: KeychainEnvironment,
) -> Result<(), KeychainError> {
    info!(
        "Storing FileProvider configuration in keychain (env: {:?})",
        env
    );

    // Store API key
    store_keychain_item(env, API_KEY_ACCOUNT, &config.api_key)?;

    // Store base URL
    store_keychain_item(env, BASE_URL_ACCOUNT, &config.base_url)?;

    info!("FileProvider configuration stored successfully");
    Ok(())
}

/// Load FileProvider configuration from the keychain
#[cfg(target_os = "macos")]
pub fn load_config(env: KeychainEnvironment) -> Result<FileProviderConfig, KeychainError> {
    info!(
        "Loading FileProvider configuration from keychain (env: {:?})",
        env
    );

    let api_key = load_keychain_item(env, API_KEY_ACCOUNT)?;
    let base_url = load_keychain_item(env, BASE_URL_ACCOUNT)?;

    Ok(FileProviderConfig::new(api_key, base_url))
}

/// Store a single item in the keychain
#[cfg(target_os = "macos")]
fn store_keychain_item(
    env: KeychainEnvironment,
    account: &str,
    value: &str,
) -> Result<(), KeychainError> {
    let service = env.service_name();
    tracing::debug!(
        "Storing keychain item: service={}, account={}",
        service,
        account
    );

    let keychain = SecKeychain::default()?;
    tracing::debug!("Using default keychain");

    // Use set_generic_password which handles both create and update
    match keychain.set_generic_password(service, account, value.as_bytes()) {
        Ok(_) => {
            tracing::debug!("Successfully stored keychain item for account: {}", account);
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to store keychain item: {}", e);
            Err(e.into())
        }
    }
}

/// Load a single item from the keychain
#[cfg(target_os = "macos")]
fn load_keychain_item(env: KeychainEnvironment, account: &str) -> Result<String, KeychainError> {
    let service = env.service_name();
    tracing::debug!(
        "Loading keychain item: service={}, account={}",
        service,
        account
    );

    let keychain = SecKeychain::default()?;
    tracing::debug!("Using default keychain");

    match keychain.find_generic_password(service, account) {
        Ok((password_data, _item)) => {
            tracing::debug!("Successfully loaded keychain item for account: {}", account);

            String::from_utf8(password_data.as_ref().to_vec())
                .map_err(|_| KeychainError::InvalidData)
        }
        Err(e) => {
            tracing::error!(
                "Failed to load keychain item for account {}: {}",
                account,
                e
            );
            Err(e.into())
        }
    }
}

/// Update only the stored base URL. Used at GUI startup when the backend
/// binds an ephemeral loopback port — the FileProvider extension reads
/// `base_url` from this keychain entry and would otherwise still point at
/// the previous run's port.
#[cfg(target_os = "macos")]
pub fn update_base_url(base_url: &str, env: KeychainEnvironment) -> Result<(), KeychainError> {
    store_keychain_item(env, BASE_URL_ACCOUNT, base_url)
}

/// Store the photo-ingress daemon's publish credentials (device token +
/// node base URL). Same account names as the FileProvider service so the
/// Swift reader stays symmetric.
#[cfg(target_os = "macos")]
pub fn store_photo_ingress_config(api_key: &str, base_url: &str) -> Result<(), KeychainError> {
    store_keychain_item_with_service(PHOTO_INGRESS_SERVICE, API_KEY_ACCOUNT, api_key.as_bytes())?;
    store_keychain_item_with_service(PHOTO_INGRESS_SERVICE, BASE_URL_ACCOUNT, base_url.as_bytes())
}

/// Load the photo-ingress credentials: `(api_key, base_url)`.
#[cfg(target_os = "macos")]
pub fn load_photo_ingress_config() -> Result<(String, String), KeychainError> {
    let api_key = load_keychain_item_bytes_with_service(PHOTO_INGRESS_SERVICE, API_KEY_ACCOUNT)?;
    let base_url = load_keychain_item_bytes_with_service(PHOTO_INGRESS_SERVICE, BASE_URL_ACCOUNT)?;
    Ok((
        String::from_utf8(api_key).map_err(|_| KeychainError::InvalidData)?,
        String::from_utf8(base_url).map_err(|_| KeychainError::InvalidData)?,
    ))
}

/// Refresh only the photo-ingress base URL (GUI ephemeral-port startup).
#[cfg(target_os = "macos")]
pub fn update_photo_ingress_base_url(base_url: &str) -> Result<(), KeychainError> {
    store_keychain_item_with_service(PHOTO_INGRESS_SERVICE, BASE_URL_ACCOUNT, base_url.as_bytes())
}

/// Store the library provisioning values the daemon's startup auto-bind
/// reads. A None sidecar root removes any previous value so a re-enable
/// without one doesn't resurrect a stale path.
#[cfg(target_os = "macos")]
pub fn store_photo_ingress_provisioning(
    blob_root: &str,
    sidecar_root_remote: Option<&str>,
) -> Result<(), KeychainError> {
    store_keychain_item_with_service(PHOTO_INGRESS_SERVICE, BLOB_ROOT_ACCOUNT, blob_root.as_bytes())?;
    match sidecar_root_remote {
        Some(remote) => store_keychain_item_with_service(
            PHOTO_INGRESS_SERVICE,
            SIDECAR_ROOT_REMOTE_ACCOUNT,
            remote.as_bytes(),
        ),
        None => {
            let _ = remove_keychain_item_with_service(
                PHOTO_INGRESS_SERVICE,
                SIDECAR_ROOT_REMOTE_ACCOUNT,
            );
            Ok(())
        }
    }
}

/// Load the provisioned blob root (status display; the BOUND value lives in
/// the daemon's state.db and wins on divergence).
#[cfg(target_os = "macos")]
pub fn load_photo_ingress_blob_root() -> Result<String, KeychainError> {
    let bytes = load_keychain_item_bytes_with_service(PHOTO_INGRESS_SERVICE, BLOB_ROOT_ACCOUNT)?;
    String::from_utf8(bytes).map_err(|_| KeychainError::InvalidData)
}

/// Remove the full photo-ingress provisioning (disable flow). Tolerates
/// absent items — disable is best-effort/idempotent per step.
#[cfg(target_os = "macos")]
pub fn remove_photo_ingress_config() {
    info!("Removing photo-ingress configuration from keychain");
    let _ = remove_keychain_item_with_service(PHOTO_INGRESS_SERVICE, API_KEY_ACCOUNT);
    let _ = remove_keychain_item_with_service(PHOTO_INGRESS_SERVICE, BASE_URL_ACCOUNT);
    let _ = remove_keychain_item_with_service(PHOTO_INGRESS_SERVICE, BLOB_ROOT_ACCOUNT);
    let _ = remove_keychain_item_with_service(PHOTO_INGRESS_SERVICE, SIDECAR_ROOT_REMOTE_ACCOUNT);
}

/// Remove FileProvider configuration from keychain
#[cfg(target_os = "macos")]
pub fn remove_config(env: KeychainEnvironment) -> Result<(), KeychainError> {
    info!(
        "Removing FileProvider configuration from keychain (env: {:?})",
        env
    );

    // Remove both items, don't fail if they don't exist
    let _ = remove_keychain_item(env, API_KEY_ACCOUNT);
    let _ = remove_keychain_item(env, BASE_URL_ACCOUNT);

    info!("FileProvider configuration removed");
    Ok(())
}

/// Remove a single item from the keychain
#[cfg(target_os = "macos")]
fn remove_keychain_item(env: KeychainEnvironment, account: &str) -> Result<(), KeychainError> {
    let service = env.service_name();
    let keychain = SecKeychain::default()?;
    let (_password_data, item) = keychain.find_generic_password(service, account)?;

    item.delete();
    Ok(())
}

// ============================================================================
// Session key storage (owner auto-login)
// ============================================================================

/// Store a keychain item with a specific service name and binary data
#[cfg(target_os = "macos")]
fn store_keychain_item_with_service(
    service: &str,
    account: &str,
    value: &[u8],
) -> Result<(), KeychainError> {
    let keychain = SecKeychain::default()?;
    keychain
        .set_generic_password(service, account, value)
        .map_err(KeychainError::SecurityFramework)
}

/// Load a keychain item with a specific service name, returning raw bytes
#[cfg(target_os = "macos")]
fn load_keychain_item_bytes_with_service(
    service: &str,
    account: &str,
) -> Result<Vec<u8>, KeychainError> {
    let keychain = SecKeychain::default()?;
    match keychain.find_generic_password(service, account) {
        Ok((password_data, _item)) => Ok(password_data.as_ref().to_vec()),
        Err(e) => Err(e.into()),
    }
}

/// Remove a keychain item with a specific service name
#[cfg(target_os = "macos")]
fn remove_keychain_item_with_service(service: &str, account: &str) -> Result<(), KeychainError> {
    let keychain = SecKeychain::default()?;
    let (_password_data, item) = keychain.find_generic_password(service, account)?;
    item.delete();
    Ok(())
}

/// Store the node owner's session key in keychain for auto-login on restart
#[cfg(target_os = "macos")]
pub fn store_session_key(
    env: KeychainEnvironment,
    user_id: i32,
    privkey_bytes: &[u8],
) -> Result<(), KeychainError> {
    let service = env.session_service_name();
    store_keychain_item_with_service(service, SESSION_PRIVKEY_ACCOUNT, privkey_bytes)?;
    store_keychain_item_with_service(service, SESSION_USERID_ACCOUNT, &user_id.to_le_bytes())?;
    Ok(())
}

/// Load the node owner's session key from keychain for auto-login
#[cfg(target_os = "macos")]
pub fn load_session_key(env: KeychainEnvironment) -> Result<(i32, Vec<u8>), KeychainError> {
    let service = env.session_service_name();
    let privkey_bytes = load_keychain_item_bytes_with_service(service, SESSION_PRIVKEY_ACCOUNT)?;
    let userid_bytes = load_keychain_item_bytes_with_service(service, SESSION_USERID_ACCOUNT)?;
    if userid_bytes.len() != 4 {
        return Err(KeychainError::InvalidData);
    }
    let user_id = i32::from_le_bytes(userid_bytes.try_into().unwrap());
    Ok((user_id, privkey_bytes))
}

/// Remove the node owner's session key from keychain (logout)
#[cfg(target_os = "macos")]
pub fn remove_session_key(env: KeychainEnvironment) -> Result<(), KeychainError> {
    let service = env.session_service_name();
    let _ = remove_keychain_item_with_service(service, SESSION_PRIVKEY_ACCOUNT);
    let _ = remove_keychain_item_with_service(service, SESSION_USERID_ACCOUNT);
    Ok(())
}
