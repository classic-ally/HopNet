//! Keychain operations for HopNet FileProvider configuration
//! Stores API key and base URL securely for FileProvider extension access

#[cfg(target_os = "macos")]
use security_framework::os::macos::keychain::SecKeychain;
#[cfg(target_os = "macos")]
use security_framework::os::macos::passwords::find_generic_password;
#[cfg(target_os = "macos")]
use security_framework::base::Error as SecurityError;
use std::error::Error;
use std::fmt;
use tracing::{info, warn, error};

/// Keychain service names
const HOPNET_SERVICE: &str = "com.hopnet.desktop.fileprovider";
const HOPNET_TEST_SERVICE: &str = "com.hopnet.desktop.fileprovider.test";

/// Keychain account names
const API_KEY_ACCOUNT: &str = "api_key";
const BASE_URL_ACCOUNT: &str = "base_url";

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
pub fn store_config(config: &FileProviderConfig, env: KeychainEnvironment) -> Result<(), KeychainError> {
    info!("Storing FileProvider configuration in keychain (env: {:?})", env);
    
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
    info!("Loading FileProvider configuration from keychain (env: {:?})", env);
    
    let api_key = load_keychain_item(env, API_KEY_ACCOUNT)?;
    let base_url = load_keychain_item(env, BASE_URL_ACCOUNT)?;
    
    Ok(FileProviderConfig::new(api_key, base_url))
}

/// Store a single item in the keychain
#[cfg(target_os = "macos")]
fn store_keychain_item(env: KeychainEnvironment, account: &str, value: &str) -> Result<(), KeychainError> {
    let service = env.service_name();
    tracing::debug!("Storing keychain item: service={}, account={}", service, account);
    
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
    tracing::debug!("Loading keychain item: service={}, account={}", service, account);
    
    let keychain = SecKeychain::default()?;
    tracing::debug!("Using default keychain");
    
    match keychain.find_generic_password(service, account) {
        Ok((password_data, _item)) => {
            tracing::debug!("Successfully loaded keychain item for account: {}", account);
            
            String::from_utf8(password_data.as_ref().to_vec())
                .map_err(|_| KeychainError::InvalidData)
        }
        Err(e) => {
            tracing::error!("Failed to load keychain item for account {}: {}", account, e);
            Err(e.into())
        }
    }
}

/// Remove FileProvider configuration from keychain
#[cfg(target_os = "macos")]
pub fn remove_config(env: KeychainEnvironment) -> Result<(), KeychainError> {
    info!("Removing FileProvider configuration from keychain (env: {:?})", env);
    
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