use crate::AppState;
#[cfg(target_os = "macos")]
use block2::RcBlock;
#[cfg(target_os = "macos")]
use objc2::AnyThread;
#[cfg(target_os = "macos")]
use objc2_file_provider::{
    NSFileProviderDomain, NSFileProviderItemIdentifier, NSFileProviderManager,
    NSFileProviderRootContainerItemIdentifier, NSFileProviderWorkingSetContainerItemIdentifier,
};
#[cfg(target_os = "macos")]
use objc2_foundation::NSString;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{error, info, warn};

// Test mode signal counter - tracks how many times FileProvider refresh was signaled
static TEST_SIGNAL_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Register HopNet FileProvider domain with the system
/// This makes HopNet appear in Finder's sidebar and enables FileProvider integration
#[cfg(target_os = "macos")]
pub async fn register_fileprovider_domain(
    username: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Registering HopNet FileProvider domain with macOS for user: {}",
        username
    );

    // Create domain identifier - this should be unique per app
    let domain_identifier = NSString::from_str("com.hopnet.fileprovider.domain");

    // Create display name for Finder sidebar using username
    let display_name = NSString::from_str(&format!("{}'s HopNet", username));

    // Create the FileProvider domain using the proper initializer
    let domain = unsafe {
        NSFileProviderDomain::initWithIdentifier_displayName(
            NSFileProviderDomain::alloc(),
            &domain_identifier,
            &display_name,
        )
    };

    // Get the default FileProvider manager
    let default_manager = unsafe { NSFileProviderManager::defaultManager() };
    tracing::debug!("Default FileProvider manager: {:?}", default_manager);

    // Create completion handler block with better error reporting
    let username_for_closure = username.to_string();
    let completion_handler = RcBlock::new(move |error: *mut objc2_foundation::NSError| {
        if error.is_null() {
            info!("✅ Successfully registered HopNet FileProvider domain");
            info!(
                "HopNet should now appear in Finder sidebar as '{}'s HopNet'",
                username_for_closure
            );

            // Debug: Check the domain state after registration
            let domain_id = objc2_foundation::NSString::from_str("com.hopnet.fileprovider.domain");
            let temp_display = objc2_foundation::NSString::from_str("temp");
            let temp_domain = unsafe {
                objc2_file_provider::NSFileProviderDomain::initWithIdentifier_displayName(
                    objc2_file_provider::NSFileProviderDomain::alloc(),
                    &domain_id,
                    &temp_display,
                )
            };

            if let Some(manager) = unsafe {
                objc2_file_provider::NSFileProviderManager::managerForDomain(&temp_domain)
            } {
                tracing::debug!("Post-registration manager state: {:?}", manager);
            } else {
                tracing::debug!("Post-registration: No manager found for domain");
            }
        } else {
            unsafe {
                let error_ref = &*error;
                let code = error_ref.code();
                let description = error_ref.localizedDescription();
                error!("❌ Failed to register FileProvider domain:");
                error!("   Error code: {}", code);
                error!("   Description: {:?}", description);
                error!("   Full error: {:?}", error_ref);

                // Provide specific guidance for common error codes
                match code {
                    -2001 => {
                        error!("💡 Error -2001 usually means:");
                        error!("   1. Extension bundle has signing issues");
                        error!("   2. Previous domain registration is stale");
                        error!("   3. Extension entitlements are incorrect");
                        error!("   Try: pluginkit -r -v com.hopnet.desktop.fileprovider");
                    }
                    -1000 => {
                        error!(
                            "💡 Error -1000 usually means the extension bundle is missing or malformed"
                        );
                    }
                    _ => {
                        error!(
                            "💡 Unknown FileProvider error. Check system logs for more details."
                        );
                    }
                }
            }
        }
    });

    // Register the domain using static method
    unsafe {
        NSFileProviderManager::addDomain_completionHandler(&domain, &completion_handler);
    }

    // After registering, try to reconnect (enable) the domain
    // This might help if the domain is created in a disconnected state
    tokio::spawn(async move {
        // Wait a moment for registration to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        if let Err(e) = reconnect_fileprovider_domain().await {
            tracing::warn!("Failed to reconnect FileProvider domain: {}", e);
        }
    });

    // Since this is async, we'll return success immediately
    // The actual result will be logged in the completion handler
    Ok(())
}

/// Attempt to reconnect (enable) the FileProvider domain
#[cfg(target_os = "macos")]
pub async fn reconnect_fileprovider_domain() -> Result<(), Box<dyn std::error::Error>> {
    let domain_identifier = NSString::from_str("com.hopnet.fileprovider.domain");
    let display_name = NSString::from_str("HopNet");

    let domain = unsafe {
        NSFileProviderDomain::initWithIdentifier_displayName(
            NSFileProviderDomain::alloc(),
            &domain_identifier,
            &display_name,
        )
    };

    if let Some(manager) = unsafe { NSFileProviderManager::managerForDomain(&domain) } {
        let completion_handler = RcBlock::new(|error: *mut objc2_foundation::NSError| {
            if error.is_null() {
                info!("✅ Successfully reconnected FileProvider domain");
            } else {
                unsafe {
                    warn!("Failed to reconnect FileProvider domain: {:?}", &*error);
                }
            }
        });

        unsafe {
            manager.reconnectWithCompletionHandler(&completion_handler);
        }
        info!("FileProvider reconnect initiated");
        Ok(())
    } else {
        Err("FileProvider domain not found".into())
    }
}

/// Remove HopNet's specific FileProvider domain from the system
/// This only removes our domain, not all domains system-wide
#[cfg(target_os = "macos")]
pub async fn unregister_fileprovider_domain() -> Result<(), Box<dyn std::error::Error>> {
    info!("Removing HopNet FileProvider domain");

    // Create our domain identifier - same as used in registration
    let domain_identifier = NSString::from_str("com.hopnet.fileprovider.domain");

    // We need a display name to create the domain object, but since we're removing it,
    // the exact display name shouldn't matter as the identifier is the key
    let display_name = NSString::from_str("HopNet");

    // Create the domain object to remove
    let domain = unsafe {
        NSFileProviderDomain::initWithIdentifier_displayName(
            NSFileProviderDomain::alloc(),
            &domain_identifier,
            &display_name,
        )
    };

    // Create completion handler block
    let completion_handler = RcBlock::new(|error: *mut objc2_foundation::NSError| {
        if error.is_null() {
            info!("✅ Successfully removed HopNet FileProvider domain");
        } else {
            unsafe {
                let error_ref = &*error;
                warn!(
                    "Failed to remove HopNet FileProvider domain: {:?}",
                    error_ref
                );
            }
        }
    });

    // Remove our specific domain (not all domains system-wide)
    unsafe {
        NSFileProviderManager::removeDomain_completionHandler(&domain, &completion_handler);
    }

    info!("HopNet FileProvider domain removal initiated successfully");
    Ok(())
}

/// Check if FileProvider domain is already registered
#[cfg(target_os = "macos")]
pub async fn is_domain_registered() -> bool {
    let _domain_identifier = NSString::from_str("com.hopnet.fileprovider.domain");

    // Use completion handler to check domains asynchronously
    // For now, return false as this requires more complex async handling
    // TODO: Implement proper async domain checking with getDomainsWithCompletionHandler
    false
}

/// Signal FileProvider that files have changed and incremental sync should occur
/// This triggers enumerateChanges rather than full re-enumeration
#[cfg(target_os = "macos")]
pub async fn signal_fileprovider_refresh(
    test_mode: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Signaling FileProvider for incremental sync (enumerateChanges)");

    // In test mode, just increment counter and skip actual macOS signaling
    if test_mode {
        let count = TEST_SIGNAL_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        info!("🧪 Test mode: FileProvider signal #{} recorded", count);
        return Ok(());
    }

    // Create the domain identifier
    let domain_identifier = NSString::from_str("com.hopnet.fileprovider.domain");
    let display_name = NSString::from_str("HopNet Storage");

    tracing::debug!(
        "Domain values: identifier={:?}, display_name={:?}",
        domain_identifier,
        display_name
    );

    // Create the domain
    let domain = unsafe {
        NSFileProviderDomain::initWithIdentifier_displayName(
            NSFileProviderDomain::alloc(),
            &domain_identifier,
            &display_name,
        )
    };

    // Log the domain object properties
    unsafe {
        let actual_identifier = domain.identifier();
        let actual_display_name = domain.displayName();
        tracing::debug!(
            "Created domain object: identifier={:?}, display_name={:?}",
            actual_identifier,
            actual_display_name
        );
    }

    // Get the manager for this domain
    let manager = unsafe { NSFileProviderManager::managerForDomain(&domain) };

    if manager.is_none() {
        tracing::debug!(
            "NSFileProviderManager::managerForDomain returned None for domain identifier: {:?}",
            domain_identifier
        );
        warn!("FileProvider manager not found - domain may not be registered");
        return Ok(()); // Don't error, just skip the signal
    }

    let manager = manager.unwrap();
    tracing::debug!("Successfully obtained FileProvider manager: {:?}", manager);

    // Signal the working set to trigger enumerateChanges instead of root container
    let working_set_identifier = unsafe { &*NSFileProviderWorkingSetContainerItemIdentifier };
    tracing::debug!(
        "Signaling working set container: {:?}",
        working_set_identifier
    );

    // Create completion handler
    let completion_handler = RcBlock::new(|error: *mut objc2_foundation::NSError| {
        if error.is_null() {
            tracing::debug!("signalEnumeratorForContainerItemIdentifier completion: SUCCESS");
            info!("✅ Successfully signaled FileProvider for incremental sync");
        } else {
            unsafe {
                let error_ref = &*error;
                tracing::debug!(
                    "signalEnumeratorForContainerItemIdentifier completion: FAILED - {:?}",
                    error_ref
                );
                warn!(
                    "Failed to signal FileProvider incremental sync: {:?}",
                    error_ref
                );
            }
        }
    });

    // Signal the working set enumerator to trigger incremental sync
    tracing::debug!(
        "Calling signalEnumeratorForContainerItemIdentifier with manager={:?}, working_set={:?}",
        manager,
        working_set_identifier
    );
    unsafe {
        manager.signalEnumeratorForContainerItemIdentifier_completionHandler(
            &working_set_identifier,
            &completion_handler,
        );
    }

    Ok(())
}

/// Get the current signal count (for testing)
pub fn get_signal_count() -> usize {
    TEST_SIGNAL_COUNT.load(Ordering::SeqCst)
}

/// Initialize FileProvider on app startup based on setup state
/// Only cleans up old domains if the app is uninitialized (new installation)
#[cfg(target_os = "macos")]
pub async fn initialize_fileprovider_on_startup(
    app_state: &AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Initializing FileProvider on startup");

    // Check if the app has been set up using the same logic as get_initial_setup
    match crate::db::setup::get_initial_setup(app_state.db_pool.get()) {
        Ok(axum::http::StatusCode::OK) => {
            // App is already set up - don't clean up domains as this could affect existing users
            info!("App is already initialized - skipping FileProvider domain cleanup");
            Ok(())
        }
        Ok(axum::http::StatusCode::NOT_FOUND) => {
            // App is not set up yet - clean up any stale domains from previous installations
            info!("App is not initialized yet - cleaning up any existing FileProvider domains");
            unregister_fileprovider_domain().await?;
            Ok(())
        }
        Ok(_) | Err(_) => {
            // Database error - log warning but don't fail startup
            warn!("Could not determine app setup state - skipping FileProvider initialization");
            Ok(())
        }
    }
}
