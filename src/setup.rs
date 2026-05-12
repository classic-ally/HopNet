use crate::consensus::functions::generate_ed25519_key;
use crate::{PrivKey, PubKey, UserKeys};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::types::Node;
use crate::{db::User, db::setup};

// Re-export from common crate for API consistency
pub use hopnet_common::setup::InitialSetupPayload;

// ============================================================================
// JoinDeliver RPC types (used by coordinator → joining node over iroh)
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinDeliverRequest {
    pub join_info: crate::types::JoinInfo,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinAckResponse {
    pub success: bool,
}

pub async fn get_setup(State(app_state): State<AppState>) -> impl IntoResponse {
    match setup::get_initial_setup(app_state.db_pool.get()) {
        Ok(setupstatus) => (setupstatus, app_state.public_key),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, app_state.public_key),
    }
}

/// Process JoinInfo received from the coordinator (called from iroh handler).
/// Initializes app state, database, marks setup complete, and spawns catch-up.
pub async fn process_join_info(
    app_state: &AppState,
    join_info: crate::types::JoinInfo,
) -> Result<(), String> {
    tracing::info!(
        "Processing JoinInfo - joining as node_id={}, user_id={}",
        join_info.node_id,
        join_info.user_id
    );

    // Set user_id and node_id in app state
    app_state
        .user_id
        .set(join_info.user_id)
        .map_err(|_| "Failed to set user_id - already initialized".to_string())?;

    app_state
        .node_id
        .set(join_info.node_id)
        .map_err(|_| "Failed to set node_id - already initialized".to_string())?;

    // Initialize this_node table with identity and keys
    // Sequences, users, nodes, validators will come from genesis replay
    setup::initialize_joining_node(
        app_state.db_pool.get(),
        join_info.clone(),
        app_state.private_key.clone(),
    )
    .map_err(|e| format!("Failed to initialize joining node database: {:?}", e))?;

    // Mark setup complete — PeerValidator switches to strict mode
    app_state.iroh_transport.mark_setup_complete();

    // Spawn catch-up as background task
    let app_state_clone = app_state.clone();
    let join_info_clone = join_info.clone();
    tokio::spawn(async move {
        tracing::info!(
            "Starting background catch-up from view 0 using {} bootstrap validators",
            join_info_clone.bootstrap_validators.len()
        );

        use crate::consensus::routes::CatchUpMode;
        match crate::consensus::routes::ensure_caught_up_and_active(
            &app_state_clone,
            CatchUpMode::Convergence,
            true, // request_activation_if_needed
            0,    // tolerance_views (ignored for Convergence mode)
            Some(&join_info_clone.bootstrap_validators),
        )
        .await
        {
            Ok(readiness) => {
                if readiness.is_active {
                    tracing::info!(
                        "Catch-up and activation completed successfully for node {}",
                        join_info_clone.node_id
                    );
                } else {
                    tracing::info!(
                        "Catch-up completed for node {}, activation request submitted",
                        join_info_clone.node_id
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    "Catch-up failed for node {}: {:?}. Node will remain inactive.",
                    join_info_clone.node_id,
                    e
                );
            }
        }
    });

    tracing::info!(
        "Node {} setup initiated, catch-up running in background",
        join_info.node_id
    );

    Ok(())
}

// full setup from scratch (genesis node)
pub async fn post_setup(
    State(app_state): State<AppState>,
    Json(payload): Json<InitialSetupPayload>,
) -> Result<(StatusCode, Json<hopnet_common::setup::PassphraseResponse>), StatusCode> {
    let passphrase = crate::passphrase::generate_passphrase();

    let (user_priv_key, user_pub_key) = generate_ed25519_key();

    let user_keys = UserKeys {
        private_key: PrivKey(user_priv_key),
        public_key: PubKey(user_pub_key),
    };

    // Construct User and Node from the simplified payload
    let x25519_pubkey = crate::auth::derive_x25519_pubkey_from_user(&user_keys.private_key);

    // Wrap the user private key with the passphrase (3-5s Argon2id)
    let passphrase_clone = passphrase.clone();
    let privkey_clone = user_keys.private_key.clone();
    let wrap_result = tokio::task::spawn_blocking(move || {
        crate::auth::wrap_user_privkey(&privkey_clone, &passphrase_clone).map_err(|e| e.to_string())
    })
    .await;

    let (encrypted_privkey, key_salt) = match wrap_result {
        Ok(Ok(result)) => result,
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Capture username before moving payload fields
    #[cfg(target_os = "macos")]
    let username_for_fileprovider = payload.username.clone();

    let user = User::new(
        0, // Will be generated by the database
        payload.username,
        user_keys.public_key,
        x25519_pubkey,
        encrypted_privkey,
        key_salt,
    );

    let node = Node {
        node_id: 0, // Will be generated by the database
        name: payload.node_name,
        owner: 0, // Will be set to the generated user_id
        pubkey: app_state.public_key,
    };

    match setup::post_initial_setup(&app_state, user, node) {
        Ok((user_id, node_id)) => {
            // Set the generated user_id and node_id in app state
            app_state
                .user_id
                .set(user_id)
                .map_err(|_| StatusCode::CONFLICT)?; // Already initialized
            app_state
                .node_id
                .set(node_id)
                .map_err(|_| StatusCode::CONFLICT)?; // Already initialized

            // Populate session store for genesis session (no expiry)
            let (siv_key, siv_nonce) =
                crate::auth::derive_siv_key_from_user(&user_keys.private_key, "file_path");
            let session = crate::auth::SessionEntry {
                user_keys: user_keys.clone(),
                siv_key,
                siv_nonce,
                expires_at: chrono::Utc::now() + chrono::Duration::hours(876000),
            };
            {
                app_state
                    .session_store
                    .write()
                    .await
                    .insert(user_id, session);
            }

            // Mark setup complete — PeerValidator switches to strict mode
            app_state.iroh_transport.mark_setup_complete();

            // Register FileProvider domain with macOS using the setup username
            // Skip registration in test mode since we test FileProvider functionality directly
            #[cfg(target_os = "macos")]
            {
                if !app_state.test_mode {
                    let username = username_for_fileprovider;
                    tokio::spawn(async move {
                        if let Err(e) =
                            crate::fileprovider::domain::register_fileprovider_domain(&username)
                                .await
                        {
                            tracing::warn!("Failed to register FileProvider domain: {}", e);
                        } else {
                            tracing::info!("FileProvider domain registration completed");
                        }
                    });
                } else {
                    tracing::info!("Skipping FileProvider domain registration in test mode");
                }
            }

            // Register a FileProvider device token so the extension can authenticate
            #[cfg(all(target_os = "macos", not(debug_assertions)))]
            {
                if !app_state.test_mode {
                    let app_state_clone = app_state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = crate::devices::routes::ensure_fileprovider_device_token(
                            &app_state_clone,
                            user_id,
                        )
                        .await
                        {
                            tracing::warn!(
                                "Failed to register FileProvider device token at setup: {:?}",
                                e
                            );
                        }
                    });
                }
            }

            Ok((
                StatusCode::CREATED,
                Json(hopnet_common::setup::PassphraseResponse { passphrase }),
            ))
        }
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
