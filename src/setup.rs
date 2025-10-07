use axum::{
    extract::State, 
    response::IntoResponse,
    http::StatusCode,
    Json
};
use serde::{Serialize,Deserialize};
use crate::consensus::functions::generate_ed25519_key;
use crate::{PubKey, PrivKey, UserKeys};

use crate::consensus::types::{ConsensusPhase, Block, VoteSignMessages};
use crate::consensus::QuorumCertificate;
use crate::db::Sequence;
use crate::AppState;
use crate::{
    db::setup,
    db::User,
};
use crate::db::{DataRecord, FragmentHash, Inode};
use crate::types::{Blake3Hash, Node};

// Re-export from common crate for API consistency
pub use hopnet_common::setup::InitialSetupPayload;

pub async fn get_setup(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match setup::get_initial_setup(app_state.db_pool.get()) {
        Ok(setupstatus) => (setupstatus, app_state.public_key),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, app_state.public_key)
    }
}

/// New catch-up based join handler - receives JoinInfo and bootstraps via catch-up
/// Returns immediately (202 ACCEPTED) while catch-up runs in background
pub async fn put_join_bootstrap(
    State(app_state): State<AppState>,
    Json(join_info): Json<crate::types::JoinInfo>
) -> Result<StatusCode, StatusCode> {
    tracing::info!(
        "PUT /setup (catch-up based) called - joining as node_id={}, user_id={}",
        join_info.node_id,
        join_info.user_id
    );

    // Derive user public key from private key
    let user_pub_key = PubKey(join_info.user_privkey.verifying_key());

    // Set up UserKeys in app state
    let user_keys = UserKeys {
        private_key: join_info.user_privkey.clone(),
        public_key: user_pub_key,
    };

    app_state.user_keys.set(user_keys)
        .map_err(|_| {
            tracing::error!("Failed to set user keys in app state - already initialized");
            StatusCode::CONFLICT
        })?;

    // Set user_id and node_id in app state
    app_state.user_id.set(join_info.user_id)
        .map_err(|_| StatusCode::CONFLICT)?;

    app_state.node_id.set(join_info.node_id)
        .map_err(|_| StatusCode::CONFLICT)?;

    // Initialize SIV keys from user private key
    app_state.initialize_siv_keys()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Initialize this_node table with identity and keys
    // Sequences, users, nodes, validators will come from genesis replay
    setup::initialize_joining_node(
        app_state.db_pool.get(),
        join_info.clone(),
        app_state.private_key.clone(),
    ).map_err(|e| {
        tracing::error!("Failed to initialize joining node database: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

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
            true,  // request_activation_if_needed
            0,     // tolerance_views (ignored for Convergence mode)
            Some(&join_info_clone.bootstrap_validators)
        ).await {
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

    // Return 202 ACCEPTED - request accepted, processing asynchronously
    Ok(StatusCode::ACCEPTED)
}

// full setup from scratch
pub async fn post_setup(
    State(app_state): State<AppState>,
    Json(payload): Json<InitialSetupPayload>
) -> Result<StatusCode, StatusCode> {

    let (user_priv_key, user_pub_key) = generate_ed25519_key();
    
    let user_keys = UserKeys {
        private_key: PrivKey(user_priv_key),
        public_key: PubKey(user_pub_key),
    };
    
    // Set user keys in app state (can only be done once)
    app_state.user_keys.set(user_keys.clone())
        .map_err(|_| StatusCode::CONFLICT)?; // Already initialized
    
    // Initialize SIV keys from user private key
    app_state.initialize_siv_keys()?;

    // Construct User and Node from the simplified payload
    let x25519_pubkey = crate::auth::derive_x25519_pubkey_from_user(&user_keys.private_key);
    // Capture username before moving payload fields
    #[cfg(target_os = "macos")]
    let username_for_fileprovider = payload.username.clone();
    
    let user = User::new_with_password(
        0, // Will be generated by the database
        payload.username,
        payload.password,
        user_keys.public_key,
        x25519_pubkey,
    ).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let node = Node {
        node_id: 0, // Will be generated by the database
        name: payload.node_name,
        ip_address: payload.ip_address,
        port: payload.port,
        owner: 0, // Will be set to the generated user_id
        pubkey: app_state.public_key, // Placeholder, will use app_state.public_key
    };

    match setup::post_initial_setup(&app_state, user, node, user_keys.private_key) {
        Ok((user_id, node_id)) => {
            // Set the generated user_id and node_id in app state
            app_state.user_id.set(user_id)
                .map_err(|_| StatusCode::CONFLICT)?; // Already initialized
            app_state.node_id.set(node_id)
                .map_err(|_| StatusCode::CONFLICT)?; // Already initialized
            
            // Register FileProvider domain with macOS using the setup username
            // Skip registration in test mode since we test FileProvider functionality directly
            #[cfg(target_os = "macos")]
            {
                if !app_state.test_mode {
                    let username = username_for_fileprovider;
                    tokio::spawn(async move {
                        if let Err(e) = crate::fileprovider::domain::register_fileprovider_domain(&username).await {
                            tracing::warn!("Failed to register FileProvider domain: {}", e);
                        } else {
                            tracing::info!("FileProvider domain registration completed");
                        }
                    });
                } else {
                    tracing::info!("Skipping FileProvider domain registration in test mode");
                }
            }
            
            Ok(StatusCode::CREATED)
        },
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
