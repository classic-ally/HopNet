use crate::consensus::dispatch::generate_ed25519_key;
use crate::{PrivKey, PubKey, UserKeys};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::types::Node;
use crate::{db::User, db::setup};

// Re-export from common crate for API consistency
pub use hopnet_common::setup::InitialSetupPayload;

// ============================================================================
// "setup" scope wire types (coordinator → joining node over the mesh). This
// module owns the payload codec; the server side lives in
// `net::scopes::SetupScope`.
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinDeliverRequest {
    pub join_info: crate::types::JoinInfo,
}

/// Wire request for the "setup" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum SetupRequest {
    /// Deliver JoinInfo to a joining node (coordinator → new node)
    JoinDeliver(JoinDeliverRequest),
}

/// Wire response for the "setup" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum SetupResponse {
    /// Ack for JoinDeliver
    JoinAck {
        success: bool,
    },
    Error {
        message: String,
    },
}

pub async fn get_setup(State(app_state): State<AppState>) -> impl IntoResponse {
    match setup::get_initial_setup(app_state.db_pool.get()) {
        Ok(setupstatus) => (setupstatus, app_state.public_key),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, app_state.public_key),
    }
}

/// Why a join-code entry was refused.
#[derive(Debug)]
pub enum JoinCodeError {
    /// The node is already set up — set-up nodes bind their magic at
    /// boot and never take this path.
    AlreadySetUp,
    /// A DIFFERENT code was already adopted this process lifetime;
    /// restart to re-enter.
    Conflict,
    /// The comms adoption failed (mirrors Conflict at the transport).
    Comms(String),
}

/// The join-code entry seam (RFC-025 S5, §Settled Questions #2): the
/// interactive endpoint and HOPNET_JOIN_CODE both land here. Fresh
/// nodes only; idempotent for the SAME code (the orchestrator may
/// re-POST).
pub async fn adopt_join_code(app_state: &AppState, code: [u8; 4]) -> Result<(), JoinCodeError> {
    if app_state
        .setup_complete
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return Err(JoinCodeError::AlreadySetUp);
    }
    if app_state.entered_join_code.set(code).is_err()
        && app_state.entered_join_code.get() != Some(&code)
    {
        return Err(JoinCodeError::Conflict);
    }
    app_state
        .comms
        .adopt_magic(code)
        .await
        .map_err(|e| JoinCodeError::Comms(e.to_string()))?;
    tracing::info!(
        code = %hopnet_comms::alpn::format_mesh_code(&code),
        "mesh code adopted — endpoint now serves the mesh's ALPN families"
    );
    Ok(())
}

#[derive(Deserialize)]
pub struct JoinCodeBody {
    pub code: String,
}

/// POST /api/setup/join-code — the interactive (and orchestrator) join
/// seam. Open pre-setup exactly like POST /api/setup: an attacker with
/// HTTP access to a fresh node could feed it a code, but that is the
/// operator's device surface, and it is STRICTLY better than pre-S5,
/// where any JoinDeliver completed with no code at all. Join
/// authorization proper stays with the JoinInfo trust layers (RFC-025
/// §Non-Goals).
pub async fn post_setup_join_code(
    State(app_state): State<AppState>,
    Json(body): Json<JoinCodeBody>,
) -> axum::response::Response {
    let Some(code) = hopnet_comms::alpn::parse_mesh_code(&body.code) else {
        return (
            StatusCode::BAD_REQUEST,
            "not a mesh code (need 8 hex digits, e.g. 9F3A-01CC)",
        )
            .into_response();
    };
    match adopt_join_code(&app_state, code).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(JoinCodeError::AlreadySetUp) => {
            (StatusCode::CONFLICT, "node is already set up").into_response()
        }
        Err(JoinCodeError::Conflict) => (
            StatusCode::CONFLICT,
            "a different mesh code was already entered — restart this node to re-enter",
        )
            .into_response(),
        Err(JoinCodeError::Comms(e)) => (
            StatusCode::CONFLICT,
            format!("mesh identity already settled: {e} — restart this node to re-enter"),
        )
            .into_response(),
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

    // Pre-flight (RFC-025 S5): the delivered anchor must match the code
    // the operator entered, BEFORE anything is written. By ALPN
    // construction the coordinator's dial already proved magic
    // agreement, so a mismatch here means a lying or buggy coordinator
    // — the cheap, structured refusal while no state exists (the
    // coordinator's delivery loop gives up on a refusal).
    let Some(entered) = app_state.entered_join_code.get() else {
        return Err(
            "no mesh code entered — a JoinDeliver should be impossible before code entry"
                .to_string(),
        );
    };
    if join_info.anchor[..4] != entered[..] {
        return Err(format!(
            "delivered anchor {} does not match the entered mesh code {}",
            hopnet_comms::alpn::format_mesh_code(
                join_info.anchor[..4].try_into().expect("4-byte truncation")
            ),
            hopnet_comms::alpn::format_mesh_code(entered),
        ));
    }

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

    // Mark setup complete — the peer directory switches to strict mode
    app_state
        .setup_complete
        .store(true, std::sync::atomic::Ordering::Relaxed);

    // Spawn the malachite join bootstrap as a background task: trusted
    // height-0 genesis install → engine spawn → decided-value sync to tip →
    // validator activation request.
    let app_state_clone = app_state.clone();
    let join_info_clone = join_info.clone();
    tokio::spawn(async move {
        tracing::info!(
            "Starting join bootstrap using {} bootstrap validators",
            join_info_clone.bootstrap_validators.len()
        );

        let reached = match crate::consensus::malachite::engine::bootstrap_join(
            &app_state_clone,
            &join_info_clone,
        )
        .await
        {
            Ok(h) => h,
            // The install-time anchor check fired (RFC-025 S5): the
            // FETCHED state does not match the entered code — a lying
            // coordinator. Roll the join back so a restart returns this
            // node to fresh (the OnceCells and the adopted magic wedge
            // THIS process; the DB row is the only persistent write).
            Err(crate::consensus::malachite::engine::JoinError::AnchorMismatch {
                installed,
                entered,
            }) => {
                if let Err(e) = setup::rollback_joining_node(app_state_clone.db_pool.get()) {
                    tracing::error!("join rollback failed: {e:?} — wipe the data dir manually");
                }
                // Back to never-joined: the agreement (if any write
                // raced ahead) leaves with the membership.
                crate::regenesis::boot::remove_agreed_version(
                    &crate::db::shared::get_database_path(),
                );
                app_state_clone
                    .setup_complete
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    installed_anchor = %hopnet_comms::alpn::format_mesh_code(
                        installed[..4].try_into().expect("4-byte truncation")),
                    entered_code = %hopnet_comms::alpn::format_mesh_code(&entered),
                    "join ABORTED: the fetched mesh identity does not match the entered \
                     code (RFC-025). RESTART this node before joining again."
                );
                return;
            }
            Err(crate::consensus::malachite::engine::JoinError::Other(e)) => {
                tracing::error!(
                    "Join bootstrap failed for node {}: {e}. Node will remain inactive.",
                    join_info_clone.node_id
                );
                return;
            }
        };

        // Joining IS accepting the agreement. An epoch-1 join has no
        // committed version to cite, so the joiner's binary stamps
        // (delivery rode the locked class — both ends provably run the
        // same release); an epoch>=2 join already stamped the record's
        // required version, and this rewrite is byte-identical (the
        // join version gate enforces equality).
        crate::regenesis::boot::write_agreed_version(
            &crate::db::shared::get_database_path(),
            crate::version::effective_running_code(),
        );

        // Mesh-initiated seating (RFC-CONSENSUS-002 S5): the joining node
        // never requests a seat — it registers, syncs, answers probes, and
        // waits to be noticed by the seat-proposal scan on the validators.
        tracing::info!(
            "Join bootstrap complete for node {} (height {reached}); pooled, awaiting mesh seating",
            join_info_clone.node_id
        );
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

            // Mark setup complete — the peer directory switches to strict mode
            app_state
                .setup_complete
                .store(true, std::sync::atomic::Ordering::Relaxed);

            // Founding a mesh IS the version agreement: epoch 1 records
            // no version in committed state, so the coordinator's own
            // binary stamps the agreed-version marker.
            crate::regenesis::boot::write_agreed_version(
                &crate::db::shared::get_database_path(),
                crate::version::effective_running_code(),
            );

            // The genesis node minted the mesh identity THIS instant —
            // adopt it on the live endpoint (RFC-025 S5). Without this
            // the coordinator stays deferred (TLS-dead, dials erroring)
            // until its next restart, and no node could ever be added.
            {
                let magic = app_state
                    .db_pool
                    .get()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
                    .and_then(|conn| {
                        crate::regenesis::genesis::mesh_magic(&conn, &crate::paths::data_dir())
                            .map_err(|e| {
                                tracing::error!("mesh magic after genesis: {e}");
                                StatusCode::INTERNAL_SERVER_ERROR
                            })
                    })?;
                app_state.comms.adopt_magic(magic).await.map_err(|e| {
                    tracing::error!("adopting the genesis mesh magic: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
                tracing::info!(
                    code = %hopnet_comms::alpn::format_mesh_code(&magic),
                    "mesh identity minted and adopted at genesis"
                );
            }

            // Genesis is installed and identity set — start the consensus
            // engine (paused on-demand at height 1 until work arrives).
            if let Err(e) = crate::consensus::malachite::engine::spawn_engine(&app_state) {
                tracing::error!("failed to start consensus engine after genesis: {e}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }

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
                        // Photo ingress deliberately NOT provisioned here:
                        // minting is enablement-gated behind the
                        // /photo-ingress/enable route (a fresh node cannot
                        // already be enabled).
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
