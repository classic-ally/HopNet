//! Host adapter for the drive's HTTP/business seams (RFC-015 Stage D4).
//!
//! One `DriveHost` implements all of hopnet-drive's host capabilities
//! (pattern: `files::substrate_host::SubstrateHost` for the RFC-014
//! seams): sessions with host-side key derivation, consensus submission
//! with host-side signing, blob reconstruction over the substrate seams,
//! and the takeout import gate as write admission.

use std::sync::Arc;

use axum::http::StatusCode;

use crate::AppState;
use hopnet_drive::host::{
    BlobStreamer, BoxFuture, ByteStream, DriveState, SessionAccess, SessionError, TxGateway,
    TxSigner, TxSpec, TxSubmitError, UserSession, WriteAdmission, WriteCheckError, WriteDenied,
};

pub struct DriveHost {
    app_state: AppState,
}

impl SessionAccess for DriveHost {
    fn user_session(&self, user_id: i32) -> BoxFuture<'_, Result<UserSession, SessionError>> {
        Box::pin(async move {
            // AppState::get_session maps expired → 401, missing → 428; the
            // seam preserves exactly that split.
            let entry = self
                .app_state
                .get_session(user_id)
                .await
                .map_err(|status| match status {
                    StatusCode::UNAUTHORIZED => SessionError::Unauthorized,
                    _ => SessionError::PreconditionRequired,
                })?;

            // Derive the drive-facing key material host-side — the ed25519
            // identity key never crosses the seam.
            let x25519_privkey =
                crate::auth::derive_x25519_privkey_from_user(&entry.user_keys.private_key);

            Ok(UserSession {
                siv_key: entry.siv_key,
                siv_nonce: entry.siv_nonce,
                x25519_privkey,
            })
        })
    }
}

impl TxGateway for DriveHost {
    fn submit_batch(&self, txs: Vec<TxSpec>) -> BoxFuture<'_, Vec<Result<(), TxSubmitError>>> {
        Box::pin(async move {
            let n = txs.len();

            // Sign everything first — a signing failure aborts the batch
            // before anything reaches consensus (matching the pre-seam flow,
            // where transactions were built before any submission).
            let mut signed = Vec::with_capacity(n);
            for spec in txs {
                let result = match spec.signer {
                    TxSigner::Node => crate::consensus::dispatch::create_signed_transaction(
                        &self.app_state,
                        spec.function.to_string(),
                        spec.payload,
                    ),
                    TxSigner::User(user_id) => {
                        crate::consensus::dispatch::create_signed_user_transaction(
                            &self.app_state,
                            spec.function.to_string(),
                            spec.payload,
                            user_id,
                        )
                        .await
                    }
                };
                match result {
                    Ok(tx) => signed.push(tx),
                    Err(_) => return (0..n).map(|_| Err(TxSubmitError::Signing)).collect(),
                }
            }

            // ONE consensus submission for the whole batch, per-entry results.
            self.app_state
                .consensus_queue
                .submit_batch(signed)
                .await
                .into_iter()
                .map(|r| {
                    r.map_err(|e| match e {
                        crate::consensus::queue::ConsensusSubmitError::Rejected(reason) => {
                            TxSubmitError::Rejected(reason)
                        }
                        _ => TxSubmitError::Submit,
                    })
                })
                .collect()
        })
    }
}

impl BlobStreamer for DriveHost {
    fn stream(
        &self,
        manifest: hopnet_storage::store::BlobManifest,
        per_blob_key: Option<chacha20poly1305::Key>,
        range: Option<(u64, u64)>,
    ) -> ByteStream {
        Box::pin(hopnet_storage::api::get(
            Some(crate::files::substrate_host::get_net(&self.app_state)),
            self.app_state.fragments_dir.clone(),
            manifest,
            per_blob_key,
            range,
        ))
    }
}

impl WriteAdmission for DriveHost {
    fn check_write(&self, user_id: i32) -> BoxFuture<'_, Result<(), WriteCheckError>> {
        Box::pin(async move {
            // The takeout import gate: writes are denied while the user has
            // an active import (`status IN (Pending, Importing)`). Check
            // failures surface as Internal (500), matching the pre-seam
            // middleware.
            let conn = self
                .app_state
                .db_pool
                .get()
                .map_err(|_| WriteCheckError::Internal)?;
            let active = hopnet_takeout::db::imports::has_active_import(&conn, user_id)
                .map_err(|_| WriteCheckError::Internal)?;

            if active {
                return Err(WriteCheckError::Denied(WriteDenied {
                    reason: "import in progress".to_string(),
                }));
            }
            Ok(())
        })
    }
}

/// Build the drive's axum state over this host. Cheap: one `DriveHost`
/// allocation cloned into the seam fields plus pool/OnceCell Arc clones —
/// callers may construct it on the fly (takeout does, per materialized
/// batch).
pub fn drive_state(app_state: &AppState) -> DriveState {
    let host = Arc::new(DriveHost {
        app_state: app_state.clone(),
    });
    DriveState {
        db_pool: app_state.db_pool.clone(),
        fragments_dir: app_state.fragments_dir.clone(),
        test_mode: app_state.test_mode,
        node_id: app_state.node_id.clone(),
        sessions: host.clone(),
        txs: host.clone(),
        blobs: host.clone(),
        notify: Arc::new(crate::handlers::HostNotifier {
            test_mode: app_state.test_mode,
        }),
        write_admission: host,
    }
}
