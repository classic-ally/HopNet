//! Shared-library access convergence worker (RFC-011 Phase 3).
//!
//! One per signed-in user, spawned beside the sidecar sync worker and
//! sharing its lifetime (the session key is the capability — no session,
//! no unwrapping). Each pass computes, per library the user belongs to,
//! the delta between the ASSERTED access set (members ∪ pending
//! invitees) and the access rows that actually exist, then converges:
//!
//! - missing wraps → unwrap own, rewrap for the target, submit
//!   `library_access_grant` (OR-IGNORE handler: racing workers from
//!   other members are harmless; first committed wrap wins);
//! - stale users (rows but neither member nor invitee) → submit
//!   `library_access_revoke` (row-deletion revocation).
//!
//! Invite pre-staging, post-accept backfill, kick cleanup, and crash
//! repair are all this same loop; `poke` (from the invite route) just
//! makes the first pass immediate. A rejected tx means consensus state
//! moved under the pass — log and let the next tick re-derive, never
//! retry the same batch.
//!
//! The loop iterates [`ConvergeLane`]s. Today there is only `Access`
//! (row convergence). A future `Keys` lane — rotate file keys after a
//! kick so remembered keys stop decrypting new fetches — slots in here
//! without changing the tick, batch, or tx shape. Deliberately designed,
//! deliberately not built (rewrap-only revocation is the accepted v1).

use std::sync::Arc;

use hopnet_common::CustomUUID;
use hopnet_photos::envelopes::{
    LibraryAccessGrantPayload, LibraryAccessRevokePayload, LibraryBlobGrant, LibraryMetadataGrant,
};
use hopnet_photos_core::dispatch::PhotoDispatch;
use hopnet_storage::crypto::StaticRecipient;

use super::dispatch_local::Submitter;

/// The convergence dimensions. `Access` converges rows toward the
/// assertion; a future `Keys` variant converges key generations.
enum ConvergeLane {
    Access,
}

const LANES: &[ConvergeLane] = &[ConvergeLane::Access];

/// Grant batch cap per tx — mirrors the sync batch limit.
const GRANT_BATCH: u32 = 500;
/// Full-pass cap per tick: a pass that still finds work after this many
/// rounds yields to the next tick (drain_sync precedent).
const MAX_ROUNDS: usize = 200;

pub(super) async fn converge_worker(
    user_id: i32,
    recipient: StaticRecipient,
    app_state: crate::AppState,
    notify: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    // Immediate first pass — a fresh enable may already owe grants.
    run_passes(user_id, &recipient, &app_state).await;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = notify.notified() => run_passes(user_id, &recipient, &app_state).await,
            _ = interval.tick() => run_passes(user_id, &recipient, &app_state).await,
        }
    }
}

async fn run_passes(user_id: i32, recipient: &StaticRecipient, app_state: &crate::AppState) {
    for lane in LANES {
        match lane {
            ConvergeLane::Access => {
                for round in 0..MAX_ROUNDS {
                    match access_pass(user_id, recipient, app_state).await {
                        Ok(true) => continue, // submitted work — re-derive
                        Ok(false) => break,   // converged
                        Err(e) => {
                            // State moved (kick/leave race) or transient DB
                            // trouble — next tick re-derives from scratch.
                            tracing::debug!(%user_id, round, "photos converge pass: {e}");
                            break;
                        }
                    }
                }
            }
        }
    }
}

/// The per-target grant work one pass derived, ready to rewrap.
pub(crate) struct GrantWork {
    library_id: CustomUUID,
    target: i32,
    target_pubkey: [u8; 32],
    /// (photo_id, own ephemeral, own wrapped metadata key)
    meta: Vec<(CustomUUID, [u8; 32], Vec<u8>)>,
    /// Own blob-access rows for blocks the target lacks.
    blobs: Vec<hopnet_storage::BlobAccess>,
}

pub(crate) struct RevokeWork {
    library_id: CustomUUID,
    target: i32,
    photo_ids: Vec<CustomUUID>,
    data_block_ids: Vec<CustomUUID>,
}

/// One access round: derive the delta, submit at most one grant tx per
/// (library, target) plus revokes. Returns whether anything was
/// submitted (true = run another round).
async fn access_pass(
    user_id: i32,
    recipient: &StaticRecipient,
    app_state: &crate::AppState,
) -> Result<bool, String> {
    let pool = app_state.db_pool.clone();
    let (grants, revokes) = tokio::task::spawn_blocking(move || derive_delta(&pool, user_id))
        .await
        .map_err(|e| format!("join: {e}"))?
        .map_err(|e| format!("delta: {e}"))?;

    if grants.is_empty() && revokes.is_empty() {
        return Ok(false);
    }

    let submitter = Submitter::new(Arc::new(app_state.clone()), user_id);
    let mut submitted = false;

    for work in grants {
        let target_pk = hopnet_storage::x25519_dalek::PublicKey::from(work.target_pubkey);
        let mut entries = Vec::with_capacity(work.meta.len());
        for (photo_id, eph, wrapped) in &work.meta {
            match hopnet_photos_core::crypto::rewrap_metadata_key(
                photo_id, eph, wrapped, recipient, &target_pk,
            ) {
                Ok((ephemeral_pubkey, encrypted_metadata_key)) => {
                    entries.push(LibraryMetadataGrant {
                        photo_id: photo_id.clone(),
                        ephemeral_pubkey,
                        encrypted_metadata_key,
                    });
                }
                // Own wrap unreadable (key mismatch/corruption): skip the
                // photo; another member's worker covers it.
                Err(e) => tracing::warn!(%user_id, %photo_id, "metadata rewrap failed: {e}"),
            }
        }
        let mut blob_wraps = Vec::with_capacity(work.blobs.len());
        for access in &work.blobs {
            match hopnet_photos_core::crypto::rewrap_blob_key(access, recipient, &target_pk) {
                Ok(wrapped) => blob_wraps.push(LibraryBlobGrant {
                    data_block_id: access.blob_id.clone(),
                    ephemeral_pubkey: wrapped.ephemeral_pubkey,
                    wrapped_key: wrapped.wrapped_key,
                }),
                Err(e) => {
                    tracing::warn!(%user_id, blob = %access.blob_id, "blob rewrap failed: {e}")
                }
            }
        }
        if entries.is_empty() && blob_wraps.is_empty() {
            continue;
        }
        let payload = LibraryAccessGrantPayload {
            library_id: work.library_id.clone(),
            user_id: work.target,
            entries,
            blob_wraps,
            operation_id: CustomUUID::new(None),
        };
        let bytes = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .map_err(|e| format!("encode grant: {e}"))?;
        match submitter
            .submit_transaction("library_access_grant", bytes)
            .await
        {
            Ok(()) => submitted = true,
            Err(e) => tracing::debug!(
                %user_id, library = %work.library_id, target = work.target,
                "grant rejected (state moved?): {e}"
            ),
        }
    }

    for work in revokes {
        let payload = LibraryAccessRevokePayload {
            library_id: work.library_id.clone(),
            user_id: work.target,
            photo_ids: work.photo_ids,
            data_block_ids: work.data_block_ids,
            operation_id: CustomUUID::new(None),
        };
        let bytes = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .map_err(|e| format!("encode revoke: {e}"))?;
        match submitter
            .submit_transaction("library_access_revoke", bytes)
            .await
        {
            Ok(()) => submitted = true,
            Err(e) => tracing::debug!(
                %user_id, library = %work.library_id, target = work.target,
                "revoke rejected (state moved?): {e}"
            ),
        }
    }

    Ok(submitted)
}

/// Synchronous delta derivation (rusqlite — runs under spawn_blocking).
pub(crate) fn derive_delta(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    user_id: i32,
) -> Result<(Vec<GrantWork>, Vec<RevokeWork>), String> {
    use hopnet_photos::db::{libraries, photos};
    let conn = pool.get().map_err(|e| format!("pool: {e}"))?;

    let mut grants = Vec::new();
    let mut revokes = Vec::new();

    for lib in libraries::libraries_for_member(&conn, user_id).map_err(|e| format!("{e:?}"))? {
        for (target, target_pubkey) in
            libraries::assertion_targets(&conn, &lib).map_err(|e| format!("{e:?}"))?
        {
            if target == user_id {
                continue;
            }
            let missing_meta = libraries::missing_metadata_grants(&conn, &lib, target, GRANT_BATCH)
                .map_err(|e| format!("{e:?}"))?;
            let missing_blobs =
                libraries::missing_blob_grants(&conn, &lib, &target_pubkey, GRANT_BATCH)
                    .map_err(|e| format!("{e:?}"))?;
            if missing_meta.is_empty() && missing_blobs.is_empty() {
                continue;
            }
            let meta = libraries::own_metadata_wraps(&conn, user_id, &missing_meta)
                .map_err(|e| format!("{e:?}"))?;
            let mut blobs = Vec::with_capacity(missing_blobs.len());
            for (_photo, block) in &missing_blobs {
                if let Some(access) = photos::get_blob_access_for_user(&conn, block, user_id)
                    .map_err(|e| format!("{e:?}"))?
                {
                    blobs.push(access);
                }
                // else: no own wrap for this block — another member covers.
            }
            if meta.is_empty() && blobs.is_empty() {
                continue;
            }
            grants.push(GrantWork {
                library_id: lib.clone(),
                target,
                target_pubkey,
                meta,
                blobs,
            });
        }

        for target in libraries::stale_access_users(&conn, &lib).map_err(|e| format!("{e:?}"))? {
            let Some(pubkey) =
                libraries::user_x25519_pubkey(&conn, target).map_err(|e| format!("{e:?}"))?
            else {
                continue;
            };
            let (photo_ids, data_block_ids) =
                libraries::stale_access_rows(&conn, &lib, target, &pubkey, GRANT_BATCH)
                    .map_err(|e| format!("{e:?}"))?;
            if photo_ids.is_empty() && data_block_ids.is_empty() {
                continue;
            }
            revokes.push(RevokeWork {
                library_id: lib.clone(),
                target,
                photo_ids,
                data_block_ids,
            });
        }
    }

    Ok((grants, revokes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> r2d2::Pool<r2d2_sqlite::SqliteConnectionManager> {
        let dir = std::env::temp_dir().join(format!("hopnet-converge-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("delta-{:x}.sqlite", rand_suffix()));
        let manager = r2d2_sqlite::SqliteConnectionManager::file(&path);
        let pool = r2d2::Pool::builder().max_size(2).build(manager).unwrap();
        let conn = pool.get().unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE users (user_id INTEGER PRIMARY KEY, x25519_pubkey BLOB);
             CREATE TABLE consensus_meta (key TEXT PRIMARY KEY, value BLOB);",
        )
        .unwrap();
        hopnet_storage::store::install_schema(&conn).unwrap();
        hopnet_photos::db::install_schema(&conn).unwrap();
        for uid in 1..=3 {
            conn.execute(
                "INSERT INTO users (user_id, x25519_pubkey) VALUES (?1, ?2)",
                rusqlite::params![uid, vec![uid as u8; 32]],
            )
            .unwrap();
        }
        pool
    }

    fn rand_suffix() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        nanos ^ (COUNTER.fetch_add(1, Ordering::Relaxed) << 48)
    }

    // Impact: derive_delta IS the convergence brain — over-derivation
    // grants strangers, under-derivation starves invitees forever.
    // Should: emit one grant per (library, uncovered target) pairing the
    // caller's own wraps with the target, skip the caller themself, and
    // emit a revoke for a wrap-holding user who is neither member nor
    // invitee.
    // Should not: derive grants for tombstoned photos, or grants the
    // caller holds no own wrap to rewrap from.
    #[test]
    fn derive_delta_grants_and_revokes() {
        let pool = pool();
        let conn = pool.get().unwrap();
        let lib: hopnet_common::CustomUUID =
            "00000000-0000-0000-0000-0000000000a1".parse().unwrap();
        conn.execute(
            "INSERT INTO shared_libraries (id, encrypted_name, name_nonce) VALUES (?1, x'00', x'00')",
            rusqlite::params![lib],
        )
        .unwrap();
        // User 1 = member (the deriving worker); user 2 = pending invitee.
        conn.execute(
            "INSERT INTO shared_library_members (library_id, user_id) VALUES (?1, 1)",
            rusqlite::params![lib],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO shared_library_invites
               (library_id, user_id, invited_by, operation_id, ephemeral_pubkey, wrapped_key)
             VALUES (?1, 2, 1, 'op', x'00', x'00')",
            rusqlite::params![lib],
        )
        .unwrap();
        // Photos: one live with the caller's wrap, one live WITHOUT a
        // caller wrap, one tombstoned.
        let live: hopnet_common::CustomUUID =
            "00000000-0000-0000-0000-0000000000b1".parse().unwrap();
        let unwrapped: hopnet_common::CustomUUID =
            "00000000-0000-0000-0000-0000000000b2".parse().unwrap();
        let dead: hopnet_common::CustomUUID =
            "00000000-0000-0000-0000-0000000000b3".parse().unwrap();
        for (id, deleted) in [(&live, false), (&unwrapped, false), (&dead, true)] {
            conn.execute(
                "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce, deleted_at)
                 VALUES (?1, ?2, 1, x'00', x'00', ?3)",
                rusqlite::params![id, lib, deleted.then_some("2026-01-01T00:00:00Z")],
            )
            .unwrap();
        }
        // 32-byte ephemerals: own_metadata_wraps validates the shape.
        for uid in [1, 3] {
            // uid 3 = stale wrap holder (neither member nor invitee).
            conn.execute(
                "INSERT INTO photo_metadata_access (photo_id, user_id, ephemeral_pubkey, encrypted_metadata_key)
                 VALUES (?1, ?2, ?3, x'00')",
                rusqlite::params![live, uid, vec![0xEEu8; 32]],
            )
            .unwrap();
        }
        drop(conn);

        let (grants, revokes) = derive_delta(&pool, 1).unwrap();

        // Grants: user 2 (invitee) and user 3?? — no: 3 is not asserted, so
        // only user 2. The caller (1) never targets themself.
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].target, 2);
        assert_eq!(grants[0].target_pubkey, [2u8; 32]);
        let granted: Vec<_> = grants[0].meta.iter().map(|(p, _, _)| p.clone()).collect();
        assert_eq!(
            granted,
            vec![live.clone()],
            "only the live, caller-wrapped photo is grantable"
        );

        assert_eq!(revokes.len(), 1);
        assert_eq!(revokes[0].target, 3);
        assert_eq!(revokes[0].photo_ids, vec![live]);
    }

    // Should: derive nothing for a fully converged library (grants and
    // revokes both empty), so the pass loop terminates.
    #[test]
    fn derive_delta_converged_is_empty() {
        let pool = pool();
        let conn = pool.get().unwrap();
        let lib: hopnet_common::CustomUUID =
            "00000000-0000-0000-0000-0000000000a2".parse().unwrap();
        conn.execute(
            "INSERT INTO shared_libraries (id, encrypted_name, name_nonce) VALUES (?1, x'00', x'00')",
            rusqlite::params![lib],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO shared_library_members (library_id, user_id) VALUES (?1, 1)",
            rusqlite::params![lib],
        )
        .unwrap();
        drop(conn);
        let (grants, revokes) = derive_delta(&pool, 1).unwrap();
        assert!(grants.is_empty() && revokes.is_empty());
    }
}
