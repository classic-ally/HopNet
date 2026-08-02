//! Photos consensus transaction handlers (RFC-011 Phase 1).
//!
//! Registration crosses the crate boundary via `inventory::submit!` (the
//! registry lives in hopnet-projection); the host's boot tripwire asserts
//! nothing was dropped at link time. DB mutations run in BOTH validate and
//! execute passes; side effects (notifier, work) fire only under execute.

use crate::db::libraries;
use crate::db::photos::{
    delete_favorite, delete_ingress_responsibility_for_library, device_belongs_to_user,
    edit_photo_content, edit_photo_metadata, hard_delete_expired_photo, insert_favorite,
    insert_photo_entry, lookup_photo_authz, restore_photo, soft_delete_photo, undo_content_edit,
    upsert_ingress_responsibility,
};
use crate::envelopes::{
    CreateSharedLibraryPayload, LibraryAccessGrantPayload, LibraryAccessRevokePayload,
    LibraryInviteAcceptPayload, LibraryInviteDeclinePayload, LibraryInvitePayload,
    LibraryRemoveMemberPayload, PhotoAddPayload, PhotoCleanupExpiredPayload, PhotoDeletePayload,
    PhotoEditContentPayload, PhotoEditMetadataPayload, PhotoFavoritePayload,
    PhotoIngressClaimPayload, PhotoRestorePayload, PhotoUndoPayload, PhotoUnfavoritePayload,
};
use hopnet_common::CustomUUID;
use hopnet_projection::{DatabaseError, HandlerCtx, HandlerResult, TransactionHandler, TxMeta};

/// Write authorization for an existing photo: the uploader always may;
/// for shared photos any library member may (RFC-011 equal standing).
/// Deterministic — membership is consensus state.
fn photo_write_allowed(
    db_tx: &rusqlite::Transaction,
    user_id: i32,
    uploaded_by: i32,
    library_id: Option<&CustomUUID>,
) -> Result<bool, DatabaseError> {
    if uploaded_by == user_id {
        return Ok(true);
    }
    match library_id {
        Some(lib) => libraries::is_member(db_tx, lib, user_id),
        None => Ok(false),
    }
}

/// Every consensus function this projection registers — the host boot
/// tripwire asserts these are present in its dispatch table (guards
/// against a linker dropping cross-crate inventory registrations).
pub const TX_FUNCTIONS: &[&str] = &[
    "photo_add",
    "photo_delete",
    "photo_restore",
    "photo_cleanup_expired",
    "photo_edit_content",
    "photo_edit_metadata",
    "photo_undo",
    "photo_favorite",
    "photo_unfavorite",
    "photo_ingress_claim",
    "create_shared_library",
    "library_invite",
    "library_invite_accept",
    "library_invite_decline",
    "library_remove_member",
    "library_access_grant",
    "library_access_revoke",
];

/// Subset of [`TX_FUNCTIONS`] that users may submit directly (excludes
/// node-signed handlers like `photo_cleanup_expired`).
pub const USER_TX_FUNCTIONS: &[&str] = &[
    "photo_add",
    "photo_delete",
    "photo_restore",
    "photo_edit_content",
    "photo_edit_metadata",
    "photo_undo",
    "photo_favorite",
    "photo_unfavorite",
    "photo_ingress_claim",
    "create_shared_library",
    "library_invite",
    "library_invite_accept",
    "library_invite_decline",
    "library_remove_member",
    "library_access_grant",
    "library_access_revoke",
];

const _USER_TX_COUNT: () = assert!(
    USER_TX_FUNCTIONS.len() + 1 == TX_FUNCTIONS.len(),
    "USER_TX_FUNCTIONS out of sync with TX_FUNCTIONS — did you add a node-signed handler?"
);

/// Subset of [`USER_TX_FUNCTIONS`] that thin-client DEVICES may submit
/// (`POST /api/photos/client/transaction`). Excludes `photo_ingress_claim`:
/// responsibility claims are JWT-route only in v1, so a daemon can never
/// designate itself — the enablement UI (or any logged-in session) issues
/// the claim deliberately.
pub const DEVICE_TX_FUNCTIONS: &[&str] = &[
    "photo_add",
    "photo_delete",
    "photo_restore",
    "photo_edit_content",
    "photo_edit_metadata",
    "photo_undo",
    "photo_favorite",
    "photo_unfavorite",
];

const _DEVICE_TX_COUNT: () = assert!(
    DEVICE_TX_FUNCTIONS.len() + 8 == USER_TX_FUNCTIONS.len(),
    "DEVICE_TX_FUNCTIONS out of sync with USER_TX_FUNCTIONS — decide whether the new user tx is device-submittable. JWT-only today: photo_ingress_claim + the 7 shared-library membership txs (a daemon must never mint, invite into, or grant access on a library)"
);

// --- photo_add ---

pub struct PhotoAddHandler;

impl TransactionHandler for PhotoAddHandler {
    fn name(&self) -> &'static str {
        "photo_add"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoAddPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        let user_id = tx.user_id.ok_or_else(|| {
            tracing::warn!("photo_add: requires user authentication");
            DatabaseError::AuthorizationError
        })?;

        for entry in &payload.entries {
            if entry.uploaded_by != user_id {
                tracing::warn!(
                    "photo_add: user {} attempted to add photo for {}",
                    user_id,
                    entry.uploaded_by,
                );
                return Err(DatabaseError::AuthorizationError);
            }
            // Shared-library adds require membership — deterministic
            // against consensus state; the device route cannot check this
            // (the payload is opaque bincode at the route layer).
            if let Some(lib) = &entry.library_id
                && !libraries::is_member(db_tx, lib, user_id)?
            {
                tracing::warn!("photo_add: user {user_id} is not a member of library {lib}",);
                return Err(DatabaseError::AuthorizationError);
            }
            insert_photo_entry(db_tx, entry, ctx.fragments_dir)?;
        }

        Ok(())
    }
}

inventory::submit! {
    &PhotoAddHandler as &dyn TransactionHandler
}

// --- photo_delete ---

pub struct PhotoDeleteHandler;

impl TransactionHandler for PhotoDeleteHandler {
    fn name(&self) -> &'static str {
        "photo_delete"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoDeletePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        let user_id = tx.user_id.ok_or_else(|| {
            tracing::warn!("photo_delete: requires user authentication");
            DatabaseError::AuthorizationError
        })?;

        for entry in &payload.entries {
            // Look up the photo's owner + library scope.
            let row: Result<(i32, Option<hopnet_common::CustomUUID>), _> = db_tx.query_row(
                "SELECT uploaded_by, library_id FROM photos WHERE id = ?1",
                rusqlite::params![entry.photo_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            );
            let (uploaded_by, library_id) = match row {
                Ok(r) => r,
                Err(_) => {
                    // Photo not found — already deleted or never existed.
                    // Idempotent skip (drive precedent: DeleteFilesHandler
                    // at hopnet-drive/src/handlers.rs:214-222).
                    continue;
                }
            };

            // Authorization: the uploader, or any member of the photo's
            // shared library (equal standing — RFC-011).
            if !photo_write_allowed(db_tx, user_id, uploaded_by, library_id.as_ref())? {
                tracing::warn!(
                    "photo_delete: user {} not authorized for photo {} (owned by {})",
                    user_id,
                    entry.photo_id,
                    uploaded_by,
                );
                return Err(DatabaseError::AuthorizationError);
            }

            let deleted_at = entry
                .operation_id
                .extract_timestamp()
                .map(|dt| dt.to_rfc3339())
                .ok_or_else(|| {
                    tracing::warn!(
                        "photo_delete: operation_id {} is not UUIDv7",
                        entry.operation_id,
                    );
                    DatabaseError::InvalidPayload
                })?;
            // TODO(Phase 3): operation_id is client-minted with no bound
            // against consensus time. A malicious shared-library member
            // could backdate it 30+ days to skip the recovery window for
            // other members. Validate the operation_id timestamp is within
            // a sane window of the consensus block timestamp (derivable
            // from tx metadata, consistent with dispatch.rs:101).

            soft_delete_photo(
                db_tx,
                &entry.photo_id,
                user_id,
                &deleted_at,
                library_id.as_ref(),
                &entry.operation_id,
            )?;
        }

        Ok(())
    }
}

inventory::submit! {
    &PhotoDeleteHandler as &dyn TransactionHandler
}

// --- photo_restore ---

pub struct PhotoRestoreHandler;

impl TransactionHandler for PhotoRestoreHandler {
    fn name(&self) -> &'static str {
        "photo_restore"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoRestorePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        let user_id = tx.user_id.ok_or_else(|| {
            tracing::warn!("photo_restore: requires user authentication");
            DatabaseError::AuthorizationError
        })?;

        for entry in &payload.entries {
            // Look up owner + library scope for authorization.
            let row: Result<(i32, Option<hopnet_common::CustomUUID>), _> = db_tx.query_row(
                "SELECT uploaded_by, library_id FROM photos WHERE id = ?1",
                rusqlite::params![entry.photo_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            );
            let (uploaded_by, library_id) = match row {
                Ok(r) => r,
                Err(_) => {
                    // Photo not found — already hard-deleted by cleanup,
                    // or never existed. Idempotent skip.
                    continue;
                }
            };

            if !photo_write_allowed(db_tx, user_id, uploaded_by, library_id.as_ref())? {
                tracing::warn!(
                    "photo_restore: user {} not authorized for photo {} (owned by {})",
                    user_id,
                    entry.photo_id,
                    uploaded_by,
                );
                return Err(DatabaseError::AuthorizationError);
            }

            restore_photo(
                db_tx,
                &entry.photo_id,
                user_id,
                library_id.as_ref(),
                &entry.operation_id,
            )?;
        }

        Ok(())
    }
}

inventory::submit! {
    &PhotoRestoreHandler as &dyn TransactionHandler
}

// --- photo_cleanup_expired ---

pub struct PhotoCleanupExpiredHandler;

impl TransactionHandler for PhotoCleanupExpiredHandler {
    fn name(&self) -> &'static str {
        "photo_cleanup_expired"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoCleanupExpiredPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Node-signed only — a user-signed submission could hard-delete
        // another member's tombstoned photo within the recovery window.
        // TODO(Phase 3): scan_cutoff is payload data — a malicious or
        // skewed-clock node can submit a far-future cutoff to bypass the
        // 30-day window. Clamp against the consensus block timestamp when
        // dispatch exposes one. Symmetric with the operation_id backdating
        // TODO at photo_delete.
        if tx.user_id.is_some() {
            tracing::warn!("photo_cleanup_expired: user-signed submissions rejected");
            return Err(DatabaseError::AuthorizationError);
        }

        for photo_id in &payload.photo_ids {
            match hard_delete_expired_photo(db_tx, photo_id, &payload.scan_cutoff) {
                Ok(()) | Err(DatabaseError::NotFound) => {}
                Err(e) => {
                    tracing::error!(
                        "photo_cleanup_expired: hard-delete {} failed: {:?}",
                        photo_id,
                        e,
                    );
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

inventory::submit! {
    &PhotoCleanupExpiredHandler as &dyn TransactionHandler
}

// --- photo_edit_content ---

pub struct PhotoEditContentHandler;

impl TransactionHandler for PhotoEditContentHandler {
    fn name(&self) -> &'static str {
        "photo_edit_content"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoEditContentPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            if entry.resources.is_empty() {
                return Err(DatabaseError::InvalidPayload);
            }
            let (uploaded_by, deleted_at, library_id) =
                lookup_photo_authz(db_tx, &entry.photo_id)?.ok_or(DatabaseError::NotFound)?;
            if !photo_write_allowed(db_tx, user_id, uploaded_by, library_id.as_ref())? {
                return Err(DatabaseError::AuthorizationError);
            }
            if deleted_at.is_some() {
                // Tombstoned — reject edits.
                return Err(DatabaseError::ConflictError);
            }
            edit_photo_content(db_tx, entry, ctx.fragments_dir, user_id)?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoEditContentHandler as &dyn TransactionHandler }

// --- photo_edit_metadata ---

pub struct PhotoEditMetadataHandler;

impl TransactionHandler for PhotoEditMetadataHandler {
    fn name(&self) -> &'static str {
        "photo_edit_metadata"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoEditMetadataPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            let (uploaded_by, deleted_at, library_id) =
                lookup_photo_authz(db_tx, &entry.photo_id)?.ok_or(DatabaseError::NotFound)?;
            if !photo_write_allowed(db_tx, user_id, uploaded_by, library_id.as_ref())? {
                return Err(DatabaseError::AuthorizationError);
            }
            if deleted_at.is_some() {
                return Err(DatabaseError::ConflictError);
            }
            edit_photo_metadata(db_tx, entry, user_id)?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoEditMetadataHandler as &dyn TransactionHandler }

// --- photo_undo ---

pub struct PhotoUndoHandler;

impl TransactionHandler for PhotoUndoHandler {
    fn name(&self) -> &'static str {
        "photo_undo"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoUndoPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            let (uploaded_by, deleted_at, library_id) =
                lookup_photo_authz(db_tx, &entry.photo_id)?.ok_or(DatabaseError::NotFound)?;
            if !photo_write_allowed(db_tx, user_id, uploaded_by, library_id.as_ref())? {
                return Err(DatabaseError::AuthorizationError);
            }
            if deleted_at.is_some() {
                return Err(DatabaseError::ConflictError);
            }
            undo_content_edit(
                db_tx,
                &entry.photo_id,
                &entry.target_operation_id,
                &entry.operation_id,
                user_id,
            )?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoUndoHandler as &dyn TransactionHandler }

// --- photo_favorite ---

pub struct PhotoFavoriteHandler;

impl TransactionHandler for PhotoFavoriteHandler {
    fn name(&self) -> &'static str {
        "photo_favorite"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoFavoritePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            let Some((uploaded_by, deleted_at, library_id)) =
                lookup_photo_authz(db_tx, &entry.photo_id)?
            else {
                continue; // hard-deleted — idempotent skip
            };
            if deleted_at.is_some() {
                return Err(DatabaseError::ConflictError);
            }
            // Favorites are per-user state, but the photo must be YOURS to
            // see: uploader or library member. (Previously unchecked — a
            // non-member could favorite any guessed photo_id and mint an
            // operation row against it.)
            if !photo_write_allowed(db_tx, user_id, uploaded_by, library_id.as_ref())? {
                return Err(DatabaseError::AuthorizationError);
            }
            insert_favorite(db_tx, &entry.photo_id, user_id)?;
            crate::db::photos::upsert_photo_changes(db_tx, &entry.photo_id)?;
            crate::db::photos::insert_operation_row(
                db_tx,
                &entry.operation_id,
                None,
                &entry.photo_id,
                6,
                user_id,
                None,
                None,
                None,
                None,
            )?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoFavoriteHandler as &dyn TransactionHandler }

// --- photo_unfavorite ---

pub struct PhotoUnfavoriteHandler;

impl TransactionHandler for PhotoUnfavoriteHandler {
    fn name(&self) -> &'static str {
        "photo_unfavorite"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoUnfavoritePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        for entry in &payload.entries {
            // Skip missing or tombstoned photos. soft_delete_photo already
            // clears favorites, so the delete is a no-op, but rejecting
            // tombstoned avoids logging a spurious operation.
            let Some((uploaded_by, deleted_at, library_id)) =
                lookup_photo_authz(db_tx, &entry.photo_id)?
            else {
                continue;
            };
            if deleted_at.is_some() {
                continue;
            }
            if !photo_write_allowed(db_tx, user_id, uploaded_by, library_id.as_ref())? {
                return Err(DatabaseError::AuthorizationError);
            }
            delete_favorite(db_tx, &entry.photo_id, user_id)?;
            crate::db::photos::upsert_photo_changes(db_tx, &entry.photo_id)?;
            crate::db::photos::insert_operation_row(
                db_tx,
                &entry.operation_id,
                None,
                &entry.photo_id,
                7,
                user_id,
                None,
                None,
                None,
                None,
            )?;
        }
        Ok(())
    }
}

inventory::submit! { &PhotoUnfavoriteHandler as &dyn TransactionHandler }

// --- photo_ingress_claim ---

pub struct PhotoIngressClaimHandler;

impl TransactionHandler for PhotoIngressClaimHandler {
    fn name(&self) -> &'static str {
        "photo_ingress_claim"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<PhotoIngressClaimPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        let user_id = tx.user_id.ok_or_else(|| {
            tracing::warn!("photo_ingress_claim: requires user authentication");
            DatabaseError::AuthorizationError
        })?;

        // Deterministic: device_tokens is consensus-replicated, so every
        // validator sees the same ownership state at this height.
        if !device_belongs_to_user(db_tx, &payload.device_id, user_id)? {
            tracing::warn!(
                "photo_ingress_claim: device {} does not belong to user {}",
                payload.device_id,
                user_id,
            );
            return Err(DatabaseError::AuthorizationError);
        }

        // A shared-scope claim is only meaningful for a member: kicked
        // users lose the scope with their membership (the remove handler
        // deletes their responsibility rows), and a non-member must not
        // pre-stage one.
        if let Some(lib) = &payload.library_id
            && !crate::db::libraries::is_member(db_tx, lib, user_id)?
        {
            tracing::warn!(
                "photo_ingress_claim: user {} is not a member of library {}",
                user_id,
                lib,
            );
            return Err(DatabaseError::AuthorizationError);
        }

        upsert_ingress_responsibility(
            db_tx,
            user_id,
            payload.library_id.as_ref(),
            &payload.device_id,
            &payload.operation_id,
        )
    }
}

inventory::submit! { &PhotoIngressClaimHandler as &dyn TransactionHandler }

// --- create_shared_library ---

pub struct CreateSharedLibraryHandler;

impl TransactionHandler for CreateSharedLibraryHandler {
    fn name(&self) -> &'static str {
        "create_shared_library"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<CreateSharedLibraryPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        // The creator can only wrap the library key for themself here;
        // everyone else arrives via invite.
        if payload.creator_key.user_id != user_id {
            tracing::warn!(
                "create_shared_library: creator wrap targets user {} but submitter is {}",
                payload.creator_key.user_id,
                user_id,
            );
            return Err(DatabaseError::AuthorizationError);
        }

        libraries::insert_library(
            db_tx,
            &payload.library_id,
            &payload.encrypted_name,
            &payload.name_nonce,
        )?;
        libraries::insert_member(db_tx, &payload.library_id, user_id)?;
        libraries::insert_library_key(
            db_tx,
            &payload.library_id,
            user_id,
            &payload.creator_key.ephemeral_pubkey,
            &payload.creator_key.wrapped_key,
        )?;
        Ok(())
    }
}

inventory::submit! { &CreateSharedLibraryHandler as &dyn TransactionHandler }

// --- library_invite ---

pub struct LibraryInviteHandler;

impl TransactionHandler for LibraryInviteHandler {
    fn name(&self) -> &'static str {
        "library_invite"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<LibraryInvitePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;
        let invitee = payload.invitee.user_id;

        if !libraries::is_member(db_tx, &payload.library_id, user_id)? {
            return Err(DatabaseError::AuthorizationError);
        }
        if invitee == user_id || libraries::is_member(db_tx, &payload.library_id, invitee)? {
            return Err(DatabaseError::ConflictError);
        }
        // Invitee must be a known mesh user with a wrappable pubkey.
        if libraries::user_x25519_pubkey(db_tx, invitee)?.is_none() {
            return Err(DatabaseError::InvalidPayload);
        }

        libraries::insert_invite(
            db_tx,
            &payload.library_id,
            invitee,
            user_id,
            &payload.operation_id,
            &payload.invitee.ephemeral_pubkey,
            &payload.invitee.wrapped_key,
        )
    }
}

inventory::submit! { &LibraryInviteHandler as &dyn TransactionHandler }

// --- library_invite_accept ---

pub struct LibraryInviteAcceptHandler;

impl TransactionHandler for LibraryInviteAcceptHandler {
    fn name(&self) -> &'static str {
        "library_invite_accept"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<LibraryInviteAcceptPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        // Consent: only the invitee's own signature accepts. Re-delivery
        // is deterministic — the invite row is gone, so NotFound.
        let (eph, wrapped) = libraries::get_invite_wrap(db_tx, &payload.library_id, user_id)?
            .ok_or(DatabaseError::NotFound)?;

        libraries::insert_member(db_tx, &payload.library_id, user_id)?;
        // Promote the invite-parked wrap; OR IGNORE tolerates a re-grant.
        libraries::insert_library_key(db_tx, &payload.library_id, user_id, &eph, &wrapped)?;
        libraries::delete_invite(db_tx, &payload.library_id, user_id)?;
        // Signal the new member's sidecar to backfill the library. No
        // photo_changes writes — the photos did not change; the view did.
        libraries::upsert_view_change(db_tx, user_id, &payload.library_id)
    }
}

inventory::submit! { &LibraryInviteAcceptHandler as &dyn TransactionHandler }

// --- library_invite_decline ---

pub struct LibraryInviteDeclineHandler;

impl TransactionHandler for LibraryInviteDeclineHandler {
    fn name(&self) -> &'static str {
        "library_invite_decline"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<LibraryInviteDeclinePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        // The invitee may refuse; any member may retract (equal standing).
        if payload.invitee_user_id != user_id
            && !libraries::is_member(db_tx, &payload.library_id, user_id)?
        {
            return Err(DatabaseError::AuthorizationError);
        }
        if !libraries::delete_invite(db_tx, &payload.library_id, payload.invitee_user_id)? {
            return Err(DatabaseError::NotFound);
        }
        Ok(())
    }
}

inventory::submit! { &LibraryInviteDeclineHandler as &dyn TransactionHandler }

// --- library_remove_member ---

pub struct LibraryRemoveMemberHandler;

impl TransactionHandler for LibraryRemoveMemberHandler {
    fn name(&self) -> &'static str {
        "library_remove_member"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<LibraryRemoveMemberPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        // Self-removal is leave; removing another member is kick. Both
        // require the SUBMITTER to be a member (equal standing).
        if !libraries::is_member(db_tx, &payload.library_id, user_id)? {
            return Err(DatabaseError::AuthorizationError);
        }

        let was_member = libraries::delete_member(db_tx, &payload.library_id, payload.user_id)?;
        let was_invitee = libraries::delete_invite(db_tx, &payload.library_id, payload.user_id)?;
        if !was_member && !was_invitee {
            return Err(DatabaseError::NotFound);
        }
        libraries::delete_library_key(db_tx, &payload.library_id, payload.user_id)?;
        // The target's client detects membership loss by diff and purges;
        // a dangling view signal would just point at a library they can no
        // longer read. The convergence worker revokes their access rows
        // lazily (row-deletion revocation; key rotation is a future lane).
        libraries::delete_view_change(db_tx, payload.user_id, &payload.library_id)?;
        // Membership loss dissolves the target's ingress claim on this
        // scope: their daemon must not keep passing the thin-client tx
        // gate for a library they can no longer publish into.
        delete_ingress_responsibility_for_library(db_tx, payload.user_id, &payload.library_id)
    }
}

inventory::submit! { &LibraryRemoveMemberHandler as &dyn TransactionHandler }

// --- library_access_grant ---

pub struct LibraryAccessGrantHandler;

impl TransactionHandler for LibraryAccessGrantHandler {
    fn name(&self) -> &'static str {
        "library_access_grant"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<LibraryAccessGrantPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        if payload.entries.is_empty() && payload.blob_wraps.is_empty() {
            return Err(DatabaseError::InvalidPayload);
        }
        if !libraries::is_member(db_tx, &payload.library_id, user_id)? {
            return Err(DatabaseError::AuthorizationError);
        }
        // Target must be asserted: member or pending invitee. A grant to
        // anyone else would be an access leak no read gate could catch
        // for personal-library semantics.
        let target = payload.user_id;
        if !libraries::is_member(db_tx, &payload.library_id, target)?
            && !libraries::is_invitee(db_tx, &payload.library_id, target)?
        {
            return Err(DatabaseError::AuthorizationError);
        }
        // The recipient pubkey comes from consensus state, never the wire.
        let target_pubkey =
            libraries::user_x25519_pubkey(db_tx, target)?.ok_or(DatabaseError::InvalidPayload)?;

        for grant in &payload.entries {
            // Live photos of THIS library only — tombstoned photos are
            // never granted (invitees don't inherit the recovery window),
            // and a foreign photo id must not smuggle access.
            if !libraries::photo_in_library_live(db_tx, &grant.photo_id, &payload.library_id)? {
                return Err(DatabaseError::ValidationError);
            }
            libraries::insert_metadata_access_grant(db_tx, grant, target)?;
        }
        for grant in &payload.blob_wraps {
            if !libraries::block_in_library(db_tx, &grant.data_block_id, &payload.library_id)? {
                return Err(DatabaseError::ValidationError);
            }
            libraries::insert_blob_access_grant(db_tx, grant, &target_pubkey)?;
        }
        // Late-grant signal: an already-accepted member re-backfills the
        // library on this height bump. NO photo_changes writes — a grant
        // changes the target's view, not the photo.
        libraries::upsert_view_change(db_tx, target, &payload.library_id)
    }
}

inventory::submit! { &LibraryAccessGrantHandler as &dyn TransactionHandler }

// --- library_access_revoke ---

pub struct LibraryAccessRevokeHandler;

impl TransactionHandler for LibraryAccessRevokeHandler {
    fn name(&self) -> &'static str {
        "library_access_revoke"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<LibraryAccessRevokePayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;
        let user_id = tx.user_id.ok_or(DatabaseError::AuthorizationError)?;

        if !libraries::is_member(db_tx, &payload.library_id, user_id)? {
            return Err(DatabaseError::AuthorizationError);
        }
        // Revoke only converges toward the assertion: the target must be
        // NEITHER member nor invitee. Without this inversion a member
        // could stealth-kick a peer by stripping wraps while the
        // membership row (and the read gate it opens) still stands —
        // removals go through library_remove_member.
        let target = payload.user_id;
        if libraries::is_member(db_tx, &payload.library_id, target)?
            || libraries::is_invitee(db_tx, &payload.library_id, target)?
        {
            return Err(DatabaseError::AuthorizationError);
        }
        let target_pubkey =
            libraries::user_x25519_pubkey(db_tx, target)?.ok_or(DatabaseError::InvalidPayload)?;

        for photo_id in &payload.photo_ids {
            libraries::delete_metadata_access(db_tx, photo_id, target)?;
        }
        for block_id in &payload.data_block_ids {
            libraries::delete_blob_access(db_tx, block_id, &target_pubkey)?;
        }
        Ok(())
    }
}

inventory::submit! { &LibraryAccessRevokeHandler as &dyn TransactionHandler }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelopes::{
        MetadataAccessEntry, PhotoAddEntry, PhotoCleanupExpiredPayload, PhotoDeleteEntry,
        PhotoEditContentEntry, PhotoFavoriteEntry, PhotoResourceOp, PhotoRestoreEntry,
        PhotoUnfavoriteEntry,
    };
    use hopnet_common::{Blake3Hash, CustomUUID};
    use hopnet_projection::Projection;
    use rusqlite::Connection;

    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(
            "CREATE TABLE users (user_id INTEGER PRIMARY KEY, x25519_pubkey BLOB);
             CREATE TABLE consensus_meta (key TEXT PRIMARY KEY, value BLOB);",
        )
        .unwrap();
        hopnet_storage::store::install_schema(&conn).unwrap();
        crate::db::install_schema(&conn).unwrap();
        // Real-shaped 32-byte pubkeys: the membership handlers resolve and
        // validate them (a short blob is a RecallError by design).
        for uid in [1, 2, 3] {
            conn.execute(
                "INSERT INTO users (user_id, x25519_pubkey) VALUES (?1, ?2)",
                rusqlite::params![uid, vec![uid as u8; 32]],
            )
            .unwrap();
        }
        // Host-owned device_tokens mirror (src/db/shared.rs DDL) — the
        // responsibility table FKs it and the claim handler reads it.
        conn.execute_batch(
            "CREATE TABLE device_tokens (
                 id TEXT PRIMARY KEY,
                 user_id INTEGER NOT NULL,
                 api_key_hash BLOB NOT NULL,
                 encrypted_device_name TEXT NOT NULL,
                 wrapped_user_key BLOB NOT NULL,
                 FOREIGN KEY (user_id) REFERENCES users(user_id)
             );
             INSERT INTO device_tokens VALUES ('00000000-0000-0000-0000-0000000000d1', 1, x'00', 'enc', x'00');
             INSERT INTO device_tokens VALUES ('00000000-0000-0000-0000-0000000000d2', 1, x'00', 'enc', x'00');
             INSERT INTO device_tokens VALUES ('00000000-0000-0000-0000-0000000000d9', 2, x'00', 'enc', x'00');",
        )
        .unwrap();
        conn
    }

    fn ctx(fragments_dir: &str) -> HandlerCtx<'_> {
        HandlerCtx {
            fragments_dir,
            node_id: None,
            height: 0,
            notifier: &NoopNotifier,
            work: &NoopScheduler,
        }
    }

    fn make_blob_op(blob_id: CustomUUID) -> hopnet_storage::store::BlobInsertOp {
        hopnet_storage::store::BlobInsertOp {
            blob_id,
            integrity_hash: Blake3Hash::from_bytes([0xCC; 32]),
            added_bytes: 0,
            file_size: 100,
            fragments: vec![],
            access: vec![],
        }
    }

    struct NoopNotifier;
    impl hopnet_projection::ChangeNotifier for NoopNotifier {
        fn files_changed(&self) {}
        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()> {
            tokio::sync::broadcast::channel(1).1
        }
    }

    struct NoopScheduler;
    impl hopnet_projection::WorkScheduler for NoopScheduler {
        fn schedule(&self, _subsystem: &str, _key: String) {}
    }

    // --- Helpers ---

    fn add_payload_bytes(
        photo_id: CustomUUID,
        uploaded_by: i32,
        blob_id: CustomUUID,
        op_id: CustomUUID,
        library_id: Option<CustomUUID>,
    ) -> Vec<u8> {
        let payload = PhotoAddPayload {
            entries: vec![PhotoAddEntry {
                photo_id,
                library_id,
                uploaded_by,
                encrypted_metadata: b"enc_meta".to_vec(),
                metadata_nonce: [0u8; 12],
                resources: vec![PhotoResourceOp {
                    resource_type: 0,
                    op: make_blob_op(blob_id),
                }],
                metadata_access: vec![MetadataAccessEntry {
                    user_id: uploaded_by,
                    ephemeral_pubkey: [0x42; 32],
                    encrypted_metadata_key: vec![0xFF; 48],
                }],
                operation_id: op_id,
                cloud_fingerprint: None,
            }],
        };
        bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap()
    }

    /// Run a handler's validate pass on a fresh tx. The tx is rolled back
    /// implicitly — production does validate in a SAVEPOINT.
    fn validate(
        conn: &Connection,
        handler: &dyn TransactionHandler,
        function: &str,
        payload: &[u8],
        user_id: Option<i32>,
    ) -> HandlerResult {
        let tx = conn.unchecked_transaction().unwrap();
        let meta = TxMeta {
            function,
            payload,
            submitter_node: 0,
            user_id,
        };
        handler.process(&meta, false, &ctx("/tmp/fragments"), &tx)
    }

    /// Run a handler's execute (apply) pass on a fresh tx and commit.
    fn apply(
        conn: &Connection,
        handler: &dyn TransactionHandler,
        function: &str,
        payload: &[u8],
        user_id: Option<i32>,
    ) {
        let tx = conn.unchecked_transaction().unwrap();
        let meta = TxMeta {
            function,
            payload,
            submitter_node: 0,
            user_id,
        };
        handler
            .process(&meta, true, &ctx("/tmp/fragments"), &tx)
            .unwrap();
        tx.commit().unwrap();
    }

    // --- Tests ---

    /// photo_add: validate passes cleanly, apply persists.
    #[test]
    fn photo_add_validate_then_apply() {
        let conn = fixture();
        let bytes = add_payload_bytes(
            CustomUUID::retention_cutoff(0),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        validate(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1)).unwrap();
        apply(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));

        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM photos", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    /// photo_add: uploaded_by must equal tx.user_id.
    #[test]
    fn photo_add_rejects_wrong_uploader() {
        let conn = fixture();
        let bytes = add_payload_bytes(
            CustomUUID::retention_cutoff(0),
            99,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        let result = validate(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));
        assert!(matches!(result, Err(DatabaseError::AuthorizationError)));
    }

    // Should: reject a photo_add into a nonexistent library (no
    // membership can exist for it) with an authorization error.
    #[test]
    fn photo_add_rejects_nonexistent_library() {
        let conn = fixture();
        let bytes = add_payload_bytes(
            CustomUUID::retention_cutoff(0),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            Some(CustomUUID::retention_cutoff(99)),
        );
        let result = validate(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));
        assert!(
            matches!(result, Err(DatabaseError::AuthorizationError)),
            "non-member add into an unknown library must be rejected"
        );
    }

    /// photo_delete: validates and tombstones in separate passes.
    #[test]
    fn photo_delete_tombstones() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let del_payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        validate(
            &conn,
            &PhotoDeleteHandler,
            "photo_delete",
            &del_bytes,
            Some(1),
        )
        .unwrap();
        apply(
            &conn,
            &PhotoDeleteHandler,
            "photo_delete",
            &del_bytes,
            Some(1),
        );

        let deleted_at: Option<String> = conn
            .query_row(
                "SELECT deleted_at FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(deleted_at.is_some());
    }

    /// photo_delete: non-owner must be rejected.
    #[test]
    fn photo_delete_rejects_non_owner() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let del_payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        let result = validate(
            &conn,
            &PhotoDeleteHandler,
            "photo_delete",
            &del_bytes,
            Some(2),
        );
        assert!(matches!(result, Err(DatabaseError::AuthorizationError)));
    }

    /// photo_restore: active (not tombstoned) photo must fail.
    #[test]
    fn restore_active_photo_fails() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let restore_payload = PhotoRestorePayload {
            entries: vec![PhotoRestoreEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let restore_bytes =
            bincode::serde::encode_to_vec(&restore_payload, bincode::config::standard()).unwrap();
        let result = validate(
            &conn,
            &PhotoRestoreHandler,
            "photo_restore",
            &restore_bytes,
            Some(1),
        );
        assert!(result.is_err(), "restore of active photo must fail");
    }

    /// Non-owner restore must be rejected (mirrors non-owner delete).
    #[test]
    fn restore_rejects_non_owner() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));
        let tx = conn.unchecked_transaction().unwrap();
        soft_delete_photo(
            &tx,
            &photo_id,
            1,
            "2025-06-01T00:00:00Z",
            None,
            &CustomUUID::retention_cutoff(3),
        )
        .unwrap();
        tx.commit().unwrap();

        let restore_payload = PhotoRestorePayload {
            entries: vec![PhotoRestoreEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(4),
            }],
        };
        let restore_bytes =
            bincode::serde::encode_to_vec(&restore_payload, bincode::config::standard()).unwrap();
        let result = validate(
            &conn,
            &PhotoRestoreHandler,
            "photo_restore",
            &restore_bytes,
            Some(2),
        );
        assert!(matches!(result, Err(DatabaseError::AuthorizationError)));
    }

    /// Handler-written deleted_at must be parseable by SQLite's datetime()
    /// (the cleanup query at photos.md:382-385 uses datetime(deleted_at, '+30 days')).
    /// to_rfc3339() produces "...+00:00" or "...Z" — both valid for datetime().
    #[test]
    fn handler_written_deleted_at_is_sqlite_datetime_parseable() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let del_payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        apply(
            &conn,
            &PhotoDeleteHandler,
            "photo_delete",
            &del_bytes,
            Some(1),
        );

        let parsed: Option<String> = conn
            .query_row(
                "SELECT datetime(deleted_at, '+30 days') FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            parsed.is_some(),
            "datetime(deleted_at, '+30 days') must be non-NULL for cleanup query"
        );
    }

    /// committed_blob_ids on PhotosProjection extracts all blob ids from
    /// a photo_add payload.
    #[test]
    fn committed_blob_ids_photo_add() {
        let payload = PhotoAddPayload {
            entries: vec![
                PhotoAddEntry {
                    photo_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                    library_id: None,
                    uploaded_by: 1,
                    encrypted_metadata: vec![],
                    metadata_nonce: [0u8; 12],
                    resources: vec![PhotoResourceOp {
                        resource_type: 0,
                        op: make_blob_op("00000000-0000-0000-0000-0000000000a1".parse().unwrap()),
                    }],
                    metadata_access: vec![],
                    operation_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
                    cloud_fingerprint: None,
                },
                PhotoAddEntry {
                    photo_id: "00000000-0000-0000-0000-000000000003".parse().unwrap(),
                    library_id: None,
                    uploaded_by: 1,
                    encrypted_metadata: vec![],
                    metadata_nonce: [0u8; 12],
                    resources: vec![
                        PhotoResourceOp {
                            resource_type: 0,
                            op: make_blob_op(
                                "00000000-0000-0000-0000-0000000000b1".parse().unwrap(),
                            ),
                        },
                        PhotoResourceOp {
                            resource_type: 2,
                            op: make_blob_op(
                                "00000000-0000-0000-0000-0000000000b2".parse().unwrap(),
                            ),
                        },
                    ],
                    metadata_access: vec![],
                    operation_id: "00000000-0000-0000-0000-000000000004".parse().unwrap(),
                    cloud_fingerprint: None,
                },
            ],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();

        let proj = crate::PhotosProjection;
        let ids = proj.committed_blob_ids("photo_add", &encoded);

        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0].to_string(), "00000000-0000-0000-0000-0000000000a1");
        assert_eq!(ids[1].to_string(), "00000000-0000-0000-0000-0000000000b1");
        assert_eq!(ids[2].to_string(), "00000000-0000-0000-0000-0000000000b2");
    }

    /// committed_blob_ids returns empty for unknown functions + garbage.
    #[test]
    fn committed_blob_ids_unknown_or_garbage_is_empty() {
        let proj = crate::PhotosProjection;
        assert!(proj.committed_blob_ids("photo_delete", &[]).is_empty());
        assert!(proj.committed_blob_ids("photo_restore", &[]).is_empty());
        assert!(
            proj.committed_blob_ids("photo_edit_content", &[])
                .is_empty()
        );
        assert!(
            proj.committed_blob_ids("photo_add", b"not valid bincode")
                .is_empty()
        );
    }

    /// committed_blob_ids on PhotosProjection extracts blob ids from
    /// a photo_edit_content payload (both primary edit + thumbnails).
    #[test]
    fn committed_blob_ids_photo_edit_content() {
        let payload = crate::envelopes::PhotoEditContentPayload {
            entries: vec![PhotoEditContentEntry {
                photo_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                resources: vec![
                    PhotoResourceOp {
                        resource_type: 1,
                        op: make_blob_op("00000000-0000-0000-0000-000000000e01".parse().unwrap()),
                    },
                    PhotoResourceOp {
                        resource_type: 5,
                        op: make_blob_op("00000000-0000-0000-0000-000000000e05".parse().unwrap()),
                    },
                ],
                encrypted_metadata: None,
                metadata_nonce: None,
                operation_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
            }],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let ids = crate::PhotosProjection.committed_blob_ids("photo_edit_content", &encoded);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].to_string(), "00000000-0000-0000-0000-000000000e01");
        assert_eq!(ids[1].to_string(), "00000000-0000-0000-0000-000000000e05");
    }

    /// photo_add writes a photo_metadata_access row for the uploader.
    #[test]
    fn photo_add_writes_metadata_access_row() {
        let conn = fixture();
        let bytes = add_payload_bytes(
            CustomUUID::retention_cutoff(0),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));

        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM photo_metadata_access WHERE user_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "uploader must have a photo_metadata_access row");
    }

    /// photo_cleanup_expired hard-deletes a tombstoned photo beyond the
    /// 30-day window. The scan_cutoff rides the payload — all validators
    /// apply the same predicate.
    #[test]
    fn cleanup_expired_hard_deletes() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        // Soft-delete with a date 40 days ago (well beyond 30d window).
        let del_payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        apply(
            &conn,
            &PhotoDeleteHandler,
            "photo_delete",
            &del_bytes,
            Some(1),
        );

        // Hard-set deleted_at to 40 days ago so the cutoff check passes.
        conn.execute(
            "UPDATE photos SET deleted_at = '2025-06-01T00:00:00Z' WHERE id = ?1",
            rusqlite::params![photo_id],
        )
        .unwrap();

        // Cleanup with a cutoff after the 30-day window.
        let cleanup_payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![photo_id.clone()],
            scan_cutoff: "2099-01-01T00:00:00Z".into(),
        };
        let cleanup_bytes =
            bincode::serde::encode_to_vec(&cleanup_payload, bincode::config::standard()).unwrap();
        validate(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        )
        .unwrap();
        apply(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        );

        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!exists, "expired photo must be hard-deleted");
    }

    /// cleanup skips a photo whose 30-day window hasn't elapsed yet.
    #[test]
    fn cleanup_skips_within_window() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        // Tombstone with a date only 5 days ago — still within the 30d window.
        conn.execute(
            "UPDATE photos SET deleted_at = '2026-07-23T00:00:00Z' WHERE id = ?1",
            rusqlite::params![photo_id],
        )
        .unwrap();

        // Cutoff also 5 days ago — window not elapsed.
        let cleanup_payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![photo_id.clone()],
            scan_cutoff: "2026-07-23T00:00:00Z".into(),
        };
        let cleanup_bytes =
            bincode::serde::encode_to_vec(&cleanup_payload, bincode::config::standard()).unwrap();
        apply(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        );

        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM photos WHERE id = ?1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(exists, "within-window photo must survive cleanup");
    }

    /// cleanup idempotently skips a missing photo (another node's earlier
    /// tx already deleted it).
    #[test]
    fn cleanup_skips_missing_photo() {
        let conn = fixture();
        let cleanup_payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![CustomUUID::retention_cutoff(99)],
            scan_cutoff: "2099-01-01T00:00:00Z".into(),
        };
        let cleanup_bytes =
            bincode::serde::encode_to_vec(&cleanup_payload, bincode::config::standard()).unwrap();
        validate(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        )
        .unwrap();
        apply(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            None,
        );
        // No error, no side effects.
    }

    /// User-signed cleanup must be rejected.
    #[test]
    fn cleanup_rejects_user_signed() {
        let conn = fixture();
        let cleanup_payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![CustomUUID::retention_cutoff(0)],
            scan_cutoff: "2099-01-01T00:00:00Z".into(),
        };
        let cleanup_bytes =
            bincode::serde::encode_to_vec(&cleanup_payload, bincode::config::standard()).unwrap();
        let result = validate(
            &conn,
            &PhotoCleanupExpiredHandler,
            "photo_cleanup_expired",
            &cleanup_bytes,
            Some(1),
        );
        assert!(matches!(result, Err(DatabaseError::AuthorizationError)));
    }

    // --- edit handler tests ---

    #[test]
    fn photo_edit_content_applies() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let new_blob_id = CustomUUID::retention_cutoff(3);
        let edit_payload = crate::envelopes::PhotoEditContentPayload {
            entries: vec![PhotoEditContentEntry {
                photo_id: photo_id.clone(),
                resources: vec![PhotoResourceOp {
                    resource_type: 1,
                    op: make_blob_op(new_blob_id.clone()),
                }],
                encrypted_metadata: None,
                metadata_nonce: None,
                operation_id: CustomUUID::retention_cutoff(4),
            }],
        };
        let edit_bytes =
            bincode::serde::encode_to_vec(&edit_payload, bincode::config::standard()).unwrap();
        validate(
            &conn,
            &PhotoEditContentHandler,
            "photo_edit_content",
            &edit_bytes,
            Some(1),
        )
        .unwrap();
        apply(
            &conn,
            &PhotoEditContentHandler,
            "photo_edit_content",
            &edit_bytes,
            Some(1),
        );

        let res: String = conn
            .query_row(
                "SELECT data_block_id FROM photo_resources WHERE photo_id=?1 AND resource_type=1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(res, new_blob_id.to_string());
    }

    #[test]
    fn photo_edit_content_rejects_tombstoned() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));
        // Tombstone.
        let del_payload = crate::envelopes::PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(5),
            }],
        };
        let del_bytes =
            bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap();
        apply(
            &conn,
            &PhotoDeleteHandler,
            "photo_delete",
            &del_bytes,
            Some(1),
        );

        let edit_payload = crate::envelopes::PhotoEditContentPayload {
            entries: vec![PhotoEditContentEntry {
                photo_id: photo_id.clone(),
                resources: vec![PhotoResourceOp {
                    resource_type: 1,
                    op: make_blob_op(CustomUUID::retention_cutoff(6)),
                }],
                encrypted_metadata: None,
                metadata_nonce: None,
                operation_id: CustomUUID::retention_cutoff(7),
            }],
        };
        let edit_bytes =
            bincode::serde::encode_to_vec(&edit_payload, bincode::config::standard()).unwrap();
        let result = validate(
            &conn,
            &PhotoEditContentHandler,
            "photo_edit_content",
            &edit_bytes,
            Some(1),
        );
        assert!(matches!(result, Err(DatabaseError::ConflictError)));
    }

    #[test]
    fn photo_favorite_inserts() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let fav_payload = crate::envelopes::PhotoFavoritePayload {
            entries: vec![PhotoFavoriteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let fav_bytes =
            bincode::serde::encode_to_vec(&fav_payload, bincode::config::standard()).unwrap();
        apply(
            &conn,
            &PhotoFavoriteHandler,
            "photo_favorite",
            &fav_bytes,
            Some(1),
        );

        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM photo_favorites WHERE photo_id=?1 AND user_id=1",
                rusqlite::params![photo_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn photo_favorite_rejects_tombstoned() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));
        let del_payload = crate::envelopes::PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(4),
            }],
        };
        apply(
            &conn,
            &PhotoDeleteHandler,
            "photo_delete",
            &bincode::serde::encode_to_vec(&del_payload, bincode::config::standard()).unwrap(),
            Some(1),
        );

        let fav_payload = crate::envelopes::PhotoFavoritePayload {
            entries: vec![PhotoFavoriteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(5),
            }],
        };
        let fav_bytes =
            bincode::serde::encode_to_vec(&fav_payload, bincode::config::standard()).unwrap();
        let result = validate(
            &conn,
            &PhotoFavoriteHandler,
            "photo_favorite",
            &fav_bytes,
            Some(1),
        );
        assert!(matches!(result, Err(DatabaseError::ConflictError)));
    }

    #[test]
    fn photo_unfavorite_is_idempotent() {
        let conn = fixture();
        let photo_id = CustomUUID::retention_cutoff(0);
        let add_bytes = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            None,
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add_bytes, Some(1));

        let unfav_payload = crate::envelopes::PhotoUnfavoritePayload {
            entries: vec![PhotoUnfavoriteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(3),
            }],
        };
        let unfav_bytes =
            bincode::serde::encode_to_vec(&unfav_payload, bincode::config::standard()).unwrap();
        // Delete non-existent favorite (idempotent).
        validate(
            &conn,
            &PhotoUnfavoriteHandler,
            "photo_unfavorite",
            &unfav_bytes,
            Some(1),
        )
        .unwrap();
        apply(
            &conn,
            &PhotoUnfavoriteHandler,
            "photo_unfavorite",
            &unfav_bytes,
            Some(1),
        );
    }

    // --- cloud_fingerprint + photo_ingress_claim ---

    fn add_payload_with_fp(
        photo_id: CustomUUID,
        blob_id: CustomUUID,
        op_id: CustomUUID,
        fingerprint: Option<[u8; 32]>,
    ) -> Vec<u8> {
        let payload = PhotoAddPayload {
            entries: vec![PhotoAddEntry {
                photo_id,
                library_id: None,
                uploaded_by: 1,
                encrypted_metadata: b"enc_meta".to_vec(),
                metadata_nonce: [0u8; 12],
                resources: vec![PhotoResourceOp {
                    resource_type: 0,
                    op: make_blob_op(blob_id),
                }],
                metadata_access: vec![MetadataAccessEntry {
                    user_id: 1,
                    ephemeral_pubkey: [0x42; 32],
                    encrypted_metadata_key: vec![0xFF; 48],
                }],
                operation_id: op_id,
                cloud_fingerprint: fingerprint,
            }],
        };
        bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap()
    }

    fn claim_bytes(device_id: &str) -> Vec<u8> {
        claim_bytes_scoped(device_id, None)
    }

    fn claim_bytes_scoped(device_id: &str, library_id: Option<&str>) -> Vec<u8> {
        let payload = crate::envelopes::PhotoIngressClaimPayload {
            device_id: device_id.parse().unwrap(),
            operation_id: CustomUUID::new(None),
            library_id: library_id.map(|l| l.parse().unwrap()),
        };
        bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap()
    }

    fn read_holder(conn: &Connection, user_id: i32) -> Option<String> {
        read_holder_scoped(conn, user_id, None)
    }

    fn read_holder_scoped(
        conn: &Connection,
        user_id: i32,
        library_id: Option<&str>,
    ) -> Option<String> {
        match library_id {
            None => conn.query_row(
                "SELECT device_id FROM photo_ingress_responsibility
                 WHERE user_id = ?1 AND library_id IS NULL",
                rusqlite::params![user_id],
                |r| r.get(0),
            ),
            Some(lib) => conn.query_row(
                "SELECT device_id FROM photo_ingress_responsibility
                 WHERE user_id = ?1 AND library_id = ?2",
                rusqlite::params![user_id, lib],
                |r| r.get(0),
            ),
        }
        .ok()
    }

    // Should: persist the wire fingerprint as lowercase hex on the photos row.
    #[test]
    fn photo_add_persists_fingerprint_hex() {
        let conn = fixture();
        let bytes = add_payload_with_fp(
            CustomUUID::retention_cutoff(0),
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            Some([0xAB; 32]),
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));

        let stored: String = conn
            .query_row("SELECT cloud_fingerprint FROM photos", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stored, "ab".repeat(32));
    }

    // Impact: the UNIQUE backstop is what makes admission races harmless —
    // the losing device's tx must fail deterministically on every validator
    // so it re-resolves and adopts instead of committing a duplicate.
    // Should: reject a second photo_add carrying an already-committed
    // fingerprint under a different photo_id.
    #[test]
    fn photo_add_duplicate_fingerprint_rejected() {
        let conn = fixture();
        let first = add_payload_with_fp(
            CustomUUID::retention_cutoff(0),
            CustomUUID::retention_cutoff(1),
            CustomUUID::retention_cutoff(2),
            Some([0xAB; 32]),
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &first, Some(1));

        let loser = add_payload_with_fp(
            CustomUUID::retention_cutoff(10),
            CustomUUID::retention_cutoff(11),
            CustomUUID::retention_cutoff(12),
            Some([0xAB; 32]),
        );
        let result = validate(&conn, &PhotoAddHandler, "photo_add", &loser, Some(1));
        assert!(
            matches!(result, Err(DatabaseError::InsertError)),
            "duplicate fingerprint must fail the insert, got {result:?}"
        );
    }

    // Should: admit any number of NULL-fingerprint photos (local-only
    // assets are exempt from dedupe).
    #[test]
    fn photo_add_null_fingerprints_coexist() {
        let conn = fixture();
        for i in 0..2u32 {
            let bytes = add_payload_with_fp(
                CustomUUID::retention_cutoff(i as i64 * 10),
                CustomUUID::retention_cutoff(i as i64 * 10 + 1),
                CustomUUID::retention_cutoff(i as i64 * 10 + 2),
                None,
            );
            apply(&conn, &PhotoAddHandler, "photo_add", &bytes, Some(1));
        }
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM photos", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    // Should: claim responsibility for an owned device, and transfer it by
    // re-claiming a different owned device (upsert — last claim wins).
    #[test]
    fn ingress_claim_upserts_and_transfers() {
        let conn = fixture();
        let dev_a = "00000000-0000-0000-0000-0000000000d1";
        let dev_b = "00000000-0000-0000-0000-0000000000d2";

        let bytes = claim_bytes(dev_a);
        validate(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &bytes,
            Some(1),
        )
        .unwrap();
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &bytes,
            Some(1),
        );
        assert_eq!(read_holder(&conn, 1).as_deref(), Some(dev_a));

        let bytes = claim_bytes(dev_b);
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &bytes,
            Some(1),
        );
        assert_eq!(
            read_holder(&conn, 1).as_deref(),
            Some(dev_b),
            "transfer = re-claim"
        );
    }

    // Impact: the ownership check is the only thing stopping one user from
    // hijacking another user's publish pipeline onto their own device.
    // Should not: allow claiming a device that belongs to another user, an
    // unknown device, or claiming without user auth.
    #[test]
    fn ingress_claim_rejects_foreign_missing_and_unauthed() {
        let conn = fixture();

        // Device d9 belongs to user 2; user 1 must not claim it.
        let foreign = claim_bytes("00000000-0000-0000-0000-0000000000d9");
        assert!(matches!(
            validate(
                &conn,
                &PhotoIngressClaimHandler,
                "photo_ingress_claim",
                &foreign,
                Some(1)
            ),
            Err(DatabaseError::AuthorizationError)
        ));

        let missing = claim_bytes("00000000-0000-0000-0000-0000000000ee");
        assert!(matches!(
            validate(
                &conn,
                &PhotoIngressClaimHandler,
                "photo_ingress_claim",
                &missing,
                Some(1)
            ),
            Err(DatabaseError::AuthorizationError)
        ));

        let owned = claim_bytes("00000000-0000-0000-0000-0000000000d1");
        assert!(matches!(
            validate(
                &conn,
                &PhotoIngressClaimHandler,
                "photo_ingress_claim",
                &owned,
                None
            ),
            Err(DatabaseError::AuthorizationError)
        ));

        assert_eq!(read_holder(&conn, 1), None, "no claim may have landed");
    }

    // Should: reject a shared-scope claim from a non-member and accept the
    // same claim once the user has joined the library.
    // Should not: change personal-claim validation in any way.
    #[test]
    fn shared_claim_requires_membership() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x300);
        let lib_s = lib.to_string();

        // User 2 owns d9 but is not a member yet.
        let bytes = claim_bytes_scoped("00000000-0000-0000-0000-0000000000d9", Some(&lib_s));
        assert!(matches!(
            validate(
                &conn,
                &PhotoIngressClaimHandler,
                "photo_ingress_claim",
                &bytes,
                Some(2)
            ),
            Err(DatabaseError::AuthorizationError)
        ));

        invite(&conn, &lib, 1, 2);
        accept(&conn, &lib, 2);
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &bytes,
            Some(2),
        );
        assert_eq!(
            read_holder_scoped(&conn, 2, Some(&lib_s)).as_deref(),
            Some("00000000-0000-0000-0000-0000000000d9"),
        );

        // Personal claims never involve membership.
        let personal = claim_bytes("00000000-0000-0000-0000-0000000000d1");
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &personal,
            Some(1),
        );
        assert_eq!(
            read_holder(&conn, 1).as_deref(),
            Some("00000000-0000-0000-0000-0000000000d1"),
        );
    }

    // Should: hold personal + per-library responsibility rows for one user
    // simultaneously, and transfer a shared-scope claim without touching
    // the other scopes.
    // Impact: the upsert's conflict targets must match the partial UNIQUE
    // pair exactly — a mismatch would insert duplicates instead of
    // transferring, and this is the test that would catch it.
    #[test]
    fn per_scope_rows_coexist() {
        let conn = fixture();
        let lib_a = create_library(&conn, 1, 0x310);
        let lib_b = create_library(&conn, 1, 0x320);
        let (a, b) = (lib_a.to_string(), lib_b.to_string());
        let dev1 = "00000000-0000-0000-0000-0000000000d1";
        let dev2 = "00000000-0000-0000-0000-0000000000d2";

        for bytes in [
            claim_bytes(dev1),
            claim_bytes_scoped(dev1, Some(&a)),
            claim_bytes_scoped(dev2, Some(&b)),
        ] {
            apply(
                &conn,
                &PhotoIngressClaimHandler,
                "photo_ingress_claim",
                &bytes,
                Some(1),
            );
        }
        assert_eq!(read_holder(&conn, 1).as_deref(), Some(dev1));
        assert_eq!(
            read_holder_scoped(&conn, 1, Some(&a)).as_deref(),
            Some(dev1)
        );
        assert_eq!(
            read_holder_scoped(&conn, 1, Some(&b)).as_deref(),
            Some(dev2)
        );

        // Transfer lib_a to dev2: personal and lib_b rows must not move.
        let transfer = claim_bytes_scoped(dev2, Some(&a));
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &transfer,
            Some(1),
        );
        assert_eq!(
            read_holder_scoped(&conn, 1, Some(&a)).as_deref(),
            Some(dev2)
        );
        assert_eq!(read_holder(&conn, 1).as_deref(), Some(dev1));
        assert_eq!(
            read_holder_scoped(&conn, 1, Some(&b)).as_deref(),
            Some(dev2)
        );
        assert_eq!(
            count(&conn, "SELECT COUNT(*) FROM photo_ingress_responsibility"),
            3,
            "transfer must never insert a duplicate scope row"
        );
    }

    // Should: let two members hold responsibility for the same library
    // independently, one row each.
    // Impact: responsibility partitions publishing per member; dedup of
    // the actual photos across members is the fingerprint pair's job.
    #[test]
    fn two_members_claim_same_library() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x330);
        invite(&conn, &lib, 1, 2);
        accept(&conn, &lib, 2);
        let lib_s = lib.to_string();

        let one = claim_bytes_scoped("00000000-0000-0000-0000-0000000000d1", Some(&lib_s));
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &one,
            Some(1),
        );
        let two = claim_bytes_scoped("00000000-0000-0000-0000-0000000000d9", Some(&lib_s));
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &two,
            Some(2),
        );

        assert_eq!(
            read_holder_scoped(&conn, 1, Some(&lib_s)).as_deref(),
            Some("00000000-0000-0000-0000-0000000000d1"),
        );
        assert_eq!(
            read_holder_scoped(&conn, 2, Some(&lib_s)).as_deref(),
            Some("00000000-0000-0000-0000-0000000000d9"),
        );
    }

    // Should: dissolve the removed member's responsibility row for that
    // library on kick, leaving their personal claim intact.
    // Impact: without this, a kicked member's daemon keeps passing the
    // thin-client tx gate for a library it can no longer publish into.
    #[test]
    fn kick_dissolves_scope_claim() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x340);
        invite(&conn, &lib, 1, 2);
        accept(&conn, &lib, 2);
        let lib_s = lib.to_string();

        let personal = claim_bytes("00000000-0000-0000-0000-0000000000d9");
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &personal,
            Some(2),
        );
        let scoped = claim_bytes_scoped("00000000-0000-0000-0000-0000000000d9", Some(&lib_s));
        apply(
            &conn,
            &PhotoIngressClaimHandler,
            "photo_ingress_claim",
            &scoped,
            Some(2),
        );

        let kick = enc(&LibraryRemoveMemberPayload {
            library_id: lib.clone(),
            user_id: 2,
            operation_id: CustomUUID::retention_cutoff(0x341),
        });
        apply(
            &conn,
            &LibraryRemoveMemberHandler,
            "library_remove_member",
            &kick,
            Some(1),
        );

        assert_eq!(read_holder_scoped(&conn, 2, Some(&lib_s)), None);
        assert_eq!(
            read_holder(&conn, 2).as_deref(),
            Some("00000000-0000-0000-0000-0000000000d9"),
            "personal claim must survive a library kick"
        );
    }

    // --- Shared-library membership lifecycle ---

    use crate::envelopes::{
        CreateSharedLibraryPayload, LibraryAccessGrantPayload, LibraryAccessRevokePayload,
        LibraryBlobGrant, LibraryInviteAcceptPayload, LibraryInviteDeclinePayload,
        LibraryInvitePayload, LibraryKeyWrap, LibraryMetadataGrant, LibraryRemoveMemberPayload,
    };

    fn enc<T: serde::Serialize>(payload: &T) -> Vec<u8> {
        bincode::serde::encode_to_vec(payload, bincode::config::standard()).unwrap()
    }

    fn key_wrap(user_id: i32) -> LibraryKeyWrap {
        LibraryKeyWrap {
            user_id,
            ephemeral_pubkey: [0x11; 32],
            wrapped_key: vec![0x22; 48],
        }
    }

    /// Apply create_shared_library for `creator`; returns the library id.
    fn create_library(conn: &Connection, creator: i32, seq: i64) -> CustomUUID {
        let lib = CustomUUID::retention_cutoff(seq);
        let bytes = enc(&CreateSharedLibraryPayload {
            library_id: lib.clone(),
            encrypted_name: vec![0xEE; 8],
            name_nonce: [0u8; 12],
            creator_key: key_wrap(creator),
            operation_id: CustomUUID::retention_cutoff(seq + 1),
        });
        apply(
            conn,
            &CreateSharedLibraryHandler,
            "create_shared_library",
            &bytes,
            Some(creator),
        );
        lib
    }

    fn invite(conn: &Connection, lib: &CustomUUID, inviter: i32, invitee: i32) {
        let bytes = enc(&LibraryInvitePayload {
            library_id: lib.clone(),
            invitee: key_wrap(invitee),
            operation_id: CustomUUID::retention_cutoff(900),
        });
        apply(
            conn,
            &LibraryInviteHandler,
            "library_invite",
            &bytes,
            Some(inviter),
        );
    }

    fn accept(conn: &Connection, lib: &CustomUUID, invitee: i32) {
        let bytes = enc(&LibraryInviteAcceptPayload {
            library_id: lib.clone(),
            operation_id: CustomUUID::retention_cutoff(901),
        });
        apply(
            conn,
            &LibraryInviteAcceptHandler,
            "library_invite_accept",
            &bytes,
            Some(invitee),
        );
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    // Should: create the library row, the creator's membership, and the
    // creator's key wrap in one op.
    // Should not: accept a creator wrap targeting another user.
    // Impact: creation is the only path that mints a library — a foreign
    // creator wrap would hand the library key namespace to a non-member.
    #[test]
    fn create_shared_library_mints_row_member_and_key() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x100);
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM shared_libraries"), 1);
        assert!(crate::db::libraries::is_member(&conn, &lib, 1).unwrap());
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM shared_library_keys"), 1);

        let foreign = enc(&CreateSharedLibraryPayload {
            library_id: CustomUUID::retention_cutoff(0x200),
            encrypted_name: vec![0xEE; 8],
            name_nonce: [0u8; 12],
            creator_key: key_wrap(2), // wrap for someone else
            operation_id: CustomUUID::retention_cutoff(0x201),
        });
        assert!(matches!(
            validate(
                &conn,
                &CreateSharedLibraryHandler,
                "create_shared_library",
                &foreign,
                Some(1)
            ),
            Err(DatabaseError::AuthorizationError)
        ));
    }

    // Should: let only members invite, refuse duplicate/self/member/unknown
    // invitees, and park the invitee's key wrap on the invite row.
    #[test]
    fn library_invite_authz_matrix() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x100);

        // Non-member cannot invite.
        let by_stranger = enc(&LibraryInvitePayload {
            library_id: lib.clone(),
            invitee: key_wrap(2),
            operation_id: CustomUUID::retention_cutoff(0x300),
        });
        assert!(matches!(
            validate(
                &conn,
                &LibraryInviteHandler,
                "library_invite",
                &by_stranger,
                Some(2)
            ),
            Err(DatabaseError::AuthorizationError)
        ));
        // Self-invite and member-invite are conflicts.
        let self_invite = enc(&LibraryInvitePayload {
            library_id: lib.clone(),
            invitee: key_wrap(1),
            operation_id: CustomUUID::retention_cutoff(0x301),
        });
        assert!(matches!(
            validate(
                &conn,
                &LibraryInviteHandler,
                "library_invite",
                &self_invite,
                Some(1)
            ),
            Err(DatabaseError::ConflictError)
        ));
        // Unknown mesh user is rejected.
        let unknown = enc(&LibraryInvitePayload {
            library_id: lib.clone(),
            invitee: key_wrap(99),
            operation_id: CustomUUID::retention_cutoff(0x302),
        });
        assert!(matches!(
            validate(
                &conn,
                &LibraryInviteHandler,
                "library_invite",
                &unknown,
                Some(1)
            ),
            Err(DatabaseError::InvalidPayload)
        ));

        invite(&conn, &lib, 1, 2);
        assert!(crate::db::libraries::is_invitee(&conn, &lib, 2).unwrap());
        // Duplicate invite conflicts.
        let dup = enc(&LibraryInvitePayload {
            library_id: lib.clone(),
            invitee: key_wrap(2),
            operation_id: CustomUUID::retention_cutoff(0x303),
        });
        assert!(matches!(
            validate(
                &conn,
                &LibraryInviteHandler,
                "library_invite",
                &dup,
                Some(1)
            ),
            Err(DatabaseError::ConflictError)
        ));
    }

    // Should: on accept, insert membership, promote the invite wrap into
    // shared_library_keys, delete the invite, and write the invitee's
    // view-change signal.
    // Should not: honor a second delivery (invite gone → NotFound) or an
    // accept from a user who was never invited.
    // Impact: accept is the consent boundary — membership (and therefore
    // the read gate) must open only on the invitee's own signature.
    #[test]
    fn invite_accept_promotes_membership_and_signals() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x100);
        invite(&conn, &lib, 1, 2);

        // A never-invited user cannot accept.
        let bytes = enc(&LibraryInviteAcceptPayload {
            library_id: lib.clone(),
            operation_id: CustomUUID::retention_cutoff(0x400),
        });
        assert!(matches!(
            validate(
                &conn,
                &LibraryInviteAcceptHandler,
                "library_invite_accept",
                &bytes,
                Some(3)
            ),
            Err(DatabaseError::NotFound)
        ));

        accept(&conn, &lib, 2);
        assert!(crate::db::libraries::is_member(&conn, &lib, 2).unwrap());
        assert!(!crate::db::libraries::is_invitee(&conn, &lib, 2).unwrap());
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM shared_library_keys WHERE user_id = 2"
            ),
            1,
            "invite wrap promoted"
        );
        assert_eq!(
            crate::db::libraries::read_view_changes(&conn, 2)
                .unwrap()
                .len(),
            1,
            "backfill signal written"
        );
        // Re-delivery: invite row is gone.
        assert!(matches!(
            validate(
                &conn,
                &LibraryInviteAcceptHandler,
                "library_invite_accept",
                &bytes,
                Some(2)
            ),
            Err(DatabaseError::NotFound)
        ));
    }

    // Should: allow the invitee to refuse and any member to retract;
    // refuse strangers.
    #[test]
    fn invite_decline_authz() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x100);
        invite(&conn, &lib, 1, 2);

        let bytes = enc(&LibraryInviteDeclinePayload {
            library_id: lib.clone(),
            invitee_user_id: 2,
            operation_id: CustomUUID::retention_cutoff(0x500),
        });
        assert!(matches!(
            validate(
                &conn,
                &LibraryInviteDeclineHandler,
                "library_invite_decline",
                &bytes,
                Some(3)
            ),
            Err(DatabaseError::AuthorizationError)
        ));
        apply(
            &conn,
            &LibraryInviteDeclineHandler,
            "library_invite_decline",
            &bytes,
            Some(2),
        );
        assert!(!crate::db::libraries::is_invitee(&conn, &lib, 2).unwrap());
    }

    // Should: let any member remove any member (kick) or themself (leave),
    // clearing membership, key wrap, pending invite, and view signal.
    // Should not: let a non-member remove anyone.
    #[test]
    fn remove_member_kick_and_leave() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x100);
        invite(&conn, &lib, 1, 2);
        accept(&conn, &lib, 2);

        // Non-member cannot kick.
        let by_stranger = enc(&LibraryRemoveMemberPayload {
            library_id: lib.clone(),
            user_id: 2,
            operation_id: CustomUUID::retention_cutoff(0x600),
        });
        assert!(matches!(
            validate(
                &conn,
                &LibraryRemoveMemberHandler,
                "library_remove_member",
                &by_stranger,
                Some(3)
            ),
            Err(DatabaseError::AuthorizationError)
        ));

        // Member 2 kicks member 1 (equal standing).
        let kick = enc(&LibraryRemoveMemberPayload {
            library_id: lib.clone(),
            user_id: 1,
            operation_id: CustomUUID::retention_cutoff(0x601),
        });
        apply(
            &conn,
            &LibraryRemoveMemberHandler,
            "library_remove_member",
            &kick,
            Some(2),
        );
        assert!(!crate::db::libraries::is_member(&conn, &lib, 1).unwrap());
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM shared_library_keys WHERE user_id = 1"
            ),
            0,
            "kicked member's key wrap cleared"
        );

        // Removing someone who is neither member nor invitee → NotFound.
        assert!(matches!(
            validate(
                &conn,
                &LibraryRemoveMemberHandler,
                "library_remove_member",
                &kick,
                Some(2)
            ),
            Err(DatabaseError::NotFound)
        ));
    }

    // Should: grant metadata + blob wraps to a member or invitee, resolve
    // the recipient pubkey from consensus state, and signal the target's
    // view change.
    // Should not: grant to a stranger, grant a tombstoned photo, grant a
    // photo or block from another library, or accept an empty batch.
    // Impact: the grant handler is the write gate for pre-staged invitee
    // access — these rejections are what keeps 'access rows exist early'
    // from ever meaning 'access leaked early'.
    #[test]
    fn access_grant_validation_matrix() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x100);
        invite(&conn, &lib, 1, 2);

        // Member 1 adds a photo into the library (membership path).
        let photo_id = CustomUUID::retention_cutoff(0x700);
        let blob_id = CustomUUID::retention_cutoff(0x701);
        let add = add_payload_bytes(
            photo_id.clone(),
            1,
            blob_id.clone(),
            CustomUUID::retention_cutoff(0x702),
            Some(lib.clone()),
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add, Some(1));

        let grant = |target: i32, photo: &CustomUUID, block: &CustomUUID| {
            enc(&LibraryAccessGrantPayload {
                library_id: lib.clone(),
                user_id: target,
                entries: vec![LibraryMetadataGrant {
                    photo_id: photo.clone(),
                    ephemeral_pubkey: [0x77; 32],
                    encrypted_metadata_key: vec![0x88; 48],
                }],
                blob_wraps: vec![LibraryBlobGrant {
                    data_block_id: block.clone(),
                    ephemeral_pubkey: [0x99; 32],
                    wrapped_key: vec![0xAA; 48],
                }],
                operation_id: CustomUUID::retention_cutoff(0x703),
            })
        };

        // Stranger target rejected.
        assert!(matches!(
            validate(
                &conn,
                &LibraryAccessGrantHandler,
                "library_access_grant",
                &grant(3, &photo_id, &blob_id),
                Some(1)
            ),
            Err(DatabaseError::AuthorizationError)
        ));
        // Foreign photo rejected.
        assert!(matches!(
            validate(
                &conn,
                &LibraryAccessGrantHandler,
                "library_access_grant",
                &grant(2, &CustomUUID::retention_cutoff(0x7FF), &blob_id),
                Some(1)
            ),
            Err(DatabaseError::ValidationError)
        ));
        // Empty batch rejected.
        let empty = enc(&LibraryAccessGrantPayload {
            library_id: lib.clone(),
            user_id: 2,
            entries: vec![],
            blob_wraps: vec![],
            operation_id: CustomUUID::retention_cutoff(0x704),
        });
        assert!(matches!(
            validate(
                &conn,
                &LibraryAccessGrantHandler,
                "library_access_grant",
                &empty,
                Some(1)
            ),
            Err(DatabaseError::InvalidPayload)
        ));

        // Grant to the pending invitee succeeds and signals.
        apply(
            &conn,
            &LibraryAccessGrantHandler,
            "library_access_grant",
            &grant(2, &photo_id, &blob_id),
            Some(1),
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM photo_metadata_access WHERE user_id = 2"
            ),
            1
        );
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM blob_access"), 1);
        assert_eq!(
            crate::db::libraries::read_view_changes(&conn, 2)
                .unwrap()
                .len(),
            1
        );

        // Tombstone the photo; a fresh grant of it must be rejected.
        let del = enc(&PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::new(None),
            }],
        });
        apply(&conn, &PhotoDeleteHandler, "photo_delete", &del, Some(1));
        assert!(matches!(
            validate(
                &conn,
                &LibraryAccessGrantHandler,
                "library_access_grant",
                &grant(2, &photo_id, &blob_id),
                Some(1)
            ),
            Err(DatabaseError::ValidationError)
        ));
    }

    // Should: revoke access rows only for a user who is neither member nor
    // invitee.
    // Should not: strip a live member's or invitee's wraps — that would be
    // a stealth kick bypassing library_remove_member.
    // Impact: the inversion IS the anti-abuse property of the revoke tx.
    #[test]
    fn access_revoke_requires_departed_target() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x100);
        invite(&conn, &lib, 1, 2);
        accept(&conn, &lib, 2);

        let photo_id = CustomUUID::retention_cutoff(0x800);
        let blob_id = CustomUUID::retention_cutoff(0x801);
        let add = add_payload_bytes(
            photo_id.clone(),
            1,
            blob_id.clone(),
            CustomUUID::retention_cutoff(0x802),
            Some(lib.clone()),
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add, Some(1));
        let grant = enc(&LibraryAccessGrantPayload {
            library_id: lib.clone(),
            user_id: 2,
            entries: vec![LibraryMetadataGrant {
                photo_id: photo_id.clone(),
                ephemeral_pubkey: [0x77; 32],
                encrypted_metadata_key: vec![0x88; 48],
            }],
            blob_wraps: vec![],
            operation_id: CustomUUID::retention_cutoff(0x803),
        });
        apply(
            &conn,
            &LibraryAccessGrantHandler,
            "library_access_grant",
            &grant,
            Some(1),
        );

        let revoke = enc(&LibraryAccessRevokePayload {
            library_id: lib.clone(),
            user_id: 2,
            photo_ids: vec![photo_id.clone()],
            data_block_ids: vec![],
            operation_id: CustomUUID::retention_cutoff(0x804),
        });
        // Live member → rejected.
        assert!(matches!(
            validate(
                &conn,
                &LibraryAccessRevokeHandler,
                "library_access_revoke",
                &revoke,
                Some(1)
            ),
            Err(DatabaseError::AuthorizationError)
        ));

        // Kick, then revoke converges.
        let kick = enc(&LibraryRemoveMemberPayload {
            library_id: lib.clone(),
            user_id: 2,
            operation_id: CustomUUID::retention_cutoff(0x805),
        });
        apply(
            &conn,
            &LibraryRemoveMemberHandler,
            "library_remove_member",
            &kick,
            Some(1),
        );
        apply(
            &conn,
            &LibraryAccessRevokeHandler,
            "library_access_revoke",
            &revoke,
            Some(1),
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM photo_metadata_access WHERE user_id = 2"
            ),
            0,
            "departed user's wraps removed"
        );
    }

    // Should: allow a co-member to soft-delete a shared photo they did not
    // upload (equal standing), and reject a favorite from a non-member.
    // Impact: pins the authz widening — shared photos answer to the
    // member set, not just the uploader; personal photos answer only to
    // their owner.
    #[test]
    fn widened_authz_member_delete_and_nonmember_favorite() {
        let conn = fixture();
        let lib = create_library(&conn, 1, 0x100);
        invite(&conn, &lib, 1, 2);
        accept(&conn, &lib, 2);

        let photo_id = CustomUUID::retention_cutoff(0x900);
        let add = add_payload_bytes(
            photo_id.clone(),
            1,
            CustomUUID::retention_cutoff(0x901),
            CustomUUID::retention_cutoff(0x902),
            Some(lib.clone()),
        );
        apply(&conn, &PhotoAddHandler, "photo_add", &add, Some(1));

        // Non-member favorite rejected (previously unchecked).
        let fav = enc(&crate::envelopes::PhotoFavoritePayload {
            entries: vec![PhotoFavoriteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::retention_cutoff(0x903),
            }],
        });
        assert!(matches!(
            validate(
                &conn,
                &PhotoFavoriteHandler,
                "photo_favorite",
                &fav,
                Some(3)
            ),
            Err(DatabaseError::AuthorizationError)
        ));

        // Co-member (not uploader) soft-deletes.
        let del = enc(&PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: photo_id.clone(),
                operation_id: CustomUUID::new(None),
            }],
        });
        apply(&conn, &PhotoDeleteHandler, "photo_delete", &del, Some(2));
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM photos WHERE deleted_at IS NOT NULL"
            ),
            1
        );
    }
}
