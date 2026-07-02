//! Change classification and application (spec §Change classification).
//!
//! Observer events and reconciliation-scan re-deliveries both funnel through
//! this module: descriptors via [`apply_change`] (spec kinds: new photo,
//! resource change, metadata-only, scope change), removals via
//! [`apply_removal`] (deletion). Every path is idempotent — PhotoKit delivers
//! 2–4 near-identical events per user action (spike), and the scan re-delivers
//! everything the observer already reported.

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;

use crate::descriptor::AssetDescriptor;
use crate::error::Result;
use crate::ids::{LibraryId, PhotoId};
use crate::model::{PhotoRecord, ResourceRecord, ResourceType};
use crate::paths::{BlobPaths, DataDir};
use crate::resolve::{
    Resolution, SeedOutcome, diff_resources, resolve_descriptor, seed_descriptor,
};
use crate::sidecar_io::{edit_sidecar_deleted_at, write_photo_sidecar};
use crate::store::StateStore;

/// What one descriptor event means for a known photo. Built by the pure
/// [`plan_changes`]; executed by [`apply_change`]. An all-empty plan is
/// classified [`Classification::NoOp`] — the hot path, given PhotoKit's
/// redundant deliveries.
#[derive(Debug, PartialEq, Eq)]
pub struct ChangePlan {
    pub photo_id: PhotoId,
    /// Photo is tombstoned but the asset is alive (restore-from-Recently-
    /// Deleted arrives as an INSERT with the same identity — spike).
    pub restore: bool,
    /// Known `cloud_id` arrived under a different bound scope: hard move
    /// `(src, dst)` (spec §Asset migrating between libraries).
    pub transition: Option<(LibraryId, LibraryId)>,
    /// Resource types newly enumerated (first edit, etc.): mint pending rows.
    pub add_resources: Vec<ResourceType>,
    /// Written edit-mutable rows whose descriptor `fileSize` differs from the
    /// stored `size_bytes`: reopen for refetch (re-edit detection, user
    /// decision — equal or absent sizes are assumed unchanged).
    pub reopen_resources: Vec<ResourceType>,
    /// Resource types no longer enumerated (revert): delete rows, decrement
    /// blobs. Never contains `Original`/`PairedVideo` (see
    /// `original_disappeared`).
    pub remove_resources: Vec<ResourceType>,
    /// Descriptor modification date is newer than stored: refresh sidecar
    /// metadata and stamp `asset_modified_at`.
    pub metadata_refresh: bool,
    /// The diff claimed an original-class resource vanished — a PhotoKit
    /// invariant violation. Logged (`original_disappeared`), row kept: the
    /// spec's "the original row is never overwritten" is load-bearing.
    pub original_disappeared: bool,
}

impl ChangePlan {
    fn empty(photo_id: PhotoId) -> Self {
        Self {
            photo_id,
            restore: false,
            transition: None,
            add_resources: Vec::new(),
            reopen_resources: Vec::new(),
            remove_resources: Vec::new(),
            metadata_refresh: false,
            original_disappeared: false,
        }
    }

    pub fn is_noop(&self) -> bool {
        !self.restore
            && self.transition.is_none()
            && self.add_resources.is_empty()
            && self.reopen_resources.is_empty()
            && self.remove_resources.is_empty()
            && !self.metadata_refresh
            && !self.original_disappeared
    }
}

/// One descriptor event, classified (spec §Change classification). Deletions
/// are not here — they arrive as removed local_ids ([`apply_removal`]) or
/// scan synthesis, never as descriptors.
#[derive(Debug, PartialEq, Eq)]
pub enum Classification {
    /// Not previously known (or unmapped/adopted): the seed outcome IS the
    /// application — rows minted / library adopted / unmapped row recorded.
    Seeded(SeedOutcome),
    /// Known photo with work to do.
    Known(ChangePlan),
    /// Known photo, nothing newer. The hot path.
    NoOp { photo_id: PhotoId },
}

/// Edit-mutable resource types (spec `photo_resources` notes): the only rows
/// a re-edit replaces in place. Originals are never overwritten.
const EDIT_MUTABLE: [ResourceType; 3] = [
    ResourceType::Edited,
    ResourceType::EditedPairedVideo,
    ResourceType::AdjustmentData,
];

/// Original-class rows that must never be deleted on a diff's say-so.
const NEVER_REMOVED: [ResourceType; 2] = [ResourceType::Original, ResourceType::PairedVideo];

/// Pure planner: diff a known photo's stored state against a fresh
/// descriptor. No I/O — unit-testable without fixtures.
pub fn plan_changes(
    photo: &PhotoRecord,
    existing: &[ResourceRecord],
    desc: &AssetDescriptor,
    metadata_changed: bool,
    scope_library: Option<&LibraryId>,
) -> ChangePlan {
    let mut plan = ChangePlan::empty(photo.photo_id.clone());
    plan.restore = photo.deleted_at.is_some();
    plan.metadata_refresh = metadata_changed;

    if let (Some(stored), Some(current)) = (&photo.library_id, scope_library)
        && stored != current
    {
        plan.transition = Some((stored.clone(), current.clone()));
    }

    // Incoming resource set, mapped and deduped (unknown PH types were logged
    // at mint; here they are skipped silently).
    let incoming: BTreeSet<ResourceType> = desc
        .resources
        .iter()
        .filter_map(|r| ResourceType::from_ph_type(r.ph_resource_type))
        .collect();
    let incoming_vec: Vec<ResourceType> = incoming.iter().copied().collect();
    let diff = diff_resources(existing, &incoming_vec);

    plan.add_resources = diff.added.iter().copied().collect();
    for rt in &diff.removed {
        if NEVER_REMOVED.contains(rt) {
            plan.original_disappeared = true;
        } else {
            plan.remove_resources.push(*rt);
        }
    }

    // Re-edit detection: descriptor fileSize vs stored size_bytes on written
    // edit-mutable rows. First descriptor entry per mapped type wins (only
    // Original has multiple PH sources, and it is not edit-mutable).
    let mut sizes: BTreeMap<ResourceType, u64> = BTreeMap::new();
    for r in &desc.resources {
        if let (Some(rt), Some(size)) = (
            ResourceType::from_ph_type(r.ph_resource_type),
            r.expected_size,
        ) {
            sizes.entry(rt).or_insert(size);
        }
    }
    for row in existing {
        if diff.unchanged.contains(&row.resource_type)
            && EDIT_MUTABLE.contains(&row.resource_type)
            && row.written_at.is_some()
            && let (Some(stored), Some(incoming_size)) =
                (row.size_bytes, sizes.get(&row.resource_type))
            && stored != *incoming_size as i64
        {
            plan.reopen_resources.push(row.resource_type);
        }
    }

    plan
}

/// Counters from one [`apply_change`] call, for daemon reporting.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ChangeOutcome {
    pub restored: bool,
    pub transitioned: bool,
    pub resources_added: u64,
    pub resources_reopened: u64,
    pub resources_removed: u64,
    pub metadata_refreshed: bool,
}

/// Classify one descriptor (resolution + plan). Resolution itself may write
/// (adoption, unmapped-row mint, seed mint) — those paths are idempotent;
/// the `Known` path is read-only until [`apply_change`] executes the plan.
pub async fn classify(store: &StateStore, desc: &AssetDescriptor) -> Result<Classification> {
    match resolve_descriptor(store, desc).await? {
        Resolution::KnownByCloudId {
            photo_id,
            metadata_changed,
            scope_changed,
        } => {
            let photo = store.photo(&photo_id).await?.ok_or_else(|| {
                crate::IngressError::Invariant(format!("resolved photo {photo_id} has no row"))
            })?;
            let existing = store.resources_for_photo(&photo_id).await?;
            let scope_library = if scope_changed || photo.deleted_at.is_some() {
                store
                    .library_for_scope(desc.scope)
                    .await?
                    .map(|c| c.library_id)
            } else {
                photo.library_id.clone()
            };
            let plan = plan_changes(
                &photo,
                &existing,
                desc,
                metadata_changed,
                scope_library.as_ref(),
            );
            if plan.is_noop() {
                Ok(Classification::NoOp { photo_id })
            } else {
                Ok(Classification::Known(plan))
            }
        }
        Resolution::Adopted { photo_id, .. } => {
            Ok(Classification::Seeded(SeedOutcome::Adopted { photo_id }))
        }
        Resolution::UnmappedScope { photo_id } => {
            Ok(Classification::Seeded(SeedOutcome::Unmapped { photo_id }))
        }
        Resolution::NeedsContentHash => {
            Ok(Classification::Seeded(seed_descriptor(store, desc).await?))
        }
    }
}

/// Classify + apply one descriptor event. The single entry point observer
/// events and scan re-deliveries funnel through. Idempotent: re-applying an
/// identical descriptor classifies `NoOp`.
///
/// Application order for a `Known` plan (each step idempotent): restore
/// (tx + log), then hard move, then the resource plan (one tx; reaped blob
/// files deleted post-commit), then sidecar rewrite followed by the
/// `asset_modified_at` stamp — fs before stamp, so a crash re-delivers the
/// refresh rather than losing it.
pub async fn apply_change(
    store: &StateStore,
    data_dir: &DataDir,
    desc: &AssetDescriptor,
) -> Result<(Classification, ChangeOutcome)> {
    let classification = classify(store, desc).await?;
    let Classification::Known(plan) = &classification else {
        return Ok((classification, ChangeOutcome::default()));
    };

    let mut outcome = ChangeOutcome::default();

    if plan.restore {
        outcome.restored = restore_photo(store, &plan.photo_id).await?;
    }

    if let Some((src, dst)) = &plan.transition {
        crate::transition::execute_transition(store, data_dir, &plan.photo_id, src, dst).await?;
        outcome.transitioned = true;
    }

    // Effective library AFTER any transition — removals decrement blobs where
    // the transition just moved them.
    let library = match &plan.transition {
        Some((_, dst)) => Some(dst.clone()),
        None => store
            .photo(&plan.photo_id)
            .await?
            .and_then(|p| p.library_id),
    };

    if !plan.add_resources.is_empty()
        || !plan.reopen_resources.is_empty()
        || !plan.remove_resources.is_empty()
        || plan.original_disappeared
    {
        let mut reap: Vec<(crate::ids::ContentHash, String)> = Vec::new();
        let mut tx = store.pool().begin().await?;

        for rt in &plan.add_resources {
            crate::store::resources::insert_pending_resource(&mut *tx, &plan.photo_id, *rt).await?;
            outcome.resources_added += 1;
        }
        for rt in &plan.reopen_resources {
            if crate::store::resources::reopen_resource(&mut *tx, &plan.photo_id, *rt).await? {
                outcome.resources_reopened += 1;
            }
        }
        if !plan.remove_resources.is_empty() {
            let rows =
                crate::store::resources::resources_for_photo(&mut *tx, &plan.photo_id).await?;
            for rt in &plan.remove_resources {
                let Some(row) = rows.iter().find(|r| r.resource_type == *rt) else {
                    continue;
                };
                if let (Some(hash), Some(library)) = (&row.content_hash, &library)
                    && row.written_at.is_some()
                    && let Some(ext) =
                        crate::store::blobs::decrement_and_reap(&mut tx, library, hash).await?
                {
                    reap.push((hash.clone(), ext));
                }
                crate::store::resources::delete_resource_row(&mut *tx, &plan.photo_id, *rt).await?;
                outcome.resources_removed += 1;
            }
        }
        if plan.original_disappeared {
            crate::store::log::append(
                &mut *tx,
                "original_disappeared",
                Some(&plan.photo_id),
                Some(serde_json::json!({ "local_id": desc.local_id })),
            )
            .await?;
        }
        if !plan.add_resources.is_empty() || !plan.reopen_resources.is_empty() {
            crate::store::photos::clear_materialized(&mut *tx, &plan.photo_id).await?;
        } else if !plan.remove_resources.is_empty() {
            // A revert can leave every REMAINING row written on a photo whose
            // materialized_at was cleared by the earlier edit — nothing is
            // pending, so no write will ever re-stamp it. Same guarded stamp
            // as mark_resource_written.
            sqlx::query(
                "UPDATE photos SET materialized_at = ? \
                 WHERE photo_id = ? AND materialized_at IS NULL \
                   AND NOT EXISTS (SELECT 1 FROM photo_resources \
                                   WHERE photo_id = ? AND written_at IS NULL)",
            )
            .bind(Utc::now())
            .bind(&plan.photo_id)
            .bind(&plan.photo_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        // Post-commit: reaped blob files. A crash here leaves benign orphans.
        if let Some(library) = &library
            && !reap.is_empty()
            && let Some(config) = store.library(library).await?
        {
            let paths = BlobPaths::new(&config.blob_root);
            for (hash, ext) in &reap {
                let _ = std::fs::remove_file(paths.blob_path(hash, ext));
            }
        }
    }

    // Sidecar rewrite: only when the photo is materialized — a sidecar
    // reflects committed state; drain (re)writes it at completion otherwise.
    let state_changed = outcome.restored
        || outcome.transitioned
        || outcome.resources_removed > 0
        || plan.metadata_refresh;
    if state_changed
        && let Some(photo) = store.photo(&plan.photo_id).await?
        && photo.materialized_at.is_some()
    {
        write_photo_sidecar(store, data_dir, desc, &plan.photo_id).await?;
    }
    if plan.metadata_refresh {
        if let Some(at) = desc.asset_modified_at {
            crate::store::photos::set_asset_modified_at(store.pool(), &plan.photo_id, at).await?;
        }
        outcome.metadata_refreshed = true;
    }

    Ok((classification, outcome))
}

/// Outcome of [`apply_removal`].
#[derive(Debug, PartialEq, Eq)]
pub enum RemovalOutcome {
    Tombstoned {
        photo_id: PhotoId,
    },
    /// Already tombstoned, never ingested, or a stale local_id — dropped;
    /// the reconciliation scan is the deletion backstop for all three.
    Unknown,
}

/// Apply an observer `removed` event. Resolution is by live `local_id` —
/// the asset is gone from PhotoKit, so its cloud mapping is unavailable and
/// `local_id` is the only handle `removedObjects` still exposes.
pub async fn apply_removal(
    store: &StateStore,
    data_dir: &DataDir,
    local_id: &str,
) -> Result<RemovalOutcome> {
    let Some(photo) =
        crate::store::photos::photo_by_local_id_active(store.pool(), local_id).await?
    else {
        return Ok(RemovalOutcome::Unknown);
    };
    if tombstone_photo(store, data_dir, &photo).await? {
        Ok(RemovalOutcome::Tombstoned {
            photo_id: photo.photo_id,
        })
    } else {
        Ok(RemovalOutcome::Unknown)
    }
}

/// Tombstone one photo (spec §Tombstone): `deleted_at` + `deletion_observed`
/// in one transaction, then the sidecar's `deleted_at` via read-modify-write
/// (the asset no longer exists in PhotoKit, so recomposition is impossible;
/// a photo that never materialized has no sidecar and is skipped silently).
/// Resource rows and blob refcounts are deliberately untouched — bytes stay
/// through the retention window. Shared by removal events and the scan's
/// offline-deletion synthesis. Returns false (no-op) when already tombstoned.
pub(crate) async fn tombstone_photo(
    store: &StateStore,
    data_dir: &DataDir,
    photo: &PhotoRecord,
) -> Result<bool> {
    let now = Utc::now();
    let mut tx = store.pool().begin().await?;
    let tombstoned = crate::store::photos::tombstone_photo(&mut *tx, &photo.photo_id, now).await?;
    if !tombstoned {
        return Ok(false);
    }
    crate::store::log::append(&mut *tx, "deletion_observed", Some(&photo.photo_id), None).await?;
    tx.commit().await?;

    if let Some(library) = &photo.library_id {
        edit_sidecar_deleted_at(data_dir, library, &photo.photo_id, Some(now))?;
    }
    Ok(true)
}

/// Restore one photo (spec §Restore inside the window): clear `deleted_at` +
/// `restore_observed` in one transaction. The sidecar update is the caller's
/// job — a restore arrives with a live descriptor, so [`apply_change`]
/// recomposes the sidecar when the photo is materialized and falls back to a
/// read-modify-write otherwise.
pub(crate) async fn restore_photo(store: &StateStore, photo_id: &PhotoId) -> Result<bool> {
    let mut tx = store.pool().begin().await?;
    let restored = crate::store::photos::restore_photo(&mut *tx, photo_id).await?;
    if !restored {
        return Ok(false);
    }
    crate::store::log::append(&mut *tx, "restore_observed", Some(photo_id), None).await?;
    tx.commit().await?;
    Ok(true)
}
