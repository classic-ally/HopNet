//! The match-precedence engine: identity resolution per the spec's
//! §Asset Identity Model (rule 1, then 2a–2c).
//!
//! Two entry points with a caller-visible handoff: rule 1 needs no bytes;
//! rules 2a–2c need the original resource's BLAKE3, which only the fetch
//! path (Phase 2+) can produce. Phase 1 tests supply hashes directly.

use std::collections::BTreeSet;

use crate::descriptor::AssetDescriptor;
use crate::error::{IngressError, Result};
use crate::ids::{derive_group_id, ContentHash, GroupDomain, PhotoId};
use crate::model::{GroupType, ResourceType};
use crate::store::StateStore;
use crate::store::{libraries, log, photos, resources};

/// Outcome of the no-bytes resolution pass (rule 1).
#[derive(Debug, PartialEq, Eq)]
pub enum Resolution {
    /// Rule 1 hit: photo known by `cloud_id`. `local_id` refreshed opportunistically.
    KnownByCloudId {
        photo_id: PhotoId,
        /// Descriptor's `modificationDate` is newer than stored (or stored is
        /// NULL — never successfully synced): metadata refresh needed.
        metadata_changed: bool,
        /// Known `cloud_id` arrived with a different library scope. Detected
        /// and reported here; the hard move is Phase 4.
        scope_changed: bool,
    },
    /// Rule 1 miss: caller must materialize + hash the original, then call
    /// [`resolve_with_hash`].
    NeedsContentHash,
    /// Descriptor's scope has no bound library: photo row minted with
    /// `library_id = NULL`, ingest blocked until the user binds the scope.
    UnmappedScope { photo_id: PhotoId },
}

/// Outcome of the hash resolution pass (rules 2a–2c).
#[derive(Debug, PartialEq, Eq)]
pub enum HashResolution {
    /// Rule 2a: late binding — an existing hash-matched photo had no
    /// `cloud_id`; it is now populated. Same `photo_id`.
    LateBound { photo_id: PhotoId },
    /// Rule 2b: byte-identical original under a *different* `cloud_id` —
    /// a distinct logical photo. The blob refcount increment happens at
    /// write time (Phase 2); this outcome just reports the shared hash.
    NewPhotoSharedBlob { photo_id: PhotoId, shared_hash: ContentHash },
    /// Rule 2c: genuinely new.
    NewPhoto { photo_id: PhotoId },
}

/// Rule 1. One transaction; idempotent (re-delivery re-resolves here).
pub async fn resolve_descriptor(store: &StateStore, desc: &AssetDescriptor) -> Result<Resolution> {
    let mut tx = store.pool().begin().await?;

    if let Some(cloud_id) = desc.cloud_id.as_deref()
        && let Some(existing) = photos::photo_by_cloud_id(&mut *tx, cloud_id).await?
    {
        if existing.local_id.as_deref() != Some(desc.local_id.as_str()) {
            photos::update_local_id(&mut *tx, &existing.photo_id, &desc.local_id).await?;
        }

        // NULL stored modification date = never successfully synced → changed.
        let metadata_changed = match (existing.asset_modified_at, desc.asset_modified_at) {
            (None, _) => true,
            (Some(stored), Some(incoming)) => incoming > stored,
            (Some(_), None) => false,
        };

        let current_scope_library = libraries::resolve_scope(&mut *tx, desc.scope).await?;
        let scope_changed = match (&existing.library_id, &current_scope_library) {
            (Some(stored), Some(current)) => stored != current,
            // Unmapped-then-bound (or vice versa) is a binding change, not a
            // PhotoKit scope transition; the routing rule handles it.
            _ => false,
        };

        tx.commit().await?;
        return Ok(Resolution::KnownByCloudId {
            photo_id: existing.photo_id,
            metadata_changed,
            scope_changed,
        });
    }

    // No cloud_id match. If the scope has no bound library, record the
    // discovery (so it is never lost) and block ingest.
    let library = libraries::resolve_scope(&mut *tx, desc.scope).await?;
    if library.is_none() {
        let photo_id = mint_photo(&mut tx, desc, None).await?;
        log::append(
            &mut *tx,
            "scope_unmapped",
            Some(&photo_id),
            Some(serde_json::json!({ "scope": format!("{:?}", desc.scope) })),
        )
        .await?;
        tx.commit().await?;
        return Ok(Resolution::UnmappedScope { photo_id });
    }

    // Rule 1 miss with a bound library: nothing is minted here — the photo
    // row is minted in resolve_with_hash, so rule 2a never has to merge a
    // provisional row.
    tx.commit().await?;
    Ok(Resolution::NeedsContentHash)
}

/// Rules 2a–2c. One transaction; requires the original resource's hash.
pub async fn resolve_with_hash(
    store: &StateStore,
    desc: &AssetDescriptor,
    original_hash: &ContentHash,
) -> Result<HashResolution> {
    let mut tx = store.pool().begin().await?;

    let library = libraries::resolve_scope(&mut *tx, desc.scope)
        .await?
        .ok_or(IngressError::UnmappedScope(desc.scope))?;

    let existing = resources::photo_by_original_hash(&mut *tx, &library, original_hash).await?;

    let outcome = match existing {
        // Rule 2a: previously-local asset that just gained a cloud_id, or a
        // re-import on another device. Reuse the photo_id.
        Some(photo) if photo.cloud_id.is_none() => {
            if let Some(cloud_id) = desc.cloud_id.as_deref() {
                photos::set_cloud_id(&mut *tx, &photo.photo_id, cloud_id).await?;
            }
            photos::update_local_id(&mut *tx, &photo.photo_id, &desc.local_id).await?;
            HashResolution::LateBound { photo_id: photo.photo_id }
        }
        // Rule 2b: distinct logical photo with a byte-identical original.
        Some(_) => {
            let photo_id = mint_photo(&mut tx, desc, Some(&library)).await?;
            HashResolution::NewPhotoSharedBlob {
                photo_id,
                shared_hash: original_hash.clone(),
            }
        }
        // Rule 2c: new photo.
        None => {
            let photo_id = mint_photo(&mut tx, desc, Some(&library)).await?;
            HashResolution::NewPhoto { photo_id }
        }
    };

    tx.commit().await?;
    Ok(outcome)
}

/// Mint the `photos` row plus one pending `photo_resources` row per mapped
/// descriptor resource (mint-before-materialize). Unknown PH resource types
/// are skipped with an `unknown_resource_type` log event — never block the
/// asset (user decision).
async fn mint_photo(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    desc: &AssetDescriptor,
    library: Option<&crate::ids::LibraryId>,
) -> Result<PhotoId> {
    let photo_id = PhotoId::mint();

    let (group_id, group_type, is_pick) = match &desc.burst {
        Some(b) => (
            Some(derive_group_id(GroupDomain::Burst, &b.burst_identifier)),
            Some(GroupType::Burst as i64),
            b.is_pick,
        ),
        None => (None, None, false),
    };

    photos::insert_photo(
        &mut **tx,
        photos::NewPhoto {
            photo_id: &photo_id,
            library_id: library,
            cloud_id: desc.cloud_id.as_deref(),
            local_id: &desc.local_id,
            group_id: group_id.as_deref(),
            group_type,
            is_group_pick: is_pick,
            asset_modified_at: desc.asset_modified_at,
        },
    )
    .await
    .map_err(|e| match &e {
        // Surface the spec's fail-loud posture: a UNIQUE violation on
        // cloud_id means Apple's invariant broke or state diverged.
        IngressError::Db(sqlx::Error::Database(db))
            if db.message().contains("photos.cloud_id") =>
        {
            IngressError::CloudIdConflict(desc.cloud_id.clone().unwrap_or_default())
        }
        _ => e,
    })?;

    // Dedup mapped types: PH photo(1) and video(2) both map to Original, and
    // the (photo_id, resource_type) PK admits one row each.
    let mut seen: BTreeSet<i64> = BTreeSet::new();
    for res in &desc.resources {
        match ResourceType::from_ph_type(res.ph_resource_type) {
            Some(rt) => {
                if seen.insert(rt as i64) {
                    resources::insert_pending_resource(&mut **tx, &photo_id, rt).await?;
                }
            }
            None => {
                log::append(
                    &mut **tx,
                    "unknown_resource_type",
                    Some(&photo_id),
                    Some(serde_json::json!({
                        "ph_resource_type": res.ph_resource_type,
                        "uti": res.uti,
                    })),
                )
                .await?;
            }
        }
    }

    Ok(photo_id)
}

/// Difference between a photo's stored resource set and a freshly-enumerated
/// one. Pure; used by edit-classification (and its tests) now, Phase 4 later.
#[derive(Debug, PartialEq, Eq)]
pub struct ResourceSetDiff {
    pub added: BTreeSet<ResourceType>,
    pub removed: BTreeSet<ResourceType>,
    pub unchanged: BTreeSet<ResourceType>,
}

pub fn diff_resources(
    existing: &[crate::model::ResourceRecord],
    incoming: &[ResourceType],
) -> ResourceSetDiff {
    let old: BTreeSet<ResourceType> = existing.iter().map(|r| r.resource_type).collect();
    let new: BTreeSet<ResourceType> = incoming.iter().copied().collect();
    ResourceSetDiff {
        added: new.difference(&old).copied().collect(),
        removed: old.difference(&new).copied().collect(),
        unchanged: old.intersection(&new).copied().collect(),
    }
}
