//! Library configuration commands (spec §Storage root configuration):
//! add, bind, rename (display name), set-retention, set-mesh-id. List is
//! a plain read via [`StateStore::libraries`].
//!
//! `library_id` is GENERATED (two words from an embedded list, e.g.
//! `brave_otter`) and immutable — it is the PK, an FK target, and the
//! local sidecar path component; `display_name` is the mutable
//! human-facing label. An explicit `--id` override exists for scripts.
//!
//! Every write here takes the exclusive run lock (a live daemon refuses
//! the edit) and runs the Tier-1 refcount repair on an unclean reclaim —
//! the reclaim signal must never be swallowed.

use chrono::Utc;

use crate::descriptor::LibraryScope;
use crate::error::{IngressError, Result};
use crate::ids::LibraryId;
use crate::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig};
use crate::paths::DataDir;
use crate::store::StateStore;

const ADJECTIVES: [&str; 32] = [
    "amber", "bold", "brave", "brisk", "calm", "clever", "cosmic", "crisp", "eager", "fable",
    "fond", "gentle", "glad", "golden", "keen", "lively", "lucid", "mellow", "nimble", "plucky",
    "proud", "quiet", "rapid", "rustic", "silver", "solid", "spry", "stout", "sunny", "swift",
    "vivid", "witty",
];

const NOUNS: [&str; 32] = [
    "aspen", "badger", "beacon", "birch", "bison", "brook", "cedar", "comet", "crane", "delta",
    "falcon", "fjord", "gable", "harbor", "heron", "lantern", "linden", "marmot", "meadow",
    "orchard", "osprey", "otter", "pebble", "pinecone", "prairie", "quartz", "raven", "sparrow",
    "summit", "thicket", "walnut", "willow",
];

/// Generate a fresh two-word library id, regenerating on collision with an
/// existing row. 1024 combinations against a handful of libraries — a
/// failure to find one means something is deeply wrong.
pub fn generate_library_id(existing: &[LibraryId]) -> Result<LibraryId> {
    for _ in 0..64 {
        // UUIDv7's trailing bytes are random (the leading six are the
        // timestamp) — the only entropy source this crate already ships.
        let uuid = uuid::Uuid::now_v7();
        let bytes = uuid.as_bytes();
        let candidate = LibraryId::new(format!(
            "{}_{}",
            ADJECTIVES[bytes[15] as usize % ADJECTIVES.len()],
            NOUNS[bytes[14] as usize % NOUNS.len()],
        ));
        if !existing.contains(&candidate) {
            return Ok(candidate);
        }
    }
    Err(IngressError::Invariant(
        "could not generate an unused library id".into(),
    ))
}

#[derive(Debug, Clone)]
pub struct AddLibraryOptions {
    /// Explicit id override (scripts/tests); validated by the caller via
    /// [`LibraryId::parse`]. None = generate.
    pub id: Option<LibraryId>,
    /// None = default by scope ("Personal Library" / "Shared Library").
    pub display_name: Option<String>,
    pub scope: LibraryScope,
    pub retention_days: i64,
}

#[derive(Debug)]
pub struct AddedLibrary {
    pub config: LibraryConfig,
}

pub async fn add_library(
    store: &StateStore,
    data_dir: &DataDir,
    opts: &AddLibraryOptions,
) -> Result<AddedLibrary> {
    let _lock = acquire_repairing(store, data_dir).await?;

    if opts.retention_days < 0 {
        return Err(IngressError::Invariant(
            "retention days must be >= 0".into(),
        ));
    }

    let existing = store.libraries().await?;
    let library_id = match &opts.id {
        Some(id) => {
            if existing.iter().any(|l| &l.library_id == id) {
                return Err(IngressError::Invariant(format!(
                    "library {id} already exists"
                )));
            }
            id.clone()
        }
        None => {
            let ids: Vec<LibraryId> = existing.iter().map(|l| l.library_id.clone()).collect();
            generate_library_id(&ids)?
        }
    };

    let scope_binding = match opts.scope {
        // MVP invariant (spec §libraries notes): exactly one NULL-scope
        // row — the personal routing rule picks it with LIMIT 1, so a
        // second one would route personal photos arbitrarily.
        LibraryScope::Personal => {
            if let Some(l) = existing.iter().find(|l| l.scope_binding.is_none()) {
                return Err(IngressError::Invariant(format!(
                    "a personal library already exists ({})",
                    l.library_id
                )));
            }
            None
        }
        LibraryScope::Shared => {
            if let Some(l) = existing
                .iter()
                .find(|l| l.scope_binding.as_deref() == Some(ICLOUD_SHARED_LIBRARY_BINDING))
            {
                return Err(IngressError::Invariant(format!(
                    "the shared scope is already bound to {}",
                    l.library_id
                )));
            }
            Some(ICLOUD_SHARED_LIBRARY_BINDING.to_string())
        }
    };

    let config = LibraryConfig {
        library_id: library_id.clone(),
        display_name: opts.display_name.clone().unwrap_or_else(|| {
            match opts.scope {
                LibraryScope::Personal => "Personal Library",
                LibraryScope::Shared => "Shared Library",
            }
            .to_string()
        }),
        scope_binding,
        retention_days: opts.retention_days,
        created_at: Utc::now(),
        mesh_library_id: None,
    };

    let mut tx = store.pool().begin().await?;
    crate::store::libraries::insert(&mut *tx, &config).await?;
    crate::store::log::append(
        &mut *tx,
        "library_added",
        None,
        Some(serde_json::json!({
            "library": library_id.to_string(),
            "scope": match opts.scope {
                LibraryScope::Personal => "personal",
                LibraryScope::Shared => "shared",
            },
        })),
    )
    .await?;
    tx.commit().await?;

    Ok(AddedLibrary { config })
}

/// Attach the shared-scope marker to a library, or detach it (`scope` =
/// None). Detaching is refused while another NULL-scope row exists — it
/// would create a second personal-routing candidate.
pub async fn bind_scope(
    store: &StateStore,
    data_dir: &DataDir,
    id: &LibraryId,
    scope: Option<LibraryScope>,
) -> Result<()> {
    let _lock = acquire_repairing(store, data_dir).await?;

    let existing = store.libraries().await?;
    let Some(target) = existing.iter().find(|l| &l.library_id == id) else {
        return Err(IngressError::Invariant(format!("no library {id}")));
    };
    let binding = match scope {
        Some(LibraryScope::Shared) => {
            if let Some(l) = existing.iter().find(|l| {
                l.scope_binding.as_deref() == Some(ICLOUD_SHARED_LIBRARY_BINDING)
                    && &l.library_id != id
            }) {
                return Err(IngressError::Invariant(format!(
                    "the shared scope is already bound to {}",
                    l.library_id
                )));
            }
            Some(ICLOUD_SHARED_LIBRARY_BINDING)
        }
        Some(LibraryScope::Personal) | None => {
            if target.mesh_library_id.is_some() {
                return Err(IngressError::Invariant(
                    "library is bound to a mesh shared library — clear the mesh \
                     binding first (a NULL-scope row must never carry a mesh target)"
                        .into(),
                ));
            }
            if let Some(l) = existing
                .iter()
                .find(|l| l.scope_binding.is_none() && &l.library_id != id)
            {
                return Err(IngressError::Invariant(format!(
                    "unbinding would create a second personal-routing candidate \
                     (a NULL-scope library already exists: {})",
                    l.library_id
                )));
            }
            None
        }
    };

    let mut tx = store.pool().begin().await?;
    crate::store::libraries::update_scope_binding(&mut *tx, id, binding).await?;
    crate::store::log::append(
        &mut *tx,
        "library_bound",
        None,
        Some(serde_json::json!({
            "library": id.to_string(),
            "binding": binding,
        })),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Set or clear the mesh publish target for a shared library. Setting
/// requires the row to be scope-bound — personal libraries publish to the
/// personal partition by definition, and the publish pass partitions by
/// `mesh_library_id` alone on the strength of this invariant. The value
/// is validated as a UUID here (CLI is the only writer), so a malformed
/// stored id can only come from direct DB edits.
pub async fn set_mesh_library_id(
    store: &StateStore,
    data_dir: &DataDir,
    id: &LibraryId,
    mesh: Option<&str>,
) -> Result<()> {
    let _lock = acquire_repairing(store, data_dir).await?;
    let target = store
        .library(id)
        .await?
        .ok_or_else(|| IngressError::Invariant(format!("no library {id}")))?;
    if let Some(mesh) = mesh {
        if target.scope_binding.is_none() {
            return Err(IngressError::Invariant(
                "only a scope-bound (shared) library can bind a mesh library — \
                 personal libraries publish to the personal partition"
                    .into(),
            ));
        }
        uuid::Uuid::parse_str(mesh)
            .map_err(|e| IngressError::Invariant(format!("mesh library id is not a UUID: {e}")))?;
    }

    let mut tx = store.pool().begin().await?;
    crate::store::libraries::update_mesh_library_id(&mut *tx, id, mesh).await?;
    crate::store::log::append(
        &mut *tx,
        "mesh_library_bound",
        None,
        Some(serde_json::json!({
            "library": id.to_string(),
            "mesh_library_id": mesh,
        })),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Change the display name. The `library_id` itself is immutable by
/// design — it lives in every sidecar document and in the local sidecar
/// tree layout; the display name is the label meant to change.
pub async fn rename_library(
    store: &StateStore,
    data_dir: &DataDir,
    id: &LibraryId,
    display_name: &str,
) -> Result<()> {
    let _lock = acquire_repairing(store, data_dir).await?;
    let old = store
        .library(id)
        .await?
        .ok_or_else(|| IngressError::Invariant(format!("no library {id}")))?;

    let mut tx = store.pool().begin().await?;
    crate::store::libraries::update_display_name(&mut *tx, id, display_name).await?;
    crate::store::log::append(
        &mut *tx,
        "library_renamed",
        None,
        Some(serde_json::json!({
            "library": id.to_string(),
            "old": old.display_name,
            "new": display_name,
        })),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn set_retention(
    store: &StateStore,
    data_dir: &DataDir,
    id: &LibraryId,
    days: i64,
) -> Result<()> {
    let _lock = acquire_repairing(store, data_dir).await?;
    if days < 0 {
        return Err(IngressError::Invariant(
            "retention days must be >= 0".into(),
        ));
    }
    let old = store
        .library(id)
        .await?
        .ok_or_else(|| IngressError::Invariant(format!("no library {id}")))?;

    let mut tx = store.pool().begin().await?;
    crate::store::libraries::update_retention(&mut *tx, id, days).await?;
    crate::store::log::append(
        &mut *tx,
        "retention_changed",
        None,
        Some(serde_json::json!({
            "library": id.to_string(),
            "old": old.retention_days,
            "new": days,
        })),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Take the exclusive run lock; run Tier-1 refcount repair on an unclean
/// reclaim before any config change (mirrors `cleanup::run_standalone`).
async fn acquire_repairing(
    store: &StateStore,
    data_dir: &DataDir,
) -> Result<crate::runlock::DrainLock> {
    std::fs::create_dir_all(data_dir.root())
        .map_err(|e| IngressError::Invariant(format!("data dir: {e}")))?;
    let acquired = crate::runlock::DrainLock::acquire(data_dir)?;
    if acquired.unclean {
        crate::recovery::repair_refcounts(store).await?;
    }
    Ok(acquired.lock)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: generated ids land in sidecar documents and on-disk paths;
    // one outside the [a-z0-9_] charset would corrupt the layout.
    // Should: always produce parseable two-word ids and avoid collisions.
    #[test]
    fn generated_ids_parse_and_avoid_collisions() {
        let mut existing = Vec::new();
        for _ in 0..100 {
            let id = generate_library_id(&existing).unwrap();
            assert!(LibraryId::parse(id.as_str()).is_ok(), "{id}");
            assert!(id.as_str().contains('_'));
            assert!(!existing.contains(&id));
            existing.push(id);
        }
    }
}
