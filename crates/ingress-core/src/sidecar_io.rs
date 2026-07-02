//! Sidecar file writing (local hot path). Remote replication is Phase 3+ —
//! `photos.sidecar_replicated_at` stays NULL (schema default), which already
//! means "replication pending".

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::descriptor::AssetDescriptor;
use crate::error::{IngressError, Result};
use crate::ids::{LibraryId, PhotoId};
use crate::paths::DataDir;
use crate::sidecar::Sidecar;
use crate::store::StateStore;

fn io_err(e: std::io::Error) -> IngressError {
    IngressError::Invariant(format!("sidecar io: {e}"))
}

/// Atomic local sidecar write: `<root>/<YYYY/MM>/<photo_id>.json` via
/// temp-in-same-dir + rename (spec: sidecars are "rewritten in place
/// (temp + rename)").
pub fn write_sidecar_local(root: &Path, sidecar: &Sidecar) -> Result<PathBuf> {
    let final_path = root.join(sidecar.rel_path());
    let parent = final_path
        .parent()
        .expect("sidecar rel_path has YYYY/MM parents");
    fs::create_dir_all(parent).map_err(io_err)?;

    let tmp = final_path.with_extension("json.tmp");
    fs::write(&tmp, sidecar.to_json()?).map_err(io_err)?;
    fs::rename(&tmp, &final_path).map_err(io_err)?;
    Ok(final_path)
}

/// Compose and locally write the sidecar for a photo from current state.
///
/// Takes the `AssetDescriptor` because sidecar-only fields (media type,
/// subtypes, favorite, capture metadata) are deliberately not persisted in
/// `state.db` — the caller must still have the descriptor in hand. The FFI
/// session retains descriptors per inflight photo for exactly this.
pub async fn write_photo_sidecar(
    store: &StateStore,
    data_dir: &DataDir,
    desc: &AssetDescriptor,
    photo_id: &PhotoId,
) -> Result<PathBuf> {
    let photo = store
        .photo(photo_id)
        .await?
        .ok_or_else(|| IngressError::Invariant(format!("no photo row for {photo_id}")))?;
    let library_id = photo
        .library_id
        .clone()
        .ok_or_else(|| IngressError::Invariant(format!("photo {photo_id} has no library")))?;
    let library = store
        .library(&library_id)
        .await?
        .ok_or_else(|| IngressError::Invariant(format!("no library row {library_id}")))?;
    let resources = store.resources_for_photo(photo_id).await?;

    let sidecar = Sidecar::compose(
        &photo,
        &library,
        desc.media_type,
        &desc.media_subtypes,
        desc.favorite,
        &desc.capture,
        &resources,
    )?;
    write_sidecar_local(&data_dir.sidecar_root(&library_id), &sidecar)
}

/// Locate a photo's sidecar under one library's root. The `YYYY/MM` path
/// segment is keyed on `captured_at`, which is not persisted in `state.db` —
/// so tombstone/move flows (no live descriptor to recompose from) must walk
/// the two-level tree. Deletions and transitions are rare; the walk is cheap.
pub fn find_sidecar(root: &Path, photo_id: &PhotoId) -> Result<Option<PathBuf>> {
    let file_name = format!("{photo_id}.json");
    let years = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err(e)),
    };
    for year in years.flatten().filter(|e| e.path().is_dir()) {
        let months = fs::read_dir(year.path()).map_err(io_err)?;
        for month in months.flatten().filter(|e| e.path().is_dir()) {
            let candidate = month.path().join(&file_name);
            if candidate.is_file() {
                return Ok(Some(candidate));
            }
        }
    }
    Ok(None)
}

/// Every `*.json` under a sidecar root's two-level `YYYY/MM` tree —
/// recovery's rebuild walk and fsck-adjacent audits. Non-directories at the
/// year/month levels and non-`.json` leaves are skipped silently (the tree
/// may carry `.tmp` staging leftovers). Missing root = empty.
pub fn walk_sidecars(root: &Path) -> Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let years = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(found),
        Err(e) => return Err(io_err(e)),
    };
    for year in years.flatten().filter(|e| e.path().is_dir()) {
        let months = fs::read_dir(year.path()).map_err(io_err)?;
        for month in months.flatten().filter(|e| e.path().is_dir()) {
            for entry in fs::read_dir(month.path()).map_err(io_err)?.flatten() {
                let path = entry.path();
                if path.is_file() && path.extension().is_some_and(|e| e == "json") {
                    found.push(path);
                }
            }
        }
    }
    found.sort();
    Ok(found)
}

/// Read-modify-write of an existing sidecar's `deleted_at` (spec §Tombstone
/// step 5 / §Restore step 2). The asset is gone from PhotoKit at delete time,
/// so recomposition is impossible — the on-disk document is the source.
/// Returns `None` (silently) when the photo never materialized a sidecar.
pub fn edit_sidecar_deleted_at(
    data_dir: &DataDir,
    library: &LibraryId,
    photo_id: &PhotoId,
    deleted_at: Option<DateTime<Utc>>,
) -> Result<Option<PathBuf>> {
    let root = data_dir.sidecar_root(library);
    let Some(path) = find_sidecar(&root, photo_id)? else {
        return Ok(None);
    };
    let mut sidecar = Sidecar::from_json(&fs::read_to_string(&path).map_err(io_err)?)?;
    sidecar.deleted_at = deleted_at;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, sidecar.to_json()?).map_err(io_err)?;
    fs::rename(&tmp, &path).map_err(io_err)?;
    Ok(Some(path))
}

/// Relocate a sidecar between library roots on a hard move (spec §Asset
/// migrating step 6): rewrite with the destination `library_id`, then remove
/// the stale source copy — a leftover src document would resurrect the photo
/// in the wrong library during disaster recovery. `None` when the photo has
/// no sidecar yet (unmaterialized).
pub fn move_sidecar(
    data_dir: &DataDir,
    photo_id: &PhotoId,
    src: &LibraryId,
    dst: &LibraryId,
) -> Result<Option<PathBuf>> {
    let Some(old_path) = find_sidecar(&data_dir.sidecar_root(src), photo_id)? else {
        return Ok(None);
    };
    let mut sidecar = Sidecar::from_json(&fs::read_to_string(&old_path).map_err(io_err)?)?;
    sidecar.library_id = dst.clone();
    let new_path = write_sidecar_local(&data_dir.sidecar_root(dst), &sidecar)?;
    fs::remove_file(&old_path).map_err(io_err)?;
    Ok(Some(new_path))
}
