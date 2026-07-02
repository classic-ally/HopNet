//! Sidecar file writing (local hot path). Remote replication is Phase 3+ —
//! `photos.sidecar_replicated_at` stays NULL (schema default), which already
//! means "replication pending".

use std::fs;
use std::path::{Path, PathBuf};

use crate::descriptor::AssetDescriptor;
use crate::error::{IngressError, Result};
use crate::ids::PhotoId;
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
    let parent = final_path.parent().expect("sidecar rel_path has YYYY/MM parents");
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
