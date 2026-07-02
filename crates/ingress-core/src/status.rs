//! CLI `status` views: library, pipeline, and per-photo (spec §Phase 6).
//!
//! Pure reads — every function here works on a read-only store and takes
//! no lock. Human rendering lives in the CLI binary; these structs are the
//! `--json` output shapes.

use std::path::PathBuf;

use crate::error::Result;
use crate::ids::PhotoId;
use crate::model::{LibraryConfig, PhotoRecord, ResourceRecord};
use crate::paths::{BlobPaths, DataDir};
use crate::sidecar_io::find_sidecar;
use crate::store::{LibraryStats, LogEvent, RetrySummary, StateStore};

/// How many log events the per-photo view tails by default.
pub const PHOTO_LOG_TAIL: i64 = 20;

#[derive(Debug, serde::Serialize)]
pub struct StatusReport {
    pub libraries: Vec<LibraryStatus>,
    pub pipeline: PipelineStatus,
}

#[derive(Debug, serde::Serialize)]
pub struct LibraryStatus {
    pub config: LibraryConfig,
    pub stats: LibraryStats,
}

/// Work-queue posture across all libraries. The per-resource split follows
/// the derivable-state rule (spec §photo_resources notes): fresh work has
/// no backoff deadline, awaiting-retry has one below the cap, gave-up sits
/// at the cap.
#[derive(Debug, serde::Serialize)]
pub struct PipelineStatus {
    /// Photos whose PhotoKit scope has no configured binding.
    pub unmapped_photos: i64,
    /// Unwritten resources that never failed a fetch.
    pub resources_pending: i64,
    #[serde(flatten)]
    pub retries: RetrySummary,
}

/// The `status` overview: per-library counters plus pipeline posture.
pub async fn status(store: &StateStore, retry_cap: i64) -> Result<StatusReport> {
    let configs = store.libraries().await?;
    let stats = store.library_stats().await?;
    let libraries = configs
        .into_iter()
        .zip(stats)
        .map(|(config, stats)| LibraryStatus { config, stats })
        .collect();
    let pipeline = PipelineStatus {
        unmapped_photos: store.count_unmapped_photos().await?,
        resources_pending: store.count_pending_resources().await?,
        retries: store.retry_summary(retry_cap).await?,
    };
    Ok(StatusReport {
        libraries,
        pipeline,
    })
}

#[derive(Debug, serde::Serialize)]
pub struct PhotoStatus {
    pub photo: PhotoRecord,
    pub resources: Vec<ResourceStatus>,
    /// The local sidecar document, if present in the YYYY/MM tree.
    pub sidecar_local: Option<PathBuf>,
    /// Newest-first ingest-log tail for this photo.
    pub events: Vec<LogEvent>,
}

#[derive(Debug, serde::Serialize)]
pub struct ResourceStatus {
    #[serde(flatten)]
    pub record: ResourceRecord,
    /// Reconstructed blob path (photo mapped + resource hashed), else None.
    pub blob_path: Option<PathBuf>,
    /// Whether the blob file exists on disk; None when no path resolves
    /// (never conflate "unknown" with "missing" — an absent mount must not
    /// read as byte loss).
    pub blob_exists: Option<bool>,
}

/// Per-photo view. `key` is tried as a `photo_id` first, then a `cloud_id`.
/// Returns None when neither matches.
pub async fn photo_status(
    store: &StateStore,
    data_dir: &DataDir,
    key: &str,
) -> Result<Option<PhotoStatus>> {
    let by_id = store.photo(&PhotoId::from_string(key)).await?;
    let photo = match by_id {
        Some(p) => p,
        None => match store.photo_by_cloud_id(key).await? {
            Some(p) => p,
            None => return Ok(None),
        },
    };

    let library: Option<LibraryConfig> = match &photo.library_id {
        Some(id) => store.library(id).await?,
        None => None,
    };
    let blob_paths = library.as_ref().map(|lib| BlobPaths::new(&lib.blob_root));

    let mut resources = Vec::new();
    for record in store.resources_for_photo(&photo.photo_id).await? {
        let blob_path = match (&blob_paths, &record.content_hash, &record.ext) {
            (Some(paths), Some(hash), Some(ext)) => Some(paths.blob_path(hash, ext)),
            _ => None,
        };
        let blob_exists = blob_path.as_ref().map(|p| p.is_file());
        resources.push(ResourceStatus {
            record,
            blob_path,
            blob_exists,
        });
    }

    let sidecar_local = match &photo.library_id {
        Some(lib) => find_sidecar(&data_dir.sidecar_root(lib), &photo.photo_id)?,
        None => None,
    };
    let events = store
        .log_tail_for_photo(&photo.photo_id, PHOTO_LOG_TAIL)
        .await?;

    Ok(Some(PhotoStatus {
        photo,
        resources,
        sidecar_local,
        events,
    }))
}
