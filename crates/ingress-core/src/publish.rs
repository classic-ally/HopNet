//! The HopNet publish queue: pushes completed photos into a HopNet node.
//!
//! Mirrors the sidecar-replication tick's shape (claim a batch, act, stamp)
//! with one structural difference: publishing streams multi-GB originals
//! over HTTP, so the daemon runs the pass in ONE spawned background task
//! instead of inline, keeping the event loop responsive.
//!
//! ## Lazy coupling
//!
//! The daemon owns its lifecycle; the node does not. When the node is
//! unreachable the pass PARKS — the batch aborts, no retry attempts are
//! consumed, and observation/ingest continue untouched. Reachability edges
//! are logged once (`node_unreachable` / `node_regained`), not per tick.
//!
//! ## Scope (this phase)
//!
//! - Personal partition only (`scope_binding IS NULL` in the claim query).
//! - Initial publish only: the predicate is `published_at IS NULL`, and a
//!   re-materialized published photo is NOT re-enqueued — consensus rejects
//!   duplicate photo ids, so re-edit propagation needs its own content-update
//!   transaction (future phase). Deliberately NO `published_at = NULL` reset
//!   on `mark_resource_written`.
//! - Tombstone propagation, favorites, and shared libraries are out of scope.
//! - `published_at` doubles as the GC predicate for a future buffer-mode
//!   retention phase; blobs and sidecars are untouched here.
//!
//! ## Metadata source
//!
//! The sidecar JSON is the publish metadata source: it exists exactly when a
//! photo is publishable (written at photo-complete), reflects committed state
//! only, and persisting descriptor fields into state.db would duplicate it
//! into a second source of truth.

use std::collections::HashSet;
use std::path::PathBuf;

use chrono::Utc;

use crate::error::Result;
use crate::ids::{ContentHash, PhotoId};
use crate::model::{LibraryConfig, PhotoRecord, ResourceType};
use crate::paths::{BlobPaths, DataDir};
use crate::scheduler::BackoffConfig;
use crate::sidecar::Sidecar;
use crate::sidecar_io::find_sidecar;
use crate::store::StateStore;

/// Publish-tick configuration (daemon loop cadence + retry policy).
#[derive(Debug, Clone)]
pub struct PublishConfig {
    /// Tick cadence; same class as sidecar replication.
    pub interval: std::time::Duration,
    /// Photos claimed per pass. Small: claimed photos are registered
    /// inflight for the duration, deferring their PhotoKit events.
    pub batch: i64,
    /// Attempts before a photo is terminal (operator reset required).
    pub retry_cap: i64,
    /// Transient-failure backoff (base 60s, max 6h — publish failures are
    /// slower-moving than fetch failures).
    pub backoff: BackoffConfig,
}

impl Default for PublishConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(60),
            batch: 4,
            retry_cap: 5,
            backoff: BackoffConfig {
                base: std::time::Duration::from_secs(60),
                max: std::time::Duration::from_secs(6 * 3600),
            },
        }
    }
}

/// One written resource, resolved to its on-disk blob at claim time.
#[derive(Debug, Clone)]
pub struct PublishResource {
    pub resource_type: ResourceType,
    pub content_hash: ContentHash,
    pub ext: String,
    pub size_bytes: i64,
    pub blob_path: PathBuf,
}

/// Everything a publisher needs for one photo. Assembled fresh each pass —
/// the library (and thus blob paths) is re-resolved at claim time because a
/// hard move can relocate blobs between enqueue and publish.
#[derive(Debug, Clone)]
pub struct PublishItem {
    pub photo: PhotoRecord,
    pub library: LibraryConfig,
    pub sidecar: Sidecar,
    pub resources: Vec<PublishResource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    Published,
    /// The confirm probe found the photo already committed (a previous
    /// ambiguous attempt actually landed) — stamp, never re-submit.
    AlreadyPublished,
}

#[derive(Debug, Clone)]
pub enum PublishError {
    /// The node cannot be reached (connect/timeout/shedding). The pass
    /// parks: batch aborted, no attempts consumed.
    NodeUnreachable(String),
    /// Permanent: retrying the same item cannot help (validation/mapping
    /// failure). Attempts jump to the cap.
    Rejected(String),
    /// Worth retrying with backoff.
    Transient(String),
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeUnreachable(m) => write!(f, "node unreachable: {m}"),
            Self::Rejected(m) => write!(f, "rejected: {m}"),
            Self::Transient(m) => write!(f, "transient: {m}"),
        }
    }
}

/// The publish seam. Core stays free of HTTP/HopNet types the same way
/// `ResourceFetcher` keeps PhotoKit out — the concrete impl (HTTP dispatch +
/// RFC-011 mapping) lives out-of-crate.
#[async_trait::async_trait]
pub trait Publisher: Send + Sync + 'static {
    async fn publish(&self, item: PublishItem) -> std::result::Result<PublishOutcome, PublishError>;
}

/// One pass's counters (absorbed into daemon totals).
#[derive(Debug, Default, Clone)]
pub struct PublishReport {
    pub published: u64,
    pub already_published: u64,
    /// Transient failures still under the retry cap.
    pub failed: u64,
    /// Photos whose attempts reached the cap this pass.
    pub gave_up: u64,
    /// Claimed photos whose sidecar was missing (crash window) — skipped.
    pub missing_sidecar: u64,
    /// The pass aborted early because the node was unreachable.
    pub parked: bool,
}

impl PublishReport {
    pub fn absorb(&mut self, other: &PublishReport) {
        self.published += other.published;
        self.already_published += other.already_published;
        self.failed += other.failed;
        self.gave_up += other.gave_up;
        self.missing_sidecar += other.missing_sidecar;
        self.parked = other.parked;
    }
}

/// Edge-trigger for reachability logging (mirrors `ReplicationState`).
#[derive(Debug, Default, Clone)]
pub struct PublishState {
    unreachable: bool,
}

/// Claim helper for the daemon tick: publishable photos minus `skip`.
pub async fn claim_publishable(
    store: &StateStore,
    cfg: &PublishConfig,
    skip: &HashSet<PhotoId>,
) -> Result<Vec<PhotoRecord>> {
    Ok(
        crate::store::photos::publishable_photos(store.pool(), Utc::now(), cfg.retry_cap, cfg.batch)
            .await?
            .into_iter()
            .filter(|p| !skip.contains(&p.photo_id))
            .collect(),
    )
}

/// Run one publish pass over `claimed`. The caller has already registered
/// the claimed ids inflight (their PhotoKit events defer until the pass
/// ends, which also excludes supersede/hard-move races on the blob reads).
pub async fn run_publish_pass(
    store: &StateStore,
    data_dir: &DataDir,
    publisher: &dyn Publisher,
    cfg: &PublishConfig,
    claimed: Vec<PhotoRecord>,
    state: &mut PublishState,
) -> Result<PublishReport> {
    let mut report = PublishReport::default();

    for photo in claimed {
        let item = match assemble_item(store, data_dir, &photo).await? {
            Ok(item) => item,
            Err(skip) => {
                match skip {
                    AssembleSkip::MissingSidecar => {
                        report.missing_sidecar += 1;
                        let _ = store
                            .append_log(
                                "publish_sidecar_missing",
                                Some(&photo.photo_id),
                                None,
                            )
                            .await;
                    }
                    AssembleSkip::Transient(msg) => {
                        record_failure(store, cfg, &photo, &msg, &mut report).await?;
                    }
                }
                continue;
            }
        };

        match publisher.publish(item).await {
            Ok(outcome) => {
                crate::store::photos::mark_published(store.pool(), &photo.photo_id, Utc::now())
                    .await?;
                match outcome {
                    PublishOutcome::Published => report.published += 1,
                    PublishOutcome::AlreadyPublished => report.already_published += 1,
                }
                if state.unreachable {
                    state.unreachable = false;
                    let _ = store.append_log("node_regained", None, None).await;
                }
            }
            Err(PublishError::NodeUnreachable(msg)) => {
                if !state.unreachable {
                    state.unreachable = true;
                    let _ = store
                        .append_log(
                            "node_unreachable",
                            None,
                            Some(serde_json::json!({ "error": msg })),
                        )
                        .await;
                }
                report.parked = true;
                break;
            }
            Err(PublishError::Rejected(msg)) => {
                crate::store::photos::record_publish_failure(
                    store.pool(),
                    &photo.photo_id,
                    cfg.retry_cap,
                    None,
                    &msg,
                )
                .await?;
                report.gave_up += 1;
                let _ = store
                    .append_log(
                        "publish_rejected",
                        Some(&photo.photo_id),
                        Some(serde_json::json!({ "error": msg })),
                    )
                    .await;
            }
            Err(PublishError::Transient(msg)) => {
                record_failure(store, cfg, &photo, &msg, &mut report).await?;
            }
        }
    }

    Ok(report)
}

enum AssembleSkip {
    MissingSidecar,
    Transient(String),
}

/// Resolve one claimed photo to a `PublishItem`, re-reading library and
/// resource state fresh (hard-move safety). Recoverable problems return
/// `Err(AssembleSkip)` inside `Ok` — the outer `Result` is store I/O only.
async fn assemble_item(
    store: &StateStore,
    data_dir: &DataDir,
    photo: &PhotoRecord,
) -> Result<std::result::Result<PublishItem, AssembleSkip>> {
    let Some(library_id) = &photo.library_id else {
        return Ok(Err(AssembleSkip::Transient("photo has no library".into())));
    };
    let Some(library) = store.library(library_id).await? else {
        return Ok(Err(AssembleSkip::Transient(format!(
            "library {library_id} vanished"
        ))));
    };
    if library.scope_binding.is_some() {
        // Re-bound to the shared partition between claim and pass.
        return Ok(Err(AssembleSkip::Transient(
            "library re-bound to a shared scope".into(),
        )));
    }

    let sidecar_root = data_dir.sidecar_root(library_id);
    let Some(sidecar_path) = find_sidecar(&sidecar_root, &photo.photo_id)? else {
        return Ok(Err(AssembleSkip::MissingSidecar));
    };
    let sidecar: Sidecar = match std::fs::read(&sidecar_path)
        .map_err(|e| e.to_string())
        .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|e| e.to_string()))
    {
        Ok(sidecar) => sidecar,
        Err(e) => {
            return Ok(Err(AssembleSkip::Transient(format!(
                "sidecar unreadable: {e}"
            ))));
        }
    };

    let blob_paths = BlobPaths::new(&library.blob_root);
    let mut resources = Vec::new();
    for record in store.resources_for_photo(&photo.photo_id).await? {
        if record.written_at.is_none() {
            continue;
        }
        let (Some(hash), Some(ext), Some(size)) =
            (&record.content_hash, &record.ext, record.size_bytes)
        else {
            return Ok(Err(AssembleSkip::Transient(format!(
                "written resource {} missing hash/ext/size",
                record.resource_type.as_str()
            ))));
        };
        let blob_path = blob_paths.blob_path(hash, ext);
        if !blob_path.exists() {
            return Ok(Err(AssembleSkip::Transient(format!(
                "blob missing on disk: {}",
                blob_path.display()
            ))));
        }
        resources.push(PublishResource {
            resource_type: record.resource_type,
            content_hash: hash.clone(),
            ext: ext.clone(),
            size_bytes: size,
            blob_path,
        });
    }
    if resources.is_empty() {
        return Ok(Err(AssembleSkip::Transient(
            "materialized photo has no written resources".into(),
        )));
    }

    Ok(Ok(PublishItem {
        photo: photo.clone(),
        library,
        sidecar,
        resources,
    }))
}

async fn record_failure(
    store: &StateStore,
    cfg: &PublishConfig,
    photo: &PhotoRecord,
    msg: &str,
    report: &mut PublishReport,
) -> Result<()> {
    let attempts = photo.publish_attempts + 1;
    let next_retry = Utc::now()
        + chrono::Duration::from_std(crate::scheduler::backoff::delay(&cfg.backoff, attempts))
            .unwrap_or_else(|_| chrono::Duration::hours(6));
    crate::store::photos::record_publish_failure(
        store.pool(),
        &photo.photo_id,
        attempts,
        Some(next_retry),
        msg,
    )
    .await?;
    if attempts >= cfg.retry_cap {
        report.gave_up += 1;
        let _ = store
            .append_log(
                "publish_gave_up",
                Some(&photo.photo_id),
                Some(serde_json::json!({ "error": msg, "attempts": attempts })),
            )
            .await;
    } else {
        report.failed += 1;
    }
    Ok(())
}
