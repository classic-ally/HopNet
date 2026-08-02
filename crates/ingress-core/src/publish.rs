//! The HopNet publish queue: pushes completed photos into a HopNet node,
//! then evicts their spool bytes once the publish is consensus-decided.
//!
//! Claim a batch, act, stamp — run in ONE spawned background task instead
//! of inline, because publishing streams multi-GB originals over HTTP and
//! the event loop must stay responsive.
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
//! - `published_at` is the eviction predicate: each pass ends by stamping
//!   `evicted_at` on blobs whose every referencing photo is decided and
//!   unlinking their spool files (hash-liveness gated; see `cleanup.rs`).
//!
//! ## Metadata source
//!
//! The descriptor capsule (`photos.descriptor_json`) plus the live DB rows:
//! `Sidecar::compose` builds the publish document per pass, so it always
//! reflects committed state with no stored copy to drift. A NULL capsule
//! (pre-column photo awaiting scan backfill) skips without burning an
//! attempt.

use std::collections::HashSet;
use std::path::PathBuf;

use chrono::Utc;

use crate::error::Result;
use crate::ids::{ContentHash, PhotoId};
use crate::model::{LibraryConfig, PhotoRecord, ResourceType};
use crate::paths::SpoolPaths;
use crate::scheduler::BackoffConfig;
use crate::descriptor::DescriptorCapsule;
use crate::sidecar::Sidecar;
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
    /// Hex fingerprint from the resolve pre-pass (None = no cloud_id).
    /// Hex keeps ingress-core free of HopNet crypto types.
    pub cloud_fingerprint: Option<String>,
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

/// The caller's publish standing per the node's responsibility record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Responsibility {
    /// This device holds ingress responsibility — publish freely.
    Holder,
    /// Another device holds it — adopt only, park mutations.
    Other,
    /// No claim exists — adopt only, park mutations (claims are explicit
    /// and JWT-issued; a daemon never claims for itself).
    Unclaimed,
}

/// One cloud_id resolved by the node: its fingerprint (travels into the
/// publish payload) and any already-committed consensus photo id (→ adopt).
#[derive(Debug, Clone)]
pub struct ResolveEntry {
    pub cloud_id: String,
    pub fingerprint: String,
    pub committed_photo_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolveOutcome {
    pub responsibility: Responsibility,
    pub entries: Vec<ResolveEntry>,
}

/// The publish seam. Core stays free of HTTP/HopNet types the same way
/// `ResourceFetcher` keeps PhotoKit out — the concrete impl (HTTP dispatch +
/// RFC-011 mapping) lives out-of-crate.
#[async_trait::async_trait]
pub trait Publisher: Send + Sync + 'static {
    async fn publish(&self, item: PublishItem) -> std::result::Result<PublishOutcome, PublishError>;

    /// Pre-pass identity resolution for ONE publish scope: cloud_ids →
    /// fingerprints + committed ids + the caller's responsibility standing
    /// in that scope. `library_id` None = the personal partition, Some =
    /// the mesh shared-library UUID (the node then fingerprints under the
    /// library-scoped key, so entries match ANY member's committed
    /// photos). The default is the legacy/no-dedupe publisher — proceeds
    /// as holder with no fingerprints, preserving pre-identity behavior
    /// for mocks.
    async fn resolve(
        &self,
        library_id: Option<&str>,
        cloud_ids: &[String],
    ) -> std::result::Result<ResolveOutcome, PublishError> {
        let _ = (library_id, cloud_ids);
        Ok(ResolveOutcome {
            responsibility: Responsibility::Holder,
            entries: Vec::new(),
        })
    }
}

/// One pass's counters (absorbed into daemon totals).
#[derive(Debug, Default, Clone)]
pub struct PublishReport {
    pub published: u64,
    pub already_published: u64,
    /// Photos the mesh already held under another device's publish —
    /// stamped with the remote consensus id, nothing uploaded.
    pub adopted: u64,
    /// Transient failures still under the retry cap.
    pub failed: u64,
    /// Photos whose attempts reached the cap this pass.
    pub gave_up: u64,
    /// Claimed photos with a NULL descriptor capsule (pre-column rows
    /// awaiting scan backfill) — skipped, no attempt burned.
    pub missing_descriptor: u64,
    /// Blobs spool-evicted at the end of the pass (every referent decided).
    pub evicted_blobs: u64,
    /// The pass aborted early because the node was unreachable.
    pub parked: bool,
    /// The pass held its remaining photos because this device does not
    /// hold ingress responsibility (adoption still ran). No attempts
    /// consumed — a claim/transfer unparks the next pass.
    pub parked_responsibility: bool,
}

impl PublishReport {
    pub fn absorb(&mut self, other: &PublishReport) {
        self.published += other.published;
        self.already_published += other.already_published;
        self.adopted += other.adopted;
        self.failed += other.failed;
        self.gave_up += other.gave_up;
        self.missing_descriptor += other.missing_descriptor;
        self.evicted_blobs += other.evicted_blobs;
        self.parked = other.parked;
        self.parked_responsibility = other.parked_responsibility;
    }
}

/// Edge-trigger for reachability logging (mirrors `ReplicationState`).
/// Responsibility parking is tracked per publish scope (None = personal,
/// Some = mesh library UUID) — losing standing in one shared library must
/// not mute or flap the edges of the others.
#[derive(Debug, Default, Clone)]
pub struct PublishState {
    unreachable: bool,
    not_responsible: std::collections::HashSet<Option<String>>,
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
///
/// The pass partitions by publish scope — the photo's library's
/// `mesh_library_id` (None = personal partition) — and runs one
/// resolve→adopt→gate→publish sequence per scope, personal first, then
/// mesh ids in sorted order. Responsibility standing, parking, and
/// resolve-failure attempt burning are all per-scope: a kicked member's
/// 403ing shared library must not starve the personal queue. Node
/// unreachability is the one whole-pass condition — it parks everything.
pub async fn run_publish_pass(
    store: &StateStore,
    spool: &SpoolPaths,
    publisher: &dyn Publisher,
    cfg: &PublishConfig,
    claimed: Vec<PhotoRecord>,
    state: &mut PublishState,
) -> Result<PublishReport> {
    let mut report = PublishReport::default();

    let libraries = store.libraries().await?;
    let mesh_of: std::collections::HashMap<&crate::ids::LibraryId, Option<&str>> = libraries
        .iter()
        .map(|l| (&l.library_id, l.mesh_library_id.as_deref()))
        .collect();

    // Partition preserving claim order within each scope. A photo whose
    // library is unknown or mesh-unbound lands in the personal partition
    // here; assemble_item re-reads the library fresh and skips it before
    // anything is published (claim-vs-pass race window only).
    let mut partitions: Vec<(Option<String>, Vec<PhotoRecord>)> = Vec::new();
    for photo in claimed {
        let scope: Option<String> = photo
            .library_id
            .as_ref()
            .and_then(|lib| mesh_of.get(lib).copied().flatten())
            .map(str::to_string);
        match partitions.iter_mut().find(|(s, _)| *s == scope) {
            Some((_, photos)) => photos.push(photo),
            None => partitions.push((scope, vec![photo])),
        }
    }
    partitions.sort_by(|(a, _), (b, _)| a.cmp(b)); // None (personal) first

    for (scope, photos) in partitions {
        let control =
            run_scope_pass(store, spool, publisher, cfg, scope.as_deref(), photos, state, &mut report)
                .await?;
        if control == ScopeControl::ParkAll {
            break;
        }
    }

    // Spool eviction rides the end of every pass so decided bytes leave
    // local disk with minimal residence; the cleanup tick sweeps whatever a
    // crash window strands.
    report.evicted_blobs =
        crate::cleanup::evict_published_blobs(store, spool, EVICT_BATCH).await?;

    Ok(report)
}

#[derive(Debug, PartialEq, Eq)]
enum ScopeControl {
    Continue,
    /// The node is unreachable — no other scope can do better this pass.
    ParkAll,
}

/// One scope's resolve→adopt→gate→publish sequence (the v1 whole-pass
/// body, scoped). Mutates the shared `report`; per-scope failures burn
/// attempts only for this scope's photos.
#[allow(clippy::too_many_arguments)]
async fn run_scope_pass(
    store: &StateStore,
    spool: &SpoolPaths,
    publisher: &dyn Publisher,
    cfg: &PublishConfig,
    scope: Option<&str>,
    claimed: Vec<PhotoRecord>,
    state: &mut PublishState,
    report: &mut PublishReport,
) -> Result<ScopeControl> {
    // --- Resolve pre-pass: fingerprints + remote adoption + standing. ---
    // One batch call per scope; NULL-cloud_id photos simply get no entry
    // (they publish with no fingerprint and are exempt from dedupe).
    let cloud_ids: Vec<String> = claimed.iter().filter_map(|p| p.cloud_id.clone()).collect();
    let resolved = match publisher.resolve(scope, &cloud_ids).await {
        Ok(outcome) => outcome,
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
            return Ok(ScopeControl::ParkAll);
        }
        Err(e) => {
            // A failing resolve blocks this scope: burn one attempt per
            // claimed photo so a persistently broken resolve (e.g. a 403ing
            // library after a kick) backs off and eventually surfaces as
            // gave_up instead of spinning silently — while the other scopes
            // keep publishing.
            let msg = format!("resolve failed: {e}");
            for photo in &claimed {
                record_failure(store, cfg, photo, &msg, report).await?;
            }
            return Ok(ScopeControl::Continue);
        }
    };
    // Deliberately NOT the reachability recovery edge: `node_regained` stays
    // pinned to a publish success (its pre-identity meaning), so a resolve
    // that succeeds while uploads still fail cannot flap the edge logs.

    let by_cloud_id: std::collections::HashMap<&str, &ResolveEntry> = resolved
        .entries
        .iter()
        .map(|e| (e.cloud_id.as_str(), e))
        .collect();

    // Adoption runs regardless of responsibility standing — it is read-only
    // node-side and purely local otherwise, and it is exactly what makes a
    // responsibility handoff a cheap sweep instead of a full re-upload.
    let mut remaining: Vec<(PhotoRecord, Option<String>)> = Vec::with_capacity(claimed.len());
    for photo in claimed {
        let entry = photo
            .cloud_id
            .as_deref()
            .and_then(|cid| by_cloud_id.get(cid).copied());
        match entry.and_then(|e| e.committed_photo_id.as_deref()) {
            Some(remote) if remote == photo.photo_id.as_str() => {
                // Our own earlier (ambiguous) submit actually landed —
                // self-resolution subsumes the confirm probe.
                crate::store::photos::mark_published(store.pool(), &photo.photo_id, Utc::now())
                    .await?;
                report.already_published += 1;
            }
            Some(remote) => {
                crate::store::photos::mark_adopted(
                    store.pool(),
                    &photo.photo_id,
                    remote,
                    Utc::now(),
                )
                .await?;
                report.adopted += 1;
                let _ = store
                    .append_log(
                        "publish_adopted",
                        Some(&photo.photo_id),
                        Some(serde_json::json!({ "consensus_photo_id": remote })),
                    )
                    .await;
            }
            None => {
                let fingerprint = entry.map(|e| e.fingerprint.clone());
                remaining.push((photo, fingerprint));
            }
        }
    }

    // --- Responsibility gate: mutations are holder-only, per scope. ---
    let scope_key = scope.map(str::to_string);
    if resolved.responsibility != Responsibility::Holder {
        if !remaining.is_empty() {
            if state.not_responsible.insert(scope_key) {
                let status = match resolved.responsibility {
                    Responsibility::Other => "other",
                    _ => "unclaimed",
                };
                let _ = store
                    .append_log(
                        "publish_not_responsible",
                        None,
                        Some(serde_json::json!({ "holder": status, "library": scope })),
                    )
                    .await;
            }
            report.parked_responsibility = true;
        }
        return Ok(ScopeControl::Continue);
    }
    if state.not_responsible.remove(&scope_key) {
        let _ = store
            .append_log(
                "responsibility_regained",
                None,
                Some(serde_json::json!({ "library": scope })),
            )
            .await;
    }

    for (photo, fingerprint) in remaining {
        let mut item = match assemble_item(store, spool, &photo).await? {
            Ok(item) => item,
            Err(skip) => {
                match skip {
                    AssembleSkip::MissingDescriptor => {
                        report.missing_descriptor += 1;
                        let _ = store
                            .append_log(
                                "publish_descriptor_missing",
                                Some(&photo.photo_id),
                                None,
                            )
                            .await;
                    }
                    AssembleSkip::Transient(msg) => {
                        record_failure(store, cfg, &photo, &msg, report).await?;
                    }
                }
                continue;
            }
        };
        item.cloud_fingerprint = fingerprint;

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
                return Ok(ScopeControl::ParkAll);
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
                record_failure(store, cfg, &photo, &msg, report).await?;
            }
        }
    }

    Ok(ScopeControl::Continue)
}

/// Eviction cap per pass (same stall rationale as the hard-delete batch).
const EVICT_BATCH: i64 = 500;

enum AssembleSkip {
    MissingDescriptor,
    Transient(String),
}

/// Resolve one claimed photo to a `PublishItem`, re-reading library and
/// resource state fresh (hard-move safety). Recoverable problems return
/// `Err(AssembleSkip)` inside `Ok` — the outer `Result` is store I/O only.
///
/// Metadata comes from the photo row's publish-metadata capsule
/// (`descriptor_json`), re-composed against the authoritative DB rows —
/// identity, group, tombstone, and resource state are read live, so
/// tombstone/move flows never have to edit the stored capsule.
async fn assemble_item(
    store: &StateStore,
    spool: &SpoolPaths,
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
    if library.scope_binding.is_some() && library.mesh_library_id.is_none() {
        // Scope-bound with no publish target: re-bound (or mesh-unbound)
        // between claim and pass. Defense-in-depth — libconfig writes take
        // the exclusive run lock, so a live daemon only sees this after
        // direct DB edits.
        return Ok(Err(AssembleSkip::Transient(
            "shared library not bound to a mesh library".into(),
        )));
    }

    let Some(capsule_json) = &photo.descriptor_json else {
        return Ok(Err(AssembleSkip::MissingDescriptor));
    };
    let capsule: DescriptorCapsule = match serde_json::from_str(capsule_json) {
        Ok(capsule) => capsule,
        Err(e) => {
            return Ok(Err(AssembleSkip::Transient(format!(
                "descriptor capsule unreadable: {e}"
            ))));
        }
    };

    let blob_paths = spool;
    let records = store.resources_for_photo(&photo.photo_id).await?;
    let mut resources = Vec::new();
    for record in &records {
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

    let sidecar = match Sidecar::compose(
        photo,
        &library,
        capsule.media_type,
        &capsule.media_subtypes,
        capsule.favorite,
        &capsule.capture,
        &records,
    ) {
        Ok(sidecar) => sidecar,
        Err(e) => {
            return Ok(Err(AssembleSkip::Transient(format!(
                "metadata recompose failed: {e}"
            ))));
        }
    };

    Ok(Ok(PublishItem {
        photo: photo.clone(),
        library,
        sidecar,
        resources,
        cloud_fingerprint: None, // stamped by the pass from the resolve entry
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
