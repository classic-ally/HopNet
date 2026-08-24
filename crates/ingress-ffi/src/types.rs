//! FFI records/enums mirroring `ingress_core::descriptor` 1:1.
//!
//! Deliberately hand-mirrored (not uniffi remote/custom types): the boundary
//! stays explicit and independently versionable, and conversion failures are
//! typed errors. Timestamps cross as ISO 8601 strings — `captured_at` must
//! preserve its local UTC offset (the sidecar serializes it verbatim), and
//! `SystemTime` cannot represent that.

#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiLibraryScope {
    Personal,
    Shared,
}

#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiMediaType {
    Image,
    Video,
    LivePhoto,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiCamera {
    pub make: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiLocation {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiBurstInfo {
    pub burst_identifier: String,
    pub is_pick: bool,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiCaptureMetadata {
    /// ISO 8601 with UTC offset (e.g. `2019-08-14T16:22:03+02:00`).
    pub captured_at: Option<String>,
    pub pixel_width: Option<u32>,
    pub pixel_height: Option<u32>,
    pub orientation: Option<u16>,
    pub duration_ms: Option<u64>,
    pub camera: Option<FfiCamera>,
    pub location: Option<FfiLocation>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiResourceDescriptor {
    /// Raw `PHAssetResourceType` value; mapping happens Rust-side.
    pub ph_resource_type: i32,
    pub uti: String,
    pub original_filename: Option<String>,
    pub expected_size: Option<u64>,
    pub locally_available: Option<bool>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiAssetDescriptor {
    pub local_id: String,
    pub cloud_id: Option<String>,
    pub scope: FfiLibraryScope,
    pub media_type: FfiMediaType,
    pub media_subtypes: Vec<String>,
    /// ISO 8601 UTC.
    pub asset_modified_at: Option<String>,
    pub favorite: bool,
    pub burst: Option<FfiBurstInfo>,
    pub capture: FfiCaptureMetadata,
    pub resources: Vec<FfiResourceDescriptor>,
}

/// Outcome of the no-bytes resolution pass (mirrors `resolve::Resolution`).
#[derive(Debug, Clone, uniffi::Enum)]
pub enum FfiResolution {
    AlreadyKnown {
        photo_id: String,
        metadata_changed: bool,
        scope_changed: bool,
    },
    /// Previously-unmapped photo adopted its now-bound library.
    Adopted {
        photo_id: String,
    },
    NeedsOriginal,
    UnmappedScope {
        photo_id: String,
    },
}

/// Outcome of a seed pass (mirrors `resolve::SeedOutcome`).
#[derive(Debug, Clone, uniffi::Enum)]
pub enum FfiSeedOutcome {
    AlreadyKnown { photo_id: String },
    Adopted { photo_id: String },
    MintedPending { photo_id: String, resources: u32 },
    Unmapped { photo_id: String },
}

/// Drain knobs (CLI flags; spec defaults).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiDrainOptions {
    pub fetch_concurrency: u32,
    pub retry_cap: i64,
    pub retry_base_secs: u64,
    pub retry_max_secs: u64,
    pub reserve_floor_gib: u64,
    pub pressure_pause_secs: u64,
    pub storage_poll_secs: u64,
}

/// Drain outcome (mirrors `scheduler::DrainReport`).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiDrainReport {
    pub photos_completed: u64,
    pub resources_written: u64,
    pub resources_deduped: u64,
    pub bytes_written: u64,
    pub late_binding_merges: u64,
    pub swept_partials: u64,
    pub pauses: u64,
    pub awaiting_retry: i64,
    pub gave_up: i64,
    /// ISO 8601, when retries remain.
    pub earliest_next_retry_at: Option<String>,
}

/// Light reconciliation-scan probe (mirrors `scan::ScanProbe`) — identity +
/// scope + modification date only, NO resource enumeration.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiScanProbe {
    pub local_id: String,
    pub cloud_id: Option<String>,
    pub scope: FfiLibraryScope,
    /// ISO 8601 UTC.
    pub asset_modified_at: Option<String>,
}

/// Per-asset probe verdict (mirrors `scan::ScanVerdict`).
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiScanVerdict {
    /// Nothing to do — skip the full descriptor.
    Done,
    /// Build the full descriptor and push via `observe_descriptors`.
    NeedsFull,
}

/// Scan outcome (mirrors `scan::ScanSummary`).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiScanSummary {
    pub probed: u64,
    pub needed_full: u64,
    pub deletions_synthesized: u64,
    pub gave_up_reset: u64,
    pub synthesis_skipped: bool,
}

/// Outcome of the daemon's startup library ensure (see
/// `IngressSession::ensure_personal_library`).
#[derive(Debug, Clone, uniffi::Enum)]
pub enum FfiEnsureLibraryOutcome {
    /// No personal library existed; one was created (CLI-equivalent
    /// defaults).
    Created { library_id: String },
    /// A personal library already existed — nothing written.
    AlreadyExists { library_id: String },
}

/// Daemon knobs — the drain knobs plus the lifecycle-tick cadences (the
/// rescan timer is owned by the platform side, which also owns enumeration).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiDaemonOptions {
    pub fetch_concurrency: u32,
    pub retry_cap: i64,
    pub retry_base_secs: u64,
    pub retry_max_secs: u64,
    pub reserve_floor_gib: u64,
    pub pressure_pause_secs: u64,
    pub storage_poll_secs: u64,
    /// Hourly lifecycle job cadence (hard deletes, log pruning, spool
    /// eviction sweep).
    pub cleanup_interval_secs: u64,
    /// HopNet publish tick: node base URL (WITHOUT `/api`) + RFC-012 device
    /// token (`{device_id}.{secret}`). Both set = publishing on; either
    /// absent = ingest-only daemon (the pre-integration behavior). The
    /// platform side owns credential storage (keychain) — the core never
    /// reads secrets itself.
    pub publish_node_url: Option<String>,
    pub publish_device_token: Option<String>,
    /// Publish tick cadence (seconds).
    pub publish_interval_secs: u64,
    /// Uploads in flight per scope during a publish pass. Clamped to >= 1.
    /// The claim batch is derived as `2 *` this, mirroring the fetch path.
    pub publish_concurrency: u32,
}

/// Lifecycle knobs for the one-shot `cleanup` subcommand.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiCleanupOptions {
    pub log_retention_days: i64,
    pub hard_delete_batch: u32,
}

/// Lifecycle outcome (mirrors `cleanup::CleanupReport`).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiCleanupReport {
    pub photos_hard_deleted: u64,
    pub blob_files_deleted: u64,
    pub log_rows_pruned: u64,
    pub spool_evicted: u64,
}

/// Publish-tick aggregates (mirrors `publish::PublishReport`).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiPublishReport {
    pub published: u64,
    pub already_published: u64,
    /// Photos the mesh already held (cloud-fingerprint match) — stamped
    /// published locally, nothing uploaded.
    pub adopted: u64,
    pub failed: u64,
    pub gave_up: u64,
    /// Skipped: publish-metadata capsule absent (awaiting heal backfill).
    pub missing_descriptor: u64,
    /// Blobs spool-evicted at the end of the pass (every referent decided
    /// in HopNet; local bytes deleted).
    pub evicted_blobs: u64,
    /// The last pass aborted because the node was unreachable.
    pub parked: bool,
    /// The last pass held its photos because this device does not hold
    /// ingress responsibility (claim via the node's JWT route to unpark).
    pub parked_responsibility: bool,
}

/// Daemon outcome (mirrors `daemon::DaemonReport`): drain counters plus the
/// event side and the lifecycle-tick aggregates.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiDaemonReport {
    pub drain: FfiDrainReport,
    pub events_applied: u64,
    pub events_deferred: u64,
    pub deletions: u64,
    pub restores: u64,
    pub transitions: u64,
    pub resources_reopened: u64,
    pub cleanup: FfiCleanupReport,
    pub publish: FfiPublishReport,
}

/// How the original's hash resolved (mirrors `resolve::HashResolution`).
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiHashResolutionKind {
    NewPhoto,
    LateBound,
    SharedBlob,
    /// `begin_resource` path: the photo already existed.
    ExistingPhoto,
}

/// Result of a completed resource stream.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiWriteOutcome {
    pub photo_id: String,
    pub resolution_kind: FfiHashResolutionKind,
    pub content_hash: String,
    pub size_bytes: u64,
    pub ext: String,
    pub deduped: bool,
    pub blob_path: String,
    pub photo_completed: bool,
    /// Set when `photo_completed` and the publish-metadata capsule was
    /// persisted to state.db.
    pub descriptor_persisted: bool,
}
