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
    AlreadyKnown { photo_id: String, metadata_changed: bool, scope_changed: bool },
    /// Previously-unmapped photo adopted its now-bound library.
    Adopted { photo_id: String },
    NeedsOriginal,
    UnmappedScope { photo_id: String },
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
    /// Set when `photo_completed` and the local sidecar was written.
    pub sidecar_path: Option<String>,
}
