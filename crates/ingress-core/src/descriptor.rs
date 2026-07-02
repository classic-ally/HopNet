//! The Swift↔Rust handover boundary: what the PhotoKit layer hands the core
//! for every observed asset. Shapes are dictated by the Phase 0 spike
//! (`spikes/photokit/FINDINGS.md`) — plain data, no PhotoKit types.

use chrono::{DateTime, FixedOffset, Utc};

/// Which library partition an asset belongs to.
///
/// Binary by construction: iCloud supports at most one Shared Photo Library
/// per account, and PhotoKit exposes no scope identifier (detection is the
/// `participatesInLibraryScope` private property; see FINDINGS).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LibraryScope {
    Personal,
    Shared,
}

/// Media kind, as the sidecar serializes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaType {
    Image,
    Video,
    LivePhoto,
}

/// Burst membership as PhotoKit reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BurstInfo {
    /// Opaque PhotoKit identifier. Never persisted raw — hashed into
    /// `group_id` (see `ids::derive_group_id`).
    pub burst_identifier: String,
    /// PhotoKit's "user pick" hint; exactly one frame per burst.
    pub is_pick: bool,
}

/// Camera make/model pair.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Camera {
    pub make: Option<String>,
    pub model: Option<String>,
}

/// GPS coordinates, present only when the asset has location data.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Location {
    pub lat: f64,
    pub lon: f64,
}

/// PhotoKit-derived capture metadata. Feeds sidecars only — none of this
/// lands in `state.db`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CaptureMetadata {
    /// Capture time as PhotoKit resolves it, preserving the local UTC offset.
    pub captured_at: Option<DateTime<FixedOffset>>,
    pub pixel_width: Option<u32>,
    pub pixel_height: Option<u32>,
    /// EXIF orientation (1–8).
    pub orientation: Option<u16>,
    /// Video / Live Photo duration.
    pub duration_ms: Option<u64>,
    pub camera: Option<Camera>,
    pub location: Option<Location>,
}

/// One `PHAssetResource` as enumerated on the asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceDescriptor {
    /// Raw `PHAssetResourceType` value. Mapping to [`crate::model::ResourceType`]
    /// happens Rust-side so the table has a single, unit-testable home.
    pub ph_resource_type: i32,
    /// Uniform type identifier (e.g. `public.heic`); canonical extension is
    /// derived from this.
    pub uti: String,
    pub original_filename: Option<String>,
    /// The `fileSize` KVC value — byte-accurate per the spike, but still
    /// `Option` because the key is undocumented.
    pub expected_size: Option<u64>,
    /// The `locallyAvailable` KVC value; advisory "will this fetch hit the
    /// network" signal.
    pub locally_available: Option<bool>,
}

/// Everything the Swift layer knows about one `PHAsset`, in plain data.
#[derive(Debug, Clone, PartialEq)]
pub struct AssetDescriptor {
    /// `PHAsset.localIdentifier` — device-scoped convenience handle, not identity.
    pub local_id: String,
    /// `PHCloudIdentifier.stringValue`; `None` for local-only assets.
    pub cloud_id: Option<String>,
    pub scope: LibraryScope,
    pub media_type: MediaType,
    /// PhotoKit computed subtype flags: "hdr", "screenshot", "panorama", "slomo", …
    pub media_subtypes: Vec<String>,
    /// `PHAsset.modificationDate` — drives the fast path's no-op check.
    pub asset_modified_at: Option<DateTime<Utc>>,
    pub favorite: bool,
    pub burst: Option<BurstInfo>,
    pub capture: CaptureMetadata,
    pub resources: Vec<ResourceDescriptor>,
}
