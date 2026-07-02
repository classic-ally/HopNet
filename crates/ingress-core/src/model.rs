//! Persistent record types mirroring the `state.db` schema
//! (spec §Local State Schema), plus the enums shared with sidecars.

use chrono::{DateTime, Utc};

use crate::ids::{ContentHash, LibraryId, PhotoId};

/// RFC-011 resource type values, used verbatim in `photo_resources.resource_type`.
/// Thumbnails (5, 6) are deliberately absent — never stored by the daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(i64)]
pub enum ResourceType {
    Original = 0,
    Edited = 1,
    PairedVideo = 2,
    AdjustmentData = 3,
    RawAlternate = 4,
    EditedPairedVideo = 7,
}

impl ResourceType {
    /// Map a raw `PHAssetResourceType` value (spec §PhotoKit → ingress
    /// resource mapping, spike-verified).
    ///
    /// `None` = unrecognized type. Per the archive-known-and-log decision,
    /// the caller records an `unknown_resource_type` ingest-log event and
    /// skips the resource without blocking the asset.
    pub fn from_ph_type(ph: i32) -> Option<Self> {
        match ph {
            1 | 2 => Some(Self::Original),       // photo / video
            5 | 6 => Some(Self::Edited),         // fullSizePhoto / fullSizeVideo
            9 => Some(Self::PairedVideo),        // pairedVideo
            7 => Some(Self::AdjustmentData),     // adjustmentData
            4 => Some(Self::RawAlternate),       // alternatePhoto
            10 => Some(Self::EditedPairedVideo), // fullSizePairedVideo
            _ => None,
        }
    }

    /// Sidecars serialize resource types by NAME, not integer.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Edited => "edited",
            Self::PairedVideo => "paired_video",
            Self::AdjustmentData => "adjustment_data",
            Self::RawAlternate => "raw_alternate",
            Self::EditedPairedVideo => "edited_paired_video",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "original" => Some(Self::Original),
            "edited" => Some(Self::Edited),
            "paired_video" => Some(Self::PairedVideo),
            "adjustment_data" => Some(Self::AdjustmentData),
            "raw_alternate" => Some(Self::RawAlternate),
            "edited_paired_video" => Some(Self::EditedPairedVideo),
            _ => None,
        }
    }
}

/// RFC-011 group type values (`photos.group_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[repr(i64)]
pub enum GroupType {
    Burst = 0,
    Stack = 1,
    PanoramaFrames = 2,
    HdrBracket = 3,
}

impl GroupType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Burst => "burst",
            Self::Stack => "stack",
            Self::PanoramaFrames => "panorama_frames",
            Self::HdrBracket => "hdr_bracket",
        }
    }
}

/// A `photos` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PhotoRecord {
    pub photo_id: PhotoId,
    /// NULL = unmapped scope; ingest blocked until the user binds it.
    pub library_id: Option<LibraryId>,
    pub cloud_id: Option<String>,
    pub local_id: Option<String>,

    pub group_id: Option<String>,
    pub group_type: Option<i64>,
    pub group_index: Option<i64>,
    pub is_group_pick: bool,

    pub discovered_at: DateTime<Utc>,
    pub asset_modified_at: Option<DateTime<Utc>>,
    pub materialized_at: Option<DateTime<Utc>>,
    pub sidecar_replicated_at: Option<DateTime<Utc>>,

    pub deleted_at: Option<DateTime<Utc>>,
}

/// A `photo_resources` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ResourceRecord {
    pub photo_id: PhotoId,
    pub resource_type: ResourceType,
    /// NULL until fetched+hashed; commits together with `written_at`
    /// (two-state rule, spec §Per-resource state machine).
    pub content_hash: Option<ContentHash>,
    pub ext: Option<String>,
    pub size_bytes: Option<i64>,

    pub written_at: Option<DateTime<Utc>>,
    pub retry_count: i64,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

/// A `libraries` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LibraryConfig {
    pub library_id: LibraryId,
    pub display_name: String,
    pub blob_root: String,
    pub sidecar_root_remote: Option<String>,
    /// PhotoKit scope binding; the shared library uses the fixed marker
    /// `icloud-shared-library` (binary scope signal, one SPL per account).
    pub scope_binding: Option<String>,
    pub retention_days: i64,
    pub created_at: DateTime<Utc>,
}

/// The fixed `scope_binding` marker for the iCloud Shared Photo Library.
pub const ICLOUD_SHARED_LIBRARY_BINDING: &str = "icloud-shared-library";

/// A `blobs` row (refcount bookkeeping; file I/O is Phase 2+).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct BlobRecord {
    pub library_id: LibraryId,
    pub content_hash: ContentHash,
    pub ext: String,
    pub size_bytes: i64,
    pub ref_count: i64,
    pub written_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the PH mapping table is the single source of truth for which
    // PhotoKit resources get archived; a wrong entry silently drops or
    // misfiles user bytes.
    // Should: map every spike-verified PHAssetResourceType to its RFC-011 value.
    // Should not: map unknown types (audio=3, adjustmentBasePhoto=8).
    #[test]
    fn ph_type_mapping_matches_spec_table() {
        assert_eq!(ResourceType::from_ph_type(1), Some(ResourceType::Original));
        assert_eq!(ResourceType::from_ph_type(2), Some(ResourceType::Original));
        assert_eq!(ResourceType::from_ph_type(5), Some(ResourceType::Edited));
        assert_eq!(ResourceType::from_ph_type(6), Some(ResourceType::Edited));
        assert_eq!(
            ResourceType::from_ph_type(9),
            Some(ResourceType::PairedVideo)
        );
        assert_eq!(
            ResourceType::from_ph_type(7),
            Some(ResourceType::AdjustmentData)
        );
        assert_eq!(
            ResourceType::from_ph_type(4),
            Some(ResourceType::RawAlternate)
        );
        assert_eq!(
            ResourceType::from_ph_type(10),
            Some(ResourceType::EditedPairedVideo)
        );
        assert_eq!(ResourceType::from_ph_type(3), None);
        assert_eq!(ResourceType::from_ph_type(8), None);
        assert_eq!(ResourceType::from_ph_type(999), None);
    }

    // Should: round-trip every resource type through its sidecar name.
    #[test]
    fn resource_type_name_round_trip() {
        for rt in [
            ResourceType::Original,
            ResourceType::Edited,
            ResourceType::PairedVideo,
            ResourceType::AdjustmentData,
            ResourceType::RawAlternate,
            ResourceType::EditedPairedVideo,
        ] {
            assert_eq!(ResourceType::from_name(rt.as_str()), Some(rt));
        }
    }
}
