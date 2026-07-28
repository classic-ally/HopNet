//! Source-independent photo assets passed from ingest adapters to publishers.

use crate::metadata::PhotoMetadata;
use hopnet_common::Blake3Hash;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceIdentity {
    /// Adapter-owned namespace. Callers must canonicalize it before building
    /// an identity; the shared model preserves it as opaque data.
    pub source: String,
    /// Adapter-owned identifier. Callers must canonicalize it before building
    /// an identity; the shared model preserves it as opaque data.
    pub id: String,
}

impl SourceIdentity {
    pub fn new(source: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            id: id.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
pub enum ResourceKind {
    Original = 0,
    Edited = 1,
    PairedVideo = 2,
    AdjustmentData = 3,
    RawAlternate = 4,
    ThumbnailSmall = 5,
    ThumbnailMedium = 6,
    EditedPairedVideo = 7,
}

impl ResourceKind {
    pub const ALL: [Self; 8] = [
        Self::Original,
        Self::Edited,
        Self::PairedVideo,
        Self::AdjustmentData,
        Self::RawAlternate,
        Self::ThumbnailSmall,
        Self::ThumbnailMedium,
        Self::EditedPairedVideo,
    ];

    pub const fn as_wire(self) -> i32 {
        self as i32
    }

    pub const fn from_wire(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Original),
            1 => Some(Self::Edited),
            2 => Some(Self::PairedVideo),
            3 => Some(Self::AdjustmentData),
            4 => Some(Self::RawAlternate),
            5 => Some(Self::ThumbnailSmall),
            6 => Some(Self::ThumbnailMedium),
            7 => Some(Self::EditedPairedVideo),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Edited => "edited",
            Self::PairedVideo => "paired_video",
            Self::AdjustmentData => "adjustment_data",
            Self::RawAlternate => "raw_alternate",
            Self::ThumbnailSmall => "thumbnail_small",
            Self::ThumbnailMedium => "thumbnail_medium",
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
            "thumbnail_small" => Some(Self::ThumbnailSmall),
            "thumbnail_medium" => Some(Self::ThumbnailMedium),
            "edited_paired_video" => Some(Self::EditedPairedVideo),
            _ => None,
        }
    }

    pub const fn is_thumbnail(self) -> bool {
        matches!(self, Self::ThumbnailSmall | Self::ThumbnailMedium)
    }
}

impl TryFrom<i32> for ResourceKind {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Self::from_wire(value).ok_or(value)
    }
}

impl From<ResourceKind> for i32 {
    fn from(kind: ResourceKind) -> Self {
        kind.as_wire()
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceContent {
    pub byte_len: u64,
    pub content_hash: Option<Blake3Hash>,
    pub format_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhotoResource {
    pub kind: ResourceKind,
    pub content: ResourceContent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhotoAsset {
    pub source: SourceIdentity,
    pub metadata: PhotoMetadata,
    pub resources: Vec<PhotoResource>,
}

impl PhotoAsset {
    /// Validates resource structure. Metadata field validation belongs to the
    /// source adapter until the media-type contract is finalized.
    pub fn validate(&self) -> Result<(), AssetValidationError> {
        if self.source.source.is_empty() {
            return Err(AssetValidationError::EmptySourceNamespace);
        }
        if self.source.id.is_empty() {
            return Err(AssetValidationError::EmptySourceId);
        }
        if self.resources.is_empty() {
            return Err(AssetValidationError::NoResources);
        }

        let mut seen = 0u16;
        for resource in &self.resources {
            let bit = 1u16 << resource.kind.as_wire();
            if seen & bit != 0 {
                return Err(AssetValidationError::DuplicateResourceKind(resource.kind));
            }
            seen |= bit;

            if resource.content.byte_len == 0 {
                return Err(AssetValidationError::EmptyResource(resource.kind));
            }
        }

        if self.resource(ResourceKind::Original).is_none() {
            return Err(AssetValidationError::MissingOriginalResource);
        }

        Ok(())
    }

    pub fn resource(&self, kind: ResourceKind) -> Option<&PhotoResource> {
        self.resources.iter().find(|resource| resource.kind == kind)
    }

    pub fn primary_display_kind(&self) -> Option<ResourceKind> {
        if self.resource(ResourceKind::Edited).is_some() {
            Some(ResourceKind::Edited)
        } else if self.resource(ResourceKind::Original).is_some() {
            Some(ResourceKind::Original)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AssetValidationError {
    #[error("source namespace must not be empty")]
    EmptySourceNamespace,
    #[error("source id must not be empty")]
    EmptySourceId,
    #[error("asset has no resources")]
    NoResources,
    #[error("duplicate resource kind: {0}")]
    DuplicateResourceKind(ResourceKind),
    #[error("resource {0} declares zero bytes")]
    EmptyResource(ResourceKind),
    #[error("asset has no original resource")]
    MissingOriginalResource,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource(kind: ResourceKind) -> PhotoResource {
        PhotoResource {
            kind,
            content: ResourceContent {
                byte_len: 1,
                content_hash: None,
                format_hint: Some("image/jpeg".into()),
            },
        }
    }

    fn asset(resources: Vec<PhotoResource>) -> PhotoAsset {
        PhotoAsset {
            source: SourceIdentity::new("upload", "asset-1"),
            metadata: PhotoMetadata {
                date_taken: "2025-01-01T00:00:00Z".into(),
                media_type: 0,
                ..Default::default()
            },
            resources,
        }
    }

    #[test]
    fn resource_kind_wire_values_match_rfc011() {
        assert_eq!(ResourceKind::Original.as_wire(), 0);
        assert_eq!(ResourceKind::Edited.as_wire(), 1);
        assert_eq!(ResourceKind::PairedVideo.as_wire(), 2);
        assert_eq!(ResourceKind::AdjustmentData.as_wire(), 3);
        assert_eq!(ResourceKind::RawAlternate.as_wire(), 4);
        assert_eq!(ResourceKind::ThumbnailSmall.as_wire(), 5);
        assert_eq!(ResourceKind::ThumbnailMedium.as_wire(), 6);
        assert_eq!(ResourceKind::EditedPairedVideo.as_wire(), 7);
    }

    #[test]
    fn resource_kind_wire_values_round_trip() {
        for kind in ResourceKind::ALL {
            assert_eq!(ResourceKind::from_wire(kind.as_wire()), Some(kind));
        }
        assert_eq!(ResourceKind::from_wire(-1), None);
        assert_eq!(ResourceKind::from_wire(8), None);
    }

    #[test]
    fn resource_kind_converts_to_and_from_wire_integers() {
        for kind in ResourceKind::ALL {
            let wire: i32 = kind.into();
            assert_eq!(ResourceKind::try_from(wire), Ok(kind));
        }
        assert_eq!(ResourceKind::try_from(8), Err(8));
    }

    #[test]
    fn resource_kind_names_round_trip() {
        for kind in ResourceKind::ALL {
            assert_eq!(ResourceKind::from_name(kind.as_str()), Some(kind));
        }
        assert_eq!(ResourceKind::from_name("thumbnail"), None);
        assert_eq!(ResourceKind::from_name(""), None);
    }

    #[test]
    fn resource_kind_json_uses_spec_names() {
        assert_eq!(
            serde_json::to_string(&ResourceKind::ThumbnailSmall).unwrap(),
            "\"thumbnail_small\""
        );
        assert_eq!(
            serde_json::from_str::<ResourceKind>("\"edited_paired_video\"").unwrap(),
            ResourceKind::EditedPairedVideo
        );
    }

    #[test]
    fn thumbnails_are_classified() {
        assert!(ResourceKind::ThumbnailSmall.is_thumbnail());
        assert!(ResourceKind::ThumbnailMedium.is_thumbnail());
        assert!(!ResourceKind::Original.is_thumbnail());
        assert!(!ResourceKind::Edited.is_thumbnail());
    }

    #[test]
    fn validates_original_only_asset() {
        assert!(asset(vec![resource(ResourceKind::Original)])
            .validate()
            .is_ok());
    }

    #[test]
    fn validates_full_resource_set() {
        let resources = ResourceKind::ALL.into_iter().map(resource).collect();
        assert!(asset(resources).validate().is_ok());
    }

    #[test]
    fn rejects_empty_resources() {
        assert_eq!(
            asset(Vec::new()).validate(),
            Err(AssetValidationError::NoResources)
        );
    }

    #[test]
    fn rejects_duplicate_resources() {
        assert_eq!(
            asset(vec![
                resource(ResourceKind::Original),
                resource(ResourceKind::Original)
            ])
            .validate(),
            Err(AssetValidationError::DuplicateResourceKind(
                ResourceKind::Original
            ))
        );
    }

    #[test]
    fn rejects_thumbnail_only_asset() {
        assert_eq!(
            asset(vec![
                resource(ResourceKind::ThumbnailSmall),
                resource(ResourceKind::ThumbnailMedium)
            ])
            .validate(),
            Err(AssetValidationError::MissingOriginalResource)
        );
    }

    #[test]
    fn rejects_edited_only_asset() {
        assert_eq!(
            asset(vec![resource(ResourceKind::Edited)]).validate(),
            Err(AssetValidationError::MissingOriginalResource)
        );
    }

    #[test]
    fn rejects_zero_byte_resource() {
        let mut original = resource(ResourceKind::Original);
        original.content.byte_len = 0;
        assert_eq!(
            asset(vec![original]).validate(),
            Err(AssetValidationError::EmptyResource(ResourceKind::Original))
        );
    }

    #[test]
    fn rejects_empty_source_fields_in_order() {
        let mut empty_namespace = asset(vec![resource(ResourceKind::Original)]);
        empty_namespace.source.source.clear();
        assert_eq!(
            empty_namespace.validate(),
            Err(AssetValidationError::EmptySourceNamespace)
        );

        let mut empty_id = asset(vec![resource(ResourceKind::Original)]);
        empty_id.source.id.clear();
        assert_eq!(
            empty_id.validate(),
            Err(AssetValidationError::EmptySourceId)
        );
    }

    #[test]
    fn edited_resource_is_preferred_for_display() {
        let photo = asset(vec![
            resource(ResourceKind::Original),
            resource(ResourceKind::Edited),
        ]);
        assert_eq!(photo.primary_display_kind(), Some(ResourceKind::Edited));
    }

    #[test]
    fn original_is_display_fallback() {
        let photo = asset(vec![resource(ResourceKind::Original)]);
        assert_eq!(photo.primary_display_kind(), Some(ResourceKind::Original));
    }

    #[test]
    fn resource_lookup_finds_matching_kind() {
        let photo = asset(vec![
            resource(ResourceKind::Original),
            resource(ResourceKind::PairedVideo),
        ]);
        assert_eq!(
            photo.resource(ResourceKind::PairedVideo).unwrap().kind,
            ResourceKind::PairedVideo
        );
        assert!(photo.resource(ResourceKind::RawAlternate).is_none());
    }

    #[test]
    fn resource_kind_display_matches_name() {
        assert_eq!(
            ResourceKind::ThumbnailMedium.to_string(),
            "thumbnail_medium"
        );
    }

    #[test]
    fn asset_json_round_trips() {
        let mut photo = asset(vec![resource(ResourceKind::Original)]);
        photo.resources[0].content.content_hash = Some(Blake3Hash::from_bytes([0x11; 32]));
        let json = serde_json::to_vec(&photo).unwrap();
        assert_eq!(serde_json::from_slice::<PhotoAsset>(&json).unwrap(), photo);
    }

    #[test]
    fn source_identity_is_namespaced() {
        assert_ne!(
            SourceIdentity::new("photokit", "asset-1"),
            SourceIdentity::new("upload", "asset-1")
        );
    }
}
