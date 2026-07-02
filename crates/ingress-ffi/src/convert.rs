//! FFI ↔ core conversions. Parse failures map to `FfiError::InvalidDescriptor`.

use chrono::{DateTime, FixedOffset, Utc};
use ingress_core::descriptor::{
    AssetDescriptor, BurstInfo, Camera, CaptureMetadata, LibraryScope, Location, MediaType,
    ResourceDescriptor,
};

use crate::error::FfiError;
use crate::types::*;

impl From<FfiLibraryScope> for LibraryScope {
    fn from(s: FfiLibraryScope) -> Self {
        match s {
            FfiLibraryScope::Personal => LibraryScope::Personal,
            FfiLibraryScope::Shared => LibraryScope::Shared,
        }
    }
}

impl From<FfiMediaType> for MediaType {
    fn from(m: FfiMediaType) -> Self {
        match m {
            FfiMediaType::Image => MediaType::Image,
            FfiMediaType::Video => MediaType::Video,
            FfiMediaType::LivePhoto => MediaType::LivePhoto,
        }
    }
}

fn parse_utc(field: &str, s: &str) -> Result<DateTime<Utc>, FfiError> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| FfiError::InvalidDescriptor { msg: format!("{field}: {e}") })
}

fn parse_offset(field: &str, s: &str) -> Result<DateTime<FixedOffset>, FfiError> {
    DateTime::parse_from_rfc3339(s)
        .map_err(|e| FfiError::InvalidDescriptor { msg: format!("{field}: {e}") })
}

pub fn descriptor_from_ffi(d: FfiAssetDescriptor) -> Result<AssetDescriptor, FfiError> {
    Ok(AssetDescriptor {
        local_id: d.local_id,
        cloud_id: d.cloud_id,
        scope: d.scope.into(),
        media_type: d.media_type.into(),
        media_subtypes: d.media_subtypes,
        asset_modified_at: d
            .asset_modified_at
            .as_deref()
            .map(|s| parse_utc("asset_modified_at", s))
            .transpose()?,
        favorite: d.favorite,
        burst: d.burst.map(|b| BurstInfo {
            burst_identifier: b.burst_identifier,
            is_pick: b.is_pick,
        }),
        capture: CaptureMetadata {
            captured_at: d
                .capture
                .captured_at
                .as_deref()
                .map(|s| parse_offset("captured_at", s))
                .transpose()?,
            pixel_width: d.capture.pixel_width,
            pixel_height: d.capture.pixel_height,
            orientation: d.capture.orientation,
            duration_ms: d.capture.duration_ms,
            camera: d.capture.camera.map(|c| Camera { make: c.make, model: c.model }),
            location: d.capture.location.map(|l| Location { lat: l.lat, lon: l.lon }),
        },
        resources: d
            .resources
            .into_iter()
            .map(|r| ResourceDescriptor {
                ph_resource_type: r.ph_resource_type,
                uti: r.uti,
                original_filename: r.original_filename,
                expected_size: r.expected_size,
                locally_available: r.locally_available,
            })
            .collect(),
    })
}
