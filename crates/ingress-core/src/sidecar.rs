//! Sidecar JSON documents (spec §Sidecar Format).
//!
//! One document per photo; the off-device half of the recovery contract.
//! Serialization matches the spec example literally: `camera` / `location` /
//! `group` / `cloud_id` are omitted when absent; `deleted_at` / `duration_ms`
//! are explicit nulls.

use std::path::PathBuf;

use chrono::{DateTime, Datelike, FixedOffset, Utc};
use serde::{Deserialize, Serialize};

use crate::descriptor::{Camera, CaptureMetadata, Location, MediaType};
use crate::error::{IngressError, Result};
use crate::ids::{LibraryId, PhotoId};
use crate::model::{GroupType, LibraryConfig, PhotoRecord, ResourceRecord};

/// Current sidecar schema tag.
pub const SIDECAR_SCHEMA_V1: &str = "hopnet-photo-ingress/v1";

impl Serialize for MediaType {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(match self {
            MediaType::Image => "image",
            MediaType::Video => "video",
            MediaType::LivePhoto => "live_photo",
        })
    }
}

impl<'de> Deserialize<'de> for MediaType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.as_str() {
            "image" => Ok(MediaType::Image),
            "video" => Ok(MediaType::Video),
            "live_photo" => Ok(MediaType::LivePhoto),
            other => Err(serde::de::Error::custom(format!(
                "unknown media_type {other:?}"
            ))),
        }
    }
}

/// Burst/stack grouping as serialized in sidecars (type by NAME).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SidecarGroup {
    pub id: String,
    #[serde(rename = "type")]
    pub group_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<i64>,
    pub is_pick: bool,
}

/// One written resource as serialized in sidecars (type by NAME).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SidecarResource {
    #[serde(rename = "type")]
    pub resource_type: String,
    pub content_hash: String,
    pub ext: String,
    pub size_bytes: i64,
}

/// A sidecar document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sidecar {
    pub schema: String,
    pub photo_id: PhotoId,
    pub library_id: LibraryId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud_id: Option<String>,
    pub ingested_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,

    pub captured_at: Option<DateTime<FixedOffset>>,
    pub media_type: MediaType,
    #[serde(default)]
    pub media_subtypes: Vec<String>,
    pub pixel_width: Option<u32>,
    pub pixel_height: Option<u32>,
    pub orientation: Option<u16>,
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera: Option<Camera>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    pub favorite: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<SidecarGroup>,

    pub resources: Vec<SidecarResource>,
}

impl Sidecar {
    /// Compose a sidecar from current state. Only durably-written resources
    /// enter the list — a sidecar reflects committed state only.
    pub fn compose(
        photo: &PhotoRecord,
        library: &LibraryConfig,
        media_type: MediaType,
        media_subtypes: &[String],
        favorite: bool,
        capture: &CaptureMetadata,
        resources: &[ResourceRecord],
    ) -> Result<Self> {
        let group = photo.group_id.as_ref().map(|gid| SidecarGroup {
            id: gid.clone(),
            group_type: match photo.group_type {
                Some(1) => GroupType::Stack.as_str(),
                Some(2) => GroupType::PanoramaFrames.as_str(),
                Some(3) => GroupType::HdrBracket.as_str(),
                _ => GroupType::Burst.as_str(),
            }
            .to_string(),
            index: photo.group_index,
            is_pick: photo.is_group_pick,
        });

        let resources = resources
            .iter()
            .filter(|r| r.written_at.is_some())
            .map(|r| {
                Ok(SidecarResource {
                    resource_type: r.resource_type.as_str().to_string(),
                    content_hash: r
                        .content_hash
                        .as_ref()
                        .ok_or_else(|| {
                            IngressError::Invariant(format!(
                                "written resource without content_hash on {}",
                                photo.photo_id
                            ))
                        })?
                        .to_string(),
                    ext: r.ext.clone().unwrap_or_default(),
                    size_bytes: r.size_bytes.unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema: SIDECAR_SCHEMA_V1.to_string(),
            photo_id: photo.photo_id.clone(),
            library_id: library.library_id.clone(),
            cloud_id: photo.cloud_id.clone(),
            ingested_at: photo.discovered_at,
            deleted_at: photo.deleted_at,
            captured_at: capture.captured_at,
            media_type,
            media_subtypes: media_subtypes.to_vec(),
            pixel_width: capture.pixel_width,
            pixel_height: capture.pixel_height,
            orientation: capture.orientation,
            duration_ms: capture.duration_ms,
            camera: capture.camera.clone(),
            location: capture.location,
            favorite,
            group,
            resources,
        })
    }

    /// Parse and version-check a sidecar document.
    pub fn from_json(json: &str) -> Result<Self> {
        #[derive(Deserialize)]
        struct SchemaOnly {
            schema: String,
        }
        let probe: SchemaOnly = serde_json::from_str(json)?;
        if probe.schema != SIDECAR_SCHEMA_V1 {
            return Err(IngressError::UnsupportedSidecarSchema(probe.schema));
        }
        Ok(serde_json::from_str(json)?)
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Relative path under a sidecar root: `YYYY/MM/<photo_id>.json`, keyed
    /// by capture date, falling back to ingest date.
    pub fn rel_path(&self) -> PathBuf {
        let (y, m) = match self.captured_at {
            Some(t) => (t.year(), t.month()),
            None => (self.ingested_at.year(), self.ingested_at.month()),
        };
        PathBuf::from(format!("{y:04}/{m:02}/{}.json", self.photo_id))
    }
}
