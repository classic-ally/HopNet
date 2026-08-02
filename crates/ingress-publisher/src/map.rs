//! Ingress state → RFC-011 `PhotoAsset` mapping.
//!
//! Everything here is pure and total over committed ingress state; any
//! failure is a mapping bug or state corruption, so callers surface map
//! errors as `PublishError::Rejected` (retrying cannot help).

use hopnet_common::Blake3Hash;
use hopnet_photos_core::asset::{
    PhotoAsset, PhotoResource, ResourceContent, ResourceKind, SourceIdentity,
};
use hopnet_photos_core::metadata::PhotoMetadata;
use ingress_core::descriptor::MediaType;
use ingress_core::publish::PublishItem;

/// Extensions the interim raw rule treats as RAW originals (mirrors the
/// `ext_for_uti` raw set; RFC-011 media_type 3 = raw).
const RAW_EXTS: [&str; 6] = ["dng", "arw", "cr2", "cr3", "nef", "raf"];

/// MIME for `format_hint`, covering `ext_for_uti`'s output set. Unknown
/// extensions (and plist adjustment data) degrade to octet-stream.
pub fn mime_for_ext(ext: &str) -> &'static str {
    match ext {
        "jpg" | "jpeg" => "image/jpeg",
        "heic" => "image/heic",
        "heif" => "image/heif",
        "png" => "image/png",
        "gif" => "image/gif",
        "tif" | "tiff" => "image/tiff",
        "webp" => "image/webp",
        "avci" => "image/avci",
        "mov" => "video/quicktime",
        "mp4" => "video/mp4",
        "dng" => "image/x-adobe-dng",
        "arw" => "image/x-sony-arw",
        "cr2" => "image/x-canon-cr2",
        "cr3" => "image/x-canon-cr3",
        "nef" => "image/x-nikon-nef",
        "raf" => "image/x-fuji-raf",
        _ => "application/octet-stream",
    }
}

/// RFC-011 `media_type`: 0 = photo, 1 = video, 2 = live photo, 3 = raw.
/// Raw detection is the interim extension-based rule on the Original
/// resource (PhotoKit exposes no direct "raw asset" flag at the descriptor
/// level the sidecar preserves).
pub fn media_type_code(item: &PublishItem) -> i32 {
    match item.sidecar.media_type {
        MediaType::Video => 1,
        MediaType::LivePhoto => 2,
        MediaType::Image => {
            let original_is_raw = item
                .resources
                .iter()
                .find(|r| r.resource_type.as_str() == ResourceKind::Original.as_str())
                .map(|r| RAW_EXTS.contains(&r.ext.as_str()))
                .unwrap_or(false);
            if original_is_raw { 3 } else { 0 }
        }
    }
}

/// Sidecar group-type name → RFC-011 code (same values the ingress DB uses).
pub fn group_type_code(name: &str) -> Result<i32, String> {
    match name {
        "burst" => Ok(0),
        "stack" => Ok(1),
        "panorama_frames" => Ok(2),
        "hdr_bracket" => Ok(3),
        other => Err(format!("unknown group type `{other}`")),
    }
}

/// Ingress hex ContentHash → the 32-byte `Blake3Hash` RFC-011 carries.
pub fn parse_content_hash(hex_hash: &str) -> Result<Blake3Hash, String> {
    let bytes = hex::decode(hex_hash).map_err(|e| format!("content hash not hex: {e}"))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "content hash is not 32 bytes".to_string())?;
    Ok(Blake3Hash::from_bytes(arr))
}

fn clamp_i32(value: impl TryInto<i32>) -> Option<i32> {
    value.try_into().ok().or(Some(i32::MAX))
}

/// Build the RFC-011 asset for one publishable photo. `SourceIdentity` is
/// dropped at publish (metadata is encrypted client-side) but `validate()`
/// requires it non-empty; `cloud_id` is the cross-device-stable choice, the
/// daemon-minted photo id the fallback for local-only assets.
pub fn to_photo_asset(item: &PublishItem) -> Result<PhotoAsset, String> {
    let sidecar = &item.sidecar;

    let date_taken = sidecar
        .captured_at
        .map(|at| at.to_rfc3339())
        .unwrap_or_else(|| sidecar.ingested_at.to_rfc3339());

    let (group_id, group_type, group_index, is_group_pick) = match &sidecar.group {
        Some(group) => (
            Some(group.id.clone()),
            Some(group_type_code(&group.group_type)?),
            group.index.and_then(clamp_i32),
            Some(i32::from(group.is_pick)),
        ),
        None => (None, None, None, None),
    };

    let metadata = PhotoMetadata {
        date_taken,
        media_type: media_type_code(item),
        width: sidecar.pixel_width.and_then(clamp_i32),
        height: sidecar.pixel_height.and_then(clamp_i32),
        duration_ms: sidecar.duration_ms.and_then(clamp_i32),
        camera_make: sidecar.camera.as_ref().and_then(|c| c.make.clone()),
        camera_model: sidecar.camera.as_ref().and_then(|c| c.model.clone()),
        latitude: sidecar.location.as_ref().map(|l| l.lat),
        longitude: sidecar.location.as_ref().map(|l| l.lon),
        orientation: sidecar.orientation.map(i32::from),
        group_id,
        group_type,
        group_index,
        is_group_pick,
    };

    let mut resources = Vec::with_capacity(item.resources.len());
    for r in &item.resources {
        // Names (and wire discriminants) are identical between the ingress
        // ResourceType and RFC-011 ResourceKind by design.
        let kind = ResourceKind::from_name(r.resource_type.as_str()).ok_or_else(|| {
            format!(
                "resource type `{}` has no RFC-011 kind",
                r.resource_type.as_str()
            )
        })?;
        if r.size_bytes <= 0 {
            return Err(format!("resource {} has non-positive size", kind));
        }
        resources.push(PhotoResource {
            kind,
            content: ResourceContent {
                byte_len: r.size_bytes as u64,
                content_hash: Some(parse_content_hash(r.content_hash.as_str())?),
                format_hint: Some(mime_for_ext(&r.ext).to_string()),
            },
        });
    }

    let asset = PhotoAsset {
        source: SourceIdentity::new(
            "apple_photos",
            item.photo
                .cloud_id
                .clone()
                .unwrap_or_else(|| item.photo.photo_id.as_str().to_string()),
        ),
        metadata,
        resources,
    };
    asset.validate().map_err(|e| e.to_string())?;
    Ok(asset)
}
