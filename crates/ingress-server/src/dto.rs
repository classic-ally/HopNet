//! API data-transfer types + the keyset cursor.
//!
//! These are the shapes the REST layer (next slice) returns; `#[typeshare]`
//! annotations for TypeScript generation land then. All numeric fields are
//! `i64`/`f64` to match SQLite column affinity.

use serde::Serialize;
use typeshare::typeshare;

/// Opaque keyset cursor over `(sort_ms DESC, photo_id DESC)`. Serialized to the
/// client as a single base64url token so pagination internals stay private.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub sort_ms: i64,
    pub photo_id: String,
}

impl Cursor {
    /// `"<sort_ms>:<photo_id>"`, base64url (no padding). Hand-rolled so the
    /// crate carries no base64 dependency this slice.
    pub fn to_token(&self) -> String {
        b64url_encode(format!("{}:{}", self.sort_ms, self.photo_id).as_bytes())
    }

    pub fn from_token(token: &str) -> Option<Cursor> {
        let raw = b64url_decode(token)?;
        let s = String::from_utf8(raw).ok()?;
        let (ms, id) = s.split_once(':')?;
        Some(Cursor {
            sort_ms: ms.parse().ok()?,
            photo_id: id.to_string(),
        })
    }
}

/// Browse filters. Each flag is tri-state: `Some(true)` = only, `Some(false)`
/// = exclude, `None` = don't care. Date-range is a deliberate future slot.
#[derive(Debug, Clone, Default)]
pub struct PhotoFilter {
    /// `media_type == "video"`.
    pub video: Option<bool>,
    /// `media_type == "live_photo"`.
    pub live: Option<bool>,
    /// Photo has a `raw_alternate` resource.
    pub raw: Option<bool>,
    pub favorite: Option<bool>,
}

#[typeshare]
#[derive(Debug, Clone, Serialize)]
pub struct LibrarySummary {
    pub library_id: String,
    pub display_name: String,
    /// Shared (multi-person) library — fused views badge its assets.
    pub shared: bool,
    /// Non-tombstoned stills (image + live_photo).
    #[typeshare(serialized_as = "number")]
    pub photo_count: i64,
    /// Non-tombstoned videos.
    #[typeshare(serialized_as = "number")]
    pub video_count: i64,
}

#[typeshare]
#[derive(Debug, Clone, Serialize)]
pub struct PhotoSummary {
    pub photo_id: String,
    pub library_id: String,
    pub captured_at: Option<String>,
    pub media_type: String,
    pub is_live_photo: bool,
    #[typeshare(serialized_as = "number")]
    pub pixel_width: Option<i64>,
    #[typeshare(serialized_as = "number")]
    pub pixel_height: Option<i64>,
    #[typeshare(serialized_as = "number")]
    pub orientation: Option<i64>,
    #[typeshare(serialized_as = "number")]
    pub duration_ms: Option<i64>,
    pub favorite: bool,
    pub media_subtypes: Vec<String>,
    pub group_id: Option<String>,
    pub group_type: Option<String>,
}

#[typeshare]
#[derive(Debug, Clone, Serialize)]
pub struct ResourceInfo {
    pub resource_type: String,
    pub content_hash: String,
    pub ext: String,
    #[typeshare(serialized_as = "number")]
    pub size_bytes: i64,
}

#[typeshare]
#[derive(Debug, Clone, Serialize)]
pub struct PhotoDetail {
    pub photo_id: String,
    pub library_id: String,
    pub cloud_id: Option<String>,
    pub captured_at: Option<String>,
    pub ingested_at: String,
    pub media_type: String,
    pub media_subtypes: Vec<String>,
    #[typeshare(serialized_as = "number")]
    pub pixel_width: Option<i64>,
    #[typeshare(serialized_as = "number")]
    pub pixel_height: Option<i64>,
    #[typeshare(serialized_as = "number")]
    pub orientation: Option<i64>,
    #[typeshare(serialized_as = "number")]
    pub duration_ms: Option<i64>,
    pub camera_make: Option<String>,
    pub camera_model: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub favorite: bool,
    pub group_id: Option<String>,
    pub group_type: Option<String>,
    #[typeshare(serialized_as = "number")]
    pub group_index: Option<i64>,
    pub group_is_pick: Option<bool>,
    pub resources: Vec<ResourceInfo>,
}

#[typeshare]
#[derive(Debug, Clone, Serialize)]
pub struct PhotoPage {
    pub items: Vec<PhotoSummary>,
    pub next_cursor: Option<String>,
}

/// One month of the browse timeline, newest-first, for the histogram rail.
#[typeshare]
#[derive(Debug, Clone, Serialize)]
pub struct MonthBucket {
    /// `"YYYY-MM"` derived from `sort_ms` (so undated photos land under their
    /// ingest month, same total order the grid scrolls through).
    pub month: String,
    #[typeshare(serialized_as = "number")]
    pub count: i64,
}

// --- minimal base64url (RFC 4648 §5, no padding) ---------------------------

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn b64url_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(B64[(n >> 18 & 63) as usize] as char);
        out.push(B64[(n >> 12 & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64[(n >> 6 & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(B64[(n & 63) as usize] as char);
        }
    }
    out
}

fn b64url_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        B64.iter().position(|&b| b == c).map(|p| p as u32)
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let mut n = 0u32;
        let mut bits = 0;
        for &c in chunk {
            n = n << 6 | val(c)?;
            bits += 6;
        }
        // Left-align the collected bits, then emit whole bytes.
        n <<= 24 - bits;
        for i in 0..(bits / 8) {
            out.push((n >> (16 - i * 8) & 0xff) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: round-trip a cursor through its opaque token.
    // Impact: pagination breaks entirely if the token is lossy.
    #[test]
    fn cursor_token_round_trips() {
        let c = Cursor {
            sort_ms: 1_719_800_000_123,
            photo_id: "019f24d7-8770-7703-b68a-09e97f69424f".to_string(),
        };
        let back = Cursor::from_token(&c.to_token()).expect("decodes");
        assert_eq!(c, back);
    }

    // Should not: accept malformed tokens (return None, never panic).
    #[test]
    fn cursor_token_rejects_garbage() {
        assert!(Cursor::from_token("!!!not base64!!!").is_none());
        assert!(Cursor::from_token("YWJj").is_none()); // "abc" — no ':' separator
    }
}
