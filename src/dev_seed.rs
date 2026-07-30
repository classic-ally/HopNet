//! Deterministic synthetic photo generation + HTTP seeding helpers.
//!
//! Consumed by the `photo-seeder` bin (manual seeding of a dev node or a
//! mesh node) and by the orchestrator's `photos-upload-consistency` test,
//! which regenerates the same bytes in-process to byte-compare against
//! node-served content. Determinism is therefore load-bearing: no clock
//! reads, no RNG — everything derives arithmetically from `(seed, index)`.
//!
//! Base-URL convention: helpers take the node origin WITHOUT `/api`
//! (e.g. `http://localhost:34632`) and append `/api/...` themselves.

use anyhow::{Context, bail};
use hopnet_photos_core::asset::{
    PhotoAsset, PhotoResource, ResourceContent, ResourceKind, SourceIdentity,
};
use hopnet_photos_core::metadata::PhotoMetadata;

pub struct GeneratedPhoto {
    pub asset: PhotoAsset,
    /// `(kind, encoded bytes)`: original + thumbnail_medium + thumbnail_small.
    pub resources: Vec<(ResourceKind, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct PostedPhoto {
    pub photo_id: String,
    pub operation_id: String,
}

const CAMERAS: &[(&str, &str)] = &[
    ("Apple", "iPhone 16 Pro"),
    ("Fujifilm", "X-T5"),
    ("Sony", "A7 IV"),
    ("Canon", "EOS R6"),
];

/// Newest seeded month; photos walk backwards from here so the histogram
/// rail gets `months` distinct buckets. Fixed epoch — never `now()`.
const BASE_YEAR: i32 = 2026;
const BASE_MONTH: u32 = 6;

fn mix(seed: u64, index: u32) -> u64 {
    seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(index as u64)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9)
}

fn date_taken(seed: u64, index: u32, months: u32) -> String {
    let months_back = (index % months.max(1)) as i32;
    let mut year = BASE_YEAR;
    let mut month = BASE_MONTH as i32 - months_back;
    while month < 1 {
        month += 12;
        year -= 1;
    }
    let h = mix(seed, index);
    let day = 1 + (h % 27) as u32; // 1..=27, valid in every month
    let hour = (h >> 8) % 24;
    let minute = (h >> 16) % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:00Z")
}

fn encode_jpeg(image: &image::DynamicImage) -> Vec<u8> {
    let mut cursor = std::io::Cursor::new(Vec::new());
    let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut cursor, 85);
    image
        .write_with_encoder(encoder)
        .expect("in-memory JPEG encode cannot fail");
    cursor.into_inner()
}

/// Deterministic: same `(seed, index, months)` -> byte-identical output.
pub fn generate_photo(seed: u64, index: u32, months: u32) -> GeneratedPhoto {
    let h = mix(seed, index);
    // Per-index gradient coefficients — visually distinct cells in the grid.
    let (a, b, c) = ((h % 7 + 1) as u32, ((h >> 4) % 5 + 1) as u32, (h >> 8) as u32);
    let full = image::RgbImage::from_fn(1600, 1200, |x, y| {
        image::Rgb([
            ((x * a / 6 + c) % 256) as u8,
            ((y * b / 5 + c / 3) % 256) as u8,
            (((x + y) * (a + b) / 11 + c / 7) % 256) as u8,
        ])
    });
    let full = image::DynamicImage::ImageRgb8(full);
    let medium = full.resize(1024, 1024, image::imageops::FilterType::Lanczos3);
    let small = full.resize(256, 256, image::imageops::FilterType::Lanczos3);

    let resources = vec![
        (ResourceKind::Original, encode_jpeg(&full)),
        (ResourceKind::ThumbnailMedium, encode_jpeg(&medium)),
        (ResourceKind::ThumbnailSmall, encode_jpeg(&small)),
    ];

    // {0,0,2,0,3} cycle: mostly stills, some live/raw so their badges light
    // up. Never 1 (video) — video bytes are deferred.
    let media_type = match index % 5 {
        2 => 2,
        4 => 3,
        _ => 0,
    };
    let camera = CAMERAS[(h % CAMERAS.len() as u64) as usize];
    let with_camera = index % 3 != 2; // some photos leave optionals unset

    let metadata = PhotoMetadata {
        date_taken: date_taken(seed, index, months),
        media_type,
        width: Some(1600),
        height: Some(1200),
        camera_make: with_camera.then(|| camera.0.to_string()),
        camera_model: with_camera.then(|| camera.1.to_string()),
        ..Default::default()
    };

    let asset = PhotoAsset {
        source: SourceIdentity::new("dev_seed", format!("{seed}-{index}")),
        metadata,
        resources: resources
            .iter()
            .map(|(kind, bytes)| PhotoResource {
                kind: *kind,
                content: ResourceContent {
                    byte_len: bytes.len() as u64,
                    content_hash: None,
                    format_hint: Some("image/jpeg".into()),
                },
            })
            .collect(),
    };

    GeneratedPhoto { asset, resources }
}

/// Bootstrap a fresh node's genesis user. Returns the generated passphrase.
pub async fn setup_node(
    client: &reqwest::Client,
    base_url: &str,
    username: &str,
    node_name: &str,
) -> anyhow::Result<String> {
    let response = client
        .post(format!("{base_url}/api/setup"))
        .json(&serde_json::json!({ "username": username, "node_name": node_name }))
        .send()
        .await
        .context("POST /api/setup")?;
    if response.status() != reqwest::StatusCode::CREATED {
        bail!("setup: {} {}", response.status(), response.text().await.unwrap_or_default());
    }
    let body: serde_json::Value = response.json().await.context("setup response")?;
    body.get("passphrase")
        .and_then(|p| p.as_str())
        .map(str::to_string)
        .context("setup response missing passphrase")
}

/// Log in and return a JWT. Retries for up to ~45s: the Argon2id key unwrap
/// takes 3-5s per attempt and a freshly started node may not be ready yet.
pub async fn login(
    client: &reqwest::Client,
    base_url: &str,
    username: &str,
    passphrase: &str,
) -> anyhow::Result<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(45);
    let mut last_error = String::new();
    loop {
        match client
            .post(format!("{base_url}/api/login"))
            .json(&serde_json::json!({ "username": username, "passphrase": passphrase }))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                let body: serde_json::Value = response.json().await.context("login response")?;
                return body
                    .get("token")
                    .and_then(|t| t.as_str())
                    .map(str::to_string)
                    .context("login response missing token");
            }
            Ok(response) => {
                last_error = format!("{}", response.status());
            }
            Err(e) => {
                last_error = e.to_string();
            }
        }
        if std::time::Instant::now() > deadline {
            bail!("login failed after retries: {last_error}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

pub async fn enable_sidecar(
    client: &reqwest::Client,
    base_url: &str,
    jwt: &str,
) -> anyhow::Result<()> {
    let response = client
        .post(format!("{base_url}/api/photos/sidecar/enable"))
        .header("Authorization", format!("Bearer {jwt}"))
        .send()
        .await
        .context("POST /api/photos/sidecar/enable")?;
    if !response.status().is_success() {
        bail!(
            "enable sidecar: {} {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }
    Ok(())
}

/// POST one generated photo through the manual ingest route.
pub async fn post_photo(
    client: &reqwest::Client,
    base_url: &str,
    jwt: &str,
    photo: &GeneratedPhoto,
) -> anyhow::Result<PostedPhoto> {
    let mut form = reqwest::multipart::Form::new()
        .text("asset", serde_json::to_string(&photo.asset)?);
    for (kind, bytes) in &photo.resources {
        form = form.part(
            kind.as_str().to_string(),
            reqwest::multipart::Part::bytes(bytes.clone())
                .file_name(format!("{}.jpg", kind.as_str())),
        );
    }
    let response = client
        .post(format!("{base_url}/api/photos"))
        .header("Authorization", format!("Bearer {jwt}"))
        .multipart(form)
        .send()
        .await
        .context("POST /api/photos")?;
    if response.status() != reqwest::StatusCode::CREATED {
        bail!(
            "ingest: {} {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }
    let body: serde_json::Value = response.json().await.context("ingest response")?;
    Ok(PostedPhoto {
        photo_id: body
            .get("photo_id")
            .and_then(|v| v.as_str())
            .context("ingest response missing photo_id")?
            .to_string(),
        operation_id: body
            .get("operation_id")
            .and_then(|v| v.as_str())
            .context("ingest response missing operation_id")?
            .to_string(),
    })
}

/// Seed `count` photos; the returned vec preserves index order, so
/// `result[i]` corresponds to `generate_photo(seed, i, months)`.
pub async fn seed_photos(
    client: &reqwest::Client,
    base_url: &str,
    jwt: &str,
    seed: u64,
    count: u32,
    months: u32,
) -> anyhow::Result<Vec<PostedPhoto>> {
    let mut posted = Vec::with_capacity(count as usize);
    for index in 0..count {
        let photo = generate_photo(seed, index, months);
        posted.push(post_photo(client, base_url, jwt, &photo).await?);
    }
    Ok(posted)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the orchestrator e2e byte-compares regenerated bytes against
    // node-served content; any nondeterminism breaks that check silently at
    // a distance.
    // Should: two calls with identical (seed, index, months) yield
    // byte-identical resources and equal assets.
    #[test]
    fn generate_photo_is_deterministic() {
        let a = generate_photo(42, 3, 6);
        let b = generate_photo(42, 3, 6);
        assert_eq!(a.asset, b.asset);
        assert_eq!(a.resources.len(), b.resources.len());
        for ((kind_a, bytes_a), (kind_b, bytes_b)) in a.resources.iter().zip(&b.resources) {
            assert_eq!(kind_a, kind_b);
            assert_eq!(bytes_a, bytes_b, "{kind_a} bytes must be identical");
        }
    }

    // Should not: produce identical original bytes for different indices.
    #[test]
    fn generate_photo_varies_by_index() {
        let a = generate_photo(42, 0, 6);
        let b = generate_photo(42, 1, 6);
        assert_ne!(a.resources[0].1, b.resources[0].1);
    }

    // Should: every generated asset passes validation and each declared
    // byte_len equals the encoded length — the publisher's exact-length
    // enforcement rejects any mismatch mid-put.
    #[test]
    fn generated_assets_validate_and_declare_true_lengths() {
        for index in 0..12 {
            let photo = generate_photo(42, index, 6);
            photo.asset.validate().expect("generated asset must validate");
            for (kind, bytes) in &photo.resources {
                let declared = photo
                    .asset
                    .resources
                    .iter()
                    .find(|r| r.kind == *kind)
                    .expect("resource declared")
                    .content
                    .byte_len;
                assert_eq!(declared, bytes.len() as u64, "byte_len for {kind}");
            }
        }
    }

    // Should: 12 photos over 6 months land in exactly 6 distinct months
    // (the histogram-rail coverage the seeder promises).
    #[test]
    fn date_taken_spans_requested_months() {
        let months: std::collections::HashSet<String> = (0..12)
            .map(|i| generate_photo(42, i, 6).asset.metadata.date_taken[..7].to_string())
            .collect();
        assert_eq!(months.len(), 6);
    }

    // Should not: emit media_type 1 (video) — video bytes are deferred;
    // only image (0), live (2), and raw (3) appear.
    #[test]
    fn media_types_cycle_image_live_raw() {
        let types: std::collections::HashSet<i32> = (0..20)
            .map(|i| generate_photo(42, i, 6).asset.metadata.media_type)
            .collect();
        assert!(!types.contains(&1), "video media_type must not appear");
        assert_eq!(types, [0, 2, 3].into_iter().collect());
    }
}
