//! Rendition Renderer (slice 2b): decode a blob and produce a cached JPEG at a
//! bounded size. HEIC via libheif, video poster via ffmpeg, everything else via
//! the pure-Rust `image` crate. Renditions are a viewer-side cache keyed by
//! content_hash — never stored as blobs (keeps the store RFC-011-pure).
//!
//! The decode paths are the ones validated in the Nix decode spike.

use std::path::{Path, PathBuf};
use std::sync::Once;

use image::{DynamicImage, ImageFormat, RgbImage, imageops::FilterType};

/// Which rendition size. Both are decoded from the photo's `original` resource;
/// a video original yields a poster frame.
#[derive(Debug, Clone, Copy)]
pub enum Variant {
    Thumb,
    Display,
}

impl Variant {
    fn max_dim(self) -> u32 {
        match self {
            Variant::Thumb => 400,
            Variant::Display => 2048,
        }
    }
    fn subdir(self) -> &'static str {
        match self {
            Variant::Thumb => "thumb",
            Variant::Display => "display",
        }
    }
}

/// Return the cached JPEG path for this blob + variant, generating it on a miss.
/// CPU-bound decode/encode runs on a blocking thread. The cache is keyed by
/// content_hash, so the result is immutable and safe to cache forever.
pub async fn render_to_cache(
    cache_dir: &Path,
    content_hash: &str,
    blob_path: &Path,
    ext: &str,
    variant: Variant,
) -> anyhow::Result<PathBuf> {
    let out = cache_dir
        .join(variant.subdir())
        .join(format!("{content_hash}.jpg"));
    if out.is_file() {
        return Ok(out);
    }

    let blob_path = blob_path.to_path_buf();
    let ext = ext.to_ascii_lowercase();
    let out_clone = out.clone();
    tokio::task::spawn_blocking(move || generate(&blob_path, &ext, variant, &out_clone))
        .await
        .map_err(|e| anyhow::anyhow!("render join: {e}"))??;
    Ok(out)
}

fn generate(blob: &Path, ext: &str, variant: Variant, out: &Path) -> anyhow::Result<()> {
    let img = match ext {
        "heic" | "heif" => decode_heic(blob)?,
        "mov" | "mp4" | "m4v" => decode_video_poster(blob)?,
        // jpg/jpeg/png/etc — pure-Rust decode.
        _ => image::open(blob)?,
    };
    let d = variant.max_dim();
    let thumb = img.resize(d, d, FilterType::Lanczos3);

    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // temp + rename: concurrent renders of the same key both land atomically.
    let tmp = out.with_extension(format!("jpg.tmp.{}", std::process::id()));
    thumb.to_rgb8().save_with_format(&tmp, ImageFormat::Jpeg)?;
    std::fs::rename(&tmp, out)?;
    Ok(())
}

fn decode_heic(blob: &Path) -> anyhow::Result<DynamicImage> {
    use libheif_rs::{ColorSpace, HeifContext, LibHeif, RgbChroma};
    let path = blob
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non-utf8 blob path"))?;
    let lib = LibHeif::new();
    let ctx = HeifContext::read_from_file(path)?;
    let handle = ctx.primary_image_handle()?;
    let image = lib.decode(&handle, ColorSpace::Rgb(RgbChroma::Rgb), None)?;
    interleaved_to_dynamic(image.width(), image.height(), &image)
}

fn interleaved_to_dynamic(
    w: u32,
    h: u32,
    image: &libheif_rs::Image,
) -> anyhow::Result<DynamicImage> {
    let planes = image.planes();
    let plane = planes
        .interleaved
        .ok_or_else(|| anyhow::anyhow!("no interleaved rgb plane"))?;
    let stride = plane.stride;
    let data = plane.data;
    let mut buf = Vec::with_capacity((w * h * 3) as usize);
    for y in 0..h as usize {
        let s = y * stride;
        buf.extend_from_slice(&data[s..s + (w * 3) as usize]);
    }
    let rgb = RgbImage::from_raw(w, h, buf).ok_or_else(|| anyhow::anyhow!("rgb buffer size"))?;
    Ok(DynamicImage::ImageRgb8(rgb))
}

static FFMPEG_INIT: Once = Once::new();

fn decode_video_poster(blob: &Path) -> anyhow::Result<DynamicImage> {
    use ffmpeg::format::Pixel;
    use ffmpeg::media::Type;
    use ffmpeg::software::scaling::{Context as Scaler, Flags};
    use ffmpeg::util::frame::video::Video;
    use ffmpeg_next as ffmpeg;

    FFMPEG_INIT.call_once(|| {
        let _ = ffmpeg::init();
    });

    let mut ictx = ffmpeg::format::input(&blob)?;
    let stream = ictx
        .streams()
        .best(Type::Video)
        .ok_or_else(|| anyhow::anyhow!("no video stream"))?;
    let idx = stream.index();
    let mut decoder = ffmpeg::codec::context::Context::from_parameters(stream.parameters())?
        .decoder()
        .video()?;
    let mut scaler = Scaler::get(
        decoder.format(),
        decoder.width(),
        decoder.height(),
        Pixel::RGB24,
        decoder.width(),
        decoder.height(),
        Flags::BILINEAR,
    )?;

    for (s, packet) in ictx.packets() {
        if s.index() != idx {
            continue;
        }
        decoder.send_packet(&packet)?;
        let mut frame = Video::empty();
        if decoder.receive_frame(&mut frame).is_ok() {
            let mut rgb = Video::empty();
            scaler.run(&frame, &mut rgb)?;
            let (w, h) = (rgb.width(), rgb.height());
            let stride = rgb.stride(0);
            let data = rgb.data(0);
            let mut buf = Vec::with_capacity((w * h * 3) as usize);
            for y in 0..h as usize {
                let so = y * stride;
                buf.extend_from_slice(&data[so..so + (w * 3) as usize]);
            }
            let img =
                RgbImage::from_raw(w, h, buf).ok_or_else(|| anyhow::anyhow!("rgb buffer size"))?;
            return Ok(DynamicImage::ImageRgb8(img));
        }
    }
    Err(anyhow::anyhow!("no frame decoded"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the grid is unusable if renditions don't shrink or aren't valid
    // JPEGs. Should: downscale a source image to within the variant bound and
    // write a decodable JPEG; a second call hits the cache (no regeneration).
    #[tokio::test]
    async fn renders_and_caches_a_thumbnail() {
        let dir = tempfile::tempdir().unwrap();
        // A pure-Rust source (no C libs needed for this path): a 1000x800 PNG.
        let src = dir.path().join("src.png");
        image::RgbImage::from_fn(1000, 800, |x, _| image::Rgb([(x % 256) as u8, 0, 0]))
            .save(&src)
            .unwrap();

        let cache = dir.path().join("cache");
        let out = render_to_cache(&cache, "deadbeef", &src, "png", Variant::Thumb)
            .await
            .unwrap();
        assert!(out.is_file());
        let decoded = image::open(&out).unwrap();
        assert!(decoded.width() <= 400 && decoded.height() <= 400);
        assert!(decoded.width() == 400 || decoded.height() == 400); // fit to bound

        // Second call: cache hit — same path, and it must not error.
        let out2 = render_to_cache(&cache, "deadbeef", &src, "png", Variant::Thumb)
            .await
            .unwrap();
        assert_eq!(out, out2);
    }
}
