//! UTI → canonical extension derivation (spec §Architecture: "<ext> is the
//! canonical extension for the resource's UTI").
//!
//! Fallback chain mirrors the archive-and-log posture used for unknown
//! resource types: known table → original filename's extension → `bin` —
//! bytes are never held hostage to a naming table. The `Fallback` case is
//! the caller's cue to emit an `unknown_uti` ingest-log event.

/// How an extension was derived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtDerivation {
    /// UTI found in the known table.
    Known(&'static str),
    /// Unknown UTI; extension taken from the original filename.
    FromFilename(String),
    /// Unknown UTI, no usable filename: `bin`. Caller logs `unknown_uti`.
    Fallback,
}

impl ExtDerivation {
    pub fn ext(&self) -> &str {
        match self {
            ExtDerivation::Known(e) => e,
            ExtDerivation::FromFilename(e) => e,
            ExtDerivation::Fallback => "bin",
        }
    }
}

/// Derive the canonical on-disk extension for a resource.
pub fn ext_for_uti(uti: &str, original_filename: Option<&str>) -> ExtDerivation {
    let known = match uti {
        "public.heic" => Some("heic"),
        "public.heif" => Some("heif"),
        "public.jpeg" => Some("jpg"),
        "public.png" => Some("png"),
        "com.compuserve.gif" => Some("gif"),
        "public.tiff" => Some("tif"),
        "com.apple.quicktime-movie" => Some("mov"),
        "public.mpeg-4" => Some("mp4"),
        "com.adobe.raw-image" => Some("dng"),
        "com.sony.arw-raw-image" => Some("arw"),
        "com.canon.cr2-raw-image" => Some("cr2"),
        "com.canon.cr3-raw-image" => Some("cr3"),
        "com.nikon.raw-image" => Some("nef"),
        "com.fuji.raw-image" => Some("raf"),
        "com.apple.property-list" => Some("plist"),
        "org.webmproject.webp" => Some("webp"),
        "public.avci" => Some("avci"),
        _ => None,
    };
    if let Some(ext) = known {
        return ExtDerivation::Known(ext);
    }
    if let Some(name) = original_filename
        && let Some((_, ext)) = name.rsplit_once('.')
        && !ext.is_empty()
        && ext.len() <= 8
        && ext.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return ExtDerivation::FromFilename(ext.to_ascii_lowercase());
    }
    ExtDerivation::Fallback
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the extension is part of the on-disk blob name forever; a
    // wrong mapping is permanent (first-writer-wins in `blobs`).
    // Should: map every spike-verified UTI to its canonical extension.
    #[test]
    fn spike_verified_utis() {
        assert_eq!(ext_for_uti("public.heic", None), ExtDerivation::Known("heic"));
        assert_eq!(ext_for_uti("public.jpeg", None), ExtDerivation::Known("jpg"));
        assert_eq!(
            ext_for_uti("com.apple.quicktime-movie", None),
            ExtDerivation::Known("mov")
        );
        assert_eq!(
            ext_for_uti("com.sony.arw-raw-image", None),
            ExtDerivation::Known("arw")
        );
        assert_eq!(
            ext_for_uti("com.apple.property-list", None),
            ExtDerivation::Known("plist")
        );
    }

    // Should: fall back to the original filename's extension for unknown UTIs.
    // Should not: trust filenames without a sane extension shape.
    #[test]
    fn filename_fallback() {
        assert_eq!(
            ext_for_uti("com.example.exotic", Some("IMG_0001.XYZ")),
            ExtDerivation::FromFilename("xyz".into())
        );
        assert_eq!(ext_for_uti("com.example.exotic", Some("no-extension")), ExtDerivation::Fallback);
        assert_eq!(
            ext_for_uti("com.example.exotic", Some("weird.ext-with-dash")),
            ExtDerivation::Fallback
        );
        assert_eq!(ext_for_uti("com.example.exotic", None), ExtDerivation::Fallback);
        assert_eq!(ext_for_uti("com.example.exotic", None).ext(), "bin");
    }
}
