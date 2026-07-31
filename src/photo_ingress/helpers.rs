//! Platform-independent pieces of the enablement routes: request validation
//! and status assembly. Kept free of keychain/ServiceManagement so Linux
//! `cargo test` covers them.

use super::{AgentRegistration, PhotoIngressStatus};

/// The blob root must be absolute — same invariant `add_library` enforces —
/// so the daemon never binds a library relative to launchd's working dir.
pub fn validate_blob_root(blob_root: &str) -> Result<(), String> {
    if blob_root.is_empty() {
        return Err("blob_root must not be empty".into());
    }
    if !std::path::Path::new(blob_root).is_absolute() {
        return Err(format!("blob_root must be an absolute path (got {blob_root:?})"));
    }
    Ok(())
}

/// The device-id half of an RFC-012 token (`{device_id}.{secret}`), without
/// validating the UUID shape (the caller's DB lookup does that).
pub fn device_id_from_token(api_key: &str) -> Option<&str> {
    let (id, secret) = api_key.split_once('.')?;
    (!id.is_empty() && !secret.is_empty()).then_some(id)
}

/// Assemble the status response from independently-gathered facts.
/// `keychain` is the stored `(api_key, base_url)` pair when provisioned.
pub fn build_status(
    registration: AgentRegistration,
    keychain: Option<(String, String)>,
    blob_root: Option<String>,
    device_row_present: bool,
) -> PhotoIngressStatus {
    let (device_id, node_base_url) = match &keychain {
        Some((api_key, base_url)) => (
            device_id_from_token(api_key).map(str::to_string),
            Some(base_url.clone()),
        ),
        None => (None, None),
    };
    PhotoIngressStatus {
        registration,
        keychain_provisioned: keychain.is_some(),
        device_id,
        device_row_present,
        blob_root,
        node_base_url,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: accept absolute paths and reject relative or empty ones —
    // the 400 gate ahead of any keychain write.
    #[test]
    fn blob_root_validation() {
        assert!(validate_blob_root("/Users/a/blobs").is_ok());
        assert!(validate_blob_root("relative/blobs").is_err());
        assert!(validate_blob_root("").is_err());
        assert!(validate_blob_root("~/blobs").is_err());
    }

    // Should: extract the device id from a well-formed token.
    // Should not: produce an id from a token missing either half.
    #[test]
    fn device_id_parsing() {
        assert_eq!(device_id_from_token("abc-123.s3cret"), Some("abc-123"));
        assert_eq!(device_id_from_token("no-dot-token"), None);
        assert_eq!(device_id_from_token(".secret-only"), None);
        assert_eq!(device_id_from_token("id-only."), None);
    }

    // Should: report provisioned fields only when the keychain pair exists,
    // deriving the device id from the stored token.
    #[test]
    fn status_assembly_provisioned() {
        let status = build_status(
            AgentRegistration::Enabled,
            Some(("dev-1.secret".into(), "http://127.0.0.1:4242".into())),
            Some("/blobs".into()),
            true,
        );
        assert_eq!(status.registration, AgentRegistration::Enabled);
        assert!(status.keychain_provisioned);
        assert_eq!(status.device_id.as_deref(), Some("dev-1"));
        assert!(status.device_row_present);
        assert_eq!(status.blob_root.as_deref(), Some("/blobs"));
        assert_eq!(status.node_base_url.as_deref(), Some("http://127.0.0.1:4242"));
    }

    // Should not: invent device/url fields on an unprovisioned status.
    #[test]
    fn status_assembly_unprovisioned() {
        let status = build_status(AgentRegistration::NotRegistered, None, None, false);
        assert!(!status.keychain_provisioned);
        assert_eq!(status.device_id, None);
        assert_eq!(status.node_base_url, None);
    }
}
