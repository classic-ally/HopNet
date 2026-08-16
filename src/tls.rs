//! Per-node TLS identity for the pinned-HTTPS network surface
//! (docs/specs/pinned-https.md).
//!
//! The node's ONLY network listener speaks TLS with a self-signed
//! certificate generated at first boot and persisted under
//! `{data}/hopnet/tls/`. Clients authenticate the node by pinning the
//! certificate's SPKI SHA-256 fingerprint, learned out-of-band during
//! device pairing; certificate contents (SAN, CN, validity) carry no
//! trust weight. The plaintext listener is loopback-only and is not
//! covered by this module.
//!
//! The private key is not zeroized on drop: it lives for the process
//! lifetime inside rustls' ServerConfig anyway.

use once_cell::sync::OnceCell;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const CERT_FILE: &str = "node-cert.pem";
const KEY_FILE: &str = "node-key.pem";

/// The node's TLS identity as loaded from (or first persisted to) disk.
pub struct TlsIdentity {
    cert_pem: String,
    key_pem: String,
    /// Lower-hex SHA-256 of the certificate's SubjectPublicKeyInfo DER —
    /// the value clients pin.
    pub spki_sha256: String,
}

/// Runtime facts about the TLS listener, published once at startup for
/// the pairing-info handler. Mirrors the `ACTUAL_BACKEND_PORT` idiom:
/// the value exists iff the listener came up.
pub struct TlsRuntimeInfo {
    pub https_port: u16,
    pub spki_sha256: String,
}

static RUNTIME: OnceCell<TlsRuntimeInfo> = OnceCell::new();

pub fn publish_runtime_info(info: TlsRuntimeInfo) {
    let _ = RUNTIME.set(info);
}

pub fn runtime_info() -> Option<&'static TlsRuntimeInfo> {
    RUNTIME.get()
}

/// `{data}/hopnet/tls` for a durable node; inside the disposable tree for an
/// ephemeral one, which therefore presents a fresh SPKI on every boot.
pub fn default_tls_dir() -> PathBuf {
    crate::paths::tls_dir()
}

pub fn spki_sha256_hex(spki_der: &[u8]) -> String {
    hex::encode(ring::digest::digest(&ring::digest::SHA256, spki_der))
}

/// Load the persisted identity, or generate and persist one on first
/// boot. Partial or unparseable on-disk material is an ERROR, never a
/// regeneration: a silent re-key would invalidate the SPKI pin held by
/// every paired client. Deleting the tls/ directory is the (manual)
/// re-key path.
pub fn load_or_generate(dir: &Path) -> Result<TlsIdentity, String> {
    let cert_path = dir.join(CERT_FILE);
    let key_path = dir.join(KEY_FILE);
    match (cert_path.exists(), key_path.exists()) {
        (true, true) => load(&cert_path, &key_path),
        (false, false) => generate(dir, &cert_path, &key_path),
        _ => Err(format!(
            "partial TLS material in {}: exactly one of {CERT_FILE}/{KEY_FILE} exists; \
             refusing to re-key because paired clients pin the current SPKI — \
             delete the directory to generate a fresh identity",
            dir.display()
        )),
    }
}

fn load(cert_path: &Path, key_path: &Path) -> Result<TlsIdentity, String> {
    let cert_pem = std::fs::read_to_string(cert_path)
        .map_err(|e| format!("tls cert read {}: {e}", cert_path.display()))?;
    let key_pem = std::fs::read_to_string(key_path)
        .map_err(|e| format!("tls key read {}: {e}", key_path.display()))?;
    // Parse the key too so corruption surfaces here, not at handshake time.
    rcgen::KeyPair::from_pem(&key_pem)
        .map_err(|e| format!("tls key parse (delete the tls/ dir to re-key): {e}"))?;
    let spki_sha256 = spki_from_cert_pem(&cert_pem)?;
    Ok(TlsIdentity {
        cert_pem,
        key_pem,
        spki_sha256,
    })
}

fn generate(dir: &Path, cert_path: &Path, key_path: &Path) -> Result<TlsIdentity, String> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| format!("tls keygen: {e}"))?;
    // SAN/CN are informational only — the pin is the trust anchor.
    let mut params = rcgen::CertificateParams::new(vec!["hopnet-node".to_string()])
        .map_err(|e| format!("tls cert params: {e}"))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "HopNet Node");
    // Fixed, far-future validity: rotation is future work (pinned-https.md)
    // and expiry is not part of the trust decision.
    params.not_before = rcgen::date_time_ymd(2026, 1, 1);
    params.not_after = rcgen::date_time_ymd(2126, 1, 1);
    let cert = params
        .self_signed(&key)
        .map_err(|e| format!("tls self-sign: {e}"))?;

    let cert_pem = cert.pem();
    let key_pem = key.serialize_pem();

    std::fs::create_dir_all(dir).map_err(|e| format!("tls dir {}: {e}", dir.display()))?;
    write_atomic(key_path, key_pem.as_bytes(), true)?;
    write_atomic(cert_path, cert_pem.as_bytes(), false)?;

    let spki_sha256 = spki_from_cert_pem(&cert_pem)?;
    Ok(TlsIdentity {
        cert_pem,
        key_pem,
        spki_sha256,
    })
}

/// tmp.{pid} + rename (the write_lineage_bytes shape); `secret` files get
/// 0600 at creation, the bearer-credential pattern from hopnet-mount.
fn write_atomic(path: &Path, bytes: &[u8], secret: bool) -> Result<(), String> {
    use std::io::Write;

    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if secret {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts
            .open(&tmp)
            .map_err(|e| format!("tls write {}: {e}", tmp.display()))?;
        file.write_all(bytes)
            .map_err(|e| format!("tls write {}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| format!("tls rename {}: {e}", path.display()))?;
    Ok(())
}

/// Extract the SPKI fingerprint from a certificate PEM. Always computed
/// from the certificate (not the key) so the fingerprint reported to the
/// pairing UI is exactly what a connecting client will observe.
fn spki_from_cert_pem(cert_pem: &str) -> Result<String, String> {
    let der = first_cert_der(cert_pem)?;
    let (_, cert) = x509_parser::parse_x509_certificate(&der)
        .map_err(|e| format!("tls cert parse (delete the tls/ dir to re-key): {e}"))?;
    Ok(spki_sha256_hex(cert.public_key().raw))
}

fn first_cert_der(cert_pem: &str) -> Result<Vec<u8>, String> {
    let mut reader = cert_pem.as_bytes();
    let cert = rustls_pemfile::certs(&mut reader)
        .next()
        .ok_or_else(|| "tls cert PEM contains no certificate".to_string())?
        .map_err(|e| format!("tls cert PEM: {e}"))?;
    Ok(cert.to_vec())
}

/// rustls ServerConfig with the ring provider chosen EXPLICITLY — the
/// dependency graph must never rely on a process-default provider (a
/// second provider appearing in the tree would turn that into a panic).
pub fn server_config(identity: &TlsIdentity) -> Result<rustls::ServerConfig, String> {
    let certs = rustls_pemfile::certs(&mut identity.cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("tls cert PEM: {e}"))?;
    let key = rustls_pemfile::private_key(&mut identity.key_pem.as_bytes())
        .map_err(|e| format!("tls key PEM: {e}"))?
        .ok_or_else(|| "tls key PEM contains no private key".to_string())?;

    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| format!("tls protocol versions: {e}"))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|e| format!("tls cert/key: {e}"))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: generate a cert and key on first boot and persist them under
    // tls/ in the data dir.
    #[test]
    fn first_boot_generates_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let tls_dir = dir.path().join("tls");
        let identity = load_or_generate(&tls_dir).unwrap();

        assert!(tls_dir.join(CERT_FILE).exists());
        assert!(tls_dir.join(KEY_FILE).exists());
        assert_eq!(identity.spki_sha256.len(), 64);
        assert!(
            identity
                .spki_sha256
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    // Should: reload the identical identity (same SPKI fingerprint) on
    // subsequent boots.
    #[test]
    fn reload_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let tls_dir = dir.path().join("tls");
        let first = load_or_generate(&tls_dir).unwrap();
        let second = load_or_generate(&tls_dir).unwrap();
        assert_eq!(first.spki_sha256, second.spki_sha256);
        assert_eq!(first.cert_pem, second.cert_pem);
    }

    // Should: report a fingerprint that matches the SPKI embedded in the
    // certificate as persisted on disk (what a connecting client observes).
    #[test]
    fn fingerprint_matches_persisted_cert() {
        let dir = tempfile::tempdir().unwrap();
        let tls_dir = dir.path().join("tls");
        let identity = load_or_generate(&tls_dir).unwrap();

        let on_disk = std::fs::read_to_string(tls_dir.join(CERT_FILE)).unwrap();
        let der = first_cert_der(&on_disk).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&der).unwrap();
        assert_eq!(identity.spki_sha256, spki_sha256_hex(cert.public_key().raw));
    }

    // Should: produce distinct identities for distinct nodes (fresh keypair
    // per data dir, nothing derived from shared material).
    #[test]
    fn identities_are_unique_per_data_dir() {
        let a = load_or_generate(&tempfile::tempdir().unwrap().path().join("tls")).unwrap();
        let b = load_or_generate(&tempfile::tempdir().unwrap().path().join("tls")).unwrap();
        assert_ne!(a.spki_sha256, b.spki_sha256);
    }

    // Should: write the private key file with 0600 permissions.
    #[cfg(unix)]
    #[test]
    fn key_file_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let tls_dir = dir.path().join("tls");
        load_or_generate(&tls_dir).unwrap();
        let mode = std::fs::metadata(tls_dir.join(KEY_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    // Impact: a silent re-key would invalidate the SPKI pin held by every
    // paired client.
    // Should not: silently regenerate over corrupt or partial on-disk
    // material.
    #[test]
    fn corrupt_or_partial_material_errors() {
        let dir = tempfile::tempdir().unwrap();
        let tls_dir = dir.path().join("tls");
        load_or_generate(&tls_dir).unwrap();

        // Partial: key missing, cert present.
        std::fs::remove_file(tls_dir.join(KEY_FILE)).unwrap();
        assert!(load_or_generate(&tls_dir).is_err());
        assert!(tls_dir.join(CERT_FILE).exists(), "must not touch survivors");

        // Corrupt: both present, cert is garbage.
        let dir2 = tempfile::tempdir().unwrap();
        let tls_dir2 = dir2.path().join("tls");
        let original = load_or_generate(&tls_dir2).unwrap();
        std::fs::write(tls_dir2.join(CERT_FILE), "not a pem").unwrap();
        assert!(load_or_generate(&tls_dir2).is_err());
        // And the garbage must survive for postmortem, not be overwritten.
        assert_eq!(
            std::fs::read_to_string(tls_dir2.join(CERT_FILE)).unwrap(),
            "not a pem"
        );
        let _ = original;
    }

    // Should: build a working rustls ServerConfig from a generated identity.
    #[test]
    fn server_config_builds() {
        let dir = tempfile::tempdir().unwrap();
        let identity = load_or_generate(&dir.path().join("tls")).unwrap();
        let config = server_config(&identity).unwrap();
        assert_eq!(config.alpn_protocols.len(), 2);
    }
}
