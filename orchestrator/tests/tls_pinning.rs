use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

/// End-to-end pinned-HTTPS (docs/specs/pinned-https.md): every other
/// scenario reaches nodes through `insecure_client`, so this one proves
/// the actual trust mechanism a paired device uses — an SPKI-pinning
/// client against the fingerprint the node advertises for pairing.
pub struct PinnedTlsAccess;

#[derive(Debug, Deserialize)]
struct PairingInfoResponse {
    tls_enabled: bool,
    https_port: Option<u16>,
    spki_sha256: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RegisterDeviceResponse {
    device_id: String,
    api_key: String,
}

mod pin {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{
        WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature,
    };
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};

    /// Accepts exactly one SPKI SHA-256. Everything else about the
    /// certificate — chain, validity window, hostname — is deliberately
    /// ignored: the pin IS the trust decision, mirroring what the Hop
    /// Drive client will do.
    #[derive(Debug)]
    pub struct PinnedSpki {
        expected_hex: String,
        algs: WebPkiSupportedAlgorithms,
    }

    impl PinnedSpki {
        pub fn new(expected_hex: String) -> Self {
            Self {
                expected_hex,
                algs: rustls::crypto::ring::default_provider().signature_verification_algorithms,
            }
        }
    }

    impl ServerCertVerifier for PinnedSpki {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            let (_, cert) = x509_parser::parse_x509_certificate(end_entity)
                .map_err(|e| rustls::Error::General(format!("cert parse: {e}")))?;
            let got = hex::encode(ring::digest::digest(
                &ring::digest::SHA256,
                cert.public_key().raw,
            ));
            if got == self.expected_hex {
                Ok(ServerCertVerified::assertion())
            } else {
                Err(rustls::Error::General(format!(
                    "SPKI pin mismatch: got {got}, expected {}",
                    self.expected_hex
                )))
            }
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls12_signature(message, cert, dss, &self.algs)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls13_signature(message, cert, dss, &self.algs)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.algs.supported_schemes()
        }
    }
}

/// A reqwest client whose ONLY trust anchor is the given SPKI SHA-256.
fn pinned_client(spki_hex: &str) -> Result<Client> {
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(pin::PinnedSpki::new(spki_hex.to_string())))
    .with_no_client_auth();
    Ok(Client::builder().use_preconfigured_tls(tls).build()?)
}

// Should: serve the full API surface on the TLS listener to a client
// pinning the advertised SPKI.
// Should not: complete a handshake for a client pinning a different SPKI.
impl TestScenario for PinnedTlsAccess {
    fn name(&self) -> &'static str {
        "tls-pinned-https"
    }

    fn description(&self) -> &'static str {
        "Fetch the node's SPKI fingerprint, verify a pinning client can use the API and a wrong pin cannot handshake"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let node = &nodes[0];

        println!("\nRunning pinned-TLS checks:");

        // Step 1: pairing info over the (insecure-client) TLS surface.
        let insecure = crate::insecure_client();
        let url = crate::node_url(node, "/api/devices/pairing-info");
        let pairing: PairingInfoResponse = insecure
            .get(&url)
            .header("Authorization", format!("Bearer {}", node.jwt_token))
            .timeout(Duration::from_secs(10))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let spki_ok = pairing.tls_enabled
            && pairing.https_port.is_some()
            && pairing.spki_sha256.as_deref().is_some_and(|s| {
                s.len() == 64
                    && s.chars()
                        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            });
        print_and_add_check(
            &mut result,
            Check {
                name: "pairing-info advertises TLS with a 64-hex SPKI fingerprint".to_string(),
                passed: spki_ok,
                detail: pairing.spki_sha256.clone(),
            },
        );
        if !spki_ok {
            result.duration = start.elapsed();
            return Ok(result);
        }
        let spki = pairing.spki_sha256.unwrap();

        // Step 2: register a device for the DocumentProvider surface.
        let register_url = crate::node_url(node, "/api/devices/register");
        let device: RegisterDeviceResponse = insecure
            .post(&register_url)
            .header("Authorization", format!("Bearer {}", node.jwt_token))
            .json(&serde_json::json!({ "device_name": "tls-pin-probe" }))
            .timeout(Duration::from_secs(10))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Register probe device".to_string(),
                passed: true,
                detail: Some(format!("device_id: {}", device.device_id)),
            },
        );

        // Step 3: the pinned client can use the DocumentProvider API.
        // Poll: device-token replication is a consensus round away.
        let pinned = pinned_client(&spki)?;
        let enumerate_url = crate::node_url(node, "/api/integrations/documentprovider/enumerate");
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut pinned_ok = false;
        let mut last = String::new();
        while Instant::now() < deadline {
            match pinned
                .get(&enumerate_url)
                .header("Authorization", format!("Bearer {}", device.api_key))
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    pinned_ok = true;
                    break;
                }
                Ok(resp) => last = format!("status {}", resp.status()),
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Pinned client reaches the DocumentProvider surface".to_string(),
                passed: pinned_ok,
                detail: (!pinned_ok).then_some(last),
            },
        );

        // Step 4: a wrong pin must fail at the handshake, before any HTTP.
        let mut wrong = spki.clone().into_bytes();
        wrong[0] = if wrong[0] == b'0' { b'1' } else { b'0' };
        let wrong = String::from_utf8(wrong).unwrap();
        let mispinned = pinned_client(&wrong)?;
        let refused = mispinned
            .get(&enumerate_url)
            .header("Authorization", format!("Bearer {}", device.api_key))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .is_err();
        print_and_add_check(
            &mut result,
            Check {
                name: "Mis-pinned client cannot complete a handshake".to_string(),
                passed: refused,
                detail: None,
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
