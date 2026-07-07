use crate::db::types::XPubKey;

// --- Consensus payload structs ---

/// Drive-owned (RFC-015, Stage D3): the share consensus payloads are drive
/// wire types and live in hopnet-drive's envelopes (with their handlers);
/// re-exported here so call sites don't churn.
pub use hopnet_drive::envelopes::{
    AcceptSharePayload, DeclineSharePayload, ShareFilePayload, UnsharePayload,
};

/// Drive-owned (RFC-015): IncomingShareUpdate is a drive wire type and
/// lives in hopnet-drive's envelopes; re-exported here so call sites
/// don't churn.
pub use hopnet_drive::envelopes::IncomingShareUpdate;

// --- API request/response types (re-exported from common) ---

pub use hopnet_common::shares::{
    AcceptShareRequest, IncomingShareResponse, ShareCountResponse, ShareDetailResponse,
    ShareParticipant, ShareRequest,
};

// --- Display name crypto ---

/// Drive-owned (RFC-015, Stage D4): the display-name crypto moved with the
/// shares routes to hopnet_drive::http::shares; thin delegation here keeps
/// the XPubKey-typed signature and unit tests stable.
pub fn encrypt_display_name(
    plaintext: &str,
    recipient_x25519_pubkey: &XPubKey,
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    hopnet_drive::http::shares::encrypt_display_name(
        plaintext,
        recipient_x25519_pubkey.as_x25519(),
    )
}

/// Decrypt a display name using the recipient's X25519 private key.
pub use hopnet_drive::http::shares::decrypt_display_name;

#[cfg(test)]
mod tests {
    use super::*;
    use chacha20poly1305::aead::OsRng;
    use x25519_dalek::PublicKey as X25519PublicKey;

    #[test]
    fn test_display_name_encrypt_decrypt_round_trip() {
        let secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let pubkey = X25519PublicKey::from(&secret);
        let xpubkey = XPubKey::from(pubkey);

        let plaintext = "document.pdf";
        let (eph_pubkey, ciphertext) = encrypt_display_name(plaintext, &xpubkey).unwrap();
        let decrypted = decrypt_display_name(&eph_pubkey, &ciphertext, &secret).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_display_name_wrong_key_fails() {
        let secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let pubkey = X25519PublicKey::from(&secret);
        let xpubkey = XPubKey::from(pubkey);

        let wrong_secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);

        let (eph_pubkey, ciphertext) = encrypt_display_name("secret.txt", &xpubkey).unwrap();
        let result = decrypt_display_name(&eph_pubkey, &ciphertext, &wrong_secret);
        assert!(result.is_err());
    }

    #[test]
    fn test_display_name_unicode() {
        let secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let pubkey = X25519PublicKey::from(&secret);
        let xpubkey = XPubKey::from(pubkey);

        let plaintext = "notes 2026.txt";
        let (eph_pubkey, ciphertext) = encrypt_display_name(plaintext, &xpubkey).unwrap();
        let decrypted = decrypt_display_name(&eph_pubkey, &ciphertext, &secret).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
