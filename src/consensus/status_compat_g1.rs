//! Generation-1 frozen inventory: the status scope's wire vocabulary
//! (RFC-025 §Scope Classes, §The Generation Contract).
//!
//! FROZEN ONCE RELEASED. This file may contain nothing but vocabulary
//! types, the GENERATION label, and their byte goldens — any change
//! after the release tag fails `scripts/check-compat-freeze.sh` unless
//! `COMPAT_HEAD` was bumped (a mint adds `status_compat_g2.rs`, never
//! edits this file). The normative byte contract is
//! `hopnet-comms/docs/wire.md`; the generation-0 adapter lives beside
//! the handler in `evidence.rs`, never here.

/// The generation this module's vocabulary belongs to. Pinned against
/// the served window by the cross-crate tie test in `net::scopes`.
pub const GENERATION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum StatusRequest {
    /// Carries the PROBER's decided height: a probe teaches both sides —
    /// the responder learns the prober's height here, the prober learns
    /// the responder's from the Pong. Without this, steady-state probe
    /// circularity (each side's probes keeping the other's view fresh)
    /// leaves exactly one probe direction per pair and the responder
    /// heightless.
    ///
    /// Also the hello of the (epoch, version) handshake (RFC-019 S6):
    /// both sides learn each other's identity and log a structured
    /// refusal on mismatch — turning the silent signature-domain failure
    /// (chain_id is mixed into every vote) into a diagnosable one. The
    /// responder still answers and records contact: reachability is a
    /// transport fact, orthogonal to epoch membership.
    Ping {
        decided_height: u64,
        epoch: u64,
        version_code: u32,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum StatusResponse {
    /// Current decided height (0 pre-genesis/pre-engine — reachability is
    /// a transport property; a zero height just fails catch-up gates),
    /// plus the responder's (epoch, version) — see Ping — and its served
    /// compat window: one round trip answers "can we talk, and if not,
    /// why not" (RFC-025 §Rejection & Diagnosability). The window fields
    /// sit LAST so a generation-0 decoder reads a valid prefix — defense
    /// in depth for released binaries, not the normative path (that is
    /// the generation-0 adapter).
    Pong {
        decided_height: u64,
        epoch: u64,
        version_code: u32,
        floor: u32,
        head: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: these bytes are the generation-1 wire contract — silent
    // drift severs status probing between releases (wire.md).
    // Should: encode the documented byte layout exactly — variant
    // varint, then field varints in declaration order.
    #[test]
    fn g1_ping_golden() {
        let bytes = bincode::serde::encode_to_vec(
            &StatusRequest::Ping {
                decided_height: 7,
                epoch: 2,
                version_code: 20260806,
            },
            bincode::config::standard(),
        )
        .unwrap();
        let expected: &[u8] = &[
            0x00, // variant Ping
            0x07, // decided_height
            0x02, // epoch
            0xFC, 0xC6, 0x27, 0x35, 0x01, // version_code 20260806 (u32 varint)
        ];
        assert_eq!(bytes, expected, "g1 Ping wire format drifted");
    }

    // Should: append the window fields after version_code — a
    // generation-0 decoder reads a valid prefix.
    #[test]
    fn g1_pong_golden() {
        let bytes = bincode::serde::encode_to_vec(
            &StatusResponse::Pong {
                decided_height: 7,
                epoch: 2,
                version_code: 20260806,
                floor: 0,
                head: 1,
            },
            bincode::config::standard(),
        )
        .unwrap();
        let expected: &[u8] = &[
            0x00, // variant Pong
            0x07, // decided_height
            0x02, // epoch
            0xFC, 0xC6, 0x27, 0x35, 0x01, // version_code 20260806
            0x00, // floor
            0x01, // head
        ];
        assert_eq!(bytes, expected, "g1 Pong wire format drifted");
    }
}
