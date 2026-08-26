//! Generation-0 frozen inventory: the status vocabulary every
//! pre-enforcement binary speaks (RFC-025 §Evolution — `hopnet/1.0`
//! served as the initial previous generation across the cutover).
//!
//! FROZEN. Byte-for-byte the shapes shipped before enforcement; any
//! change after the release tag fails `scripts/check-compat-freeze.sh`.
//! Retirement (the first real mint) deletes this file whole under a
//! `RETIRES: compat_g0` commit trailer. The generation-0 adapter lives
//! beside the handler in `evidence.rs`, never here.

/// The generation this module's vocabulary belongs to. Pinned against
/// the served floor by the cross-crate tie test in `net::scopes`.
pub const GENERATION: u32 = 0;

/// The pre-enforcement Ping — byte-identical to the generation-1 Ping
/// (the equality golden below is the adapter's license to pass request
/// bytes through untouched).
#[derive(serde::Serialize, serde::Deserialize)]
pub enum StatusRequest {
    Ping {
        decided_height: u64,
        epoch: u64,
        version_code: u32,
    },
}

/// The pre-enforcement Pong: no window fields.
#[derive(serde::Serialize, serde::Deserialize)]
pub enum StatusResponse {
    Pong {
        decided_height: u64,
        epoch: u64,
        version_code: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: these bytes are what every released pre-enforcement binary
    // decodes — the generation-0 contract survives in this file, not in
    // git archaeology.
    // Should: encode the pre-enforcement Pong exactly.
    #[test]
    fn g0_pong_golden() {
        let bytes = bincode::serde::encode_to_vec(
            &StatusResponse::Pong {
                decided_height: 7,
                epoch: 2,
                version_code: 20260806,
            },
            bincode::config::standard(),
        )
        .unwrap();
        let expected: &[u8] = &[
            0x00, // variant Pong
            0x07, // decided_height
            0x02, // epoch
            0xFC, 0xC6, 0x27, 0x35, 0x01, // version_code 20260806
        ];
        assert_eq!(bytes, expected, "g0 Pong wire format drifted");
    }

    // Impact: the adapter passes request bytes through untouched — this
    // equality is its license.
    // Should: encode the generation-0 and head Pings identically.
    #[test]
    fn g0_ping_bytes_equal_head_ping_bytes() {
        let g0 = bincode::serde::encode_to_vec(
            &StatusRequest::Ping {
                decided_height: 7,
                epoch: 2,
                version_code: 20260806,
            },
            bincode::config::standard(),
        )
        .unwrap();
        let head = bincode::serde::encode_to_vec(
            &super::super::status_compat_g1::StatusRequest::Ping {
                decided_height: 7,
                epoch: 2,
                version_code: 20260806,
            },
            bincode::config::standard(),
        )
        .unwrap();
        assert_eq!(g0, head);
    }
}
