//! Host-side networking glue over the `hopnet-comms` transport (RFC-017):
//! the peer directory (nodes-table knowledge), the scope handlers, and the
//! debug routes. The envelope — framing, request ids, dedup, retries,
//! runtimes — lives in `hopnet_comms`; payloads are opaque bytes encoded by
//! the module that owns each scope.

pub mod directory;
pub mod routes;
pub mod scopes;

use hopnet_comms::{CommsError, ProtocolError};

/// Encode a scope payload/response — bincode with the standard config, the
/// wire codec every scope owner uses (payloads are opaque to comms).
pub fn encode_payload<T: serde::Serialize>(msg: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(msg, bincode::config::standard())
        .expect("scope payload encoding cannot fail")
}

/// Decode a scope payload/response; failures map onto the comms error
/// taxonomy as malformed-response protocol errors.
pub fn decode_payload<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, CommsError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(msg, _)| msg)
        .map_err(|e| CommsError::Protocol(ProtocolError::MalformedResponse(e.to_string())))
}
