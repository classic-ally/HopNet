// Shared setup types
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// Initial setup payload for creating a new HopNet node from scratch
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct InitialSetupPayload {
    pub username: String,
    pub node_name: String,
}

/// Response containing a server-generated passphrase
#[derive(Debug, Serialize)]
#[typeshare]
pub struct PassphraseResponse {
    pub passphrase: String,
}
