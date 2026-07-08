//! Host implementation of the comms peer directory: peer knowledge is the
//! nodes table (bincode-encoded pubkeys, matching `PubKey::to_sql()`) plus
//! the setup-mode bypass. Injected into `IrohComms` at bind time — the
//! transport's before-registration hook consults it to reject unknown peers
//! before any path registration (no IP disclosure via holepunching), and the
//! accept path uses it to attribute inbound connections to node ids.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hopnet_comms::{BoxFuture, PeerDirectory};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::types::PubKey;

pub struct HostPeerDirectory {
    db_pool: Pool<SqliteConnectionManager>,
    /// Whether this node has completed setup (genesis or JoinInfo received).
    /// Shared with the host — when false, all incoming connections are allowed.
    setup_complete: Arc<AtomicBool>,
}

impl HostPeerDirectory {
    pub fn new(db_pool: Pool<SqliteConnectionManager>, setup_complete: Arc<AtomicBool>) -> Self {
        Self {
            db_pool,
            setup_complete,
        }
    }
}

impl PeerDirectory for HostPeerDirectory {
    fn is_known(&self, pubkey: &[u8; 32]) -> BoxFuture<'_, bool> {
        // Setup mode: allow all incoming connections before this node has been
        // initialized (received JoinInfo or completed genesis setup). The window
        // is brief and the JoinInfo itself requires the user's private key.
        if !self.setup_complete.load(Ordering::Relaxed) {
            return Box::pin(async { true });
        }

        // Encode the remote's public key in the same bincode format
        // used by PubKey::to_sql() so the query matches the DB BLOB.
        let pubkey = PubKey(
            ed25519_dalek::VerifyingKey::from_bytes(pubkey)
                .expect("iroh EndpointId is valid Ed25519"),
        );
        let pubkey_encoded = bincode::serde::encode_to_vec(pubkey, bincode::config::standard())
            .expect("PubKey encoding cannot fail");

        // Blocking pool checkout + query — this hook runs inside iroh's accept
        // machinery on the net runtime, which must never block (see the
        // PeerDirectory contract / hopnet_comms::net_rt).
        let db_pool = self.db_pool.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || match db_pool.get() {
                Ok(conn) => conn
                    .query_row(
                        "SELECT 1 FROM nodes WHERE pubkey = ?",
                        [pubkey_encoded.as_slice()],
                        |_| Ok(()),
                    )
                    .is_ok(),
                Err(e) => {
                    tracing::error!("failed to get DB connection in peer validator: {}", e);
                    false
                }
            })
            .await
            .unwrap_or(false)
        })
    }

    fn node_id(&self, pubkey: &[u8; 32]) -> BoxFuture<'_, Option<i32>> {
        // The before-registration hook already rejected unknown peers before
        // the connection was established, so this is just for resolving the
        // node_id for logging/routing. Blocking pool checkout — runs on the
        // blocking pool, never on a net worker.
        let pubkey = ed25519_dalek::VerifyingKey::from_bytes(pubkey)
            .ok()
            .map(PubKey);
        let db_pool = self.db_pool.clone();
        Box::pin(async move {
            let pubkey = pubkey?;
            tokio::task::spawn_blocking(move || {
                let conn = db_pool.get().ok()?;
                let pubkey_encoded =
                    bincode::serde::encode_to_vec(pubkey, bincode::config::standard()).ok()?;
                conn.query_row(
                    "SELECT node_id FROM nodes WHERE pubkey = ?",
                    [pubkey_encoded.as_slice()],
                    |row| row.get(0),
                )
                .ok()
            })
            .await
            .ok()
            .flatten()
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::types::PubKey;

    /// Verify that the pubkey encoding used in the directory matches what PubKey::to_sql() stores.
    /// This is the core invariant: the hook must query with the same format the DB uses.
    #[test]
    fn peer_validator_pubkey_encoding_matches_db_format() {
        // Generate a random Ed25519 keypair (same flow as node setup)
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&rand::random());
        let verifying_key = signing_key.verifying_key();
        let pubkey = PubKey(verifying_key);

        // What the DB stores (via PubKey::to_sql → bincode encode)
        let db_encoded =
            bincode::serde::encode_to_vec(pubkey, bincode::config::standard()).unwrap();

        // What the directory produces from the raw transport pubkey bytes
        let hook_pubkey = PubKey(
            ed25519_dalek::VerifyingKey::from_bytes(&verifying_key.to_bytes())
                .expect("valid Ed25519"),
        );
        let hook_encoded =
            bincode::serde::encode_to_vec(hook_pubkey, bincode::config::standard()).unwrap();

        assert_eq!(
            db_encoded, hook_encoded,
            "peer directory encoding must match DB storage format"
        );
    }

    /// Verify the encoding differs from raw bytes (the old buggy behavior).
    #[test]
    fn bincode_encoded_pubkey_differs_from_raw_bytes() {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&rand::random());
        let pubkey = PubKey(signing_key.verifying_key());

        let raw_bytes = signing_key.verifying_key().to_bytes();
        let encoded = bincode::serde::encode_to_vec(pubkey, bincode::config::standard()).unwrap();

        assert_ne!(
            raw_bytes.as_slice(),
            encoded.as_slice(),
            "bincode-encoded PubKey should differ from raw 32-byte key (has length prefix)"
        );
        assert!(
            encoded.len() > 32,
            "bincode encoding should be longer than raw key"
        );
    }
}
