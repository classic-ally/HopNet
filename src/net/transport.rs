use iroh::endpoint::{AfterHandshakeOutcome, EndpointHooks};
use iroh::{Endpoint, PublicKey, SecretKey, endpoint::Connection};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::Instrument;

use super::protocol::{IrohRequest, IrohResponse};
use crate::types::PubKey;

/// ALPN protocol identifier for HopNet
pub const HOPNET_ALPN: &[u8] = b"hopnet/1.0";

/// Maximum message size (8MB) - prevents allocation attacks from malicious peers
const MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

/// Timeout for establishing a new connection (relay discovery + QUIC handshake).
/// Generous enough for relay/holepunch but prevents indefinite hangs.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

// ============================================================================
// Error Types
// ============================================================================

/// Top-level error type for iroh transport operations
#[derive(Debug)]
pub enum IrohError {
    /// Retryable transport failures (connection, stream, timeout)
    Transport(TransportError),
    /// Non-retryable protocol failures (auth, validation, peer errors)
    Protocol(ProtocolError),
}

/// Transport-layer errors - generally retryable
#[derive(Debug)]
pub enum TransportError {
    ConnectionFailed(String),
    StreamFailed(String),
    Timeout,
}

/// Protocol-layer errors - generally non-retryable
#[derive(Debug)]
pub enum ProtocolError {
    ValueMismatch {
        field: &'static str,
        expected: String,
        got: String,
    },
    PeerError(String),
    MalformedResponse(String),
    MessageTooLarge(usize),
    // Future: AuthRejected, InvalidSignature, etc.
}

impl IrohError {
    /// Whether this error should trigger a retry
    pub fn is_retryable(&self) -> bool {
        matches!(self, IrohError::Transport(_))
    }
}

impl std::fmt::Display for IrohError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IrohError::Transport(e) => write!(f, "transport error: {}", e),
            IrohError::Protocol(e) => write!(f, "protocol error: {}", e),
        }
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::ConnectionFailed(msg) => write!(f, "connection failed: {}", msg),
            TransportError::StreamFailed(msg) => write!(f, "stream failed: {}", msg),
            TransportError::Timeout => write!(f, "timeout"),
        }
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::ValueMismatch {
                field,
                expected,
                got,
            } => {
                write!(f, "{} mismatch: expected {}, got {}", field, expected, got)
            }
            ProtocolError::PeerError(msg) => write!(f, "peer error: {}", msg),
            ProtocolError::MalformedResponse(msg) => write!(f, "malformed response: {}", msg),
            ProtocolError::MessageTooLarge(size) => write!(f, "message too large: {} bytes", size),
        }
    }
}

impl std::error::Error for IrohError {}

// ============================================================================
// Wire Format Helpers
// ============================================================================

/// Encode a bincode message to bytes (without writing to stream)
pub fn encode_message<T: serde::Serialize>(msg: &T) -> Result<Vec<u8>, IrohError> {
    bincode::serde::encode_to_vec(msg, bincode::config::standard())
        .map_err(|e| IrohError::Protocol(ProtocolError::MalformedResponse(e.to_string())))
}

/// Send length-prefixed raw bytes on a stream
pub async fn send_raw(
    stream: &mut iroh::endpoint::SendStream,
    bytes: &[u8],
) -> Result<(), IrohError> {
    let len = bytes.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
    stream
        .write_all(bytes)
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
    Ok(())
}

/// Send a length-prefixed bincode message
pub async fn send_message<T: serde::Serialize>(
    stream: &mut iroh::endpoint::SendStream,
    msg: &T,
) -> Result<(), IrohError> {
    let bytes = encode_message(msg)?;
    send_raw(stream, &bytes).await
}

/// Send a request with request_id prefix: [8-byte id LE][4-byte len LE][bincode]
async fn send_request(
    stream: &mut iroh::endpoint::SendStream,
    request_id: u64,
    req: &IrohRequest,
) -> Result<(), IrohError> {
    stream
        .write_all(&request_id.to_le_bytes())
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
    send_message(stream, req).await
}

/// Receive a length-prefixed bincode message
pub async fn recv_message<T: serde::de::DeserializeOwned>(
    stream: &mut iroh::endpoint::RecvStream,
) -> Result<T, IrohError> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(IrohError::Protocol(ProtocolError::MessageTooLarge(len)));
    }

    let mut bytes = vec![0u8; len];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;

    let (msg, _) = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .map_err(|e| IrohError::Protocol(ProtocolError::MalformedResponse(e.to_string())))?;
    Ok(msg)
}

// ============================================================================
// Peer Validation Hook
// ============================================================================

/// Hook that rejects connections from unknown peers before path registration,
/// preventing IP address disclosure via holepunching to unauthorized nodes.
struct PeerValidator {
    db_pool: Pool<SqliteConnectionManager>,
    setup_complete: Arc<AtomicBool>,
}

impl std::fmt::Debug for PeerValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerValidator").finish()
    }
}

impl EndpointHooks for PeerValidator {
    async fn before_registration<'a>(
        &'a self,
        remote_id: &'a iroh::EndpointId,
        _alpn: &'a [u8],
        side: iroh::endpoint::Side,
    ) -> AfterHandshakeOutcome {
        // Only validate incoming (server-side) connections. Outgoing connections
        // are initiated intentionally by our application to known peers, and TLS
        // certificates prevent impersonation.
        if side == iroh::endpoint::Side::Client {
            return AfterHandshakeOutcome::Accept;
        }

        // Setup mode: allow all incoming connections before this node has been
        // initialized (received JoinInfo or completed genesis setup). The window
        // is brief and the JoinInfo itself requires the user's private key.
        if !self.setup_complete.load(Ordering::Relaxed) {
            return AfterHandshakeOutcome::Accept;
        }

        // Encode the remote's public key in the same bincode format
        // used by PubKey::to_sql() so the query matches the DB BLOB.
        let pubkey = PubKey(
            ed25519_dalek::VerifyingKey::from_bytes(remote_id.as_bytes())
                .expect("iroh EndpointId is valid Ed25519"),
        );
        let pubkey_encoded = bincode::serde::encode_to_vec(pubkey, bincode::config::standard())
            .expect("PubKey encoding cannot fail");

        let is_known = match self.db_pool.get() {
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
        };

        if is_known {
            AfterHandshakeOutcome::Accept
        } else {
            tracing::warn!("rejected iroh connection from unknown node: {}", remote_id);
            AfterHandshakeOutcome::Reject {
                error_code: 1u32.into(),
                reason: b"unknown node".to_vec(),
            }
        }
    }
}

// ============================================================================
// IrohTransport
// ============================================================================

/// Transport layer for iroh-based inter-node communication
/// Clone is cheap - both fields are reference-counted internally
#[derive(Clone)]
pub struct IrohTransport {
    endpoint: Endpoint,
    /// Connection cache keyed by node_id
    connections: Arc<RwLock<HashMap<i32, Connection>>>,
    /// Whether this node has completed setup (genesis or JoinInfo received).
    /// Shared with PeerValidator — when false, all incoming connections are allowed.
    setup_complete: Arc<AtomicBool>,
    /// Self-hosted relay (HOPNET_RELAY_URL). When set, the endpoint uses ONLY
    /// this relay (no n0 relays, no public discovery) and every dial pins the
    /// peer's address to it — removes all external network dependencies for
    /// orchestrator meshes and private deployments.
    custom_relay: Option<iroh::RelayUrl>,
}

impl IrohTransport {
    /// Create a new IrohTransport with the given secret key and database pool.
    /// `is_setup_complete` should be true if this node already has persisted state (restart),
    /// false if this is a fresh node waiting for genesis setup or JoinInfo.
    ///
    /// `HOPNET_RELAY_URL` switches the endpoint from the n0 preset (public
    /// relays + pkarr discovery) to a single self-hosted relay with no
    /// address-lookup services.
    pub async fn new(
        secret_key: SecretKey,
        db_pool: Pool<SqliteConnectionManager>,
        is_setup_complete: bool,
    ) -> Result<Self, IrohError> {
        let setup_complete = Arc::new(AtomicBool::new(is_setup_complete));

        let custom_relay: Option<iroh::RelayUrl> = match std::env::var("HOPNET_RELAY_URL") {
            Ok(url) => Some(url.parse().map_err(|e| {
                IrohError::Transport(TransportError::ConnectionFailed(format!(
                    "invalid HOPNET_RELAY_URL {url:?}: {e}"
                )))
            })?),
            Err(_) => None,
        };

        let builder = match &custom_relay {
            Some(url) => {
                tracing::info!("using self-hosted iroh relay {url} (public discovery disabled)");
                Endpoint::builder(iroh::endpoint::presets::Minimal)
                    .relay_mode(iroh::RelayMode::Custom(iroh::RelayMap::from(url.clone())))
            }
            None => Endpoint::builder(iroh::endpoint::presets::N0),
        };

        let endpoint = builder
            .secret_key(secret_key)
            .alpns(vec![HOPNET_ALPN.to_vec()])
            .hooks(PeerValidator {
                db_pool,
                setup_complete: setup_complete.clone(),
            })
            .bind()
            .await
            .map_err(|e| IrohError::Transport(TransportError::ConnectionFailed(e.to_string())))?;

        Ok(Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            setup_complete,
            custom_relay,
        })
    }

    /// Mark this node's setup as complete. After this call, the PeerValidator
    /// switches to strict mode and rejects unknown peers.
    pub fn mark_setup_complete(&self) {
        self.setup_complete.store(true, Ordering::Relaxed);
    }

    /// Get the node ID (public key) for this endpoint
    pub fn node_id(&self) -> PublicKey {
        self.endpoint.id()
    }

    /// Get a reference to the endpoint for accept loop
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Get or establish a connection to a peer.
    /// Uses cached connection if available. Connection establishment is bounded
    /// by CONNECTION_TIMEOUT to prevent indefinite hangs when a peer is unreachable.
    pub async fn get_connection(
        &self,
        node_id: i32,
        peer_node_id: PublicKey,
    ) -> Result<Connection, IrohError> {
        // Check cache first
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(&node_id)
                && conn.close_reason().is_none()
            {
                return Ok(conn.clone());
            }
        }

        // Establish new connection with timeout. With a self-hosted relay
        // there is no discovery — pin the peer's address to our relay.
        let dial_addr = {
            let mut addr = iroh::EndpointAddr::new(peer_node_id.into());
            if let Some(url) = &self.custom_relay {
                addr = addr.with_relay_url(url.clone());
            }
            addr
        };
        let conn = tokio::time::timeout(
            CONNECTION_TIMEOUT,
            self.endpoint.connect(dial_addr, HOPNET_ALPN),
        )
        .await
        .map_err(|_| {
            IrohError::Transport(TransportError::ConnectionFailed(format!(
                "connection to node {} timed out after {:?}",
                node_id, CONNECTION_TIMEOUT
            )))
        })?
        .map_err(|e| IrohError::Transport(TransportError::ConnectionFailed(e.to_string())))?;

        // Cache it
        {
            let mut connections = self.connections.write().await;
            connections.insert(node_id, conn.clone());
        }

        Ok(conn)
    }

    /// Send a ping to a peer and wait for pong
    /// Returns round-trip time in nanoseconds
    pub async fn ping(&self, node_id: i32, peer_node_id: PublicKey) -> Result<u64, IrohError> {
        let start = Instant::now();
        let nonce = rand::random::<u64>();

        let response = self
            .request(
                node_id,
                peer_node_id,
                &IrohRequest::Ping { nonce },
                Duration::from_secs(5),
            )
            .await?;

        match response {
            IrohResponse::Pong { nonce: got } if got == nonce => {
                Ok(start.elapsed().as_nanos() as u64)
            }
            IrohResponse::Pong { nonce: got } => {
                Err(IrohError::Protocol(ProtocolError::ValueMismatch {
                    field: "nonce",
                    expected: nonce.to_string(),
                    got: got.to_string(),
                }))
            }
            IrohResponse::Error { message } => {
                Err(IrohError::Protocol(ProtocolError::PeerError(message)))
            }
            other => Err(IrohError::Protocol(ProtocolError::MalformedResponse(
                format!("unexpected response to Ping: {:?}", other),
            ))),
        }
    }

    /// Send a request and receive a response on a new bidirectional stream.
    /// Handles the full connection lifecycle: uses cached connection first, and if the
    /// stream fails or times out (zombie connection), evicts the cache and retries once
    /// with a fresh connection before failing the caller.
    ///
    /// Each logical request gets a random `request_id` that is reused across retries,
    /// allowing the receiver to deduplicate retried requests.
    ///
    /// Connection establishment is outside the timeout budget (bounded separately by
    /// CONNECTION_TIMEOUT). The `timeout` parameter covers only stream I/O — opening
    /// the stream, sending the request, and receiving the response.
    pub async fn request(
        &self,
        node_id: i32,
        peer_node_id: PublicKey,
        req: &IrohRequest,
        timeout: Duration,
    ) -> Result<IrohResponse, IrohError> {
        let request_id: u64 = rand::random();
        let span =
            tracing::debug_span!("rpc_req", id = %format!("{:016x}", request_id), to = node_id);
        async {
            let conn = self.get_connection(node_id, peer_node_id).await?;

            match Self::try_request(&conn, request_id, req, timeout).await {
                Ok(response) => Ok(response),
                Err(e) if e.is_retryable() => {
                    // Transport error (timeout or stream failure) — connection may be zombie.
                    // Evict and retry once with a fresh connection, reusing the same request_id
                    // so the receiver can deduplicate.
                    self.remove_connection(node_id).await;
                    let conn = self.get_connection(node_id, peer_node_id).await?;
                    Self::try_request(&conn, request_id, req, timeout).await
                }
                Err(e) => Err(e),
            }
        }
        .instrument(span)
        .await
    }

    /// Attempt a single request on an existing connection.
    async fn try_request(
        conn: &Connection,
        request_id: u64,
        req: &IrohRequest,
        timeout: Duration,
    ) -> Result<IrohResponse, IrohError> {
        tokio::time::timeout(timeout, async {
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;

            send_request(&mut send, request_id, req).await?;
            send.finish()
                .map_err(|e| IrohError::Transport(TransportError::StreamFailed(e.to_string())))?;

            recv_message(&mut recv).await
        })
        .await
        .map_err(|_| IrohError::Transport(TransportError::Timeout))?
    }

    /// Remove a connection from the cache (e.g., on error or timeout)
    pub async fn remove_connection(&self, node_id: i32) {
        let mut connections = self.connections.write().await;
        connections.remove(&node_id);
    }

    /// Establish and cache a connection to a peer at a KNOWN address,
    /// bypassing discovery. Used by in-process tests over loopback, where
    /// endpoints know each other's bound sockets directly.
    pub async fn connect_to_addr(
        &self,
        node_id: i32,
        addr: iroh::EndpointAddr,
    ) -> Result<(), IrohError> {
        let conn = tokio::time::timeout(CONNECTION_TIMEOUT, self.endpoint.connect(addr, HOPNET_ALPN))
            .await
            .map_err(|_| IrohError::Transport(TransportError::Timeout))?
            .map_err(|e| IrohError::Transport(TransportError::ConnectionFailed(e.to_string())))?;
        self.connections.write().await.insert(node_id, conn);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PubKey;

    /// Verify that the pubkey encoding used in PeerValidator matches what PubKey::to_sql() stores.
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

        // What the PeerValidator hook produces from the iroh EndpointId
        let iroh_secret = iroh::SecretKey::from_bytes(&signing_key.to_bytes());
        let iroh_public = iroh_secret.public();
        let hook_pubkey = PubKey(
            ed25519_dalek::VerifyingKey::from_bytes(iroh_public.as_bytes()).expect("valid Ed25519"),
        );
        let hook_encoded =
            bincode::serde::encode_to_vec(hook_pubkey, bincode::config::standard()).unwrap();

        assert_eq!(
            db_encoded, hook_encoded,
            "PeerValidator encoding must match DB storage format"
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
