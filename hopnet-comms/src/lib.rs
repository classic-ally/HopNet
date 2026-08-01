//! Inter-node communication vocabulary (RFC-017).
//!
//! Comms owns the ENVELOPE: peer identity, scope routing, request ids,
//! framing, timeouts. Payloads are opaque bytes — encode/decode belongs to
//! the module that owns the scope (storage decodes its FragmentRequest, the
//! consensus shell decodes its gossip, …). One handler per scope namespace;
//! a duplicate registration is a boot-time panic, same philosophy as the
//! host's dispatch-table tripwire.
//!
//! Default features carry ZERO dependencies — this face is safe for any
//! crate in the workspace. The real transport ([`IrohComms`]) lives behind
//! the `iroh` feature and is linked by the host only.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

#[cfg(feature = "iroh")]
mod iroh_impl;
/// The raw iroh crate, for test harnesses that must impersonate a foreign
/// endpoint (e.g. the orchestrator's reject-unknown probe). Production code
/// goes through [`IrohComms`]; containment means no other workspace manifest
/// names iroh.
#[cfg(feature = "iroh")]
pub use iroh;
#[cfg(feature = "iroh")]
pub use iroh_impl::{
    net_rt, Call, CallOptions, EndpointAddr, IrohComms, ScopeRegistry, HOPNET_ALPN,
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A mesh peer: consensus node id + raw Ed25519 public key bytes. Plain
/// data — how the key maps to transport identity is the implementation's
/// business; how it maps to the nodes table is the host's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerRef {
    pub node_id: i32,
    pub pubkey: [u8; 32],
}

// ============================================================================
// Error taxonomy (ported verbatim from the host's net/transport.rs so every
// call-site mapping is mechanical)
// ============================================================================

/// Top-level error type for comms operations.
#[derive(Debug)]
pub enum CommsError {
    /// Retryable transport failures (connection, stream, timeout).
    Transport(TransportError),
    /// Non-retryable protocol failures (validation, peer errors).
    Protocol(ProtocolError),
}

/// Transport-layer errors - generally retryable.
#[derive(Debug)]
pub enum TransportError {
    ConnectionFailed(String),
    StreamFailed(String),
    Timeout,
}

/// Protocol-layer errors - generally non-retryable.
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
}

impl CommsError {
    /// Whether this error should trigger a retry.
    pub fn is_retryable(&self) -> bool {
        matches!(self, CommsError::Transport(_))
    }
}

impl std::fmt::Display for CommsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommsError::Transport(e) => write!(f, "transport error: {}", e),
            CommsError::Protocol(e) => write!(f, "protocol error: {}", e),
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

impl std::error::Error for CommsError {}

// ============================================================================
// Client seams
// ============================================================================

/// Single request → single response over the mesh. Envelope-only: the
/// payload is opaque bytes; the caller encodes, the scope owner decodes.
/// `timeout` covers stream I/O only — connection establishment is bounded
/// separately by the implementation's connect budget.
pub trait Rpc: Send + Sync {
    fn rpc(
        &self,
        peer: &PeerRef,
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<u8>, CommsError>> + Send;
}

/// Fire-and-forget fan-out: one send per peer (spawned; the call returns
/// immediately), each expecting a single ack frame within `timeout`;
/// failures are logged at debug. Peer enumeration is the CALLER's job —
/// the nodes table stays host-side.
pub trait Broadcast: Send + Sync {
    fn broadcast(
        &self,
        peers: &[PeerRef],
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
    );
}

/// Host-injected peer knowledge (the nodes table and its pubkey encoding
/// stay host-side). Also carries the setup-mode bypass — comms never
/// learns what "setup" means, only what the directory answers.
///
/// CONTRACT: implementations are called from the transport's network
/// runtime and must never block it — hop blocking DB work through
/// `spawn_blocking` internally.
pub trait PeerDirectory: Send + Sync {
    /// Gate for INCOMING connections (the before-registration hook):
    /// unknown peers are rejected before any path registration happens.
    fn is_known(&self, pubkey: &[u8; 32]) -> BoxFuture<'_, bool>;

    /// Inbound attribution: pubkey → consensus node id. Handlers observe
    /// -1 when unknown (matches the historical host behavior).
    fn node_id(&self, pubkey: &[u8; 32]) -> BoxFuture<'_, Option<i32>>;
}

// ============================================================================
// Scope handlers (server side)
// ============================================================================

/// Single-response scope handler. The transport deduplicates retried
/// requests by request id (response-byte cache) for rpc scopes.
pub trait RpcHandler: Send + Sync {
    fn handle(&self, peer: PeerRef, payload: Vec<u8>) -> BoxFuture<'_, Vec<u8>>;
}

/// Multi-frame scope handler (two-phase protocols like transaction
/// forwarding: ack frame, then result frame). NO transport-level dedup —
/// the protocol owns its idempotency (e.g. the consensus nonce table).
pub trait StreamHandler: Send + Sync {
    fn handle(&self, peer: PeerRef, payload: Vec<u8>, out: Box<dyn FrameSink>)
        -> BoxFuture<'_, ()>;
}

/// Server-side response writer for [`StreamHandler`]. The handler sends
/// zero or more frames, then finishes the stream.
pub trait FrameSink: Send {
    fn send(&mut self, frame: Vec<u8>) -> BoxFuture<'_, Result<(), CommsError>>;
    fn finish(self: Box<Self>) -> Result<(), CommsError>;
}

/// Scope namespace reserved for the transport's own liveness ping.
pub const PING_SCOPE: &str = "ping";

// ScopeRegistry (the scope → handler map) lives with the transport
// implementation behind the `iroh` feature: registration without a
// transport to dispatch it is meaningless, and only the host — which
// links the transport — registers scopes. The handler TRAITS above stay
// in the vocabulary so scope owners can define handlers anywhere.
