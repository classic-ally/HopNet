//! The iroh transport implementation (feature `iroh`). Ported from the
//! host's `src/net/{transport,handler}.rs` at RFC-017 Stage 1a — every
//! constant, runtime-placement decision, and retry/dedup semantic is kept
//! verbatim; only the envelope changed (scope routing instead of a
//! monolithic request enum).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use iroh::endpoint::{AfterHandshakeOutcome, Connection, EndpointHooks};
use iroh::{Endpoint, PublicKey, SecretKey};
use tokio::sync::RwLock;
use tracing::Instrument;

use crate::{
    BoxFuture, CommsError, FrameSink, PeerDirectory, PeerRef, ProtocolError, RpcHandler,
    StreamHandler, TransportError, PING_SCOPE,
};

pub(crate) enum ScopeEntry {
    Rpc(Arc<dyn RpcHandler>),
    Streamed(Arc<dyn StreamHandler>),
}

/// One handler per scope namespace. Duplicate registration — or claiming
/// the reserved "ping" scope — panics at registration time (boot tripwire:
/// a scope collision is a programming error, not a runtime condition).
#[derive(Default)]
pub struct ScopeRegistry {
    pub(crate) entries: HashMap<&'static str, ScopeEntry>,
}

impl ScopeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a single-response handler for `scope`.
    pub fn rpc(&mut self, scope: &'static str, handler: Arc<dyn RpcHandler>) {
        self.insert(scope, ScopeEntry::Rpc(handler));
    }

    /// Register a multi-frame handler for `scope`.
    pub fn streamed(&mut self, scope: &'static str, handler: Arc<dyn StreamHandler>) {
        self.insert(scope, ScopeEntry::Streamed(handler));
    }

    fn insert(&mut self, scope: &'static str, entry: ScopeEntry) {
        assert!(
            scope != PING_SCOPE,
            "scope \"{PING_SCOPE}\" is reserved by the transport"
        );
        assert!(
            scope.len() <= u8::MAX as usize && !scope.is_empty(),
            "scope name must be 1..=255 bytes"
        );
        assert!(
            self.entries.insert(scope, entry).is_none(),
            "duplicate comms scope registration: {scope:?}"
        );
    }
}

pub use iroh::EndpointAddr;

/// ALPN protocol identifier for HopNet
pub const HOPNET_ALPN: &[u8] = b"hopnet/1.0";

/// Maximum frame size (8MB) - prevents allocation attacks from malicious peers
const MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

/// Timeout for establishing a new connection (relay discovery + QUIC handshake).
/// Generous enough for relay/holepunch but prevents indefinite hangs.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Receiver-side dedup entries live this long after first sight.
const DEDUP_TTL: Duration = Duration::from_secs(300);

/// Dedicated runtime for iroh networking: the endpoint's internal actors
/// (magicsock, relay client, keepalives), every outbound dial's connection
/// driver, and the accept loop live here.
///
/// WHY: mesh liveness must not depend on the API layer behaving. Under burst
/// HTTP load the main runtime's workers park in blocking DB checkouts; when
/// iroh's actors shared that runtime, missed keepalives dropped the relay
/// connection and partitioned the mesh — freezing consensus (a partitioned
/// Tendermint node cannot advance rounds alone).
///
/// DISCIPLINE (same contract class as the consensus shell thread): tasks on
/// this runtime must never block — no r2d2 checkouts, no rusqlite, no
/// block_in_place, no synchronous file I/O. Scope handlers run inline on
/// stream tasks here and must hop to their own runtime for anything
/// blocking (see the spawn-policy note on [`IrohComms::start`]).
///
/// NOTE (verified against the iroh fork + vendored noq): binding here places
/// the endpoint actors, but outbound dials spawn their per-connection driver
/// on the CALLER's runtime — so dials are also routed through here
/// (get_connection / connect_to_addr).
pub fn net_rt() -> &'static tokio::runtime::Runtime {
    static NET_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    NET_RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(3)
            .thread_name("iroh-net")
            .enable_all()
            .build()
            .expect("failed to build iroh net runtime")
    })
}

// ============================================================================
// Wire format
// ============================================================================
// request stream :  [8B request_id LE][1B scope_len][scope utf8][4B payload_len LE][payload]
// response stream:  repeated frames of [4B len LE][bytes]   (rpc = exactly one frame)

/// The request-stream header bytes, extracted pure so the envelope
/// golden can pin them without a live endpoint (the byte layout is
/// normative — hopnet-comms/docs/wire.md).
fn encode_envelope_header(request_id: u64, scope: &str, payload_len: u32) -> Vec<u8> {
    let mut header = Vec::with_capacity(8 + 1 + scope.len() + 4);
    header.extend_from_slice(&request_id.to_le_bytes());
    header.push(scope.len() as u8);
    header.extend_from_slice(scope.as_bytes());
    header.extend_from_slice(&payload_len.to_le_bytes());
    header
}

async fn write_envelope(
    send: &mut iroh::endpoint::SendStream,
    request_id: u64,
    scope: &str,
    payload: &[u8],
) -> Result<(), CommsError> {
    let header = encode_envelope_header(request_id, scope, payload.len() as u32);
    send.write_all(&header)
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    send.write_all(payload)
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    Ok(())
}

struct EnvelopeHeader {
    request_id: u64,
    scope: String,
    payload: Vec<u8>,
}

async fn read_envelope(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<EnvelopeHeader, CommsError> {
    let mut id_buf = [0u8; 8];
    recv.read_exact(&mut id_buf)
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    let request_id = u64::from_le_bytes(id_buf);

    let mut len_buf = [0u8; 1];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    let mut scope_buf = vec![0u8; len_buf[0] as usize];
    recv.read_exact(&mut scope_buf)
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    let scope = String::from_utf8(scope_buf)
        .map_err(|e| CommsError::Protocol(ProtocolError::MalformedResponse(e.to_string())))?;

    let payload = read_frame_body(recv).await?;
    Ok(EnvelopeHeader {
        request_id,
        scope,
        payload,
    })
}

async fn read_frame_body(recv: &mut iroh::endpoint::RecvStream) -> Result<Vec<u8>, CommsError> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(CommsError::Protocol(ProtocolError::MessageTooLarge(len)));
    }
    let mut bytes = vec![0u8; len];
    recv.read_exact(&mut bytes)
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    Ok(bytes)
}

async fn write_frame(
    send: &mut iroh::endpoint::SendStream,
    frame: &[u8],
) -> Result<(), CommsError> {
    send.write_all(&(frame.len() as u32).to_le_bytes())
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    send.write_all(frame)
        .await
        .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
    Ok(())
}

// ============================================================================
// Peer validation hook
// ============================================================================

/// Rejects connections from unknown peers before path registration,
/// preventing IP address disclosure via holepunching to unauthorized nodes.
/// Peer knowledge (and the setup-mode bypass) lives in the host-injected
/// [`PeerDirectory`].
struct HookAdapter {
    directory: Arc<dyn PeerDirectory>,
}

impl std::fmt::Debug for HookAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookAdapter").finish()
    }
}

impl EndpointHooks for HookAdapter {
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

        if self.directory.is_known(remote_id.as_bytes()).await {
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
// IrohComms
// ============================================================================

type DedupMap = std::sync::Mutex<HashMap<u64, Arc<tokio::sync::OnceCell<Vec<u8>>>>>;

/// Options for [`IrohComms::open_call`].
#[derive(Default)]
pub struct CallOptions {
    /// Override the default connect budget (CONNECTION_TIMEOUT 10s). The
    /// transaction-forward client uses a tight 2s budget so a dead proposer
    /// fails fast.
    pub connect_timeout: Option<Duration>,
}

/// A multi-frame call in progress (client side). Each `recv` reads one
/// response frame; the server side decides how many frames the protocol has.
pub struct Call {
    recv: iroh::endpoint::RecvStream,
}

impl Call {
    pub async fn recv(&mut self, timeout: Duration) -> Result<Vec<u8>, CommsError> {
        tokio::time::timeout(timeout, read_frame_body(&mut self.recv))
            .await
            .map_err(|_| CommsError::Transport(TransportError::Timeout))?
    }
}

/// The iroh-backed comms implementation. Clone is cheap — every field is
/// reference-counted internally.
#[derive(Clone)]
pub struct IrohComms {
    endpoint: Endpoint,
    /// Connection cache keyed by node_id.
    connections: Arc<RwLock<HashMap<i32, Connection>>>,
    /// Receiver-side request dedup: request_id → response-byte cache.
    dedup: Arc<DedupMap>,
    directory: Arc<dyn PeerDirectory>,
    /// Self-hosted relay (host passes HOPNET_RELAY_URL). When set, the
    /// endpoint uses ONLY this relay (no n0 relays, no public discovery) and
    /// every dial pins the peer's address to it.
    custom_relay: Option<iroh::RelayUrl>,
    scopes: Arc<OnceLock<ScopeRegistry>>,
    started: Arc<AtomicBool>,
}

impl IrohComms {
    /// Bind the endpoint. `secret` is the node's raw Ed25519 secret key;
    /// `relay_url` switches from the n0 preset (public relays + pkarr
    /// discovery) to a single self-hosted relay with no address-lookup
    /// services (reading the env var is host policy, not comms').
    pub async fn bind(
        secret: [u8; 32],
        directory: Arc<dyn PeerDirectory>,
        relay_url: Option<String>,
    ) -> Result<Self, CommsError> {
        let custom_relay: Option<iroh::RelayUrl> = match relay_url {
            Some(url) => Some(url.parse().map_err(|e| {
                CommsError::Transport(TransportError::ConnectionFailed(format!(
                    "invalid relay url {url:?}: {e}"
                )))
            })?),
            None => None,
        };

        // Bind ON the net runtime: iroh spawns its actor tasks (magicsock,
        // relay client, endpoint driver) on the ambient runtime at bind time —
        // this is what pins the whole relay/keepalive machinery to net_rt.
        let bind_relay = custom_relay.clone();
        let hook_directory = directory.clone();
        let endpoint = net_rt()
            .spawn(async move {
                let builder = match &bind_relay {
                    Some(url) => {
                        tracing::info!(
                            "using self-hosted iroh relay {url} (public discovery disabled)"
                        );
                        Endpoint::builder(iroh::endpoint::presets::Minimal)
                            .relay_mode(iroh::RelayMode::Custom(iroh::RelayMap::from(url.clone())))
                    }
                    None => Endpoint::builder(iroh::endpoint::presets::N0),
                };
                builder
                    .secret_key(SecretKey::from_bytes(&secret))
                    .alpns(vec![HOPNET_ALPN.to_vec()])
                    .hooks(HookAdapter {
                        directory: hook_directory,
                    })
                    .bind()
                    .await
            })
            .await
            .map_err(|e| CommsError::Transport(TransportError::ConnectionFailed(e.to_string())))?
            .map_err(|e| CommsError::Transport(TransportError::ConnectionFailed(e.to_string())))?;

        Ok(Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            dedup: Arc::new(std::sync::Mutex::new(HashMap::new())),
            directory,
            custom_relay,
            scopes: Arc::new(OnceLock::new()),
            started: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Install the scope map and spawn the accept loop on the net runtime.
    ///
    /// SPAWN POLICY: comms spawns one task per connection and per stream on
    /// the net runtime, decodes the envelope, resolves the peer, and invokes
    /// the scope handler INLINE on the stream task. Handlers that need the
    /// database (or any blocking work) hop to their own runtime via a
    /// captured `Handle` — comms never sees another runtime.
    pub fn start(&self, scopes: ScopeRegistry) {
        assert!(
            self.scopes.set(scopes).is_ok() && !self.started.swap(true, Ordering::SeqCst),
            "IrohComms::start called twice"
        );
        let comms = self.clone();
        net_rt().spawn(async move {
            loop {
                match comms.endpoint.accept().await {
                    Some(incoming) => {
                        let comms = comms.clone();
                        tokio::spawn(async move {
                            if let Err(e) = comms.handle_connection(incoming).await {
                                tracing::warn!("iroh connection error: {}", e);
                            }
                        });
                    }
                    None => {
                        tracing::info!("iroh endpoint closed, stopping accept loop");
                        break;
                    }
                }
            }
        });
    }

    /// This endpoint's public key bytes.
    pub fn local_pubkey(&self) -> [u8; 32] {
        *self.endpoint.id().as_bytes()
    }

    /// The underlying endpoint — in-crate and host integration tests only.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Liveness ping (comms-internal "ping" scope): random nonce echo.
    /// Returns round-trip time in nanoseconds.
    pub async fn ping(&self, peer: &PeerRef) -> Result<u64, CommsError> {
        let start = Instant::now();
        let nonce = rand::random::<u64>();
        let reply = self
            .rpc_inner(
                peer,
                PING_SCOPE,
                nonce.to_le_bytes().to_vec(),
                Duration::from_secs(5),
            )
            .await?;
        if reply == nonce.to_le_bytes() {
            Ok(start.elapsed().as_nanos() as u64)
        } else {
            Err(CommsError::Protocol(ProtocolError::ValueMismatch {
                field: "nonce",
                expected: nonce.to_string(),
                got: format!("{reply:?}"),
            }))
        }
    }

    /// Open a multi-frame call (two-phase protocols). NO auto-retry, NO
    /// dedup registration — the protocol owns idempotency and NoAck
    /// handling (the transaction-forward client evicts + retries itself).
    pub async fn open_call(
        &self,
        peer: &PeerRef,
        scope: &'static str,
        payload: Vec<u8>,
        opts: CallOptions,
    ) -> Result<Call, CommsError> {
        let connect_budget = opts.connect_timeout.unwrap_or(CONNECTION_TIMEOUT);
        let conn = self
            .get_connection_with_budget(peer, connect_budget)
            .await?;
        let (mut send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
        let request_id: u64 = rand::random();
        write_envelope(&mut send, request_id, scope, &payload).await?;
        send.finish()
            .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
        Ok(Call { recv })
    }

    /// Remove a connection from the cache (e.g. on error, timeout, or the
    /// forward client's NoAck eviction).
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
        addr: EndpointAddr,
    ) -> Result<(), CommsError> {
        // Same net-runtime dial routing as get_connection (driver placement).
        let endpoint = self.endpoint.clone();
        let mut dial = net_rt().spawn(async move { endpoint.connect(addr, HOPNET_ALPN).await });
        let conn = match tokio::time::timeout(CONNECTION_TIMEOUT, &mut dial).await {
            Err(_) => {
                dial.abort();
                return Err(CommsError::Transport(TransportError::Timeout));
            }
            Ok(Err(join_err)) => {
                return Err(CommsError::Transport(TransportError::ConnectionFailed(
                    join_err.to_string(),
                )));
            }
            Ok(Ok(Err(e))) => {
                return Err(CommsError::Transport(TransportError::ConnectionFailed(
                    e.to_string(),
                )));
            }
            Ok(Ok(Ok(conn))) => conn,
        };
        self.connections.write().await.insert(node_id, conn);
        Ok(())
    }

    /// Get or establish a connection to a peer. Uses cached connection if
    /// available; establishment is bounded by `connect_budget`.
    async fn get_connection_with_budget(
        &self,
        peer: &PeerRef,
        connect_budget: Duration,
    ) -> Result<Connection, CommsError> {
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(&peer.node_id) {
                if conn.close_reason().is_none() {
                    return Ok(conn.clone());
                }
            }
        }

        let peer_key = PublicKey::from_bytes(&peer.pubkey).map_err(|e| {
            CommsError::Transport(TransportError::ConnectionFailed(format!(
                "invalid peer pubkey for node {}: {e}",
                peer.node_id
            )))
        })?;
        // With a self-hosted relay there is no discovery — pin the peer's
        // address to our relay.
        let dial_addr = {
            let mut addr = EndpointAddr::new(peer_key);
            if let Some(url) = &self.custom_relay {
                addr = addr.with_relay_url(url.clone());
            }
            addr
        };
        // Dial ON the net runtime: the per-connection driver (ACKs, keepalives,
        // retransmits) is spawned on the runtime that polls connect() — from
        // the main runtime it would starve under API load. Abort the dial task
        // on timeout so cancelled dials don't accumulate.
        let endpoint = self.endpoint.clone();
        let mut dial =
            net_rt().spawn(async move { endpoint.connect(dial_addr, HOPNET_ALPN).await });
        let node_id = peer.node_id;
        let conn = match tokio::time::timeout(connect_budget, &mut dial).await {
            Err(_) => {
                dial.abort();
                return Err(CommsError::Transport(TransportError::ConnectionFailed(
                    format!("connection to node {node_id} timed out after {connect_budget:?}"),
                )));
            }
            Ok(Err(join_err)) => {
                return Err(CommsError::Transport(TransportError::ConnectionFailed(
                    join_err.to_string(),
                )));
            }
            Ok(Ok(Err(e))) => {
                return Err(CommsError::Transport(TransportError::ConnectionFailed(
                    e.to_string(),
                )));
            }
            Ok(Ok(Ok(conn))) => conn,
        };

        {
            let mut connections = self.connections.write().await;
            connections.insert(peer.node_id, conn.clone());
        }
        Ok(conn)
    }

    /// One rpc attempt + the retry-once-on-retryable semantics, reusing the
    /// SAME request_id so the receiver can deduplicate.
    async fn rpc_inner(
        &self,
        peer: &PeerRef,
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, CommsError> {
        let request_id: u64 = rand::random();
        self.rpc_with_id(peer, scope, payload, timeout, request_id)
            .await
    }

    async fn rpc_with_id(
        &self,
        peer: &PeerRef,
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
        request_id: u64,
    ) -> Result<Vec<u8>, CommsError> {
        let span = tracing::debug_span!("rpc_req", id = %format!("{:016x}", request_id), to = peer.node_id);
        async {
            let conn = self
                .get_connection_with_budget(peer, CONNECTION_TIMEOUT)
                .await?;

            match Self::try_rpc(&conn, request_id, scope, &payload, timeout).await {
                Ok(response) => Ok(response),
                Err(e) if e.is_retryable() => {
                    // Transport error (timeout or stream failure) — connection
                    // may be zombie. Evict and retry once with a fresh
                    // connection, reusing the same request_id so the receiver
                    // can deduplicate.
                    self.remove_connection(peer.node_id).await;
                    let conn = self
                        .get_connection_with_budget(peer, CONNECTION_TIMEOUT)
                        .await?;
                    Self::try_rpc(&conn, request_id, scope, &payload, timeout).await
                }
                Err(e) => Err(e),
            }
        }
        .instrument(span)
        .await
    }

    /// Attempt a single request on an existing connection. `timeout` covers
    /// stream I/O only (open, send, receive one frame).
    async fn try_rpc(
        conn: &Connection,
        request_id: u64,
        scope: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>, CommsError> {
        tokio::time::timeout(timeout, async {
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
            write_envelope(&mut send, request_id, scope, payload).await?;
            send.finish()
                .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))?;
            read_frame_body(&mut recv).await
        })
        .await
        .map_err(|_| CommsError::Transport(TransportError::Timeout))?
    }

    // ------------------------------------------------------------------
    // Server side
    // ------------------------------------------------------------------

    async fn handle_connection(
        &self,
        incoming: iroh::endpoint::Incoming,
    ) -> Result<(), CommsError> {
        // Unknown peers are already rejected by the before_registration hook
        // before the connection reaches this point (no holepunching occurs).
        let conn = incoming
            .await
            .map_err(|e| CommsError::Transport(TransportError::ConnectionFailed(e.to_string())))?;
        let remote = conn.remote_id();
        let pubkey = *remote.as_bytes();
        // The hook already vetted the peer; this resolves attribution only.
        let node_id = self.directory.node_id(&pubkey).await.unwrap_or(-1);
        let peer = PeerRef { node_id, pubkey };
        tracing::debug!("accepted iroh connection from node {}", peer.node_id);

        loop {
            match conn.accept_bi().await {
                Ok((send, recv)) => {
                    let comms = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = comms.handle_stream(send, recv, peer).await {
                            tracing::debug!("iroh stream error from node {}: {}", peer.node_id, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::debug!("iroh connection closed from node {}: {}", peer.node_id, e);
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_stream(
        &self,
        mut send: iroh::endpoint::SendStream,
        mut recv: iroh::endpoint::RecvStream,
        peer: PeerRef,
    ) -> Result<(), CommsError> {
        let envelope = read_envelope(&mut recv).await?;
        let span = tracing::debug_span!(
            "rpc_req",
            id = %format!("{:016x}", envelope.request_id),
            from = peer.node_id
        );

        async {
            // Comms-reserved liveness ping: pure nonce echo, no app state.
            if envelope.scope == PING_SCOPE {
                write_frame(&mut send, &envelope.payload).await?;
                return send.finish().map_err(|e| {
                    CommsError::Transport(TransportError::StreamFailed(e.to_string()))
                });
            }

            let Some(entry) = self
                .scopes
                .get()
                .and_then(|s| s.entries.get(envelope.scope.as_str()))
            else {
                tracing::warn!(
                    "no handler registered for comms scope {:?} (from node {})",
                    envelope.scope,
                    peer.node_id
                );
                return Ok(()); // drop the stream; the peer sees a stream error
            };

            match entry {
                ScopeEntry::Rpc(handler) => {
                    // Receiver-side dedup: first caller computes; retried
                    // requests (same id) wait for and reuse the same bytes.
                    let cell = {
                        let mut cache = self.dedup.lock().unwrap();
                        cache
                            .entry(envelope.request_id)
                            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                            .clone()
                    };
                    let dedup = self.dedup.clone();
                    let request_id = envelope.request_id;
                    tokio::spawn(async move {
                        tokio::time::sleep(DEDUP_TTL).await;
                        dedup.lock().unwrap().remove(&request_id);
                    });

                    let handler = handler.clone();
                    let payload = envelope.payload;
                    let response = cell
                        .get_or_init(|| async move { handler.handle(peer, payload).await })
                        .await;
                    write_frame(&mut send, response).await?;
                    send.finish().map_err(|e| {
                        CommsError::Transport(TransportError::StreamFailed(e.to_string()))
                    })
                }
                ScopeEntry::Streamed(handler) => {
                    // Multi-frame protocol: no dedup (the protocol owns
                    // idempotency); the handler drives the sink to completion.
                    let sink: Box<dyn FrameSink> = Box::new(IrohFrameSink { send });
                    handler.handle(peer, envelope.payload, sink).await;
                    Ok(())
                }
            }
        }
        .instrument(span)
        .await
    }
}

struct IrohFrameSink {
    send: iroh::endpoint::SendStream,
}

impl FrameSink for IrohFrameSink {
    fn send(&mut self, frame: Vec<u8>) -> BoxFuture<'_, Result<(), CommsError>> {
        Box::pin(async move { write_frame(&mut self.send, &frame).await })
    }

    fn finish(mut self: Box<Self>) -> Result<(), CommsError> {
        self.send
            .finish()
            .map_err(|e| CommsError::Transport(TransportError::StreamFailed(e.to_string())))
    }
}

impl crate::Rpc for IrohComms {
    fn rpc(
        &self,
        peer: &PeerRef,
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, CommsError>> + Send {
        let peer = *peer;
        async move { self.rpc_inner(&peer, scope, payload, timeout).await }
    }
}

impl crate::Broadcast for IrohComms {
    fn broadcast(
        &self,
        peers: &[PeerRef],
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
    ) {
        for peer in peers {
            let comms = self.clone();
            let peer = *peer;
            let payload = payload.clone();
            // Fire-and-forget: fresh request id per send (no dedup value —
            // the ack is the only response), failures logged at debug.
            net_rt().spawn(async move {
                if let Err(e) = comms.rpc_inner(&peer, scope, payload, timeout).await {
                    tracing::debug!("broadcast to node {} failed: {}", peer.node_id, e);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Rpc, RpcHandler, StreamHandler};
    use std::sync::atomic::AtomicUsize;

    // Impact: the envelope header is the one framing every scope shares;
    // silent drift here severs RPC between releases while every payload
    // golden still passes (hopnet-comms/docs/wire.md).
    // Should: encode the documented byte layout exactly — request id LE,
    // one-byte scope length, scope utf8, payload length LE.
    #[test]
    fn envelope_header_golden() {
        let header = encode_envelope_header(0x0123_4567_89ab_cdef, "status", 4);
        let expected: &[u8] = &[
            0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, // request_id LE
            0x06, // scope_len
            b's', b't', b'a', b't', b'u', b's', // scope
            0x04, 0x00, 0x00, 0x00, // payload_len LE
        ];
        assert_eq!(header, expected, "envelope wire format drifted");
    }

    /// Directory that knows every peer (loopback meshes).
    struct AllowAll;
    impl PeerDirectory for AllowAll {
        fn is_known(&self, _pubkey: &[u8; 32]) -> BoxFuture<'_, bool> {
            Box::pin(async { true })
        }
        fn node_id(&self, _pubkey: &[u8; 32]) -> BoxFuture<'_, Option<i32>> {
            Box::pin(async { Some(7) })
        }
    }

    /// Directory that knows nobody (reject path).
    struct DenyAll;
    impl PeerDirectory for DenyAll {
        fn is_known(&self, _pubkey: &[u8; 32]) -> BoxFuture<'_, bool> {
            Box::pin(async { false })
        }
        fn node_id(&self, _pubkey: &[u8; 32]) -> BoxFuture<'_, Option<i32>> {
            Box::pin(async { None })
        }
    }

    struct Echo {
        calls: Arc<AtomicUsize>,
    }
    impl RpcHandler for Echo {
        fn handle(&self, _peer: PeerRef, payload: Vec<u8>) -> BoxFuture<'_, Vec<u8>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { payload })
        }
    }

    /// Two-frame protocol: an "ack" frame, then the payload uppercased.
    struct TwoPhase;
    impl StreamHandler for TwoPhase {
        fn handle(
            &self,
            _peer: PeerRef,
            payload: Vec<u8>,
            mut out: Box<dyn FrameSink>,
        ) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                out.send(b"ack".to_vec()).await.unwrap();
                out.send(payload.to_ascii_uppercase()).await.unwrap();
                out.finish().unwrap();
            })
        }
    }

    fn loopback_addr(comms: &IrohComms) -> EndpointAddr {
        let ep = comms.endpoint();
        let mut addr = EndpointAddr::new(ep.id());
        for sock in ep.bound_sockets() {
            let sock = if sock.ip().is_unspecified() {
                std::net::SocketAddr::new(
                    match sock {
                        std::net::SocketAddr::V4(_) => {
                            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                        }
                        std::net::SocketAddr::V6(_) => {
                            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
                        }
                    },
                    sock.port(),
                )
            } else {
                sock
            };
            addr = addr.with_ip_addr(sock);
        }
        addr
    }

    /// Bind a pair, register `scopes` on B, connect A→B directly (loopback,
    /// no discovery), return (A, peer-ref-of-B, call-count handles).
    async fn pair(
        server_directory: Arc<dyn PeerDirectory>,
        scopes: ScopeRegistry,
    ) -> (IrohComms, IrohComms, PeerRef) {
        let a = IrohComms::bind(rand::random(), Arc::new(AllowAll), None)
            .await
            .unwrap();
        let b = IrohComms::bind(rand::random(), server_directory, None)
            .await
            .unwrap();
        b.start(scopes);
        a.start(ScopeRegistry::new());
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();
        (a, b, peer_b)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scope_roundtrip_and_ping() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut scopes = ScopeRegistry::new();
        scopes.rpc(
            "echo",
            Arc::new(Echo {
                calls: calls.clone(),
            }),
        );
        let (a, _b, peer_b) = pair(Arc::new(AllowAll), scopes).await;

        let reply = a
            .rpc(&peer_b, "echo", b"bonjour".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(reply, b"bonjour");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let rtt = a.ping(&peer_b).await.unwrap();
        assert!(rtt > 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streamed_two_frame_call() {
        let mut scopes = ScopeRegistry::new();
        scopes.streamed("twophase", Arc::new(TwoPhase));
        let (a, _b, peer_b) = pair(Arc::new(AllowAll), scopes).await;

        let mut call = a
            .open_call(&peer_b, "twophase", b"abc".to_vec(), CallOptions::default())
            .await
            .unwrap();
        assert_eq!(call.recv(Duration::from_secs(5)).await.unwrap(), b"ack");
        assert_eq!(call.recv(Duration::from_secs(5)).await.unwrap(), b"ABC");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dedup_same_request_id_invokes_handler_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut scopes = ScopeRegistry::new();
        scopes.rpc(
            "echo",
            Arc::new(Echo {
                calls: calls.clone(),
            }),
        );
        let (a, _b, peer_b) = pair(Arc::new(AllowAll), scopes).await;

        let id: u64 = rand::random();
        let r1 = a
            .rpc_with_id(&peer_b, "echo", b"x".to_vec(), Duration::from_secs(5), id)
            .await
            .unwrap();
        let r2 = a
            .rpc_with_id(&peer_b, "echo", b"x".to_vec(), Duration::from_secs(5), id)
            .await
            .unwrap();
        assert_eq!(r1, r2);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "retried request (same id) must not re-invoke the handler"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_peer_rejected_at_registration() {
        // Server B knows nobody: A's connection must be rejected before any
        // stream can serve (QUIC handshake completes, then app-layer reject).
        let mut scopes = ScopeRegistry::new();
        scopes.rpc(
            "echo",
            Arc::new(Echo {
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        );
        let a = IrohComms::bind(rand::random(), Arc::new(AllowAll), None)
            .await
            .unwrap();
        let b = IrohComms::bind(rand::random(), Arc::new(DenyAll), None)
            .await
            .unwrap();
        b.start(scopes);
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };

        // Either the direct connect fails, or the connection dies on use.
        let connected = a.connect_to_addr(2, loopback_addr(&b)).await;
        if connected.is_ok() {
            let result = a
                .rpc(&peer_b, "echo", b"hi".to_vec(), Duration::from_secs(3))
                .await;
            assert!(result.is_err(), "rejected peer must not get service");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_frame_rejected() {
        let mut scopes = ScopeRegistry::new();
        scopes.rpc(
            "echo",
            Arc::new(Echo {
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        );
        let (a, _b, peer_b) = pair(Arc::new(AllowAll), scopes).await;

        let oversized = vec![0u8; MAX_MESSAGE_SIZE + 1];
        let result = a
            .rpc(&peer_b, "echo", oversized, Duration::from_secs(5))
            .await;
        assert!(result.is_err(), "server must reject frames over the cap");
    }

    #[test]
    #[should_panic(expected = "duplicate comms scope registration")]
    fn duplicate_scope_registration_panics() {
        let mut scopes = ScopeRegistry::new();
        let calls = Arc::new(AtomicUsize::new(0));
        scopes.rpc(
            "dup",
            Arc::new(Echo {
                calls: calls.clone(),
            }),
        );
        scopes.rpc("dup", Arc::new(Echo { calls }));
    }

    #[test]
    #[should_panic(expected = "reserved by the transport")]
    fn ping_scope_reserved() {
        let mut scopes = ScopeRegistry::new();
        scopes.rpc(
            "ping",
            Arc::new(Echo {
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        );
    }
}
