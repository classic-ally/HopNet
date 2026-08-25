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

use crate::alpn::{self, AcceptTier};
use crate::{
    BoxFuture, CommsError, FrameSink, PeerDirectory, PeerRef, ProtocolError, RpcHandler,
    ScopeClass, StreamHandler, TransportError, PING_SCOPE,
};

pub(crate) struct ScopeEntry {
    pub(crate) class: ScopeClass,
    pub(crate) kind: ScopeKind,
}

pub(crate) enum ScopeKind {
    /// `prev` is the previous-generation adapter — `Some` iff the scope
    /// is compat-class (the window invariant, RFC-025 contract rule 4,
    /// enforced at registration by `rpc_compat`'s mandatory parameter).
    Rpc {
        head: Arc<dyn RpcHandler>,
        prev: Option<Arc<dyn RpcHandler>>,
    },
    Streamed(Arc<dyn StreamHandler>),
}

/// One handler per scope namespace. Duplicate registration — or claiming
/// the reserved "ping" scope — panics at registration time (boot tripwire:
/// a scope collision is a programming error, not a runtime condition).
///
/// Every registration names its ALPN class (RFC-025 §Scope Classes): the
/// plain methods register locked scopes — the safe default, exact-version
/// only — and the `_compat` variants register the windowed compat class.
#[derive(Default)]
pub struct ScopeRegistry {
    pub(crate) entries: HashMap<&'static str, ScopeEntry>,
}

impl ScopeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a single-response handler for `scope` (locked class).
    pub fn rpc(&mut self, scope: &'static str, handler: Arc<dyn RpcHandler>) {
        self.insert_classed(
            scope,
            ScopeClass::Locked,
            ScopeKind::Rpc {
                head: handler,
                prev: None,
            },
        );
    }

    /// Register a multi-frame handler for `scope` (locked class).
    pub fn streamed(&mut self, scope: &'static str, handler: Arc<dyn StreamHandler>) {
        self.insert_classed(scope, ScopeClass::Locked, ScopeKind::Streamed(handler));
    }

    /// Register a single-response handler on the compat class. Compat is
    /// an allowlist (RFC-025): admission requires a named cross-version
    /// consumer and freezeable vocabulary. `prev` — the
    /// previous-generation adapter (or the same Arc when the vocabularies
    /// are byte-identical) — is mandatory: a compat scope always serves
    /// exactly the window [head-1, head], so "forgot the adapter" is a
    /// compile error, not a field incident.
    pub fn rpc_compat(
        &mut self,
        scope: &'static str,
        head: Arc<dyn RpcHandler>,
        prev: Arc<dyn RpcHandler>,
    ) {
        self.insert_classed(
            scope,
            ScopeClass::Compat,
            ScopeKind::Rpc {
                head,
                prev: Some(prev),
            },
        );
    }

    /// Register a multi-frame handler on the compat class. Head-only: no
    /// streamed compat scope exists today, so there is no prev slot to
    /// carry — a future streamed compat admission grows one alongside
    /// its named cross-version consumer.
    pub fn streamed_compat(&mut self, scope: &'static str, handler: Arc<dyn StreamHandler>) {
        self.insert_classed(scope, ScopeClass::Compat, ScopeKind::Streamed(handler));
    }

    /// The registered class of `scope` — the authority for dial-side
    /// family selection and the host's class-pin test.
    pub fn class_of(&self, scope: &str) -> Option<ScopeClass> {
        self.entries.get(scope).map(|e| e.class)
    }

    /// Every registered scope with its class, for the class-pin test.
    pub fn scopes(&self) -> impl Iterator<Item = (&'static str, ScopeClass)> + '_ {
        self.entries
            .iter()
            .map(|(scope, entry)| (*scope, entry.class))
    }

    fn insert_classed(&mut self, scope: &'static str, class: ScopeClass, kind: ScopeKind) {
        self.insert(scope, ScopeEntry { class, kind });
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

/// The pre-enforcement ALPN, kept exported for harnesses that
/// impersonate a legacy dialer (generation 0 — RFC-025 §Evolution).
pub const HOPNET_ALPN: &[u8] = alpn::LEGACY_ALPN;

/// The node's ALPN identity. The code and window are settled at bind
/// (RFC-025 §The ALPN Scheme: every string known at bind, no
/// committed-state reads); the magic is settled at bind for set-up
/// nodes and ADOPTED at join-code entry for fresh ones (S5). Unadopted
/// = DEFERRED: an empty ALPN serve list (TLS-dead inbound — QUIC's
/// strict ALPN fatals any offer against an empty list) and every dial
/// errors. A drive-by JoinDeliver cannot complete TLS before the
/// operator enters the mesh code.
pub(crate) struct AlpnIdentity {
    magic: OnceLock<[u8; 4]>,
    code: u32,
    head: u32,
}

impl AlpnIdentity {
    fn new(magic: Option<[u8; 4]>) -> Self {
        let cell = OnceLock::new();
        if let Some(m) = magic {
            let _ = cell.set(m);
        }
        Self {
            magic: cell,
            code: hopnet_common::version::effective_running_code(),
            head: alpn::effective_compat_head(),
        }
    }

    fn serve_list(&self) -> Vec<Vec<u8>> {
        match self.magic.get() {
            Some(magic) => alpn::serve_list(magic, self.code, self.head),
            None => Vec::new(),
        }
    }

    /// Accept-tier classification of a negotiated ALPN. Deferred mode
    /// serves nothing, so no connection can reach this — defensive
    /// Unknown.
    fn classify(&self, alpn_bytes: &[u8]) -> AcceptTier {
        match self.magic.get() {
            Some(magic) => alpn::classify_accept(magic, self.code, self.head, alpn_bytes),
            None => AcceptTier::Unknown,
        }
    }

    /// The generation a DIALED connection's negotiated ALPN commits both
    /// sides to. Compat connections only by contract; a locked ALPN here
    /// is a caller bug — answer head (the locked family always speaks
    /// head vocabulary) and note it. Deferred cannot have dialed.
    fn generation_of(&self, alpn_bytes: &[u8]) -> u32 {
        match self.magic.get() {
            None => {
                tracing::debug!("generation_of on a deferred endpoint");
                self.head
            }
            Some(magic) => match alpn::parse_alpn(magic, alpn_bytes) {
                alpn::ParsedAlpn::Compat(g) => g,
                parsed => {
                    tracing::debug!("generation_of on non-compat ALPN ({parsed:?})");
                    self.head
                }
            },
        }
    }
}

/// The class of an ACCEPTED connection, read once from the negotiated
/// ALPN and threaded through dispatch. Locked connections serve every
/// scope; a compat connection serves only compat-class scopes plus the
/// transport ping (RFC-025 §Scope Classes).
#[derive(Debug, Clone, Copy)]
enum ConnClass {
    Locked,
    Compat(u32),
}

/// Connection-cache key: one cached connection per peer per family. The
/// compat entry may have negotiated any in-window generation — the
/// negotiated ALPN, not the key, is the codec authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ConnKey {
    Locked,
    Compat,
}

/// What to offer on a dial. Compat dials offer the whole window in ONE
/// handshake (`ConnectOptions::with_additional_alpns`) and TLS selects
/// the highest mutual generation (RFC-025 §Settled Questions).
enum DialPlan {
    Single(Vec<u8>),
    Multi { head: Vec<u8>, rest: Vec<Vec<u8>> },
}

impl AlpnIdentity {
    /// Cache key + dial plan from ONE read of the magic — no torn read
    /// across a concurrent adoption. Deferred → error: nothing may dial
    /// before the mesh identity exists.
    fn dial_identity(&self, class: ScopeClass) -> Result<(ConnKey, DialPlan), CommsError> {
        let Some(magic) = self.magic.get() else {
            return Err(CommsError::Protocol(ProtocolError::EndpointDeferred));
        };
        Ok(match class {
            ScopeClass::Locked => (
                ConnKey::Locked,
                DialPlan::Single(alpn::locked_alpn(magic, self.code)),
            ),
            ScopeClass::Compat => {
                let mut offer = alpn::compat_offer(magic, self.head);
                let head = offer.remove(0);
                (ConnKey::Compat, DialPlan::Multi { head, rest: offer })
            }
        })
    }
}

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
    identity: Arc<AlpnIdentity>,
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
        alpn_bytes: &'a [u8],
        side: iroh::endpoint::Side,
    ) -> AfterHandshakeOutcome {
        // Only validate incoming (server-side) connections. Outgoing connections
        // are initiated intentionally by our application to known peers, and TLS
        // certificates prevent impersonation.
        if side == iroh::endpoint::Side::Client {
            return AfterHandshakeOutcome::Accept;
        }

        // Unknown-node first: strangers never learn our generation window.
        if !self.directory.is_known(remote_id.as_bytes()).await {
            tracing::warn!("rejected iroh connection from unknown node: {}", remote_id);
            return AfterHandshakeOutcome::Reject {
                error_code: alpn::REJECT_UNKNOWN_NODE.into(),
                reason: b"unknown node".to_vec(),
            };
        }

        match self.identity.classify(alpn_bytes) {
            // The retired tier (RFC-025): TLS was accepted solely so this
            // structured reject can name the floor.
            AcceptTier::Retired { floor } => AfterHandshakeOutcome::Reject {
                error_code: alpn::REJECT_COMPAT_RETIRED.into(),
                reason: alpn::encode_retired_reason(floor, self.identity.code),
            },
            // Defensive: a negotiated ALPN we never listed is a transport
            // bug, not a tier — refuse rather than misdispatch.
            AcceptTier::Unknown => AfterHandshakeOutcome::Reject {
                error_code: alpn::REJECT_UNKNOWN_NODE.into(),
                reason: b"unknown alpn".to_vec(),
            },
            AcceptTier::ServedLocked | AcceptTier::ServedCompat(_) => AfterHandshakeOutcome::Accept,
        }
    }
}

// ============================================================================
// IrohComms
// ============================================================================

type DedupMap = std::sync::Mutex<HashMap<u64, Arc<tokio::sync::OnceCell<Vec<u8>>>>>;

/// Options for [`IrohComms::bind`].
#[derive(Default)]
pub struct BindOptions {
    /// Self-hosted relay URL. When set, the endpoint uses ONLY this relay
    /// (no n0 relays, no public discovery) and every dial pins the peer's
    /// address to it. Reading the env var is host policy, not comms'.
    pub relay_url: Option<String>,
    /// Mesh magic (RFC-025): the 4-byte truncation of the anchor chain
    /// id. Set-up nodes derive it at boot and pass Some. None = DEFERRED
    /// (S5): the endpoint binds with an EMPTY ALPN serve list — TLS-dead
    /// inbound, dials error — until [`IrohComms::adopt_magic`] installs
    /// the identity at join-code entry.
    pub magic: Option<[u8; 4]>,
}

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
    /// Connection cache: one entry per peer per ALPN family.
    connections: Arc<RwLock<HashMap<(i32, ConnKey), Connection>>>,
    /// Known-address hints (loopback tests / connect_to_addr): consulted
    /// by every family's dial before falling back to discovery.
    addr_hints: Arc<RwLock<HashMap<i32, EndpointAddr>>>,
    /// Receiver-side request dedup: request_id → response-byte cache.
    dedup: Arc<DedupMap>,
    directory: Arc<dyn PeerDirectory>,
    /// Self-hosted relay (host passes HOPNET_RELAY_URL). When set, the
    /// endpoint uses ONLY this relay (no n0 relays, no public discovery) and
    /// every dial pins the peer's address to it.
    custom_relay: Option<iroh::RelayUrl>,
    identity: Arc<AlpnIdentity>,
    scopes: Arc<OnceLock<ScopeRegistry>>,
    started: Arc<AtomicBool>,
}

impl IrohComms {
    /// Bind the endpoint. `secret` is the node's raw Ed25519 secret key;
    /// the ALPN identity (locked code + compat window, RFC-025) is settled
    /// here from the workspace-unified effective-code seam and
    /// `opts.magic` — see [`BindOptions`].
    pub async fn bind(
        secret: [u8; 32],
        directory: Arc<dyn PeerDirectory>,
        opts: BindOptions,
    ) -> Result<Self, CommsError> {
        Self::bind_with_identity(
            secret,
            directory,
            opts.relay_url,
            AlpnIdentity::new(opts.magic),
        )
        .await
    }

    async fn bind_with_identity(
        secret: [u8; 32],
        directory: Arc<dyn PeerDirectory>,
        relay_url: Option<String>,
        identity: AlpnIdentity,
    ) -> Result<Self, CommsError> {
        let identity = Arc::new(identity);
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
        let hook_identity = identity.clone();
        let serve_list = identity.serve_list();
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
                    // Accept-list order IS TLS negotiation preference.
                    .alpns(serve_list)
                    .hooks(HookAdapter {
                        directory: hook_directory,
                        identity: hook_identity,
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
            addr_hints: Arc::new(RwLock::new(HashMap::new())),
            dedup: Arc::new(std::sync::Mutex::new(HashMap::new())),
            directory,
            custom_relay,
            identity,
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

    /// Adopt the mesh magic on a deferred endpoint (RFC-025 S5): sets
    /// the interior-mutable identity, swaps the served ALPN list onto
    /// the live endpoint (inbound-only — `set_alpns`; outbound dials
    /// read the identity per dial), and clears the connection cache
    /// (defensively — deferred dials error and deferred inbound is
    /// TLS-dead, so nothing can have been cached). Once-only: the first
    /// adoption wins; repeating the SAME magic is Ok (idempotent
    /// re-entry); a DIFFERENT magic errors — the identity is settled
    /// for the process lifetime, restart to re-enter.
    pub async fn adopt_magic(&self, magic: [u8; 4]) -> Result<(), CommsError> {
        match self.identity.magic.set(magic) {
            Ok(()) => {
                self.endpoint.set_alpns(self.identity.serve_list());
                let mut connections = self.connections.write().await;
                if !connections.is_empty() {
                    tracing::warn!(
                        "connection cache non-empty at magic adoption ({} entries)",
                        connections.len()
                    );
                }
                connections.clear();
                Ok(())
            }
            Err(_) => {
                let settled = self.identity.magic.get().copied();
                if settled == Some(magic) {
                    Ok(())
                } else {
                    Err(CommsError::Protocol(ProtocolError::ValueMismatch {
                        field: "mesh_magic",
                        expected: format!("{:02x?}", settled),
                        got: format!("{magic:02x?}"),
                    }))
                }
            }
        }
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
        self.ping_on(peer, ScopeClass::Compat).await
    }

    /// The ping over the LOCKED family (RFC-025 S5): completing TLS on
    /// the locked ALPN proves the peer speaks OUR mesh's magic at OUR
    /// exact release — the same-mesh reachability check the Add Node
    /// probe needs. The compat ping cannot prove this while generation 0
    /// is in-window: its offer falls through to the magic-less legacy
    /// string, so a wrong-code (or foreign) node still answers it.
    pub async fn ping_locked(&self, peer: &PeerRef) -> Result<u64, CommsError> {
        self.ping_on(peer, ScopeClass::Locked).await
    }

    /// The ping short-circuit is served on every connection class, so a
    /// nonce echo works over whichever family `class` dials.
    async fn ping_on(&self, peer: &PeerRef, class: ScopeClass) -> Result<u64, CommsError> {
        let start = Instant::now();
        let nonce = rand::random::<u64>();
        let request_id: u64 = rand::random();
        let (reply, _) = self
            .rpc_with_id_on(
                peer,
                PING_SCOPE,
                nonce.to_le_bytes().to_vec(),
                Duration::from_secs(5),
                request_id,
                class,
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
            .get_connection_with_budget(peer, connect_budget, self.scope_class(scope))
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

    /// Remove a peer's connections from the cache (e.g. on error, timeout,
    /// or the forward client's NoAck eviction). Evicts EVERY family's
    /// entry: eviction is about the peer being a zombie, not a family.
    pub async fn remove_connection(&self, node_id: i32) {
        let mut connections = self.connections.write().await;
        connections.retain(|(id, _), _| *id != node_id);
    }

    /// The registered ALPN class of `scope`, for dial-side family
    /// selection. The transport ping is compat-class by definition;
    /// unregistered scopes dial locked — the safe default (exact match).
    fn scope_class(&self, scope: &str) -> ScopeClass {
        if scope == PING_SCOPE {
            return ScopeClass::Compat;
        }
        self.scopes
            .get()
            .and_then(|s| s.class_of(scope))
            .unwrap_or(ScopeClass::Locked)
    }

    /// A refusal encoded in a QUIC close, if this is one (RFC-025):
    /// `no_application_protocol` (crypto 0x178) → AlpnRejected; the hook's
    /// COMPAT_RETIRED application close → CompatRetired.
    fn refusal_from_close(reason: &iroh::endpoint::ConnectionError) -> Option<crate::RefusalError> {
        use iroh::endpoint::{ConnectionError, TransportErrorCode, VarInt};
        match reason {
            ConnectionError::ConnectionClosed(close)
                if close.error_code == TransportErrorCode::crypto(0x78) =>
            {
                Some(crate::RefusalError::AlpnRejected)
            }
            ConnectionError::ApplicationClosed(close)
                if close.error_code == VarInt::from(alpn::REJECT_COMPAT_RETIRED) =>
            {
                let (floor, node_version) = alpn::parse_retired_reason(&close.reason)
                    .unwrap_or_else(|| {
                        tracing::warn!("unparseable COMPAT_RETIRED reason bytes");
                        (0, 0)
                    });
                Some(crate::RefusalError::CompatRetired {
                    floor,
                    node_version,
                })
            }
            _ => None,
        }
    }

    /// A structural refusal recorded on the connection's close, if any.
    fn refusal_on(conn: &Connection) -> Option<crate::RefusalError> {
        conn.close_reason()
            .as_ref()
            .and_then(Self::refusal_from_close)
    }

    /// Walk a connect error's source chain for a structural refusal.
    fn classify_connect_error(
        e: &(dyn std::error::Error + 'static),
    ) -> Option<crate::RefusalError> {
        let mut cursor: Option<&(dyn std::error::Error + 'static)> = Some(e);
        while let Some(err) = cursor {
            if let Some(conn_err) = err.downcast_ref::<iroh::endpoint::ConnectionError>() {
                return Self::refusal_from_close(conn_err);
            }
            cursor = err.source();
        }
        None
    }

    /// One bounded dial on the net runtime (the per-connection driver is
    /// spawned on the runtime that polls connect() — from the main runtime
    /// it would starve under API load; the abort keeps cancelled dials
    /// from accumulating). Refusals come back typed, never retryable.
    async fn dial_bounded(
        &self,
        dial_addr: EndpointAddr,
        plan: DialPlan,
        budget: Duration,
        node_id: i32,
    ) -> Result<Connection, CommsError> {
        let endpoint = self.endpoint.clone();
        let mut dial = net_rt().spawn(async move {
            match plan {
                DialPlan::Single(alpn_bytes) => endpoint.connect(dial_addr, &alpn_bytes).await,
                DialPlan::Multi { head, rest } => {
                    let opts = iroh::endpoint::ConnectOptions::new().with_additional_alpns(rest);
                    let connecting = endpoint.connect_with_opts(dial_addr, &head, opts).await?;
                    Ok(connecting.await?)
                }
            }
        });
        let conn = match tokio::time::timeout(budget, &mut dial).await {
            Err(_) => {
                dial.abort();
                return Err(CommsError::Transport(TransportError::ConnectionFailed(
                    format!("connection to node {node_id} timed out after {budget:?}"),
                )));
            }
            Ok(Err(join_err)) => {
                return Err(CommsError::Transport(TransportError::ConnectionFailed(
                    join_err.to_string(),
                )));
            }
            Ok(Ok(Err(e))) => {
                return Err(match Self::classify_connect_error(&e) {
                    Some(refusal) => CommsError::Refused(refusal),
                    None => CommsError::Transport(TransportError::ConnectionFailed(e.to_string())),
                });
            }
            Ok(Ok(Ok(conn))) => conn,
        };
        // A hook reject is racy dial-side: connect() may return Ok with the
        // close already recorded. One immediate check keeps a refused
        // connection out of the cache; a reject landing later surfaces on
        // first use as a stream error → evict → redial → classified then.
        if let Some(reason) = conn.close_reason() {
            if let Some(refusal) = Self::refusal_from_close(&reason) {
                return Err(CommsError::Refused(refusal));
            }
        }
        Ok(conn)
    }

    /// Establish and cache a connection to a peer at a KNOWN address,
    /// bypassing discovery. Used by in-process tests over loopback, where
    /// endpoints know each other's bound sockets directly. The address is
    /// remembered so later dials on ANY family reach the peer too.
    pub async fn connect_to_addr(
        &self,
        node_id: i32,
        addr: EndpointAddr,
    ) -> Result<(), CommsError> {
        self.addr_hints.write().await.insert(node_id, addr.clone());
        let (key, plan) = self.identity.dial_identity(ScopeClass::Locked)?;
        let conn = self
            .dial_bounded(addr, plan, CONNECTION_TIMEOUT, node_id)
            .await?;
        self.connections.write().await.insert((node_id, key), conn);
        Ok(())
    }

    /// Get or establish a connection to a peer on the family `class`
    /// dials. Uses the cached connection if available; establishment is
    /// bounded by `connect_budget`.
    async fn get_connection_with_budget(
        &self,
        peer: &PeerRef,
        connect_budget: Duration,
        class: ScopeClass,
    ) -> Result<Connection, CommsError> {
        let (conn_key, plan) = self.identity.dial_identity(class)?;
        let key = (peer.node_id, conn_key);
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(&key) {
                // The remote identity must match the peer we were asked
                // for: the Add Node probe keys every candidate under
                // node_id -1, so a stale hit here would "verify" a NEW
                // joiner with TLS that was completed by the PREVIOUS one
                // (S5 gate-2 finding). A mismatched entry is simply
                // skipped — the fresh dial below overwrites it.
                if conn.close_reason().is_none() && conn.remote_id().as_bytes() == &peer.pubkey {
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
        // A recorded address hint (loopback) wins; otherwise discovery —
        // and with a self-hosted relay there is none, so the peer's
        // address pins to our relay.
        let dial_addr = match self.addr_hints.read().await.get(&peer.node_id) {
            Some(hint) => hint.clone(),
            None => {
                let mut addr = EndpointAddr::new(peer_key);
                if let Some(url) = &self.custom_relay {
                    addr = addr.with_relay_url(url.clone());
                }
                addr
            }
        };
        let conn = self
            .dial_bounded(dial_addr, plan, connect_budget, peer.node_id)
            .await?;
        if matches!(class, ScopeClass::Compat) {
            tracing::debug!(
                alpn = %String::from_utf8_lossy(conn.alpn()),
                "compat dial to node {} negotiated",
                peer.node_id
            );
        }

        {
            let mut connections = self.connections.write().await;
            connections.insert(key, conn.clone());
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
            .map(|(response, _)| response)
    }

    /// Returns the response bytes AND the connection that served the
    /// successful attempt — its negotiated ALPN is the codec authority
    /// (`rpc_negotiated`); plain rpc discards it.
    async fn rpc_with_id(
        &self,
        peer: &PeerRef,
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
        request_id: u64,
    ) -> Result<(Vec<u8>, Connection), CommsError> {
        let class = self.scope_class(scope);
        self.rpc_with_id_on(peer, scope, payload, timeout, request_id, class)
            .await
    }

    /// `rpc_with_id` with the dial family forced — the locked-family
    /// ping's seam (the registry's class stays the authority everywhere
    /// else).
    async fn rpc_with_id_on(
        &self,
        peer: &PeerRef,
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
        request_id: u64,
        class: ScopeClass,
    ) -> Result<(Vec<u8>, Connection), CommsError> {
        let span = tracing::debug_span!("rpc_req", id = %format!("{:016x}", request_id), to = peer.node_id);
        async {
            let conn = self
                .get_connection_with_budget(peer, CONNECTION_TIMEOUT, class)
                .await?;

            match Self::try_rpc(&conn, request_id, scope, &payload, timeout).await {
                Ok(response) => Ok((response, conn)),
                Err(e) if e.is_retryable() => {
                    // A LATE hook reject (wire.md's third surfacing mode)
                    // arrives as exactly this stream failure — the close
                    // that caused it is recorded by now, so the refusal
                    // classifies deterministically before any retry.
                    if let Some(refusal) = Self::refusal_on(&conn) {
                        self.remove_connection(peer.node_id).await;
                        return Err(CommsError::Refused(refusal));
                    }
                    // Transport error (timeout or stream failure) — connection
                    // may be zombie. Evict and retry once with a fresh
                    // connection, reusing the same request_id so the receiver
                    // can deduplicate. Refused is NOT retryable (RFC-025): a
                    // structural refusal never earns the evict-and-redial.
                    self.remove_connection(peer.node_id).await;
                    let conn = self
                        .get_connection_with_budget(peer, CONNECTION_TIMEOUT, class)
                        .await?;
                    match Self::try_rpc(&conn, request_id, scope, &payload, timeout).await {
                        Ok(response) => Ok((response, conn)),
                        Err(e) if e.is_retryable() => {
                            let refusal = Self::refusal_on(&conn);
                            self.remove_connection(peer.node_id).await;
                            match refusal {
                                Some(r) => Err(CommsError::Refused(r)),
                                None => Err(e),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
        }
        .instrument(span)
        .await
    }

    /// Like [`Rpc::rpc`], returning the generation the connection's
    /// negotiated ALPN commits both sides to — the codec authority for
    /// decoding the response (RFC-025: "no dialer ever guesses what a
    /// peer speaks"). Compat scopes only by contract.
    pub async fn rpc_negotiated(
        &self,
        peer: &PeerRef,
        scope: &'static str,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<(Vec<u8>, u32), CommsError> {
        let request_id: u64 = rand::random();
        let (response, conn) = self
            .rpc_with_id(peer, scope, payload, timeout, request_id)
            .await?;
        Ok((response, self.identity.generation_of(conn.alpn())))
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

        // The negotiated ALPN, read once per connection, is the class every
        // stream on it dispatches under (RFC-025 §Scope Classes). The hook
        // already rejected retired/unknown tiers; a disagreement here is a
        // transport bug — refuse rather than misdispatch. A deferred
        // endpoint serves an empty ALPN list, so nothing can negotiate:
        // classify's deferred arm (Unknown) makes any arrival here drop.
        let conn_class = match self.identity.classify(conn.alpn()) {
            AcceptTier::ServedLocked => ConnClass::Locked,
            AcceptTier::ServedCompat(generation) => ConnClass::Compat(generation),
            tier => {
                tracing::warn!(
                    "dropping connection from node {} on unserved ALPN tier {:?}",
                    peer.node_id,
                    tier
                );
                return Ok(());
            }
        };
        tracing::debug!(
            "accepted iroh connection from node {} ({:?})",
            peer.node_id,
            conn_class
        );

        loop {
            match conn.accept_bi().await {
                Ok((send, recv)) => {
                    let comms = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = comms.handle_stream(send, recv, peer, conn_class).await {
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
        conn_class: ConnClass,
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

            // Admissibility (RFC-025 §Scope Classes): a compat connection
            // never carries locked traffic — a matched peer dials locked
            // scopes on the locked family, so this only fires on a bug or
            // a mismatched peer probing; same surface as unknown scope.
            if let ConnClass::Compat(generation) = conn_class {
                if entry.class == ScopeClass::Locked {
                    tracing::warn!(
                        "locked-class scope {:?} refused on compat connection \
                         (generation {generation}, from node {})",
                        envelope.scope,
                        peer.node_id
                    );
                    return Ok(());
                }
            }

            match &entry.kind {
                ScopeKind::Rpc { head, prev } => {
                    // Generation-keyed dispatch (RFC-025 §Evolution): the
                    // negotiated ALPN is the codec authority for every
                    // stream on the connection; a matched peer's locked
                    // connection speaks head.
                    let handler = if entry.class == ScopeClass::Compat {
                        let generation = match conn_class {
                            ConnClass::Compat(g) => g,
                            ConnClass::Locked => self.identity.head,
                        };
                        if generation == self.identity.head {
                            head
                        } else if generation == alpn::compat_floor(self.identity.head) {
                            prev.as_ref()
                                .expect("compat scope registered without prev handler")
                        } else {
                            // Unreachable via classify_accept; refuse
                            // rather than misdecode.
                            tracing::warn!(
                                "no handler for generation {generation} of scope {:?} \
                                 (from node {})",
                                envelope.scope,
                                peer.node_id
                            );
                            return Ok(());
                        }
                    } else {
                        head
                    };

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
                ScopeKind::Streamed(handler) => {
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
        let a = IrohComms::bind(
            rand::random(),
            Arc::new(AllowAll),
            BindOptions {
                relay_url: None,
                magic: Some([0x9f, 0x3a, 0x01, 0xcc]),
            },
        )
        .await
        .unwrap();
        let b = IrohComms::bind(
            rand::random(),
            server_directory,
            BindOptions {
                relay_url: None,
                magic: Some([0x9f, 0x3a, 0x01, 0xcc]),
            },
        )
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
        assert_eq!(r1.0, r2.0);
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
        let a = IrohComms::bind(
            rand::random(),
            Arc::new(AllowAll),
            BindOptions {
                relay_url: None,
                magic: Some([0x9f, 0x3a, 0x01, 0xcc]),
            },
        )
        .await
        .unwrap();
        let b = IrohComms::bind(
            rand::random(),
            Arc::new(DenyAll),
            BindOptions {
                relay_url: None,
                magic: Some([0x9f, 0x3a, 0x01, 0xcc]),
            },
        )
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

    // ------------------------------------------------------------------
    // RFC-025 enforcement (identity-injected binds)
    // ------------------------------------------------------------------

    const MAGIC: [u8; 4] = [0x9f, 0x3a, 0x01, 0xcc];
    const CODE: u32 = 20260806;

    /// Bind with an injected ALPN identity — the only way to vary code or
    /// COMPAT_HEAD in-process (both are compile-time in production).
    async fn bind_for_test(magic: Option<[u8; 4]>, code: u32, head: u32) -> IrohComms {
        let cell = OnceLock::new();
        if let Some(m) = magic {
            let _ = cell.set(m);
        }
        IrohComms::bind_with_identity(
            rand::random(),
            Arc::new(AllowAll),
            None,
            AlpnIdentity {
                magic: cell,
                code,
                head,
            },
        )
        .await
        .unwrap()
    }

    /// A compat "st" scope with SEPARATE head/prev handlers so tests can
    /// assert which generation served.
    fn compat_echo_windowed(
        head_calls: &Arc<AtomicUsize>,
        prev_calls: &Arc<AtomicUsize>,
    ) -> ScopeRegistry {
        let mut scopes = ScopeRegistry::new();
        scopes.rpc_compat(
            "st",
            Arc::new(Echo {
                calls: head_calls.clone(),
            }),
            Arc::new(Echo {
                calls: prev_calls.clone(),
            }),
        );
        scopes
    }

    fn compat_echo(calls: &Arc<AtomicUsize>) -> ScopeRegistry {
        compat_echo_windowed(calls, calls)
    }

    async fn negotiated_compat_alpn(comms: &IrohComms, node_id: i32) -> Vec<u8> {
        comms
            .connections
            .read()
            .await
            .get(&(node_id, ConnKey::Compat))
            .expect("compat connection cached")
            .alpn()
            .to_vec()
    }

    // Should: negotiate the head generation between two same-window peers
    // over a single compat connection, cached under the compat family.
    #[tokio::test(flavor = "multi_thread")]
    async fn compat_dial_negotiates_head_between_matched_pair() {
        let calls = Arc::new(AtomicUsize::new(0));
        let a = bind_for_test(Some(MAGIC), CODE, 1).await;
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        b.start(compat_echo(&calls));
        a.start(compat_echo(&Arc::new(AtomicUsize::new(0))));
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();

        let reply = a
            .rpc(&peer_b, "st", b"pong".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(reply, b"pong");
        assert_eq!(
            negotiated_compat_alpn(&a, 2).await,
            alpn::compat_alpn(&MAGIC, 1)
        );
    }

    // Impact: the mint lifecycle depends on this — a straggler one
    // generation behind must keep talking through the head's adapter.
    // Should: negotiate the previous generation when the dialer's window
    // trails the server's, in a single connect (no second dial), and
    // dispatch to the PREVIOUS-generation handler.
    // Should not: invoke the head handler for a floor-negotiated stream.
    #[tokio::test(flavor = "multi_thread")]
    async fn mixed_window_pair_negotiates_previous_generation() {
        let head_calls = Arc::new(AtomicUsize::new(0));
        let prev_calls = Arc::new(AtomicUsize::new(0));
        let a = bind_for_test(Some(MAGIC), CODE, 1).await; // offers [1, 0]
        let b = bind_for_test(Some(MAGIC), CODE, 2).await; // serves [2, 1]
        b.start(compat_echo_windowed(&head_calls, &prev_calls));
        a.start(compat_echo(&Arc::new(AtomicUsize::new(0))));
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();

        let reply = a
            .rpc(&peer_b, "st", b"old".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(reply, b"old");
        assert_eq!(
            negotiated_compat_alpn(&a, 2).await,
            alpn::compat_alpn(&MAGIC, 1)
        );
        assert_eq!(prev_calls.load(Ordering::SeqCst), 1);
        assert_eq!(head_calls.load(Ordering::SeqCst), 0);
    }

    // Impact: this seam is what makes a mint non-breaking for in-window
    // stragglers — the floor generation gets its own codec path.
    // Should: route a floor-negotiated connection's streams to the prev
    // handler and a head-negotiated connection's to the head handler.
    #[tokio::test(flavor = "multi_thread")]
    async fn generation_dispatch_selects_the_floor_handler() {
        let head_calls = Arc::new(AtomicUsize::new(0));
        let prev_calls = Arc::new(AtomicUsize::new(0));
        // Same window on both sides: negotiation lands on head.
        let a = bind_for_test(Some(MAGIC), CODE, 2).await;
        let b = bind_for_test(Some(MAGIC), CODE, 2).await;
        b.start(compat_echo_windowed(&head_calls, &prev_calls));
        a.start(compat_echo(&Arc::new(AtomicUsize::new(0))));
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();
        a.rpc(&peer_b, "st", b"x".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(head_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prev_calls.load(Ordering::SeqCst), 0);
    }

    // Impact: the S5 bind gate — a fresh node with no adopted mesh
    // identity must be unreachable at TLS (a drive-by JoinDeliver cannot
    // reach any HopNet code) and structurally unable to dial; adoption
    // brings both up without a rebind.
    // Should: refuse every dial INTO a deferred endpoint, error every
    // dial FROM it with EndpointDeferred (no retry burn), and serve both
    // families normally after adopt_magic.
    #[tokio::test(flavor = "multi_thread")]
    async fn deferred_endpoint_is_tls_dead_until_adopted() {
        let calls = Arc::new(AtomicUsize::new(0));
        let a = bind_for_test(Some(MAGIC), CODE, 1).await;
        let b = bind_for_test(None, CODE, 1).await; // deferred
        b.start(compat_echo(&calls));
        a.start(compat_echo(&Arc::new(AtomicUsize::new(0))));
        let peer_a = PeerRef {
            node_id: 1,
            pubkey: a.local_pubkey(),
        };

        // Inbound to the deferred node: no negotiable protocol.
        assert!(
            a.connect_to_addr(2, loopback_addr(&b)).await.is_err(),
            "an enforcement dialer must not reach a deferred endpoint"
        );

        // Outbound from the deferred node: structurally impossible.
        match b.connect_to_addr(1, loopback_addr(&a)).await {
            Err(CommsError::Protocol(ProtocolError::EndpointDeferred)) => {}
            other => panic!("expected EndpointDeferred, got {other:?}"),
        }
        match b
            .rpc(&peer_a, "st", b"x".to_vec(), Duration::from_secs(2))
            .await
        {
            Err(CommsError::Protocol(ProtocolError::EndpointDeferred)) => {}
            other => panic!("expected EndpointDeferred, got {other:?}"),
        }

        // Adoption: same magic → both directions come up, no rebind.
        b.adopt_magic(MAGIC).await.unwrap();
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();
        let reply = a
            .rpc(&peer_b, "st", b"up".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(reply, b"up");
        b.connect_to_addr(1, loopback_addr(&a)).await.unwrap();
        b.rpc(&peer_a, "st", b"back".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
    }

    // Impact: the identity is settled for the process lifetime — a
    // conflicting second code must error (restart is the re-entry path),
    // while the orchestrator's idempotent re-POST must stay Ok.
    // Should: accept the same magic again and refuse a different one.
    #[tokio::test(flavor = "multi_thread")]
    async fn adopt_magic_is_once_only() {
        let b = bind_for_test(None, CODE, 1).await;
        b.adopt_magic(MAGIC).await.unwrap();
        b.adopt_magic(MAGIC).await.unwrap(); // idempotent
        match b.adopt_magic([0x01, 0x02, 0x03, 0x04]).await {
            Err(CommsError::Protocol(ProtocolError::ValueMismatch { field, .. })) => {
                assert_eq!(field, "mesh_magic");
            }
            other => panic!("expected ValueMismatch, got {other:?}"),
        }
        // Adoption on an already-Some (set-up) endpoint follows the same
        // rule: same magic Ok, different errors.
        let c = bind_for_test(Some(MAGIC), CODE, 1).await;
        c.adopt_magic(MAGIC).await.unwrap();
        assert!(c.adopt_magic([0x0a; 4]).await.is_err());
    }

    // Impact: the Add Node probe's whole value (RFC-025 S5) — while
    // generation 0 is in-window, the compat ping falls through to the
    // magic-less legacy string and cannot distinguish our mesh from a
    // wrong-code node; only the locked family proves same-mesh.
    // Should: answer a locked ping between matched peers and refuse one
    // from a different-magic dialer that the compat ping still answers.
    #[tokio::test(flavor = "multi_thread")]
    async fn locked_ping_proves_same_mesh_where_compat_cannot() {
        let a = bind_for_test(Some(MAGIC), CODE, 1).await;
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        let wrong = bind_for_test(Some([0x01, 0x02, 0x03, 0x04]), CODE, 1).await;
        b.start(ScopeRegistry::new());
        a.start(ScopeRegistry::new());
        wrong.start(ScopeRegistry::new());
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        let peer_wrong = PeerRef {
            node_id: 3,
            pubkey: wrong.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();
        a.connect_to_addr(3, loopback_addr(&wrong)).await.ok();

        assert!(a.ping_locked(&peer_b).await.is_ok());
        // The compat ping reaches the wrong-magic node via legacy…
        assert!(a.ping(&peer_wrong).await.is_ok());
        // …the locked ping refuses it.
        match a.ping_locked(&peer_wrong).await {
            Err(CommsError::Refused(crate::RefusalError::AlpnRejected)) => {}
            other => panic!("expected Refused(AlpnRejected), got {other:?}"),
        }
    }

    // Impact: regression guard for the S5 gate-2 cache bug — the Add
    // Node probe keys every candidate under node_id -1, and a cached
    // connection from the PREVIOUS candidate answered the ping for the
    // next one, "verifying" a wrong-code node with someone else's TLS.
    // Should: refuse a locked ping whose peer pubkey differs from the
    // cached connection's remote identity under the same node_id.
    #[tokio::test(flavor = "multi_thread")]
    async fn stale_cached_connection_never_answers_for_a_different_peer() {
        let a = bind_for_test(Some(MAGIC), CODE, 1).await;
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        let wrong = bind_for_test(Some([0x01, 0x02, 0x03, 0x04]), CODE, 1).await;
        b.start(ScopeRegistry::new());
        a.start(ScopeRegistry::new());
        wrong.start(ScopeRegistry::new());

        // Probe candidate one: dial + cache under -1.
        a.connect_to_addr(-1, loopback_addr(&b)).await.unwrap();
        let peer_b = PeerRef {
            node_id: -1,
            pubkey: b.local_pubkey(),
        };
        assert!(a.ping_locked(&peer_b).await.is_ok());

        // Probe candidate two under the SAME node_id: the hint moves to
        // the wrong-magic node (the locked pre-dial itself fails), but
        // the open cached connection to b is still keyed -1.
        let _ = a.connect_to_addr(-1, loopback_addr(&wrong)).await;
        let peer_wrong = PeerRef {
            node_id: -1,
            pubkey: wrong.local_pubkey(),
        };
        match a.ping_locked(&peer_wrong).await {
            Err(CommsError::Refused(crate::RefusalError::AlpnRejected)) => {}
            other => panic!("expected Refused(AlpnRejected), got {other:?}"),
        }
    }

    // Should: report the generation the negotiated ALPN commits both
    // sides to — head between matched enforcement peers, zero when the
    // compat dial fell through to the legacy string (mismatched magic:
    // the gen-0 cutover caveat).
    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_negotiated_reports_the_connection_generation() {
        let calls = Arc::new(AtomicUsize::new(0));

        // Matched enforcement pair → head.
        let a = bind_for_test(Some(MAGIC), CODE, 1).await;
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        b.start(compat_echo(&calls));
        a.start(compat_echo(&Arc::new(AtomicUsize::new(0))));
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();
        let (_, generation) = a
            .rpc_negotiated(&peer_b, "st", b"x".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(generation, 1);

        // A different-magic server: the compat offer's only mutual
        // protocol is the magic-less legacy string → generation 0.
        let c = bind_for_test(Some([0x01, 0x02, 0x03, 0x04]), CODE, 1).await;
        c.start(compat_echo(&calls));
        let peer_c = PeerRef {
            node_id: 3,
            pubkey: c.local_pubkey(),
        };
        a.connect_to_addr(3, loopback_addr(&c)).await.ok();
        let (_, generation) = a
            .rpc_negotiated(&peer_c, "st", b"x".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(generation, 0);
    }

    // Should: refuse a below-window dialer with the structured
    // CompatRetired naming the server's floor and version — regardless of
    // which of the racy reject surfaces (connect error, recorded close,
    // or first-use failure then redial) delivers it.
    #[tokio::test(flavor = "multi_thread")]
    async fn retired_dialer_gets_structured_compat_retired() {
        let calls = Arc::new(AtomicUsize::new(0));
        let a = bind_for_test(Some(MAGIC), CODE, 1).await; // offers [1, 0] — both retired
        let b = bind_for_test(Some(MAGIC), CODE, 3).await; // window [2, 3]
        b.start(compat_echo(&calls));
        a.start(compat_echo(&Arc::new(AtomicUsize::new(0))));
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        // Locked family still matches (same code) — record the addr hint.
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();

        // The hook reject is racy dial-side; a first attempt may surface
        // as a generic stream fault before the close lands. Retry briefly:
        // the classification must converge on the typed refusal.
        let mut refusal = None;
        for _ in 0..5 {
            match a
                .rpc(&peer_b, "st", b"x".to_vec(), Duration::from_secs(3))
                .await
            {
                Err(CommsError::Refused(r)) => {
                    refusal = Some(r);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                Ok(_) => panic!("retired dialer must not get service"),
            }
        }
        match refusal {
            Some(crate::RefusalError::CompatRetired {
                floor,
                node_version,
            }) => {
                assert_eq!(floor, 2, "reject must name the server's floor");
                assert_eq!(node_version, CODE);
            }
            other => panic!("expected CompatRetired, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    // Should: serve a locked scope between exact-code peers, and refuse a
    // one-release-newer dialer with the typed AlpnRejected.
    #[tokio::test(flavor = "multi_thread")]
    async fn locked_family_exact_match_accept_and_reject() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut scopes = ScopeRegistry::new();
        scopes.rpc(
            "lk",
            Arc::new(Echo {
                calls: calls.clone(),
            }),
        );
        let a = bind_for_test(Some(MAGIC), CODE, 1).await;
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        b.start(scopes);
        a.start(ScopeRegistry::new());
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();
        let reply = a
            .rpc(&peer_b, "lk", b"hi".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(reply, b"hi");

        // A skewed release: exact-match refuses at TLS.
        let c = bind_for_test(Some(MAGIC), CODE + 1, 1).await;
        c.start(ScopeRegistry::new());
        let result = c.connect_to_addr(3, loopback_addr(&b)).await;
        match result {
            Err(CommsError::Refused(crate::RefusalError::AlpnRejected)) => {}
            other => panic!("expected Refused(AlpnRejected), got {other:?}"),
        }
    }

    // Impact: the admissibility rule — compat connections must never carry
    // state-permuting traffic, whatever a peer's registry claims.
    // Should not: dispatch a locked-class scope arriving over a compat
    // connection; the dialer sees the unknown-scope surface, the handler
    // never runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn locked_scope_refused_on_compat_connection() {
        let calls = Arc::new(AtomicUsize::new(0));
        // B registers "lk" as LOCKED; A (mis)registers it as compat, so
        // A's dial rides the compat family.
        let mut b_scopes = ScopeRegistry::new();
        b_scopes.rpc(
            "lk",
            Arc::new(Echo {
                calls: calls.clone(),
            }),
        );
        let dummy: Arc<dyn RpcHandler> = Arc::new(Echo {
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let mut a_scopes = ScopeRegistry::new();
        a_scopes.rpc_compat("lk", dummy.clone(), dummy);
        let a = bind_for_test(Some(MAGIC), CODE, 1).await;
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        b.start(b_scopes);
        a.start(a_scopes);
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };
        a.connect_to_addr(2, loopback_addr(&b)).await.unwrap();

        let result = a
            .rpc(&peer_b, "lk", b"x".to_vec(), Duration::from_secs(3))
            .await;
        assert!(result.is_err(), "locked scope over compat must not serve");
        assert!(
            !matches!(result, Err(CommsError::Refused(_))),
            "stream drop is the unknown-scope surface, not a refusal"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    // Should: fail TLS negotiation for an ALPN outside the grammar, with
    // no scope handler ever invoked (foreign traffic exercises no HopNet
    // code).
    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_alpn_fails_tls() {
        let calls = Arc::new(AtomicUsize::new(0));
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        b.start(compat_echo(&calls));

        let foreign = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind()
            .await
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            foreign.connect(loopback_addr(&b), b"hopnet/9.9"),
        )
        .await;
        // Tri-modal reject surface: error, timeout, or a dead connection.
        if let Ok(Ok(conn)) = result {
            assert!(
                tokio::time::timeout(Duration::from_secs(3), conn.closed())
                    .await
                    .is_ok(),
                "foreign-ALPN connection must die"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    // Impact: pins the deliberate cutover caveat — the locked family is
    // magic-gated, but generation 0 is the magic-less legacy string, so
    // cross-mesh protection cannot cover legacy-compatible compat dials
    // until the first real mint retires it (wire.md).
    // Should: refuse a mismatched-magic dialer on the locked family with
    // AlpnRejected.
    // Should not: refuse its compat dial while generation 0 is in-window —
    // it negotiates the legacy string by design.
    #[tokio::test(flavor = "multi_thread")]
    async fn magic_mismatch_locked_rejected_compat_falls_to_legacy() {
        let calls = Arc::new(AtomicUsize::new(0));
        let a = bind_for_test(Some([0x01, 0x02, 0x03, 0x04]), CODE, 1).await;
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        b.start(compat_echo(&calls));
        a.start(compat_echo(&Arc::new(AtomicUsize::new(0))));
        let peer_b = PeerRef {
            node_id: 2,
            pubkey: b.local_pubkey(),
        };

        // Locked dial: no mutual ALPN (different magic) — typed refusal.
        // The addr hint is recorded before the dial, so the compat dial
        // below still knows where B lives.
        let result = a.connect_to_addr(2, loopback_addr(&b)).await;
        match result {
            Err(CommsError::Refused(crate::RefusalError::AlpnRejected)) => {}
            other => panic!("expected Refused(AlpnRejected), got {other:?}"),
        }

        let reply = a
            .rpc(&peer_b, "st", b"legacy".to_vec(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(reply, b"legacy");
        assert_eq!(
            negotiated_compat_alpn(&a, 2).await,
            alpn::LEGACY_ALPN.to_vec()
        );
    }

    // Impact: cutover service for real pre-enforcement stragglers — an
    // enforcement node must still answer a RAW legacy dialer (the old
    // binary's exact ALPN) on the compat class while generation 0 is
    // in-window.
    // Should: complete TLS on the bare legacy string against a
    // magic-Some server.
    #[tokio::test(flavor = "multi_thread")]
    async fn raw_legacy_dialer_negotiates_generation_zero() {
        let calls = Arc::new(AtomicUsize::new(0));
        let b = bind_for_test(Some(MAGIC), CODE, 1).await;
        b.start(compat_echo(&calls));

        // A legacy binary impersonated at the transport level: raw iroh
        // endpoint, the legacy ALPN, no magic knowledge.
        let legacy = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind()
            .await
            .unwrap();
        let conn = tokio::time::timeout(
            Duration::from_secs(5),
            legacy.connect(loopback_addr(&b), alpn::LEGACY_ALPN),
        )
        .await
        .expect("legacy dial timed out")
        .expect("legacy dial refused");
        assert_eq!(conn.alpn(), alpn::LEGACY_ALPN);
    }
}
