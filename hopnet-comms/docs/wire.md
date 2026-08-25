# hopnet-comms wire contract

This document is the normative byte contract for HopNet inter-node RPC
(RFC-025 cites it rather than duplicating it). It lives in the crate
that enforces it and moves in the same PR as any change to what it
describes. The enforcing code is `src/alpn.rs` (vocabulary) and
`src/iroh_impl.rs` (transport); every rule below is pinned by a unit
test there.

## ALPN grammar

```
alpn      = locked / compat / legacy
locked    = "hopnet/" magichex "/v/" code
compat    = "hopnet/" magichex "/compat/" generation
legacy    = "hopnet/1.0"
magichex  = 8 lowercase-hex-digits        ; the 4 magic bytes, in order
code      = canonical-decimal-u32         ; CalVer code, e.g. 20260806
generation = canonical-decimal-u32, >= 1  ; generation 0 has no string
```

- The magic is the 4-byte truncation of the anchor chain id — the
  mesh's permanent epoch-1 identity. The operator-facing MESH CODE is
  the same four bytes displayed `XXXX-XXXX` (uppercase); the wire
  grammar below is unchanged by it. A fresh node binds with an EMPTY
  serve list (TLS-dead) until the code is adopted (RFC-025 S5).
- `code` is the node's effective CalVer code
  (`hopnet_common::version::effective_running_code`).
- Canonical decimal: no leading zeros, no sign, fits u32. Parsing is
  strict — uppercase hex, non-canonical decimals, or trailing bytes make
  the string foreign.
- **Generation 0 IS the legacy literal** `hopnet/1.0`, carrying no
  magic: pre-enforcement dialers know none. Consequence (deliberate,
  cutover-scoped): cross-mesh accident protection does not cover
  legacy-compatible compat dials until the first real mint retires
  generation 0.

Worked examples for magic `9f 3a 01 cc`, code 20260806:

```
hopnet/9f3a01cc/v/20260806    locked
hopnet/9f3a01cc/compat/1      compat generation 1
hopnet/1.0                    compat generation 0 (legacy)
```

## Accept tiers and offer rules

A node's accept list, in order (ORDER IS TLS NEGOTIATION PREFERENCE):

1. its locked ALPN (exact code),
2. compat generations head down to the floor (`COMPAT_HEAD`, window
   `[head-1, head]`),
3. every retired generation (below the floor) — TLS-accepted solely so
   the registration hook can deliver the structured reject below.

Anything else fails TLS negotiation with `no_application_protocol`.

Dial side: locked scopes dial the single locked ALPN; compat scopes
offer `[head, head-1]` in one handshake
(`ConnectOptions::with_additional_alpns`) and the server's preference
order selects the highest mutual generation. The negotiated protocol is
read from the connection and is the codec authority for every stream on
it.

Scope admissibility per connection: a locked (or pre-enforcement)
connection serves every scope; a compat connection serves only
compat-class scopes plus the transport ping — a locked-class scope
arriving over compat is dropped exactly like an unknown scope.

## Generation numbering

Generations are absolute, contiguous integers starting at 1; 0 means
pre-enforcement (the legacy ALPN). `COMPAT_HEAD` (src/alpn.rs) is the
single source: the window, the offer, the ALPN strings, and the retired
set (everything below the window — derived, never maintained) all
follow from it. A mint bumps the constant, adds the new head module,
and keeps the previous generation's adapter (RFC-025 §The Generation
Contract).

## Envelope byte layout

```
request stream :  [8B request_id LE][1B scope_len][scope utf8][4B payload_len LE][payload]
response stream:  repeated frames of [4B len LE][bytes]     (rpc = exactly one frame)
```

Frame cap: 8 MiB (`MAX_MESSAGE_SIZE`). Golden (request_id
`0x0123456789abcdef`, scope `status`, payload_len 4):

```
ef cd ab 89 67 45 23 01 06 73 74 61 74 75 73 04 00 00 00
```

Payloads are opaque bytes; codecs belong to scope owners.

## Frozen vocabulary inventory

Payload codec everywhere: bincode 2 `config::standard()` (varint
little-endian, positional — field and variant order IS the wire).
Frozen modules live beside their owners, contain nothing but vocabulary
and goldens, and are byte-frozen by `scripts/check-compat-freeze.sh`
against the latest release tag; retirement deletes a module whole under
a `RETIRES: compat_g<N>` commit trailer. Adapters live beside handlers
in unfrozen files.

### Generation 1 (head)

| scope | types | module | goldens |
|-------|-------|--------|---------|
| status | `StatusRequest::Ping{decided_height, epoch, version_code}`, `StatusResponse::Pong{decided_height, epoch, version_code, floor, head}` | `src/consensus/status_compat_g1.rs` | in-module byte arrays |
| regenesis | `RegenesisNetRequest` (EpochInfo, LineageFetch, SnapshotInfo, SnapshotChunk), `RegenesisNetResponse` (EpochInfo, Lineage, SnapshotInfo, SnapshotChunk, NotAvailable, Error) + `LINEAGE_FETCH_MAX`, `SNAPSHOT_CHUNK_MAX` | `src/regenesis/compat_g1.rs` | in-module byte arrays + the LineageRecord closure hex |

Reached-into encodings (what the OLD binary parses — RFC-025 §Scope
Classes): `Lineage.records` blobs are on-disk `LineageRecord` bytes
(`src/regenesis/genesis.rs`), which the straggler decodes down through
`EpochGenesisRecord`, `Block`, the transaction closure,
`WireCommitCertificate`/`WireSig`, and the uuid/ed25519/bincode serde
behavior underneath — all transitively pinned by the closure golden in
`compat_g1.rs`. The snapshot artifact does NOT freeze (append + hash
only; format is RFC-020's `ARTIFACT_VERSION`).

### Generation 0 (pre-enforcement, in-window until the first mint)

Identical to generation 1 minus the Pong's `(floor, head)` window
fields. Status is served by the `StatusCompatG0` adapter (request bytes
pass through — the G0/G1 Pings are byte-identical — and the Pong drops
the window fields); regenesis is served by the head handler directly
(generations byte-identical; the equality goldens in
`src/regenesis/compat_g0.rs` are the license). Frozen shapes:
`src/consensus/status_compat_g0.rs`, `src/regenesis/compat_g0.rs`.
Pre-enforcement (magic-None) nodes serve generation-0 vocabulary on
compat scopes — the negotiated ALPN is the codec authority on every
pairing.

## Reject error-code registry

Application error codes on QUIC CONNECTION_CLOSE, sent by the
before-registration hook. Codes are never reused.

| code | name           | reason bytes                                  |
|------|----------------|-----------------------------------------------|
| 1    | UNKNOWN_NODE   | informal utf8 (`"unknown node"`, `"unknown alpn"`) |
| 2    | COMPAT_RETIRED | `[0x01][floor u32 LE][node_version u32 LE]` (9 bytes) |

- COMPAT_RETIRED reason bytes: tag `0x01`; an unknown tag or wrong
  length is treated as floor/version unknown (0) — the error code alone
  is authoritative for the refusal class.
- ALPN negotiation failure is not an application close: it surfaces as
  the TLS `no_application_protocol` alert, QUIC crypto transport error
  `0x178` (0x100 | alert 0x78).

### Reject surfacing (dial side)

A hook reject is racy from the dialer's view and may surface as any of:
a connect error, a successfully returned connection whose
`close_reason()` is already set, or a first-use stream failure followed
by an evict-and-redial that classifies on the second attempt. The
transport checks `close_reason()` once after every successful dial;
callers only ever see the typed `CommsError::Refused` — never a partial
exchange.

Host-side consumption (RFC-025 S4): `AlpnRejected` from a locked dial
is DEFUSED — resolved against the peer's latest Pong (cache or one
status probe) into version-skew, epoch-ahead, or transport-anomaly —
at the consensus sync driver and the transaction-forward path;
`CompatRetired` needs no probe (the reason bytes already name the
floor) and is logged at error level directly. Gossip broadcast has no
per-peer error channel by design; the ~1s status prober is the idle
backstop that names any mismatched peer within one probe cadence.

## Generation 0 service (cutover)

While `COMPAT_HEAD == 1`, generation 0 is in-window: the legacy ALPN is
served for compat-class scopes with legacy envelope semantics unchanged,
so pre-enforcement stragglers stage across the enforcement boundary.
The first mint (head 2) retires it through the ordinary window slide;
legacy dialers then receive COMPAT_RETIRED.
