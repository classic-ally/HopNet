# RFC-025: RPC Version Enforcement — ALPN Classes and Compat Generations

**Status**: Draft (2026-08-17)
**Depends on**: RFC-017 (hopnet-comms — the envelope, scope registry,
and fork reject hook this design gates with), RFC-019 (epoch
boundaries and the HANDSHAKE requirement this discharges; S7 staged
join is the cross-version consumer the compat class exists for),
RFC-023 (client-compat — the floor/probe/typed-error patterns
transposed here to the node↔node axis)
**Amends**: RFC-017 (the single static ALPN becomes the two-class
scheme; the envelope joins the compat generation contract), RFC-019
(the handshake as-built note — structured warn on mismatch — is
superseded by transport refusal)
**Related**: RFC-020 (module ordinals — the axis deliberately kept
off the wire, and the freeze-and-append discipline this RFC mirrors
for wire vocabulary; the crossing-advisory slice additionally waits
on its schema-evolution machinery for the attested-generation
column), RFC-024 (the hold-not-spin and named-error handling
precedents the straggler UX follows)
**Unblocks**: evolving RPC message schemas on a non-disposable mesh
(retires the "bincode positional wire compat broken — accepted, dev
meshes are disposable" caveat); mixed-version contact becoming a
diagnosable, typed state instead of silent misdecode

## Summary

Every iroh RPC scope is either version-locked or compat. Locked
scopes — everything that can permute state — ride an ALPN carrying
the mesh magic and the node's exact CalVer code, so mixed-version
delivery is structurally impossible and vocabulary evolves freely
each release. Compat scopes — status, regenesis fetch, ping: the
surfaces a straggler needs — ride a generation-numbered ALPN under
a fixed two-generation window; any vocabulary change mints a new
generation with an adapter for the previous one, CI-enforced
against the release tag. Refusals are typed, not silent: a
distinguishable TLS reject resolved against the status Pong names
version skew; retired generations get a structured reject naming
the floor; consensus liveness counts only what the transport
proves version-matched. The mesh magic doubles as the join code,
closing the drive-by JoinDeliver hole in the same stroke. The
enforcement release lands big-bang, serving `hopnet/1.0` as
generation 0 so pre-enforcement stragglers cross the cutover.

## Motivation

- **The handshake requirement is half-implemented.** RFC-019's
  HANDSHAKE gate says refuse mismatched (epoch, version) peers. As
  built: the status Ping/Pong carries `version_code` both ways, but
  the handler warns on epoch alone (`src/consensus/evidence.rs:328`)
  and the probe drops the peer's version unread
  (`src/consensus/evidence.rs:386`). Nothing refuses.
- **The transport carries no version signal.** One frozen ALPN
  literal, `hopnet/1.0`, never derived from the CalVer code
  (`hopnet-comms/src/iroh_impl.rs:69`); no version field in the
  envelope (`hopnet-comms/src/iroh_impl.rs:116`); payloads are
  positional untagged bincode (`src/net/mod.rs:13`). Any two
  releases connect.
- **Mixed-version failure is unnamed, three ways:**
  - unknown scope: stream dropped, the dialer sees a retryable
    transport fault (`hopnet-comms/src/iroh_impl.rs:682`)
  - undecodable payload: in-band error string, connection stays
    healthy, reachability evidence still recorded — an undecodable
    StatusRequest even answers a well-formed Pong
    (`src/consensus/evidence.rs:338`)
  - misdecodable payload — positional bincode reading reshaped
    fields as a different meaning — fails not at all; the sole
    tripwire is one same-build golden test
    (`hopnet-consensus/tests/codec.rs:218`, WireVote only)
- **RFC-020 removes the excuse.** Broken wire compat is recorded as
  accepted because "dev meshes are disposable" (RFC-019 as-built
  notes). The mesh is now non-disposable, and RFC-020 exists to let
  schemas evolve on it — every future change silently breaks any
  node not upgraded at that instant, until version skew is a
  refused, diagnosable state.

## The Model

- **Enforcement rides the release axis.** Of RFC-020's three axes,
  RPC compatibility is an availability question — can these two
  binaries exchange these bytes — and that is the CalVer release
  code. Module ordinals stay off the wire: in-tree modules make the
  ordinal vector a compile-time function of the release code, and
  ordinals do not cover RPC message schemas anyway (they version
  schema/section shape, not bincode structs). Epoch stays at the
  payload layer, where it is already enforced in structured form
  (signature domain, DecidedFetch's EpochMismatch).
- **Two ALPN classes, two compat rules.** Every scope is either
  *locked* — exact-match on the release code, the transport analogue
  of the epoch boot gate — or *compat* — served across versions
  under a windowed generation contract (§The Generation Contract).
  Locked vocabulary evolves freely with every release; compat
  vocabulary is the only wire surface that carries a compatibility
  burden.
- **Two invariants govern classification:**
  - *no cross-version state permutation* — any scope that can
    mutate replicated state, enqueue transactions, or participate
    in consensus is locked, without exception
  - *compat is an allowlist* — read-only is necessary but not
    sufficient; a scope earns the compat class only with a named
    cross-version consumer and a freezeable vocabulary, and its
    admission ships with the goldens that make the freeze real
- **Exact-match closes the boot gate's gap, and adds no supported
  constraint.** The epoch version gate is exact — but it only runs
  at a sealed boundary (`src/regenesis/boot.rs:321`); a normal boot
  checks nothing (`src/regenesis/boot.rs:296`), so a node restarted
  mid-epoch into a different release joins the mesh silently today
  (#58 adds the boot-time check as defence in depth). The supported
  upgrade flow never changes versions mid-epoch (RFC-021 stages,
  the boundary activates), so the locked class constrains no
  supported operation — it turns the unsupported path from silent
  hazard into refused, diagnosable state.

## The ALPN Scheme

- **Two ALPN families replace `hopnet/1.0`, both carrying the mesh
  magic:**
  - `hopnet/<magic>/v/<code>` — the locked class; `<code>` is the
    node's effective CalVer code (`effective_running_code()`, so
    the orchestrator's override seam works unchanged)
  - `hopnet/<magic>/compat/<G>` — the compat class; `<G>` is a
    small integer generation, minted only on breaking vocabulary
    change (§The Generation Contract), unrelated to release cadence
  - `<magic>` is the 4-byte truncation of the anchor chain id —
    the mesh's permanent epoch-1 identity, NOT the per-epoch
    chain id, which would lock stragglers out of the compat class
    at exactly the boundary it exists for
- **The magic is defence in depth, not authorization.** ALPN is
  observable on the wire; it prevents cross-mesh accidents and
  keeps scanner traffic from reaching any HopNet code, and nothing
  may lean on it as a security boundary — identity stays the pubkey
  directory (RFC-017), trust stays signatures and lineage.
- **The magic doubles as the join code.** A joining node binds no
  endpoint until the operator enters the mesh code (`XXXX-XXXX`,
  the magic in hex) at Join Network; it then binds the ordinary
  families, so `setup` needs no special ALPN and a drive-by
  JoinDeliver cannot complete TLS — an attacker needs the address,
  the pubkey, AND an online brute-force of the code inside the
  setup window. After genesis install the node verifies the
  installed anchor matches the entered code: at this size an
  additional layer between something you know (the code) and
  something you trust (the Add Node dialog runs on a device
  already trusted not to serve adversarial state), not a
  cryptographic commitment. Join authorization proper stays with
  the JoinInfo trust layers, out of scope here.
- **Accept is three tiers:**
  - *served* — the locked family at the node's own code, plus
    compat generations `[floor..head]`; dispatched normally
  - *retired* — compat generations below the floor (derived from
    the head constant, §Evolution — never a maintained list);
    TLS-accepted solely to deliver a structured reject through the
    fork's registration hook (`COMPAT_RETIRED`, reason bytes naming
    the floor and the node's version — the 426-body analogue)
  - *unknown* — everything else; fails TLS negotiation
- **Version only — epoch stays out of the string.** The locked
  code is compile-time and the magic is settled at join, so every
  ALPN is known at bind with no committed state read; epoch would
  break that (a boundary changes it mid-lifetime) for no gain —
  cross-epoch refusal is already structured at the payload layer,
  and the boot gate couples version to epoch among honest live
  nodes anyway.

## Scope Classes

- **The classification**, every scope named — the registry pin test
  fails on any scope missing from it:
  - **locked:**
    - `consensus` — votes, proposals, block sync; state permutation
    - `txforward` — enqueues transactions; state permutation
    - `setup` — delivers JoinInfo; state permutation, and the join
      ceremony is deliberately version-exact
    - `storage` — fragment placement and transfer; state permutation
    - `metrics` — read-only, but an evolving vocabulary with no
      cross-version consumer: freezing it buys nothing
  - **compat:**
    - `status` — named consumer: diagnosing any mismatched peer
      (§Rejection & Diagnosability); its Ping/Pong vocabulary
      freezes as generation-1 content at the enforcement release
    - `regenesis` — named consumer: RFC-019 S7 stragglers staging
      while still running the old binary
    - `ping` — comms-internal raw nonce echo; nothing to freeze
- **Boundary behaviour: the probe is the guarantee, the defuser is
  the fast path.** A straggler mid-sync when the mesh crosses a
  version-changing boundary can no longer reach the consensus
  scope, so it never sees the in-band EpochMismatch signpost
  (`src/consensus/malachite/gossip.rs:49`) — its dials now fail as
  `AlpnRejected`, which the defuser resolves against the peer's
  Pong into a typed "epoch/version ahead" signal
  (§Rejection & Diagnosability), pivoting it into the epoch join
  as fast as the signpost did.
  - *the probe is the backstop for idle nodes* — the defuser only
    fires on an active dial; a lagging node with nothing to dial is
    found by the status prober instead, which pings every known
    peer ~1s and triggers the epoch join on any Pong from a newer
    epoch (`src/consensus/evidence.rs:434`). That arm is already
    hardened to work when every participation-dependent signal has
    died — the voted-out-validator incident recorded at
    `src/consensus/evidence.rs:420`
  - *the EpochMismatch signpost stays load-bearing* — a boundary
    that does NOT change the release (config or membership
    regenesis) keeps the locked ALPNs identical, so the straggler
    still reaches the consensus scope and gets the in-band answer
    exactly as today. The signpost is unused only across
    version-changing boundaries; do not remove it as dead code
- **What freezes is what the OLD binary must understand — not just
  the wire types.** Some compat responses carry opaque byte blobs
  whose real format lives elsewhere; the freeze follows who parses
  them:
  - *LineageRecords freeze.* `Lineage` responses are `Vec<Vec<u8>>`
    on the wire, but the straggler's old binary immediately parses
    each blob (`src/regenesis/rpc.rs:54`) to verify the chain hop
    by hop — so the LineageRecord encoding is generation content,
    golden-pinned, even though no wire type names it. Reshaping it
    strands every straggler while passing every wire-type golden
  - *the artifact does not.* The old binary never parses snapshot
    chunks — it hashes the assembled file against the verified
    record's snapshot_hash and stages it to disk; parsing happens
    post-upgrade under the new binary. Artifact format evolution
    stays RFC-020's business (ordinals, ARTIFACT_VERSION)

## The Generation Contract

- **The contract** — rules every compat generation must satisfy,
  numbered for citation:
  1. **Frozen once released.** A generation in a release tag is
     immutable: its envelope framing, wire types, and reached-into
     encodings (§Scope Classes) are byte-pinned by goldens and CI
     release-tag diffs. Evolution never edits G; it mints G+1.
  2. **Any vocabulary change mints G+1.** There is no
     within-generation evolution — additive or otherwise. CI
     enforces this without judgment: any diff in compat vocabulary
     against the latest release tag without a generation bump
     fails (§Validation & Tripwires). Both sides offer their whole
     window at the handshake and TLS ALPN negotiation selects the
     highest mutual generation, so no dialer ever guesses what a
     peer speaks.
  3. **The minting release retains the G-1 handler.** A mint ships
     the new head AND an adapter serving the previous generation —
     frozen copies of G-1's types plus conversions into the head
     handler (§Evolution) — or the mint is a breaking release for
     every straggler mid-stage. For a purely additive mint the
     adapter is near-identity; only reshapes make it do real work.
  4. **The window is [G-1, G], fixed — and CI-pinned.** One
     previous generation, always: mints are rare on this surface,
     nodes must upgrade promptly to stay in the mesh regardless,
     and the fresh join serves any current binary from nothing —
     deeper history is liveness theater. The window invariant is a
     CI gate, not a convention: a build that serves anything other
     than exactly [G-1, G] — a mint that forgot the adapter, a
     retirement that jumped early — fails (§Validation &
     Tripwires). Below the window is the retired tier's structured
     reject (§The ALPN Scheme).
  5. **Retirement deletes.** When G+1 mints, G-1's frozen modules,
     adapter, and goldens are removed; the retired set needs no
     bookkeeping — it is every generation below the window, derived
     from the head constant (§Evolution). History survives in git
     and release tags, not in the binary.

## Evolution

- **Generations are absolute, contiguous integers; the head
  constant is the mint.** `COMPAT_HEAD` names the current
  generation; the window (`COMPAT_HEAD - 1 ..= COMPAT_HEAD`), the
  ALPN strings, and the retired set (everything below the window —
  derived, never maintained) all follow from it. A mint is: bump
  the constant, add the new head module, keep the previous one.
  Generation numbering starts at 1; 0 is reserved to mean
  "pre-enforcement" in diagnostics.
- **A mint creates a generation module; the adapter shims, never
  forks.** The outgoing head's types stay in their absolute module
  (`compat_g4.rs`, never renamed) beside the vocabulary owner,
  byte-pinned by the goldens that pinned them as head. The core
  handler speaks head types only; the previous-generation handler
  is a pure codec adapter — decode the old request, convert, call
  the core, convert back, encode the old response. A head semantic
  the old response vocabulary cannot express must surface at the
  adapter as the old vocabulary's error verb — compile-time
  friction that forces the "does this strand old peers?" question
  at mint time, not in the field.
- **The mint lifecycle, three releases wide at most:**
  - *release R mints G+1* — head module added, G handler becomes
    the adapter, `COMPAT_HEAD` bumped; stragglers on G notice
    nothing
  - *the mesh crosses to R* at the next boundary — the locked
    class's exact-match makes the fleet's dual-serve complete
    immediately
  - *a later release R' mints G+2* — G's module and adapter are
    deleted; anyone still dialing G gets the retired tier's
    structured reject
- **Cutover: `hopnet/1.0` is generation 0.** The enforcement
  release serves the legacy ALPN as the initial previous
  generation — compat scopes only, legacy envelope semantics
  unchanged — so a pre-enforcement straggler still stages across
  the enforcement boundary. The first real mint retires it through
  the ordinary window slide; no special case survives.
- **Crossing-time advisory, not a gate.** The consensus
  precondition for an upgrade regenesis covers seated validators
  only; registered-but-dark nodes (pool, storage-only, powered
  off) are outside the agreement, and a node dark across TWO mints
  falls below the window — fresh join is its only way back. The
  upgrade advisory therefore names every registered node whose
  last-known generation would fall below the target release's
  window, while the cheap remedy (bring it up before the next
  crossing) still exists. Never a hard gate: a permanently-dead
  node must not veto upgrades, and a dark node's attestation is
  stale by definition.
  - *the generation is attested, never inferred* — the
    NodeStagedVersion attestation (`src/db/versions.rs`) grows the
    node's `COMPAT_HEAD`; committed state then records what each
    node actually speaks, surviving the node going dark. No
    release→generation mapping exists to fall out of sync
  - *this slice waits on RFC-020* — the `nodes` column is a
    replicated schema change, exactly the class RFC-023 deferred
    its v2 visibility for. Enforcement, defusing, and probes are
    live-wire mechanisms with no schema change: everything but
    this advisory lands independently

## Rejection & Diagnosability

- **Two typed refusals replace the generic transport fault**, both
  first-class `hopnet_comms` errors, neither retryable:
  - `AlpnRejected` — the peer completed TLS but refused every
    offered protocol (a `no_application_protocol` alert,
    distinguishable from timeouts and resets). Meaning: alive,
    reachable, and either version-mismatched on the locked family
    or a different mesh entirely — the defuser resolves which
  - `CompatRetired { floor, node_version }` — the retired tier's
    hook reject, parsed from the reason bytes: the peer names the
    oldest generation it still serves and its own version. Meaning:
    this node is below the window; self-staging is over and the
    remedy is a fresh join with a current release
- **The defuser turns `AlpnRejected` into a named answer.** Comms
  surfaces the typed error; the host resolves it against the peer's
  Pong — the evidence cache if fresh, one status probe otherwise:
  - *Pong answers, versions differ* — bubble a typed
    version-mismatch naming both codes (and the epoch, which
    pivots a straggler into the epoch join without waiting for the
    prober)
  - *Pong answers, versions match* — a genuine transport anomaly;
    fall back to today's retry-and-evict handling
  - *the compat dial fails too* — wrong magic or not a HopNet
    node; stays opaque by design (§The ALPN Scheme)
- **The Pong is the policy readout.** Generation-1 Ping/Pong
  carries `decided_height`, `epoch`, `version_code` (all already
  on the wire today) plus the served window `(floor, head)` — one
  round trip answers "can we talk, and if not, why not", the
  RFC-023 health-probe pattern on the node axis. `classify_pong`
  finally consumes the version it has been dropping
  (`src/consensus/evidence.rs:386`):
  - *EpochJoin* (peer epoch ahead) — unchanged, still first
    priority: a version mismatch at a boundary is the epoch join's
    business, not an operator's
  - *VersionSkew* (same epoch, different version) — new arm: an
    unsupported state that must SCREAM — structured error-level
    logging naming both codes, a persistent status-view banner,
    never a warn that scrolls away. Driving the operator from this
    signal to a remedy (a unified versioning UI, including
    refusing state-permuting client requests while behind) is
    deliberately out of scope — its own RFC (§Non-Goals)
  - *Stranded* (peer's window excludes every generation we speak)
    — new arm: named operator error, fresh join is the remedy
  - *KickSync / Nothing* — unchanged
- **Liveness and visibility split along the class line.** The
  consensus liveness clock (`last_contact`, the vote-out driver —
  `src/consensus/evidence.rs:189,650`) is refreshed by exactly two
  things: any locked-class exchange — version match already proven
  by the transport, no health check needed — and version-matched
  Pongs, the prober's guaranteed path. Everything else on the
  compat class updates a visibility timestamp only (last-seen, for
  the status view), never the vote-out clock:
  - a seated validator lagging at a boundary is chatty on the
    compat class (staging, probes) exactly while it cannot vote —
    counting that chatter as liveness would shield it from the
    vote-out it deserves. Compat chatter makes a peer visible;
    only the locked class makes it live
  - the undecodable-StatusRequest hole (`evidence.rs:338` — a
    malformed request still refreshing the clock) closes for free:
    status is compat-class, so it no longer touches the consensus
    clock at all
  - the regenesis scope's record_contact
    (`src/regenesis/rpc.rs:93`) moves to the visibility timestamp
    — a straggler fetching lineage is reachable, and the status
    view says so, without brightening its seat

## Placement

- **Comms is dumb about payloads and policy, authoritative about
  wire-protocol identity** — which it always was: the `hopnet/1.0`
  literal lives there today.
  - *comms owns*: `COMPAT_HEAD`, window and retired-set
    arithmetic, ALPN string construction, generation-keyed
    dispatch and connection cache, the hook reject, the typed
    errors
  - *the locked code self-derives*: the workspace-unified CalVer
    token (RFC-023 S1) means comms' own compile-time version IS
    the node's; the effective-code override seam hoists to
    hopnet-common so test mode behaves identically everywhere
  - *injected at construction*: the magic (runtime state — DB or
    join-code entry) and per-scope classes (registration)
  - *payloads stay out*: vocabulary types, adapters, and their
    goldens live with their owners — comms staying
    payload-agnostic is what lets hopnet-storage dep the
    zero-dependency face (RFC-017)
  - *CI centralizes accordingly*: every wire-mechanism invariant —
    window, string formats, retired arithmetic, envelope golden —
    is a unit test inside comms; vocabulary tripwires sit beside
    the types they pin; ONE host-side integration test ties the
    crates (frozen modules' generation labels match what comms
    serves) (§Validation & Tripwires)
- **The normative byte contract lives in `hopnet-comms/docs/`.**
  A wire document — ALPN string grammar, envelope byte layout,
  generation numbering, reject error codes, the frozen-vocabulary
  inventory per generation — is the authority this RFC cites
  rather than duplicates, the RFC-020 relationship to its chain
  files. It starts the in-crate docs pattern: the contract sits in
  the crate that enforces it and moves in the same PR as any
  change to it.

## RFC-017/019 Amendments

- **RFC-017, the single ALPN.** "Single ALPN (`hopnet/1.0`), all
  message types share the connection" is superseded by the
  two-family scheme; the envelope section gains a pointer to the
  wire document as the normative byte contract. The scope table
  gains the class column. The `before_registration` hook's
  vocabulary grows the `COMPAT_RETIRED` reject beside "unknown
  node".
- **RFC-019, the handshake as-built note.** "Structured warn on
  mismatch" is superseded: mismatched peers are refused at the
  transport (locked) or answered and classified (compat), and
  `version_code` is consumed, not dropped. The HANDSHAKE
  requirement's "refuse mismatched peers" is discharged — with the
  deliberate narrowing that epoch rides the payload layer, not the
  hello (§The ALPN Scheme).
- **RFC-019, the disposability caveat.** "Bincode positional wire
  compat with older binaries is broken by these fields — accepted,
  dev meshes are disposable" retires: locked vocabulary changes
  are structurally invisible cross-version, and compat vocabulary
  changes are governed by the generation contract.

## Failure Modes

Ordered by when they can occur:

- **A version-changing boundary crossing (routine).** Every
  not-yet-restarted node's locked dials fail as `AlpnRejected`
  while restarted peers come up on the new code; defusers name the
  skew, probes classify it, and the window closes as each node
  activates. No message is half-delivered: connections either
  never open or speak a matched codec end to end.
- **A straggler inside the supported generation window.** Locked
  dials refused, compat dials served; the epoch join stages over
  the compat class and the node parks awaiting its version
  (RFC-019 S7, unchanged).
- **A straggler below the supported generation window.**
  `CompatRetired` names the floor; the status view shows Stranded;
  the remedy is a fresh join with a current release. Data-safe —
  blobs and identity ride `this_node` and the join ceremony as
  today.
- **A wrong join code at setup.** The node binds a magic no
  coordinator speaks: the ceremony visibly times out, the operator
  re-enters the code. If a rogue JoinInfo slips through anyway,
  the anchor check aborts the join at install (§The ALPN Scheme).
- **A mint escapes CI without its adapter.** The served window
  collapses to [G, G] and in-window stragglers are rejected as if
  retired — wrong, but loud and typed, never a misdecode. Recovery
  is a patched release; the window invariant exists to make this
  unreachable (§Validation & Tripwires).
- **Foreign or scanner traffic.** Wrong magic, wrong ALPN: dies in
  TLS negotiation, exercising no HopNet code, deliberately opaque.

## Validation & Tripwires

CI, diffed against the latest release tag (the freeze boundary,
RFC-020's pattern):

- compat vocabulary changed without a `COMPAT_HEAD` bump — any
  byte-level diff in frozen-inventory types, judgment-free
- a bump without both modules: HEAD must carry exactly the head
  module and its predecessor's frozen module + adapter (the window
  invariant, contract rule 4)
- a released generation module edited: byte-identity required

Test-level gates:

- comms unit tests: window and retired-set arithmetic from
  `COMPAT_HEAD`, ALPN string grammar goldens, the envelope golden
- per-generation vocabulary goldens beside their owners — wire
  types AND reached-into encodings (LineageRecord, §Scope Classes)
- cross-generation roundtrip: every G-1 verb encoded with the
  frozen G-1 codec, served through the head binary's adapter, its
  response decoded back under the frozen G-1 decoder — the
  schema-evolution parity gate transposed to the wire
- the class pin: every registered scope names its class; the
  frozen-inventory list matches the compat registrations
- the cross-crate tie: frozen modules' generation labels match the
  window comms serves

Orchestrator gates:

- a mixed-version mesh (the version override seam): locked scopes
  refused with `AlpnRejected`, compat scopes answering, the status
  view naming VersionSkew — both directions
- a boundary crossing with a live straggler: stages over the
  compat class, parks, activates, rejoins — S7 end to end over
  enforced ALPNs
- a retired-generation dialer (the `iroh_reject_unknown` pattern):
  receives `CompatRetired` naming the floor
- the join ceremony: code entry binds the endpoint, a wrong code
  times out visibly, the anchor check aborts a mismatched install

## Non-Goals

Each tracked, not forgotten:

- **per-module ordinals on the wire** — in-tree modules make the
  ordinal vector a compile-time function of the release code
  (§The Model). REVISIT if modules ever pluginize: independently
  deployed modules break that function, and the ordinal vector
  becomes wire-relevant
- **epoch in the ALPN** — payload-layer enforcement is already
  structured where it matters; §The ALPN Scheme records the
  deliberate narrowing of RFC-019's (epoch, version) hello
- **capability negotiation** — RFC-023's clause, one axis down: no
  peer branches on another's advertised version or generation
- **a HELLO handshake or response status channel** — rejected
  design: it rebuilds what ALPN negotiation provides, imperatively
  and forgettably (§Rejection & Diagnosability)
- **a unified versioning UI** — driving operators from VersionSkew
  and Stranded to a remedy, including refusing state-permuting
  client requests while behind: its own RFC, touching every
  DeviceToken surface
- **JoinDeliver authorization** — who may invite a node stays with
  the JoinInfo trust layers; the join code narrows exposure
  (§The ALPN Scheme) but is not authorization

## Implementation Slices

One PR, built and reviewed as ordered stages (RFC-020's pattern):
each stage lands green on the branch, and the branch merges only
once S6 is complete. The release that follows the merge IS the
enforcement release — the ALPN swap severs pre-enforcement RPC by
design, so there is no incremental landing, and the mesh crosses
it as an ordinary upgrade regenesis. S-final trails independently
(blocked on RFC-020).

- [~] S1 — comms mechanism: the two ALPN families, magic
      injection, `COMPAT_HEAD` + window/retired arithmetic,
      generation-keyed dispatch and connection cache, typed
      `AlpnRejected`/`CompatRetired`, the retired reject tier, the
      effective-code seam hoisted to hopnet-common, the
      multi-generation ALPN offer (native,
      `ConnectOptions::with_additional_alpns` — §Settled
      Questions), comms unit gates + envelope golden, and the
      `hopnet-comms/docs/` wire document
- [ ] S2 — host wiring: class-explicit registration
      (`rpc`/`rpc_compat`), the §Scope Classes table enacted, the
      magic derived from the anchor at boot, the class pin test
- [ ] S3 — generation 1 freezes: the frozen inventory (status +
      regenesis vocabulary, LineageRecord), the Pong's window
      fields, per-type goldens, the cross-generation roundtrip
      harness, the release-tag CI tripwires, generation 0 served
      for cutover (§Evolution)
- [ ] S4 — diagnosability: the defuser, `classify_pong`'s
      VersionSkew/Stranded arms, the liveness/visibility split,
      status-view surfacing
- [ ] S5 — setup and join: the join-code entry gating endpoint
      bind (interactive and `HOPNET_JOIN_CODE`, §Settled
      Questions), JoinInfo carrying the anchor, the install-time
      anchor check, the ceremony orchestrator scenario (including
      the headless wrong-code case)
- [ ] S6 — orchestrator gates: the mixed-version mesh, the
      boundary crossing with a live straggler, the
      retired-generation dialer
- [!] S-final — crossing-time advisory: `COMPAT_HEAD` in the
      NodeStagedVersion attestation, the `nodes` column, and the
      advisory's below-window report. Blocked on RFC-020 (the
      replicated schema change; see §Evolution).

## Settled Questions

Both settled 2026-08-24, ahead of S1:

1. **The dial-side generation offer: native multi-ALPN.**
   Re-settled same day, superseding the two-attempt fallback: that
   choice rested on a false premise — the pinned fork already
   carries upstream's `ConnectOptions::with_additional_alpns`
   (`Endpoint::connect_with_opts`), so the native offer needs NO
   fork change either; the fallback's decisive criterion (patch
   surface stays at exactly the registration hook) is satisfied
   by both, and the fallback's costs (a second handshake per
   mid-transition dial and across the whole generation-0 cutover,
   plus disambiguating ALPN-reject from other dial failures
   before redialing) buy nothing. Compat dials offer
   `[head, head-1]` in one handshake; fork-pinned semantics
   (`connect_multiple_alpn_negotiated`): accept-side list order
   is preference order, dial-side order is irrelevant, the
   highest mutual generation is selected — contract rule 2
   holds literally. The negotiated generation is read from the
   connection. Locked dials stay single-ALPN (one code per
   node).
2. **The join code without a human: an env/config bootstrap
   channel into the same entry seam.** `HOPNET_JOIN_CODE` (env
   wins over the config-file key — the `HOPNET_DB_*` precedence)
   feeds the same code-entry seam as Join Network, read only in
   the pre-anchor state. The install-time anchor check runs
   unchanged, and after install the magic derives from the
   installed anchor only — a stale value left behind in config is
   inert, never a second identity source. A wrong code fails
   headless the way it fails interactively: ceremony timeout,
   error-level log, failed health. The orchestrator reads the
   code from the coordinator — the Add Node surface exposes it
   for humans anyway — and forwards it like `HOPNET_DB_*`. The
   code is deliberately not a secret (it rides every ALPN
   string), so plain config is fine; it IS the setup window's
   brute-force barrier, so keep it out of public repos.
