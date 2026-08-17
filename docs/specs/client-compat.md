# RFC-023: Client API Compatibility — CalVer Identity and Session-Time Skew Enforcement

**Status**: Draft (2026-08-16)
**Depends on**: RFC-012 (device-token session establishment — the
handshake this rides), RFC-016 (projection registry — where client
surfaces are declared), RFC-018 (hopnet-mount — the first enforcing
client)
**Amends**: RFC-016 (`Projection` grows `min_client()`); RFC-018
(connect behaviour gains the version check); RFC-012 (session
establishment carries versions)
**Related**: RFC-020 (module versioning — the replay axis, fenced off
in Non-Goals); RFC-021 (the node upgrade channel, deliberately
untouched); RFC-011 (photos — the second client surface, arriving);
RFC-024 (the mount auto-upgrade wrapper consuming this RFC's signals)

## Motivation

The workspace manifest calls its version "the SINGLE authoritative
version token" (`Cargo.toml:15`), and for nodes that is true: release
tags are `v{version}`, nodes attest it into committed state, and the
epoch boot gate compares it by exact equality. Side binaries ship on
their own lifecycle and send nothing about their compatibility across
the wire: a client running arbitrarily old code is invisible to the
node it talks to and to the operator.

The fix is not to force clients into the node's upgrade channel,
because node↔node and client↔node compatibility follow different
rules:

- **Node ↔ node.** Every node replays the same block history through
  the same transition function, so any change to that function — even
  a bug fix — changes the result (RFC-020). Two node versions are
  either identical or incompatible; a version range is meaningless
  here, and the epoch boundary rightly compares by exact equality.
- **Client ↔ node.** The client-facing HTTP surfaces
  (`/api/integrations/*`, RFC-018) evolve additively: an old client
  keeps working until an endpoint it uses is removed or changed. A
  range of supported versions is the honest description.

The design follows directly. Every binary in the monorepo carries the
same CalVer version, which says one thing only: which release tag it
was built from. Separately, each client-facing surface declares the
oldest client version it still supports. Nodes keep comparing versions
by equality; client surfaces compare against that range.

Concretely: clients adopt the workspace CalVer; projections declare
supported client ranges in their RFC-016 manifest; session
establishment exchanges versions and both sides enforce, fail-closed;
client versions surface wherever devices are listed. Client binary
*rollout* stays out of scope (Non-Goals). Timing is deliberate:
photos (RFC-011) arrives as the second client surface soon, and
per-surface declaration in the manifest means it inherits the
mechanism on day one.

## Identity: one CalVer

The root manifest gains `[workspace.package]` carrying the CalVer
version; client crates inherit it (`version.workspace = true`). That
kills `hopnet-mount`'s `0.1.0` — a default nobody chose, with nothing
depending on it. Every client binary answers `--version` with its
compile-time token, the same rule RFC-021's seeding comparisons rely
on. Non-cargo clients (the signed macOS app) already build from the
same `v*` tag; they stamp the same token. A client version thereby
names exactly one thing — the release tag its API expectations came
from — which is what makes "oldest supported client" well-defined in
the next section.

## Declaration: the manifest

Compatibility is declared where surfaces are declared (RFC-016), at
two levels — a projection-wide default plus a per-mount override:

```rust
// hopnet-projection
pub trait Projection: Send + Sync {
    // ...existing methods (RFC-016/017/019)...
    /// Oldest client version code every surface of this projection
    /// supports. The coverage backstop: one line versions the whole
    /// projection. None = no declaration.
    fn min_client(&self) -> Option<u32> { None }
}

pub struct Mount {
    pub prefix: &'static str,
    pub auth: AuthClass,
    pub router: axum::Router,
    /// Per-surface override of the projection-wide minimum.
    pub min_client: Option<u32>,
}
```

Values are the RFC-019 numeric codes (`year*10000 + month*100 +
counter`, `src/version.rs`) — integer order IS calendar order, so
enforcement is one comparison. The parse/format helpers hoist into
`hopnet-common` so hopnet-projection and client crates can name them;
the host crate sits above both and cannot be depended on (RFC-016
layering).

**Resolution happens at declaration, never on the wire:**

- Effective minimum for a surface: the mount's `min_client` if set,
  else the projection's.
- A `DeviceToken` mount resolving to neither is a boot-tripwire
  failure — versioning is a precondition of the auth class, not an
  option. As built (S2): a sibling assertion
  (`projections::assert_client_compat_coverage`) that runs right
  after capabilities construction, because the RFC-015 tripwire runs
  before AppState exists and cannot walk `mounts()`. Host-owned
  DeviceToken surfaces (RFC-018 S8 statfs, the photos client
  dispatch) cannot escape coverage just because no manifest declares
  them: they carry named minimums in
  `projections::HOST_DEVICE_TOKEN_MIN_CLIENT` — the
  `storage_host::TX_FUNCTIONS` precedent.
- The host flattens the result into a surface → minimum map at boot,
  beside the existing registry loops. Clients only ever see resolved
  values: declaration is hierarchical for coverage and devex,
  negotiation stays flat.

Two levels exist because one projection's surfaces genuinely diverge.
Drive today: fileprovider/documentprovider are frozen (RFC-018) while
`/api/integrations/mount` is "tightly coupled to this consumer as
needs evolve". Photos tomorrow: a generic CRUD surface consumed by
every app wants a conservative floor, while the Apple-ingress surface
has exactly one consumer and can raise its minimum aggressively. The
projection default keeps every surface covered by at least SOME
declaration; the override prices each surface honestly.

Only `AuthClass::DeviceToken` mounts participate. `UserJwt` surfaces
serve the SPA, which the node itself delivers — it cannot skew.

## Negotiation: the wire

There is no session-establishment event to negotiate at: device-token
auth is per-request middleware (`src/devices/auth.rs:33`), and the
node-side "session" is a lazily-bootstrapped key cache the client
never observes. Negotiation is therefore distributed:

- **Identity crosses the wire** — each side advertises its own
  version.
- **Policy is applied at each end** — each side decides which
  versions it accepts.

**Client → node.** Every request to a `DeviceToken` surface carries
the client's identity as a header:

```
x-hopnet-client-version: 20260802
```

- A thin layer on every `DeviceToken` mount compares it against the
  surface's resolved minimum — one integer comparison, no I/O.
- Missing or malformed headers are REJECTED like failing ones:
  device tokens exist for separate-lifecycle binaries, so an
  unversioned request on this auth class is exactly the invisible
  skew this RFC exists to kill.
- Rejection is `426 Upgrade Required`, JSON body naming the surface,
  the minimum, and the node's version code — distinct from 401/403
  so a version rejection never reads as a credential problem.

**The probe.** Every `DeviceToken` surface MUST expose
`GET {prefix}/health` (the RFC-018 mount and fileprovider contract,
made mandatory; a registry test walks the mounts and expects it to
answer). The version layer covers it like any route, which makes it
the consumer-lifecycle anchor. As built (S3): probes stay HOST-side
and unauthenticated (RFC-018's own constraint — auth wraps whole
routers, so health cannot live inside a DeviceToken mount) but each
health route is individually version-gated with its surface's
resolved minimum; documentprovider and `/photos/client` gained the
probes they lacked, and statfs — one route, same consumer — is
probed via the mount surface's health. The pinned surface→probe
pairing lives in the `client_compat` registry test.

- On startup and on watch-reconnect, the client probes health with
  its header. One round trip settles both policies before any user
  action:
  - `426` → the client's standardized upgrade-required handler
    (surface "client too old" loudly; never generic EIO).
  - `200` → the body carries the node's version code; the client
    checks it against its compiled-in `min_node` (the oldest node
    release providing every endpoint this build calls) and refuses
    with a named error if the node is too old.
- Mid-operation 426 (a node upgraded underneath a running client)
  routes to the same handler.

**Deployment.** Mandatory rejection couples the enforcement release
to client updates — accepted deliberately:

- The enforcement release also teaches every in-repo client (mount,
  fileprovider appex, documentprovider, photo ingress) to send the
  header and handle 426; initial minimums for all surfaces are that
  same release.
- Pre-header clients get a clean 426, strictly better than the
  silent skew they exhibit today.
- Clients on managed upgrade channels (the mount is expected to
  inherit the node's nix auto-upgrade infrastructure eventually)
  make the coupling invisible in practice.

## Visibility

Mandatory 426 already removed the dangerous half of the gap —
staleness cannot be silent. Visibility is the convenience half, and
it is deliberately staged to avoid a replicated schema change today:

**v1 — none.** Enforcement is stateless: the header is parsed on
consumption, compared, and discarded. The stale client's user is
told directly by the standardized 426 handler; the node retains
nothing.

**v2 — replicated on change (deferred).**

- One nullable `client_version` column on device metadata; the node
  submits a tx only when an observed version differs from the
  committed one — once per client upgrade, so consensus traffic is
  negligible. Every node's device listing then shows fleet-wide
  client staleness.
- The dependency is precise, and much smaller than RFC-020:
  additive nullable columns already cross an upgrade boundary
  structurally free — RFC-019 Gate 3 rebuilds a FRESH database via
  `initialize` and imports the sealed artifact, so imported rows
  take the DDL default. What is missing is format tolerance:
  post-import section verification must compare in the ARTIFACT's
  format version, and importers must accept format N−1
  (`src/regenesis/boot.rs` currently treats a section format
  mismatch as fatal). v2 waits on exactly that, nothing more.

Throughout: versions are self-reported. This mechanism is
cooperative skew protection, NOT authorization — nothing may ever
lean on a version claim as a security boundary.

## Non-Goals

Each tracked, not forgotten:

- **Replay/module versioning** — RFC-020's axis entirely.
  `min_client` never gates consensus; node↔node comparison stays
  exact-match. The two kinds of version share the manifest and
  nothing else.
- **Client binary rollout** — how a stale client gets new bytes
  (nix auto-upgrade inheritance for the mount, home-manager, app
  updates). RFC-021 defers non-node deployment classes behind the
  `UpgradeProvider` seam; a client channel is its own RFC. The
  coupling contract that RFC should build against is sketched below
  (Deferred: rollout coupling) — S4 ships the machine-consumable
  signal it hooks.
- **v2 visibility** — the replicated `client_version` column,
  deferred above behind snapshot format tolerance (its named,
  minimal dependency).
- **UserJwt surfaces** — served by the node itself; they cannot
  skew, so they carry no declarations.
- **Capability negotiation** — minimums are a floor, not discovery.
  A client MUST NOT branch on the node's advertised version to
  select endpoints ("if node ≥ X use the new route"); if a client
  needs an endpoint, it raises its `min_node`. This keeps the
  header from mutating into ad-hoc capability sniffing.

## Deferred: rollout coupling (informative)

**Discharged by [RFC-024](mount-upgrade.md)** (2026-08-16), which
specifies the wrapper against exactly this contract.

How the mount's future auto-upgrade should consume this RFC's
signals — recorded so the client-channel RFC inherits the contract
instead of re-deriving it. The shape is RFC-021's
filesystem-plus-exit-code coupling, minus the epoch gate, plus one
client-specific obligation: the wrapper is version-AWARE, not
newest-wins.

- **The wrapper targets compatibility, not recency.** Its job is to
  provide a mount binary compatible with the node the user points it
  at: the newest release whose own `min_node` the target node
  satisfies AND whose version satisfies the node's `min_client` —
  the skew window, evaluated wrapper-side. Blind newest can
  overshoot a lagging node.
- **The node's half of the window is one unauthenticated probe.**
  A versioned health probe answers `node_version`; a header-less
  probe's 426 body hands over `min_client` + `node_version` together
  — a complete policy readout, no token required.
- **The candidate's half comes from building it.** A candidate
  release's own `min_node` is compiled into that release, so the
  wrapper builds the candidate (which staging requires anyway) and
  asks the staged binary to print its requirement — the honest-bytes
  route: the answer IS the bytes, and cannot drift the way a
  feed-published number could. An incompatible newest means checking
  the next-older tag.
- **426 from the daemon is the backstop, not the trigger.** Staging
  stays proactive on the release-feed timer (build → verify the
  staged `--version` answers the tag, the honest-bytes rule → atomic
  profile-symlink flip). A daemon 426 adds urgency, and its
  `min_client` doubles as a floor hint the wrapper may act on
  immediately rather than waiting for the next tick.
- **Escalation is safe by construction.** Lazy activation exists to
  protect open handles — but under a mid-operation 426 the surface
  is already refusing everything, and durable staging + startup
  orphan-upload recovery (RFC-018 S7) mean a restart loses no dirty
  bytes. Immediate restart beats limping.
- **Zero IPC.** ExecStart goes through a profile symlink (seeded
  newest-compatible by home-manager, mirroring
  `nix/hopnet-module.nix`); the wrapper is a subcommand on a systemd
  user timer whose ONLY output is the symlink flip; the daemon's
  ONLY output is exit 75 (`RestartForceExitStatus=75`), and only
  when the profile points at a version other than its own — a 426
  with nothing flipped holds and re-probes rather than exiting into
  the same binary. systemd is the sole coordinator.
- **A too-old NODE is not the wrapper's to force.** When the window
  is empty in the other direction (`min_node` unsatisfiable by the
  target node), the wrapper holds the newest COMPATIBLE binary
  rather than flipping past the node, and the daemon's named error
  points at the node.

## Implementation Slices

Each PR-sized, landing green:

- [x] S1 — identity (2026-08-16): `[workspace.package]` +
      `version.workspace = true` across all 10 member crates;
      pure version-code helpers hoisted to `hopnet-common::version`
      (node re-exports, call sites unchanged); `hopnet-mount
      --version` answers the token and `hopnet_mount::version_code()`
      is the compiled-in code S4's header sends. Nix parity: the
      mount derivation drops its hardcoded 0.1.0 for the parsed
      workspace version, with a flake assert guarding crane's silent
      0.0.1 placeholder fallback (which would otherwise quietly
      disable the module's newest-wins re-seeding).
- [x] S2 — declaration (2026-08-16): `Projection::min_client()`
      (default None, trait tail) + `Mount.min_client`; drive declares
      the projection-wide floor 20260802 (2026.8.2 — pre-flag-day,
      safe because header-less clients are rejected regardless), all
      five mounts inherit. Coverage tripwire landed as the post-caps
      sibling `assert_client_compat_coverage` (see Declaration), with
      host-owned DeviceToken surfaces covered via
      `HOST_DEVICE_TOKEN_MIN_CLIENT`; registry test
      `device_token_surfaces_all_declare_minimums` pins it in CI.
- [x] S3 — node enforcement (2026-08-16): `client_version_gate`
      wraps every `DeviceToken` surface (manifest mounts via the
      restructured loop, statfs and photos-client via their host
      table entries), gate OUTERMOST so 426 precedes auth; the 426
      body (`UpgradeRequiredResponse`) and header constant live in
      `hopnet_common::compat` and are typeshared. Health payloads
      carry `node_version` (`#[serde(default)]` — 0 = pre-RFC-023
      node); documentprovider + photos-client probes added. Pulled
      forward from S4 by necessity: the mount transport sends the
      header as a reqwest default (stack tests break otherwise) —
      S4 keeps the 426 handler UX, `min_node`, and the remaining
      clients. Registry + gate tests in `src/client_compat.rs`;
      end-to-end 426 shape asserted in the mount stack suite.
      *(RFC-024 S3 addition, 2026-08-16: `HOPNET_MIN_CLIENT_OVERRIDE`,
      a test-mode-only seam inside the gate — raises the effective
      minimum (never lowers: `max()` over the compiled value) and is
      advertised in the 426 body, so the mount-upgrade VM test can
      force the upgrade-required state against a single build.)*
- [x] S4 — clients + end-to-end (2026-08-16): mount gains the typed
      `TransportError::UpgradeRequired` (426 bodies parsed at every
      transport site), `MIN_NODE`/`check_node_version` at the
      preflight (both `mount` and `login`), and the watch loop's
      hold-not-spin handler (loud once, max-backoff re-probe, clears
      on reconnect); `health()` returns `HealthReport` carrying
      `node_version`. As-built notes: the appex identity is a
      generated Swift constant from the workspace version
      (00-generate script) with a configured URLSession default
      header and an `.upgradeRequired` ApiError case; the photo
      ingress publisher takes its identity from hopnet-common's own
      compiled token (excluded-workspace crates path-dep it from the
      same checkout) and classifies 426 as park; the
      orchestrator was itself a header-less client broken by S3 —
      its device-token calls now ride a shared `device_client()`.
      Orchestrator test `client-version-skew` covers header-less /
      stale / current probes plus the client-side min_node refusal
      via a node claiming 2020.1.1.
      *(Android parity addendum, 2026-08-17: the original S4 note
      called the Android documentprovider "a NO-OP (pure local mock,
      no HTTP)" — by then HopDrive was already a live pinned-HTTPS
      client, so S3 had silently broken it. Completed the Android
      leg: identity is a Gradle-derived `BuildConfig` code parsed
      from the workspace Cargo.toml at configure time and attached
      in the pinned-client interceptor (every request incl. the SSE
      watch stream); 426 bodies parse into a typed
      `UpgradeRequiredException` feeding a sticky `UpgradeState`
      (loud once per episode: banner in the app + one system
      notification, cleared by the next successful request); the
      watch loop parks at max backoff. E2E: `scripts/android/e2e.sh`
      boots a current node and a `HOPNET_MIN_CLIENT_OVERRIDE`-raised
      node and runs the instrumented `UpgradeRequiredTest` against
      both over pinned TLS from the emulator. `min_node` preflight
      parity is explicitly deferred — see hop-drive-android.md.)*

## Open Questions

1. **Snapshot format tolerance** — the v2 visibility dependency
   (import accepts format N−1, verification compares in the
   artifact's format). Pull it out of RFC-020 as a standalone
   near-term slice? It unlocks additive schema changes generally,
   not just this RFC.
2. **Grace-release mode** — a warn-only stance on missing headers
   (served, logged, flagged) as a one-release cushion. Pointless
   for a fleet whose clients the operator controls; likely needed
   before third-party deployments exist. Decide when that matters.
3. **macOS update prompt** — should the app's update flow surface
   the 426 payload's minimum directly as its "update required"
   prompt, closing the loop from rejection to remedy?
