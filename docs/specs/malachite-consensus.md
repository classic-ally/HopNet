# RFC-013: Malachite Consensus

Supersedes RFC-001 (`consensus-system.md`, retired). Branch: `consensus-malachite`.

## Overview

HopNet's consensus is built on **Malachite** (`arc-malachitebft-*` crates,
github.com/circlefin/malachite, Apache-2.0): a Quint-specified, model-based-tested
implementation of Tendermint (arXiv:1807.04938). HopNet consumes the tier-1
**effect API** (`core-consensus`): a pure, synchronous state machine with no I/O of
its own. Every side effect (networking, signing, timers, persistence, deciding) is
surfaced as a typed `Effect` the host handles inline.

### Why the migration

A 2026-07 audit of the bespoke engine (RFC-001) found it was structurally
"Tendermint minus the lock rule" and identified a safety hole: timeout-certificate
application cleared the prepared-block state and leaders always built fresh blocks,
so no vote rule pinned a locked replica to its Propose-QC'd block. A leader could
commit block B via Lock QC while the rest of the network TC'd past it and committed
B′ at the same height. The mitigations in place (post-TC GST wait, intra-view sync)
were timing-based, not safety rules. Rather than patch a bespoke protocol into a
correct Tendermint, HopNet adopted a formally specified one and kept the
application seams.

## Architecture

### Layering

```
┌─────────────────────────────────────────────────┐
│ main crate: src/consensus/                      │
│   malachite/engine.rs  spawn/wiring/driver      │
│   malachite/app.rs     HopNetApplication        │
│   malachite/gossip.rs  iroh publisher + accept  │
│   malachite/sync.rs    decided-value sync client│
│   queue.rs             PendingPool + forwarding │
├─────────────────────────────────────────────────┤
│ hopnet-consensus crate (workspace member)       │
│   host.rs    sans-io HostCore (effect routing)  │
│   shell.rs   tokio shell (feature "shell")      │
│   store.rs   SQLite WAL + decided history       │
│   context/signing/codec/verify/config/types     │
│   sim.rs     deterministic simulator + fuzzing  │
├─────────────────────────────────────────────────┤
│ arc-malachitebft-core-consensus (=0.7.0-pre)    │
│   Quint-verified Tendermint state machine       │
└─────────────────────────────────────────────────┘
```

The crate is transport- and pool-agnostic: the application plugs in behind four
sync trait seams (`Application`, `Storage`, `Gossip`, `Timers`). `HostCore`
performs no I/O and no awaiting — production wraps it in a tokio shell,
the simulator wraps it in a virtual-clock loop, and the fuzzer exercises the
same code that ships.

`HostCore` is `!Send` (the engine is a genawaiter generator), so the shell runs on
a dedicated thread with a `current_thread` tokio runtime. Consequence: everything
reachable from `Application::apply_block` (the transaction dispatch table) must be
shell-thread-safe — no `block_in_place`, no multi-thread-runtime-only APIs. The
shell `catch_unwind`s and aborts the process on panic so a dead consensus thread
can never leave a zombie node serving HTTP.

### One-transaction decide

Consensus state and application state live in the same SQLite database, and a
decide is ONE transaction: apply the block through the dispatch table, insert
into `decided_blocks` + `decided_certificates`, bump
`consensus_meta[last_decided_height]`, truncate the WAL at-or-below the height,
commit (through the app-provided commit callback, so commit latency lands in the
`/debug/db-stats` histogram). App state and decided history cannot diverge across
a crash. No awaits occur while the transaction is open.

### Proposals: PartsOnly + Rule-8 validation

`ValuePayload::PartsOnly` — the engine's own Proposal message never hits the wire.
The host publishes its own wire proposal carrying the full block; the RECEIVING
node validates it (signature checks, parent linkage, nonce dedup, dispatch-table
dry-run — "Rule 8": validate before voting) and feeds
`Input::ProposedValue(…, validity)` with its own verdict. This makes the
validation seam structurally un-bypassable: there is no code path by which a
received proposal reaches the engine pre-marked Valid. (The alternative,
`ProposalOnly`, hardcodes `Validity::Valid` upstream.)

`Application::validate_block` takes a `ValidationOrigin{Live, Sync}`: sync-time
validation skips time-dependent checks (transaction staleness, nonce-cleanup
windows — the verified commit certificate vouches for history), while
deterministic checks (signatures, parent linkage, handler dry-run) run for both.

### Proposer selection

`select_proposer = validators[(height + round) % n]` — deterministic round-robin,
reproducing both old rotation behaviors (advance per block, skip per timeout
round). Validator sets are per-height (`effective_height` machinery survives from
the old design) and sorted deterministically.

## Quorum profiles: two fault models, two theorems

The fault model is an explicit, genesis-fixed, per-mesh parameter
(`consensus_meta[quorum_profile]`, set from `HOPNET_QUORUM_PROFILE` at genesis).
It feeds the engine's `ThresholdParams` and every certificate verification.

**Theorem (Bft profile).** With `quorum > 2/3` (Tendermint defaults) and
n ≥ 3f+1 validators, safety and liveness hold with up to f Byzantine validators.
This is the upstream-verified Tendermint result; equivocation by tolerated
validators cannot violate agreement, because any two quorums intersect in at
least one honest validator.

**Theorem (Majority profile).** With `quorum > 1/2`, any two quorums intersect in
at least one validator. IF no validator equivocates (crash-fault assumption),
that intersection is sound and agreement holds with up to ⌈n/2⌉−1 crashed
validators. This profile is only sound where every node key is trusted — home
meshes where all machines share an owner. It gives a 2-node mesh decidability
(quorum 2 = both nodes) and a 3-node mesh crash tolerance of 1 (quorum 2).
Equivocation by a compromised key CAN violate agreement under this profile;
that is the explicit trade.

The `honest` threshold (skip-round trigger) is kept at majority in the Majority
profile — conservative; only affects liveness, never safety.

## On-demand heights

Malachite by default runs heights continuously (empty blocks or idle round
churn) — unacceptable for a home mesh. HopNet defers `StartHeight(h+1)` after a
decide (and at boot, when the pending height's WAL is empty) until work exists.

Wake rules (host code, not Quint-verified — covered by dedicated sim tests and
the `consensus-leader-down` orchestrator test):

1. A node with local work starts its pending height immediately, regardless of
   whether it is the proposer.
2. A paused node resumes on the first inbound wire message at the pending height
   (a message at a higher height triggers sync instead).

Boot rule: a non-empty WAL for the pending height means the height was active at
crash — start and replay immediately, never pause (pausing a height with WAL
state could suppress a recovery republish).

Safety is untouched (the gate defers only height START, never votes/rounds/WAL).
Within a started height, crash handling is unchanged verified Tendermint
(timeouts, skip rounds). Known cost: when the work-holder is not the round-0
proposer, the wake burns roughly one propose timeout before rotation.

## WAL format and crash recovery

```sql
CREATE TABLE consensus_wal (
    height      INTEGER NOT NULL,
    seq         INTEGER NOT NULL,
    entry_type  INTEGER NOT NULL,   -- 0 ConsensusMsg | 1 Timeout | 2 ProposedValue
    entry       BLOB NOT NULL,      -- bincode WireWalEntry (crate-owned mirror)
    PRIMARY KEY (height, seq)
);
```

Entries are crate-owned wire mirrors (bincode, pinned by golden-bytes tests) —
the malachite types never serialize directly. `wal_append` is a single
autocommit INSERT, as durable as the connection's `synchronous` pragma. The
engine publishes a message only AFTER its WAL entry is appended — that ordering
is the no-equivocation-across-crash guarantee, so the consensus connection must
not weaken `synchronous` below the deployment's crash model.

Replay contract (confirmed against the reference engine):
`start_height(h, is_restart=false)` feeds `StartHeight`, then replays stored
entries for `h` in seq order as ordinary inputs (Vote→`Input::Vote`,
Timeout→`Input::TimeoutElapsed`, ProposedValue→`Input::ProposedValue`).
`is_restart=true` RESETS the WAL (truncates, no replay) — used for height jumps
after sync. During replay the host is in a `Recovering` phase: WAL appends are
suppressed (replayed entries must not re-append) and value-build requests are
dropped; publishes are NOT suppressed (harmless republish, matches upstream).
Replayed Proposal/ProposedValue entries re-populate the host's block map — a
decide immediately after restart must find its block. A torn final WAL entry is
dropped; corruption mid-log is an error.

## Decided-value sync

The malachite `sync` crate is libp2p-flavored and unused; HopNet rolls its own
decided-block transfer over iroh (`src/consensus/malachite/sync.rs`).

- Client fetches `(block, certificate)` pairs in chunks of 50 via
  `IrohRequest::DecidedFetch{from,to}`, hint-peer first, rotating on failure.
- Structural checks client-side (height contiguity, block hash, cert/value-id
  match); CRYPTOGRAPHIC verification is the engine's
  (`VerifyCommitCertificate` against the validator set at that height — a lying
  peer cannot forge history without quorum keys).
- Values are fed as `Input::SyncValueResponse` at exactly the engine's current
  height; each decide runs the SAME one-transaction apply path as live
  consensus, auto-advancing the loop.
- The host drops sync values with `cert.height <` the engine's current height:
  a restarting node runs sync and live consensus concurrently, and live can
  decide a height before the in-flight sync value for it lands — re-applying
  would flip the app's verdict (nonces already committed) and wedge the engine.
  Ahead-of-height sync values are NOT dropped (the engine buffers them;
  chunked sync depends on it).
- Trigger: any wire message at a height above current buffers the message and
  fires a sync to that target. Exhausting all peers without reaching the target
  is an error, retried on the next trigger (the target may come from an
  in-flight vote for a height peers haven't decided yet).

**Joining-node bootstrap (trusted height 0).** A joiner has empty
validator/node tables — the engine cannot even compute a validator set. The
genesis (block, certificate) pair is fetched from a bootstrap validator and
accepted as TRUSTED: the synthetic genesis certificate carries no signatures
(there is no validator set before the genesis transaction creates one).
Structural checks only; the genesis transaction is applied directly, outside
the engine. Everything after height 0 is engine-verified against the validator
sets genesis establishes. The chain id (32-byte genesis block hash, in
`consensus_meta`) binds all signing payloads to the mesh — signatures cannot be
replayed across meshes.

## Persistence summary

| Table | Contents |
|---|---|
| `consensus_wal` | Per-height engine WAL (see above) |
| `decided_blocks` | height PK, block_hash UNIQUE, round, full bincode block |
| `decided_certificates` | Commit certificate per height (node-local quorum proof — vote subsets legitimately differ across nodes; excluded from divergence checks) |
| `consensus_meta` | `last_decided_height`, `chain_id`, `quorum_profile` |

The legacy `blocks` / `quorum_certificates` / `timeout_certificates` tables died
with the bespoke engine; `this_node` holds identity only. Divergence checking
compares `decided_blocks` (and app tables), not certificates/WAL/meta.

## Transport

Consensus messages ride the existing iroh RPC layer (ALPN `hopnet/1.0`, bincode,
fire-and-forget broadcast, spawn-per-peer, 3s timeout): `IrohRequest::ConsensusMsg`
(votes, liveness msgs, host wire proposals), `DecidedFetch` (sync), and
`TransactionForward` (two-phase ACK to the current proposer; `NotProposer{height,
round}` redirects). Timeouts are `LinearTimeouts`, scalable via
`HOPNET_CONSENSUS_TIMEOUT_MS`.

Non-proposer nodes forward queued transactions to the proposer from the shell's
`RoundInfo` watch (live round) or deterministic `select_proposer` over the pending
height while paused. Undeliverable batches resume the node's own engine (wake
rule 1 corollary).

## Testing

- **Crate units** (~52): OUR surface only — the engine is upstream-verified.
  Determinism, codec golden-bytes, signing injectivity (domain-separated,
  chain-id-bound payloads), certificate verification (dedup, forged sigs,
  sub-quorum, profile boundaries), WAL replay incl. torn entries.
- **Deterministic simulator + fuzz** (`sim.rs`): virtual-clock event loop
  (seeded SplitMix64), fault injection (drop/delay/duplicate/partition/
  crash+restart), continuous oracles (agreement, height contiguity,
  no-equivocation checked at broadcast). 200-seed corpus in CI; liveness
  asserted only for live quorums (a stranded node needs sync, by design).
  Fuzzing found two real host bugs pre-production (RestreamProposal value
  re-send; live-input leniency).
- **Loopback integration** (`src/consensus/tests/malachite_integration.rs`):
  real iroh transports, real DBs, 2-of-3 majority mesh decides dispatch-table
  blocks, laggard syncs to byte-identical history.
- **Orchestrator** (Docker meshes, self-hosted iroh relay): consensus-leader-down
  (idle-proposer wake), consensus-lagging-catch-up, consensus-bft-quorum-loss
  (negative control), barrier tests (before_decide, before_publish_proposal),
  plus the full application suite and divergence checks.

## Configuration

| Env var | Effect |
|---|---|
| `HOPNET_QUORUM_PROFILE` | `bft` (default) or `majority` — genesis-fixed |
| `HOPNET_CONSENSUS_TIMEOUT_MS` | Scales `LinearTimeouts` base |
| `HOPNET_RELAY_URL` | Custom iroh relay (orchestrator meshes; disables n0 discovery) |
| `HOPNET_DB_*` | SQLite pragma overrides (see `src/db/shared.rs`) |

## Implementation status

Stages 0–6 complete (spike, crate scaffold, sans-io host, simulator+fuzz, real
adapters, cutover+deletion of the bespoke engine, orchestrator rebuild with
self-hosted relay). Stage 7 (bench vs old-engine baseline, full-suite pre-merge
gate, real-mesh soak) in progress. Full stage history: the consensus-malachite
plan file and branch log.
