# RFC-020: Module Versioning

**Status**: Draft (2026-07-23)
**Depends on**: RFC-013 (atomic decide + decided-value sync), RFC-016
(projection registry), RFC-CONSENSUS-002 (height-based activation),
RFC-STORAGE-002 (module-namespaced DB surfaces).
**Amends**: RFC-016 (Stage 7 — version threading through
`Projection::mounts()`/`work()`); RFC-STORAGE-002 (discharges the
"placement changes ship as coordinated binary upgrades" decision).
**Normative model**: scoped. The capability/activation core is modeled
in `hopnet-consensus/spec/validator_membership.qnt` (importing module) —
there, the model wins. The conventions in this RFC are prose-normative;
their enforcement layer is CI tripwires and orchestrator gates. See
Evidence.

## Motivation

HopNet has no migration path. `is_schema_initialized`
(`src/db/shared.rs:57`) is a boolean probe for one table's existence,
`initialize` (`src/db/shared.rs:279`) is a single `execute_batch` of the
current binary's whole schema, and the workspace contains zero
`ALTER TABLE`. The first release that reaches a machine we do not own
freezes the schema or breaks that install.

The obvious fix — a linear migration list applied at boot — is wrong
here, because the database is not the state. App state is a
deterministic function of the block history: `on_sync_value`
(`hopnet-consensus/src/host.rs:450`) feeds every historical block
through the same atomic apply path as live consensus, so a node joining
next year re-executes blocks decided this year. There is no database to
migrate. There is a state machine to evolve, and every node must evolve
it at the same height or diverge.

That reframing carries two consequences the rest of this RFC develops.
Schema and handler logic are the same problem — a migration is a change
to the transition function, and the DDL is one part of it. And
versioning cannot borrow semver's intuitions: a bug fix in a handler is
consensus-breaking *by definition*, because if the committed output did
not change it was not a fix. There is no compatible change to a
replay-bound function, so there is no minor axis. Versions are flat
ordinals.

The work is cheap now and permanently expensive later. Every mechanism
below is unconstrained while all meshes are disposable; the day one mesh
outlives one release, the base schema, the migration chain, and the
transaction envelope are all frozen retroactively.

## The module graph

Modules are the unit of versioning: each owns a migration chain, a
capability entry, and an independent activation height. That is only
coherent if the graph they form is acyclic — otherwise every activation
would have to be multi-module by default, and per-module versions would
buy nothing.

This is **not** RFC-016's crate dependency graph. Modules are ordered by
data dependency (foreign keys, and the DDL ordering they force), and the
two graphs invert where it matters most: `users` and `nodes` live in the
host crate, RFC-016's composition root at the *top* of the crate graph,
but every projection references them, which puts them at the *bottom*
here. That inversion is why the host crate cannot be one module.

```
consensus     consensus_wal, decided_blocks, decided_certificates,
              consensus_meta, validators, hopnet_consensus_policy
              (FK-isolated — references nothing, referenced by nothing)

identity      sequences, users, nodes, this_node, device_tokens
   │
   ├─────────────────────────────┬──────────────────┐
   ▼                             ▼                  ▼
storage                      telemetry          takeout
data_blocks, blob_access,    metrics,           (→ users)
mesh_key*, fragment_hashes,  pending_fragment_
fragment_inventory (→ nodes),  requests,
hopnet_storage_policy/pins   fragment_request_
   │                           metrics (→ nodes)
   ▼                         (leaf — nothing references it)
drive  (→ users, data_blocks)     photos (planned)
```

Evidence: `hopnet-drive/src/db/mod.rs:33,40,65,80-82,92-93`;
`hopnet-takeout/src/db/mod.rs:31,48`; `hopnet-storage/src/store.rs:131`;
`src/db/shared.rs:308,350,371,388,410`. The consensus crate contains no
`REFERENCES` clause at all.

The graph is a target, not the tree today. `identity` and `telemetry`
are aspirational — their tables come from one monolithic `execute_batch`
in `initialize` (`src/db/shared.rs:279`), with no seam between them.
This RFC does not block on splitting them.

## The pilot

`consensus`, `storage`, and each projection already own schema
installers (`src/db/shared.rs:425,440,442`), so versioning them extends
a seam that exists. `identity` and `telemetry` have none, and factoring
them is deferred — sequencing that refactor ahead of the mechanism would
put the least-understood work first.

Drive is the pilot: already a module in every sense the design needs, a
pure leaf (nothing references its tables, so its migrations can never
force another chain into the same crossing), and the module with the
most real schema churn. It is also where being wrong is cheapest — a
deterministic migration bug wedges every node identically at the
activation height, recoverable by a patched binary redefining that
version's chain, legal because nobody crossed. The same bug in
`consensus` breaks the machinery that processes activations.

The pilot establishes the gate every later module inherits: a fresh node
replaying genesis to tip across an activation boundary lands on a
byte-identical schema to a node that lived through it.
