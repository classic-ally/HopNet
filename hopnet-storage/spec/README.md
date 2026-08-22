# Formal spec — storage durability, placement & block lifecycle

`storage_policy.qnt` is the normative model of the storage layer's
placement/repair/GC policy (RFC-STORAGE-001) and, since RFC-STORAGE-003,
of the whole block lifecycle: distribution from birth, the
`(confirmed, target)` handoff pair, declare/confirm, belief-vs-truth
divergence, and eviction protection. The prose contracts are
`durability-policy.md` (RFC-STORAGE-001) and `block-lifecycle.md`
(RFC-STORAGE-003) in this directory; where they disagree, the model
wins.

## Module map

- `placement` — the pure placement functions (select+modulo, plain HRW,
  capped HRW, balanced capped rendezvous) and `buildTable`. Nonlinear
  integer arithmetic (`f % |members|`, `mix/weight`); Apalache CANNOT
  handle it, so the control logic never calls these — only the
  `run`/`test` configs do, to build a lookup table on the Rust backend.
- `storage_policy` — the control logic: adversarial environment
  (sleep/wake/depart/corrupt/evict/delete/drop-mark), the deterministic
  `engineTick` ladder (view sync → declare → re-encode → pull → belief
  sync → confirm), and the invariants. Placement is an injected
  `const PLACE` lookup, so this module is arithmetic-free and
  Apalache-verifiable. Three inits: `init` (conformant + confirmed —
  the RFC-STORAGE-001 baseline), `initOriginHeld` (birth: single-origin
  copies, `confirmed = NULL`), `initMidFlight` (generalized
  protocol-consistent mid-pipeline states, for exhaustive checking from
  every neighborhood).
- `scaled` / `scaled_hrw` / `scaled_bal` / `scaled_neg` / `scaled_tier1`
  — Rust-backend configs (K=2, 6 classes) that build their table via
  `buildTable`; each carries its witness runs. `scaled_bal` (the adopted
  balanced-capped variant) carries the RFC-STORAGE-003 lifecycle
  witnesses: origin-held convergence, the region trace, drop-mark heal
  both directions, premature-eviction safety, surplus lapse, supersede,
  the CALM_BOUND tightness pair, and a random-burst convergence
  regression.
- `verify_table` + `scaled_verify` — exhaustive verification with full
  fault budgets: a LITERAL precomputed balanced-capped table (no
  arithmetic) fed to the control logic for Apalache.
- `scaled_verify_birth` — same table, MAX_GONE = 0: the origin-held
  safety config. A single-copy blob whose origin disk dies before first
  replication is data loss by nature (the upload durability window every
  system has), so whole-disk death is out of budget from this init;
  sleep, eviction pressure, corruption, and mark loss stay adversarial.
- `table_guard` — asserts the literal table equals `buildTable` output,
  plus the bridge/spread/burst/sigma lemmas by complete enumeration.
- `full` — real parameters (K=10, 30 classes, 10 nodes) for deep
  random simulation.

## Invariants and the checking regime (RFC-STORAGE-003)

- `safety` (INV-DURABLE) and `evictSafe` (INV-EVICT-SAFE): exhaustive
  bounded model checking (Apalache), small depth, from all three inits,
  with `envDropMark` enabled. `evictSafe` flags an eviction that empties
  an undeleted class's extant copies *on truthful belief* — impossible
  under a correct protection predicate + belt, so any violation is
  definitionally a guard regression. False-belief harm is the
  environment's, budgeted, bounded by INV-DURABLE.
- `convergence` (INV-CONVERGE): `deleted or calm < CALM_BOUND or
  not(available) or converged`. The `available` precondition is
  deliberate: with fewer than K classes on online nodes the engine has
  nothing to read from and idles without penalty — convergence is
  promised for calm windows in which the chunk is reconstructible.
  Checked by scripted witnesses (every stuck-point region) plus random
  simulation — exhaustive checking cannot reach depth ≥ CALM_BOUND.
- `spread` (INV-SPREAD): unchanged from RFC-STORAGE-001.

CALM_BOUND is counted, not tuned, and pinned tight in both directions
by `calmTightnessTest`: the worst window at scaled size is 13 ticks
(re-admission view sync + 2 hope-gated decay ticks + view sync +
declare + 6 moves + belief sync + confirm); 12 provably do not suffice.
The count was falsified against biased-step random search (`probeStep`
shape: adversary bursts, then a pure engine window) at 300k+ samples
per init. The rejected sync-above-movers ladder measures 19 on the same
config (~2 ticks per move); the adopted sync-below-movers ladder is
also the faithful image of the implementation, whose inventory rows
land via batched self-checks, not per-pull.

## Toolchain

Quint (Informal Systems) via npx — not packaged in nixpkgs. `quint
verify` spawns Apalache (JVM); use the nix-provided JDK. First `verify`
downloads the Apalache distribution to `~/.quint/` (one-time, ~2 min).

```bash
# Parse + typecheck (fast, run on every edit)
npx @informalsystems/quint typecheck spec/storage_policy.qnt

# Witness/unit tests per config (Rust backend, seconds; CI runs these)
for m in scaled scaled_hrw scaled_bal scaled_neg scaled_tier1 table_guard; do
  npx @informalsystems/quint test spec/storage_policy.qnt --main $m
done

# Random simulation — full-scale config (K=10, 30 classes)
npx @informalsystems/quint run spec/storage_policy.qnt --main full \
  --invariant safety --max-samples 10000 --max-steps 60

# EXHAUSTIVE bounded model checking (Apalache), the S0 matrix: safety
# and evictSafe from each init, drops enabled. Set-heavy state makes it
# slow; keep --max-steps small. `convergence` is vacuous below
# CALM_BOUND consecutive engine ticks — check it via the scripted
# witnesses and the biased-step simulation instead.
# Regime notes (measured 2026-08-25, 14-core/30G desktop):
# - Pass BOTH invariants in one call: the SMT unrolling dominates and
#   is shared, so one combined run costs about one invariant's worth.
# - Depths are per-init: 6 from the narrow inits (~4h birth, ~1.7h
#   init); 3 from the generalized initMidFlight (~27h). Depth 4 from
#   initMidFlight is intractable here — >16h and still inside step 4 —
#   and initMidFlight trades depth for breadth by design: every
#   counterexample it has produced surfaced within 3 steps. Raise its
#   depth only on much bigger hardware.
# - Cap memory. z3's native allocations ignore -Xmx (a depth-4
#   midFlight run grew past 17G and took the whole box down); the
#   systemd scope makes a blowup kill the cell, not the machine.
# - One verify at a time: the quint<->Apalache gRPC server binds a
#   fixed port, so concurrent verifies collide fatally.
v() {
  systemd-run --user --scope -q -p MemoryMax=20G -- \
    nix shell nixpkgs#jdk -c npx @informalsystems/quint verify \
    spec/storage_policy.qnt --invariant safety,evictSafe "$@"
}
v --main scaled_verify --init init --max-steps 6
v --main scaled_verify --init initMidFlight --max-steps 3
v --main scaled_verify_birth --init initOriginHeld --max-steps 6
```

CI (`.forgejo/workflows/check-linux.yml`, job `spec`) runs the fast
half — typecheck + the per-config witness suites — on every push/PR.
The Apalache matrix stays a local ritual.

## Regenerating the verify table

If the placement functions in module `placement` change, the literal
`verify_table::BAL_TABLE` must be regenerated (the `table_guard` test
fails otherwise). Print the fresh table from the REPL:

```bash
printf '%s\n' \
  'pure val W = Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1)' \
  'Set(1,2,3,4).powerset().exclude(Set(Set())).mapBy(s => range(0,6).foldl(Map(), (a,f) => a.put(f, placeVariant(3, f, 6, s, W))))' \
  '.exit' \
| npx @informalsystems/quint -r spec/storage_policy.qnt::scaled --backend=typescript
```

Verified working 2026-07-13 with quint 0.32.0, node 24.
