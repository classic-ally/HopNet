# Formal spec — storage durability & placement policy (RFC-STORAGE-001)

`storage_policy.qnt` is the normative model of the storage layer's
placement/repair/GC policy. The prose contract is RFC-STORAGE-001,
`durability-policy.md` in this directory; where they disagree, the
model wins.

## Module map

- `placement` — the pure placement functions (select+modulo, plain HRW,
  capped HRW) and `buildTable`. Nonlinear integer arithmetic
  (`f % |members|`, `mix/weight`); Apalache CANNOT handle it, so the
  control logic never calls these — only the `run`/`test` configs do,
  to build a lookup table on the Rust backend.
- `storage_policy` — the control logic: adversarial environment
  (sleep/wake/depart/corrupt/evict/delete), the deterministic
  `engineTick` (decay-gated view sync → self-check → pull → re-encode),
  and the invariants. Placement is an injected `const PLACE` lookup, so
  this module is arithmetic-free and Apalache-verifiable.
- `scaled` / `scaled_hrw` / `scaled_chrw` / `scaled_neg` / `scaled_tier1`
  — Rust-backend configs (K=2, 6 classes) that build their table via
  `buildTable`; each carries the witness runs.
- `verify_table` + `scaled_verify` — the exhaustive-verification config:
  a LITERAL precomputed capped-HRW table (no arithmetic) fed to the
  control logic for Apalache.
- `table_guard` — asserts the literal table equals `buildTable` output.
- `full` — real parameters (K=10, 30 classes, 10 nodes) for deep
  random simulation.

## Toolchain

Quint (Informal Systems) via npx — not packaged in nixpkgs. `quint
verify` spawns Apalache (JVM); use the nix-provided JDK. First `verify`
downloads the Apalache distribution to `~/.quint/` (one-time, ~2 min).

```bash
# Parse + typecheck (fast, run on every edit)
npx @informalsystems/quint typecheck spec/storage_policy.qnt

# Witness/unit tests per config (Rust backend, seconds)
for m in scaled scaled_hrw scaled_chrw scaled_neg scaled_tier1 table_guard; do
  npx @informalsystems/quint test spec/storage_policy.qnt --main $m
done

# Random simulation — full-scale config (K=10, 30 classes)
npx @informalsystems/quint run spec/storage_policy.qnt --main full \
  --invariant safety --max-samples 10000 --max-steps 60

# EXHAUSTIVE bounded model checking (Apalache). Set-heavy state makes it
# slow (~1-2 min/state); keep --max-steps small. safety = INV-DURABLE,
# spread = INV-SPREAD. `recovery` is vacuous below --max-steps 13 (needs
# CALM_BOUND consecutive engine ticks) — check convergence via the
# scripted witnesses instead.
nix shell nixpkgs#jdk -c npx @informalsystems/quint verify \
  spec/storage_policy.qnt --main scaled_verify --invariant safety --max-steps 6
```

## Regenerating the verify table

If the placement functions in module `placement` change, the literal
`verify_table::CHRW_TABLE` must be regenerated (the `table_guard` test
fails otherwise). Print the fresh table from the REPL:

```bash
printf '%s\n' \
  'pure val W = Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1)' \
  'Set(1,2,3,4).powerset().exclude(Set(Set())).mapBy(s => range(0,6).foldl(Map(), (a,f) => a.put(f, placeVariant(2, f, 6, s, W))))' \
  '.exit' \
| npx @informalsystems/quint -r spec/storage_policy.qnt::scaled --backend=typescript
```

Verified working 2026-07-13 with quint 0.32.0, node 24.
