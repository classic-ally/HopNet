# Formal spec — storage durability & placement policy (RFC-018)

`storage_policy.qnt` is the normative model of the storage layer's
placement/repair/GC policy. The prose contract lives in
`docs/specs/storage-durability-policy.md`; where they disagree, the
model wins.

## Toolchain

Quint (Informal Systems) via npx — not packaged in nixpkgs. `quint
verify` spawns Apalache (JVM); use the nix-provided JDK. First `verify`
downloads the Apalache distribution to `~/.quint/` (one-time, ~2 min).

```bash
# Parse + unit tests (fast, run on every edit)
npx @informalsystems/quint parse spec/storage_policy.qnt
npx @informalsystems/quint test spec/storage_policy.qnt

# Random simulation — full-scale config (K=10, 30 fragments)
npx @informalsystems/quint run spec/storage_policy.qnt --main full \
  --invariant safety --max-samples 10000 --max-steps 60

# Exhaustive bounded model checking — scaled config (K=2, 6 fragments)
nix shell nixpkgs#jdk -c npx @informalsystems/quint verify \
  spec/storage_policy.qnt --main scaled --invariant safety --max-steps 12
```

Verified working 2026-07-13 with quint 0.32.0, node 24.
