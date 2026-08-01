# RFC-CONSENSUS-001 — model checking

Normative model: `validator_membership.qnt` (where prose and model
disagree, the model wins). Prose: `validator-membership.md`.

The same file carries RFC-019's boundary model (`epoch_policy`, the
regenesis epoch transition layered over a `membership_policy`
instance); its prose counterpart is `regenesis-seal-contract.md`.

Toolchain: quint 0.32.0 via npx, node 24. Apalache is fetched to
`~/.quint/` on first `verify` and needs a JDK — run it under
`nix shell nixpkgs#jdk -c`.

## Commands

```bash
# typecheck (fast)
npx @informalsystems/quint typecheck hopnet-consensus/spec/validator_membership.qnt

# enumeration lemmas + all witness/NEG runs (Rust backend, seconds each)
for m in math_guard cfg_seam cfg_small cfg_solo cfg_duo cfg_ten \
         cfg_bft_pinned cfg_maj7 cfg_maj7_perbatch cfg_neg_ceiling \
         cfg_cliff_guarded cfg_neg_fairness cfg_evidence_pos \
         cfg_evidence_neg_theft cfg_evidence_neg_asym cfg_partition \
         cfg_sched epoch_policy; do
  npx @informalsystems/quint test hopnet-consensus/spec/validator_membership.qnt --main $m
done

# random adversarial simulation
npx @informalsystems/quint run hopnet-consensus/spec/validator_membership.qnt \
  --main cfg_seam --invariant safetyInv --max-samples 10000 --max-steps 30
npx @informalsystems/quint run hopnet-consensus/spec/validator_membership.qnt \
  --main cfg_evidence_neg_theft --invariant invNoHarm --max-samples 10000 --max-steps 30
npx @informalsystems/quint run hopnet-consensus/spec/validator_membership.qnt \
  --main epoch_policy --init epochInit --step epochStep \
  --invariant epochSafetyInv --max-samples 10000 --max-steps 30

# Apalache — INDUCTIVE (no depth bound: arbitrary conforming state +
# one step preserves indInv, and indInv implies safetyInv)
nix shell nixpkgs#jdk -c npx @informalsystems/quint verify \
  hopnet-consensus/spec/validator_membership.qnt \
  --main cfg_seam --inductive-invariant indInv --invariant safetyInv
# likewise --main cfg_small / cfg_bft_pinned / cfg_maj7

# Apalache — INDUCTIVE, epoch machine (RFC-019 regenesis boundary; the
# membership matrix transfers across the boundary depth-free — the
# bridge). Non-default init/step names: epoch_policy layers new state
# over an imported membership_policy instance.
nix shell nixpkgs#jdk -c npx @informalsystems/quint verify \
  hopnet-consensus/spec/validator_membership.qnt \
  --main epoch_policy --init epochInit --step epochStep \
  --inductive-invariant epochIndInv --invariant epochSafetyInv

# Apalache — bounded, evidence machine (the no-stall theorem)
nix shell nixpkgs#jdk -c npx @informalsystems/quint verify \
  hopnet-consensus/spec/validator_membership.qnt \
  --main cfg_evidence_neg_theft --invariant invNoHarm --max-steps 10
```

## Which layer checks what

| Property | Layer |
|---|---|
| Parity, batch, seam, monotonicity, posture, exposure, disjoint-quorum closed forms (v = 1..30, all three modes) | enumeration lemma (`math_guard`) |
| Literal table == closed form; scaled-constant orderings | drift guard runs |
| INV-NO-HARM, CEILING (proven-quorum), INV-FLOOR, NO-EXILE (enabledness half) | Apalache **inductive** (`indInv`) — depth-free |
| No-stall theorem under broken evidence contract | Apalache bounded (evidence machine) + 10k-trace sim |
| RECOVERY, GUAR-HONEST-SET, longest-dark-first, seam round-trip, heal round-trip, mass-dark trajectory, free option, exemptions, dead-ends | scripted witness runs |
| Per-batch ceiling non-composition; cliff-crossing stall; fairness bound tightness; headroom theft; asymmetric-refresh block | scripted NEG runs |
| Partition safety | DISJOINT-QUORUM lemma + block-relative witnesses |
| Seal safety (`decidedPastSeal` ghost), boundary carries the seated set, membership matrix transfers across the boundary (the bridge) | Apalache **inductive** (`epochIndInv`) — depth-free |
| Full boundary with dark-seat vote-out from the carried set; abort round-trip; structural quiet period; forward-only seal; drain termination | scripted witness runs (`epoch_policy`) |

Bounded-depth caveat: lazy-band removals need kill + T_LAZY ticks +
commit (12+ steps at production-shaped constants) — bounded runs at
small depth would be silently vacuous for them. That is why the main
safety claims are checked INDUCTIVELY (depth-free) and the deep
trajectories are scripted witnesses, not left to the random scheduler
(which never finds N-consecutive-tick schedules).

## Results (2026-08-01, all green)

| check | verdict | runtime |
|---|---|---|
| math_guard lemmas (9, complete enumeration v=1..30) | pass | <1 s |
| 29 witness/NEG runs across 17 configs (incl. 5 `epoch_policy` boundary witnesses) | pass | seconds each |
| random sim: seam 10k / ten 5k / bft_pinned 5k / evidence 10k / sched 5k / epoch 10k traces | no violation | ~1 s each |
| Apalache inductive `indInv` + `safetyInv`: cfg_small | NoError | 9.7 s |
| — cfg_bft_pinned | NoError | 9.7 s |
| — cfg_maj7 | NoError | 13.9 s |
| — cfg_seam (1..9, AUTO composite) | NoError | 34.1 s |
| — cfg_ten (1..10) | NoError | 77.4 s |
| Apalache inductive `epochIndInv` + `epochSafetyInv`: epoch_policy (1..9, the boundary bridge) | NoError | 38.1 s |
| Apalache bounded: evidence machine `invNoHarm`, depth 10 | NoError | 46.0 s |

Partition machine is deliberately witness-grade: its impossibility
core (two disjoint quorums) is the DISJOINT-QUORUM enumeration lemma;
the block-relative dynamics are scripted witnesses.

## Quint gotchas (inherited from the storage effort + new)

- Apalache rejects nonlinear integer arithmetic on state — quorum
  arithmetic is injected as literal tables (`q_tables`), drift-guarded
  against the closed forms.
- **Primed assignment binds tighter than `or`**: `x' = a or b` parses
  as `(x' = a) or b` and silently discards `b`. Parenthesize boolean
  RHS: `x' = (a or b)`. This cost us a ghost-flag vacuity bug.
- Inductive invariants must be Apalache-ASSIGNABLE: every variable
  constrained via `.in(...)` / `== ...` at the top level of the
  conjunction (the invariant doubles as the state generator).
- The random simulator never produces long consecutive-tick schedules;
  anything temporal needs a scripted `run`.
- REPL: use `--backend=typescript` (Rust backend readline bug).
- On NixOS the auto-fetched Rust evaluator
  (`~/.quint/rust-evaluator-*/quint_evaluator`) is a generic-linux
  dynamic binary and won't start (stub-ld). Patch it once:
  `patchelf --set-interpreter <glibc>/lib64/ld-linux-x86-64.so.2
  --set-rpath <glibc>/lib:<libgcc>/lib` (paths via
  `nix shell nixpkgs#binutils -c ldd` on the binary).
