# Orchestrator Infrastructure

HopNet includes a Docker-based orchestrator for testing mesh networks. The following Claude infrastructure is available:

**Agents:**
- `orchestrator-runner` (haiku) - Executes orchestrator commands (create mesh, run tests, check divergence). Returns structured results with SUCCESS, FAILURE, or NEEDS_DEBUG status.
- `orchestrator-debugger` (sonnet) - Investigates failures. Analyzes container logs, consensus history, and divergence patterns to identify root causes.

**Skill:**
- `orchestrator-reference` - Command syntax, available tests, and workflow documentation. Loaded automatically by both agents.

**Workflow:**
- Use the `orchestrator-reference` skill before invoking any agents and whenever working with the orchestrator to ensure you understand its syntax.
- When invoking `orchestrator-runner`, include mesh context: which mesh to use (if any exists), whether to reuse or create fresh, and any state requirements.
- When `orchestrator-runner` returns `NEEDS_DEBUG`, automatically invoke `orchestrator-debugger` with the failure context to investigate.

# Documentation Management
- ALWAYS check docs/system-overview.md for current project status and priorities
- When making code changes that affect system status, update progress indicators in:
  - docs/system-overview.md (high-level system component status)
  - Relevant RFCs in docs/specs/ (detailed implementation phase status)
  - Progress indicator format: [x] = Complete, [~] = In Progress, [ ] = Not Started, [!] = Blocked
- When implementing new features, update the relevant system status and progress tracking
- When creating new major subsystems, consider if they need their own RFC in docs/specs/
- Keep docs/system-overview.md as the single source of truth for project status

# Code-Documentation Sync Requirements
When making changes to the codebase:
1. Update progress indicators in docs/system-overview.md to reflect actual implementation status
2. Update corresponding RFC implementation phase status (e.g., Phase 1 [~] to Phase 1 [x] when complete)
3. If adding new major features, update the system component descriptions
4. If changing system architecture, update both the overview and relevant RFCs
5. Ensure the "Current Focus" section reflects what's actually being worked on

# Logging
- Use tracing for all Rust logging
- Use INFO levels sparingly to avoid slowdown due to excessive logging

# Database Writes
- Always use `crate::db::shared::commit_timed(tx)` instead of `tx.commit()`. It records commit latency into the always-on histogram exposed at `GET /debug/db-stats`. Same signature as `Transaction::commit`.
- Pragma tuning is via `HOPNET_DB_*` env vars (see `src/db/shared.rs::env_pragma_overrides`); orchestrator forwards them to containers automatically.
- Pull concurrency is via `HOPNET_PULL_WINDOW` / `HOPNET_PULL_FETCH_GLOBAL` / `HOPNET_PULL_FETCH_PER_PEER` / `HOPNET_PULL_URGENT_RESERVE` / `HOPNET_PULL_REBUILDS` (defaults 64/24/8/4/2, see `hopnet-storage/src/engine/fetch.rs::PullLimits`); orchestrator forwards `HOPNET_PULL_*` too.
- Urgent repair's startup grace is `HOPNET_REPAIR_GRACE_SECS` (default 900, 0 = off; see `src/storage_host/jobs.rs::REPAIR_GRACE`); orchestrator forwards `HOPNET_REPAIR_*`.
- Free-space floors (`hopnet-storage/src/admission.rs`): uploads stop at `HOPNET_STORAGE_MIN_FREE_BYTES` (10 GiB); replica writes (pulls, rebuilds, re-encodes) stop at `HOPNET_PULL_MIN_FREE_BYTES` / `HOPNET_PULL_MIN_FREE_PCT` (max(20 GiB, 2%); `0` bytes disables) and resume `HOPNET_PULL_RESUME_FREE_BYTES` higher (default max(10 GiB, 1%)).

# Debugging
- macOS debugging with the `log` command requires the use of sudo
- tail the last ~10 lines with cargo check to ensure you can see the final result; only if a build fails do you need to get more output.
- Heap profiling (Linux node only; jemalloc via `src/main.rs`, handlers in `src/debug/heap.rs`). Owner JWT required, others get 403:
  1. `curl -k -X POST -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' -d '{"active":true}' https://<node>:34632/api/debug/heap/profiling`
  2. Wait while the suspect workload runs (sampling only sees allocations made while active).
  3. `curl -k -H "Authorization: Bearer $JWT" https://<node>:34632/api/debug/heap/profile > heap.pb.gz` (409 while inactive).
  4. `nix shell nixpkgs#pprof --command pprof -top <path-to-hopnet-binary> heap.pb.gz` (or `go tool pprof`).
  5. POST `{"active":false}` to stop sampling (this also resets the collected samples).
  `GET /api/debug/heap/stats` reports jemalloc `allocated`/`active`/`resident`/`mapped`/`retained` and profiler state. Built-in config is `prof:true,prof_active:false,lg_prof_sample:19,background_thread:true` (exported `malloc_conf`); override with the standard `MALLOC_CONF` env var (e.g. `MALLOC_CONF=prof_active:true` to sample from boot). macOS (and HopNet.app) keeps the system allocator; the routes return 501 there.

# Git Commits
- Never include your attribution in commits

# Releases
A `v*` tag builds on the macbook runner and every node auto-stages it; every release is an epoch crossing. Before tagging:
1. REQUIRED gate: the cross-release orchestrator test must pass. Load the previous release's image (`nix develop --command scripts/build-release-image.sh v<previous>`) and this build's (`nix build .#packages.<system>.dockerImage && ./target/release/orchestrator load-image`), then `./target/release/orchestrator test --test release-crossing`. It crosses a mesh born on the previous release into this build and rejoins a node that was offline through the seal through the old artifact (the path consensus-bugs.md entry 19 broke).
2. The unit gates in CI must be green, including `every_sealed_ordinal_imports_through_a_spec_of_its_own_version` and the previous-release fixture tests (`previous_release_*`).
3. Any snapshot section `format_version` bump needs its frozen predecessor spec and a `resolve_import_plan` arm, even when only the version changes (node-local tables and indexes included).

After tagging:
- Bump `PREVIOUS_RELEASE` in `orchestrator/tests/regenesis.rs` to the new tag.
- Refresh `src/regenesis/fixtures/previous-release-join/` from a worktree at the new tag (`cargo test --lib regenerate_previous_release_join_fixture -- --ignored`, README beside the files) and commit it to master.

# Important Instruction Reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
When working on HopNet features, proactively maintain documentation sync with code changes.