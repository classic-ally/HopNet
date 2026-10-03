# Previous-release join fixture

A straggler as the PREVIOUS release's binary leaves it: its own
(unsealed, epoch-1) `database.db`, plus the staged epoch join it
downloaded from a peer that crossed into epoch 2 (`epoch-2.bin` lineage
record, `snapshot.bin` artifact, `manifest.bin`).

Current contents: written by **2026.10.8** (storage section @5).

Consumed by:
- `previous_release_straggler_crosses_into_this_build` (src/regenesis/boot.rs)
  — the staged-join boot path end to end;
- `previous_release_artifact_builds_against_its_own_hash` (src/db/chains.rs)
  — the artifact build alone.

Together they prove a seal written by the previous release imports into
this build. That broke at 2026.10.5 (consensus-bugs.md entry 19).

## Refreshing (every release, after tagging)

From a worktree checked out at the NEW release tag:

    cargo test --lib regenerate_previous_release_join_fixture -- --ignored

then copy this directory into master and update the version line above.
The next release's build then tests against it.
