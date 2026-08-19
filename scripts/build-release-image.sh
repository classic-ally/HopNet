#!/usr/bin/env bash
# Build the docker image of a RELEASED tag and load it into THIS
# checkout's container namespace under `hopnet:<hash>-<version>` — the
# old-release half of the RFC-020 cutover rehearsal
# (`orchestrator test --test regenesis-cutover`).
#
# The tag is built in a throwaway git worktree; the load runs from THIS
# checkout's orchestrator (never the tag's own — the namespace scheme
# and the load-image retagging both live here).
#
# Usage: scripts/build-release-image.sh v2026.8.5
set -euo pipefail

tag="${1:?usage: build-release-image.sh <release tag, e.g. v2026.8.5>}"
ver="${tag#v}"
root="$(git rev-parse --show-toplevel)"

case "$(uname -m)" in
x86_64) system="x86_64-linux" ;;
aarch64 | arm64) system="aarch64-linux" ;; # macOS: needs the remote builder
*)
    echo "unsupported build arch $(uname -m)" >&2
    exit 1
    ;;
esac

tmp="$(mktemp -d)"
cleanup() {
    git -C "$root" worktree remove --force "$tmp/src" 2>/dev/null || true
    rm -rf "$tmp"
}
trap cleanup EXIT

echo "==> building $tag ($system) in a throwaway worktree"
git -C "$root" worktree add --quiet "$tmp/src" "$tag"
nix build --out-link "$tmp/result" "path:$tmp/src#packages.$system.dockerImage"

echo "==> building this checkout's orchestrator"
cargo build --release --manifest-path "$root/Cargo.toml" \
    --bin orchestrator --features skip-frontend

hash="$("$root/target/release/orchestrator" prefix | awk '/checkout hash:/ {print $3}')"
image="hopnet:${hash}-${ver}"

echo "==> loading as $image"
HOPNET_ORCH_IMAGE="$image" "$root/target/release/orchestrator" \
    load-image --archive "$tmp/result"

echo "==> done: $image"
