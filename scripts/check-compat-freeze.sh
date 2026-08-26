#!/usr/bin/env bash
# RFC-025 release-tag tripwires (§Validation & Tripwires).
#
# The latest release tag is the freeze boundary for the compat wire
# vocabulary. Frozen files are every `*compat_g<N>.rs` that existed at
# the tag; COMPAT_HEAD (hopnet-comms/src/alpn.rs) is the mint marker.
# This script refuses, relative to that tag:
#   1. COMPAT_HEAD regressing below the tag's value
#   2. edits/renames of frozen generation modules — a mint ADDS files,
#      it never edits released ones (contract rule 1)
#   3. deletions of frozen generation modules without a
#      "RETIRES: compat_g<N>" commit trailer (contract rule 5's
#      explicit, reviewed marker — retirement deletes whole files)
#   4. a served window in HEAD other than exactly [COMPAT_HEAD-1,
#      COMPAT_HEAD] at the file level: a mint that forgot its modules,
#      or a retirement that jumped early (contract rule 4)
#
# Per-scope completeness (every compat scope carries both handlers) is
# compile-time (rpc_compat's mandatory prev) plus the cross-crate tie
# test in net::scopes — not this script's job.
#
# Loud by design: a missing tag is a FAILURE, never a skip.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

TAG=$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null) || {
    echo "check-compat-freeze: no release tag reachable (need full history + tags; fetch-depth: 0)" >&2
    exit 1
}
echo "check-compat-freeze: freeze boundary is ${TAG}"

head_at() {
    # The file may not exist at pre-enforcement tags — empty means
    # "nothing was frozen there", never a failure.
    { git show "$1:hopnet-comms/src/alpn.rs" 2>/dev/null || true; } \
        | sed -n 's/^pub const COMPAT_HEAD: u32 = \([0-9]\+\);.*/\1/p' | head -1
}

CUR=$(head_at HEAD)
if [ -z "${CUR}" ]; then
    echo "check-compat-freeze: COMPAT_HEAD not found in HEAD's alpn.rs" >&2
    exit 1
fi
TAG_HEAD=$(head_at "${TAG}")
fail=0

# --- 1. generations never regress ---
if [ -n "${TAG_HEAD}" ] && [ "${CUR}" -lt "${TAG_HEAD}" ]; then
    echo "check-compat-freeze: COMPAT_HEAD regressed (${TAG_HEAD} at ${TAG} -> ${CUR})" >&2
    fail=1
fi

# --- 2+3. frozen modules: byte-identity, RETIRES deletion escape ---
# The frozen set is DERIVED from the tag's tree (files added since the
# tag are not yet frozen); pre-enforcement tags freeze nothing.
frozen=$(git ls-tree -r --name-only "${TAG}" | grep -E 'compat_g[0-9]+\.rs$' || true)
if [ -z "${frozen}" ]; then
    echo "check-compat-freeze: nothing frozen at ${TAG} (pre-enforcement tag)"
else
    retires=$(git log --format=%B "${TAG}..HEAD" | grep -oE 'RETIRES: compat_g[0-9]+' | sort -u || true)
    while IFS=$'\t' read -r status file _; do
        [ -n "${status}" ] || continue
        echo "${frozen}" | grep -qx "${file}" || continue
        case "${status}" in
            D*)
                generation=$(basename "${file}" | grep -oE 'compat_g[0-9]+')
                if echo "${retires}" | grep -qx "RETIRES: ${generation}"; then
                    echo "check-compat-freeze: ${file} retired under explicit marker (RETIRES: ${generation})"
                else
                    echo "check-compat-freeze: FROZEN module ${file} deleted without a RETIRES trailer" >&2
                    fail=1
                fi
                ;;
            M*|R*)
                echo "check-compat-freeze: FROZEN module ${file} was ${status} — a mint adds files, it never edits released ones" >&2
                fail=1
                ;;
        esac
    done < <(git diff --name-status "${TAG}..HEAD")
fi

# --- 4. the served window exists at the file level, and nothing below it ---
head_files=$(git ls-files | grep -E "compat_g[0-9]+\.rs$" || true)
floor=$((CUR > 0 ? CUR - 1 : 0))
for g in $(seq "${floor}" "${CUR}"); do
    if ! echo "${head_files}" | grep -qE "compat_g${g}\.rs$"; then
        echo "check-compat-freeze: no compat_g${g}.rs module for in-window generation ${g} (window [${floor}, ${CUR}])" >&2
        fail=1
    fi
done
while IFS= read -r file; do
    [ -n "${file}" ] || continue
    g=$(basename "${file}" | grep -oE '[0-9]+')
    if [ "${g}" -lt "${floor}" ]; then
        echo "check-compat-freeze: ${file} is below the window [${floor}, ${CUR}] — retirement deletes whole files" >&2
        fail=1
    fi
done <<< "${head_files}"

if [ "${fail}" -ne 0 ]; then
    echo "check-compat-freeze: FAILED (RFC-025 §Validation & Tripwires)" >&2
    exit 1
fi
echo "check-compat-freeze: OK"
