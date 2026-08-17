#!/usr/bin/env bash
# RFC-020 release-tag tripwires (§Validation & Tripwires).
#
# The latest release tag is the freeze boundary: only released steps can
# have been crossed by a mesh. This script refuses, relative to that tag:
#   1. edits/deletions of released step files — unless a commit in
#      tag..HEAD carries a "REDEFINES: <module>/<NNNN>" trailer naming
#      the file (contract rule 1's explicit, reviewed marker)
#   2. new step files that do not sort strictly above the tag's
#      per-module head
#   3. duplicate NNNN ordinal prefixes within a module folder
#   4. ordinal gaps (+1 contiguity) within a module folder
#
# Loud by design: a missing tag is a FAILURE, never a skip.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

TAG=$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null) || {
    echo "check-chains: no release tag reachable (need full history + tags; fetch-depth: 0)" >&2
    exit 1
}
echo "check-chains: freeze boundary is ${TAG}"

MIGRATION_PATHS=(migrations hopnet-storage/migrations hopnet-drive/migrations hopnet-photos/migrations hopnet-takeout/migrations)
fail=0

# --- 1. released steps are frozen (byte-identical), REDEFINES escape ---
redefines=$(git log --format=%B "${TAG}..HEAD" | grep -oE 'REDEFINES: [a-z_]+/[0-9]{4}' | sort -u || true)
while IFS=$'\t' read -r status file _; do
    [ -n "${status}" ] || continue
    case "${status}" in
        M*|D*|R*)
            # Only files that existed at the tag are frozen.
            if git cat-file -e "${TAG}:${file}" 2>/dev/null; then
                module=$(basename "$(dirname "${file}")")
                ordinal=$(basename "${file}" | cut -c1-4)
                if echo "${redefines}" | grep -qx "REDEFINES: ${module}/${ordinal}"; then
                    echo "check-chains: ${file} redefined under explicit marker (REDEFINES: ${module}/${ordinal})"
                else
                    echo "check-chains: FROZEN step ${file} was ${status} without a REDEFINES trailer" >&2
                    fail=1
                fi
            fi
            ;;
    esac
done < <(git diff --name-status "${TAG}..HEAD" -- "${MIGRATION_PATHS[@]}" | awk '$2 ~ /\.sql$/ || $3 ~ /\.sql$/')

# --- 2-4. per-folder ordering, against the tag's per-module head ---
for dir in $(find "${MIGRATION_PATHS[@]}" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort); do
    module=$(basename "${dir}")
    ordinals=$(find "${dir}" -maxdepth 1 -name '[0-9][0-9][0-9][0-9]_*.sql' -printf '%f\n' | cut -c1-4 | sort)
    [ -n "${ordinals}" ] || continue

    dup=$(echo "${ordinals}" | uniq -d)
    if [ -n "${dup}" ]; then
        echo "check-chains: duplicate ordinal(s) in ${dir}: ${dup}" >&2
        fail=1
    fi

    prev=""
    while read -r ord; do
        if [ -n "${prev}" ] && [ $((10#${ord})) -ne $((10#${prev} + 1)) ]; then
            echo "check-chains: ordinal gap in ${dir}: ${prev} -> ${ord}" >&2
            fail=1
        fi
        prev="${ord}"
    done <<< "${ordinals}"

    tag_head=$(git ls-tree --name-only "${TAG}" -- "${dir}/" 2>/dev/null \
        | grep -oE '[0-9]{4}_[^/]*\.sql$' | cut -c1-4 | sort | tail -1 || true)
    if [ -n "${tag_head}" ]; then
        while IFS= read -r new_file; do
            [ -n "${new_file}" ] || continue
            ord=$(basename "${new_file}" | cut -c1-4)
            if [ $((10#${ord})) -le $((10#${tag_head})) ]; then
                echo "check-chains: new step ${new_file} does not sort above ${module}'s released head ${tag_head}" >&2
                fail=1
            fi
        done < <(git diff --name-only --diff-filter=A "${TAG}..HEAD" -- "${dir}/" | grep '\.sql$' || true)
    fi
done

if [ "${fail}" -ne 0 ]; then
    echo "check-chains: FAILED (RFC-020 §Validation & Tripwires)" >&2
    exit 1
fi
echo "check-chains: OK"
