#!/bin/bash
# Single source for the workspace CalVer identity in the macOS build
# scripts (RFC-026 S1). The authoritative token is [workspace.package]
# version in the root Cargo.toml; everything in the bundle answers it.
#
# Usage:
#   source version.sh          defines WORKSPACE_VERSION (e.g. 2026.8.4)
#                              and VERSION_CODE (year*10000+month*100+counter)
#   version.sh --check         fail if the photo-ingress Info.plist
#                              disagrees with the workspace version
#   version.sh --write         stamp the photo-ingress Info.plist from the
#                              workspace version (run at CalVer bump time,
#                              commit the result)
#
# The plist is checked rather than rewritten at build time so builds never
# dirty the tree; --check tripwires (stage 0 + Linux CI) catch drift.

_VERSION_SH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_VERSION_PROJECT_ROOT="$(dirname "$(dirname "$_VERSION_SH_DIR")")"
_DAEMON_PLIST="$_VERSION_PROJECT_ROOT/apple/PhotoIngress/Sources/photo-ingress/Info.plist"

WORKSPACE_VERSION="$(grep -m1 '^version = "' "$_VERSION_PROJECT_ROOT/Cargo.toml" | sed 's/^version = "\(.*\)"$/\1/')"
IFS='.' read -r _V_YEAR _V_MONTH _V_COUNTER <<< "$WORKSPACE_VERSION"
if ! [[ "$_V_YEAR" =~ ^[0-9]{4}$ && "$_V_MONTH" =~ ^[0-9]+$ && "$_V_COUNTER" =~ ^[0-9]+$ ]]; then
    echo "❌ Workspace version '$WORKSPACE_VERSION' is not CalVer YYYY.M.N"
    exit 1
fi
VERSION_CODE=$((_V_YEAR * 10000 + _V_MONTH * 100 + _V_COUNTER))

# Sourced: stop here, exporting only the two variables above.
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
    return 0
fi

set -euo pipefail

_plist_value_after_key() {
    # Print the <string> value on the line following <key>$1</key>.
    awk -v key="<key>$1</key>" '
        index($0, key) { getline; gsub(/.*<string>|<\/string>.*/, ""); print; exit }
    ' "$_DAEMON_PLIST"
}

case "${1:-}" in
    --check)
        PLIST_SHORT="$(_plist_value_after_key CFBundleShortVersionString)"
        PLIST_BUNDLE="$(_plist_value_after_key CFBundleVersion)"
        FAIL=0
        if [ "$PLIST_SHORT" != "$WORKSPACE_VERSION" ]; then
            echo "❌ photo-ingress Info.plist CFBundleShortVersionString '$PLIST_SHORT' != workspace version '$WORKSPACE_VERSION'"
            FAIL=1
        fi
        if [ "$PLIST_BUNDLE" != "$VERSION_CODE" ]; then
            echo "❌ photo-ingress Info.plist CFBundleVersion '$PLIST_BUNDLE' != version code '$VERSION_CODE'"
            FAIL=1
        fi
        if [ "$FAIL" -ne 0 ]; then
            echo "   Run scripts/macos/version.sh --write and commit the result."
            exit 1
        fi
        echo "✅ photo-ingress Info.plist agrees with workspace version $WORKSPACE_VERSION ($VERSION_CODE)"
        ;;
    --write)
        awk -v ver="$WORKSPACE_VERSION" -v code="$VERSION_CODE" '
            /<key>CFBundleShortVersionString<\/key>/ {
                print; getline
                sub(/<string>[^<]*<\/string>/, "<string>" ver "</string>"); print; next
            }
            /<key>CFBundleVersion<\/key>/ {
                print; getline
                sub(/<string>[^<]*<\/string>/, "<string>" code "</string>"); print; next
            }
            { print }
        ' "$_DAEMON_PLIST" > "$_DAEMON_PLIST.tmp"
        mv "$_DAEMON_PLIST.tmp" "$_DAEMON_PLIST"
        echo "✅ Stamped photo-ingress Info.plist: $WORKSPACE_VERSION ($VERSION_CODE)"
        ;;
    *)
        echo "Usage: version.sh [--check|--write]  (or source it for WORKSPACE_VERSION/VERSION_CODE)"
        exit 1
        ;;
esac
