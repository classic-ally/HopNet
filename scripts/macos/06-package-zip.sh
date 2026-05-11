#!/bin/bash
set -euo pipefail

# Stage 6: Package notarized+stapled .app into distributable .zip via ditto
# (Apple's recommended archive — preserves xattrs, resource forks, codesign).
# Output: $PROJECT_ROOT/dist/HopNet-<version>-<arch>.app.zip + manifest.json

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "🗜  Stage 6: Package for distribution"

APP_BUNDLE_PATH_FILE="$PROJECT_ROOT/scripts/macos/.app_bundle_path"
if [ ! -f "$APP_BUNDLE_PATH_FILE" ]; then
  echo "❌ App bundle path file missing"
  exit 1
fi
APP_BUNDLE=$(cat "$APP_BUNDLE_PATH_FILE")
APP_NAME=$(basename "$APP_BUNDLE")

# Prefer exact tag; fall back to short SHA for dev builds
if VERSION=$(git -C "$PROJECT_ROOT" describe --tags --exact-match 2>/dev/null); then
  :
else
  VERSION=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD)
fi

ARCH=$(uname -m)
DIST_DIR="$PROJECT_ROOT/dist"
mkdir -p "$DIST_DIR"

OUT_ZIP="$DIST_DIR/HopNet-${VERSION}-${ARCH}.app.zip"
rm -f "$OUT_ZIP"

echo "📦 Creating distribution zip via ditto..."
/usr/bin/ditto -c -k --sequesterRsrc --keepParent "$APP_BUNDLE" "$OUT_ZIP"

SHA=$(shasum -a 256 "$OUT_ZIP" | awk '{print $1}')
# Portable byte count — `stat -f%z` (BSD) vs `stat -c %s` (GNU) varies by
# userland; `nix develop` shadows /usr/bin/stat with GNU coreutils.
SIZE=$(wc -c < "$OUT_ZIP" | tr -d ' ')

# Manifest for CI consumption (avoids re-parsing in workflow YAML)
cat > "$DIST_DIR/manifest.json" <<EOF
{
  "version": "$VERSION",
  "arch": "$ARCH",
  "filename": "$(basename "$OUT_ZIP")",
  "path": "$OUT_ZIP",
  "sha256": "$SHA",
  "size_bytes": $SIZE,
  "app_bundle_name": "$APP_NAME"
}
EOF

# Drop a sibling .sha256 file alongside the zip for direct verification
echo "$SHA  $(basename "$OUT_ZIP")" > "$OUT_ZIP.sha256"

echo "✅ Stage 6 complete"
echo "   Artifact: $OUT_ZIP"
echo "   Size:     $SIZE bytes"
echo "   SHA-256:  $SHA"
echo "   Manifest: $DIST_DIR/manifest.json"
