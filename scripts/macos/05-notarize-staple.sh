#!/bin/bash
set -euo pipefail

# Stage 5: Submit signed bundle to Apple notary service, staple ticket.
# Required env: APPLE_ID, TEAM_ID, NOTARY_PASSWORD (app-specific password from
# appleid.apple.com — NOT your Apple ID password).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "📬 Stage 5: Notarize + staple"

# Source .env if present (when running standalone, not via wrapper)
if [ -f "$PROJECT_ROOT/scripts/macos/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$PROJECT_ROOT/scripts/macos/.env"
    set +a
fi

: "${APPLE_ID:?APPLE_ID env var required}"
: "${TEAM_ID:?TEAM_ID env var required}"
: "${NOTARY_PASSWORD:?NOTARY_PASSWORD env var required (app-specific password)}"

APP_BUNDLE_PATH_FILE="$PROJECT_ROOT/scripts/macos/.app_bundle_path"
if [ ! -f "$APP_BUNDLE_PATH_FILE" ]; then
  echo "❌ App bundle path file missing. Did stages 02-04 run?"
  exit 1
fi
APP_BUNDLE=$(cat "$APP_BUNDLE_PATH_FILE")

if [ ! -d "$APP_BUNDLE" ]; then
  echo "❌ App bundle not found at $APP_BUNDLE"
  exit 1
fi

# notarytool requires a zip/dmg/pkg, not a .app directory directly
WORK_DIR=$(dirname "$APP_BUNDLE")
APP_NAME=$(basename "$APP_BUNDLE")
NOTARY_ZIP="$WORK_DIR/${APP_NAME%.app}-notary.zip"

echo "📦 Zipping $APP_BUNDLE for notary submission..."
rm -f "$NOTARY_ZIP"
/usr/bin/ditto -c -k --sequesterRsrc --keepParent "$APP_BUNDLE" "$NOTARY_ZIP"

echo "📤 Submitting to Apple notary service (typically 3-10 min)..."
xcrun notarytool submit "$NOTARY_ZIP" \
  --apple-id "$APPLE_ID" \
  --team-id "$TEAM_ID" \
  --password "$NOTARY_PASSWORD" \
  --wait

echo "📎 Stapling ticket to bundle..."
xcrun stapler staple "$APP_BUNDLE"

echo "🔍 Verifying stapled ticket..."
xcrun stapler validate "$APP_BUNDLE"

# Clean up the notary submission zip; distribution zip created in stage 06
rm -f "$NOTARY_ZIP"

echo "✅ Stage 5 complete: bundle notarized + stapled"
