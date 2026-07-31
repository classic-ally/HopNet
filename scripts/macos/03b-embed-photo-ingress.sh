#!/bin/bash
set -euo pipefail

# Stage 3b: Embed the photo-ingress daemon + its LaunchAgent plist.
#
# The daemon binary lands in Contents/MacOS/ and the SMAppService agent
# plist in Contents/Library/LaunchAgents/ — both must be in place before
# stage 4 signs (daemon individually, then the outer app seals the plist
# as a resource).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

# The agent identifier: LaunchAgent label + plist filename. Appears in
# exactly three places, which must stay in sync:
#   1. apple/PhotoIngress/Sources/photo-ingress/Info.plist (CFBundleIdentifier)
#   2. this variable
#   3. src/photo_ingress/service.rs (AGENT_PLIST_NAME)
AGENT_ID="com.hopnet.desktop.photo-ingress"

echo "📦 Stage 3b: Embedding photo-ingress daemon..."

APP_BUNDLE_PATH_FILE="$PROJECT_ROOT/scripts/macos/.app_bundle_path"
if [ ! -f "$APP_BUNDLE_PATH_FILE" ]; then
    echo "❌ App bundle path not found. Did stage 2 complete successfully?"
    exit 1
fi
APP_BUNDLE=$(cat "$APP_BUNDLE_PATH_FILE")

DAEMON_BINARY="$PROJECT_ROOT/apple/PhotoIngress/.build/release/photo-ingress"
if [ ! -f "$DAEMON_BINARY" ]; then
    echo "❌ photo-ingress binary not found. Did stage 1b complete successfully?"
    exit 1
fi

cp "$DAEMON_BINARY" "$APP_BUNDLE/Contents/MacOS/photo-ingress"
chmod +x "$APP_BUNDLE/Contents/MacOS/photo-ingress"
echo "✅ Daemon binary → Contents/MacOS/photo-ingress"

# The SMAppService agent plist. BundleProgram is the SMAppService-required
# bundle-relative executable path; ProgramArguments still supplies argv
# (and the daemon defaults to the `daemon` subcommand if argv ever arrives
# empty). No --data-dir: the daemon defaults to the canonical
# ~/.local/share/hopnet-photo-ingress (plists cannot expand ~), and
# --log-to-data-dir makes it own its log file (StandardOutPath cannot be
# user-relative either). Credentials + blob root come from the keychain,
# provisioned by the /photo-ingress/enable route before registration.
AGENTS_DIR="$APP_BUNDLE/Contents/Library/LaunchAgents"
mkdir -p "$AGENTS_DIR"
cat > "$AGENTS_DIR/$AGENT_ID.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$AGENT_ID</string>
    <key>BundleProgram</key>
    <string>Contents/MacOS/photo-ingress</string>
    <key>ProgramArguments</key>
    <array>
        <string>photo-ingress</string>
        <string>daemon</string>
        <string>--log-to-data-dir</string>
    </array>
    <key>KeepAlive</key>
    <true/>
    <key>ProcessType</key>
    <string>Background</string>
    <key>AssociatedBundleIdentifiers</key>
    <array>
        <string>com.hopnet.desktop</string>
    </array>
</dict>
</plist>
EOF
echo "✅ Agent plist → Contents/Library/LaunchAgents/$AGENT_ID.plist"

echo "✅ Stage 3b completed: photo-ingress embedded"
