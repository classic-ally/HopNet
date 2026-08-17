#!/bin/bash
set -euo pipefail

# Stage 3: Create FileProvider Extension Bundle

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
SWIFT_PROJECT_DIR="$PROJECT_ROOT/apple/HopNetFileProvider"
EXTENSION_NAME="HopNetFileProviderExtension"
EXTENSION_BUNDLE_ID="com.hopnet.desktop.fileprovider"

# RFC-026 S1: the appex answers the workspace CalVer like every other
# bundle surface (the heredoc below interpolates these).
source "$SCRIPT_DIR/version.sh"

echo "📦 Stage 3: Creating FileProvider extension bundle..."

# Read the app bundle path from previous stage
APP_BUNDLE_PATH_FILE="$PROJECT_ROOT/scripts/macos/.app_bundle_path"
if [ ! -f "$APP_BUNDLE_PATH_FILE" ]; then
    echo "❌ App bundle path not found. Did stage 2 complete successfully?"
    exit 1
fi

APP_BUNDLE=$(cat "$APP_BUNDLE_PATH_FILE")
EXTENSION_DIR="$APP_BUNDLE/Contents/PlugIns/$EXTENSION_NAME.appex"

# Check for executable first, then dylib as fallback
SWIFT_BINARY="$SWIFT_PROJECT_DIR/.build/release/$EXTENSION_NAME"
SWIFT_DYLIB="$SWIFT_PROJECT_DIR/.build/release/lib$EXTENSION_NAME.dylib"

if [ -f "$SWIFT_BINARY" ]; then
    ACTUAL_BINARY="$SWIFT_BINARY"
    echo "✅ Found Swift executable at: $SWIFT_BINARY"
elif [ -f "$SWIFT_DYLIB" ]; then
    ACTUAL_BINARY="$SWIFT_DYLIB"
    echo "✅ Found Swift library at: $SWIFT_DYLIB"
else
    echo "❌ Swift binary not found. Did stage 1 complete successfully?"
    exit 1
fi

# Create extension directory structure
mkdir -p "$EXTENSION_DIR/Contents/MacOS"
mkdir -p "$EXTENSION_DIR/Contents/Resources"

# Copy the Swift binary (dylib or executable)
cp "$ACTUAL_BINARY" "$EXTENSION_DIR/Contents/MacOS/$EXTENSION_NAME"
chmod +x "$EXTENSION_DIR/Contents/MacOS/$EXTENSION_NAME"

# Copy provisioning profile from main app if it exists
MAIN_APP_PROFILE="$APP_BUNDLE/Contents/embedded.provisionprofile"
if [ -f "$MAIN_APP_PROFILE" ]; then
    echo "📋 Copying provisioning profile from main app to extension"
    cp "$MAIN_APP_PROFILE" "$EXTENSION_DIR/Contents/embedded.provisionprofile"
else
    echo "⚠️  No provisioning profile found in main app"
fi

# Create Info.plist for the extension
cat > "$EXTENSION_DIR/Contents/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDevelopmentRegion</key>
    <string>en</string>
    <key>CFBundleDisplayName</key>
    <string>HopNet FileProvider</string>
    <key>CFBundleExecutable</key>
    <string>$EXTENSION_NAME</string>
    <key>CFBundleIdentifier</key>
    <string>$EXTENSION_BUNDLE_ID</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleName</key>
    <string>HopNet FileProvider</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>$WORKSPACE_VERSION</string>
    <key>CFBundleVersion</key>
    <string>$VERSION_CODE</string>
    <key>NSExtension</key>
    <dict>
        <key>NSExtensionFileProviderSupportsEnumeration</key>
        <true/>
        <key>NSExtensionPointIdentifier</key>
        <string>com.apple.fileprovider-nonui</string>
        <key>NSExtensionPrincipalClass</key>
        <string>HopNetFileProviderExtension.HopNetFileProviderExtension</string>
    </dict>
</dict>
</plist>
EOF

echo "✅ Stage 3 completed: Extension bundle created at $EXTENSION_DIR"