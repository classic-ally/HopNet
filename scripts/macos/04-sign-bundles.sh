#!/bin/bash
set -euo pipefail

# Stage 4: Sign Both Extension and Main App

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
EXTENSION_NAME="HopNetFileProviderExtension"

# Configuration
SIGNING_IDENTITY="${SIGNING_IDENTITY:-"E87DEBE3C741ACD88DDB3E8C04E0FB01723C1EC4"}"

echo "🔐 Stage 4: Signing extension and main app..."

# Read the app bundle path from previous stage
APP_BUNDLE_PATH_FILE="$PROJECT_ROOT/scripts/macos/.app_bundle_path"
if [ ! -f "$APP_BUNDLE_PATH_FILE" ]; then
    echo "❌ App bundle path not found. Did previous stages complete successfully?"
    exit 1
fi

APP_BUNDLE=$(cat "$APP_BUNDLE_PATH_FILE")
EXTENSION_DIR="$APP_BUNDLE/Contents/PlugIns/$EXTENSION_NAME.appex"

# Verify extension directory exists
if [ ! -d "$EXTENSION_DIR" ]; then
    echo "❌ Extension directory not found. Did stage 3 complete successfully?"
    exit 1
fi

# Step 1: Sign the extension
echo "🔐 Signing extension with identity: $SIGNING_IDENTITY"

# Check if the binary is an executable or dylib
EXTENSION_BINARY="$EXTENSION_DIR/Contents/MacOS/$EXTENSION_NAME"
if file "$EXTENSION_BINARY" | grep -q "executable"; then
    echo "  Extension is an executable, signing with entitlements..."
    # For executables, sign with entitlements
    codesign --force --sign "$SIGNING_IDENTITY" \
        --options runtime \
        --timestamp \
        --entitlements "$PROJECT_ROOT/apple/entitlements.plist" \
        "$EXTENSION_DIR"
else
    echo "  Extension is a library, signing binary first..."
    # For dylibs, sign the binary first then the bundle
    codesign --force --sign "$SIGNING_IDENTITY" \
        --options runtime \
        --timestamp \
        --entitlements "$PROJECT_ROOT/apple/entitlements.plist" \
        "$EXTENSION_BINARY"
    
    codesign --force --sign "$SIGNING_IDENTITY" \
        --options runtime \
        --timestamp \
        --entitlements "$PROJECT_ROOT/apple/entitlements.plist" \
        "$EXTENSION_DIR"
fi

# Verify extension signature
if codesign --verify --verbose "$EXTENSION_DIR" 2>/dev/null; then
    echo "✅ Extension signature verified"
else
    echo "❌ Extension signature verification failed"
    exit 1
fi

# Step 2: Sign the main app with deep signing to include the extension
echo "🔐 Re-signing main app with embedded extension..."
codesign --force --sign "$SIGNING_IDENTITY" \
    --options runtime \
    --timestamp \
    --deep \
    --entitlements "$PROJECT_ROOT/apple/entitlements.plist" \
    "$APP_BUNDLE"

# Verify main app signature
if codesign --verify --verbose "$APP_BUNDLE" 2>/dev/null; then
    echo "✅ Main app signature verified"
else
    echo "❌ Main app signature verification failed"
    exit 1
fi

echo "✅ Stage 4 completed: Both extension and main app signed successfully"