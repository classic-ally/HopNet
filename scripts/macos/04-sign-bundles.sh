#!/bin/bash
set -euo pipefail

# Stage 4: Sign Both Extension and Main App

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
EXTENSION_NAME="HopNetFileProviderExtension"

# Source .env if present (when running standalone, not via wrapper)
if [ -z "${SIGNING_IDENTITY:-}" ] && [ -f "$PROJECT_ROOT/scripts/macos/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$PROJECT_ROOT/scripts/macos/.env"
    set +a
fi

# Auto-detect from login keychain when still unset
if [ -z "${SIGNING_IDENTITY:-}" ]; then
    SIGNING_IDENTITY=$(security find-identity -v -p codesigning login.keychain 2>/dev/null \
        | awk '/Developer ID Application/ {print $2; exit}')
fi
if [ -z "${SIGNING_IDENTITY:-}" ]; then
    echo "❌ No SIGNING_IDENTITY set and no Developer ID Application cert in login keychain."
    exit 1
fi

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

# Step 0: Sign the photo-ingress daemon (inner-first: daemon → appex →
# outer app). Hardened runtime + timestamp for notarization; deliberately
# NO entitlements file and NO provisioning profile — the daemon is
# unsandboxed (arbitrary blob roots, incl. SMB mounts) and carries no
# restricted entitlements, so there is nothing for AMFI to match against a
# profile. Its signing identifier comes from the __TEXT,__info_plist
# CFBundleIdentifier (com.hopnet.desktop.photo-ingress), which is what TCC
# keys the Photos grant on — keep the cert stable or grants reset.
DAEMON_BINARY="$APP_BUNDLE/Contents/MacOS/photo-ingress"
if [ -f "$DAEMON_BINARY" ]; then
    echo "🔐 Signing photo-ingress daemon..."
    codesign --force --sign "$SIGNING_IDENTITY" \
        --options runtime \
        --timestamp \
        "$DAEMON_BINARY"
    if codesign --verify --verbose "$DAEMON_BINARY" 2>/dev/null; then
        echo "✅ Daemon signature verified"
    else
        echo "❌ Daemon signature verification failed"
        exit 1
    fi
else
    echo "❌ photo-ingress daemon not found. Did stage 3b complete successfully?"
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

# Step 2: Sign the main app with its OWN entitlements (without --deep so the
# extension's signature is preserved; --deep re-signs nested binaries with the
# outer entitlements, which would strip fileprovider.testing-mode from the
# extension).
#
# Main app entitlements omit fileprovider.testing-mode + the per-extension
# application/keychain groups because the main app's bundle ID has no
# provisioning profile granting them, and AMFI rejects the launch with
# "No matching profile found" otherwise.
echo "🔐 Re-signing main app with embedded extension..."
codesign --force --sign "$SIGNING_IDENTITY" \
    --options runtime \
    --timestamp \
    --entitlements "$PROJECT_ROOT/apple/entitlements-app.plist" \
    "$APP_BUNDLE"

# Verify main app signature
if codesign --verify --verbose "$APP_BUNDLE" 2>/dev/null; then
    echo "✅ Main app signature verified"
else
    echo "❌ Main app signature verification failed"
    exit 1
fi

echo "✅ Stage 4 completed: Both extension and main app signed successfully"