#!/bin/bash
set -euo pipefail

# HopNet macOS Build Runner
# Orchestrates multi-stage build process for HopNet with Swift FileProvider extension

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
MACOS_SCRIPTS_DIR="$SCRIPT_DIR/macos"

# Source local .env (gitignored) if present — supplies SIGNING_IDENTITY,
# APPLE_ID, TEAM_ID, NOTARY_PASSWORD without exporting them globally.
# See scripts/macos/.env.example for template.
if [ -f "$MACOS_SCRIPTS_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$MACOS_SCRIPTS_DIR/.env"
    set +a
fi

# Auto-detect codesign identity from login keychain when not explicitly set.
# Picks the first Developer ID Application cert; override via SIGNING_IDENTITY.
if [ -z "${SIGNING_IDENTITY:-}" ]; then
    SIGNING_IDENTITY=$(security find-identity -v -p codesigning login.keychain 2>/dev/null \
        | awk '/Developer ID Application/ {print $2; exit}')
fi
if [ -z "${SIGNING_IDENTITY:-}" ]; then
    echo "❌ No SIGNING_IDENTITY set and no Developer ID Application cert in login keychain."
    echo "   Either set SIGNING_IDENTITY (or copy scripts/macos/.env.example → .env)"
    echo "   or import a Developer ID cert into the login keychain."
    exit 1
fi
export SIGNING_IDENTITY

echo "🚀 Building HopNet with Swift FileProvider extension..."
echo "📍 Project root: $PROJECT_ROOT"
echo "🔐 Signing identity: $SIGNING_IDENTITY"
echo ""

# Clean up any previous build state
rm -f "$MACOS_SCRIPTS_DIR/.app_bundle_path"

# Stage 0: Generate Swift types from Rust types
echo "=================================================================================="
if [ -x "$MACOS_SCRIPTS_DIR/00-generate-swift-types.sh" ]; then
    "$MACOS_SCRIPTS_DIR/00-generate-swift-types.sh"
else
    echo "❌ Stage 0 script not found or not executable"
    exit 1
fi

# Stage 1: Build Swift FileProvider Extension
echo ""
echo "=================================================================================="
if [ -x "$MACOS_SCRIPTS_DIR/01-build-swift-extension.sh" ]; then
    "$MACOS_SCRIPTS_DIR/01-build-swift-extension.sh"
else
    echo "❌ Stage 1 script not found or not executable"
    exit 1
fi

# Stage 2: Build Main Rust Application
echo ""
echo "=================================================================================="
if [ -x "$MACOS_SCRIPTS_DIR/02-build-rust-app.sh" ]; then
    "$MACOS_SCRIPTS_DIR/02-build-rust-app.sh"
else
    echo "❌ Stage 2 script not found or not executable"
    exit 1
fi

# Stage 3: Create Extension Bundle
echo ""
echo "=================================================================================="
if [ -x "$MACOS_SCRIPTS_DIR/03-create-extension-bundle.sh" ]; then
    "$MACOS_SCRIPTS_DIR/03-create-extension-bundle.sh"
else
    echo "❌ Stage 3 script not found or not executable"
    exit 1
fi

# Stage 4: Sign Both Extension and Main App
echo ""
echo "=================================================================================="
if [ -x "$MACOS_SCRIPTS_DIR/04-sign-bundles.sh" ]; then
    "$MACOS_SCRIPTS_DIR/04-sign-bundles.sh"
else
    echo "❌ Stage 4 script not found or not executable"
    exit 1
fi

# Stages 5+6: notarize + package. Only run when notary creds present
# (CI sets APPLE_ID/TEAM_ID/NOTARY_PASSWORD; dev builds skip these).
if [ -n "${APPLE_ID:-}" ] && [ -n "${TEAM_ID:-}" ] && [ -n "${NOTARY_PASSWORD:-}" ]; then
    echo ""
    echo "=================================================================================="
    "$MACOS_SCRIPTS_DIR/05-notarize-staple.sh"

    echo ""
    echo "=================================================================================="
    "$MACOS_SCRIPTS_DIR/06-package-zip.sh"
else
    echo ""
    echo "ℹ️  Skipping notarize + package (APPLE_ID/TEAM_ID/NOTARY_PASSWORD not set)"
fi

# Final verification and cleanup
echo ""
echo "=================================================================================="
echo "🔍 Final verification..."

# Read final app bundle path
if [ -f "$MACOS_SCRIPTS_DIR/.app_bundle_path" ]; then
    APP_BUNDLE=$(cat "$MACOS_SCRIPTS_DIR/.app_bundle_path")
    EXTENSION_DIR="$APP_BUNDLE/Contents/PlugIns/HopNetFileProviderExtension.appex"
    EXTENSION_BINARY="$EXTENSION_DIR/Contents/MacOS/HopNetFileProviderExtension"
    
    echo "📦 App bundle: $APP_BUNDLE"
    echo "📦 Extension: $EXTENSION_DIR"
    
    if [ -f "$EXTENSION_BINARY" ] && [ -x "$EXTENSION_BINARY" ]; then
        echo "✅ Extension binary exists and is executable"
    else
        echo "❌ Extension binary missing or not executable"
        exit 1
    fi
    
    # Clean up temporary file
    rm -f "$MACOS_SCRIPTS_DIR/.app_bundle_path"
    
    echo ""
    echo "🎉 Build complete! HopNet with Swift FileProvider extension ready."
    echo "📍 App bundle: $APP_BUNDLE"
    echo "📍 Extension: $EXTENSION_DIR"
    echo ""
    echo "To test the FileProvider extension:"
    echo "1. Run the app: open \"$APP_BUNDLE\""
    echo "2. Check Console.app for extension logs (search for 'HopNetFileProviderExtension')"
    echo "3. Look for FileProvider registration in System Preferences > Extensions"
else
    echo "❌ App bundle path not found. Build may have failed."
    exit 1
fi