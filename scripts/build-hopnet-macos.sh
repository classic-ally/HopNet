#!/bin/bash
set -euo pipefail

# HopNet macOS Build Runner
# Orchestrates multi-stage build process for HopNet with Swift FileProvider extension

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
MACOS_SCRIPTS_DIR="$SCRIPT_DIR/macos"

# Configuration
SIGNING_IDENTITY="${SIGNING_IDENTITY:-"E87DEBE3C741ACD88DDB3E8C04E0FB01723C1EC4"}"
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