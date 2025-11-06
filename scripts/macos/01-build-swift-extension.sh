#!/bin/bash
set -euo pipefail

# Stage 1: Build Swift FileProvider Extension

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
SWIFT_PROJECT_DIR="$PROJECT_ROOT/apple/HopNetFileProvider"
EXTENSION_NAME="HopNetFileProviderExtension"

echo "🔨 Stage 1: Building Swift FileProvider extension..."

# Use real Xcode SDK path (not Nix's)
XCODE_SDK="/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"

if [ ! -d "$XCODE_SDK" ]; then
    echo "❌ Xcode SDK not found at: $XCODE_SDK"
    echo "Please ensure Xcode is installed and run: xcode-select --install"
    exit 1
fi

echo "📍 Using Xcode SDK: $XCODE_SDK"

# Build the Swift package with clean environment
cd "$SWIFT_PROJECT_DIR"
env -i \
    HOME="$HOME" \
    USER="$USER" \
    PATH="/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin" \
    SDKROOT="$XCODE_SDK" \
    DEVELOPER_DIR="/Applications/Xcode.app/Contents/Developer" \
    swift build --configuration release

# Verify the built binary exists
SWIFT_BINARY="$SWIFT_PROJECT_DIR/.build/release/$EXTENSION_NAME"
if [ -f "$SWIFT_BINARY" ]; then
    echo "✅ Swift binary built successfully at: $SWIFT_BINARY"
else
    echo "❌ Swift binary not found at: $SWIFT_BINARY"
    exit 1
fi

echo "✅ Stage 1 completed: Swift extension built"