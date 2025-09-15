#!/bin/bash
set -euo pipefail

# Stage 1: Build Swift FileProvider Extension

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
SWIFT_PROJECT_DIR="$PROJECT_ROOT/apple/HopNetFileProvider"
EXTENSION_NAME="HopNetFileProviderExtension"

echo "🔨 Stage 1: Building Swift FileProvider extension..."

# Build the Swift package
cd "$SWIFT_PROJECT_DIR"
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