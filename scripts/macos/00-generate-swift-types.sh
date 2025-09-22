#!/bin/bash
set -euo pipefail

# Stage 0: Generate Swift types from Rust types
# This must run before building the Swift FileProvider extension

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "🔧 Stage 0: Generating Swift types from Rust types..."

# Check if typeshare is installed
if ! command -v typeshare &> /dev/null && ! command -v ~/.cargo/bin/typeshare &> /dev/null; then
    echo "⚠️  typeshare not found. Installing..."
    cargo install typeshare-cli
fi

# Determine typeshare path
if command -v typeshare &> /dev/null; then
    TYPESHARE="typeshare"
else
    TYPESHARE="$HOME/.cargo/bin/typeshare"
fi

# Define paths
COMMON_SRC="$PROJECT_ROOT/common/src"
OUTPUT_FILE="$PROJECT_ROOT/apple/HopNetFileProvider/Sources/HopNetFileProviderExtension/Generated/HopNetTypes.swift"
OUTPUT_DIR="$(dirname "$OUTPUT_FILE")"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Generate Swift types
echo "🔄 Running typeshare..."
"$TYPESHARE" \
    --lang swift \
    --output-file "$OUTPUT_FILE" \
    "$COMMON_SRC"

if [ $? -eq 0 ]; then
    echo "✅ Swift types generated successfully"
    echo "📄 Output: $OUTPUT_FILE"

    # Remove TakeoutRecord from Swift types (not needed for FileProvider)
    echo "🔧 Removing TakeoutRecord from Swift types..."
    sed -i '' '/^\/\/\/ Takeout record for user data export requests$/,/^}$/d' "$OUTPUT_FILE"
    echo "✅ TakeoutRecord removed from Swift types"
else
    echo "❌ Failed to generate Swift types"
    exit 1
fi

echo "✅ Stage 0 complete: Swift types generated"