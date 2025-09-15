#!/bin/bash
set -euo pipefail

# Stage 2: Build Main Rust Application with Tauri

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "🔨 Stage 2: Building main Rust application..."

# Build the main HopNet application (skip DMG creation)
cd "$PROJECT_ROOT"
cargo tauri build --bundles app

# Find the built app bundle
APP_BUNDLE=""
POSSIBLE_LOCATIONS=(
    "$PROJECT_ROOT/target/release/bundle/macos"
    "$PROJECT_ROOT/src-tauri/target/release/bundle/macos"
)

for location in "${POSSIBLE_LOCATIONS[@]}"; do
    if [ -d "$location" ]; then
        FOUND_APP=$(find "$location" -name "*.app" -type d | head -n 1)
        if [ -n "$FOUND_APP" ]; then
            APP_BUNDLE="$FOUND_APP"
            echo "📦 Found app bundle: $APP_BUNDLE"
            break
        fi
    fi
done

if [ -z "$APP_BUNDLE" ]; then
    echo "❌ No app bundle found after build"
    exit 1
fi

# Store the app bundle path for next stages
echo "$APP_BUNDLE" > "$PROJECT_ROOT/scripts/macos/.app_bundle_path"

echo "✅ Stage 2 completed: Main app built at $APP_BUNDLE"