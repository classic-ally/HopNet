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

# Fix any Nix store linkages to use system libraries
MAIN_BINARY="$APP_BUNDLE/Contents/MacOS/HopNet"
if [ -f "$MAIN_BINARY" ]; then
    echo "🔧 Checking for Nix linkages..."

    # Find all Nix store paths in the binary
    NIX_LIBS=$(otool -L "$MAIN_BINARY" | grep '/nix/store' | awk '{print $1}' || true)

    if [ -n "$NIX_LIBS" ]; then
        echo "⚠️  Found Nix linkages - fixing for portability..."

        while IFS= read -r nix_path; do
            # Extract the library name
            lib_name=$(basename "$nix_path")

            # Determine system path based on library
            if [[ "$lib_name" == libiconv* ]]; then
                system_path="/usr/lib/libiconv.2.dylib"
            elif [[ "$lib_name" == libz* ]]; then
                system_path="/usr/lib/libz.1.dylib"
            elif [[ "$lib_name" == libssl* ]] || [[ "$lib_name" == libcrypto* ]]; then
                # OpenSSL - use system location
                system_path="/usr/lib/$lib_name"
            else
                echo "  ⚠️  Unknown Nix library: $lib_name - skipping"
                continue
            fi

            echo "  Replacing: $nix_path"
            echo "       with: $system_path"
            install_name_tool -change "$nix_path" "$system_path" "$MAIN_BINARY"
        done <<< "$NIX_LIBS"

        echo "✅ Fixed Nix linkages - binary is now portable"
    else
        echo "✅ No Nix linkages found - binary is portable"
    fi
else
    echo "⚠️  Main binary not found at expected location"
fi

# Remove orchestrator binary if it was included (we don't want it in the .app)
ORCHESTRATOR_BINARY="$APP_BUNDLE/Contents/MacOS/orchestrator"
if [ -f "$ORCHESTRATOR_BINARY" ]; then
    echo "🗑️  Removing orchestrator binary from app bundle (not needed for distribution)"
    rm "$ORCHESTRATOR_BINARY"
fi

# Store the app bundle path for next stages
echo "$APP_BUNDLE" > "$PROJECT_ROOT/scripts/macos/.app_bundle_path"

echo "✅ Stage 2 completed: Main app built at $APP_BUNDLE"