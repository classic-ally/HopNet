set -euo pipefail

# Generate TypeScript types from Rust types
# This updates the frontend types for fault tolerance curve functionality

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🔧 Generating TypeScript types from Rust types..."

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
OUTPUT_FILE="$PROJECT_ROOT/frontend/src/lib/types.ts"

# Generate TypeScript types
echo "🔄 Running typeshare..."
"$TYPESHARE" \
    --lang typescript \
    --output-file "$OUTPUT_FILE" \
    "$COMMON_SRC"

if [ $? -eq 0 ]; then
    echo "✅ TypeScript types generated successfully"
    echo "📄 Output: $OUTPUT_FILE"
else
    echo "❌ Failed to generate TypeScript types"
    exit 1
fi

echo "✅ TypeScript types generation complete"