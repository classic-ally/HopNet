#!/bin/bash
# Build the photo-ingress vertical slice: Rust staticlib -> UniFFI Swift
# bindings -> Swift executable.
#
# Rust builds run inside `nix shell` (the flake's rust is 1.93; sqlx needs
# 1.94+). The Swift build deliberately escapes nix: the nix-pinned apple-sdk
# mismatches the system Swift compiler (see spikes/photokit/FINDINGS.md).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
CRATES="$PROJECT_ROOT/crates"
PKG="$PROJECT_ROOT/apple/PhotoIngress"

echo "==> [1/4] cargo build -p ingress-ffi --release"
nix shell nixpkgs#rustc nixpkgs#cargo --command \
    cargo build --manifest-path "$CRATES/Cargo.toml" -p ingress-ffi --release

echo "==> [2/4] uniffi-bindgen (Swift, library mode)"
nix shell nixpkgs#rustc nixpkgs#cargo --command \
    cargo run --manifest-path "$CRATES/Cargo.toml" -p ingress-ffi --release --bin uniffi-bindgen -- \
    generate --library "$CRATES/target/release/libingress_ffi.a" \
    --language swift --out-dir "$CRATES/target/uniffi-swift"

echo "==> [3/4] distribute generated bindings into the package (committed)"
mkdir -p "$PKG/Sources/PhotoIngressKit/Generated" "$PKG/Sources/CIngressFFI/include"
cp "$CRATES/target/uniffi-swift/ingress_ffi.swift" "$PKG/Sources/PhotoIngressKit/Generated/"
cp "$CRATES/target/uniffi-swift/ingress_ffiFFI.h" "$PKG/Sources/CIngressFFI/include/"
cp "$CRATES/target/uniffi-swift/ingress_ffiFFI.modulemap" "$PKG/Sources/CIngressFFI/include/module.modulemap"

echo "==> [4/4] swift build (system Xcode SDK)"
cd "$PKG"
DEVELOPER_DIR="/Applications/Xcode.app/Contents/Developer"
SDKROOT="$(env -i DEVELOPER_DIR="$DEVELOPER_DIR" /usr/bin/xcrun --sdk macosx --show-sdk-path)"
env -i HOME="$HOME" USER="$USER" TERM="${TERM:-xterm}" \
    PATH="/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin" \
    DEVELOPER_DIR="$DEVELOPER_DIR" SDKROOT="$SDKROOT" \
    swift build --configuration release

echo "built: $PKG/.build/release/photo-ingress"
