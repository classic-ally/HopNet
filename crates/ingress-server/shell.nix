# Dev shell for building ingress-server, which needs C libraries for the
# Renderer (slice 2b): libheif (HEIC decode, via pkg-config) and ffmpeg 7
# (video poster frames, via bindgen). bindgenHook sets LIBCLANG_PATH; we add
# the ffmpeg dev include so ffmpeg-sys's bindgen finds the headers.
#
# Usage:  nix-shell crates/ingress-server/shell.nix --run 'cargo build -p ingress-server'
#
# Pin ffmpeg_7, NOT ffmpeg 8 — ffmpeg 8 removed libavcodec/avfft.h, which
# ffmpeg-sys-next 7.x still #includes (build fails otherwise).
#
# For thor's nix-config, the equivalent derivation buildInputs are
# [ libheif ffmpeg_7 ] with nativeBuildInputs [ pkg-config rustPlatform.bindgenHook ].
{ pkgs ? import <nixpkgs> { } }:
pkgs.mkShell {
  nativeBuildInputs = [
    pkgs.rustc
    pkgs.cargo
    pkgs.pkg-config
    pkgs.rustPlatform.bindgenHook
  ];
  buildInputs = [
    pkgs.libheif
    pkgs.ffmpeg_7
  ];
  # ffmpeg-sys-next's bindgen needs the ffmpeg headers on the clang include path.
  BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.ffmpeg_7.dev}/include";
}
