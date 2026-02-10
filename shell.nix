# Import the Nixpkgs library with rust overlay for latest Rust
let
  rustOverlay = import (builtins.fetchTarball "https://github.com/oxalica/rust-overlay/archive/master.tar.gz");
  pkgs = import <nixpkgs> { overlays = [ rustOverlay ]; };
  rustToolchain = pkgs.rust-bin.stable.latest.default.override {
    extensions = [ "rust-analyzer" "clippy" ];
  };
in

# Define the Nix expression for the app + its environment
pkgs.stdenv.mkDerivation {
  name = "rust";

  # Specify the packages to be available in the environment
  buildInputs = [
    rustToolchain

    # openssl
    pkgs.openssl.dev
    pkgs.pkg-config

    # frontend
    pkgs.nodejs_24
    pkgs.pnpm
  ];

  shellHook = ''
    export HOPNET_EPHEMERAL_DB=1
  '';
}
