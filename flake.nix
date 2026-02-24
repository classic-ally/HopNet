{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, rust-overlay, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = fn: nixpkgs.lib.genAttrs systems (system:
        fn (import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        })
      );
    in
    {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          buildInputs = [
            (pkgs.rust-bin.stable.latest.default.override {
              extensions = [ "clippy" "rust-src" ];
            })
            pkgs.rust-analyzer

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
        };
      });
    };
}
