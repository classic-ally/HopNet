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
      packages = forAllSystems (pkgs:
        let
          rustPlatform = pkgs.makeRustPlatform {
            cargo = pkgs.rust-bin.stable.latest.default;
            rustc = pkgs.rust-bin.stable.latest.default;
          };
        in {
          default = rustPlatform.buildRustPackage {
            pname = "hopnet";
            version = "0.1.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
              outputHashes = {
                "iroh-0.96.1" = "sha256-+nasc9F8OsegyrdDGN/WsZ4niIZEz7Qe44qPN82sKKU=";
              };
            };

            cargoBuildFlags = [ "--bin" "hopnet" ];
            buildFeatures = [ "skip-frontend" ];

            preBuild = "mkdir -p frontend/dist";

            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.openssl ];

            doCheck = false;
          };
        }
      );

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
