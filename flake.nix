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

          generatedTypes = pkgs.stdenvNoCC.mkDerivation {
            pname = "hopnet-generated-types";
            version = "0.1.0";
            src = pkgs.lib.fileset.toSource {
              root = ./.;
              fileset = pkgs.lib.fileset.unions [
                ./common/src
                ./typeshare.toml
              ];
            };

            nativeBuildInputs = [ pkgs.typeshare ];

            buildPhase = ''
              runHook preBuild
              typeshare \
                --config-file typeshare.toml \
                --lang typescript \
                --output-file types.ts \
                common/src
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out
              cp types.ts $out/
              runHook postInstall
            '';
          };

          frontend = pkgs.stdenvNoCC.mkDerivation {
            pname = "hopnet-frontend";
            version = "0.1.0";
            src = ./frontend;

            pnpmDeps = pkgs.fetchPnpmDeps {
              pname = "hopnet-frontend";
              version = "0.1.0";
              src = ./frontend;
              fetcherVersion = 3;
              hash = "sha256-TKz2TdQcNkBNjMgp3ES8fLbu+7hy5thwAPSJ9gv1ITA=";
            };

            nativeBuildInputs = [ pkgs.nodejs pkgs.pnpm_10 pkgs.pnpmConfigHook ];

            buildPhase = ''
              runHook preBuild
              cp ${generatedTypes}/types.ts src/lib/types.ts
              pnpm build
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              cp -r dist $out
              runHook postInstall
            '';
          };

          hopnet = rustPlatform.buildRustPackage {
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

            preBuild = ''
              mkdir -p frontend/dist
              cp -r ${frontend}/* frontend/dist/
            '';

            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.openssl ];

            doCheck = false;
          };
        in {
          default = hopnet;
        } // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          dockerImage = pkgs.dockerTools.buildLayeredImage {
            name = "hopnet";
            tag = "latest";
            contents = [ hopnet ];
            config = {
              Entrypoint = [ "${hopnet}/bin/hopnet" ];
              ExposedPorts."34632/tcp" = {};
              Env = [ "RUST_LOG=warn,hopnet=debug" ];
            };
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
