{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
    };
  };

  outputs = { nixpkgs, rust-overlay, crane, ... }:
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
          rustToolchain = pkgs.rust-bin.stable.latest.default;
          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

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

          # crane shares one dependency-only build across every workspace
          # rebuild. `buildDepsOnly` synthesizes a stub src (Cargo.toml +
          # Cargo.lock only), compiles all third-party crates once, and
          # stashes the resulting target/ in the Nix store keyed on the
          # lockfile. Source-only edits then reuse those artifacts instead
          # of recompiling 400+ crates from scratch — big win on slow CI
          # boxes.
          commonArgs = {
            src = craneLib.cleanCargoSource ./.;
            strictDeps = true;

            cargoExtraArgs = "--features skip-frontend --bin hopnet";

            # iroh fork lives in [patch.crates-io]; crane reads outputHashes
            # from the same place buildRustPackage does.
            cargoVendorDir = craneLib.vendorCargoDeps {
              src = ./.;
              outputHashes = {
                "iroh-0.96.1" = "sha256-+nasc9F8OsegyrdDGN/WsZ4niIZEz7Qe44qPN82sKKU=";
              };
            };

            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.openssl ];

            doCheck = false;
          };

          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          hopnet = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
            pname = "hopnet";
            version = "0.1.0";

            preBuild = ''
              mkdir -p frontend/dist
              cp -r ${frontend}/* frontend/dist/
            '';
          });
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
        } // pkgs.lib.optionalAttrs (pkgs.stdenv.hostPlatform.system == "aarch64-darwin") {
          # Signed + notarized .app bundle pulled from Forgejo releases.
          # CI (.forgejo/workflows/release-macos.yml) produces this artifact;
          # bump `hopnetDesktopVersion` and `hopnetDesktopSha256` when cutting
          # a new release.
          hopnet-desktop =
            let
              version = "0.1.0-rc.2";
              sha256 = "cfc0fe2e8262c02ccc8d392d650d1a124a4be78433a6e77ea0f48c9f53b41321";
            in
            pkgs.stdenvNoCC.mkDerivation {
              pname = "hopnet-desktop";
              inherit version;

              src = pkgs.fetchurl {
                url = "https://git.bentley.sh/HopNet/HopNet/releases/download/v${version}/HopNet-v${version}-arm64.app.zip";
                inherit sha256;
              };

              nativeBuildInputs = [ pkgs.unzip ];

              # Skip default unpackPhase so we control extraction via ditto in
              # installPhase (preserves macOS resource forks + xattrs).
              dontUnpack = true;

              installPhase = ''
                runHook preInstall
                mkdir -p $out/Applications
                ${pkgs.unzip}/bin/unzip -q $src -d $out/Applications
                runHook postInstall
              '';

              # Don't re-sign — bundle is already Developer ID signed + notarized.
              dontFixup = true;

              meta = {
                description = "HopNet macOS desktop app (signed + notarized)";
                homepage = "https://hopnet.app";
                license = pkgs.lib.licenses.agpl3Only;
                platforms = [ "aarch64-darwin" ];
                sourceProvenance = [ pkgs.lib.sourceTypes.binaryNativeCode ];
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
