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

  outputs = { self, nixpkgs, rust-overlay, crane, ... }:
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
              hash = "sha256-JzfiIihYxpXVjFYbVxDaz3jUNJK6RXC3suFje6aMay0=";
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
                "iroh-0.96.1" = "sha256-DeIK89ULzQzK2Qvkje8oGxU9sN9sIN0GkCys9DXZtCg=";
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

          # Linux-only FUSE daemon (RFC-018). Its own deps-only artifact:
          # commonArgs hardcodes `--bin hopnet`, so the shared artifact
          # set never compiled fuser and friends. Shares the vendor dir —
          # the [patch.crates-io] iroh fork applies workspace-wide.
          mountArgs = commonArgs // {
            cargoExtraArgs = "-p hopnet-mount";
            buildInputs = [ pkgs.openssl pkgs.fuse3 ];
          };

          hopnet-mount = craneLib.buildPackage (mountArgs // {
            cargoArtifacts = craneLib.buildDepsOnly mountArgs;
            pname = "hopnet-mount";
            version = "0.1.0";
            meta.mainProgram = "hopnet-mount";
          });

          # Self-hosted iroh relay, built from the SAME fork/rev the nodes
          # link against (relay protocol compatibility). Ships inside the
          # docker image; the orchestrator runs one per mesh with
          # `iroh-relay --dev` (plain HTTP, no TLS) and points nodes at it
          # via HOPNET_RELAY_URL — removing the n0 public relay/DNS
          # dependency from all mesh tests.
          iroh-relay = craneLib.buildPackage {
            pname = "iroh-relay";
            version = "0.98.2";
            src = pkgs.fetchFromGitHub {
              owner = "classic-ally";
              repo = "iroh";
              rev = "d97650851c16d002c6b8cb87e64b9b906889171c";
              hash = "sha256-DeIK89ULzQzK2Qvkje8oGxU9sN9sIN0GkCys9DXZtCg=";
            };
            strictDeps = true;
            cargoExtraArgs = "-p iroh-relay --features server --bin iroh-relay";
            # The iroh repo ships a .cargo/config.toml pinning cross linkers
            # (aarch64-linux-gnu-gcc) that don't exist on a native builder.
            postPatch = ''
              rm -f .cargo/config.toml .cargo/config
            '';
            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.openssl ];
            doCheck = false;
          };

          # The photo-viewer SPA. Needs BOTH its own sources and HopNet's
          # frontend/src/lib in the tree: the vite `$ui` alias resolves
          # ../../../frontend/src/lib (zero-copy primitive reuse), so the
          # source root spans the repo, and we build from the viewer subdir.
          viewer-frontend = pkgs.stdenvNoCC.mkDerivation {
            pname = "ingress-viewer-frontend";
            version = "0.1.0";
            src = pkgs.lib.fileset.toSource {
              root = ./.;
              fileset = pkgs.lib.fileset.unions [
                ./frontend/src/lib
                ./frontend/uno.config.ts
                (pkgs.lib.fileset.difference
                  ./crates/ingress-server/frontend
                  (pkgs.lib.fileset.unions [
                    (pkgs.lib.fileset.maybeMissing ./crates/ingress-server/frontend/node_modules)
                    (pkgs.lib.fileset.maybeMissing ./crates/ingress-server/frontend/dist)
                    (pkgs.lib.fileset.maybeMissing ./crates/ingress-server/frontend/storybook-static)
                  ]))
              ];
            };
            sourceRoot = "source/crates/ingress-server/frontend";

            pnpmDeps = pkgs.fetchPnpmDeps rec {
              pname = "ingress-viewer-frontend";
              version = "0.1.0";
              src = ./crates/ingress-server/frontend;
              # pnpm >= 10.34 (nixpkgs 26.11) keeps its store index in SQLite;
              # fetcherVersion 3's tarball normalization breaks it (offline
              # install then fails ERR_PNPM_NO_OFFLINE_TARBALL) and 4 doesn't
              # exist on older nixpkgs — so gate on the pnpm that will run.
              fetcherVersion =
                if builtins.compareVersions pkgs.pnpm_10.version "10.34" >= 0 then 4 else 3;
              # fetcherVersion 3's tarball normalization is platform-independent
              # (verified: aarch64-darwin and x86_64-linux produce the same
              # hash under pnpm 10.30). Keyed on fetcherVersion because v4
              # output differs. NOTE: as of nixpkgs e73de5b (26.11 unstable),
              # pnpm 10.34 + fetcherVersion 4 offline install is broken
              # (ERR_PNPM_NO_OFFLINE_TARBALL with a complete store) — build
              # this package with a nixpkgs whose pnpm_10 is < 10.34.
              hash =
                {
                  "3" = "sha256-1k12pSoM6rgaNXhl6rUT7X5h3oTNiQuqeFMZx6UdOZU=";
                  # STALE: not recomputed after the fontsource dep change (needs
                  # pnpm >= 10.34 to produce, and that path is broken per above).
                  "4" = "sha256-rVTjvm5YduXQov0pB3biMh8plNtz7OylJ7rTg77CEug=";
                }.${toString fetcherVersion} or pkgs.lib.fakeHash;
            };

            nativeBuildInputs = [ pkgs.nodejs pkgs.pnpm_10 pkgs.pnpmConfigHook ];

            # pnpm 10.34+ re-verifies the minimumReleaseAge supply-chain
            # policy at install time; offline (in the sandbox) there is no
            # registry metadata, so verification rejects entries and surfaces
            # as ERR_PNPM_NO_OFFLINE_TARBALL. The online fetchPnpmDeps step
            # already enforced the policy — trust its result here.
            # (pnpmConfigHook only does this for pnpm >= 11.)
            prePnpmInstall = ''
              export pnpm_config_trust_lockfile=true
              pnpm config set minimum-release-age 0
            '';

            buildPhase = ''
              runHook preBuild
              # The re-exported repo-root frontend/uno.config.ts resolves its
              # @unocss/* imports from ITS directory; give it this package's
              # node_modules (the presets are declared in our package.json).
              chmod u+w ../../../frontend
              ln -s "$PWD/node_modules" ../../../frontend/node_modules
              pnpm build
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              cp -r dist $out
              runHook postInstall
            '';
          };

          # The photo-viewer backend. Lives in the standalone `crates/`
          # workspace (own Cargo.lock, no iroh patch). Built with
          # buildRustPackage rather than crane so `cargo build -p
          # ingress-server` naturally skips the sibling ingress-ffi (whose
          # UniFFI build.rs would trip crane's deps-only dummy build). Needs
          # C libs for the Renderer: libheif (HEIC, via pkg-config) and
          # ffmpeg 7 (video posters, via bindgen — ffmpeg 8 dropped avfft.h).
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };
          ingress-server = rustPlatform.buildRustPackage {
            pname = "ingress-server";
            version = "0.1.0";
            src = craneLib.cleanCargoSource ./crates;
            cargoLock.lockFile = ./crates/Cargo.lock;
            cargoBuildFlags = [ "-p" "ingress-server" ];
            doCheck = false;
            nativeBuildInputs = [ pkgs.pkg-config pkgs.rustPlatform.bindgenHook ];
            buildInputs = [ pkgs.libheif pkgs.ffmpeg_7 ];
            BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.ffmpeg_7.dev}/include";
            # include_dir! bakes the SPA into the binary at compile time;
            # cleanCargoSource strips frontend/, so restore the built dist.
            preBuild = ''
              mkdir -p ingress-server/frontend/dist
              cp -r ${viewer-frontend}/* ingress-server/frontend/dist/
            '';
            meta = {
              description = "Read-only web viewer for the Apple Photos ingress blob store";
              mainProgram = "ingress-server";
            };
          };
        in {
          default = hopnet;
          inherit ingress-server;
        } // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          inherit hopnet-mount;

          dockerImage = pkgs.dockerTools.buildLayeredImage {
            name = "hopnet";
            tag = "latest";
            # iroh-relay rides along so the orchestrator can run a relay
            # container from this same image (entrypoint override).
            # hopnet-mount + fuse3 serve the mount-cross-node-consistency
            # test (fusermount3 for the daemon's stale-mount cleanup);
            # busybox gives docker exec a shell for debugging and test IO.
            contents = [ hopnet hopnet-mount pkgs.fuse3 pkgs.busybox iroh-relay ];
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

      # hopnet-mount as a per-user service (RFC-018 S8). One shared unit
      # definition, two consumption flavors: home-manager for users who
      # manage their own profile, NixOS `systemd.user.services` for
      # host-level config. The daemon runs per-user either way — the
      # node itself may run as a different (system) user; pair with
      # `hopnet-mount login` in that deployment.
      homeManagerModules = rec {
        hopnet-mount = import ./nix/hopnet-mount-module.nix {
          inherit self;
          flavor = "hm";
        };
        default = hopnet-mount;
      };

      nixosModules = rec {
        hopnet-mount = import ./nix/hopnet-mount-module.nix {
          inherit self;
          flavor = "nixos";
        };
        default = hopnet-mount;
      };

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

            # ingress-ffi smoke tests read state.db back through the sqlite3
            # CLI rather than take a sqlx dev-dep; macOS ships one, Linux
            # does not, so the shell has to supply it.
            pkgs.sqlite
          ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
            # hopnet-mount (RFC-018): fuser links libfuse3
            pkgs.fuse3
          ];

          shellHook = ''
            export HOPNET_EPHEMERAL_DB=1
          '';
        };
      });
    };
}
