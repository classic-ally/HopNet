# HopNet headless node (RFC-021): the unit execs hopnet through a
# service-owned profile symlink instead of a pinned store path, so the
# mesh-coordinated upgrade path can atomically flip the binary with no
# rebuild and no operator. The flake-pinned package only SEEDS the
# profile, newest-wins: a deliberate flake bump moves the profile
# forward, a rebuild never regresses a self-upgrade.
#
# NixOS flavor only for now (nix-darwin/launchd deferred — RFC-021).
{ self }:
{ config, lib, pkgs, ... }:
let
  cfg = config.services.hopnet;

  profile = "${cfg.dataDir}/profile";
  stageDir = "${cfg.dataDir}/staged";

  # Newest-WITHIN-AGREEMENT seeding (RFC-025). Atomic on both arms:
  # build the candidate link beside the profile, rename over. `sort -V`
  # orders CalVer correctly. Interpolating ${cfg.package} here also
  # roots the seed generation in the system closure — the profile
  # indirection never leaves the seed collectable.
  #
  # The advance arm asks `hopnet seed-guard` (the flake binary — it is
  # in the closure and understands the markers) whether the mesh
  # agreement permits the candidate: a flake pin beyond what the mesh
  # agreed is HELD, not seeded — `nixos-rebuild switch` can no longer
  # move a node past its mesh mid-epoch. A held pin is not lost: when
  # the mesh agrees and seals, RFC-021 activation flips the profile
  # through its own doubly-authorized path. The bootstrap arm (no
  # usable profile) stays unguarded — availability wins on a wiped
  # profile, and the boot-time version-ahead gate is the safety net.
  # The guard runs with the unit's Environment (XDG_DATA_HOME), so it
  # resolves the same data dir as the daemon; any non-zero exit means
  # "don't seed".
  seedScript = pkgs.writeShellScript "hopnet-seed-profile" ''
    set -eu
    export PATH=${lib.makeBinPath [ pkgs.coreutils ]}
    seed() {
      ln -sfn ${cfg.package} "${profile}.seed"
      mv -T "${profile}.seed" "${profile}"
    }
    current_ver=""
    if [ -x "${profile}/bin/hopnet" ]; then
      current_ver=$("${profile}/bin/hopnet" --version 2>/dev/null || true)
    fi
    if [ -z "$current_ver" ]; then
      seed
      exit 0
    fi
    flake_ver="${cfg.package.version}"
    newest=$(printf '%s\n%s\n' "$flake_ver" "$current_ver" | sort -V | tail -n1)
    if [ "$newest" = "$flake_ver" ] && [ "$flake_ver" != "$current_ver" ]; then
      if ${cfg.package}/bin/hopnet seed-guard --candidate "$flake_ver"; then
        echo "hopnet: flake pin $flake_ver is newer than profile $current_ver — re-seeding"
        seed
      else
        echo "hopnet: flake pin $flake_ver held — the mesh agreement pins the runnable version"
      fi
    fi
  '';
in
{
  options.services.hopnet = {
    enable = lib.mkEnableOption "the HopNet distributed filesystem node";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.hopnet;
      defaultText = lib.literalExpression "hopnet.packages.<system>.hopnet";
      description = "The hopnet package seeding the service profile.";
    };

    dataDir = lib.mkOption {
      type = lib.types.path;
      default = "/var/lib/hopnet";
      description = ''
        Data directory: SQLite database, fragment storage, the exec profile
        and staged generations. Set `fragmentsDir` to move bulk storage
        elsewhere and leave only the latency-sensitive database here.
      '';
    };

    fragmentsDir = lib.mkOption {
      type = lib.types.path;
      default = "${cfg.dataDir}/hopnet/fragments";
      defaultText = lib.literalExpression ''"''${cfg.dataDir}/hopnet/fragments"'';
      description = ''
        Where content-addressed fragments live. The default is exactly where
        they land today, so no existing deployment moves on upgrade.

        Split this onto bulk storage when `dataDir` is on a fast disk. The
        database is small and randomly written, and every synced write gates
        the consensus round this node proposes — on spinning ZFS an fsync
        costs ~146 ms against ~0.55 ms on NVMe, which shows up directly as
        multi-second file operations for every client in the mesh. Fragments
        are bulk, sequential and latency-insensitive.

        Migrating an existing node: stop it, move everything EXCEPT
        `hopnet/fragments` to the new `dataDir`, point `fragmentsDir` at the
        old location, rebuild, start. Note the doubled path segment in the
        default — fragments live at `<dataDir>/hopnet/fragments`, not
        `<dataDir>/fragments`. Getting it wrong is caught at boot: the node
        refuses to start when the database claims fragments the store does
        not have.

        Must not be under `/home`: the unit sets `ProtectHome = true`.
      '';
    };

    relayUrl = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Self-hosted iroh relay URL (e.g. http://relay:3340). Null uses the public n0 relay network.";
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
      description = "RUST_LOG filter string.";
    };

    upgrade = {
      autoStage = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          Proactively build newly released stable versions into the nix
          store (out-link under the data directory) so the node can
          honestly attest them staged — what makes an upgrade epoch
          decidable by the mesh (RFC-021).
        '';
      };

      autoActivate = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          Cross an upgrade boundary unattended: flip the exec profile to
          the staged generation the quorum decided on and restart. Off,
          the node parks awaiting-upgrade for an operator, exactly as a
          deployment with no provider would (RFC-021 contract rule 3).
        '';
      };

      flakeRef = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = ''
          Base flake ref releases are staged from ("?ref=v<version>" is
          appended). Null derives the upstream repository from the binary
          itself; set for forks and mirrors.
        '';
      };

      nixBin = lib.mkOption {
        type = lib.types.path;
        default = "${pkgs.nix}/bin/nix";
        defaultText = lib.literalExpression ''"''${pkgs.nix}/bin/nix"'';
        description = "The nix binary stage() invokes (the VM test points it at a stub).";
      };

      releaseUrl = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Releases API endpoint for availability polling. Null derives the upstream default.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    users.users.hopnet = {
      isSystemUser = true;
      group = "hopnet";
      home = cfg.dataDir;
      createHome = true;
    };
    users.groups.hopnet = { };

    systemd.services.hopnet = {
      description = "HopNet distributed filesystem node";
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        RUST_LOG = cfg.logLevel;
        # Resolves the node's own directory to ${dataDir}/hopnet. Deliberately
        # NOT HOPNET_DATA_DIR: that variable takes the directory verbatim, so
        # switching would move every existing deployment's database up one
        # level.
        XDG_DATA_HOME = cfg.dataDir;
        HOPNET_FRAGMENTS_DIR = cfg.fragmentsDir;
        # nix evaluation/fetch caches as the service user.
        XDG_CACHE_HOME = "${cfg.dataDir}/.cache";
        SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
        # The RFC-021 deployment contract (src/upgrade/nix_provider.rs).
        HOPNET_UPGRADE_PROVIDER = "nix";
        HOPNET_UPGRADE_NIX_BIN = "${cfg.upgrade.nixBin}";
        HOPNET_UPGRADE_PROFILE = profile;
        HOPNET_UPGRADE_STAGE_DIR = stageDir;
        HOPNET_UPGRADE_AUTO_STAGE = if cfg.upgrade.autoStage then "1" else "0";
        HOPNET_UPGRADE_AUTO_ACTIVATE = if cfg.upgrade.autoActivate then "1" else "0";
      } // lib.optionalAttrs (cfg.relayUrl != null) {
        HOPNET_RELAY_URL = cfg.relayUrl;
      } // lib.optionalAttrs (cfg.upgrade.flakeRef != null) {
        HOPNET_UPGRADE_FLAKE_REF = cfg.upgrade.flakeRef;
      } // lib.optionalAttrs (cfg.upgrade.releaseUrl != null) {
        HOPNET_UPGRADE_RELEASE_URL = cfg.upgrade.releaseUrl;
      };

      # nix shells out to git to fetch a `git+https` flake ref, and a
      # systemd unit gets systemd's minimal PATH — without this, staging
      # dies on `executing "git": No such file or directory`, which is
      # exactly how the first real release attempt failed on every node
      # (every test stubs the nix binary, so none of them can catch it).
      path = [ pkgs.git ];

      serviceConfig = {
        User = "hopnet";
        Group = "hopnet";
        ExecStartPre = seedScript;
        # THE indirection: the unit is version-stable by design; only the
        # profile symlink moves (seeding above, RFC-021 activation, or a
        # newer flake pin).
        ExecStart = "${profile}/bin/hopnet";
        # Covers exit 75, the restart-request code the regenesis boundary
        # and the activation flip both exit with.
        Restart = "on-failure";
        RestartSec = 5;

        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        # The daemon socket needs write for connect(2); staging builds go
        # through the daemon, never a local chroot store.
        ReadWritePaths = lib.unique [
          cfg.dataDir
          cfg.fragmentsDir
          "/nix/var/nix/daemon-socket"
        ];
      };

      # Both paths, because they can be separate filesystems. Without the
      # fragments entry the node may start before its bulk storage is
      # mounted, create the store directory on whatever sits under the
      # mountpoint, and write fragments there — where they vanish beneath
      # the real filesystem once it arrives.
      unitConfig.RequiresMountsFor = lib.unique [ cfg.dataDir cfg.fragmentsDir ];
    };

    # `users.users.hopnet.createHome` only covers dataDir, so a fragment
    # store outside it would not exist and the node could not create it
    # against a root-owned parent.
    systemd.tmpfiles.rules =
      lib.optional (!lib.hasPrefix "${toString cfg.dataDir}/" (toString cfg.fragmentsDir))
        "d ${cfg.fragmentsDir} 0700 hopnet hopnet - -";
  };
}
