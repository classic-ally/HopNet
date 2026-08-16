# hopnet-mount systemd user unit (RFC-018 S8), shared between the
# home-manager and NixOS module flavors so the two cannot drift. The
# flavor only decides the emission syntax (home-manager's capitalized
# unit attrs vs NixOS serviceConfig) and where the package lands
# (home.packages vs environment.systemPackages).
#
# RFC-024 S2: with upgrade.enable (the default) the unit execs through
# a self-owned profile symlink, seeds it newest-wins from the flake
# pin, checks for releases before every start and on a ~6-hourly
# timer, and restarts a 426'd daemon into the flipped profile via
# exit 75.
{ self, flavor }:
{ config, lib, pkgs, ... }:
let
  cfg = config.services.hopnet-mount;

  # %h is expanded by systemd, keeping the unit per-user in both
  # flavors (a NixOS-level unit serves every user of the host).
  mountArg =
    if lib.hasPrefix "/" cfg.mountpoint
    then cfg.mountpoint
    else "%h/${cfg.mountpoint}";

  # The RFC-024 indirection, under the daemon's own data-dir default
  # (~/.local/share/hopnet). %h expands in unit directives ONLY —
  # never inside script files, which is why the seed script below
  # reads the path from its environment instead.
  profile = "%h/.local/share/hopnet/mount-profile";
  stageDir = "%h/.local/share/hopnet/mount-staged";

  # Passthrough needs CAP_SYS_ADMIN; when granted via the wrapper, the
  # unit must exec the wrapped binary, not the store path. The wrapper
  # is a COPY made at activation, so it can never follow a profile
  # flip — hence the upgrade/passthrough mutual exclusion below.
  daemonBin =
    if cfg.upgrade.enable
    then "${profile}/bin/hopnet-mount"
    else if flavor == "nixos" && cfg.allowPassthrough
    then "/run/wrappers/bin/hopnet-mount"
    else "${cfg.package}/bin/hopnet-mount";

  execStart = lib.concatStringsSep " " (
    [ daemonBin "mount" mountArg ]
    ++ lib.optionals (cfg.url != null) [ "--url" cfg.url ]
    ++ cfg.extraArgs
  );

  # fusermount3 must come from the setuid wrapper on NixOS (unprivileged
  # mounting); the store binary is the fallback for non-NixOS hosts.
  # git: nix shells out to it for git+https flake refs (the node's
  # field-proven lesson, nix/hopnet-module.nix). coreutils: `timeout`
  # bounding the pre-start upgrade run.
  unitPath = "/run/wrappers/bin:${lib.makeBinPath [ pkgs.fuse3 pkgs.git pkgs.coreutils ]}";

  # The RFC-024 deployment contract plus TLS trust for the feed/probe,
  # shared by the mount service and the follower timer's oneshot.
  upgradeEnv =
    [
      "HOPNET_MOUNT_UPGRADE_PROFILE=${profile}"
      "HOPNET_MOUNT_UPGRADE_STAGE_DIR=${stageDir}"
      "HOPNET_MOUNT_UPGRADE_NIX_BIN=${cfg.upgrade.nixBin}"
      "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
    ]
    ++ lib.optional (cfg.upgrade.flakeRef != null)
      "HOPNET_MOUNT_UPGRADE_FLAKE_REF=${cfg.upgrade.flakeRef}"
    ++ lib.optional (cfg.upgrade.releaseUrl != null)
      "HOPNET_MOUNT_UPGRADE_RELEASE_URL=${cfg.upgrade.releaseUrl}";

  serviceEnv = [ "PATH=${unitPath}" ] ++ lib.optionals cfg.upgrade.enable upgradeEnv;

  # Newest-wins seeding (nix/hopnet-module.nix transplanted): profile
  # missing → seed; flake strictly newer → deliberate pin bump,
  # re-seed; profile newer or equal → a wrapper upgrade happened, leave
  # it. The profile path comes from the unit environment (%h). The
  # version read extracts the last field because clap prints
  # "hopnet-mount 2026.8.2", via POSIX expansion (awk is not in
  # coreutils). Interpolating ${cfg.package} roots the seed generation
  # in the closure.
  seedScript = pkgs.writeShellScript "hopnet-mount-seed-profile" ''
    set -eu
    export PATH=${lib.makeBinPath [ pkgs.coreutils ]}
    profile="$HOPNET_MOUNT_UPGRADE_PROFILE"
    mkdir -p "$(dirname "$profile")"
    seed() {
      ln -sfn ${cfg.package} "''${profile}.seed"
      mv -T "''${profile}.seed" "$profile"
    }
    current_ver=""
    if [ -x "$profile/bin/hopnet-mount" ]; then
      current_ver=$("$profile/bin/hopnet-mount" --version 2>/dev/null || true)
      current_ver=''${current_ver##* }
    fi
    if [ -z "$current_ver" ]; then
      seed
      exit 0
    fi
    flake_ver="${cfg.package.version}"
    newest=$(printf '%s\n%s\n' "$flake_ver" "$current_ver" | sort -V | tail -n1)
    if [ "$newest" = "$flake_ver" ] && [ "$flake_ver" != "$current_ver" ]; then
      echo "hopnet-mount: flake pin $flake_ver is newer than profile $current_ver — re-seeding"
      seed
    fi
  '';

  upgradeArgs = lib.optionalString (cfg.url != null) " --url ${cfg.url}";
  # `-` prefix: a failed or timed-out check never blocks the start
  # (offline-safe); 120 s bounds a surprise source build at login — the
  # unbounded timer picks long builds up later.
  preUpgrade = "-${lib.getExe' pkgs.coreutils "timeout"} 120 ${profile}/bin/hopnet-mount upgrade${upgradeArgs}";
  timerExec = "${profile}/bin/hopnet-mount upgrade${upgradeArgs}";

  description = "HopNet drive FUSE mount (RFC-018)";
  followerDescription = "HopNet mount release follower (RFC-024)";

  # RestartForceExitStatus: exit 75 is the activation request (the
  # node's RFC-019 S6 convention) — restarted even if the Restart=
  # policy is ever narrowed. RestartSteps/MaxDelay damp the held-426
  # restart loop from 5 s to 10 min exponentially (systemd >= 254).
  upgradeServiceBits = {
    ExecStartPre = [ "${seedScript}" preUpgrade ];
    RestartForceExitStatus = 75;
    RestartSteps = 10;
    RestartMaxDelaySec = 600;
  };
in
{
  options.services.hopnet-mount = {
    enable = lib.mkEnableOption "the HopNet drive FUSE mount daemon";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.hopnet-mount;
      defaultText = lib.literalExpression "hopnet.packages.<system>.hopnet-mount";
      description = "The hopnet-mount package seeding the exec profile.";
    };

    mountpoint = lib.mkOption {
      type = lib.types.str;
      default = "HopDrive";
      description = "Mountpoint; a relative path is under the user's home.";
    };

    url = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = ''
        Node base URL. Left null, the daemon resolves it itself:
        login-stored config, then the node-written endpoint file, then
        the fixed dev-shape default http://127.0.0.1:34632 (only valid
        for a node run with HOPNET_DISABLE_TLS=1 HOPNET_HTTP_PORT=34632;
        RFC-022 nodes otherwise bind a kernel-assigned loopback port).
      '';
    };

    extraArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Extra arguments appended to `hopnet-mount mount`.";
    };

    allowPassthrough = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = ''
        Grant the daemon CAP_SYS_ADMIN via a security.wrappers file
        capability so FUSE passthrough (kernel-direct reads of fully
        cached files, RFC-018 S9) can activate. SECURITY TRADEOFF:
        cap_sys_admin is effectively root for anyone able to execute
        the wrapper — leave this off unless the host's users are
        trusted. NixOS module only; home-manager cannot grant file
        capabilities. Mutually exclusive with upgrade.enable.
      '';
    };

    upgrade = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          RFC-024 auto-upgrade: exec through a self-owned profile
          symlink, seed it newest-wins from the flake pin, check for
          releases before every start and on a ~6-hourly timer, and let
          a 426'd daemon restart itself into the flipped profile
          (exit 75). User units run only during a session unless
          `loginctl enable-linger` is set; the pre-start check covers
          every login regardless. Mutually exclusive with
          allowPassthrough (security.wrappers copies the binary at
          activation, so the wrapped path can never follow a profile
          flip).
        '';
      };

      flakeRef = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = ''
          Base flake ref releases are staged from
          ("?ref=refs/tags/v<version>#hopnet-mount" is appended). Null
          derives the upstream repository from the binary itself; set
          for forks and mirrors.
        '';
      };

      nixBin = lib.mkOption {
        type = lib.types.path;
        default = "${pkgs.nix}/bin/nix";
        defaultText = lib.literalExpression ''"''${pkgs.nix}/bin/nix"'';
        description = "The nix binary the wrapper's staging invokes (tests point it at a stub).";
      };

      releaseUrl = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = ''
          Releases API endpoint for the follower poll. Null derives the
          upstream default (the S3 VM test points it at a fake feed).
        '';
      };
    };
  };

  config = lib.mkIf cfg.enable (
    if flavor == "hm" then
      lib.mkMerge [
        {
          assertions = [
            {
              assertion = !cfg.allowPassthrough;
              message = ''
                services.hopnet-mount.allowPassthrough needs the NixOS
                module (file capabilities come from security.wrappers,
                which home-manager cannot manage).
              '';
            }
          ];
          home.packages = [ cfg.package ];
          systemd.user.services.hopnet-mount = {
            Unit.Description = description;
            Service = {
              ExecStart = execStart;
              Restart = "on-failure";
              RestartSec = 5;
              # Stopgap headroom over the 1024 soft default; the daemon
              # additionally bounds its own cache descriptors (LRU).
              LimitNOFILE = 65536;
              Environment = serviceEnv;
            } // lib.optionalAttrs cfg.upgrade.enable upgradeServiceBits;
            Install.WantedBy = [ "default.target" ];
          };
        }
        (lib.mkIf cfg.upgrade.enable {
          systemd.user.services.hopnet-mount-upgrade = {
            Unit.Description = followerDescription;
            Service = {
              Type = "oneshot";
              ExecStartPre = "${seedScript}";
              ExecStart = timerExec;
              Environment = [ "PATH=${unitPath}" ] ++ upgradeEnv;
            };
          };
          systemd.user.timers.hopnet-mount-upgrade = {
            Unit.Description = "${followerDescription} tick";
            # Phase-offset from the node's auto_stage tick (hours
            # 00/06/12/18 at a random minute): maximal 3 h distance,
            # jitter well inside the offset, catch-up for sessions that
            # slept through a window.
            Timer = {
              OnCalendar = "03,09,15,21:00";
              RandomizedDelaySec = "30min";
              Persistent = true;
            };
            Install.WantedBy = [ "timers.target" ];
          };
        })
      ]
    else
      lib.mkMerge [
        {
          assertions = [
            {
              assertion = !(cfg.upgrade.enable && cfg.allowPassthrough);
              message = ''
                services.hopnet-mount: upgrade.enable and
                allowPassthrough are mutually exclusive —
                security.wrappers copies the binary at activation, so
                the wrapped path can never follow a profile flip. Set
                upgrade.enable = false to keep passthrough.
              '';
            }
          ];
          environment.systemPackages = [ cfg.package ];
          # fusermount3 must be the setuid wrapper for unprivileged
          # mounting; unitPath already fronts /run/wrappers/bin, but the
          # wrappers only exist when the fuse program module is on — NOT
          # a NixOS default. (The HM arm cannot do this: the host may
          # not be NixOS.)
          programs.fuse.enable = true;
          security.wrappers = lib.mkIf cfg.allowPassthrough {
            hopnet-mount = {
              source = "${cfg.package}/bin/hopnet-mount";
              capabilities = "cap_sys_admin+ep";
              owner = "root";
              group = "root";
            };
          };
          systemd.user.services.hopnet-mount = {
            inherit description;
            serviceConfig = {
              ExecStart = execStart;
              Restart = "on-failure";
              RestartSec = 5;
              # Stopgap headroom over the 1024 soft default; the daemon
              # additionally bounds its own cache descriptors (LRU).
              LimitNOFILE = 65536;
              Environment = serviceEnv;
            } // lib.optionalAttrs cfg.upgrade.enable upgradeServiceBits;
            wantedBy = [ "default.target" ];
          };
        }
        (lib.mkIf cfg.upgrade.enable {
          systemd.user.services.hopnet-mount-upgrade = {
            description = followerDescription;
            serviceConfig = {
              Type = "oneshot";
              ExecStartPre = "${seedScript}";
              ExecStart = timerExec;
              Environment = [ "PATH=${unitPath}" ] ++ upgradeEnv;
            };
          };
          systemd.user.timers.hopnet-mount-upgrade = {
            description = "${followerDescription} tick";
            # Phase-offset from the node's auto_stage tick (hours
            # 00/06/12/18 at a random minute): maximal 3 h distance,
            # jitter well inside the offset, catch-up for sessions that
            # slept through a window.
            timerConfig = {
              OnCalendar = "03,09,15,21:00";
              RandomizedDelaySec = "30min";
              Persistent = true;
            };
            wantedBy = [ "timers.target" ];
          };
        })
      ]
  );
}
