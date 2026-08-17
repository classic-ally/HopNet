# HopNet.app as a supervised node (RFC-026 S2): a launchd user agent execs
# the app through a user-owned profile symlink, mirroring the NixOS node
# module (nix/hopnet-module.nix). The flake-pinned .app only SEEDS the
# profile, newest-wins — a deliberate pin bump moves it forward, a rebuild
# never regresses a self-upgrade (the S3 darwin provider flips the same
# symlink). Supervision alone is a day-one win: exit 75 (same-version
# regenesis restarts) relaunches through `SuccessfulExit = false`, which no
# macOS node survives today.
#
# A USER agent as the login user, deliberately: TCC prompts, SMAppService
# agent registration, and the tray GUI all require the user session. The
# agent sets HOPNET_AUTOSTART so the app starts tray-only at login, and the
# app's single-instance flock makes a Finder-launched second copy exit
# quietly instead of sharing the WAL database.
{ self }:
{ config, lib, pkgs, ... }:
let
  cfg = config.services.hopnet-desktop;

  homeDir = "/Users/${cfg.user}";
  profile = "${cfg.dataDir}/profile";
  appBundle = "${profile}";
  appBinary = "${appBundle}/Contents/MacOS/HopNet";

  # Newest-wins seeding + exec, folded into one wrapper: launchd has no
  # ExecStartPre, and the wrapper's stable store path also keeps the agent's
  # program path valid while the profile moves. `sort -V` orders CalVer;
  # coreutils supplies `mv -T` (BSD mv cannot atomically replace a symlink).
  # Interpolating ${cfg.package} roots the seed generation in the system
  # closure, so the profile target is never garbage-collected.
  agentWrapper = pkgs.writeShellScript "hopnet-desktop-agent" ''
    set -eu
    export PATH=${lib.makeBinPath [ pkgs.coreutils ]}:/usr/bin:/bin
    mkdir -p "${cfg.dataDir}"
    seed() {
      ln -sfn ${cfg.package}/Applications/HopNet.app "${profile}.seed"
      mv -T "${profile}.seed" "${profile}"
    }
    current_ver=""
    if [ -x "${appBinary}" ]; then
      current_ver=$("${appBinary}" --version 2>/dev/null || true)
    fi
    if [ -z "$current_ver" ]; then
      seed
    else
      flake_ver="${cfg.package.version}"
      newest=$(printf '%s\n%s\n' "$flake_ver" "$current_ver" | sort -V | tail -n1)
      if [ "$newest" = "$flake_ver" ] && [ "$flake_ver" != "$current_ver" ]; then
        echo "hopnet-desktop: flake pin $flake_ver is newer than profile $current_ver — re-seeding"
        seed
      fi
    fi
    exec "${appBinary}"
  '';
in
{
  options.services.hopnet-desktop = {
    enable = lib.mkEnableOption "the HopNet desktop app as a supervised launchd node";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.hopnet-desktop;
      defaultText = lib.literalExpression "hopnet.packages.<system>.hopnet-desktop";
      description = ''
        The signed .app package seeding the agent profile. Also installed
        into systemPackages so mac-app-util keeps Finder/Launch Services
        presence.
      '';
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = config.system.primaryUser;
      defaultText = lib.literalExpression "config.system.primaryUser";
      description = ''
        The login user whose session runs the agent. TCC grants, the
        keychain, and SMAppService registrations all belong to this user.
      '';
    };

    dataDir = lib.mkOption {
      type = lib.types.path;
      default = "/Users/${cfg.user}/.local/share/hopnet";
      defaultText = lib.literalExpression ''"/Users/''${cfg.user}/.local/share/hopnet"'';
      description = ''
        Node state: SQLite database, fragment storage, the exec profile.
        The default matches the app's own unconfigured fallback
        (src/paths.rs), so adopting the module on a machine that already
        ran HopNet.app supervises the existing state in place.
      '';
    };

    fragmentsDir = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      description = ''
        Bulk fragment storage outside dataDir (HOPNET_FRAGMENTS_DIR).
        Null keeps fragments under dataDir, exactly where an unconfigured
        app puts them.
      '';
    };

    relayUrl = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Iroh relay URL (HOPNET_RELAY_URL).";
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
      description = "RUST_LOG for the node process.";
    };
  };

  config = lib.mkIf cfg.enable {
    # Finder/Launch Services presence rides the normal package install —
    # mac-app-util trampolines $out/Applications/HopNet.app.
    environment.systemPackages = [ cfg.package ];

    system.activationScripts.hopnetDesktopState.text = ''
      mkdir -p '${cfg.dataDir}'
      chown '${cfg.user}':staff '${cfg.dataDir}'
      chmod 700 '${cfg.dataDir}'
    '';

    launchd.user.agents.hopnet-desktop = {
      serviceConfig = {
        Label = "com.hopnet.desktop.node";
        ProgramArguments = [ "${agentWrapper}" ];
        RunAtLoad = true;
        # `SuccessfulExit = false` is systemd's Restart=on-failure: exit 75
        # (the activation/regenesis restart request) relaunches through the
        # profile; a clean quit (tray Quit, or losing the single-instance
        # race) exits 0 and stays down. ThrottleInterval is the only
        # damping — the crash-loop guard proper lives in the provider.
        KeepAlive = {
          SuccessfulExit = false;
          Crashed = true;
        };
        ThrottleInterval = 10;
        ProcessType = "Interactive";
        StandardOutPath = "${cfg.dataDir}/agent.out.log";
        StandardErrorPath = "${cfg.dataDir}/agent.err.log";
        EnvironmentVariables = {
          HOME = homeDir;
          PATH = lib.concatStringsSep ":" [
            "/usr/bin"
            "/bin"
            "/usr/sbin"
            "/sbin"
            "/run/current-system/sw/bin"
          ];
          HOPNET_DATA_DIR = cfg.dataDir;
          # Tray-only start: the agent runs at every login and a window
          # there would greet the user on each boot.
          HOPNET_AUTOSTART = "1";
          RUST_LOG = cfg.logLevel;
        }
        // lib.optionalAttrs (cfg.relayUrl != null) {
          HOPNET_RELAY_URL = cfg.relayUrl;
        }
        // lib.optionalAttrs (cfg.fragmentsDir != null) {
          HOPNET_FRAGMENTS_DIR = cfg.fragmentsDir;
        };
      };
    };
  };
}
