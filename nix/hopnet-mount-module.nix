# hopnet-mount systemd user unit (RFC-018 S8), shared between the
# home-manager and NixOS module flavors so the two cannot drift. The
# flavor only decides the emission syntax (home-manager's capitalized
# unit attrs vs NixOS serviceConfig) and where the package lands
# (home.packages vs environment.systemPackages).
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

  execStart = lib.concatStringsSep " " (
    [ "${cfg.package}/bin/hopnet-mount" "mount" mountArg ]
    ++ lib.optionals (cfg.url != null) [ "--url" cfg.url ]
    ++ cfg.extraArgs
  );

  # fusermount3 must come from the setuid wrapper on NixOS (unprivileged
  # mounting); the store binary is the fallback for non-NixOS hosts.
  unitPath = "/run/wrappers/bin:${pkgs.fuse3}/bin";

  description = "HopNet drive FUSE mount (RFC-018)";
in
{
  options.services.hopnet-mount = {
    enable = lib.mkEnableOption "the HopNet drive FUSE mount daemon";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.hopnet-mount;
      defaultText = lib.literalExpression "hopnet.packages.<system>.hopnet-mount";
      description = "The hopnet-mount package to run.";
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
        the fixed headless default http://127.0.0.1:34632.
      '';
    };

    extraArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Extra arguments appended to `hopnet-mount mount`.";
    };
  };

  config = lib.mkIf cfg.enable (
    if flavor == "hm" then {
      home.packages = [ cfg.package ];
      systemd.user.services.hopnet-mount = {
        Unit.Description = description;
        Service = {
          ExecStart = execStart;
          Restart = "on-failure";
          RestartSec = 5;
          Environment = [ "PATH=${unitPath}" ];
        };
        Install.WantedBy = [ "default.target" ];
      };
    } else {
      environment.systemPackages = [ cfg.package ];
      systemd.user.services.hopnet-mount = {
        inherit description;
        serviceConfig = {
          ExecStart = execStart;
          Restart = "on-failure";
          RestartSec = 5;
          Environment = [ "PATH=${unitPath}" ];
        };
        wantedBy = [ "default.target" ];
      };
    }
  );
}
