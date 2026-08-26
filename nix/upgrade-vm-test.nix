# RFC-021 end-to-end, declaratively: a relay + 3-node NixOS mesh crosses
# a REAL upgrade boundary through the module's profile flip — the systemd
# restart into a genuinely different binary that neither unit tests nor
# orchestrator containers can exercise. The only stub is `nix build`
# itself (VM tests have no network): staging symlinks a pre-built
# hopnet-next from the closure, and everything downstream — availability
# poll, auto-stage, provenance, verified attestation, the quorum-decided
# start, seal, flip, exit 75, restart, Gate 1, epoch 2 — is shipping code.
{ self }:
{ pkgs, lib, ... }:
let
  system = pkgs.stdenv.hostPlatform.system;
  hopnet = self.packages.${system}.hopnet;

  # The "next release": the same tree with Cargo.toml's version patched,
  # so the compile-time version string, --version output, and Gate 1 all
  # see genuinely different bytes. A real second build, nix-cached.
  nextVersion = "2026.12.99";
  hopnet-next = hopnet.overrideAttrs (old: {
    version = nextVersion;
    postPatch = (old.postPatch or "") + ''
      sed -i 's/^version = "${old.version}"$/version = "${nextVersion}"/' Cargo.toml
      grep -q '^version = "${nextVersion}"$' Cargo.toml
    '';
  });

  # Hermetic stage(): "nix build <ref> --out-link <link> --print-out-paths"
  # becomes a symlink to the pre-built next generation. Interpolating
  # hopnet-next also roots it in the VM's closure.
  fakeNix = pkgs.writeShellScriptBin "nix" ''
    set -eu
    ln -sfn ${hopnet-next} "$4"
    echo ${hopnet-next}
  '';

  releasesJson = ''[{"tag_name":"v${nextVersion}"}]'';

  nodeConfig = { ... }: {
    imports = [ self.nixosModules.hopnet ];
    services.hopnet = {
      enable = true;
      relayUrl = "http://relay:3340";
      upgrade = {
        releaseUrl = "http://relay/releases";
        nixBin = "${fakeNix}/bin/nix";
      };
    };
    # 3-seat formation inside the test window (same policy the
    # orchestrator scenarios seed).
    systemd.services.hopnet.environment.HOPNET_GENESIS_CONSENSUS_POLICY =
      "probe_base=2;grace=1;s_full=6;p_prove=6";
    environment.systemPackages = [ pkgs.curl pkgs.jq ];
    networking.firewall.enable = false;
    virtualisation.memorySize = 1536;
  };
in
{
  name = "hopnet-upgrade";

  nodes = {
    relay = { ... }: {
      # The mesh's iroh relay (--dev: plain HTTP) and the static releases
      # feed the availability poll reads.
      systemd.services.iroh-relay = {
        wantedBy = [ "multi-user.target" ];
        after = [ "network.target" ];
        serviceConfig = {
          ExecStart = "${self.packages.${system}.iroh-relay}/bin/iroh-relay --dev";
          Restart = "on-failure";
        };
      };
      services.nginx = {
        enable = true;
        virtualHosts."relay" = {
          locations."/releases".extraConfig = ''
            default_type application/json;
            return 200 '${releasesJson}';
          '';
        };
      };
      networking.firewall.enable = false;
    };

    node0 = nodeConfig;
    node1 = nodeConfig;
    node2 = nodeConfig;
  };

  testScript = ''
    import json

    API = "https://localhost:34632/api"  # pinned-HTTPS network surface; -k because the cert is per-node self-signed
    nodes = [node0, node1, node2]

    start_all()
    relay.wait_for_unit("iroh-relay.service")
    relay.wait_for_unit("nginx.service")
    for n in nodes:
        n.wait_for_unit("hopnet.service")
        # Pre-setup GET /setup answers 404 with the node pubkey — HTTP
        # responding at all is readiness.
        n.wait_until_succeeds(f"curl -ks {API}/setup | grep -qE '.'", timeout=120)

    # --- Mesh formation over the HTTP API (the orchestrator's flow) ---
    setup = json.loads(
        node0.succeed(
            f"curl -ksf -X POST {API}/setup "
            "-H 'Content-Type: application/json' "
            "-d '{\"username\": \"allison\", \"node_name\": \"node0\"}'"
        )
    )
    passphrase = setup["passphrase"]
    login_body = json.dumps({"username": "allison", "passphrase": passphrase})

    def jwt_for(n):
        # JWTs are PER NODE (each validates with its own secret); the user
        # row is consensus-replicated, so the same passphrase logs in
        # everywhere once replication catches up.
        return json.loads(
            n.succeed(
                f"curl -ksf -X POST {API}/login "
                "-H 'Content-Type: application/json' "
                f"-d '{login_body}'"
            )
        )["token"]

    auth = f"-H 'Authorization: Bearer {jwt_for(node0)}'"

    # The join ceremony (RFC-025 S5): a fresh node binds TLS-dead until
    # the mesh code is adopted, so the code must land on each joiner
    # BEFORE the coordinator's registration probe can complete.
    mesh_code = json.loads(
        node0.succeed(f"curl -ksf {API}/views/regenesis-status {auth}")
    )["mesh_code"]
    code_body = json.dumps({"code": mesh_code})

    for i, n in enumerate(nodes[1:], start=1):
        pubkey = n.succeed(f"curl -ks {API}/setup").strip().strip('"')
        assert len(pubkey) == 64, f"node{i} pubkey: {pubkey!r}"
        n.succeed(
            f"curl -ksf -X POST {API}/setup/join-code "
            f"-H 'Content-Type: application/json' -d '{code_body}'"
        )
        body = json.dumps({"name": f"node{i}", "owner": 0, "pubkey": pubkey})
        # 504 = iroh discovery still warming; retry through it.
        node0.wait_until_succeeds(
            f"curl -ksf -X POST {API}/nodes {auth} "
            f"-H 'Content-Type: application/json' -d '{body}'",
            timeout=120,
        )

    # Heights decide and all three seats fill.
    node0.wait_until_succeeds(
        f"curl -ksf {API}/consensus {auth} | jq -e '.last_decided_height > 0'",
        timeout=180,
    )
    for n in nodes[1:]:
        n.wait_until_succeeds(
            f"curl -ksf -X POST {API}/login "
            "-H 'Content-Type: application/json' "
            f"-d '{login_body}'",
            timeout=120,
        )
    auths = [f"-H 'Authorization: Bearer {jwt_for(n)}'" for n in nodes]

    # --- RFC-021: poll → auto-stage (stub build) → verified attestation ---
    # Retried per node: a tick can transiently skip its poll (pool
    # checkout under formation load) — the retry converges on this
    # node's own staged attestation appearing in committed state.
    for i, n in enumerate(nodes):
        n.wait_until_succeeds(
            f"curl -ksf -X POST {API}/maintenance/upgrade-tick {auths[i]} "
            f"&& curl -ksf {API}/views/upgrade-readiness {auths[i]} "
            f"| jq -e '.mesh[] | select(.node_id == {i}) "
            "| .staged == \"${nextVersion}\"'",
            timeout=180,
        )
    node0.wait_until_succeeds(
        f"curl -ksf {API}/views/upgrade-readiness {auth} "
        "| jq -e '(.mesh | length) == 3 and ([.mesh[].staged] | all(. == \"${nextVersion}\"))'",
        timeout=120,
    )
    # The deployment advertises its capabilities.
    node0.succeed(
        f"curl -ksf {API}/views/upgrade-readiness {auth} "
        "| jq -e '.activation.provider == \"nix\" and .activation.auto_activate'"
    )

    # --- The upgrade boundary: decide, seal, flip, exit 75, restart ---
    h_before = json.loads(
        node0.succeed(f"curl -ksf {API}/consensus {auth}")
    )["last_decided_height"]
    node0.succeed(
        f"curl -ksf -X POST {API}/consensus/regenesis/start {auth} "
        "-H 'Content-Type: application/json' "
        "-d '{\"target_version\": \"${nextVersion}\"}'"
    )

    # Every node crosses into epoch 2 running the NEXT binary, through
    # the profile — no operator action from here on. Tokens do not
    # survive the restart; log in again once each node answers (the user
    # row is committed state and crossed the boundary with everything
    # else).
    for i, n in enumerate(nodes):
        n.wait_until_succeeds(
            f"curl -ksf -X POST {API}/login "
            "-H 'Content-Type: application/json' "
            f"-d '{login_body}'",
            timeout=300,
        )
    auths = [f"-H 'Authorization: Bearer {jwt_for(n)}'" for n in nodes]
    auth = auths[0]
    for i, n in enumerate(nodes):
        n.wait_until_succeeds(
            f"curl -ksf {API}/views/regenesis-status {auths[i]} "
            "| jq -e '.epoch == \"2\" and .phase == \"normal\"'",
            timeout=300,
        )
        version = n.succeed("/var/lib/hopnet/profile/bin/hopnet --version").strip()
        assert version == "${nextVersion}", f"profile binary is {version!r}"
        target = n.succeed("readlink /var/lib/hopnet/profile").strip()
        assert target == "${hopnet-next}", f"profile points at {target!r}"
        # The daemon's data dir is XDG_DATA_HOME/hopnet — the markers
        # live one level DOWN from dataDir (a prior assertion checked
        # the parent, vacuously true forever).
        n.succeed("test ! -e /var/lib/hopnet/hopnet/awaiting-upgrade")
        # RFC-025: the crossing stamped the agreed version, exact bytes.
        agreed = n.succeed("cat /var/lib/hopnet/hopnet/agreed-version").strip()
        assert agreed == "${nextVersion}", f"agreed-version is {agreed!r}"
        # The seed-guard wiring is live: the module's advance arm logs
        # its decision through the guard on every start that considers
        # a newer pin (the crossing's restarts exercised the script).
        restarts = int(
            n.succeed("systemctl show hopnet -p NRestarts --value").strip()
        )
        assert restarts >= 1, "the crossing must have gone through a restart"
        # A held-flake-bump leg would need a THIRD built generation
        # (flake pin > agreed while the mesh stays put) — deferred; the
        # orchestrator's in-container seed-guard legs cover the
        # decision itself.

    # The upgraded epoch decides new heights (an attestation re-converging
    # on the new running version is itself traffic).
    node0.succeed(f"curl -ksf -X POST {API}/maintenance/upgrade-tick {auth}")
    node0.wait_until_succeeds(
        f"curl -ksf {API}/consensus {auth} "
        f"| jq -e '.last_decided_height > {h_before}'",
        timeout=120,
    )
  '';
}
