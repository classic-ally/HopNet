# RFC-023 end-to-end: one NixOS machine runs a single-seat hopnet node
# (quorum(1), local iroh relay), a user-session hopnet-mount under
# alice, and a file-served release feed the test mutates between
# phases. Proves stage → flip → exit 75 → the daemon serves from the
# new binary, and the hold path (nothing compatible → loud held state,
# no restart loop). The only stubs: `nix build` (symlinks pre-built
# generations from the closure) and the node's minimum (raised mid-run
# via HOPNET_MIN_CLIENT_OVERRIDE, the test-mode seam in
# src/client_compat.rs) — everything downstream is shipping code.
{ self }:
{ pkgs, lib, ... }:
let
  system = pkgs.stdenv.hostPlatform.system;
  hopnet-mount = self.packages.${system}.hopnet-mount;

  # The "next release": the workspace Cargo.toml version patched (the
  # single authority — every crate inherits), so --version, the client
  # header, and the exit-75 gate all see genuinely different bytes.
  # MIN_NODE is untouched, so next flips forward cleanly. A real second
  # build of the workspace crates; the dep artifacts stay cached.
  nextVersion = "2026.12.99";
  hopnet-mount-next = hopnet-mount.overrideAttrs (old: {
    version = nextVersion;
    postPatch = (old.postPatch or "") + ''
      sed -i 's/^version = "${old.version}"$/version = "${nextVersion}"/' Cargo.toml
      grep -q '^version = "${nextVersion}"$' Cargo.toml
    '';
  });

  # The hold-phase generation: a stub, not a third compile. stage()
  # only ever interrogates --version (last token) and --min-node — both
  # answered; its min_node can never be satisfied by the node's real
  # version, so the walk stages it, reads the refusal, and holds.
  stubVersion = "2099.2.0";
  stubMinNode = "2099.1.1";
  hopnet-mount-stub = pkgs.writeShellScriptBin "hopnet-mount" ''
    case "''${1:-}" in
      --version|-V) echo "hopnet-mount ${stubVersion}" ;;
      --min-node)   echo "${stubMinNode}" ;;
      *) echo "hold-phase stub: not runnable" >&2; exit 1 ;;
    esac
  '';

  # Hermetic stage(): argv is `build <ref?ref=refs/tags/vX#hopnet-mount>
  # --out-link <link> --print-out-paths`. Version extracted with shell
  # parameter expansion (the mount units' PATH has no sed), mapped to a
  # pre-built generation; the interpolations root both in the closure.
  fakeNix = pkgs.writeShellScriptBin "nix" ''
    set -eu
    ref="$2"
    ver="''${ref##*refs/tags/v}"
    ver="''${ver%%#*}"
    case "$ver" in
      ${nextVersion}) store=${hopnet-mount-next} ;;
      ${stubVersion}) store=${hopnet-mount-stub} ;;
      *) echo "fake nix: no pre-built generation for $ver" >&2; exit 1 ;;
    esac
    ${pkgs.coreutils}/bin/ln -sfn "$store" "$4"
    echo "$store"
  '';
in
{
  name = "hopnet-mount-upgrade";

  nodes.machine = { ... }: {
    imports = [ self.nixosModules.hopnet self.nixosModules.hopnet-mount ];

    # Single-seat mesh: zero genesis config (QuorumProfile::Auto →
    # quorum(1) = 1; genesis seats node 0). The local relay keeps iroh
    # off public infrastructure in the network-less VM.
    services.hopnet = {
      enable = true;
      relayUrl = "http://localhost:3340";
      # The node's OWN RFC-021 follower stays hermetic and idle: an
      # empty local feed instead of a DNS failure against upstream.
      upgrade.releaseUrl = "http://localhost/node-releases.json";
    };
    # Release build: the min-client seam (and every other test seam)
    # is gated on HOPNET_TEST_MODE.
    systemd.services.hopnet.environment.HOPNET_TEST_MODE = "1";

    systemd.services.iroh-relay = {
      wantedBy = [ "multi-user.target" ];
      after = [ "network.target" ];
      serviceConfig = {
        ExecStart = "${self.packages.${system}.iroh-relay}/bin/iroh-relay --dev";
        Restart = "on-failure";
      };
    };

    # The mutable release feed: files under /var/lib/feed, read per
    # request — the testScript rewrites releases.json between phases.
    services.nginx = {
      enable = true;
      virtualHosts."localhost".root = "/var/lib/feed";
    };
    systemd.tmpfiles.rules = [ "d /var/lib/feed 0755 root root -" ];

    users.users.alice = {
      isNormalUser = true;
      uid = 1000;
    };
    # Deliberately NOT lingering declaratively: linger is enabled
    # imperatively AFTER node setup + token provisioning, so the unit's
    # first start already has everything (no preflight crash-loop into
    # the RestartSteps damping).

    services.hopnet-mount = {
      enable = true;
      url = "http://localhost:34632";
      extraArgs = [ "--token-file" "/home/alice/device-token" ];
      upgrade = {
        nixBin = "${fakeNix}/bin/nix";
        releaseUrl = "http://localhost/releases.json";
      };
    };

    environment.systemPackages = [ pkgs.curl pkgs.jq ];
    networking.firewall.enable = false;
    virtualisation.memorySize = 2048;
    virtualisation.cores = 2;
  };

  testScript = ''
    import json
    import time

    API = "http://localhost:34632/api"
    HEALTH = f"{API}/integrations/mount/health"
    PROFILE = "/home/alice/.local/share/hopnet/mount-profile"
    STAGED = "/home/alice/.local/share/hopnet/mount-staged"
    SUDO = "XDG_RUNTIME_DIR=/run/user/1000 sudo --preserve-env=XDG_RUNTIME_DIR -u alice"

    base_version = "${hopnet-mount.version}"
    next_version = "${nextVersion}"

    def code(v):
        y, m, c = v.split(".")
        return int(y) * 10000 + int(m) * 100 + int(c)

    base_code = code(base_version)
    next_code = code(next_version)
    stub_min_code = code("${stubMinNode}")

    def alice(cmd):
        return machine.succeed(f"{SUDO} {cmd}")

    def alice_wait(cmd, timeout=120):
        machine.wait_until_succeeds(f"{SUDO} {cmd}", timeout=timeout)

    # User-unit journals, read by root: the explicit field match —
    # `--user-unit` as root silently adds _UID=0 and finds nothing.
    # Only systemctl --user needs the alice + XDG_RUNTIME_DIR dance.
    MOUNT_J = "journalctl _SYSTEMD_USER_UNIT=hopnet-mount.service --no-pager -o cat"
    FOLLOWER_J = "journalctl _SYSTEMD_USER_UNIT=hopnet-mount-upgrade.service --no-pager -o cat"

    def mount_journal():
        return machine.succeed(MOUNT_J)

    def follower_journal():
        return machine.succeed(FOLLOWER_J)

    def jcount(needle):
        return mount_journal().count(needle)

    def nrestarts():
        return int(alice("systemctl --user show hopnet-mount -p NRestarts --value").strip())

    def main_pid():
        return alice("systemctl --user show hopnet-mount -p MainPID --value").strip()

    def running_exe():
        return machine.succeed(f"readlink /proc/{main_pid()}/exe").strip()

    def set_min_client(token):
        machine.succeed(
            "mkdir -p /run/systemd/system/hopnet.service.d && "
            f"printf '[Service]\\nEnvironment=HOPNET_MIN_CLIENT_OVERRIDE={token}\\n' "
            "> /run/systemd/system/hopnet.service.d/override.conf && "
            "systemctl daemon-reload && systemctl restart hopnet"
        )

    def write_feed(tags):
        body = json.dumps([{"tag_name": f"v{t}"} for t in tags])
        machine.succeed(
            f"echo '{body}' > /var/lib/feed/releases.json && "
            "chmod 644 /var/lib/feed/releases.json"
        )

    # ---- Phase 0: baseline ------------------------------------------
    machine.start()
    machine.wait_for_unit("iroh-relay.service")
    machine.wait_for_unit("nginx.service")
    machine.wait_for_unit("hopnet.service")
    machine.succeed(
        "echo '[]' > /var/lib/feed/node-releases.json && "
        "chmod 644 /var/lib/feed/node-releases.json"
    )
    write_feed([])
    machine.wait_until_succeeds(f"curl -s {API}/setup | grep -qE '.'", timeout=120)

    setup = json.loads(machine.succeed(
        f"curl -sf -X POST {API}/setup -H 'Content-Type: application/json' "
        "-d '{\"username\": \"allison\", \"node_name\": \"vm\"}'"
    ))
    login_body = json.dumps({"username": "allison", "passphrase": setup["passphrase"]})
    jwt = json.loads(machine.succeed(
        f"curl -sf -X POST {API}/login -H 'Content-Type: application/json' "
        f"-d '{login_body}'"
    ))["token"]

    reg = json.loads(machine.succeed(
        f"curl -sf -X POST {API}/devices/register -H 'Authorization: Bearer {jwt}' "
        "-H 'Content-Type: application/json' -d '{\"device_name\": \"vm-mount\"}'"
    ))
    token = reg["api_key"]  # the full {device_id}.{secret} token string
    machine.succeed(
        f"echo -n '{token}' > /home/alice/device-token && "
        "chown alice /home/alice/device-token && chmod 600 /home/alice/device-token"
    )
    # The token row is consensus-submitted; wait until it authenticates.
    machine.wait_until_succeeds(
        f"curl -sf -H 'Authorization: Bearer {token}' "
        f"-H 'x-hopnet-client-version: {base_code}' "
        f"{API}/integrations/mount/statfs",
        timeout=120,
    )

    # Everything in place — NOW start alice's session. The first
    # hopnet-mount start seeds the profile, runs the pre-start upgrade
    # (feed [], node compatible → `current:`), preflights 200, mounts.
    machine.succeed("loginctl enable-linger alice")
    machine.wait_for_unit("user@1000.service")
    alice_wait("systemctl --user is-active hopnet-mount.service", timeout=180)
    machine.wait_until_succeeds(
        "grep -q 'hopnet /home/alice/HopDrive fuse' /proc/mounts", timeout=180
    )
    # FUSE serves (as alice — FUSE denies other users by default).
    alice_wait("ls /home/alice/HopDrive", timeout=60)
    alice("sh -c 'echo upgrade-survivor > /home/alice/HopDrive/vm-test.txt'")
    alice_wait("grep -q upgrade-survivor /home/alice/HopDrive/vm-test.txt", timeout=120)

    v = machine.succeed(f"{PROFILE}/bin/hopnet-mount --version").strip()
    assert v == f"hopnet-mount {base_version}", f"baseline profile: {v!r}"
    j = mount_journal()
    assert f"current: {base_version}" in j or "offline:" in j, j[-2000:]
    pid0 = main_pid()
    n0 = nrestarts()
    exe0 = running_exe()
    assert exe0.startswith("${hopnet-mount}"), f"baseline exe: {exe0!r}"

    # ---- Phase 1: release appears; wrapper flips; daemon stays lazy --
    write_feed([next_version])
    alice("systemctl --user start hopnet-mount-upgrade.service")
    follower = follower_journal()
    assert (
        f"upgraded: {base_version} -> {next_version}" in follower
        or f"current: {next_version}" in follower  # the persistent timer beat us to it
    ), follower[-2000:]
    target = machine.succeed(f"readlink {PROFILE}").strip()
    assert target == "${hopnet-mount-next}", f"profile: {target!r}"
    prov = json.loads(machine.succeed(f"cat {STAGED}/v{next_version}.json"))
    assert prov["version"] == next_version and prov["min_node"] == base_code, prov
    # Laziness is the feature: the healthy daemon was never touched.
    assert main_pid() == pid0 and nrestarts() == n0
    assert running_exe() == exe0

    # ---- Phase 2: minimum raised → 426 → exit 75 → next binary ------
    set_min_client(next_version)
    # The seam works end-to-end: the header-less 426 body advertises
    # the raised minimum (and the node's unchanged version).
    machine.wait_until_succeeds(
        f"curl -s {HEALTH} "
        f"| jq -e '.min_client == {next_code} and .node_version == {base_code}'",
        timeout=120,
    )
    # Watcher reconnects (1s→30s backoff while the node was down),
    # 426s, holds loudly, spawns one upgrade run (already flipped →
    # `current:`), the gate sees profile ≠ running, clean unmount,
    # exit 75, restart into the profile.
    machine.wait_until_succeeds(
        f"{MOUNT_J} | grep -q 'restarting into the flipped profile'",
        timeout=300,
    )
    alice_wait("systemctl --user is-active hopnet-mount.service", timeout=180)
    machine.wait_until_succeeds(
        "grep -q 'hopnet /home/alice/HopDrive fuse' /proc/mounts", timeout=180
    )
    j = mount_journal()
    assert "hopnet-mount must be upgraded; holding until it is" in j
    assert "profile flipped while upgrade-required; requesting restart" in j
    assert "upgrade run finished" in j
    assert nrestarts() >= n0 + 1, "the crossing must have gone through a restart"
    exe2 = running_exe()
    assert exe2.startswith("${hopnet-mount-next}"), f"phase-2 exe: {exe2!r}"
    # The next binary serves, and pre-upgrade data survived the swap.
    alice_wait("grep -q upgrade-survivor /home/alice/HopDrive/vm-test.txt", timeout=180)
    n2 = nrestarts()
    pid2 = main_pid()
    restarting_lines2 = jcount("restarting into the flipped profile")
    holds2 = jcount("holding until it is")

    # ---- Phase 3: nothing compatible → loud hold, NO restart loop ---
    write_feed([next_version, "${stubVersion}"])
    set_min_client("${stubMinNode}")
    # A NEW entry into the held state.
    machine.wait_until_succeeds(
        f"test \"$({MOUNT_J} | grep -c 'holding until it is')\" -gt {holds2}",
        timeout=300,
    )
    # Its spawned wrapper run walks forward, stages the stub, reads its
    # min_node, and holds — stdout inherited into the daemon's journal.
    held_line = (
        f"held at {next_version}: node {base_version} does not satisfy "
        "min_node ${stubMinNode} of release ${stubVersion}"
    )
    machine.wait_until_succeeds(
        f"{MOUNT_J} | grep -qF '{held_line}'",
        timeout=180,
    )
    # The explicit follower run says the same thing (both triggers).
    alice("systemctl --user start hopnet-mount-upgrade.service")
    assert held_line in follower_journal()
    # Provenance for the stub landed: staged, interrogated, refused.
    prov = json.loads(machine.succeed(f"cat {STAGED}/v${stubVersion}.json"))
    assert prov["min_node"] == stub_min_code, prov

    # No restart loop: across a 90 s window (≥3 still_held re-probes at
    # the 30 s hold cadence) the unit stays active on the same PID —
    # the gate never fires because profile == running binary.
    for _ in range(9):
        time.sleep(10)
        alice("systemctl --user is-active hopnet-mount.service")
        assert main_pid() == pid2, "held daemon must not restart"
    assert nrestarts() == n2
    assert jcount("restarting into the flipped profile") == restarting_lines2
    # Held and dark, but not dead: a 426'd mount cannot revalidate
    # expired listings (reads answer EIO by design) — the assertion is
    # that it stays MOUNTED with a live daemon, holding for a release.
    machine.succeed("grep -q 'hopnet /home/alice/HopDrive fuse' /proc/mounts")
  '';
}
