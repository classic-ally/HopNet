#!/usr/bin/env bash
# End-to-end verification of the Android HopDrive build (RFC-023 leg
# included): builds the node, boots a current-version node (A) and a
# min-raised node (B, HOPNET_MIN_CLIENT_OVERRIDE), provisions device
# tokens on both, boots a headless emulator, and runs the full Gradle
# suite — assembleDebug, JVM unit tests, and connectedDebugAndroidTest
# (LiveNodeTest + PairingStoreTest against A, UpgradeRequiredTest against
# both).
#
# Run from `nix develop .#android` (provides ANDROID_HOME, JDK 17, cargo,
# jq, and the HOPNET_AAPT2 override). The first Gradle invocation needs
# network access for Maven (google() + mavenCentral()).
#
# The min-raise seam is HOPNET_MIN_CLIENT_OVERRIDE (test-mode only,
# raise-only; src/client_compat.rs). Provisioning node B itself claims a
# future version code in the identity header — identity is self-declared
# by design, so the harness may say what the gate wants to hear.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
APP_DIR="$PROJECT_ROOT/android/HopDrive"
SCRATCH="$(mktemp -d -t hopdrive-e2e-XXXXXX)"

MIN_CLIENT_RAISE="2099.1.0"
HARNESS_CLAIM_B="20990100" # >= the raise, so provisioning B passes its gate
AVD_NAME="hopdrive-e2e"
EMULATOR_BOOT_TIMEOUT=180

NODE_A_PID=""
NODE_B_PID=""
EMULATOR_STARTED=""

declare -A PHASE_RESULT
PHASES=()

phase_result() {
    PHASES+=("$1")
    PHASE_RESULT["$1"]="$2"
}

teardown() {
    local code=$?
    echo "--- teardown"
    [[ -n "$NODE_A_PID" ]] && kill "$NODE_A_PID" 2>/dev/null || true
    [[ -n "$NODE_B_PID" ]] && kill "$NODE_B_PID" 2>/dev/null || true
    if [[ -n "$EMULATOR_STARTED" ]]; then
        local serial
        serial="$(adb devices | awk '/^emulator-/{print $1; exit}')"
        [[ -n "$serial" ]] && adb -s "$serial" emu kill 2>/dev/null || true
    fi
    rm -rf "$SCRATCH"
    echo
    echo "=== summary"
    local failed=0
    for phase in "${PHASES[@]}"; do
        printf '  %-28s %s\n' "$phase" "${PHASE_RESULT[$phase]}"
        [[ "${PHASE_RESULT[$phase]}" == FAIL ]] && failed=1
    done
    if [[ $failed -eq 1 || $code -ne 0 ]]; then
        echo "RESULT: FAIL (node logs kept? no — rerun with bash -x for detail)"
        exit 1
    fi
    echo "RESULT: PASS"
}
trap teardown EXIT

# --- preflight ---------------------------------------------------------

echo "--- preflight"
: "${ANDROID_HOME:?ANDROID_HOME unset — run from: nix develop .#android}"
command -v cargo >/dev/null || { echo "cargo missing"; exit 1; }
command -v jq >/dev/null || { echo "jq missing"; exit 1; }
command -v curl >/dev/null || { echo "curl missing"; exit 1; }
if [[ ! -e /dev/kvm ]]; then
    echo "WARNING: /dev/kvm absent — emulator falls back to software rendering (slow)"
fi

# Workspace CalVer -> numeric code (same encoding as common/src/version.rs).
WORKSPACE_VERSION="$(awk '/^\[workspace.package\]/{s=1;next} /^\[/{s=0} s && /^version *=/{gsub(/.*= *"|".*/,""); print; exit}' "$PROJECT_ROOT/Cargo.toml")"
CLIENT_CODE="$(echo "$WORKSPACE_VERSION" | awk -F. '{printf "%d", $1*10000 + $2*100 + $3}')"
echo "workspace version: $WORKSPACE_VERSION (code $CLIENT_CODE)"

find_free_port() {
    local port
    for port in $(shuf -i 20000-60000 -n 50); do
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            echo "$port"
            return 0
        fi
        exec 3>&- || true
    done
    echo "no free port found" >&2
    return 1
}

# --- build the node ----------------------------------------------------

echo "--- cargo build --bin hopnet"
(cd "$PROJECT_ROOT" && cargo build --bin hopnet)
HOPNET_BIN="$PROJECT_ROOT/target/debug/hopnet"
phase_result "cargo-build" PASS

# --- boot nodes --------------------------------------------------------

# boot_node <label> <http_port> <https_port> [extra VAR=VALUE...]
boot_node() {
    local label="$1" http_port="$2" https_port="$3"
    shift 3
    env "$@" \
        HOPNET_EPHEMERAL_DB=1 \
        HOPNET_TEST_MODE=1 \
        HOPNET_HTTP_PORT="$http_port" \
        HOPNET_HTTPS_PORT="$https_port" \
        "$HOPNET_BIN" >"$SCRATCH/node-$label.log" 2>&1 &
    local pid=$!
    local url="http://127.0.0.1:$http_port/api/integrations/mount/health"
    for _ in $(seq 1 75); do
        # Any HTTP response (even 426) proves the listener is up.
        if curl -s -o /dev/null "$url"; then
            echo "$pid"
            return 0
        fi
        sleep 0.2
    done
    echo "node $label did not come up on port $http_port (see $SCRATCH/node-$label.log)" >&2
    return 1
}

A_HTTP="$(find_free_port)"; A_HTTPS="$(find_free_port)"
B_HTTP="$(find_free_port)"; B_HTTPS="$(find_free_port)"

echo "--- boot node A (current, http:$A_HTTP https:$A_HTTPS)"
NODE_A_PID="$(boot_node a "$A_HTTP" "$A_HTTPS")"
echo "--- boot node B (min-raised to $MIN_CLIENT_RAISE, http:$B_HTTP https:$B_HTTPS)"
NODE_B_PID="$(boot_node b "$B_HTTP" "$B_HTTPS" "HOPNET_MIN_CLIENT_OVERRIDE=$MIN_CLIENT_RAISE")"
phase_result "boot-nodes" PASS

# --- provision ---------------------------------------------------------

# provision <http_port> <claim_code> <user> -> "token spki https_port"
provision() {
    local http_port="$1" claim="$2" user="$3"
    local base="http://127.0.0.1:$http_port"

    local passphrase
    passphrase="$(curl -sf -X POST "$base/api/setup" \
        -H 'Content-Type: application/json' \
        -d "{\"username\":\"$user\",\"node_name\":\"$user-node\"}" | jq -r .passphrase)"
    [[ -n "$passphrase" && "$passphrase" != null ]] || { echo "setup failed on :$http_port" >&2; return 1; }

    # Test-mode mint registers a device via consensus; poll until decided.
    local api_key=""
    for _ in $(seq 1 50); do
        api_key="$(curl -s "$base/api/integrations/fileprovider/test" \
            -H "x-hopnet-client-version: $claim" | jq -r '.api_key // empty' 2>/dev/null || true)"
        [[ -n "$api_key" ]] && break
        sleep 0.2
    done
    [[ -n "$api_key" ]] || { echo "device mint failed on :$http_port" >&2; return 1; }

    local jwt
    jwt="$(curl -sf -X POST "$base/api/login" \
        -H 'Content-Type: application/json' \
        -d "{\"username\":\"$user\",\"passphrase\":\"$passphrase\"}" | jq -r .token)"
    [[ -n "$jwt" && "$jwt" != null ]] || { echo "login failed on :$http_port" >&2; return 1; }

    local info spki https_port
    info="$(curl -sf "$base/api/devices/pairing-info" -H "Authorization: Bearer $jwt")"
    spki="$(echo "$info" | jq -r .spki_sha256)"
    https_port="$(echo "$info" | jq -r .https_port)"
    [[ "$spki" =~ ^[0-9a-f]{64}$ ]] || { echo "bad spki from :$http_port: $spki" >&2; return 1; }

    echo "$api_key $spki $https_port"
}

echo "--- provision node A"
read -r A_TOKEN A_SPKI A_TLS_PORT <<<"$(provision "$A_HTTP" "$CLIENT_CODE" e2e-a)"
echo "--- provision node B (claiming $HARNESS_CLAIM_B)"
read -r B_TOKEN B_SPKI B_TLS_PORT <<<"$(provision "$B_HTTP" "$HARNESS_CLAIM_B" e2e-b)"
phase_result "provision" PASS

# --- emulator ----------------------------------------------------------

echo "--- emulator (AVD in scratch, system image android-36 x86_64)"
export ANDROID_AVD_HOME="$SCRATCH/avd"
mkdir -p "$ANDROID_AVD_HOME"
# Piped "no" answers the interactive custom-hardware-profile prompt,
# which otherwise dies on a closed stdin ("offset 0, count -1").
echo "no" | avdmanager create avd --force -n "$AVD_NAME" \
    -k "system-images;android-36;google_apis;x86_64" >/dev/null
emulator -avd "$AVD_NAME" -no-window -no-audio -no-boot-anim \
    -gpu swiftshader_indirect >"$SCRATCH/emulator.log" 2>&1 &
EMULATOR_STARTED=1

adb wait-for-device
booted=""
for _ in $(seq 1 "$EMULATOR_BOOT_TIMEOUT"); do
    if [[ "$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" == "1" ]]; then
        booted=1
        break
    fi
    sleep 1
done
[[ -n "$booted" ]] || { echo "emulator failed to boot within ${EMULATOR_BOOT_TIMEOUT}s (see $SCRATCH/emulator.log)"; exit 1; }
phase_result "emulator-boot" PASS

# --- gradle ------------------------------------------------------------

AAPT2_ARGS=()
[[ -n "${HOPNET_AAPT2:-}" ]] && AAPT2_ARGS=("-Pandroid.aapt2FromMavenOverride=$HOPNET_AAPT2")

run_gradle_phase() {
    local name="$1"
    shift
    echo "--- gradle $name"
    if (cd "$APP_DIR" && ./gradlew "$@" "${AAPT2_ARGS[@]}"); then
        phase_result "$name" PASS
    else
        phase_result "$name" FAIL
        return 1
    fi
}

run_gradle_phase assembleDebug assembleDebug
run_gradle_phase unit-tests testDebugUnitTest
run_gradle_phase instrumented connectedDebugAndroidTest \
    "-Pandroid.testInstrumentationRunnerArguments.host=10.0.2.2" \
    "-Pandroid.testInstrumentationRunnerArguments.port=$A_TLS_PORT" \
    "-Pandroid.testInstrumentationRunnerArguments.spki=$A_SPKI" \
    "-Pandroid.testInstrumentationRunnerArguments.token=$A_TOKEN" \
    "-Pandroid.testInstrumentationRunnerArguments.upgradedHost=10.0.2.2" \
    "-Pandroid.testInstrumentationRunnerArguments.upgradedPort=$B_TLS_PORT" \
    "-Pandroid.testInstrumentationRunnerArguments.upgradedSpki=$B_SPKI" \
    "-Pandroid.testInstrumentationRunnerArguments.upgradedToken=$B_TOKEN"
