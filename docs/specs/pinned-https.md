# RFC-022: Pinned HTTPS — TLS-Only Network Surface

**Status**: Implemented (2026-08)
**Depends on**: RFC-012 (device token sessions)
**Unblocks**: Android Hop Drive client (remote DocumentProvider)

## Summary

The node's HTTP API is no longer reachable over plaintext from the
network. TLS takes over the standard port (`0.0.0.0:34632`) with a
per-node self-signed certificate generated at first boot; clients
authenticate the node by pinning the certificate's SPKI SHA-256
fingerprint, learned out-of-band during device pairing. Plaintext HTTP
survives only as a loopback IPC surface on a kernel-assigned port.

## Motivation

Device tokens (`Bearer {device_id}.{secret}`, RFC-012) carry the raw
device secret on every request, and that secret unwraps the user's
Ed25519 private key on the serving node. That was acceptable while every
consumer was co-resident (FileProvider, photo-ingress, hopnet-mount all
speak to `127.0.0.1`), but Hop Drive — the Android DocumentProvider
client — talks to the node across the LAN or farther. A cleartext
network hop would expose a credential equivalent to the user's identity.

WebPKI is a poor fit for nodes on home networks (no stable public
hostname, no ACME reachability). Key-continuity pinning needs no
infrastructure and is exactly the trust model iroh already uses for
node-to-node traffic: trust a public key learned out-of-band. The
certificate is a carrier for the key, nothing more.

## Transport posture

| Surface | Bind | Port | Protocol |
|---------|------|------|----------|
| Network API (devices, browsers on LAN) | `0.0.0.0` | 34632 (`HOPNET_HTTPS_PORT`) | HTTPS, self-signed, SPKI-pinned |
| Loopback IPC (webview, FileProvider, photo-ingress, mount, seeder) | `127.0.0.1` | kernel-assigned (`HOPNET_HTTP_PORT` pins) | HTTP |
| Node-to-node | iroh | — | QUIC/TLS (unchanged) |

Both listeners serve the same router; authentication is unchanged
(JWT / device-token middleware per route).

Loopback consumers discover the dynamic plaintext port through the
existing seams: `ACTUAL_BACKEND_PORT` → Keychain `base_url` (macOS),
`$XDG_RUNTIME_DIR/hopnet/endpoint` (Linux; also read by the
photo-seeder now). The endpoint file is skipped under
`HOPNET_TEST_MODE`; the orchestrator instead forwards
`HOPNET_HTTP_PORT=34630` into containers for co-resident CLI consumers.

TLS is bound BEFORE plaintext so a kernel-assigned loopback port can
never land on the TLS port and shadow the wildcard bind.

### Failure policy

- **Headless**: TLS init failure is fatal. TLS is the only network
  surface; a warn-and-continue would leave the server dark behind a
  single log line.
- **GUI**: warn and continue loopback-only. The webview is the desktop
  app's core; a port clash with another local process must not brick
  it. The pairing UI reports `tls_enabled: false`.
- `HOPNET_DISABLE_TLS` (presence) skips the listener explicitly — dev
  runs (`HOPNET_DISABLE_TLS=1 HOPNET_HTTP_PORT=34632` reproduces the
  pre-RFC dev shape) and deployments fronted by a loopback proxy
  (e.g. `tailscale serve`).

## Certificate lifecycle

- Generated at first boot by `src/tls.rs`: ECDSA P-256 via rcgen,
  CN "HopNet Node", SAN "hopnet-node", validity 2026–2126. None of
  those fields carry trust; the pin is the trust decision.
- Persisted under `{data}/hopnet/tls/` (`node-cert.pem`,
  `node-key.pem` 0600), tmp+rename atomic writes.
- **Corrupt or partial material is an error, never a regeneration** — a
  silent re-key would invalidate the pin held by every paired client.
  Deleting the `tls/` directory is the manual re-key path; every device
  must then re-pair.
- Rotation with continuity (signed rollover), and zeroize-on-drop for
  the in-memory key, are future work.

## SPKI fingerprint

`spki_sha256` = lower-hex SHA-256 over the certificate's
SubjectPublicKeyInfo DER — pin the key, not the certificate, so a
future cert re-issue over the same key keeps pins valid. Always
computed from the persisted certificate (what a connecting client
observes), format mirroring the regenesis chain-id fingerprint (strict
64 lower-hex).

## Pairing

`GET /api/devices/pairing-info` (JWT, same surface as device
registration) returns:

```json
{ "tls_enabled": true, "https_port": 34632, "spki_sha256": "<64 lower-hex>" }
```

The device-key modal encodes QR payload v1:

```json
{ "v": 1, "kind": "hopnet-device", "host": "192.168.1.20", "port": 34632,
  "spki": "<64 lower-hex>", "token": "<device_id>.<secret_hex>" }
```

`host` is the address the operator's browser reached the node at
(`window.location.hostname`), omitted on loopback (Tauri webview, local
dev) — the pairing client prompts for it. mDNS advertisement is future
work. When the node has no TLS listener the QR falls back to the bare
API key with a visible hint.

A pairing client MUST verify the server certificate solely by comparing
the SPKI SHA-256 against the pinned value: no chain building, no
validity-window check, no hostname verification. See the
`tls-pinned-https` orchestrator scenario's verifier for the reference
implementation.

## Consequences

- **Breaking**: anything that previously hit `http://host:34632` over
  the network must switch to `https` and accept or pin the self-signed
  cert. LAN browser access to the web UI now shows a one-time
  self-signed interstitial (real certificates for the web UI are out of
  scope here).
- Orchestrator and integration tests reach nodes through
  `insecure_client()` (container boundary is the trust boundary);
  the dedicated `tls-pinned-https` scenario exercises the real pin,
  positive and negative.
- The desktop GUI now opens an authenticated network port
  (`0.0.0.0:34632`); all routes remain behind JWT / device-token auth.
- **Stale hopnet-mount configs**: a `mount.json` whose `url` was stored
  as `http://127.0.0.1:34632` in the fixed-port era outranks the
  endpoint file and now points plaintext at the TLS port. Re-run
  `hopnet-mount login` (or drop the `url` key) on affected hosts.

## Future work

- [ ] Certificate rotation with signed rollover (pin continuity)
- [ ] mDNS advertisement of `{host, port}` for pairing without manual entry
- [x] Android Hop Drive client consuming QR payload v1 ([hop-drive-android](hop-drive-android.md))
- [ ] iroh as an alternative device transport (NAT traversal, roaming)
