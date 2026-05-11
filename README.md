# HopNet

A distributed filesystem with Byzantine fault-tolerant consensus and Reed-Solomon erasure coding, designed with high-quality ergonomics for easy deployment.

> Wire protocol and on-disk format may change without migration. Not yet ready to store anything you cannot afford to lose.

## Building

Nix flake at the repository root pins the entire toolchain.

```sh
nix develop                 # enter dev shell (rust, pnpm, node, openssl, ...)
nix build .#default         # headless `hopnet` binary
```

For the signed macOS `.app` bundle (FileProvider extension + Tauri shell):

```sh
bash scripts/build-hopnet-macos.sh
```

This invokes the multi-stage build in `scripts/macos/` (typeshare → Swift
extension → Tauri/Rust → bundle embed → codesign → optional notarize+package).
Notarize/package stages activate when `APPLE_ID` / `TEAM_ID` /
`NOTARY_PASSWORD` are set (see `scripts/macos/.env.example`).

## Repository layout

```
src/             Rust backend (consensus, storage, transport, HTTP API)
common/          Shared types crate (typeshare bridges to TS + Swift)
frontend/        Svelte/Vite web UI bundled into the Tauri app, webui when headless
apple/           Swift FileProvider extension + entitlements + scripts
android/         Android StorageProvider client (in progress)
orchestrator/    Docker-based mesh test harness
snapshotter/     Database function regression-test tool
scripts/         Build + release scripting (incl. macOS pipeline)
docs/            RFCs, system overview, design notes
.forgejo/        CI workflows
```

## License

[AGPL-3.0-only](LICENSE). HopNet is strictly copyleft with no exemption for SaaS deployments. If you build a network service by modifying HopNet, you must release source for the derivative works.

## Contributing

This is solo-developer dogfood right now. Input welcome but the architecture is in flux, and breaking changes ship without notice.

Please send security disclosures to [allison@hopnet.app](mailto:allison@hopnet.app).