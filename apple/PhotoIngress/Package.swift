// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "PhotoIngress",
    platforms: [.macOS(.v14)],
    targets: [
        // Generated FFI header + modulemap (produced by
        // scripts/macos/photo-ingress-build.sh, committed).
        .target(
            name: "CIngressFFI",
            path: "Sources/CIngressFFI"
        ),
        // PhotoKit shim: descriptor extraction + resource streaming over the
        // generated UniFFI bindings.
        .target(
            name: "PhotoIngressKit",
            dependencies: ["CIngressFFI"],
            path: "Sources/PhotoIngressKit",
            linkerSettings: [
                .linkedFramework("Photos")
            ]
        ),
        // The Phase 2 vertical-slice CLI.
        .executableTarget(
            name: "photo-ingress",
            dependencies: ["PhotoIngressKit"],
            path: "Sources/photo-ingress",
            linkerSettings: [
                .linkedFramework("Photos"),
                .unsafeFlags([
                    // Rust staticlib, built by scripts/macos/photo-ingress-build.sh
                    "-L", "../../crates/target/release",
                    "-lingress_ffi",
                    // Embed Info.plist so the bare CLI can request Photos access
                    "-Xlinker", "-sectcreate",
                    "-Xlinker", "__TEXT",
                    "-Xlinker", "__info_plist",
                    "-Xlinker", "Sources/photo-ingress/Info.plist",
                ]),
            ]
        ),
    ]
)
