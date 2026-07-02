// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "photokit-spike",
    platforms: [.macOS(.v13)],
    targets: [
        .executableTarget(
            name: "photokit-spike",
            path: "Sources",
            linkerSettings: [
                .linkedFramework("Photos"),
                // Embed Info.plist so a bare CLI binary can request Photos access
                .unsafeFlags([
                    "-Xlinker", "-sectcreate",
                    "-Xlinker", "__TEXT",
                    "-Xlinker", "__info_plist",
                    "-Xlinker", "Sources/Info.plist",
                ]),
            ]
        )
    ]
)
