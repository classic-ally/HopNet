// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "HopNetFileProvider",
    platforms: [
        .macOS("14.1")
    ],
    products: [
        .executable(name: "HopNetFileProviderExtension", targets: ["HopNetFileProviderExtension"]),
        // Test executables
        .executable(name: "TestSetup", targets: ["TestSetup"]),
        .executable(name: "TestCreation", targets: ["TestCreation"]),
        .executable(name: "TestModification", targets: ["TestModification"]),
        .executable(name: "TestDeletion", targets: ["TestDeletion"])
        // TODO: Uncomment as we implement each test
        // .executable(name: "TestDownload", targets: ["TestDownload"]),
        // .executable(name: "TestErrors", targets: ["TestErrors"])
    ],
    dependencies: [],
    targets: [
        // Shared library with the real implementation (exclude main.swift)
        .target(
            name: "HopNetFileProviderCore",
            path: "Sources/HopNetFileProviderExtension",
            exclude: ["main.swift"]
        ),
        
        // Shared test helpers library
        .target(
            name: "TestHelpers",
            dependencies: ["HopNetFileProviderCore"],
            path: "Tests",
            sources: ["TestHelpers.swift"],
            linkerSettings: [
                .linkedFramework("FileProvider"),
                .linkedFramework("Foundation")
            ]
        ),
        
        .executableTarget(
            name: "HopNetFileProviderExtension",
            dependencies: ["HopNetFileProviderCore"],
            path: "Sources/HopNetFileProviderExtension",
            exclude: [
                "HopNetExtension.swift",
                "HopNetApiClient.swift", 
                "HopNetFileProviderItem.swift",
                "Generated"
            ],
            sources: ["main.swift"],
            linkerSettings: [
                .linkedFramework("FileProvider"),
                .linkedFramework("Foundation"),
                .unsafeFlags(["-Xlinker", "-e", "-Xlinker", "_NSExtensionMain"])
            ]
        ),
        
        // Test executables depend on both core and test helpers
        .executableTarget(
            name: "TestSetup",
            dependencies: ["HopNetFileProviderCore", "TestHelpers"],
            path: "Tests",
            sources: ["TestSetup.swift"],
            linkerSettings: [
                .linkedFramework("FileProvider"),
                .linkedFramework("Foundation")
            ]
        ),
        .executableTarget(
            name: "TestCreation",
            dependencies: ["HopNetFileProviderCore", "TestHelpers"],
            path: "Tests",
            sources: ["TestCreation.swift"],
            linkerSettings: [
                .linkedFramework("FileProvider"),
                .linkedFramework("Foundation")
            ]
        ),
        .executableTarget(
            name: "TestModification",
            dependencies: ["HopNetFileProviderCore", "TestHelpers"],
            path: "Tests",
            sources: ["TestModification.swift"],
            linkerSettings: [
                .linkedFramework("FileProvider"),
                .linkedFramework("Foundation")
            ]
        ),
        .executableTarget(
            name: "TestDeletion", 
            dependencies: ["HopNetFileProviderCore", "TestHelpers"],
            path: "Tests",
            sources: ["TestDeletion.swift"],
            linkerSettings: [
                .linkedFramework("FileProvider"),
                .linkedFramework("Foundation")
            ]
        )
        // TODO: Uncomment as we implement each test
        // .executableTarget(
        //     name: "TestDownload",
        //     dependencies: ["HopNetFileProviderCore", "TestHelpers"],
        //     path: "Tests", 
        //     sources: ["TestDownload.swift"],
        //     linkerSettings: [
        //         .linkedFramework("FileProvider"),
        //         .linkedFramework("Foundation")
        //     ]
        // ),
        // .executableTarget(
        //     name: "TestErrors",
        //     dependencies: ["HopNetFileProviderCore", "TestHelpers"],
        //     path: "Tests",
        //     sources: ["TestErrors.swift"],
        //     linkerSettings: [
        //         .linkedFramework("FileProvider"),
        //         .linkedFramework("Foundation")
        //     ]
        // )
    ]
)