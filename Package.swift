// swift-tools-version:6.2
import PackageDescription

let package = Package(
    name: "sqlserver-nio",
    platforms: [
        .macOS(.v10_15),
    ],
    products: [
        .library(
            name: "SQLServerKit",
            targets: ["SQLServerKit"]),
        .library(
            name: "SQLServerKitTesting",
            targets: ["SQLServerKitTesting"]),
        .executable(
            name: "sqlserver-test-fixture",
            targets: ["SQLServerFixtureTool"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.65.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.26.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.4"),
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.2.0"),
        .package(url: "https://github.com/apple/swift-collections.git", from: "1.1.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "3.0.0"),
        .package(url: "https://github.com/swiftlang/swift-docc-plugin", from: "1.4.0"),
    ],
    targets: [
        .target(
            name: "SQLServerTDS",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "Collections", package: "swift-collections"),
                .product(name: "Crypto", package: "swift-crypto"),
            ],
            path: "Sources/SQLServerTDS",
            linkerSettings: [
                .linkedFramework("GSS", .when(platforms: [.macOS])),
            ]
        ),
        .target(
            name: "SQLServerKit",
            dependencies: [
                "SQLServerTDS",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
            ],
            path: "Sources/SQLServerKit"
        ),
        .target(
            name: "SQLServerKitTesting",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
            ],
            path: "Sources/SQLServerKitTesting"
        ),
        .target(
            name: "SQLServerKitXCTestSupport",
            dependencies: [
                "SQLServerKit",
                "SQLServerKitTesting",
            ],
            path: "Sources/SQLServerKitXCTestSupport"
        ),
        .executableTarget(
            name: "SQLServerFixtureTool",
            dependencies: ["SQLServerKitTesting"],
            path: "Sources/SQLServerFixtureTool"
        ),
        .testTarget(
            name: "SQLServerKitTests",
            dependencies: [
                "SQLServerKit",
                "SQLServerKitTesting",
                "SQLServerKitXCTestSupport",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ],
            path: "Tests",
            exclude: [
                "EnvironmentConfig.swift.template",
                "README.md",
                "SETUP.md",
                "Support",
                "TestTemplate.swift",
                "CoreTests/QueryTests.swift.disabled",
                "CoreTests/StreamingTests.swift.disabled",
                "MigrationTests/ArchitectureTests.swift.disabled",
                "MigrationTests/NodeMSSQLCompatibilityTests.swift.disabled",
                "PerformanceTests/PerformanceTests.swift.disabled",
                "SQLServerTDSTests/TDSConnectionRawSqlTests.swift.disabled",
                "SQLServerTDSTests/TDSLoginDuplicateTests.swift.disabled",
                "WorkflowTests/WorkflowTests.swift.disabled",
                "TDSLayerTests"
            ]
        ),
        .testTarget(
            name: "TDSLayerTests",
            dependencies: [
                "SQLServerTDS",
                "SQLServerKitTesting",
                "SQLServerKitXCTestSupport",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/TDSLayerTests"
        ),
    ]
)
