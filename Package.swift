// swift-tools-version:5.5
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
            name: "SQLServerTDS",
            targets: ["SQLServerTDS"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    ],
    targets: [
        .target(
            name: "SQLServerKit",
            dependencies: [
                "SQLServerTDS",
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Logging", package: "swift-log"),
            ],
            path: "Sources/SQLServerKit"),
        .target(
            name: "SQLServerTDS",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Logging", package: "swift-log"),
            ],
            path: "Sources/SQLServerTDS"),

        // Connection Tests
        .testTarget(
            name: "SQLServerConnectionTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/ConnectionTests"),

        // Table Tests
        .testTarget(
            name: "SQLServerTableTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/TableTests"),

        // Index Tests
        .testTarget(
            name: "SQLServerIndexTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/IndexTests"),

        // Constraint Tests
        .testTarget(
            name: "SQLServerConstraintTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/ConstraintTests"),

        // View Tests
        .testTarget(
            name: "SQLServerViewTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/ViewTests"),

        // Trigger Tests
        .testTarget(
            name: "SQLServerTriggerTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/TriggerTests"),

        // Routine Tests
        .testTarget(
            name: "SQLServerRoutineTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/RoutineTests"),

        // Security Tests
        .testTarget(
            name: "SQLServerSecurityTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/SecurityTests"),

        // Agent Tests
        .testTarget(
            name: "SQLServerAgentTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/AgentTests"),

        // Bulk Tests
        .testTarget(
            name: "SQLServerBulkTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/BulkTests"),

        // Transaction Tests
        .testTarget(
            name: "SQLServerTransactionTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/TransactionTests"),

        // Type Tests
        .testTarget(
            name: "SQLServerTypeTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/TypeTests"),

        // DataType Tests
        .testTarget(
            name: "SQLServerDataTypeTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/DataTypeTests"),

        // Metadata Tests
        .testTarget(
            name: "SQLServerMetadataTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/MetadataTests"),

        // Advanced Tests
        .testTarget(
            name: "SQLServerAdvancedTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/AdvancedTests"),

        // Performance Tests
        .testTarget(
            name: "SQLServerPerformanceTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/PerformanceTests"),

        // Integration Tests
        .testTarget(
            name: "SQLServerIntegrationTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            path: "Tests/IntegrationTests"),
        .testTarget(
            name: "SQLServerTDSTests",
            dependencies: [
                "SQLServerTDS",
                "SQLServerKit",
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
        ),
    ]
)
