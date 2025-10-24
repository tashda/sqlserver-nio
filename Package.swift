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
        .testTarget(
            name: "SQLServerKitTests",
            dependencies: [
                "SQLServerKit",
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ]),
        .testTarget(
            name: "SQLServerTDSTests",
            dependencies: [
                "SQLServerTDS",
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ]),
    ]
)
