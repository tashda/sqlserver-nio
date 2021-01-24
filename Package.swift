// swift-tools-version:5.2
import PackageDescription

let package = Package(
    name: "swift-tds",
    platforms: [
       .macOS(.v10_15)
    ],
    products: [
        .library(name: "TDS", targets: ["TDS"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", "1.0.0" ..< "3.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    ],
    targets: [
        .target(name: "TDS", dependencies: [
            .product(name: "Logging", package: "swift-log"),
            .product(name: "Metrics", package: "swift-metrics"),
            .product(name: "NIO", package: "swift-nio"),
            .product(name: "NIOSSL", package: "swift-nio-ssl"),
        ]),
        .testTarget(name: "TDSTests", dependencies: [
            .target(name: "TDS"),
            .product(name: "NIOTestUtils", package: "swift-nio"),
        ]),
    ]
)
