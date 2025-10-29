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
            ],
            path: "Tests/SQLServerKitTests",
            sources: [
                            "SQLServerKitIntegrationTests.swift",
                            "SQLServerIntegrationTests.swift",
                            "SQLServerPrimaryKeySchemaEnumerationTests.swift",
                            "SQLServerAgentTests.swift",
                            "SQLServerAgentPermissionTests.swift",
                            "SQLServerServerSecurityTests.swift",
                            "SQLServerTableAdministrationTests.swift",
                            "SQLServerTransactionTests.swift",
                            "SQLServerBatchTests.swift",
                            "SQLServerBulkCopyTests.swift",
                            "SQLServerConnectionTests.swift",
                            "SQLServerQuerySplitterTests.swift",
                            "SQLServerRoutineTests.swift",
                            "SQLServerViewTests.swift",
                            "SQLServerIndexTests.swift",
                            "SQLServerConstraintTests.swift",
                            "SQLServerTableValuedParameterTests.swift",
                            "SQLServerTriggerTests.swift",
                            "SQLServerSecurityTests.swift",
                            "SQLServerVersionTests.swift",
                            "SQLServerTableDefinitionTests.swift",
                            "SQLServerTableDefinitionCoverageTests.swift",
                            "SQLServerTableIndexOptionsTests.swift",
                            "SQLServerTemporalPartitionedTests.swift",
                            "SQLServerLegacyLobRoundTripTests.swift",
                            "SQLServerRoutineParameterMatrixTests.swift",
                            "SQLServerTableScriptingMatrixTests.swift",
                            "SQLServerIndexMatrixTests.swift",
                            "SQLServerMetadataParameterLoadTests.swift",
                            "SQLServerMetadataConcurrencyTests.swift",
                            "SQLServerViewIndexMatrixTests.swift",
                            "SQLServerColumnstoreIndexTests.swift",
                            "SQLServerNbcRowBitmapTests.swift",
                            "SQLServerMetadataViewColumnsTests.swift",
                            "SQLServerMetadataCommentsTests.swift",
                            "SQLServerAdventureWorksRoutineTests.swift",
                            "SQLServerForeignKeyCascadeMatrixTests.swift",
                            "SQLServerPlpChunkingTests.swift",
                            "SQLServerTemporalMatrixTests.swift",
                            "SQLServerPartitionSchemeMatrixTests.swift",
                            "SQLServerTransactionIsolationMatrixTests.swift",
                            "SQLServerDataTypeRoundTripTests.swift",
                            "Test+Helpers.swift",
                            "SQLServerReturnValueDecodeTests.swift",
                            "SQLServerRPCTests.swift",
                            "SQLServerDeadlockRetryTests.swift",
                            "SQLServerClassificationDecodeTests.swift",
                            "SQLServerExplorerFlowTests.swift",
                            "SQLServerEnvDiagnosticsTests.swift"
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
