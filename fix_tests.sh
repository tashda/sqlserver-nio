#!/bin/bash

# Script to update all test files to use the new shared infrastructure

# List of files to update
files=(
    "Tests/TableTests/TableIndexOptionsTests.swift"
    "Tests/TableTests/TableTypeCoverageTests.swift"
    "Tests/TableTests/TableDefinitionTests.swift"
    "Tests/TableTests/TableScriptingTests.swift"
    "Tests/DataTypeTests/NBCRowBitmapTests.swift"
    "Tests/DataTypeTests/PLPChunkingTests.swift"
    "Tests/DataTypeTests/LegacyLOBTests.swift"
    "Tests/PerformanceTests/ExplorerFlowTests.swift"
    "Tests/PerformanceTests/BatchOperationsTests.swift"
    "Tests/IndexTests/IndexLifecycleTests.swift"
    "Tests/IndexTests/ColumnstoreIndexTests.swift"
    "Tests/IndexTests/IndexMatrixTests.swift"
    "Tests/IndexTests/ViewIndexTests.swift"
    "Tests/IntegrationTests/IntegrationTests.swift"
    "Tests/BulkTests/BulkCopyTests.swift"
    "Tests/RoutineTests/TableValuedParameterTests.swift"
    "Tests/RoutineTests/RoutineParameterMatrixTests.swift"
    "Tests/RoutineTests/AdventureWorksRoutineTests.swift"
    "Tests/RoutineTests/RoutineLifecycleTests.swift"
    "Tests/TransactionTests/IsolationMatrixTests.swift"
    "Tests/TransactionTests/TransactionLifecycleTests.swift"
    "Tests/SecurityTests/SecurityLifecycleTests.swift"
    "Tests/SecurityTests/ServerSecurityVariantsTests.swift"
    "Tests/AdvancedTests/TemporalMatrixTests.swift"
    "Tests/AdvancedTests/TemporalPartitionedTests.swift"
    "Tests/AdvancedTests/PartitionSchemeMatrixTests.swift"
    "Tests/MetadataTests/PrimaryKeyEnumerationTests.swift"
    "Tests/MetadataTests/ViewColumnsTests.swift"
    "Tests/MetadataTests/MetadataConcurrencyTests.swift"
    "Tests/MetadataTests/MetadataParameterLoadTests.swift"
    "Tests/ConnectionTests/VersionTests.swift"
    "Tests/ConnectionTests/ConnectionLifecycleTests.swift"
    "Tests/ConnectionTests/ReliabilityTests.swift"
    "Tests/ConnectionTests/RPCTests.swift"
    "Tests/ConstraintTests/ForeignKeyCascadeTests.swift"
    "Tests/ConstraintTests/ConstraintLifecycleTests.swift"
    "Tests/ViewTests/ViewLifecycleTests.swift"
    "Tests/TriggerTests/TriggerLifecycleTests.swift"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "Updating $file..."
        # Replace the class declaration and setup
        sed -i '' 's/final class \([^:]*\): XCTestCase {/final class \1: XCTestCase {\
    var group: EventLoopGroup!\
    var client: SQLServerClient!\
\
    override func setUp() async throws {\
        continueAfterFailure = false\
\
        \/\/ Load environment configuration\
        TestEnvironmentManager.loadEnvironmentVariables()\
\
        \/\/ Configure logging\
        _ = isLoggingConfigured\
\
        \/\/ Create connection\
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)\
        self.client = try await SQLServerClient.connect(\
            configuration: makeSQLServerClientConfiguration(),\
            eventLoopGroupProvider: .shared(group)\
        ).get()\
    }\
\
    override func tearDown() async throws {\
        try await client?.shutdownGracefully().get()\
        try await group?.shutdownGracefully()\
        group = nil\
    }/g' "$file"
    fi
done

echo "Done updating test files!"
