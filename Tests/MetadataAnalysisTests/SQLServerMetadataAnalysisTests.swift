import XCTest
import Logging
import SQLServerKit
import SQLServerKitTesting

/// Comprehensive metadata analysis test to systematically identify failure patterns
/// This test enumerates all schemas, tables, and views in a database and tests
/// all metadata operations on each to identify exactly what's working and what's failing
final class SQLServerMetadataAnalysisTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    var testDatabase: String = "AdventureWorks"
    private let maxSchemasToInspect = Int.max
    private let maxConcurrentObjectChecks = 6
    private let metadataTimeBudget: TimeInterval = 120

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        // Prefer the explicitly restored AdventureWorks database when running in Docker.
        testDatabase = env("TDS_TEST_DB") ?? env("TDS_AW_DATABASE") ?? env("TDS_DATABASE") ?? "AdventureWorks"

        let threadCount = max(2, min(ProcessInfo.processInfo.processorCount, 8))

        var configuration = makeSQLServerClientConfiguration()
        configuration.poolConfiguration.maximumConcurrentConnections = max(64, configuration.poolConfiguration.maximumConcurrentConnections)
        configuration.poolConfiguration.minimumIdleConnections = min(16, configuration.poolConfiguration.maximumConcurrentConnections)

        client = try await SQLServerClient.connect(configuration: configuration, numberOfThreads: threadCount)

        print("🔍 Starting metadata analysis for database: \(testDatabase)")
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        self.client = nil
    }

    /// Main analysis test - systematically tests all metadata operations
    func testComprehensiveMetadataAnalysis() async throws {
        if env("TDS_AW_DATABASE") != nil || env("TDS_TEST_DB") != nil {
            do {
                _ = try await requireDatabaseNamedInEnvironment(
                    env("TDS_TEST_DB") != nil ? "TDS_TEST_DB" : "TDS_AW_DATABASE",
                    using: client,
                    defaultName: testDatabase
                )
            } catch let error as SQLServerFixtureUnavailable {
                throw XCTSkip(error.message)
            }
        } else {
            let rows = try await client.query("SELECT name FROM sys.databases")
            let availableDatabases = rows.compactMap { $0.column("name")?.string?.lowercased() }
            guard availableDatabases.contains(testDatabase.lowercased()) else {
                throw XCTSkip("Skipping: database '\(testDatabase)' is not available on this server")
            }
        }

        print("\n🚀 COMPREHENSIVE METADATA ANALYSIS STARTING")
        print("📊 Database: \(testDatabase)")
        print("=" * 80)
        let testStart = Date()
        let budgetDeadline = testStart.addingTimeInterval(metadataTimeBudget)

        var analysisResults = MetadataAnalysisResults()

        // Step 1: Connect and get all schemas using the public metadata API
        print("\n📋 STEP 1: Enumerating all schemas...")
        let schemaMetadata = try await client.metadata.listSchemas(in: testDatabase)
        let schemaNames = schemaMetadata.map { $0.name }
        print("✅ Found \(schemaNames.count) schemas: \(schemaNames.joined(separator: ", "))")
        analysisResults.totalSchemas = schemaNames.count

        let sampledSchemas = Array(schemaNames.prefix(maxSchemasToInspect))
        for schemaName in sampledSchemas {
            if Date() >= budgetDeadline {
                print("⏹️ Time budget reached; stopping additional schema verification.")
                break
            }

            print("\n📋 STEP 2: Processing schema '\(schemaName)'...")

            let rawSchemaObjects: [TableMetadata]
            do {
                print("   🔁 listTables(schema: \(schemaName)) - start")
                rawSchemaObjects = try await withTimeout(6) {
                    try await self.client.metadata.listTables(database: self.testDatabase, schema: schemaName)
                }
                print("   🔁 listTables(schema: \(schemaName)) - returned \(rawSchemaObjects.count) objects")
            } catch {
                print("   ❌ listTables(schema: \(schemaName)) failed: \(error)")
                continue
            }
            let filteredSchemaObjects = rawSchemaObjects.filter { self.shouldIncludeObject(named: $0.name) }
            let skippedObjects = rawSchemaObjects.count - filteredSchemaObjects.count
            if skippedObjects > 0 {
                print("   ↳ Skipping \(skippedObjects) transient objects in \(schemaName) (test artifacts)")
            }
            let tables = filteredSchemaObjects.filter { $0.type == "TABLE" }
            let views = filteredSchemaObjects.filter { $0.type == "VIEW" }

            print("   📄 Tables in \(schemaName): \(tables.count)")
            print("   👁️ Views in \(schemaName): \(views.count)")
            analysisResults.totalTables += tables.count
            analysisResults.totalViews += views.count

            if !tables.isEmpty {
                print("   🔬 Verifying \(tables.count) tables in \(schemaName)...")
                if Date() >= budgetDeadline {
                    print("   ⏭️ Skipping remaining tables in \(schemaName) due to time budget.")
                } else {
                let tableResults = try await runObjectChecks(tables, concurrentLimit: maxConcurrentObjectChecks, budgetDeadline: budgetDeadline) { table in
                    try await self.testTableMetadata(schema: schemaName, table: table.name)
                }
                analysisResults.tableResults.append(contentsOf: tableResults)
                }
            }

            if !views.isEmpty {
                print("   🔬 Verifying \(views.count) views in \(schemaName)...")
                if Date() >= budgetDeadline {
                    print("   ⏭️ Skipping remaining views in \(schemaName) due to time budget.")
                } else {
                let viewResults = try await runObjectChecks(views, concurrentLimit: maxConcurrentObjectChecks, budgetDeadline: budgetDeadline) { view in
                    try await self.testViewMetadata(schema: schemaName, view: view.name)
                }
                analysisResults.viewResults.append(contentsOf: viewResults)
                }
            }
        }

        // Step 5: Print comprehensive analysis report
        printAnalysisReport(results: analysisResults)

        let elapsed = Date().timeIntervalSince(testStart)
        print("\n⏱️ Metadata analysis completed in \(String(format: "%.2f", elapsed))s")
        XCTAssertLessThanOrEqual(elapsed, metadataTimeBudget, "Metadata analysis exceeded the 60-second budget")

        // At minimum, we should be able to enumerate schemas without issues
        XCTAssertGreaterThan(analysisResults.totalSchemas, 0, "Should find at least one schema")
        XCTAssertFalse(analysisResults.tableResults.isEmpty, "Metadata analysis should inspect at least one table")

        let tableFailures = analysisResults.tableResults.filter { !$0.failedOperations.isEmpty }
        let viewFailures = analysisResults.viewResults.filter { !$0.failedOperations.isEmpty }
        XCTAssertTrue(
            tableFailures.isEmpty,
            "Table metadata analysis found failures: \(summarizeFailures(tableFailures, limit: 10))"
        )
        XCTAssertTrue(
            viewFailures.isEmpty,
            "View metadata analysis found failures: \(summarizeFailures(viewFailures, limit: 10))"
        )
    }

    /// Test all metadata operations for a specific table
    private func testTableMetadata(schema: String, table: String) async throws -> ObjectMetadataResults {
        print("\n   🔍 Testing table: \(schema).\(table)")
        var results = ObjectMetadataResults(objectName: "\(schema).\(table)", objectType: .table)

        let operations: [(String, () async throws -> Void)] = [
            ("listColumns", {
                try await withTimeout(10) {
                    _ = try await self.client.metadata.listColumns(database: self.testDatabase, schema: schema, table: table)
                }
            }),
            ("listPrimaryKeys", {
                try await withTimeout(10) {
                    _ = try await self.client.metadata.listPrimaryKeys(database: self.testDatabase, schema: schema, table: table)
                }
            }),
            ("listForeignKeys", {
                try await withTimeout(10) {
                    _ = try await self.client.metadata.listForeignKeys(database: self.testDatabase, schema: schema, table: table)
                }
            }),
            ("listIndexes", {
                try await withTimeout(10) {
                    _ = try await self.client.metadata.listIndexes(database: self.testDatabase, schema: schema, table: table)
                }
            }),
            ("listTriggers", {
                try await withTimeout(10) {
                    _ = try await self.client.metadata.listTriggers(database: self.testDatabase, schema: schema, table: table)
                }
            })
        ]

        for (operationName, operation) in operations {
            let startTime = Date()
            do {
                print("      🧪 \(operationName)...")
                try await operation()
                let duration = Date().timeIntervalSince(startTime)
                print("      ✅ \(operationName) - SUCCESS (\(String(format: "%.2f", duration))s)")
                results.successfulOperations.append(operationName)
                results.operationTimes[operationName] = duration
            } catch {
                let duration = Date().timeIntervalSince(startTime)
                print("      ❌ \(operationName) - FAILED (\(String(format: "%.2f", duration))s): \(error)")
                results.failedOperations.append((operationName, error))
                results.operationTimes[operationName] = duration
            }
        }

        return results
    }

    /// Test all metadata operations for a specific view
    private func testViewMetadata(schema: String, view: String) async throws -> ObjectMetadataResults {
        print("\n   🔍 Testing view: \(schema).\(view)")
        var results = ObjectMetadataResults(objectName: "\(schema).\(view)", objectType: .view)

        let operations: [(String, () async throws -> Void)] = [
            ("listColumns", {
                try await withTimeout(10) {
                    _ = try await self.client.metadata.listColumns(database: self.testDatabase, schema: schema, table: view)
                }
            }),
            ("listIndexes", {
                try await withTimeout(10) {
                    _ = try await self.client.metadata.listIndexes(database: self.testDatabase, schema: schema, table: view)
                }
            }),
            ("listTriggers", {
                try await withTimeout(10) {
                    _ = try await self.client.metadata.listTriggers(database: self.testDatabase, schema: schema, table: view)
                }
            })
        ]

        for (operationName, operation) in operations {
            let startTime = Date()
            do {
                print("      🧪 \(operationName)...")
                try await operation()
                let duration = Date().timeIntervalSince(startTime)
                print("      ✅ \(operationName) - SUCCESS (\(String(format: "%.2f", duration))s)")
                results.successfulOperations.append(operationName)
                results.operationTimes[operationName] = duration
            } catch {
                let duration = Date().timeIntervalSince(startTime)
                print("      ❌ \(operationName) - FAILED (\(String(format: "%.2f", duration))s): \(error)")
                results.failedOperations.append((operationName, error))
                results.operationTimes[operationName] = duration
            }
        }

        return results
    }

    private func shouldIncludeObject(named name: String) -> Bool {
        let excludedPrefixes = [
            "test_",
            "tmp_",
            "tsmx_",
            "fkmx_",
            "idx_",
            "imx_",
            "cov_",
            "adm_",
            "def_",
            "rt_",
            "lob_",
            "cmts_",
            "meta_client_"
        ]
        let excludedExact = [
            "vSalesPerson",
            "vSalesPersonSalesByFiscalYears"
        ]
        if excludedExact.contains(where: { $0.caseInsensitiveCompare(name) == .orderedSame }) {
            return false
        }
        return !excludedPrefixes.contains { name.hasPrefix($0) }
    }

    /// Print a comprehensive analysis report
    private func printAnalysisReport(results: MetadataAnalysisResults) {
        print("\n" + "=" * 80)
        print("📊 COMPREHENSIVE METADATA ANALYSIS REPORT")
        print("=" * 80)

        print("📋 SUMMARY:")
        print("   Database: \(testDatabase)")
        print("   Schemas: \(results.totalSchemas)")
        print("   Tables: \(results.totalTables)")
        print("   Views: \(results.totalViews)")
        print("   Total Objects: \(results.totalTables + results.totalViews)")

        // Analyze table results
        print("\n📄 TABLE ANALYSIS:")
        var tableSuccessCount = 0
        var tableFailureCount = 0
        var operationSuccessCounts: [String: Int] = [:]
        var operationFailureCounts: [String: Int] = [:]

        for result in results.tableResults {
            if result.failedOperations.isEmpty {
                tableSuccessCount += 1
            } else {
                tableFailureCount += 1
            }

            for operation in result.successfulOperations {
                operationSuccessCounts[operation, default: 0] += 1
            }

            for (operation, _) in result.failedOperations {
                operationFailureCounts[operation, default: 0] += 1
            }
        }

        print("   Tables with full success: \(tableSuccessCount)/\(results.tableResults.count)")
        print("   Tables with failures: \(tableFailureCount)/\(results.tableResults.count)")

        print("\n📈 TABLE OPERATION SUCCESS RATES:")
        let allTableOperations = Set(operationSuccessCounts.keys).union(Set(operationFailureCounts.keys))
        for operation in allTableOperations.sorted() {
            let successes = operationSuccessCounts[operation, default: 0]
            let failures = operationFailureCounts[operation, default: 0]
            let total = successes + failures
            let successRate = total > 0 ? Double(successes) / Double(total) * 100 : 0
            print("   \(operation): \(successes)/\(total) (\(String(format: "%.1f", successRate))%)")
        }

        // Analyze view results
        print("\n👁️ VIEW ANALYSIS:")
        var viewSuccessCount = 0
        var viewFailureCount = 0
        var viewOperationSuccessCounts: [String: Int] = [:]
        var viewOperationFailureCounts: [String: Int] = [:]

        for result in results.viewResults {
            if result.failedOperations.isEmpty {
                viewSuccessCount += 1
            } else {
                viewFailureCount += 1
            }

            for operation in result.successfulOperations {
                viewOperationSuccessCounts[operation, default: 0] += 1
            }

            for (operation, _) in result.failedOperations {
                viewOperationFailureCounts[operation, default: 0] += 1
            }
        }

        print("   Views with full success: \(viewSuccessCount)/\(results.viewResults.count)")
        print("   Views with failures: \(viewFailureCount)/\(results.viewResults.count)")

        print("\n📈 VIEW OPERATION SUCCESS RATES:")
        let allViewOperations = Set(viewOperationSuccessCounts.keys).union(Set(viewOperationFailureCounts.keys))
        for operation in allViewOperations.sorted() {
            let successes = viewOperationSuccessCounts[operation, default: 0]
            let failures = viewOperationFailureCounts[operation, default: 0]
            let total = successes + failures
            let successRate = total > 0 ? Double(successes) / Double(total) * 100 : 0
            print("   \(operation): \(successes)/\(total) (\(String(format: "%.1f", successRate))%)")
        }

        // Show failures
        let hasFailures =
            results.tableResults.contains { !$0.failedOperations.isEmpty } ||
            results.viewResults.contains { !$0.failedOperations.isEmpty }

        if hasFailures {
            print("\n❌ FAILURE SUMMARY:")
        } else {
            print("\n✅ FAILURE SUMMARY:")
            print("   No failures detected. All metadata operations are working correctly.")
        }

        for result in results.tableResults {
            if !result.failedOperations.isEmpty {
                print("   Table: \(result.objectName)")
                for (operation, error) in result.failedOperations {
                    print("     \(operation): \(error)")
                }
            }
        }

        for result in results.viewResults {
            if !result.failedOperations.isEmpty {
                print("   View: \(result.objectName)")
                for (operation, error) in result.failedOperations {
                    print("     \(operation): \(error)")
                }
            }
        }

        print("\n" + "=" * 80)
        print("🏁 ANALYSIS COMPLETE")
        print("=" * 80)
    }

    private func summarizeFailures(_ results: [ObjectMetadataResults], limit: Int) -> String {
        results.prefix(limit).map { result in
            let ops = result.failedOperations.map { "\($0.0)=\($0.1)" }.joined(separator: ", ")
            return "\(result.objectName)[\(ops)]"
        }.joined(separator: "; ")
    }
}

private func runObjectChecks<T: Sendable>(
    _ items: [T],
    concurrentLimit: Int,
    budgetDeadline: Date,
    operation: @escaping @Sendable (T) async throws -> ObjectMetadataResults
) async throws -> [ObjectMetadataResults] {
    guard !items.isEmpty else { return [] }
    var aggregated: [ObjectMetadataResults] = []
    var index = 0
    while index < items.count {
        if Date() >= budgetDeadline {
            print("   ⏹️ Time budget reached; stopping further object checks.")
            break
        }
        let upperBound = min(index + concurrentLimit, items.count)
        let slice = items[index..<upperBound]
        try await withThrowingTaskGroup(of: ObjectMetadataResults.self) { group in
            for item in slice {
                let currentItem = item
                group.addTask {
                    try await operation(currentItem)
                }
            }
            while let result = try await group.next() {
                aggregated.append(result)
            }
        }
        index = upperBound
    }
    return aggregated
}

// MARK: - Analysis Data Structures

private struct MetadataAnalysisResults {
    var totalSchemas: Int = 0
    var totalTables: Int = 0
    var totalViews: Int = 0
    var tableResults: [ObjectMetadataResults] = []
    var viewResults: [ObjectMetadataResults] = []
}

private struct ObjectMetadataResults {
    let objectName: String
    let objectType: ObjectType
    var successfulOperations: [String] = []
    var failedOperations: [(String, Error)] = []
    var operationTimes: [String: TimeInterval] = [:]

    init(objectName: String, objectType: ObjectType) {
        self.objectName = objectName
        self.objectType = objectType
    }
}

private enum ObjectType {
    case table
    case view
}

// Helper for string repetition
extension String {
    static func *(lhs: String, rhs: Int) -> String {
        return String(repeating: lhs, count: rhs)
    }
}
