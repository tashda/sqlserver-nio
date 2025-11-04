import XCTest
import NIO
import Logging
@testable import SQLServerKit

/// Comprehensive metadata analysis test to systematically identify failure patterns
/// This test enumerates all schemas, tables, and views in a database and tests
/// all metadata operations on each to identify exactly what's working and what's failing
final class SQLServerMetadataAnalysisTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    var testDatabase: String = "AdventureWorks2022"

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        testDatabase = ProcessInfo.processInfo.environment["TDS_TEST_DB"] ?? "AdventureWorks2022"

        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()

        print("🔍 Starting metadata analysis for database: \(testDatabase)")
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully()
        try await group?.shutdownGracefully()
    }

    /// Main analysis test - systematically tests all metadata operations
    func testComprehensiveMetadataAnalysis() async throws {
        print("\n🚀 COMPREHENSIVE METADATA ANALYSIS STARTING")
        print("📊 Database: \(testDatabase)")
        print("=" * 80)

        var analysisResults = MetadataAnalysisResults()

        // Step 1: Connect and get all schemas using a raw SQL query
        print("\n📋 STEP 1: Enumerating all schemas...")
        let schemas = try await withReliableConnection(client: client) { conn in
            try await conn.query("SELECT name FROM sys.schemas WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA') ORDER BY name")
        }
        print("✅ Found \(schemas.count) schemas: \(schemas.compactMap { $0.column("name")?.string }.joined(separator: ", "))")
        analysisResults.totalSchemas = schemas.count

        // Step 2: For each schema, enumerate all tables and views
        for schemaRow in schemas {
            guard let schemaName = schemaRow.column("name")?.string else { continue }
            print("\n📋 STEP 2: Processing schema '\(schemaName)'...")

            // Test tables in this schema
            let tables = try await withReliableConnection(client: client) { conn in
                try await conn.query("SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('\(schemaName)') ORDER BY name")
            }
            print("   📄 Tables in \(schemaName): \(tables.count)")
            analysisResults.totalTables += tables.count

            // Test views in this schema
            let views = try await withReliableConnection(client: client) { conn in
                try await conn.query("SELECT name FROM sys.views WHERE schema_id = SCHEMA_ID('\(schemaName)') ORDER BY name")
            }
            print("   👁️ Views in \(schemaName): \(views.count)")
            analysisResults.totalViews += views.count

            // Step 3: Test each table's metadata operations
            for tableRow in tables {
                guard let tableName = tableRow.column("name")?.string else { continue }
                print("\n   🔍 Testing table: \(schemaName).\(tableName)")
                let tableResults = try await testTableMetadata(schema: schemaName, table: tableName)
                analysisResults.tableResults.append(tableResults)
            }

            // Step 4: Test each view's metadata operations
            for viewRow in views {
                guard let viewName = viewRow.column("name")?.string else { continue }
                print("\n   🔍 Testing view: \(schemaName).\(viewName)")
                let viewResults = try await testViewMetadata(schema: schemaName, view: viewName)
                analysisResults.viewResults.append(viewResults)
            }
        }

        // Step 5: Print comprehensive analysis report
        printAnalysisReport(results: analysisResults)

        // At minimum, we should be able to enumerate schemas without issues
        XCTAssertGreaterThan(analysisResults.totalSchemas, 0, "Should find at least one schema")
    }

    /// Test all metadata operations for a specific table
    private func testTableMetadata(schema: String, table: String) async throws -> ObjectMetadataResults {
        var results = ObjectMetadataResults(objectName: "\(schema).\(table)", objectType: .table)

        let operations: [(String, () async throws -> Void)] = [
            ("listColumns", {
                _ = try await withReliableConnection(client: self.client) { conn in
                    try await conn.listColumns(database: self.testDatabase, schema: schema, table: table).get()
                }
            }),
            ("listPrimaryKeys", {
                _ = try await withReliableConnection(client: self.client) { conn in
                    try await conn.listPrimaryKeys(database: self.testDatabase, schema: schema, table: table).get()
                }
            }),
            ("listForeignKeys", {
                _ = try await withReliableConnection(client: self.client) { conn in
                    try await conn.listForeignKeys(database: self.testDatabase, schema: schema, table: table).get()
                }
            }),
            ("listIndexes", {
                _ = try await withReliableConnection(client: self.client) { conn in
                    try await conn.listIndexes(database: self.testDatabase, schema: schema, table: table).get()
                }
            }),
            ("listTriggers", {
                _ = try await withReliableConnection(client: self.client) { conn in
                    try await conn.listTriggers(database: self.testDatabase, schema: schema, table: table).get()
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
        var results = ObjectMetadataResults(objectName: "\(schema).\(view)", objectType: .view)

        let operations: [(String, () async throws -> Void)] = [
            ("listColumns", {
                _ = try await withReliableConnection(client: self.client) { conn in
                    try await conn.listColumns(database: self.testDatabase, schema: schema, table: view).get()
                }
            }),
            ("listIndexes", {
                _ = try await withReliableConnection(client: self.client) { conn in
                    try await conn.listIndexes(database: self.testDatabase, schema: schema, table: view).get()
                }
            }),
            ("listTriggers", {
                _ = try await withReliableConnection(client: self.client) { conn in
                    try await conn.listTriggers(database: self.testDatabase, schema: schema, table: view).get()
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
        print("\n❌ FAILURES DETECTED:")
        var hasFailures = false

        for result in results.tableResults {
            if !result.failedOperations.isEmpty {
                hasFailures = true
                print("   Table: \(result.objectName)")
                for (operation, error) in result.failedOperations {
                    print("     \(operation): \(error)")
                }
            }
        }

        for result in results.viewResults {
            if !result.failedOperations.isEmpty {
                hasFailures = true
                print("   View: \(result.objectName)")
                for (operation, error) in result.failedOperations {
                    print("     \(operation): \(error)")
                }
            }
        }

        if !hasFailures {
            print("   🎉 No failures detected! All metadata operations are working correctly.")
        }

        print("\n" + "=" * 80)
        print("🏁 ANALYSIS COMPLETE")
        print("=" * 80)
    }
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