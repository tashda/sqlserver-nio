import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

@available(macOS 12.0, *)
final class ExecutionPlanTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    var adminClient: SQLServerAdministrationClient!
    var tablesToDrop: [String] = []

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
        self.adminClient = SQLServerAdministrationClient(client: client)
        do { _ = try await withTimeout(10) { try await self.client.query("SELECT 1") } } catch { throw error }
    }

    override func tearDown() async throws {
        for table in tablesToDrop {
            try? await adminClient.dropTable(name: table)
        }
        tablesToDrop.removeAll()
        try? await self.client?.shutdownGracefully()
    }

    // MARK: - Estimated Plans

    func testEstimatedXML() async throws {
        let xml = try await client.executionPlan.estimatedXML("SELECT 1 AS val")
        XCTAssertTrue(xml.contains("<ShowPlanXML"), "Expected ShowPlanXML root element")
        XCTAssertTrue(xml.contains("SELECT"), "Expected statement text in plan")
    }

    func testEstimatedPlan() async throws {
        let plan = try await client.executionPlan.estimated("SELECT 1 AS val")
        XCTAssertFalse(plan.statements.isEmpty, "Expected at least one statement")
        XCTAssertNotNil(plan.buildVersion)

        let stmt = try XCTUnwrap(plan.statements.first)
        // SQL Server returns "SELECT WITHOUT QUERY" for SELECT without a FROM clause
        XCTAssertTrue(
            stmt.statementType == "SELECT" || stmt.statementType == "SELECT WITHOUT QUERY",
            "Expected SELECT or SELECT WITHOUT QUERY, got: \(stmt.statementType)"
        )
    }

    func testEstimatedPlanWithJoin() async throws {
        let tableName1 = "ep_join_a_\(UUID().uuidString.prefix(8))"
        let tableName2 = "ep_join_b_\(UUID().uuidString.prefix(8))"

        let cols1 = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
        ]
        let cols2 = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "a_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
        ]

        try await adminClient.createTable(name: tableName1, columns: cols1)
        tablesToDrop.append(tableName1)
        try await adminClient.createTable(name: tableName2, columns: cols2)
        tablesToDrop.append(tableName2)

        let sql = "SELECT a.name, b.value FROM \(tableName1) a JOIN \(tableName2) b ON a.id = b.a_id"
        let plan = try await client.executionPlan.estimated(sql)

        let stmt = try XCTUnwrap(plan.statements.first)
        XCTAssertEqual(stmt.statementType, "SELECT")

        let root = try XCTUnwrap(stmt.queryPlan?.rootOperator)
        // A join plan should have child operators
        XCTAssertFalse(root.children.isEmpty, "Join plan should have child operators")
    }

    // MARK: - Actual Plans

    func testActualPlan() async throws {
        let tableName = "ep_actual_\(UUID().uuidString.prefix(8))"
        let cols = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
        ]
        try await adminClient.createTable(name: tableName, columns: cols)
        tablesToDrop.append(tableName)

        _ = try await client.execute("INSERT INTO \(tableName) VALUES (1, N'Alice'), (2, N'Bob')")

        let (result, plan) = try await client.executionPlan.actual("SELECT * FROM \(tableName)")

        // Plan should have actual metrics
        let root = try XCTUnwrap(plan.statements.first?.queryPlan?.rootOperator)
        XCTAssertNotNil(root.actualRows, "Actual plan should have actualRows")
        XCTAssertEqual(root.actualRows, 2)

        // Data rows should still be returned
        XCTAssertEqual(result.rows.count, 2)
    }

    func testActualPlanPreservesResults() async throws {
        let tableName = "ep_preserve_\(UUID().uuidString.prefix(8))"
        let cols = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "val", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
        ]
        try await adminClient.createTable(name: tableName, columns: cols)
        tablesToDrop.append(tableName)

        _ = try await client.execute("INSERT INTO \(tableName) VALUES (1, N'one'), (2, N'two'), (3, N'three')")

        let (result, _) = try await client.executionPlan.actual("SELECT * FROM \(tableName) ORDER BY id")

        XCTAssertEqual(result.rows.count, 3)
        XCTAssertEqual(result.rows[0].column("val")?.string, "one")
        XCTAssertEqual(result.rows[1].column("val")?.string, "two")
        XCTAssertEqual(result.rows[2].column("val")?.string, "three")
    }

    // MARK: - Safety

    func testEstimatedPlanDoesNotExecute() async throws {
        let tableName = "ep_noexec_\(UUID().uuidString.prefix(8))"
        let cols = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
        ]
        try await adminClient.createTable(name: tableName, columns: cols)
        tablesToDrop.append(tableName)

        // Get estimated plan for an INSERT — should NOT actually execute
        let plan = try await client.executionPlan.estimated("INSERT INTO \(tableName) VALUES (1)")
        XCTAssertFalse(plan.statements.isEmpty)

        // Verify table is still empty
        let rows = try await client.query("SELECT COUNT(*) AS cnt FROM \(tableName)")
        let count = rows.first?.column("cnt")?.int ?? -1
        XCTAssertEqual(count, 0, "Estimated plan should not execute the INSERT")
    }

    func testCleanupOnError() async throws {
        // Execute an invalid query — SET SHOWPLAN_XML OFF should still run
        do {
            _ = try await client.executionPlan.estimated("SELECT * FROM nonexistent_table_that_does_not_exist_12345")
            XCTFail("Expected error for invalid table")
        } catch {
            // Expected
        }

        // Verify connection is still usable (SET OFF was called)
        let rows = try await client.query("SELECT 1 AS val")
        XCTAssertEqual(rows.first?.column("val")?.int, 1)
    }
}
