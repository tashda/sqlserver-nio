@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO

final class SQLServerAppMetadataLoadingTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!

    private var operationTimeout: TimeInterval {
        if let value = env("TDS_TEST_OPERATION_TIMEOUT_SECONDS"), let seconds = TimeInterval(value) {
            return max(60, seconds)
        }
        // loadDatabaseStructure is a heavy multi-query operation on AdventureWorks2022.
        // Allow up to 3 minutes for the full sequence to complete.
        return 180
    }

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        var config = makeSQLServerClientConfiguration()
        // Per-query timeout: 60s is enough for any single catalog query, while
        // still sending an attention token to prevent infinite stalls if the
        // TDS channel misbehaves or SQL Server becomes unresponsive.
        config.connection.metadataConfiguration.commandTimeout = 60
        config.connection.metadataConfiguration.extractParameterDefaults = false
        config.connection.metadataConfiguration.preferStoredProcedureColumns = false
        config.connection.metadataConfiguration.includeRoutineDefinitions = false
        config.connection.metadataConfiguration.includeTriggerDefinitions = true

        client = try await SQLServerClient.connect(
            configuration: config,
            eventLoopGroupProvider: .shared(group)
        ).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testAppMetadataLoadForAdventureWorks2022() async throws {
        guard env("TDS_AW_DATABASE") != nil else {
            throw XCTSkip("Skipping: TDS_AW_DATABASE not set")
        }
        let dbName = env("TDS_AW_DATABASE")!

        try await withTimeout(operationTimeout) {
            try await self.client.withConnection { connection in
                _ = try await connection.changeDatabase(dbName).get()

                let structure = try await connection.loadDatabaseStructure(database: dbName).get()
                let schemaNames = Set(structure.schemas.map(\.name))

                if schemaNames.contains("Production") {
                    let schema = structure.schemas.first { $0.name.caseInsensitiveCompare("Production") == .orderedSame }
                    let product = schema?.tables.first { $0.table.name.caseInsensitiveCompare("Product") == .orderedSame }
                    XCTAssertNotNil(product, "Expected Production.Product table")
                    XCTAssertFalse(product?.columns.isEmpty ?? true, "Expected columns for Production.Product")
                }

                if schemaNames.contains("Purchasing") {
                    let schema = structure.schemas.first { $0.name.caseInsensitiveCompare("Purchasing") == .orderedSame }
                    let header = schema?.tables.first { $0.table.name.caseInsensitiveCompare("PurchaseOrderHeader") == .orderedSame }
                    XCTAssertNotNil(header, "Expected Purchasing.PurchaseOrderHeader table")
                    XCTAssertFalse(header?.columns.isEmpty ?? true, "Expected columns for Purchasing.PurchaseOrderHeader")
                }

                if schemaNames.contains("dbo") {
                    let schema = structure.schemas.first { $0.name.caseInsensitiveCompare("dbo") == .orderedSame }
                    XCTAssertFalse(schema?.procedures.isEmpty ?? true, "Expected stored procedures in dbo schema")
                    XCTAssertFalse(schema?.functions.isEmpty ?? true, "Expected functions in dbo schema")
                }

                if schemaNames.contains("HumanResources") {
                    let schema = try await connection.loadSchemaStructure(database: dbName, schema: "HumanResources").get()
                    let shift = schema.tables.first { $0.table.name.caseInsensitiveCompare("Shift") == .orderedSame }
                    XCTAssertNotNil(shift, "Expected HumanResources.Shift table")
                    XCTAssertFalse(shift?.columns.isEmpty ?? true, "Expected columns for HumanResources.Shift")

                    let view = schema.views.first { $0.table.name.caseInsensitiveCompare("vEmployee") == .orderedSame }
                    XCTAssertNotNil(view, "Expected HumanResources.vEmployee view")
                }
            }
        }
    }
}
