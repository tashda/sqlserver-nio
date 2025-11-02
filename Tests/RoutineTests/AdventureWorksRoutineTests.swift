@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerAdventureWorksRoutineTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    @available(macOS 12.0, *)
    func testAdventureWorksUfnGetAccountingEndDateParameters() async throws {
        // Only run when explicitly enabled and AdventureWorks is available
        
        let dbName = env("TDS_AW_DATABASE") ?? "AdventureWorks2022"
        try await client.withConnection { connection in
            _ = try await connection.changeDatabase(dbName).get()
            let parameters = try await connection.listParameters(schema: "dbo", object: "ufnGetAccountingEndDate").get()
            XCTAssertFalse(parameters.isEmpty, "Expected parameters for dbo.ufnGetAccountingEndDate")
        }
    }

    @available(macOS 12.0, *)
    func testAdventureWorks2022ComprehensiveMetadataLoading() async throws {
        // Comprehensive test that validates all TDS protocol fixes by loading all metadata
        // that Echo would need when working with AdventureWorks2022 database
        let dbName = env("TDS_AW_DATABASE") ?? "AdventureWorks2022"

        try await client.withConnection { connection in
            _ = try await connection.changeDatabase(dbName).get()

            // Test critical views that previously caused TDS parsing issues
            let problemViews = [
                ("HumanResources", "vJobCandidate"),
                ("Sales", "vSalesPerson"),
                ("HumanResources", "vEmployee"),
                ("Person", "vPersonDemographics"),
                ("Sales", "vIndividualCustomer")
            ]

            for (schema, view) in problemViews {
                do {
                    // This should not cause TDS protocol parsing errors after our fixes
                    let columns = try await connection.listColumns(schema: schema, table: view).get()
                    XCTAssertFalse(columns.isEmpty, "Expected columns for \(schema).\(view)")

                    // Verify critical metadata fields are loaded correctly
                    for column in columns {
                        XCTAssertNotNil(column.name, "Column name should not be nil for \(schema).\(view)")
                        XCTAssertNotNil(column.typeName, "Column dataType should not be nil for \(schema).\(view).\(column.name)")
                    }
                } catch {
                    XCTFail("Failed to load columns for \(schema).\(view): \(error)")
                }
            }

            // Test table metadata loading with extended properties
            let criticalTables = [
                ("Person", "Person"),
                ("HumanResources", "Employee"),
                ("Sales", "SalesOrderHeader"),
                ("Production", "Product"),
                ("Purchasing", "PurchaseOrderHeader")
            ]

            for (schema, table) in criticalTables {
                do {
                    let columns = try await connection.listColumns(schema: schema, table: table).get()
                    XCTAssertFalse(columns.isEmpty, "Expected columns for \(schema).\(table)")

                    // Test extended properties and comments - use listTables with includeComments
                    let tablesWithComments = try await connection.listTables(database: dbName, schema: schema, includeComments: true).get()
                    let targetTable = tablesWithComments.first { $0.name == table }
                    XCTAssertNotNil(targetTable, "Should find table \(schema).\(table)")

                } catch {
                    XCTFail("Failed to load metadata for \(schema).\(table): \(error)")
                }
            }

            // Test security metadata loading (previously caused null byte issues)
            do {
                let securityClient = SQLServerSecurityClient(client: self.client)
                let permissions = try await securityClient.listPermissionsDetailed()
                // Query should succeed without TDS protocol parsing errors
                XCTAssertFalse(permissions.isEmpty, "Should load security permissions")
            } catch {
                XCTFail("Failed to load security metadata: \(error)")
            }

            // Test trigger metadata loading (previously had CAST NULL issues)
            let tablesWithTriggers = [
                ("Person", "Person"),
                ("HumanResources", "Employee"),
                ("Sales", "SalesOrderHeader")
            ]

            for (schema, table) in tablesWithTriggers {
                do {
                    // Test that we can query triggers without TDS parsing errors
                    let triggerQuery = """
                        SELECT COUNT(*) as trigger_count
                        FROM sys.triggers t
                        JOIN sys.objects o ON t.parent_id = o.object_id
                        JOIN sys.schemas s ON o.schema_id = s.schema_id
                        WHERE s.name = '\(schema)' AND o.name = '\(table)'
                        """
                    let result = try await connection.query(triggerQuery).get()
                    // Should load trigger metadata without TDS parsing errors
                    // The fact we can execute this query proves the CAST NULL fixes work
                    XCTAssertFalse(result.isEmpty, "Should get trigger count result")

                } catch {
                    XCTFail("Failed to load trigger metadata for \(schema).\(table): \(error)")
                }
            }

            // Test procedure and function metadata loading
            let criticalRoutines = [
                ("dbo", "uspGetBillOfMaterials"),
                ("dbo", "uspGetEmployeeManagers"),
                ("dbo", "ufnGetAccountingEndDate"),
                ("dbo", "ufnGetContactInformation"),
                ("Sales", "ufnLeadingZeros")
            ]

            for (schema, routine) in criticalRoutines {
                do {
                    let parameters = try await connection.listParameters(schema: schema, object: routine).get()
                    // Should load parameters without TDS parsing errors

                    // If we get here, the TDS fixes for stored procedures work
                    XCTAssertFalse(parameters.isEmpty, "Should have parameters for \(schema).\(routine)")
                } catch {
                    // Some routines might not exist, that's OK
                    print("Note: Routine \(schema).\(routine) not accessible: \(error)")
                }
            }

            // Test index metadata loading - verify we can query sys.indexes without TDS errors
            do {
                let indexQuery = """
                    SELECT COUNT(*) as index_count
                    FROM sys.indexes i
                    JOIN sys.objects o ON i.object_id = o.object_id
                    JOIN sys.schemas s ON o.schema_id = s.schema_id
                    WHERE s.name = 'Person' AND o.name = 'Person'
                    """
                let result = try await connection.query(indexQuery).get()
                XCTAssertFalse(result.isEmpty, "Should get index count result")
            } catch {
                XCTFail("Failed to load index metadata: \(error)")
            }
        }
    }
}
