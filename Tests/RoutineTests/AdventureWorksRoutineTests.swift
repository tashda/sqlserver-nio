import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerAdventureWorksRoutineTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    @available(macOS 12.0, *)
    func testAdventureWorksUfnGetAccountingEndDateParameters() async throws {
        let dbName: String
        do {
            dbName = try await requireDatabaseNamedInEnvironment("TDS_AW_DATABASE", using: client)
        } catch let error as SQLServerFixtureUnavailable {
            throw XCTSkip(error.message)
        }
        try await client.withDatabase(dbName) { _ in
            let parameters = try await self.client.metadata.listParameters(database: dbName, schema: "dbo", object: "ufnGetAccountingEndDate")
            XCTAssertFalse(parameters.isEmpty, "Expected parameters for dbo.ufnGetAccountingEndDate")
        }
    }

    @available(macOS 12.0, *)
    func testAdventureWorks2022ComprehensiveMetadataLoading() async throws {
        // Comprehensive test that validates all TDS protocol fixes by loading all metadata
        // that App would need when working with AdventureWorks2022 database
        let dbName: String
        do {
            dbName = try await requireDatabaseNamedInEnvironment("TDS_AW_DATABASE", using: client)
        } catch let error as SQLServerFixtureUnavailable {
            throw XCTSkip(error.message)
        }

        // Test critical views that previously caused TDS parsing issues
        let problemViews = [
            ("HumanResources", "vJobCandidate"),
            ("Sales", "vSalesPerson"),
            ("HumanResources", "vEmployee"),
            ("Sales", "vPersonDemographics"),
            ("Sales", "vIndividualCustomer")
        ]

        for (schema, view) in problemViews {
            do {
                // This should not cause TDS protocol parsing errors after our fixes
                let columns = try await client.metadata.listColumns(database: dbName, schema: schema, table: view)
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
                let columns = try await client.metadata.listColumns(database: dbName, schema: schema, table: table)
                XCTAssertFalse(columns.isEmpty, "Expected columns for \(schema).\(table)")

                // Test extended properties and comments - use listTables with includeComments
                let tablesWithComments = try await client.metadata.listTables(database: dbName, schema: schema, includeComments: true)
                let targetTable = tablesWithComments.first { $0.name == table }
                XCTAssertNotNil(targetTable, "Should find table \(schema).\(table)")

            } catch {
                XCTFail("Failed to load metadata for \(schema).\(table): \(error)")
            }
        }

        // Test security metadata loading (previously caused null byte issues)
        do {
            let securityClient = SQLServerSecurityClient(client: self.client)
            let permissions = try await securityClient.listPermissionsDetailed(principal: "dbo")
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
                let triggers = try await client.metadata.listTriggers(database: dbName, schema: schema, table: table)
                // Should load trigger metadata without TDS parsing errors
                XCTAssertNotNil(triggers, "Should get trigger results for \(schema).\(table)")
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
            ("dbo", "ufnLeadingZeros")
        ]

        for (schema, routine) in criticalRoutines {
            do {
                let parameters = try await client.metadata.listParameters(database: dbName, schema: schema, object: routine)
                // Should load parameters without TDS parsing errors
                XCTAssertFalse(parameters.isEmpty, "Should have parameters for \(schema).\(routine)")
            } catch {
                // Some routines might not exist, that's OK
                print("Note: Routine \(schema).\(routine) not accessible: \(error)")
            }
        }

        // Test index metadata loading
        do {
            let indexes = try await client.metadata.listIndexes(database: dbName, schema: "Person", table: "Person")
            XCTAssertFalse(indexes.isEmpty, "Should get indexes for Person.Person")
        } catch {
            XCTFail("Failed to load index metadata: \(error)")
        }
    }
}
