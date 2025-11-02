@testable import SQLServerKit
import Foundation
import XCTest
import NIO
import Logging
import Foundation

final class SQLServerTypeClientTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    private var skipDueToEnv = false

    override func setUp() async throws {
        try await super.setUp()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
    }

    @available(macOS 12.0, *)
    func testCreateUserDefinedTableType() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "udtt") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let typeClient = SQLServerTypeClient(client: dbClient)

                let tableType = UserDefinedTableTypeDefinition(
                    name: "TestTableType",
                    schema: "dbo",
                    columns: [
                        UserDefinedTableTypeColumn(name: "Id", dataType: .int, isNullable: false, isIdentity: true),
                        UserDefinedTableTypeColumn(name: "Name", dataType: .nvarchar(length: .length(100))),
                        UserDefinedTableTypeColumn(name: "Email", dataType: .nvarchar(length: .length(255))),
                        UserDefinedTableTypeColumn(name: "CreatedDate", dataType: .datetime2(precision: 3), isNullable: false)
                    ]
                )

                // Create the table type
                try await typeClient.createUserDefinedTableType(tableType)

                // Verify it was created by listing types
                let types = try await typeClient.listUserDefinedTableTypes().get()
                XCTAssertTrue(types.contains { $0.name == "TestTableType" && $0.schema == "dbo" })

                let createdType = types.first { $0.name == "TestTableType" && $0.schema == "dbo" }!
                XCTAssertEqual(createdType.columns.count, 4)
                XCTAssertEqual(createdType.columns[0].name, "Id")
                if case .int = createdType.columns[0].dataType {
                    // Expected data type
                } else {
                    XCTFail("Expected int data type")
                }
                XCTAssertFalse(createdType.columns[0].isNullable)
                XCTAssertTrue(createdType.columns[0].isIdentity)
            }
        }
    }

    @available(macOS 12.0, *)
    func testDropUserDefinedTableType() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "udtt_drop") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let typeClient = SQLServerTypeClient(client: dbClient)

                let tableType = UserDefinedTableTypeDefinition(
                    name: "TestTypeToDrop",
                    schema: "dbo",
                    columns: [
                        UserDefinedTableTypeColumn(name: "Id", dataType: .int, isNullable: false)
                    ]
                )

                // Create the table type
                try await typeClient.createUserDefinedTableType(tableType)

                // Verify it exists
                let typesBefore = try await typeClient.listUserDefinedTableTypes().get()
                XCTAssertTrue(typesBefore.contains { $0.name == "TestTypeToDrop" })

                // Drop the table type
                try await typeClient.dropUserDefinedTableType(name: "TestTypeToDrop", schema: "dbo")

                // Verify it was dropped
                let typesAfter = try await typeClient.listUserDefinedTableTypes().get()
                XCTAssertFalse(typesAfter.contains { $0.name == "TestTypeToDrop" })
            }
        }
    }

    @available(macOS 12.0, *)
    func testListUserDefinedTableTypesWithSchema() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "udtt_list") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let typeClient = SQLServerTypeClient(client: dbClient)
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                // Create custom schema first
                try await dbClient.execute("CREATE SCHEMA [custom]").get()

                // Create types in different schemas
                let dboType = UserDefinedTableTypeDefinition(
                    name: "DboType",
                    schema: "dbo",
                    columns: [UserDefinedTableTypeColumn(name: "Id", dataType: .int, isNullable: false)]
                )

                let customType = UserDefinedTableTypeDefinition(
                    name: "CustomType",
                    schema: "custom",
                    columns: [UserDefinedTableTypeColumn(name: "Id", dataType: .int, isNullable: false)]
                )

                try await typeClient.createUserDefinedTableType(dboType)
                try await typeClient.createUserDefinedTableType(customType)

                // List all types
                let allTypes = try await typeClient.listUserDefinedTableTypes().get()
                XCTAssertTrue(allTypes.count >= 2)

                // List types in specific schema
                let dboTypes = try await typeClient.listUserDefinedTableTypes(schema: "dbo").get()
                XCTAssertTrue(dboTypes.contains { $0.name == "DboType" })
                XCTAssertFalse(dboTypes.contains { $0.name == "CustomType" })

                let customTypes = try await typeClient.listUserDefinedTableTypes(schema: "custom").get()
                XCTAssertTrue(customTypes.contains { $0.name == "CustomType" })
                XCTAssertFalse(customTypes.contains { $0.name == "DboType" })
            }
        }
    }

    @available(macOS 12.0, *)
    func testUserDefinedTableTypeWithAllDataTypes() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "udtt_alldata") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let typeClient = SQLServerTypeClient(client: dbClient)

                let tableType = UserDefinedTableTypeDefinition(
                    name: "AllDataTypesType",
                    schema: "dbo",
                    columns: [
                        UserDefinedTableTypeColumn(name: "BigIntCol", dataType: .bigint, isNullable: false),
                        UserDefinedTableTypeColumn(name: "BitCol", dataType: .bit),
                        UserDefinedTableTypeColumn(name: "DecimalCol", dataType: .decimal(precision: 18, scale: 4)),
                        UserDefinedTableTypeColumn(name: "FloatCol", dataType: .float(mantissa: 53)),
                        UserDefinedTableTypeColumn(name: "RealCol", dataType: .real),
                        UserDefinedTableTypeColumn(name: "DateCol", dataType: .date),
                        UserDefinedTableTypeColumn(name: "TimeCol", dataType: .time(precision: 3)),
                        UserDefinedTableTypeColumn(name: "DateTime2Col", dataType: .datetime2(precision: 3)),
                        UserDefinedTableTypeColumn(name: "UniqueIdentifierCol", dataType: .uniqueidentifier),
                        UserDefinedTableTypeColumn(name: "VarCharCol", dataType: .varchar(length: .length(50))),
                        UserDefinedTableTypeColumn(name: "NVarCharCol", dataType: .nvarchar(length: .length(100))),
                        UserDefinedTableTypeColumn(name: "VarBinaryCol", dataType: .varbinary(length: .length(200))),
                        UserDefinedTableTypeColumn(name: "XmlCol", dataType: .xml),
                        UserDefinedTableTypeColumn(name: "MoneyCol", dataType: .money),
                        UserDefinedTableTypeColumn(name: "SmallMoneyCol", dataType: .smallmoney)
                    ]
                )

                // Create the table type
                try await typeClient.createUserDefinedTableType(tableType)

                // Verify it was created and has all columns
                let types = try await typeClient.listUserDefinedTableTypes(schema: "dbo").get()
                let createdType = types.first { $0.name == "AllDataTypesType" }!
                XCTAssertNotNil(createdType)
                XCTAssertEqual(createdType.columns.count, 15)

                // Verify specific columns
                let decimalCol = createdType.columns.first { $0.name == "DecimalCol" }!
                if case .decimal(let precision, let scale) = decimalCol.dataType {
                    XCTAssertEqual(precision, 18)
                    XCTAssertEqual(scale, 4)
                } else {
                    XCTFail("Expected decimal data type")
                }

                let varcharCol = createdType.columns.first { $0.name == "VarCharCol" }!
                if case .varchar(let length) = varcharCol.dataType, case .length(let len) = length {
                    XCTAssertEqual(len, 50)
                } else {
                    XCTFail("Expected varchar with length")
                }
            }
        }
    }
}