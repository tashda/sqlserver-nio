import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class SQLServerSecurityParityTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!
    private let TIMEOUT: TimeInterval = 60

    override func setUp() async throws {
        try await super.setUp()

        // Load environment configuration
        TestEnvironmentManager.loadEnvironmentVariables()

        // Configure logging
        _ = isLoggingConfigured


        // Create connection
        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        try await super.tearDown()
    }

    func testSecurableAwareGrantAndList() async throws {
        let dbSec = SQLServerSecurityClient(client: client)
        let adminClient = SQLServerAdministrationClient(client: client)
        // Create a temp object and grant SELECT to public
        let table = makeSchemaQualifiedName(prefix: "secobj")
        try await adminClient.createTable(
            name: table.nameOnly,
            columns: [
                .init(name: "id", definition: .standard(.init(dataType: .int, isNullable: true))),
                .init(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isNullable: true)))
            ]
        )

        let oid = ObjectIdentifier(database: nil, schema: "dbo", name: table.nameOnly, kind: .table)
        _ = try await dbSec.grant(permission: .select, on: .object(oid), to: "public").get()
        // Detailed permissions should include an OBJECT_OR_COLUMN entry
        if #available(macOS 12.0, *) {
            let details = try await dbSec.listPermissionsDetailed(principal: "public")
            XCTAssertTrue(details.contains(where: { $0.objectName?.caseInsensitiveCompare(table.nameOnly) == .orderedSame }))
        }
        _ = try await dbSec.revoke(permission: .select, on: .object(oid), from: "public").get()

        // Cleanup
        try? await adminClient.dropTable(name: table.nameOnly)
    }

    func testApplicationRoleLifecycle() async throws {
        let dbSec = SQLServerSecurityClient(client: client)
        _ = try await dbSec.createApplicationRole(name: "AppRoleNIO", password: "Pass!123", defaultSchema: "dbo").get()
        let roles = try await dbSec.listApplicationRoles().get()
        XCTAssertTrue(roles.contains(where: { $0.name.caseInsensitiveCompare("AppRoleNIO") == .orderedSame }))
        _ = try? await dbSec.alterApplicationRole(name: "AppRoleNIO", newName: "AppRoleNIO2").get()
        _ = try await dbSec.dropApplicationRole(name: "AppRoleNIO2").get()
    }

    func testSchemaHelpers() async throws {
        let dbSec = SQLServerSecurityClient(client: client)
        let adminClient = SQLServerAdministrationClient(client: client)
        let schema = "nio_ops"
        _ = try? await dbSec.dropSchema(name: schema).get()
        _ = try await dbSec.createSchema(name: schema, authorization: "dbo").get()
        let schemas = try await dbSec.listSchemas()
        XCTAssertTrue(schemas.contains(where: { $0.name.caseInsensitiveCompare(schema) == .orderedSame }))
        // Transfer a simple object
        let table = makeSchemaQualifiedName(prefix: "xfer")
        try await adminClient.createTable(
            name: table.nameOnly,
            columns: [.init(name: "id", definition: .standard(.init(dataType: .int, isNullable: true)))]
        )

        // Cleanup using defer alternative
        do {
            _ = try await dbSec.transferObjectToSchema(objectSchema: "dbo", objectName: table.nameOnly, newSchema: schema).get()
            _ = try await dbSec.alterAuthorizationOnSchema(schema: schema, principal: "dbo").get()
            // Must drop the table first before dropping the schema
            try await adminClient.dropTable(name: table.nameOnly, schema: schema)
            _ = try await dbSec.dropSchema(name: schema).get()
        } catch {
            // Best effort cleanup - drop table first, then schema
            try? await adminClient.dropTable(name: table.nameOnly, schema: schema)
            _ = try? await dbSec.dropSchema(name: schema).get()
            throw error
        }
    }
}
