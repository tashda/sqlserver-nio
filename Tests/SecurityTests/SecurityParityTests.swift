import XCTest
@testable import SQLServerKit

final class SQLServerSecurityParityTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private let TIMEOUT: TimeInterval = 60

    override func setUp() async throws {
        try await super.setUp()

        // Load environment configuration
        TestEnvironmentManager.loadEnvironmentVariables()

        // Configure logging
        _ = isLoggingConfigured

    
        // Create connection
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        try await super.tearDown()
    }

    func testSecurableAwareGrantAndList() async throws {
        let dbSec = SQLServerSecurityClient(client: client)
        // Create a temp object and grant SELECT to public
        let table = makeSchemaQualifiedName(prefix: "secobj")
        _ = try await client.execute("CREATE TABLE \(table.bracketed) (id INT, name NVARCHAR(50));").get()

        let oid = ObjectIdentifier(database: nil, schema: "dbo", name: table.nameOnly, kind: .table)
        _ = try await dbSec.grant(permission: .select, on: .object(oid), to: "public").get()
        // Detailed permissions should include an OBJECT_OR_COLUMN entry
        if #available(macOS 12.0, *) {
            let details = try await dbSec.listPermissionsDetailed(principal: "public")
            XCTAssertTrue(details.contains(where: { $0.objectName?.caseInsensitiveCompare(table.nameOnly) == .orderedSame }))
        }
        _ = try await dbSec.revoke(permission: .select, on: .object(oid), from: "public").get()

        // Cleanup
        _ = try? await client.execute("DROP TABLE \(table.bracketed);").get()
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
        let schema = "nio_ops"
        _ = try? await dbSec.dropSchema(name: schema).get()
        _ = try await dbSec.createSchema(name: schema, authorization: "dbo").get()
        let schemas = try await dbSec.listSchemas().get()
        XCTAssertTrue(schemas.contains(where: { $0.name.caseInsensitiveCompare(schema) == .orderedSame }))
        // Transfer a simple object
        let table = makeSchemaQualifiedName(prefix: "xfer")
        _ = try await client.execute("CREATE TABLE \(table.bracketed) (id INT);").get()

        // Cleanup using defer alternative
        do {
            _ = try await dbSec.transferObjectToSchema(objectSchema: "dbo", objectName: table.nameOnly, newSchema: schema).get()
            _ = try await dbSec.alterAuthorizationOnSchema(schema: schema, principal: "dbo").get()
            _ = try await dbSec.dropSchema(name: schema).get()
        } catch {
            // Best effort cleanup
            _ = try? await client.execute("DROP TABLE [\(schema)].[\(table.nameOnly)];").get()
            throw error
        }
    }
}
