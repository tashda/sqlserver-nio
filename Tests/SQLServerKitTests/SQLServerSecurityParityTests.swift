import XCTest
@testable import SQLServerKit

final class SQLServerSecurityParityTests: XCTestCase {
    private var client: SQLServerClient!
    private var group: EventLoopGroup!
    private let TIMEOUT: TimeInterval = 60

    override func setUpWithError() throws {
        try super.setUpWithError()
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_SECURITY_PARITY_TESTS", description: "security parity integration tests")
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).wait()
    }

    override func tearDownWithError() throws {
        try? client?.shutdownGracefully().wait()
        try? group?.syncShutdownGracefully()
        client = nil
        group = nil
        try super.tearDownWithError()
    }

    func testSecurableAwareGrantAndList() throws {
        let dbSec = SQLServerSecurityClient(client: client)
        // Create a temp object and grant SELECT to public
        let table = makeSchemaQualifiedName(prefix: "secobj")
        _ = try waitForResult(client.execute("CREATE TABLE \(table.bracketed) (id INT, name NVARCHAR(50));"), timeout: TIMEOUT, description: "create table")
        defer { _ = try? waitForResult(client.execute("DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop table") }

        let oid = ObjectIdentifier(database: nil, schema: "dbo", name: table.nameOnly, kind: .table)
        _ = try waitForResult(dbSec.grant(permission: .select, on: .object(oid), to: "public"), timeout: TIMEOUT, description: "grant select")
        // Detailed permissions should include an OBJECT_OR_COLUMN entry
        if #available(macOS 12.0, *) {
            let details = try waitForAsync(timeout: TIMEOUT, description: "list perms detailed") {
                try await dbSec.listPermissionsDetailed(principal: "public")
            }
            XCTAssertTrue(details.contains(where: { $0.objectName?.caseInsensitiveCompare(table.nameOnly) == .orderedSame }))
        }
        _ = try waitForResult(dbSec.revoke(permission: .select, on: .object(oid), from: "public"), timeout: TIMEOUT, description: "revoke select")
    }

    func testApplicationRoleLifecycle() throws {
        let dbSec = SQLServerSecurityClient(client: client)
        _ = try waitForResult(dbSec.createApplicationRole(name: "AppRoleNIO", password: "Pass!123", defaultSchema: "dbo"), timeout: TIMEOUT, description: "create app role")
        let roles = try waitForResult(dbSec.listApplicationRoles(), timeout: TIMEOUT, description: "list app roles")
        XCTAssertTrue(roles.contains(where: { $0.name.caseInsensitiveCompare("AppRoleNIO") == .orderedSame }))
        _ = try? waitForResult(dbSec.alterApplicationRole(name: "AppRoleNIO", newName: "AppRoleNIO2"), timeout: TIMEOUT, description: "alter app role")
        _ = try waitForResult(dbSec.dropApplicationRole(name: "AppRoleNIO2"), timeout: TIMEOUT, description: "drop app role")
    }

    func testSchemaHelpers() throws {
        let dbSec = SQLServerSecurityClient(client: client)
        let schema = "nio_ops"
        _ = try? waitForResult(dbSec.dropSchema(name: schema), timeout: TIMEOUT, description: "cleanup schema")
        _ = try waitForResult(dbSec.createSchema(name: schema, authorization: "dbo"), timeout: TIMEOUT, description: "create schema")
        let schemas = try waitForResult(dbSec.listSchemas(), timeout: TIMEOUT, description: "list schemas")
        XCTAssertTrue(schemas.contains(where: { $0.name.caseInsensitiveCompare(schema) == .orderedSame }))
        // Transfer a simple object
        let table = makeSchemaQualifiedName(prefix: "xfer")
        _ = try waitForResult(client.execute("CREATE TABLE \(table.bracketed) (id INT);"), timeout: TIMEOUT, description: "create table for transfer")
        defer { _ = try? waitForResult(client.execute("DROP TABLE [\(schema)].[\(table.nameOnly)];"), timeout: TIMEOUT, description: "drop xfer table") }
        _ = try waitForResult(dbSec.transferObjectToSchema(objectSchema: "dbo", objectName: table.nameOnly, newSchema: schema), timeout: TIMEOUT, description: "transfer object")
        _ = try waitForResult(dbSec.alterAuthorizationOnSchema(schema: schema, principal: "dbo"), timeout: TIMEOUT, description: "alter auth")
        _ = try waitForResult(dbSec.dropSchema(name: schema), timeout: TIMEOUT, description: "drop schema")
    }
}
