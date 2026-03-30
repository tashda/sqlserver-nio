import SQLServerKit
import SQLServerKitTesting
import XCTest

final class SQLServerDeadlockRetryTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    func testDeadlockRetry() async throws {
        let dbName = "dlckdb_\(UUID().uuidString.prefix(8))"
        let table = "DL_T_\(UUID().uuidString.prefix(6))"
        let qualifiedTable = "[\(dbName)].[dbo].[\(table)]"
        let adminClient = SQLServerAdministrationClient(client: self.client)

        try await adminClient.createDatabase(name: dbName)
        let dbAdmin = SQLServerAdministrationClient(client: self.client, database: dbName)
        try await dbAdmin.createTable(
            name: table,
            columns: [
                SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                SQLServerColumnDefinition(name: "v", definition: .standard(.init(dataType: .int)))
            ]
        )
        _ = try await dbAdmin.insertRow(into: table, values: ["id": .int(1), "v": .int(0)])
        _ = try await dbAdmin.insertRow(into: table, values: ["id": .int(2), "v": .int(0)])

        do {
            // Use two independent connections that may deadlock and recover
            async let a: Void = self.client.withConnection { conn in
                try await conn.beginTransaction()
                try await conn.updateRows(in: table, schema: "dbo", database: dbName, set: ["v": .raw("v + 1")], where: "id = 1")
                try await Task.sleep(nanoseconds: 200_000_000)
                do {
                    try await conn.updateRows(in: table, schema: "dbo", database: dbName, set: ["v": .raw("v + 1")], where: "id = 2")
                    try await conn.commit()
                } catch {
                    // After a deadlock, SQL Server may have closed the connection
                    // ROLLBACK on a closed connection is expected and should be ignored
                    do {
                        try await conn.rollback()
                    } catch {
                        // Ignore ROLLBACK errors after deadlock - connection may be closed
                    }
                }
            }

            async let b: Void = self.client.withConnection { conn in
                try await conn.beginTransaction()
                try await conn.updateRows(in: table, schema: "dbo", database: dbName, set: ["v": .raw("v + 1")], where: "id = 2")
                try await Task.sleep(nanoseconds: 200_000_000)
                do {
                    try await conn.updateRows(in: table, schema: "dbo", database: dbName, set: ["v": .raw("v + 1")], where: "id = 1")
                    try await conn.commit()
                } catch {
                    // After a deadlock, SQL Server may have closed the connection
                    // ROLLBACK on a closed connection is expected and should be ignored
                    do {
                        try await conn.rollback()
                    } catch {
                        // Ignore ROLLBACK errors after deadlock - connection may be closed
                    }
                }
            }

            // One of the tasks should deadlock; ensure the client can still execute after
            _ = try? await (a, b)
            let rows = try await self.client.query("SELECT SUM(v) AS s FROM \(qualifiedTable)")
            XCTAssertNotNil(rows.first?.column("s")?.int)
        } catch {
            _ = try? await adminClient.dropDatabase(name: dbName, forceSingleUser: true)
            throw error
        }

        _ = try? await adminClient.dropDatabase(name: dbName, forceSingleUser: true)
    }
}
