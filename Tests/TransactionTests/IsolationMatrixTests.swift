import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerTransactionIsolationMatrixTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1") } } catch { throw error }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    @available(macOS 12.0, *)
    func testSerializableRangeLockBlocksInsert() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "txmx") { db in
            try await withDbClient(for: db) { dbClient in
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                // Create table using SQLServerAdministrationClient
                let tableName = "isolation_test_\(UUID().uuidString.prefix(8))"
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "category", definition: .standard(.init(dataType: .nvarchar(length: .length(5)))))
                ]
                try await adminClient.createTable(name: tableName, columns: columns)

                try await dbClient.withConnection { connection in
                    try await connection.insertRow(into: tableName, values: ["id": .int(1), "category": .nString("A")])
                    try await connection.insertRow(into: tableName, values: ["id": .int(2), "category": .nString("B")])
                }

                // Pre-warm pool with 2 idle connections so the timing test is not skewed by
                // new-connection establishment time (~200-300ms over the network).
                async let w1 = dbClient.withConnection { conn in try await conn.query("SELECT 1") }
                async let w2 = dbClient.withConnection { conn in try await conn.query("SELECT 1") }
                _ = try await (w1, w2)

                let holder = Task {
                    try await dbClient.withConnection { conn in
                        try await conn.setIsolationLevel(.serializable)
                        try await conn.beginTransaction()
                        _ = try await conn.query("SELECT COUNT(*) FROM [dbo].[\(tableName)] WHERE category = N'A'").get()
                        try await Task.sleep(nanoseconds: 600_000_000)
                        try await conn.commit()
                    }
                }

                try await Task.sleep(nanoseconds: 150_000_000)
                let elapsed = try await dbClient.withConnection { conn in
                    let start = DispatchTime.now()
                    try await conn.insertRow(into: tableName, values: ["id": .int(3), "category": .nString("A")])
                    return DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
                }
                _ = try? await withTimeout(5) { try await holder.value }
                XCTAssertGreaterThan(elapsed, 300_000_000 as UInt64, "INSERT should have been blocked until SERIALIZABLE txn finished")
            }
        }
    }

    @available(macOS 12.0, *)
    func testReadCommittedSelectBlocksOnWriter() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "txmx") { db in
            try await withDbClient(for: db) { dbClient in
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                // Create table using SQLServerAdministrationClient
                let tableName = "isolation_test_\(UUID().uuidString.prefix(8))"
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(20)))))
                ]
                try await adminClient.createTable(name: tableName, columns: columns)

                try await dbClient.withConnection { connection in
                    try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Original")])
                }

                // Pre-warm pool with 2 idle connections so the timing test is not skewed by
                // new-connection establishment time (~200-300ms over the network).
                async let w1 = dbClient.withConnection { conn in try await conn.query("SELECT 1") }
                async let w2 = dbClient.withConnection { conn in try await conn.query("SELECT 1") }
                _ = try await (w1, w2)

                let writer = Task {
                    try await dbClient.withConnection { conn in
                        try await conn.beginTransaction()
                        try await conn.updateRows(in: tableName, set: ["value": .nString("Updated")], where: "id = 1")
                        try await Task.sleep(nanoseconds: 600_000_000)
                        try await conn.rollback()
                    }
                }

                try await Task.sleep(nanoseconds: 100_000_000)
                // Under READ COMMITTED (default), the reader is blocked until writer commits/rolls back
                let elapsed = try await dbClient.withConnection { conn in
                    let start = DispatchTime.now()
                    _ = try await conn.query("SELECT value FROM [dbo].[\(tableName)] WHERE id = 1").get()
                    return DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
                }
                _ = try? await withTimeout(5) { try await writer.value }
                XCTAssertGreaterThan(elapsed, 300_000_000 as UInt64, "READ COMMITTED reader should block on writer")
            }
        }
    }
}
