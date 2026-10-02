import Foundation
import SQLServerKit
import SQLServerKitTesting
import Testing

/// A table with LOB columns has several allocation units per partition. tableProperties once
/// counted every row once per unit.
@Suite(.testServer) struct TablePropertiesRowCountTests {
    @Test func rowCountIgnoresLobAllocationUnits() async throws {
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }

        try await withTemporaryDatabase(client: client, prefix: "rowcount") { database in
            let table = "lob_" + UUID().uuidString.prefix(8)
            try await withDbConnection(client: client, database: database) { connection in
                try await connection.createTable(name: String(table), columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "body", definition: .standard(.init(dataType: .nvarchar(length: .max), isNullable: true))),
                    SQLServerColumnDefinition(name: "blob", definition: .standard(.init(dataType: .varbinary(length: .max), isNullable: true))),
                ])
            }
            let rows: [[SQLServerLiteralValue]] = (1...7).map { [.int($0), .nString(String(repeating: "x", count: 9_000)), .bytes([1, 2, 3])] }
            try await client.admin.scoped(to: database).insertRows(into: String(table), columns: ["id", "body", "blob"], values: rows)

            let properties = try await client.metadata.tableProperties(database: database, schema: "dbo", table: String(table))
            #expect(properties.rowCount == 7)
        }
    }
}
