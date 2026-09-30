import Foundation
import SQLServerKit
import SQLServerKitTesting
import Testing

/// Columns of CLR types (hierarchyid, geometry, geography) have system type 240, which has no row of
/// its own in sys.types. listColumns once dropped them.
@Suite struct ClrTypeColumnsTests {
    @Test func listColumnsIncludesClrTypeColumns() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }

        try await withTemporaryDatabase(client: client, prefix: "clrcols") { database in
            let table = "clr_" + UUID().uuidString.prefix(8)
            try await withDbConnection(client: client, database: database) { connection in
                try await connection.createTable(name: String(table), columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "node", definition: .standard(.init(dataType: .hierarchyid, isNullable: true))),
                    SQLServerColumnDefinition(name: "shape", definition: .standard(.init(dataType: .geometry, isNullable: true))),
                    SQLServerColumnDefinition(name: "place", definition: .standard(.init(dataType: .geography, isNullable: true))),
                    SQLServerColumnDefinition(name: "version", definition: .standard(.init(dataType: .rowversion))),
                ])
            }
            let columns = try await withDbConnection(client: client, database: database) { connection in
                try await connection.listColumns(database: database, schema: "dbo", table: String(table))
            }
            #expect(columns.map(\.name) == ["id", "node", "shape", "place", "version"])
            #expect(columns.map { $0.typeName.lowercased() } == ["int", "hierarchyid", "geometry", "geography", "timestamp"])
        }
    }
}
