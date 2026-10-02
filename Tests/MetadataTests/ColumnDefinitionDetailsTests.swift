import Foundation
import SQLServerKit
import SQLServerKitTesting
import Testing

/// Column details a table designer needs (identity seed and increment, default and computed
/// definitions) come back whether or not comments are asked for.
@Suite(.testServer) struct ColumnDefinitionDetailsTests {
    @Test(arguments: [false, true])
    func identityDefaultAndComputedDetails(includeComments: Bool) async throws {
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }

        try await withTemporaryDatabase(client: client, prefix: "coldetails") { database in
            try await withDbConnection(client: client, database: database) { connection in
                try await connection.createTable(name: "orders", columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, identity: (100, 5)))),
                    SQLServerColumnDefinition(name: "status", definition: .standard(.init(
                        dataType: .nvarchar(length: .length(20)), defaultValue: "N'new'", comment: "Order status"))),
                    SQLServerColumnDefinition(name: "doubled", definition: .computed(expression: "[id] * 2")),
                ])
            }
            let columns = try await client.metadata.listColumns(database: database, schema: "dbo", table: "orders", includeComments: includeComments)
            let id = try #require(columns.first { $0.name == "id" })
            #expect(id.isIdentity)
            #expect(id.identitySeed == 100)
            #expect(id.identityIncrement == 5)
            let status = try #require(columns.first { $0.name == "status" })
            #expect(status.hasDefaultValue)
            #expect(status.defaultDefinition?.contains("new") == true, "\(status.defaultDefinition ?? "nil")")
            let doubled = try #require(columns.first { $0.name == "doubled" })
            #expect(doubled.isComputed)
            #expect(doubled.computedDefinition?.contains("[id]") == true, "\(doubled.computedDefinition ?? "nil")")
            if includeComments {
                #expect(status.comment == "Order status")
            }
        }
    }
}
