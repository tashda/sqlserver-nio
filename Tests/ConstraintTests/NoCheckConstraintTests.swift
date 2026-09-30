import Foundation
import SQLServerKit
import SQLServerKitTesting
import Testing

/// Constraints created without checking existing rows. WITH NOCHECK used to be appended after the
/// constraint definition, which SQL Server rejects.
@Suite struct NoCheckConstraintTests {
    @Test func constraintsSkipExistingRowsAndAreNotTrusted() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }

        try await withTemporaryDatabase(client: client, prefix: "nocheck") { database in
            try await withDbConnection(client: client, database: database) { connection in
                try await connection.createTable(name: "parents", columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                ])
                try await connection.createTable(name: "children", columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int))),
                    SQLServerColumnDefinition(name: "amount", definition: .standard(.init(dataType: .int))),
                ])
            }
            let admin = client.admin.scoped(to: database)
            // Rows that break both constraints below.
            try await admin.insertRows(into: "children", columns: ["id", "parent_id", "amount"], values: [[.int(1), .int(99), .int(-5)]])

            // The constraint client works in the connection's database.
            var configuration = makeSQLServerClientConfiguration()
            configuration.login.database = database
            let scoped = try await SQLServerClient.connect(configuration: configuration, numberOfThreads: 1)
            defer { Task { try? await scoped.shutdownGracefully() } }
            try await scoped.constraints.addForeignKey(
                name: "fk_children_parent", table: "children", columns: ["parent_id"],
                referencedTable: "parents", referencedColumns: ["id"],
                options: ForeignKeyOptions(isNotTrusted: true, notForReplication: true)
            )
            try await scoped.constraints.addCheckConstraint(name: "ck_amount", table: "children", expression: "amount >= 0", checkExisting: false)

            let keys = try await scoped.metadata.listForeignKeys(database: database, schema: "dbo", table: "children")
            let checks = try await scoped.constraints.listCheckConstraints(database: database, table: "children")
            #expect(keys.map(\.name) == ["fk_children_parent"])
            #expect(checks.map(\.name) == ["ck_amount"])
        }
    }
}
