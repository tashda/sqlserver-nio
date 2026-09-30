import Foundation
import SQLServerKit
import SQLServerKitTesting
import Testing

/// tableProperties inner-joined the table's index to sys.filegroups; a partitioned table's index is
/// on a partition scheme, so every property came back empty.
@Suite struct PartitionedTablePropertiesTests {
    @Test func partitionedTableReportsItsPartitions() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }

        try await withTemporaryDatabase(client: client, prefix: "partprops") { database in
            try await withDbConnection(client: client, database: database) { connection in
                try await connection.createPartitionFunction(name: "pf", dataType: .int, values: ["10", "20"])
                try await connection.createPartitionScheme(name: "ps", functionName: "pf")
                try await connection.createPartitionedTable(name: "t", columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                ], partitionScheme: "ps", partitionColumn: "id")
            }
            let properties = try await client.metadata.tableProperties(database: database, schema: "dbo", table: "t")
            #expect(properties.isPartitioned == true)
            #expect(properties.partitionCount == 3)
            #expect(properties.partitionScheme == "ps")
            #expect(properties.createDate != nil)
        }
    }
}
