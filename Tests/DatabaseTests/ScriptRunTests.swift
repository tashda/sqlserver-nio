import Foundation
import SQLServerKit
import SQLServerKitTesting
import Testing

@Suite struct ScriptRunTests {
    @Test func runsBatchesInTheGivenDatabaseAndRestoresTheConnection() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }

        try await withTemporaryDatabase(client: client, prefix: "script") { database in
            let summary = try await client.scripts.run("""
            CREATE TABLE dbo.counter (n int NOT NULL)
            GO
            INSERT dbo.counter VALUES (1)
            GO 3
            -- 'GO' in a string is data: 'x
            -- GO
            INSERT dbo.counter VALUES (LEN('a
            GO
            b'))
            GO
            """, database: database)
            #expect(summary.batchesRun == 3)
            let rows = try await client.metadata.tableProperties(database: database, schema: "dbo", table: "counter").rowCount
            #expect(rows == 4)
            let current = try await client.withConnection { $0.currentDatabase }
            #expect(current.caseInsensitiveCompare(database) != .orderedSame)
        }
    }

    @Test func reportsTheFailingBatchAndLine() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }
        do {
            try await client.scripts.run("SELECT 1\nGO\nSELECT * FROM dbo.does_not_exist_\(UUID().uuidString.prefix(6))\nGO\n")
            Issue.record("expected a failure")
        } catch let error as SQLServerScriptError {
            #expect(error.batchIndex == 1)
            #expect(error.line == 3)
        }
    }
}
