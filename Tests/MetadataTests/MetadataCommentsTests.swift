import XCTest
@testable import SQLServerKit
import Foundation

final class SQLServerMetadataCommentsTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    private var adminClient: SQLServerAdministrationClient!

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

        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDown() async throws {
        // Clean up connections first
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()

        self.adminClient = nil
        try await super.tearDown()
    }

    func testTableAndColumnCommentsLoaded() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "cmts") { db in
            // Create table with three columns, add comments
            let cols: [SQLServerColumnDefinition] = [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, comment: "Primary key"))),
                .init(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), comment: "Human-readable name"))),
                .init(name: "flag", definition: .standard(.init(dataType: .bit)))
            ]
            let tableName = "t_\(UUID().uuidString.prefix(8))"
            // Use admin client helper which persists comments via extended properties
            let admin = SQLServerAdministrationClient(client: self.client)
            try await withDbConnection(client: self.client, database: db) { conn in
                _ = try await conn.execute("IF OBJECT_ID(N'dbo.\(tableName)', 'U') IS NOT NULL DROP TABLE [dbo].[\(tableName)]").get()
            }
            try await withDbConnection(client: self.client, database: db) { _ in
                try await admin.createTable(name: tableName, columns: cols)
                try await admin.addTableComment(tableName: tableName, comment: "Table comment for \(tableName)")
            }

            // Verify listTables without comments does not hydrate
            let noCommentTables = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listTables(schema: "dbo").get()
            }
            XCTAssertTrue(noCommentTables.contains { $0.name == tableName && $0.comment == nil })

            // Verify listTables with comments includes table comment
            let tables = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listTables(schema: "dbo", includeComments: true).get()
            }
            guard let t = tables.first(where: { $0.name == tableName }) else {
                XCTFail("Expected table returned by listTables")
                return
            }
            XCTAssertEqual(t.comment, "Table comment for \(tableName)")

            // Verify listColumns returns column comments when requested
            let colsWithComments = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listColumns(schema: "dbo", table: tableName, includeComments: true).get()
            }
            XCTAssertEqual(colsWithComments.count, 3)
            XCTAssertEqual(colsWithComments.first(where: { $0.name == "id" })?.comment, "Primary key")
            XCTAssertEqual(colsWithComments.first(where: { $0.name == "name" })?.comment, "Human-readable name")
            XCTAssertNil(colsWithComments.first(where: { $0.name == "flag" })?.comment)

            // And without includeComments they should be nil
            let colsNoComments = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listColumns(schema: "dbo", table: tableName).get()
            }
            XCTAssertTrue(colsNoComments.allSatisfy { $0.comment == nil })
        }
    }

    func testViewColumnCommentsLoaded() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "cmtv") { db in
            let table = "src_\(UUID().uuidString.prefix(8))"
            let view = "v_\(UUID().uuidString.prefix(8))"
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[\(table)](id INT NOT NULL, name NVARCHAR(40) NOT NULL)")
            _ = try await executeInDb(client: self.client, database: db, "CREATE VIEW [dbo].[\(view)] AS SELECT id, name FROM [dbo].[\(table)]")

            // Add MS_Description to view and one of its columns
            let addViewComment = """
            EXEC sp_addextendedproperty N'MS_Description', N'View for \(view)', N'SCHEMA', N'dbo', N'VIEW', N'\(view)';
            EXEC sp_addextendedproperty N'MS_Description', N'Identifier', N'SCHEMA', N'dbo', N'VIEW', N'\(view)', N'COLUMN', N'id';
            """
            _ = try await executeInDb(client: self.client, database: db, addViewComment)

            let cols = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listColumns(schema: "dbo", table: view, includeComments: true).get()
            }
            XCTAssertEqual(cols.first(where: { $0.name == "id" })?.comment, "Identifier")
            // name column not annotated
            XCTAssertNil(cols.first(where: { $0.name == "name" })?.comment)
        }
    }

    func testRoutineAndTriggerCommentsLoaded() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "cmtr") { db in
            // Create objects
            let table = "t_\(UUID().uuidString.prefix(8))"
            let proc = "p_\(UUID().uuidString.prefix(8))"
            let funcName = "f_\(UUID().uuidString.prefix(8))"
            let trig = "tr_\(UUID().uuidString.prefix(8))"

            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[\(table)](id INT NOT NULL PRIMARY KEY)")
            _ = try await executeInDb(client: self.client, database: db, "CREATE PROCEDURE [dbo].[\(proc)] AS BEGIN SET NOCOUNT ON; SELECT 1; END")
            _ = try await executeInDb(client: self.client, database: db, "CREATE FUNCTION [dbo].[\(funcName)]() RETURNS INT AS BEGIN RETURN 1 END")
            _ = try await executeInDb(client: self.client, database: db, "CREATE TRIGGER [dbo].[\(trig)] ON [dbo].[\(table)] AFTER INSERT AS BEGIN SET NOCOUNT ON; END")

            // Add MS_Description to each
            let addComments = """
            EXEC sp_addextendedproperty N'MS_Description', N'Procedure comment', N'SCHEMA', N'dbo', N'PROCEDURE', N'\(proc)';
            EXEC sp_addextendedproperty N'MS_Description', N'Function comment', N'SCHEMA', N'dbo', N'FUNCTION', N'\(funcName)';
            EXEC sp_addextendedproperty N'MS_Description', N'Trigger comment', N'SCHEMA', N'dbo', N'TABLE', N'\(table)', N'TRIGGER', N'\(trig)';
            """
            _ = try await executeInDb(client: self.client, database: db, addComments)

            // Use single connection for all operations to prevent thread switching
            let results = try await withDbConnection(client: self.client, database: db) { conn in
                let procs = try await conn.listProcedures(schema: "dbo", includeComments: true).get()
                let funcs = try await conn.listFunctions(schema: "dbo", includeComments: true).get()
                let trigs = try await conn.listTriggers(schema: "dbo", table: table, includeComments: true).get()
                return (procs: procs, funcs: funcs, trigs: trigs)
            }

            XCTAssertTrue(results.procs.contains { $0.name == proc && $0.comment == "Procedure comment" })
            XCTAssertTrue(results.funcs.contains { $0.name == funcName && $0.comment == "Function comment" })
            XCTAssertTrue(results.trigs.contains { $0.name == trig && $0.comment == "Trigger comment" })
        }
    }
}
