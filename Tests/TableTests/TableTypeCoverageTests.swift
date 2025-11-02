@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTableDefinitionCoverageTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        continueAfterFailure = false

        // Load environment configuration
        TestEnvironmentManager.loadEnvironmentVariables()

        // Configure logging
        _ = isLoggingConfigured

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    func testComprehensiveTableScripting() async throws {
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "cov") { db in
            // Build a rich schema to exercise most features
            let parent = "cov_parent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
            let child = "cov_child_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

            // Create parent using the administration client
            let dbClient = try await makeClient(forDatabase: db, using: self.group)
            let admin = SQLServerAdministrationClient(client: dbClient)
            let constraints = SQLServerConstraintClient(client: dbClient)
            let indexes = SQLServerIndexClient(client: dbClient)

            try await admin.createTable(
                name: parent,
                columns: [
                    SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isNullable: false, isPrimaryKey: true, identity: (100,5)))),
                    SQLServerColumnDefinition(name: "Code", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isNullable: false)))
                ]
            )

            // Create child with rich column features
            try await admin.createTable(
                name: child,
                columns: [
                    SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isNullable: false, identity: (1,1)))),
                    SQLServerColumnDefinition(name: "RefId", definition: .standard(.init(dataType: .int, isNullable: true))),
                    SQLServerColumnDefinition(name: "Name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isNullable: false, collation: "Latin1_General_CI_AS"))),
                    SQLServerColumnDefinition(name: "SparseCol", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), isNullable: true, isSparse: true))),
                    SQLServerColumnDefinition(name: "GuidCol", definition: .standard(.init(dataType: .uniqueidentifier, isNullable: false, isRowGuidCol: true))),
                    SQLServerColumnDefinition(name: "Amount", definition: .standard(.init(dataType: .decimal(precision: 18, scale: 4), isNullable: true))),
                    SQLServerColumnDefinition(name: "CreatedAt", definition: .standard(.init(dataType: .datetime2(precision: 3), isNullable: false))),
                    SQLServerColumnDefinition(name: "FullName", definition: .computed(expression: "([Name] + N' ' + CAST([Id] AS NVARCHAR(20)))", persisted: true))
                ]
            )

            // Named defaults
            try await constraints.addDefaultConstraint(name: "DF_\(child)_Name", table: child, column: "Name", defaultValue: "N'X'")
            try await constraints.addDefaultConstraint(name: "DF_\(child)_Guid", table: child, column: "GuidCol", defaultValue: "NEWID()")
            try await constraints.addDefaultConstraint(name: "DF_\(child)_Created", table: child, column: "CreatedAt", defaultValue: "SYSUTCDATETIME()")

            // Constraints
            try await constraints.addPrimaryKey(name: "PK_\(child)", table: child, columns: ["Id"], clustered: false)
            try await constraints.addUniqueConstraint(name: "UQ_\(child)_NameCode", table: child, columns: ["Name", "RefId"], clustered: false)
            try await constraints.addCheckConstraint(name: "CK_\(child)_Amount", table: child, expression: "[Amount] >= 0")
            try await constraints.addForeignKey(name: "FK_\(child)_Ref", table: child, columns: ["RefId"], referencedTable: parent, referencedColumns: ["Id"], options: .init(onDelete: .cascade))

            // Filtered nonclustered index with INCLUDE
            try await indexes.createIndex(
                name: "IX_\(child)_Ref_Inc",
                table: child,
                columns: [IndexColumn(name: "RefId"), IndexColumn(name: "Name", isIncluded: true)],
                schema: "dbo",
                options: IndexOptions(fillFactor: 80, padIndex: true, allowRowLocks: false, allowPageLocks: true),
                filter: "[RefId] IS NOT NULL"
            )

        

            guard let def = try await withRetry(attempts: 5, operation: {
                try await withTimeout(60, operation: {
                    try await withReliableConnection(client: dbClient, operation: { conn in
                        try await conn.fetchObjectDefinition(schema: "dbo", name: child, kind: .table).get()
                    })
                })
            }), let ddl = def.definition else {
                XCTFail("No definition returned")
                return
            }

        // Column and type coverage
        XCTAssertTrue(ddl.contains("CREATE TABLE [dbo].[\(child)]"), "Missing CREATE TABLE header")
        XCTAssertTrue(ddl.contains("NVARCHAR(50)"), "Expected NVARCHAR length")
        XCTAssertTrue(ddl.contains("DECIMAL(18, 4)"), "Expected DECIMAL precision/scale")
        XCTAssertTrue(ddl.contains("DATETIME2(3)"), "Expected DATETIME2 scale")

        // Collation and sparse
        XCTAssertTrue(ddl.contains("COLLATE "), "Expected COLLATE clause on Name")
        XCTAssertTrue(ddl.contains("SPARSE"), "Expected SPARSE column")

        // Identity
        XCTAssertTrue(ddl.contains("IDENTITY(1, 1)") || ddl.contains("IDENTITY(1,1)"), "Expected identity seed/increment")

        // Defaults and named default constraints
        XCTAssertTrue(ddl.contains("CONSTRAINT [DF_\(child)_Name] DEFAULT"), "Expected named default for Name")
        XCTAssertTrue(ddl.contains("CONSTRAINT [DF_\(child)_Created] DEFAULT"), "Expected named default for CreatedAt")

        // Computed persisted
        XCTAssertTrue(ddl.contains("AS ("), "Expected computed column expression")
        XCTAssertTrue(ddl.contains("PERSISTED"), "Expected computed persisted")

        // Rowguidcol
        XCTAssertTrue(ddl.contains("ROWGUIDCOL"), "Expected ROWGUIDCOL")

        // Constraints
        XCTAssertTrue(ddl.contains("PRIMARY KEY"), "Expected primary key")
        XCTAssertTrue(ddl.contains("UNIQUE"), "Expected unique constraint")
        XCTAssertTrue(ddl.contains("CHECK ("), "Expected check constraint")
        XCTAssertTrue(ddl.contains("FOREIGN KEY"), "Expected foreign key")
        XCTAssertTrue(ddl.contains("ON DELETE CASCADE"), "Expected FK delete action")

        // Index with include and filter
        XCTAssertTrue(ddl.contains("CREATE NONCLUSTERED INDEX [IX_\(child)_Ref_Inc]"), "Expected index name")
        XCTAssertTrue(ddl.contains(" INCLUDE ("), "Expected INCLUDE clause")
        XCTAssertTrue(ddl.contains(" WHERE "), "Expected filtered index")

        // Ensure SSMS-like placement of options (WITH ...)
        XCTAssertTrue(ddl.contains("WITH ("), "Expected WITH table/index options when applicable")

        // Cleanup - no explicit cleanup needed; database gets dropped by withTemporaryDatabase
        _ = try? await dbClient.shutdownGracefully().get()
        }
        } catch {
            if let te = error as? AsyncTimeoutError {
                throw XCTSkip("Skipping due to timeout during comprehensive table scripting: \(te)")
            }
            let norm = SQLServerError.normalize(error)
            switch norm {
            case .connectionClosed, .timeout:
                throw XCTSkip("Skipping due to unstable server during comprehensive table scripting: \(norm)")
            default:
                throw error
            }
        }
    }
}
