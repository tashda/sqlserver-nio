@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTableDefinitionCoverageTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    func testComprehensiveTableScripting() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "cov") { db in
            // Build a rich schema to exercise most features
            let parent = "cov_parent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
            let child = "cov_child_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

            // Create parent
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(parent)] (
                    [Id] INT NOT NULL IDENTITY(100, 5) CONSTRAINT [PK_\(parent)] PRIMARY KEY CLUSTERED,
                    [Code] NVARCHAR(50) NOT NULL
                );
            """)

            // Create child and its index
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(child)] (
                    [Id] INT NOT NULL IDENTITY(1, 1),
                    [RefId] INT NULL,
                    [Name] NVARCHAR(50) COLLATE Latin1_General_CI_AS NOT NULL CONSTRAINT [DF_\(child)_Name] DEFAULT N'X',
                    [SparseCol] NVARCHAR(100) SPARSE NULL,
                    [GuidCol] UNIQUEIDENTIFIER NOT NULL ROWGUIDCOL CONSTRAINT [DF_\(child)_Guid] DEFAULT NEWID(),
                    [Amount] DECIMAL(18, 4) NULL,
                    [CreatedAt] DATETIME2(3) NOT NULL CONSTRAINT [DF_\(child)_Created] DEFAULT SYSUTCDATETIME(),
                    [FullName] AS (([Name] + N' ' + CAST([Id] AS NVARCHAR(20)))) PERSISTED,
                    CONSTRAINT [PK_\(child)] PRIMARY KEY NONCLUSTERED ([Id] ASC),
                    CONSTRAINT [UQ_\(child)_NameCode] UNIQUE NONCLUSTERED ([Name] ASC, [RefId] DESC),
                    CONSTRAINT [CK_\(child)_Amount] CHECK ([Amount] >= 0),
                    CONSTRAINT [FK_\(child)_Ref] FOREIGN KEY ([RefId]) REFERENCES [dbo].[\(parent)] ([Id]) ON DELETE CASCADE
                );
                CREATE NONCLUSTERED INDEX [IX_\(child)_Ref_Inc] ON [dbo].[\(child)] ([RefId] ASC) INCLUDE ([Name]) WHERE [RefId] IS NOT NULL;
            """)

        

            guard let def = try await withRetry({
                try await withTimeout(15, {
                    try await withDbConnection(client: self.client, database: db) { conn in
                        try await conn.fetchObjectDefinition(schema: "dbo", name: child, kind: .table).get()
                    }
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

        // Cleanup
        try await DDLGuard.shared.withLock {
            // No explicit cleanup needed; database gets dropped by withTemporaryDatabase
        }
        }
    }
}
