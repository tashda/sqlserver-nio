import XCTest
import Logging
import NIO
import NIOTestUtils
@testable import SQLServerKit
import SQLServerTDS

final class SQLServerIntegrationTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    private static var executionSummary: [String] = []
    private let TIMEOUT: TimeInterval = 60

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
    }

    override func tearDown() async throws {
        // Clean up connections first
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()

        if let run = self.testRun as? XCTestCaseRun {
            let components = self.name.split(separator: " ")
            let methodName = components.last.map { $0.replacingOccurrences(of: "]", with: "") } ?? String(self.name)
            let status: String
            if run.hasBeenSkipped {
                status = "SKIPPED"
            } else if run.totalFailureCount > 0 {
                status = "FAILED (\(run.totalFailureCount) failure)"
            } else {
                status = "PASSED"
            }
            Self.executionSummary.append("\(status): \(methodName)")
        }
        try await super.tearDown()
    }

    override class func tearDown() {
        if !executionSummary.isEmpty {
            print("=== SQLServerIntegrationTests Summary ===")
            executionSummary.forEach { print($0) }
            print("=== End Summary ===")
            executionSummary.removeAll()
        }
        super.tearDown()
    }
    
    func testTempTableCrudLifecycle() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let tableName = makeTempTableName(prefix: "crud")
                let createResult = try await conn.execute("CREATE TABLE \(tableName) (id INT PRIMARY KEY, name NVARCHAR(100));").get()
                XCTAssertEqual(createResult.rowCount, 0)

                let insertResult = try await conn.execute("INSERT INTO \(tableName) (id, name) VALUES (1, N'original');").get()
                XCTAssertEqual(insertResult.rowCount, 1)

                let updateResult = try await conn.execute("UPDATE \(tableName) SET name = N'updated' WHERE id = 1;").get()
                XCTAssertEqual(updateResult.rowCount, 1)

                let rows = try await conn.query("SELECT name FROM \(tableName) WHERE id = 1;").get()
                XCTAssertEqual(rows.count, 1)
                XCTAssertEqual(rows[0].column("name")?.string, "updated")

                let deleteResult = try await conn.execute("DELETE FROM \(tableName) WHERE id = 1;").get()
                XCTAssertEqual(deleteResult.rowCount, 1)
                let remaining = try await conn.query("SELECT COUNT(*) AS row_count FROM \(tableName);").get()
                XCTAssertEqual(remaining.first?.column("row_count")?.int, 0)

                // Manual cleanup instead of defer
                _ = try? await conn.query("IF OBJECT_ID('tempdb..\(tableName)', 'U') IS NOT NULL DROP TABLE \(tableName);").get()
            }
        }
    }
    
    func testStoredProcedureWithOutputAndResults() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "itsp") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let names = makeSchemaQualifiedName(prefix: "usp_tds")
                _ = try await conn.execute("IF OBJECT_ID(N'\(names.bare)', 'P') IS NOT NULL DROP PROCEDURE \(names.bracketed);").get()
                let createProc = """
                CREATE PROCEDURE \(names.bracketed)
                    @Input INT,
                    @Output INT OUTPUT
                AS
                BEGIN
                    SET NOCOUNT ON;
                    SET @Output = @Input + 5;
                    SELECT TOP (@Input) DatabaseName = name FROM sys.databases ORDER BY name;
                END;
                """
                _ = try await conn.execute(createProc).get()

                let parameters = try await conn.listParameters(database: db, schema: "dbo", object: names.nameOnly).get()
                XCTAssertEqual(parameters.count, 2, "Expected stored procedure to surface two parameters")
                guard let inputParam = parameters.first(where: { $0.name.caseInsensitiveCompare("@Input") == .orderedSame }) else { XCTFail("Missing @Input"); return }
                XCTAssertEqual(inputParam.typeName.lowercased(), "int")
                XCTAssertFalse(inputParam.isOutput)
                XCTAssertFalse(inputParam.hasDefaultValue)
                guard let outputParam = parameters.first(where: { $0.name.caseInsensitiveCompare("@Output") == .orderedSame }) else { XCTFail("Missing @Output"); return }
                XCTAssertTrue(outputParam.isOutput)
                XCTAssertFalse(outputParam.isReturnValue)

                let procedures = try await conn.listProcedures(database: db, schema: "dbo").get()
                XCTAssertTrue(procedures.contains(where: { $0.name.caseInsensitiveCompare(names.nameOnly) == .orderedSame }))

                let execSql = """
                DECLARE @out INT;
                EXEC \(names.bracketed) @Input = 3, @Output = @out OUTPUT;
                SELECT OutputValue = @out;
                """
                let rows = try await conn.query(execSql).get()
                let databaseRows = rows.filter { $0.column("DatabaseName")?.string != nil }
                XCTAssertGreaterThanOrEqual(databaseRows.count, 1)
                guard let outputRow = rows.last, let outputValue = outputRow.column("OutputValue")?.int else { XCTFail("Missing output"); return }
                XCTAssertEqual(outputValue, 8)

                _ = try await conn.execute("DROP PROCEDURE \(names.bracketed);").get()
            }
        }
    }
    
    func testViewDefinitionRoundTrip() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let names = makeSchemaQualifiedName(prefix: "vw_tds")

                _ = try await conn.query("IF OBJECT_ID(N'\(names.bare)', 'V') IS NOT NULL DROP VIEW \(names.bracketed);").get()
                _ = try await conn.query("SET QUOTED_IDENTIFIER ON;").get()
                let createView = """
                CREATE VIEW \(names.bracketed)
                AS
                SELECT TOP 5 name AS database_name FROM sys.databases ORDER BY name;
                """
                _ = try await conn.query(createView).get()

                let rows = try await conn.query("SELECT COUNT(*) AS row_count FROM \(names.bracketed);").get()
                guard let count = rows.first?.column("row_count")?.int else {
                    XCTFail("Expected to count rows from view")
                    return
                }
                XCTAssertGreaterThan(count, 0)

                let viewColumns = try await conn.listColumns(schema: "dbo", table: names.nameOnly).get()
                XCTAssertFalse(viewColumns.isEmpty, "Expected metadata columns for view")
                guard let firstColumn = viewColumns.first else {
                    XCTFail("Expected at least one column for the view")
                    return
                }
                XCTAssertEqual(firstColumn.name.caseInsensitiveCompare("database_name"), .orderedSame)

                let definitionRows = try await conn.query("SELECT OBJECT_DEFINITION(OBJECT_ID(N'\(names.bare)')) AS definition;").get()
                let definition = definitionRows.first?.column("definition")?.string ?? ""
                XCTAssertTrue(definition.uppercased().contains("SELECT TOP 5"), "Expected view definition to contain SELECT statement")

                // Manual cleanup
                _ = try? await conn.query("DROP VIEW \(names.bracketed);").get()
            }
        }
    }
    
    func testInformationSchemaColumnsFetchesRows() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let sql = """
                SELECT TOP (1)
                    c.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                WHERE c.TABLE_SCHEMA = 'dbo'
                ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
                """
                let rows = try await conn.query(sql).get()
                XCTAssertFalse(rows.isEmpty, "Expected INFORMATION_SCHEMA.COLUMNS to return rows for dbo schema")
            }
        }
    }
    
    func testInformationSchemaColumnCount() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let sql = "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo';"
                let rows = try await conn.query(sql).get()
                guard let count = rows.first?.column("cnt")?.int else {
                    XCTFail("Expected to read column count from INFORMATION_SCHEMA.COLUMNS")
                    return
                }
                XCTAssertGreaterThan(count, 0)
            }
        }
    }

    func testFetchObjectDefinitions() async throws {
        try await withReliableConnection(client: self.client) { conn in
            let proc = makeSchemaQualifiedName(prefix: "def_proc")
            let view = makeSchemaQualifiedName(prefix: "def_view")

            let createProc = """
            CREATE PROCEDURE \(proc.bracketed)
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT 1 AS Value;
            END;
            """
            _ = try await conn.execute(createProc).get()

            let createView = """
            CREATE VIEW \(view.bracketed)
            AS
            SELECT N'DefinitionMarker' AS Marker;
            """
            _ = try await conn.execute(createView).get()

            let identifiers = [
                SQLServerMetadataObjectIdentifier(database: nil, schema: "dbo", name: proc.nameOnly, kind: .procedure),
                SQLServerMetadataObjectIdentifier(database: nil, schema: "dbo", name: view.nameOnly, kind: .view)
            ]

            let definitions = try await conn.fetchObjectDefinitions(identifiers).get()
            guard let procDefinition = definitions.first(where: { $0.name.caseInsensitiveCompare(proc.nameOnly) == .orderedSame }) else {
                XCTFail("Expected stored procedure definition"); return
            }
            XCTAssertEqual(procDefinition.type, .procedure)
            XCTAssertFalse(procDefinition.isSystemObject)
            XCTAssertEqual(procDefinition.definition?.uppercased().contains("SELECT 1"), true)

            let singleView = try await conn.fetchObjectDefinition(schema: "dbo", name: view.nameOnly, kind: .view).get()
            XCTAssertEqual(singleView?.type, .view)
            XCTAssertEqual(singleView?.definition?.contains("DefinitionMarker"), true)

            // cleanup
            _ = try? await conn.execute("DROP VIEW \(view.bracketed)").get()
            _ = try? await conn.execute("DROP PROCEDURE \(proc.bracketed)").get()
        }
    }

    func testMetadataSearchReturnsMatches() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let table = makeSchemaQualifiedName(prefix: "search_table")
                let related = makeSchemaQualifiedName(prefix: "search_related")

                _ = try await conn.execute("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()
                _ = try await conn.execute("IF OBJECT_ID(N'\(related.bare)', 'U') IS NOT NULL DROP TABLE \(related.bracketed);").get()

                let createRelated = """
                CREATE TABLE \(related.bracketed) (
                    Id INT PRIMARY KEY,
                    Note NVARCHAR(50)
                );
                """
                _ = try await conn.execute(createRelated).get()

                let createTable = """
                CREATE TABLE \(table.bracketed) (
                    Id INT PRIMARY KEY,
                    SearchColumn NVARCHAR(50) NOT NULL,
                    RelatedId INT NULL,
                    CONSTRAINT FK_\(table.nameOnly)_Related FOREIGN KEY (RelatedId) REFERENCES \(related.bracketed)(Id)
                );
                CREATE INDEX IX_\(table.nameOnly)_SearchColumn ON \(table.bracketed)(SearchColumn);
                """
                _ = try await conn.execute(createTable).get()

                // Test the searchMetadata API
                let columnHits = try await conn.searchMetadata(query: "SearchColumn", scopes: [.columns]).get()
                XCTAssertTrue(columnHits.contains(where: { $0.matchKind == .column && $0.name.caseInsensitiveCompare(table.nameOnly) == .orderedSame }))

                let indexHits = try await conn.searchMetadata(query: "IX_\(table.nameOnly)_SearchColumn", scopes: [.indexes]).get()
                XCTAssertTrue(indexHits.contains(where: { $0.matchKind == .index && $0.detail?.contains("IX_\(table.nameOnly)_SearchColumn") == true }))

                let constraintHits = try await conn.searchMetadata(query: "FK_\(table.nameOnly)_Related", scopes: [.constraints]).get()
                XCTAssertTrue(constraintHits.contains(where: { $0.matchKind == .constraint && $0.detail?.contains("FK_\(table.nameOnly)_Related") == true }))

                // Manual cleanup
                _ = try? await conn.execute("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()
                _ = try? await conn.execute("IF OBJECT_ID(N'\(related.bare)', 'U') IS NOT NULL DROP TABLE \(related.bracketed);").get()
            }
        }
    }

    func testChangeDatabaseAndScalarHelpers() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let defaultConfig = makeSQLServerConnectionConfiguration()
                let defaultDatabase = defaultConfig.login.database

                XCTAssertEqual(conn.currentDatabase.lowercased(), defaultDatabase.lowercased())

                let masterName: String? = try await conn.queryScalar("SELECT DB_NAME();", as: String.self).get()
                XCTAssertEqual(masterName?.lowercased(), defaultDatabase.lowercased())

                let targetDatabase = "msdb"
                _ = try await conn.changeDatabase(targetDatabase).get()
                XCTAssertEqual(conn.currentDatabase.lowercased(), targetDatabase)

                let scalarAfterChange: String? = try await conn.queryScalar("SELECT DB_NAME();", as: String.self).get()
                XCTAssertEqual(scalarAfterChange?.lowercased(), targetDatabase)

                _ = try await conn.changeDatabase(defaultDatabase).get()
            }
        }
    }

    func testColumnPropertyIsComputedSingle() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let sql = """
                SELECT c.is_computed
                FROM sys.columns AS c
                WHERE c.object_id = OBJECT_ID(N'dbo.MSreplication_options')
                  AND c.name = N'value';
                """
                let rows = try await conn.query(sql).get()
                XCTAssertEqual(rows.count, 1)
                _ = rows.first?.column("is_computed")?.int
            }
        }
    }

    func testSysCatalogColumnFlags() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let table = makeSchemaQualifiedName(prefix: "meta_flags")
                _ = try await conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()

                let defaultConstraint = "DF_\(table.nameOnly)_Name"
                let create = """
                CREATE TABLE \(table.bracketed) (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    Name NVARCHAR(50) NULL CONSTRAINT \(defaultConstraint) DEFAULT (N'fallback'),
                    Computed AS Id + 1
                );
                """
                _ = try await conn.query(create).get()

                let metadataSql = """
                SELECT
                    c.name,
                    c.is_identity,
                    c.is_nullable,
                    c.is_computed
                FROM sys.columns AS c
                WHERE c.object_id = OBJECT_ID(N'\(table.bare)')
                ORDER BY c.column_id;
                """
                let rows = try await conn.query(metadataSql).get()
                XCTAssertEqual(rows.count, 3)

                let idColumn = rows[0]
                XCTAssertEqual(idColumn.column("name")?.string, "Id")
                XCTAssertEqual(idColumn.column("is_identity")?.bool, true)
                XCTAssertEqual(idColumn.column("is_nullable")?.bool, false)
                XCTAssertEqual(idColumn.column("is_computed")?.bool, false)

                let nameColumn = rows[1]
                XCTAssertEqual(nameColumn.column("name")?.string, "Name")
                XCTAssertEqual(nameColumn.column("is_identity")?.bool, false)
                XCTAssertEqual(nameColumn.column("is_nullable")?.bool, true)
                XCTAssertEqual(nameColumn.column("is_computed")?.bool, false)

                let computedColumn = rows[2]
                XCTAssertEqual(computedColumn.column("name")?.string, "Computed")
                XCTAssertEqual(computedColumn.column("is_identity")?.bool, false)
                XCTAssertEqual(computedColumn.column("is_nullable")?.bool, true)
                XCTAssertEqual(computedColumn.column("is_computed")?.bool, true)

                // Test the listColumns API and validate column metadata properties
                let metadataColumns = try await conn.listColumns(schema: "dbo", table: table.nameOnly).get()
                XCTAssertEqual(metadataColumns.count, 3)

                guard let metadataName = metadataColumns.first(where: { $0.name == "Name" }) else {
                    XCTFail("Expected Name column metadata")
                    return
                }
                XCTAssertTrue(metadataName.hasDefaultValue)
                XCTAssertEqual(metadataName.defaultDefinition?.contains("fallback"), true)

                guard let metadataComputed = metadataColumns.first(where: { $0.name == "Computed" }) else {
                    XCTFail("Expected Computed column metadata")
                    return
                }
                XCTAssertEqual(metadataComputed.name, "Computed")
                XCTAssertGreaterThan(metadataComputed.ordinalPosition, metadataName.ordinalPosition)

                // Manual cleanup
                _ = try? await conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()
            }
        }
    }

    func testMetadataCoversKeysIndexesForeignKeysAndTriggers() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let parent = makeSchemaQualifiedName(prefix: "meta_parent")
                let child = makeSchemaQualifiedName(prefix: "meta_child")
                let view = makeSchemaQualifiedName(prefix: "meta_view")
                let triggerName = "TR_\(child.nameOnly)_Audit"
                let triggerQualified = "[dbo].[\(triggerName)]"

                let parentPKName = "PK_\(parent.nameOnly)"
                let parentUniqueName = "UQ_\(parent.nameOnly)_Code"
                let parentDefaultName = "DF_\(parent.nameOnly)_Created"
                let childPKName = "PK_\(child.nameOnly)"
                let fkName = "FK_\(child.nameOnly)_Parent"
                let indexName = "IX_\(child.nameOnly)_Note"

                _ = try await conn.query("""
                IF OBJECT_ID(N'\(view.bare)', 'V') IS NOT NULL DROP VIEW \(view.bracketed);
                IF OBJECT_ID(N'\(child.bare)', 'U') IS NOT NULL DROP TABLE \(child.bracketed);
                IF OBJECT_ID(N'\(parent.bare)', 'U') IS NOT NULL DROP TABLE \(parent.bracketed);
                """).get()

                let createParent = """
                CREATE TABLE \(parent.bracketed) (
                    ParentId INT IDENTITY(1,1) NOT NULL,
                    Code NVARCHAR(50) NOT NULL,
                    Created DATETIME2 NOT NULL CONSTRAINT [\(parentDefaultName)] DEFAULT SYSUTCDATETIME(),
                    Description NVARCHAR(100) NULL,
                    CONSTRAINT [\(parentPKName)] PRIMARY KEY CLUSTERED (ParentId ASC),
                    CONSTRAINT [\(parentUniqueName)] UNIQUE (Code)
                );
                """
                _ = try await conn.query(createParent).get()

                let createChild = """
                CREATE TABLE \(child.bracketed) (
                    ChildId INT IDENTITY(1,1) NOT NULL,
                    ParentId INT NOT NULL,
                    Note NVARCHAR(50) NULL,
                    Extra NVARCHAR(20) NULL,
                    CONSTRAINT [\(childPKName)] PRIMARY KEY CLUSTERED (ChildId ASC),
                    CONSTRAINT [\(fkName)] FOREIGN KEY (ParentId)
                        REFERENCES \(parent.bracketed)(ParentId)
                        ON DELETE CASCADE
                );
                """
                _ = try await conn.query(createChild).get()

                let createIndex = """
                CREATE NONCLUSTERED INDEX [\(indexName)]
                    ON \(child.bracketed) (Note DESC)
                    INCLUDE (Extra);
                """
                _ = try await conn.query(createIndex).get()

                let createTrigger = """
                CREATE TRIGGER \(triggerQualified)
                ON \(child.bracketed)
                AFTER INSERT
                AS
                BEGIN
                    SET NOCOUNT ON;
                    SELECT TOP (0) 1;
                END;
                """
                _ = try await conn.query(createTrigger).get()

                let createView = """
                CREATE VIEW \(view.bracketed)
                AS
                SELECT p.ParentId, p.Code
                FROM \(parent.bracketed) AS p;
                """
                _ = try await conn.query(createView).get()

                // Test comprehensive metadata APIs
                let primaryKeys = try await conn.listPrimaryKeys(schema: "dbo", table: parent.nameOnly).get()
                guard let parentPK = primaryKeys.first(where: { $0.name.caseInsensitiveCompare(parentPKName) == .orderedSame }) else {
                    XCTFail("Expected primary key metadata for parent table")
                    return
                }
                XCTAssertTrue(parentPK.isClustered)
                XCTAssertEqual(parentPK.columns.count, 1)
                XCTAssertEqual(parentPK.columns.first?.column, "ParentId")

                let uniqueConstraints = try await conn.listUniqueConstraints(schema: "dbo", table: parent.nameOnly).get()
                XCTAssertTrue(uniqueConstraints.contains(where: { constraint in
                    constraint.name.caseInsensitiveCompare(parentUniqueName) == .orderedSame &&
                    constraint.columns.first?.column == "Code"
                }), "Expected unique constraint metadata for Code column")

                let indexes = try await conn.listIndexes(schema: "dbo", table: child.nameOnly).get()
                guard let customIndex = indexes.first(where: { $0.name.caseInsensitiveCompare(indexName) == .orderedSame }) else {
                    XCTFail("Expected non-clustered index metadata")
                    return
                }
                XCTAssertFalse(customIndex.isUnique)
                XCTAssertFalse(customIndex.isPrimaryKey)
                let indexColumns = customIndex.columns.map(\.column)
                XCTAssertTrue(indexColumns.contains("Note"))

                let foreignKeys = try await conn.listForeignKeys(schema: "dbo", table: child.nameOnly).get()
                guard let fk = foreignKeys.first(where: { $0.name.caseInsensitiveCompare(fkName) == .orderedSame }) else {
                    XCTFail("Expected foreign key metadata")
                    return
                }
                XCTAssertEqual(fk.referencedTable.caseInsensitiveCompare(parent.nameOnly), .orderedSame)
                XCTAssertEqual(fk.columns.first?.parentColumn, "ParentId")
                XCTAssertEqual(fk.columns.first?.referencedColumn, "ParentId")
                XCTAssertEqual(fk.deleteAction.uppercased(), "CASCADE")

                let dependencies = try await conn.listDependencies(schema: "dbo", object: parent.nameOnly).get()
                XCTAssertTrue(dependencies.contains(where: { dependency in
                    dependency.referencingObject.caseInsensitiveCompare(view.nameOnly) == .orderedSame
                }), "Expected dependency on view")

                let triggers = try await conn.listTriggers(schema: "dbo", table: child.nameOnly).get()
                guard let trigger = triggers.first(where: { $0.name.caseInsensitiveCompare(triggerName) == .orderedSame }) else {
                    XCTFail("Expected trigger metadata")
                    return
                }
                XCTAssertFalse(trigger.isInsteadOf)
                XCTAssertFalse(trigger.isDisabled)
                XCTAssertNotNil(trigger.definition)

                // Manual cleanup
                _ = try? await conn.query("""
                IF OBJECT_ID(N'\(view.bare)', 'V') IS NOT NULL DROP VIEW \(view.bracketed);
                IF OBJECT_ID(N'\(child.bare)', 'U') IS NOT NULL DROP TABLE \(child.bracketed);
                IF OBJECT_ID(N'\(parent.bare)', 'U') IS NOT NULL DROP TABLE \(parent.bracketed);
                """).get()
            }
        }
    }

    func testFunctionMetadataIncludesReturnAndParameters() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "itfn") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let function = makeSchemaQualifiedName(prefix: "fn_meta")

                _ = try await conn.execute("IF OBJECT_ID(N'\(function.bare)', 'FN') IS NOT NULL DROP FUNCTION \(function.bracketed);").get()

                let createFunction = """
                CREATE FUNCTION \(function.bracketed)
                (
                    @Input INT,
                    @Category NVARCHAR(10) = N'default'
                )
                RETURNS NVARCHAR(100)
                AS
                BEGIN
                    RETURN CONCAT('value-', @Input, '-', @Category);
                END;
                """
                _ = try await conn.execute(createFunction).get()

                let functions = try await conn.listFunctions(database: db, schema: "dbo").get()
                guard let metadata = functions.first(where: { $0.name.caseInsensitiveCompare(function.nameOnly) == .orderedSame }) else {
                    XCTFail("Expected function metadata entry")
                    return
                }
                XCTAssertEqual(metadata.type, .scalarFunction)
                XCTAssertNotNil(metadata.definition)

                let parameters = try await conn.listParameters(database: db, schema: "dbo", object: function.nameOnly).get()
                guard let category = parameters.first(where: { $0.name.caseInsensitiveCompare("@Category") == .orderedSame }) else {
                    XCTFail("Expected @Category parameter")
                    return
                }
                XCTAssertTrue(category.hasDefaultValue)
                XCTAssertEqual(category.defaultValue?.contains("default"), true)
                XCTAssertFalse(category.isOutput)

                _ = try await conn.execute("DROP FUNCTION \(function.bracketed);").get()
            }
        }
    }
    
    func testMetadataClientColumnListing() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let metadata = SQLServerMetadataClient(connection: conn)
                let databases = try await metadata.listDatabases().get()
                XCTAssertFalse(databases.isEmpty, "Expected at least one database")

                let schemaName = "meta_schema_\(UUID().uuidString.prefix(8))"
                let tableName = "meta_table_\(UUID().uuidString.prefix(8))"
                _ = try await conn.execute("EXEC('CREATE SCHEMA [\(schemaName)]');").get()

                _ = try await conn.execute("""
                CREATE TABLE [\(schemaName)].[\(tableName)](
                    id INT NOT NULL,
                    flag BIT NOT NULL,
                    value NVARCHAR(32) NOT NULL,
                    computed AS (id + 5)
                );
                """).get()

                // Test SQLServerMetadataClient APIs
                let tables = try await metadata.listTables(schema: schemaName).get()
                XCTAssertTrue(tables.contains(where: { $0.name == tableName }))

                let columns = try await metadata.listColumns(schema: schemaName, table: tableName).get()
                XCTAssertEqual(columns.count, 4, "Expected custom table to expose four columns")

                guard let flagColumn = columns.first(where: { $0.name == "flag" }) else {
                    XCTFail("Expected flag column metadata")
                    return
                }
                XCTAssertEqual(flagColumn.typeName.lowercased(), "bit")
                XCTAssertEqual(flagColumn.isNullable, false)
                XCTAssertEqual(flagColumn.isIdentity, false)
                XCTAssertFalse(flagColumn.isComputed)

                guard let computedColumn = columns.first(where: { $0.name == "computed" }) else {
                    XCTFail("Expected computed column metadata")
                    return
                }
                XCTAssertTrue(computedColumn.isComputed)

                // Manual cleanup
                _ = try? await conn.execute("DROP TABLE IF EXISTS [\(schemaName)].[\(tableName)]").get()
                _ = try? await conn.execute("DROP SCHEMA IF EXISTS [\(schemaName)]").get()
            }
        }
    }

    func testListParametersInlineTVFAndSchemaSweep() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "itprm") { db in
            try await withDbConnection(client: self.client, database: db) { conn in

        // Create a few routines, including an inline TVF with defaults
        let fn = makeSchemaQualifiedName(prefix: "ufn_inline")
        let p1 = makeSchemaQualifiedName(prefix: "usp_meta_1")
        let p2 = makeSchemaQualifiedName(prefix: "usp_meta_2")

        _ = try await conn.execute("IF OBJECT_ID(N'\(fn.bare)', 'IF') IS NOT NULL DROP FUNCTION \(fn.bracketed);").get()
        _ = try await conn.execute("IF OBJECT_ID(N'\(p1.bare)', 'P') IS NOT NULL DROP PROCEDURE \(p1.bracketed);").get()
        _ = try await conn.execute("IF OBJECT_ID(N'\(p2.bare)', 'P') IS NOT NULL DROP PROCEDURE \(p2.bracketed);").get()

        let createTVF = """
        CREATE FUNCTION \(fn.bracketed)
        (
            @Start INT,
            @Finish INT = 3
        )
        RETURNS TABLE
        AS
        RETURN (
            SELECT n
            FROM (VALUES(1),(2),(3),(4),(5)) AS t(n)
            WHERE n BETWEEN @Start AND @Finish
        );
        """
        _ = try await conn.execute(createTVF).get()

        let createP1 = """
        CREATE PROCEDURE \(p1.bracketed)
            @A INT,
            @B NVARCHAR(10) = N'def',
            @C INT OUTPUT
        AS BEGIN SET NOCOUNT ON; SET @C = @A; END
        """
        _ = try await conn.execute(createP1).get()

        let createP2 = """
        CREATE PROCEDURE \(p2.bracketed)
            @X INT
        AS BEGIN SET NOCOUNT ON; SELECT @X; END
        """
        _ = try await conn.execute(createP2).get()

        // Verify parameter metadata for inline TVF
        let tvfParams = try await conn.listParameters(database: db, schema: "dbo", object: fn.nameOnly).get()
        XCTAssertEqual(tvfParams.filter { !$0.isReturnValue }.count, 2)
        XCTAssertTrue(tvfParams.contains(where: { $0.name.caseInsensitiveCompare("@Start") == .orderedSame && !$0.isOutput }))
        XCTAssertTrue(tvfParams.contains(where: { $0.name.caseInsensitiveCompare("@Finish") == .orderedSame && $0.hasDefaultValue }))

        // Quick schema-wide sweep similar to app behavior: list functions and procedures and fetch parameters
        let meta = SQLServerMetadataClient(connection: conn)
        let _ = try await meta.listProcedures(database: db, schema: "dbo").get()
        let _ = try await meta.listFunctions(database: db, schema: "dbo").get()
        // Only exercise a handful that we just created
        for name in [p1.nameOnly, p2.nameOnly, fn.nameOnly] {
            _ = try await conn.listParameters(database: db, schema: "dbo", object: name).get()
        }
        _ = try await conn.execute("DROP PROCEDURE \(p1.bracketed);").get()
        _ = try await conn.execute("DROP PROCEDURE \(p2.bracketed);").get()
        _ = try await conn.execute("DROP FUNCTION \(fn.bracketed);").get()
            }
        }
    }
    
    func testMetadataClientSchemaFiltering() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let filteredMetadata = SQLServerMetadataClient(connection: conn, configuration: .init(includeSystemSchemas: false))
                let filteredSchemas = try await filteredMetadata.listSchemas().get()
                XCTAssertFalse(filteredSchemas.contains(where: { $0.name.caseInsensitiveCompare("sys") == .orderedSame }))

                let inclusiveMetadata = SQLServerMetadataClient(connection: conn, configuration: .init(includeSystemSchemas: true))
                let allSchemas = try await inclusiveMetadata.listSchemas().get()
                XCTAssertTrue(allSchemas.contains(where: { $0.name.caseInsensitiveCompare("sys") == .orderedSame }))
            }
        }
    }
    
    func testMetadataClientIndexesAndConstraints() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "itidx") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let tableName = "meta_idx_\(UUID().uuidString.prefix(8))"
                let pkName = "PK_\(tableName)"
                let uqName = "UQ_\(tableName)_code"
                let indexName = "IX_\(tableName)_payload"

                _ = try await conn.execute("""
                CREATE TABLE dbo.[\(tableName)](
                    id INT NOT NULL,
                    code NVARCHAR(32) NOT NULL,
                    payload INT NULL,
                    CONSTRAINT [\(pkName)] PRIMARY KEY CLUSTERED (id),
                    CONSTRAINT [\(uqName)] UNIQUE (code)
                );
                """).get()
                _ = try await conn.execute("CREATE INDEX [\(indexName)] ON dbo.[\(tableName)] (payload) INCLUDE (code);").get()

                let metadata = SQLServerMetadataClient(connection: conn)
                let primaryKeys = try await metadata.listPrimaryKeys(database: db, schema: "dbo", table: tableName).get()
                guard let pk = primaryKeys.first(where: { $0.name.caseInsensitiveCompare(pkName) == .orderedSame }) else {
                    XCTFail("Missing primary key metadata")
                    return
                }
                XCTAssertEqual(pk.columns.map { $0.column }, ["id"])

                // Regression: schema-only PK enumeration should not error and should include our table
                let schemaOnlyPKs = try await metadata.listPrimaryKeys(database: db, schema: "dbo").get()
                XCTAssertTrue(schemaOnlyPKs.contains(where: { $0.schema.caseInsensitiveCompare("dbo") == .orderedSame && $0.table.caseInsensitiveCompare(tableName) == .orderedSame }),
                              "Expected to find primary key for \(tableName) when listing PKs by schema only")
                XCTAssertTrue(pk.isClustered)

                let uniqueConstraints = try await metadata.listUniqueConstraints(database: db, schema: "dbo", table: tableName).get()
                guard let unique = uniqueConstraints.first(where: { $0.name.caseInsensitiveCompare(uqName) == .orderedSame }) else {
                    XCTFail("Missing unique constraint metadata")
                    return
                }
                XCTAssertEqual(unique.columns.map { $0.column }, ["code"])

                let indexes = try await metadata.listIndexes(database: db, schema: "dbo", table: tableName).get()
                guard let ix = indexes.first(where: { $0.name.caseInsensitiveCompare(indexName) == .orderedSame }) else {
                    XCTFail("Missing nonclustered index metadata")
                    return
                }
                XCTAssertFalse(ix.isClustered)
                XCTAssertFalse(ix.isPrimaryKey)
                let indexColumns = ix.columns.map { $0.column }
                XCTAssertTrue(indexColumns.contains("payload"))
                _ = try await conn.execute("DROP TABLE dbo.[\(tableName)];").get()
            }
        }
    }
    
    func testMetadataClientForeignKeys() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "itfk") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let parentTable = "meta_parent_\(UUID().uuidString.prefix(8))"
                let childTable = "meta_child_\(UUID().uuidString.prefix(8))"
                let fkName = "FK_\(childTable)_parent"

                _ = try await conn.execute("""
                CREATE TABLE dbo.[\(parentTable)](
                    id INT PRIMARY KEY,
                    description NVARCHAR(40) NOT NULL
                );
                """).get()

                _ = try await conn.execute("""
                CREATE TABLE dbo.[\(childTable)](
                    id INT PRIMARY KEY,
                    parent_id INT NOT NULL,
                    payload NVARCHAR(40) NULL
                );
                ALTER TABLE dbo.[\(childTable)]
                ADD CONSTRAINT [\(fkName)] FOREIGN KEY(parent_id)
                REFERENCES dbo.[\(parentTable)](id)
                ON DELETE CASCADE
                ON UPDATE NO ACTION;
                """).get()

                let metadata = SQLServerMetadataClient(connection: conn)
                let foreignKeys = try await metadata.listForeignKeys(database: db, schema: "dbo", table: childTable).get()
                guard let fk = foreignKeys.first(where: { $0.name.caseInsensitiveCompare(fkName) == .orderedSame }) else {
                    XCTFail("Missing foreign key metadata")
                    return
                }
                XCTAssertEqual(fk.referencedTable, parentTable)
                XCTAssertEqual(fk.deleteAction, "CASCADE")
                XCTAssertEqual(fk.updateAction, "NO ACTION")
                XCTAssertEqual(fk.columns.map { $0.parentColumn }, ["parent_id"])
                XCTAssertEqual(fk.columns.map { $0.referencedColumn }, ["id"])
            }
        }
    }
    
    func testMetadataClientRoutineDefinitionsToggle() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "itrdefs") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let procedureName = "meta_proc_\(UUID().uuidString.prefix(8))"
                _ = try await conn.execute("""
                CREATE PROCEDURE dbo.[\(procedureName)]
                @Value INT
                AS
                BEGIN
                    SELECT @Value + 10 AS computed;
                END;
                """).get()

                let withoutDefinitions = SQLServerMetadataClient(connection: conn, configuration: .init(includeRoutineDefinitions: false))
                let proceduresWithout = try await withoutDefinitions.listProcedures(database: db, schema: "dbo").get()
                guard let entryWithout = proceduresWithout.first(where: { $0.name.caseInsensitiveCompare(procedureName) == .orderedSame }) else {
                    XCTFail("Missing procedure metadata (no defs)")
                    return
                }
                XCTAssertNil(entryWithout.definition)

                let withDefinitions = SQLServerMetadataClient(connection: conn, configuration: .init(includeRoutineDefinitions: true))
                let proceduresWith = try await withDefinitions.listProcedures(database: db, schema: "dbo").get()
                guard let entryWith = proceduresWith.first(where: { $0.name.caseInsensitiveCompare(procedureName) == .orderedSame }) else {
                    XCTFail("Missing procedure metadata (with defs)")
                    return
                }
                XCTAssertEqual(entryWith.definition?.contains("SELECT @Value + 10"), true)
                _ = try await conn.execute("DROP PROCEDURE dbo.[\(procedureName)]").get()
            }
        }
    }
    

    func testStreamQueryEmitsMetadataAndRows() async throws {
        guard #available(macOS 12.0, *) else {
            throw XCTSkip("Streaming API requires async/await")
        }

        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let sql = "SELECT TOP (3) name, database_id FROM sys.databases ORDER BY name;"

                let finished = self.expectation(description: "stream finished")
                var firstEvent: SQLServerStreamEvent?
                var metadataColumns: [SQLServerColumnDescription] = []
                var rowCount = 0
                var doneEvents: [SQLServerStreamDone] = []
                var infoMessages: [SQLServerStreamMessage] = []

                let task = Task {
                    defer { finished.fulfill() }
                    do {
                        for try await event in conn.streamQuery(sql) {
                            if firstEvent == nil {
                                firstEvent = event
                            }
                            switch event {
                            case .metadata(let columns):
                                metadataColumns.append(contentsOf: columns)
                            case .row:
                                rowCount += 1
                            case .done(let done):
                                doneEvents.append(done)
                            case .message(let message):
                                infoMessages.append(message)
                            }
                        }
                    } catch {
                        XCTFail("stream failed with error: \(error)")
                    }
                }

                await self.fulfillment(of: [finished], timeout: self.TIMEOUT)
                task.cancel()

                guard let first = firstEvent else {
                    XCTFail("Expected at least one stream event")
                    return
                }
                switch first {
                case .metadata:
                    break
                default:
                    XCTFail("Expected metadata to arrive before rows; got \(first)")
                }

                XCTAssertFalse(metadataColumns.isEmpty, "Expected metadata before rows")
                XCTAssertGreaterThan(rowCount, 0, "Expected streamed rows")
                XCTAssertGreaterThan(doneEvents.count, 0, "Expected DONE tokens")
                XCTAssertNotNil(infoMessages) // allow empty; ensures compile
            }
        }
    }

    func testStreamQuerySupportsEarlyStop() async throws {
        guard #available(macOS 12.0, *) else {
            throw XCTSkip("Streaming API requires async/await")
        }

        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let sql = "SELECT TOP (25) o.name FROM sys.objects AS o ORDER BY o.name;"

                let firstRowExpectation = self.expectation(description: "received first streamed row")
                let completionExpectation = self.expectation(description: "stream task completed")

                var seenRows: [TDSRow] = []
                let task = Task {
                    defer { completionExpectation.fulfill() }
                    do {
                        for try await event in conn.streamQuery(sql) {
                            if case .row(let row) = event {
                                seenRows.append(row)
                                if seenRows.count == 1 {
                                    firstRowExpectation.fulfill()
                                    return
                                }
                            }
                        }
                    } catch {
                        XCTFail("streaming cancellation failure: \(error)")
                    }
                }

                await self.fulfillment(of: [firstRowExpectation], timeout: self.TIMEOUT)
                task.cancel()
                await self.fulfillment(of: [completionExpectation], timeout: self.TIMEOUT)

                XCTAssertEqual(seenRows.count, 1, "Expected to capture exactly one streamed row before stopping")

                let followUp = try await conn.query("SELECT 1 AS value;").get()
                XCTAssertEqual(followUp.first?.column("value")?.int, 1)
            }
        }
    }

    func testConnectionPoolReusesConnections() async throws {
        try await withTimeout(TIMEOUT) {
            let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

            var configuration = makeSQLServerClientConfiguration()
            configuration.poolConfiguration.maximumConcurrentConnections = 1
            configuration.poolConfiguration.minimumIdleConnections = 1

            let client = try await SQLServerClient.connect(configuration: configuration, eventLoopGroupProvider: .shared(loopGroup)).get()

            func fetchSpid() async throws -> Int {
                let rows = try await client.withConnection { connection in
                    connection.query("SELECT @@SPID AS spid;")
                }.get()
                return rows.first?.column("spid")?.int ?? -1
            }

            let firstSpid = try await fetchSpid()
            let secondSpid = try await fetchSpid()

            XCTAssertEqual(firstSpid, secondSpid, "Expected pooled client to reuse the same underlying connection when max concurrency is 1")

            _ = try? await client.shutdownGracefully().get()
            try? await loopGroup.shutdownGracefully()
        }
    }
    
    func testInformationSchemaBasicColumnsFetch() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let spidRows = try await conn.query("SELECT @@SPID AS spid;").get()
                let spid = spidRows.first?.column("spid")?.int ?? -1

                let sql = """
                SELECT TOP (10)
                    c.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                WHERE c.TABLE_SCHEMA = 'dbo'
                ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
                """

                // Start the query and wait
                _ = try await conn.query(sql).get()

                let monitorConn = try await self.client.withConnection { monitorConn in
                    return monitorConn
                }

                let monitorSql = "SELECT status, wait_type, command, cpu_time, total_elapsed_time FROM sys.dm_exec_requests WHERE session_id = \(spid);"
                let waits = try? await monitorConn.query(monitorSql).get()
                if let waitRow = waits?.first {
                    print("Monitor status: \(waitRow)")
                }

                let rows = try await conn.query(sql).get()
                XCTAssertEqual(rows.count, 10)
            }
        }
    }

    func testSchemaVersioningDetectsDefinitionChange() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "itsv") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let table = makeSchemaQualifiedName(prefix: "tbl_version")
                _ = try await conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()
                let createTable = """
                CREATE TABLE \(table.bracketed) (
                    Id INT NOT NULL PRIMARY KEY,
                    Name NVARCHAR(100) NOT NULL
                );
                """
                _ = try await conn.query(createTable).get()

                func schemaSignature() async throws -> String {
                    let sql = """
                    SELECT signature = CONVERT(VARCHAR(64), HASHBYTES('SHA2_256',
                        STRING_AGG(CONCAT_WS('|', c.column_id, c.name, t.name, c.max_length, c.precision, c.scale, c.is_nullable), ';')
                            WITHIN GROUP (ORDER BY c.column_id)
                    ))
                    FROM sys.columns AS c
                    JOIN sys.types AS t ON c.user_type_id = t.user_type_id
                    WHERE c.object_id = OBJECT_ID(N'\(table.bare)');
                    """
                    let rows = try await conn.query(sql).get()
                    return rows.first?.column("signature")?.string ?? ""
                }

                let baselineSignature = try await schemaSignature()
                XCTAssertFalse(baselineSignature.isEmpty, "Expected baseline schema signature")

                _ = try await conn.query("ALTER TABLE \(table.bracketed) ADD ModifiedAt DATETIME2 NULL;").get()
                let alteredSignature = try await schemaSignature()
                XCTAssertFalse(alteredSignature.isEmpty, "Expected altered schema signature")
                XCTAssertNotEqual(baselineSignature, alteredSignature, "Schema signature should change after altering table definition")

                _ = try await conn.query("ALTER TABLE \(table.bracketed) DROP COLUMN ModifiedAt;").get()
                let revertedSignature = try await schemaSignature()
                XCTAssertEqual(baselineSignature, revertedSignature, "Reverted schema should match baseline signature")
            }
        }
    }
    
    func testScalarAndTableValuedFunctions() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "itfn2") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let scalar = makeSchemaQualifiedName(prefix: "fn_tds_scalar")
                let tvf = makeSchemaQualifiedName(prefix: "fn_tds_table")

                _ = try await conn.query("IF OBJECT_ID(N'\(scalar.bare)', 'FN') IS NOT NULL DROP FUNCTION \(scalar.bracketed);").get()
                _ = try await conn.query("IF OBJECT_ID(N'\(tvf.bare)', 'IF') IS NOT NULL DROP FUNCTION \(tvf.bracketed);").get()

                let createScalar = """
                CREATE FUNCTION \(scalar.bracketed) (@input NVARCHAR(100))
                RETURNS NVARCHAR(200)
                AS
                BEGIN
                    RETURN CONCAT(@input, N'_suffix');
                END;
                """
                _ = try await conn.query(createScalar).get()

                let createTableFunc = """
                CREATE FUNCTION \(tvf.bracketed) (@top INT)
                RETURNS TABLE
                AS
                RETURN SELECT TOP (@top) database_id, name FROM sys.databases ORDER BY name;
                """
                _ = try await conn.query(createTableFunc).get()

                let scalarRows = try await conn.query("SELECT \(scalar.bare)(N'prefix') AS value;").get()
                XCTAssertEqual(scalarRows.first?.column("value")?.string, "prefix_suffix")

                let tableRows = try await conn.query("SELECT COUNT(*) AS cnt FROM \(tvf.bracketed)(2);").get()
                XCTAssertEqual(tableRows.first?.column("cnt")?.int, 2)

                let alterScalar = """
                ALTER FUNCTION \(scalar.bracketed) (@input NVARCHAR(100))
                RETURNS NVARCHAR(200)
                AS
                BEGIN
                    RETURN CONCAT(N'new_', @input);
                END;
                """
                _ = try await conn.query(alterScalar).get()
                let alteredRows = try await conn.query("SELECT \(scalar.bare)(N'value') AS value;").get()
                XCTAssertEqual(alteredRows.first?.column("value")?.string, "new_value")
            }
        }
    }
    
    func testDmlTriggerLifecycle() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "ittrg") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let base = makeSchemaQualifiedName(prefix: "tbl_tds_base")
                let audit = makeSchemaQualifiedName(prefix: "tbl_tds_audit")
                let trigger = makeSchemaQualifiedName(prefix: "trg_tds_insert")

                _ = try await conn.query("CREATE TABLE \(base.bracketed) (id INT PRIMARY KEY, description NVARCHAR(100));").get()
                _ = try await conn.query("CREATE TABLE \(audit.bracketed) (id INT, description NVARCHAR(100));").get()

                let createTrigger = """
                CREATE TRIGGER \(trigger.bracketed)
                ON \(base.bracketed)
                AFTER INSERT
                AS
                BEGIN
                    INSERT INTO \(audit.bracketed)(id, description)
                    SELECT id, description FROM inserted;
                END;
                """
                _ = try await conn.query(createTrigger).get()

                _ = try await conn.query("INSERT INTO \(base.bracketed) (id, description) VALUES (42, N'answer');").get()

                let auditRows = try await conn.query("SELECT description FROM \(audit.bracketed) WHERE id = 42;").get()
                XCTAssertEqual(auditRows.first?.column("description")?.string, "answer")
            }
        }
    }
    
    func testSynonymResolvesToSource() async throws {
        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let table = makeSchemaQualifiedName(prefix: "tbl_syn_src")
                let synonym = makeSchemaQualifiedName(prefix: "syn_tds")

                _ = try await conn.query("IF OBJECT_ID(N'\(synonym.bare)', 'SN') IS NOT NULL DROP SYNONYM \(synonym.bracketed);").get()
                _ = try await conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()

                _ = try await conn.query("CREATE TABLE \(table.bracketed) (name NVARCHAR(100));").get()
                _ = try await conn.query("INSERT INTO \(table.bracketed) VALUES (N'alpha'), (N'beta');").get()
                _ = try await conn.query("CREATE SYNONYM \(synonym.bracketed) FOR \(table.bare);").get()

                let rows = try await conn.query("SELECT COUNT(*) AS cnt FROM \(synonym.bracketed);").get()
                XCTAssertEqual(rows.first?.column("cnt")?.int, 2)

                // Cleanup
                _ = try? await conn.query("IF OBJECT_ID(N'\(synonym.bare)', 'SN') IS NOT NULL DROP SYNONYM \(synonym.bracketed);").get()
                _ = try? await conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()
            }
        }
    }
    
    func testPermissionGrantAndRevokeLifecycle() async throws {
        guard env("TDS_ENABLE_PERMISSIONS_TESTS") == "1" else {
            throw XCTSkip("Skipping permissions tests. Set TDS_ENABLE_PERMISSIONS_TESTS=1 to enable.")
        }

        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let table = makeSchemaQualifiedName(prefix: "perm_table")
                let userName = "tds_user_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
                let bracketedUser = "[\(userName)]"

                _ = try await conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()
                _ = try await conn.query("IF USER_ID(N'\(userName)') IS NOT NULL DROP USER \(bracketedUser);").get()

                let createTable = """
                CREATE TABLE \(table.bracketed) (
                    Id INT NOT NULL PRIMARY KEY,
                    Payload NVARCHAR(50) NOT NULL
                );
                INSERT INTO \(table.bracketed) (Id, Payload) VALUES (1, N'row');
                """
                _ = try await conn.query(createTable).get()

                _ = try await conn.query("CREATE USER \(bracketedUser) WITHOUT LOGIN;").get()

                func probeAccess() async throws -> String {
                    let script = """
                    DECLARE @result NVARCHAR(20) = N'denied';
                    BEGIN TRY
                        EXECUTE AS USER = N'\(userName)';
                        SELECT TOP (1) 1 FROM \(table.bracketed);
                        SET @result = N'granted';
                    END TRY
                    BEGIN CATCH
                        SET @result = N'denied';
                    END CATCH;
                    IF USER_NAME() <> ORIGINAL_LOGIN() REVERT;
                    SELECT outcome = @result;
                    """
                    let rows = try await conn.query(script).get()
                    return rows.last?.column("outcome")?.string ?? ""
                }

                let initialAccess = try await probeAccess()
                XCTAssertEqual(initialAccess, "denied", "User should not have SELECT permission before grant")

                _ = try await conn.query("GRANT SELECT ON \(table.bracketed) TO \(bracketedUser);").get()
                let grantedAccess = try await probeAccess()
                XCTAssertEqual(grantedAccess, "granted", "User should have SELECT permission after grant")

                _ = try await conn.query("REVOKE SELECT ON \(table.bracketed) FROM \(bracketedUser);").get()
                let revokedAccess = try await probeAccess()
                XCTAssertEqual(revokedAccess, "denied", "User should lose SELECT permission after revoke")

                // Cleanup
                _ = try? await conn.query("IF USER_ID(N'\(userName)') IS NOT NULL DROP USER \(bracketedUser);").get()
                _ = try? await conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()
            }
        }
    }

    func testSqlAgentJobLifecycle() async throws {
        guard env("TDS_ENABLE_AGENT_TESTS") == "1" else {
            throw XCTSkip("Skipping SQL Agent tests. Set TDS_ENABLE_AGENT_TESTS=1 to enable.")
        }

        try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { conn in
                let jobName = "tds_job_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
                let stepName = "step_main"

                // Check agent status using metadata client API
                let metadataClient = SQLServerMetadataClient(connection: conn)
                let agentStatus = try await metadataClient.fetchAgentStatus().get()

                if !agentStatus.isSqlAgentRunning {
                    throw XCTSkip("Not applicable: SQL Server Agent service not running on target instance")
                }

                // Use SQLServerAgentClient for all Agent operations
                let agent = SQLServerAgentClient(client: self.client)

                // Create job using API instead of raw SQL
                try await agent.createJob(named: jobName, description: "tds-nio integration test job", enabled: true)

                // Add TSQL step using API
                try await agent.addTSQLStep(jobName: jobName, stepName: stepName, command: "SET NOCOUNT ON; INSERT INTO tempdb.dbo.tds_agent_marker DEFAULT VALUES;", database: "master")

                // Attach job to server using API
                try await agent.addJobServer(jobName: jobName)

                // Create marker table for verification (minimal raw SQL for temp table)
                _ = try await conn.query("IF OBJECT_ID('tempdb.dbo.tds_agent_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_agent_marker;").get()
                _ = try await conn.query("CREATE TABLE tempdb.dbo.tds_agent_marker (id INT IDENTITY(1,1) PRIMARY KEY);").get()

                do {
                    // Start job using API
                    try await agent.startJob(named: jobName)
                } catch {
                    if let error = error as? TDSError, case .protocolError(let message) = error, message.contains("SQLSERVERAGENT") {
                        throw XCTSkip("Not applicable: SQL Server Agent refused to start job (\(message))")
                    }
                    throw error
                }

                // Monitor job completion using API
                var observedStatus: SQLServerAgentJobHistoryEntry?
                let deadline = Date().addingTimeInterval(30)
                while Date() < deadline {
                    let history = try await agent.listJobHistory(jobName: jobName, top: 1)
                    if let latest = history.first {
                        observedStatus = latest
                        if latest.runStatus != 4 { // 4 = in-progress
                            break
                        }
                    }
                    try await Task.sleep(nanoseconds: 1_000_000_000)
                }

                // Check marker table for job execution verification
                let markerRows = try await conn.query("SELECT COUNT(*) AS cnt FROM tempdb.dbo.tds_agent_marker;").get()
                let markerCount = markerRows.first?.column("cnt")?.int ?? 0

                guard let result = observedStatus else {
                    XCTFail("Agent job did not complete within allotted time. No history recorded.")
                    return
                }

                if result.runStatus != 1 { // 1 = succeeded
                    XCTFail("Agent job finished with status=\(result.runStatus) (step \(result.stepId)). Message: \(result.message)")
                }

                XCTAssertEqual(markerCount, 1, "Expected agent job step to insert one marker row (history message: \(result.message))")

                // Cleanup using API
                _ = try? await agent.deleteJob(named: jobName).get()
                _ = try? await conn.query("IF OBJECT_ID('tempdb.dbo.tds_agent_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_agent_marker;").get()
            }
        }
    }

    func testSQLServerClientMetadataFacade() async throws {
        var configuration = makeSQLServerClientConfiguration()
        configuration.metadataConfiguration.includeSystemSchemas = true

        try await withTimeout(TIMEOUT) {
            let client = try await SQLServerClient.connect(configuration: configuration, eventLoopGroupProvider: .shared(self.client.eventLoopGroup)).get()

            let databases = try await client.listDatabases().get()
            XCTAssertTrue(
                databases.contains { $0.name.caseInsensitiveCompare(configuration.login.database) == .orderedSame },
                "Expected listDatabases to include \(configuration.login.database)"
            )

            let schemas = try await client.listSchemas().get()
            XCTAssertTrue(schemas.contains { $0.name == "dbo" }, "Expected dbo schema to be present")

            let schemaName = "client_meta_schema_\(UUID().uuidString.prefix(8))"
            _ = try await client.query("EXEC('CREATE SCHEMA [\(schemaName)]');").get()

            let table = makeSchemaQualifiedName(prefix: "client_meta", schema: schemaName)
            _ = try? await client.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()

            let createSQL = """
            CREATE TABLE \(table.bracketed) (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                Name NVARCHAR(50) NOT NULL,
                NameLength AS (LEN(Name))
            );
            """
            _ = try await client.query(createSQL).get()

            let tables = try await client.listTables(schema: schemaName).get()
            XCTAssertTrue(tables.contains { $0.name == table.nameOnly }, "Expected listTables to include \(table.nameOnly)")

            let columns = try await client.listColumns(schema: schemaName, table: table.nameOnly).get()
            XCTAssertEqual(columns.count, 3, "Expected computed identity table to expose three columns")
            XCTAssertEqual(columns.first(where: { $0.name == "Id" })?.isIdentity, true, "Primary key should be flagged as identity")
            XCTAssertEqual(columns.first(where: { $0.name == "Name" })?.isNullable, false, "Non-null column should surface as not nullable")
            XCTAssertEqual(columns.first(where: { $0.name == "NameLength" })?.isComputed, true, "Computed column should surface isComputed flag")

            let cachedColumns = try await client.listColumns(schema: schemaName, table: table.nameOnly).get()
            XCTAssertEqual(cachedColumns.map(\.name), columns.map(\.name), "Cached column metadata should match initial fetch")

            let directConnection = try await SQLServerConnection.connect(
                configuration: configuration.connection,
                on: self.client.eventLoopGroup.next(),
                logger: Logger(label: "tds.sqlserver.connection.test")
            ).get()

            let directRows = try await directConnection.query("SELECT TOP (1) name FROM sys.databases ORDER BY name;").get()
            XCTAssertFalse(directRows.isEmpty, "Expected direct SQLServerConnection query to return at least one row")
            _ = try? await directConnection.close().get()

            let spidRows = try await client.withConnection { connection in
                return try await connection.query("SELECT @@SPID AS session_id;").get()
            }
            XCTAssertEqual(spidRows.count, 1)
            XCTAssertNotNil(spidRows.first?.column("session_id")?.int)

            // Cleanup
            _ = try? await client.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);").get()
            _ = try? await client.query("DROP SCHEMA IF EXISTS [\(schemaName)]").get()
            _ = try? await client.shutdownGracefully().get()
        }
    }
}
