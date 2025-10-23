import XCTest
import Logging
import NIO
import NIOTestUtils
@testable import SQLServerNIO

final class SQLServerIntegrationTests: XCTestCase {
    private static var executionSummary: [String] = []
    
    private var group: EventLoopGroup!
    private var eventLoop: EventLoop { self.group.next() }
    private let TIMEOUT: TimeInterval = 15
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_SCHEMA_TESTS", description: "schema management integration tests")
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }
    
    override func tearDownWithError() throws {
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
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
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
    
    func testTempTableCrudLifecycle() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let tableName = makeTempTableName(prefix: "crud")
        _ = try waitForResult(conn.query("CREATE TABLE \(tableName) (id INT PRIMARY KEY, name NVARCHAR(100));"), timeout: TIMEOUT, description: "create temp table")
        defer { _ = try? waitForResult(conn.query("IF OBJECT_ID('tempdb..\(tableName)', 'U') IS NOT NULL DROP TABLE \(tableName);"), timeout: TIMEOUT, description: "drop temp table") }
        
        _ = try waitForResult(conn.query("INSERT INTO \(tableName) (id, name) VALUES (1, N'original');"), timeout: TIMEOUT, description: "insert row")
        _ = try waitForResult(conn.query("UPDATE \(tableName) SET name = N'updated' WHERE id = 1;"), timeout: TIMEOUT, description: "update row")
        
        let rows = try waitForResult(conn.query("SELECT name FROM \(tableName) WHERE id = 1;"), timeout: TIMEOUT, description: "select row")
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0].column("name")?.string, "updated")
        
        _ = try waitForResult(conn.query("DELETE FROM \(tableName) WHERE id = 1;"), timeout: TIMEOUT, description: "delete row")
        let remaining = try waitForResult(conn.query("SELECT COUNT(*) AS row_count FROM \(tableName);"), timeout: TIMEOUT, description: "count rows")
        XCTAssertEqual(remaining.first?.column("row_count")?.int, 0)
    }
    
    func testStoredProcedureWithOutputAndResults() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let names = makeSchemaQualifiedName(prefix: "usp_tds")
        
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(names.bare)', 'P') IS NOT NULL DROP PROCEDURE \(names.bracketed);"), timeout: TIMEOUT, description: "drop existing proc")
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
        _ = try waitForResult(conn.query(createProc), timeout: TIMEOUT, description: "create stored procedure")
        defer {
            _ = try? waitForResult(conn.query("DROP PROCEDURE \(names.bracketed);"), timeout: TIMEOUT, description: "drop proc")
        }
        
        let execSql = """
        DECLARE @out INT;
        EXEC \(names.bracketed) @Input = 3, @Output = @out OUTPUT;
        SELECT OutputValue = @out;
        """
        let rows = try waitForResult(conn.query(execSql), timeout: TIMEOUT, description: "execute stored procedure")
        
        let databaseRows = rows.filter { $0.column("DatabaseName")?.string != nil }
        XCTAssertGreaterThanOrEqual(databaseRows.count, 1, "Expected stored procedure to return database rows")
        
        guard let outputRow = rows.last, let outputValue = outputRow.column("OutputValue")?.int else {
            XCTFail("Expected stored procedure to surface output value")
            return
        }
        XCTAssertEqual(outputValue, 8)
    }
    
    func testViewDefinitionRoundTrip() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let names = makeSchemaQualifiedName(prefix: "vw_tds")
        
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(names.bare)', 'V') IS NOT NULL DROP VIEW \(names.bracketed);"), timeout: TIMEOUT, description: "drop existing view")
        _ = try waitForResult(conn.query("SET QUOTED_IDENTIFIER ON;"), timeout: TIMEOUT, description: "set quoted identifier")
        let createView = """
        CREATE VIEW \(names.bracketed)
        AS
        SELECT TOP 5 name AS database_name FROM sys.databases ORDER BY name;
        """
        _ = try waitForResult(conn.query(createView), timeout: TIMEOUT, description: "create view")
        defer {
            _ = try? waitForResult(conn.query("DROP VIEW \(names.bracketed);"), timeout: TIMEOUT, description: "drop view")
        }
        
        let rows = try waitForResult(conn.query("SELECT COUNT(*) AS row_count FROM \(names.bracketed);"), timeout: TIMEOUT, description: "query view")
        guard let count = rows.first?.column("row_count")?.int else {
            XCTFail("Expected to count rows from view")
            return
        }
        XCTAssertGreaterThan(count, 0)
        
        let definitionRows = try waitForResult(
            conn.query("SELECT OBJECT_DEFINITION(OBJECT_ID(N'\(names.bare)')) AS definition;"),
            timeout: TIMEOUT,
            description: "read view definition"
        )
        let definition = definitionRows.first?.column("definition")?.string ?? ""
        XCTAssertTrue(definition.uppercased().contains("SELECT TOP 5"), "Expected view definition to contain SELECT statement")
    }
    
    func testInformationSchemaColumnsFetchesRows() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let sql = """
        SELECT TOP (1)
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        WHERE c.TABLE_SCHEMA = 'dbo'
        ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
        """
        let rows = try waitForResult(conn.query(sql), timeout: 60, description: "fetch information schema columns")
        XCTAssertFalse(rows.isEmpty, "Expected INFORMATION_SCHEMA.COLUMNS to return rows for dbo schema")
    }
    
    func testInformationSchemaColumnCount() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let sql = "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo';"
        let rows = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: "count information schema columns")
        guard let count = rows.first?.column("cnt")?.int else {
            XCTFail("Expected to read column count from INFORMATION_SCHEMA.COLUMNS")
            return
        }
        XCTAssertGreaterThan(count, 0)
    }

    func testColumnPropertyIsComputedSingle() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let sql = """
        SELECT c.is_computed
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(N'dbo.MSreplication_options')
          AND c.name = N'value';
        """
        let rows = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: "columnproperty iscomputed")
        XCTAssertEqual(rows.count, 1)
        _ = rows.first?.column("is_computed")?.int
    }

    func testSysCatalogColumnFlags() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let table = makeSchemaQualifiedName(prefix: "meta_flags")
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop meta table")
        defer { _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop meta table") }

        let create = """
        CREATE TABLE \(table.bracketed) (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            Name NVARCHAR(50) NULL,
            Computed AS Id + 1
        );
        """
        _ = try waitForResult(conn.query(create), timeout: TIMEOUT, description: "create meta table")

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
        let rows = try waitForResult(conn.query(metadataSql), timeout: TIMEOUT, description: "fetch sys.columns metadata")
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
    }
    
    func testMetadataClientColumnListing() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let metadata = SQLServerMetadataClient(connection: conn)
        let databases = try waitForResult(metadata.listDatabases(), timeout: TIMEOUT, description: "metadata list databases")
        XCTAssertFalse(databases.isEmpty, "Expected at least one database")

        let columns = try waitForResult(metadata.listColumns(schema: "dbo", table: "MSreplication_options"), timeout: TIMEOUT, description: "metadata list columns")
        XCTAssertEqual(columns.count, 6, "Expected known system table to expose columns")

        guard let valueColumn = columns.first(where: { $0.name == "value" }) else {
            XCTFail("Expected value column metadata")
            return
        }
        XCTAssertEqual(valueColumn.typeName.lowercased(), "bit")
        XCTAssertEqual(valueColumn.isNullable, false)
        XCTAssertEqual(valueColumn.isIdentity, false)
        XCTAssertEqual(valueColumn.isComputed, false)

        guard let nameColumn = columns.first(where: { $0.name == "optname" }) else {
            XCTFail("Expected optname column metadata")
            return
        }
        XCTAssertEqual(nameColumn.typeName.lowercased(), "sysname")
        XCTAssertEqual(nameColumn.isNullable, false)
    }
    
    func testConnectionPoolReusesConnections() throws {
        let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? loopGroup.syncShutdownGracefully() }

        var configuration = makeSQLServerClientConfiguration()
        configuration.poolConfiguration.maximumConcurrentConnections = 1
        configuration.poolConfiguration.minimumIdleConnections = 1

        let client = try waitForResult(
            SQLServerClient.connect(configuration: configuration, eventLoopGroupProvider: .shared(loopGroup)),
            timeout: TIMEOUT,
            description: "connect pooled client"
        )
        defer { _ = try? waitForResult(client.shutdownGracefully(), timeout: TIMEOUT, description: "shutdown pooled client") }

        func fetchSpid() throws -> Int {
            let rows = try waitForResult(
                client.withConnection { connection in
                    connection.query("SELECT @@SPID AS spid;")
                },
                timeout: TIMEOUT,
                description: "fetch spid"
            )
            return rows.first?.column("spid")?.int ?? -1
        }

        let firstSpid = try fetchSpid()
        let secondSpid = try fetchSpid()

        XCTAssertEqual(firstSpid, secondSpid, "Expected pooled client to reuse the same underlying connection when max concurrency is 1")
    }
    
    func testInformationSchemaBasicColumnsFetch() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        let spidRows = try waitForResult(conn.query("SELECT @@SPID AS spid;"), timeout: TIMEOUT, description: "fetch spid")
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
        let future = conn.query(sql)
        Thread.sleep(forTimeInterval: 5)
        let monitorConn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect monitor")
        defer { _ = try? waitForResult(monitorConn.close(), timeout: TIMEOUT, description: "close monitor") }
        let monitorSql = "SELECT status, wait_type, command, cpu_time, total_elapsed_time FROM sys.dm_exec_requests WHERE session_id = \(spid);"
        if let waits = try? waitForResult(monitorConn.query(monitorSql), timeout: TIMEOUT, description: "monitor waits"), let waitRow = waits.first {
            print("Monitor status: \(waitRow)")
        }
        let rows = try waitForResult(future, timeout: 30, description: "fetch basic information schema columns")
        XCTAssertEqual(rows.count, 10)
    }

    func testSchemaVersioningDetectsDefinitionChange() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let table = makeSchemaQualifiedName(prefix: "tbl_version")
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop existing versioned table")
        let createTable = """
        CREATE TABLE \(table.bracketed) (
            Id INT NOT NULL PRIMARY KEY,
            Name NVARCHAR(100) NOT NULL
        );
        """
        _ = try waitForResult(conn.query(createTable), timeout: TIMEOUT, description: "create versioned table")
        defer {
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop versioned table")
        }
        
        func schemaSignature() throws -> String {
            let sql = """
            SELECT signature = CONVERT(VARCHAR(64), HASHBYTES('SHA2_256',
                STRING_AGG(CONCAT_WS('|', c.column_id, c.name, t.name, c.max_length, c.precision, c.scale, c.is_nullable), ';')
                    WITHIN GROUP (ORDER BY c.column_id)
            ))
            FROM sys.columns AS c
            JOIN sys.types AS t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID(N'\(table.bare)');
            """
            let rows = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: "read schema signature")
            return rows.first?.column("signature")?.string ?? ""
        }
        
        let baselineSignature = try schemaSignature()
        XCTAssertFalse(baselineSignature.isEmpty, "Expected baseline schema signature")
        
        _ = try waitForResult(conn.query("ALTER TABLE \(table.bracketed) ADD ModifiedAt DATETIME2 NULL;"), timeout: TIMEOUT, description: "alter table add column")
        let alteredSignature = try schemaSignature()
        XCTAssertFalse(alteredSignature.isEmpty, "Expected altered schema signature")
        XCTAssertNotEqual(baselineSignature, alteredSignature, "Schema signature should change after altering table definition")
        
        _ = try waitForResult(conn.query("ALTER TABLE \(table.bracketed) DROP COLUMN ModifiedAt;"), timeout: TIMEOUT, description: "revert schema change")
        let revertedSignature = try schemaSignature()
        XCTAssertEqual(baselineSignature, revertedSignature, "Reverted schema should match baseline signature")
    }
    
    func testScalarAndTableValuedFunctions() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let scalar = makeSchemaQualifiedName(prefix: "fn_tds_scalar")
        let tvf = makeSchemaQualifiedName(prefix: "fn_tds_table")
        
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(scalar.bare)', 'FN') IS NOT NULL DROP FUNCTION \(scalar.bracketed);"), timeout: TIMEOUT, description: "drop scalar function")
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(tvf.bare)', 'IF') IS NOT NULL DROP FUNCTION \(tvf.bracketed);"), timeout: TIMEOUT, description: "drop table function")
        defer {
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(scalar.bare)', 'FN') IS NOT NULL DROP FUNCTION \(scalar.bracketed);"), timeout: TIMEOUT, description: "drop scalar function")
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(tvf.bare)', 'IF') IS NOT NULL DROP FUNCTION \(tvf.bracketed);"), timeout: TIMEOUT, description: "drop table function")
        }
        
        let createScalar = """
        CREATE FUNCTION \(scalar.bracketed) (@input NVARCHAR(100))
        RETURNS NVARCHAR(200)
        AS
        BEGIN
            RETURN CONCAT(@input, N'_suffix');
        END;
        """
        _ = try waitForResult(conn.query(createScalar), timeout: TIMEOUT, description: "create scalar function")
        
        let createTableFunc = """
        CREATE FUNCTION \(tvf.bracketed) (@top INT)
        RETURNS TABLE
        AS
        RETURN SELECT TOP (@top) database_id, name FROM sys.databases ORDER BY name;
        """
        _ = try waitForResult(conn.query(createTableFunc), timeout: TIMEOUT, description: "create table function")
        
        let scalarRows = try waitForResult(conn.query("SELECT \(scalar.bare)(N'prefix') AS value;"), timeout: TIMEOUT, description: "invoke scalar function")
        XCTAssertEqual(scalarRows.first?.column("value")?.string, "prefix_suffix")
        
        let tableRows = try waitForResult(conn.query("SELECT COUNT(*) AS cnt FROM \(tvf.bracketed)(2);"), timeout: TIMEOUT, description: "invoke table function")
        XCTAssertEqual(tableRows.first?.column("cnt")?.int, 2)
        
        let alterScalar = """
        ALTER FUNCTION \(scalar.bracketed) (@input NVARCHAR(100))
        RETURNS NVARCHAR(200)
        AS
        BEGIN
            RETURN CONCAT(N'new_', @input);
        END;
        """
        _ = try waitForResult(conn.query(alterScalar), timeout: TIMEOUT, description: "alter scalar function")
        let alteredRows = try waitForResult(conn.query("SELECT \(scalar.bare)(N'value') AS value;"), timeout: TIMEOUT, description: "invoke altered scalar function")
        XCTAssertEqual(alteredRows.first?.column("value")?.string, "new_value")
    }
    
    func testDmlTriggerLifecycle() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let base = makeSchemaQualifiedName(prefix: "tbl_tds_base")
        let audit = makeSchemaQualifiedName(prefix: "tbl_tds_audit")
        let trigger = makeSchemaQualifiedName(prefix: "trg_tds_insert")
        
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(trigger.bare)', 'TR') IS NOT NULL DROP TRIGGER \(trigger.bracketed);"), timeout: TIMEOUT, description: "drop trigger")
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(audit.bare)', 'U') IS NOT NULL DROP TABLE \(audit.bracketed);"), timeout: TIMEOUT, description: "drop audit table")
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(base.bare)', 'U') IS NOT NULL DROP TABLE \(base.bracketed);"), timeout: TIMEOUT, description: "drop base table")
        defer {
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(trigger.bare)', 'TR') IS NOT NULL DROP TRIGGER \(trigger.bracketed);"), timeout: TIMEOUT, description: "drop trigger")
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(audit.bare)', 'U') IS NOT NULL DROP TABLE \(audit.bracketed);"), timeout: TIMEOUT, description: "drop audit table")
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(base.bare)', 'U') IS NOT NULL DROP TABLE \(base.bracketed);"), timeout: TIMEOUT, description: "drop base table")
        }
        
        _ = try waitForResult(conn.query("CREATE TABLE \(base.bracketed) (id INT PRIMARY KEY, description NVARCHAR(100));"), timeout: TIMEOUT, description: "create base table")
        _ = try waitForResult(conn.query("CREATE TABLE \(audit.bracketed) (id INT, description NVARCHAR(100));"), timeout: TIMEOUT, description: "create audit table")
        
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
        _ = try waitForResult(conn.query(createTrigger), timeout: TIMEOUT, description: "create trigger")
        
        _ = try waitForResult(conn.query("INSERT INTO \(base.bracketed) (id, description) VALUES (42, N'answer');"), timeout: TIMEOUT, description: "insert base row")
        
        let auditRows = try waitForResult(conn.query("SELECT description FROM \(audit.bracketed) WHERE id = 42;"), timeout: TIMEOUT, description: "query audit table")
        XCTAssertEqual(auditRows.first?.column("description")?.string, "answer")
    }
    
    func testSynonymResolvesToSource() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let table = makeSchemaQualifiedName(prefix: "tbl_syn_src")
        let synonym = makeSchemaQualifiedName(prefix: "syn_tds")
        
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(synonym.bare)', 'SN') IS NOT NULL DROP SYNONYM \(synonym.bracketed);"), timeout: TIMEOUT, description: "drop synonym")
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop synonym source")
        defer {
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(synonym.bare)', 'SN') IS NOT NULL DROP SYNONYM \(synonym.bracketed);"), timeout: TIMEOUT, description: "drop synonym")
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop synonym source")
        }
        
        _ = try waitForResult(conn.query("CREATE TABLE \(table.bracketed) (name NVARCHAR(100));"), timeout: TIMEOUT, description: "create synonym source")
        _ = try waitForResult(conn.query("INSERT INTO \(table.bracketed) VALUES (N'alpha'), (N'beta');"), timeout: TIMEOUT, description: "seed synonym source")
        _ = try waitForResult(conn.query("CREATE SYNONYM \(synonym.bracketed) FOR \(table.bare);"), timeout: TIMEOUT, description: "create synonym")
        
        let rows = try waitForResult(conn.query("SELECT COUNT(*) AS cnt FROM \(synonym.bracketed);"), timeout: TIMEOUT, description: "query via synonym")
        XCTAssertEqual(rows.first?.column("cnt")?.int, 2)
    }
    
    func testPermissionGrantAndRevokeLifecycle() throws {
        guard env("TDS_ENABLE_PERMISSIONS_TESTS") == "1" else {
            throw XCTSkip("Skipping permissions tests. Set TDS_ENABLE_PERMISSIONS_TESTS=1 to enable.")
        }
        
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        let table = makeSchemaQualifiedName(prefix: "perm_table")
        let userName = "tds_user_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let bracketedUser = "[\(userName)]"
        
        _ = try waitForResult(conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop existing permissions table")
        _ = try waitForResult(conn.query("IF USER_ID(N'\(userName)') IS NOT NULL DROP USER \(bracketedUser);"), timeout: TIMEOUT, description: "drop existing database user")
        
        let createTable = """
        CREATE TABLE \(table.bracketed) (
            Id INT NOT NULL PRIMARY KEY,
            Payload NVARCHAR(50) NOT NULL
        );
        INSERT INTO \(table.bracketed) (Id, Payload) VALUES (1, N'row');
        """
        _ = try waitForResult(conn.query(createTable), timeout: TIMEOUT, description: "create permissions table")
        defer {
            _ = try? waitForResult(conn.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"), timeout: TIMEOUT, description: "drop permissions table")
        }
        
        _ = try waitForResult(conn.query("CREATE USER \(bracketedUser) WITHOUT LOGIN;"), timeout: TIMEOUT, description: "create database user")
        defer {
            _ = try? waitForResult(conn.query("IF USER_ID(N'\(userName)') IS NOT NULL DROP USER \(bracketedUser);"), timeout: TIMEOUT, description: "drop database user")
        }
        
        func probeAccess() throws -> String {
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
            let rows = try waitForResult(conn.query(script), timeout: TIMEOUT, description: "probe permissions")
            return rows.last?.column("outcome")?.string ?? ""
        }
        
        XCTAssertEqual(try probeAccess(), "denied", "User should not have SELECT permission before grant")
        
        _ = try waitForResult(conn.query("GRANT SELECT ON \(table.bracketed) TO \(bracketedUser);"), timeout: TIMEOUT, description: "grant select permission")
        XCTAssertEqual(try probeAccess(), "granted", "User should have SELECT permission after grant")
        
        _ = try waitForResult(conn.query("REVOKE SELECT ON \(table.bracketed) FROM \(bracketedUser);"), timeout: TIMEOUT, description: "revoke select permission")
        XCTAssertEqual(try probeAccess(), "denied", "User should lose SELECT permission after revoke")
    }

    func testSqlAgentJobLifecycle() throws {
        guard env("TDS_ENABLE_AGENT_TESTS") == "1" else {
            throw XCTSkip("Skipping SQL Agent tests. Set TDS_ENABLE_AGENT_TESTS=1 to enable.")
        }

        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let jobName = "tds_job_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let stepName = "step_main"

        func runSql(_ sql: String, description: String) throws {
            _ = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: description)
        }

        let agentStateRows = try waitForResult(conn.query("""
            SELECT
                is_enabled = CAST(ISNULL(SERVERPROPERTY('IsSqlAgentEnabled'), 0) AS INT),
                is_running = COALESCE((
                    SELECT TOP (1)
                        CASE WHEN status_desc = 'Running' THEN 1 ELSE 0 END
                    FROM sys.dm_server_services
                    WHERE servicename LIKE 'SQL Server Agent%'
                ), 0)
        """), timeout: TIMEOUT, description: "query agent state")

        func normalizedAgentState() -> (enabled: Int, running: Int) {
            let enabled = agentStateRows.first?.column("is_enabled")?.int ?? 0
            let running = agentStateRows.first?.column("is_running")?.int ?? 0
            return (enabled, running)
        }

        var state = normalizedAgentState()

        if state.enabled == 0 && state.running == 1 {
            // Attempt to enable Agent XPs (required for job execution) when the service is running.
            try runSql("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;", description: "enable advanced options")
            try runSql("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;", description: "enable Agent XPs")

            let refreshed = try waitForResult(conn.query("""
                SELECT
                    is_enabled = CAST(ISNULL(SERVERPROPERTY('IsSqlAgentEnabled'), 0) AS INT),
                    is_running = COALESCE((
                        SELECT TOP (1)
                            CASE WHEN status_desc = 'Running' THEN 1 ELSE 0 END
                        FROM sys.dm_server_services
                        WHERE servicename LIKE 'SQL Server Agent%'
                    ), 0)
            """), timeout: TIMEOUT, description: "re-check agent state")
            state = (
                enabled: refreshed.first?.column("is_enabled")?.int ?? 0,
                running: refreshed.first?.column("is_running")?.int ?? 0
            )
        }

        if state.running == 0 {
            throw XCTSkip("Not applicable: SQL Server Agent service not running on target instance")
        }

        let createJob = """
        EXEC msdb.dbo.sp_add_job
            @job_name = N'\(jobName)',
            @enabled = 1,
            @description = N'tds-nio integration test job';
        """
        try runSql(createJob, description: "create agent job")

        defer {
            _ = try? waitForResult(conn.query("EXEC msdb.dbo.sp_delete_job @job_name = N'\(jobName)';"), timeout: TIMEOUT, description: "cleanup job")
        }

        let addStep = """
        EXEC msdb.dbo.sp_add_jobstep
            @job_name = N'\(jobName)',
            @step_name = N'\(stepName)',
            @subsystem = N'TSQL',
            @command = N'SET NOCOUNT ON; INSERT INTO tempdb.dbo.tds_agent_marker DEFAULT VALUES;',
            @database_name = N'master';
        """
        try runSql(addStep, description: "add job step")

        try runSql("EXEC msdb.dbo.sp_add_jobserver @job_name = N'\(jobName)';", description: "attach job server")

        try runSql("IF OBJECT_ID('tempdb.dbo.tds_agent_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_agent_marker;", description: "drop agent marker table")
        try runSql("CREATE TABLE tempdb.dbo.tds_agent_marker (id INT IDENTITY(1,1) PRIMARY KEY);", description: "create agent marker table")
        defer {
            _ = try? waitForResult(conn.query("IF OBJECT_ID('tempdb.dbo.tds_agent_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_agent_marker;"), timeout: TIMEOUT, description: "cleanup agent marker table")
        }

        do {
            try runSql("EXEC msdb.dbo.sp_start_job @job_name = N'\(jobName)';", description: "start agent job")
        } catch {
            if let error = error as? TDSError, case .protocolError(let message) = error, message.contains("SQLSERVERAGENT") {
                throw XCTSkip("Not applicable: SQL Server Agent refused to start job (\(message))")
            }
            throw error
        }

        func fetchJobStatus() throws -> (status: Int, step: Int, message: String)? {
            let sql = """
            SELECT TOP (1)
                run_status,
                step_id,
                message
            FROM msdb.dbo.sysjobhistory AS h
            INNER JOIN msdb.dbo.sysjobs AS j ON h.job_id = j.job_id
            WHERE j.name = N'\(jobName)'
            ORDER BY h.instance_id DESC;
            """
            let rows = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: "fetch job history")
            guard
                let row = rows.first,
                let status = row.column("run_status")?.int,
                let step = row.column("step_id")?.int,
                let message = row.column("message")?.string
            else {
                return nil
            }
            return (status, step, message)
        }

        func currentAgentRunning() throws -> Bool {
            let rows = try waitForResult(conn.query("""
                SELECT running = COALESCE((
                    SELECT TOP (1)
                        CASE WHEN status_desc = 'Running' THEN 1 ELSE 0 END
                    FROM sys.dm_server_services
                    WHERE servicename LIKE 'SQL Server Agent%'
                ), 0)
            """), timeout: TIMEOUT, description: "check agent status")
            return (rows.first?.column("running")?.int ?? 0) == 1
        }

        var observedStatus: (status: Int, step: Int, message: String)?
        let deadline = Date().addingTimeInterval(30)
        while Date() < deadline {
            observedStatus = try fetchJobStatus()
            if let status = observedStatus?.status, status != 4 { // 4 = in-progress
                // continue polling to ensure marker row lands even when job already completed.
            }
            Thread.sleep(forTimeInterval: 1)

            let markerRows = try waitForResult(conn.query("SELECT COUNT(*) AS cnt FROM tempdb.dbo.tds_agent_marker;"), timeout: TIMEOUT, description: "check marker rows")
            if (markerRows.first?.column("cnt")?.int ?? 0) >= 1 {
                observedStatus = observedStatus ?? (status: 1, step: 0, message: "Marker detected without explicit history")
                break
            }
        }

        guard let result = observedStatus else {
            if try !currentAgentRunning() {
                throw XCTSkip("Not applicable: SQL Server Agent stopped before job history was recorded")
            }
            let history = try fetchJobStatus()
            XCTFail("Agent job did not complete within allotted time. Last history: \(history?.message ?? "none")")
            return
        }

        if result.status != 1 {
            XCTFail("Agent job finished with status=\(result.status) (step \(result.step)). Message: \(result.message)")
        }

        let finalMarkerRows = try waitForResult(conn.query("SELECT COUNT(*) AS cnt FROM tempdb.dbo.tds_agent_marker;"), timeout: TIMEOUT, description: "verify agent step executed")
        XCTAssertEqual(finalMarkerRows.first?.column("cnt")?.int, 1, "Expected agent job step to insert one marker row (history message: \(result.message))")
    }

    func testSQLServerClientMetadataFacade() throws {
        var configuration = makeSQLServerClientConfiguration()
        configuration.metadataConfiguration.includeSystemSchemas = true

        let client = try waitForResult(
            SQLServerClient.connect(configuration: configuration, eventLoopGroupProvider: .shared(group)),
            timeout: TIMEOUT,
            description: "connect SQLServerClient"
        )
        defer {
            _ = try? waitForResult(client.shutdownGracefully(), timeout: TIMEOUT, description: "shutdown SQLServerClient")
        }

        let databases = try waitForResult(client.listDatabases(on: eventLoop), timeout: TIMEOUT, description: "client list databases")
        XCTAssertTrue(
            databases.contains { $0.name.caseInsensitiveCompare(configuration.login.database) == .orderedSame },
            "Expected listDatabases to include \(configuration.login.database)"
        )

        let schemas = try waitForResult(client.listSchemas(on: eventLoop), timeout: TIMEOUT, description: "client list schemas")
        XCTAssertTrue(schemas.contains { $0.name == "dbo" }, "Expected dbo schema to be present")

        let table = makeSchemaQualifiedName(prefix: "client_meta")
        _ = try? waitForResult(
            client.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"),
            timeout: TIMEOUT,
            description: "drop existing client metadata table"
        )
        defer {
            _ = try? waitForResult(
                client.query("IF OBJECT_ID(N'\(table.bare)', 'U') IS NOT NULL DROP TABLE \(table.bracketed);"),
                timeout: TIMEOUT,
                description: "cleanup client metadata table"
            )
        }

        let createSQL = """
        CREATE TABLE \(table.bracketed) (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            Name NVARCHAR(50) NOT NULL,
            NameLength AS (LEN(Name))
        );
        """
        _ = try waitForResult(client.query(createSQL), timeout: TIMEOUT, description: "create client metadata table")

        let tables = try waitForResult(
            client.listTables(schema: "dbo", on: eventLoop),
            timeout: TIMEOUT,
            description: "client list tables"
        )
        XCTAssertTrue(tables.contains { $0.name == table.nameOnly }, "Expected listTables to include \(table.nameOnly)")

        let columns = try waitForResult(
            client.listColumns(schema: "dbo", table: table.nameOnly, on: eventLoop),
            timeout: TIMEOUT,
            description: "client list columns"
        )
        XCTAssertEqual(columns.count, 3, "Expected computed identity table to expose three columns")
        XCTAssertEqual(columns.first(where: { $0.name == "Id" })?.isIdentity, true, "Primary key should be flagged as identity")
        XCTAssertEqual(columns.first(where: { $0.name == "Name" })?.isNullable, false, "Non-null column should surface as not nullable")
        XCTAssertEqual(columns.first(where: { $0.name == "NameLength" })?.isComputed, true, "Computed column should surface isComputed flag")

        let cachedColumns = try waitForResult(
            client.listColumns(schema: "dbo", table: table.nameOnly, on: eventLoop),
            timeout: TIMEOUT,
            description: "client list columns cached"
        )
        XCTAssertEqual(cachedColumns.map(\.name), columns.map(\.name), "Cached column metadata should match initial fetch")

        let directConnection = try waitForResult(
            SQLServerConnection.connect(
                configuration: configuration.connection,
                on: eventLoop,
                logger: Logger(label: "tds.sqlserver.connection.test")
            ),
            timeout: TIMEOUT,
            description: "direct SQLServerConnection connect"
        )
        defer {
            _ = try? waitForResult(directConnection.close(), timeout: TIMEOUT, description: "direct connection close")
        }

        let directRows = try waitForResult(
            directConnection.query("SELECT TOP (1) name FROM sys.databases ORDER BY name;"),
            timeout: TIMEOUT,
            description: "direct SQLServerConnection query"
        )
        XCTAssertFalse(directRows.isEmpty, "Expected direct SQLServerConnection query to return at least one row")

        let spidRows = try waitForResult(
            client.withConnection(on: eventLoop) { connection in
                connection.query("SELECT @@SPID AS session_id;")
            },
            timeout: TIMEOUT,
            description: "client withConnection"
        )
        XCTAssertEqual(spidRows.count, 1)
        XCTAssertNotNil(spidRows.first?.column("session_id")?.int)
    }
}
