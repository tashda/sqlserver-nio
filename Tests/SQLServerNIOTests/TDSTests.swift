import XCTest
import Logging
import NIOTestUtils
@testable import SQLServerNIO

final class SQLServerConnectionTests: XCTestCase {
    
    private var group: EventLoopGroup!
    
    private var eventLoop: EventLoop { self.group.next() }
    
    private let TIMEOUT: TimeInterval = 10
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }
    
    override func tearDownWithError() throws {
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }
    
    // MARK: Tests
    func testConnectAndClose() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        try waitForResult(conn.close(), timeout: TIMEOUT, description: "close")
    }

    func testRawSqlVersion() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        let rows = try waitForResult(conn.query("SELECT @@VERSION AS version"), timeout: TIMEOUT, description: "query @@VERSION")
        XCTAssertEqual(rows.count, 1)
        
        let version = rows[0].column("version")?.string
        let regex = try NSRegularExpression(pattern: sqlServerVersionPattern)
        XCTAssertEqual(regex.matches(version), true)
    }
    
    func testRawSqlGetDate() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        let rows = try waitForResult(conn.query("SELECT GETUTCDATE() AS timestamp"), timeout: TIMEOUT, description: "query GETUTCDATE")
        XCTAssertEqual(rows.count, 1)
        
        let date = rows[0].column("timestamp")?.date
        XCTAssertEqual(date != nil, true)
    }
    
    func testMultiStatementReturnsRows() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let sql = "SET FMTONLY OFF; SELECT name FROM sys.databases ORDER BY name;"
        let rows = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: "multi-statement query")
        
        XCTAssertGreaterThanOrEqual(rows.count, 1, "Expected at least one row from sys.databases")
    }
    
    func testStoredProcedureDatabases() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let rows = try waitForResult(conn.query("EXEC sp_databases;"), timeout: TIMEOUT, description: "execute sp_databases")
        XCTAssertEqual(rows.count, 5, "Expected system stored procedure to return 5 databases")
        
        guard let metadata = rows.first?.columnMetadata else {
            XCTFail("Expected column metadata for sp_databases result set")
            return
        }
        
        let columnNames = metadata.colData.map(\.colName)
        XCTAssertEqual(columnNames.count, 3, "Expected 3 columns from sp_databases")
        XCTAssertEqual(columnNames, ["DATABASE_NAME", "DATABASE_SIZE", "REMARKS"])
    }
    
    func testSystemDatabasesExposeTables() throws {
        let conn = try waitForResult(connectSQLServer(on: eventLoop), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }
        
        func assertTablesExist(in database: String) throws {
            let sql = "SELECT COUNT(*) AS table_count FROM \(database).sys.tables;"
            let rows = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: "list tables in \(database)")
            guard
                let countValue = rows.first?.column("table_count")?.int,
                countValue > 0
            else {
                XCTFail("Expected at least one table in \(database) database")
                return
            }
        }
        
        try assertTablesExist(in: "master")
        try assertTablesExist(in: "msdb")
    }
    
    func testRemoteTLSServer() throws {
        guard env("TDS_ENABLE_REMOTE_TLS_TEST") == "1" else {
            throw XCTSkip("Skipping remote TLS test. Set TDS_ENABLE_REMOTE_TLS_TEST=1 to enable.")
        }

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? elg.syncShutdownGracefully() }

        let configuration = SQLServerConnection.Configuration(
            hostname: "swift-tds.database.windows.net",
            port: 1433,
            login: .init(
                database: "swift-tds",
                authentication: .sqlPassword(username: "swift_tds_user", password: "RP9f7PVffK6U8b9ek@Q9eH-8")
            ),
            tlsConfiguration: .makeClientConfiguration()
        )

        let conn = try waitForResult(
            SQLServerConnection.connect(configuration: configuration, on: elg.next()),
            timeout: TIMEOUT,
            description: "connect remote"
        )

        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close remote") }
        
        let rows = try waitForResult(conn.query("SELECT @@VERSION AS version"), timeout: TIMEOUT, description: "remote @@VERSION")
        XCTAssertEqual(rows.count, 1)
        
        let version = rows[0].column("version")?.string
        let regex = try NSRegularExpression(pattern: sqlServerVersionPattern)
        XCTAssertEqual(regex.matches(version), true)
    }
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
        return handler
    }
    return true
}()
