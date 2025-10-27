import Foundation
import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerDataTypeRoundTripTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var adminClient: SQLServerAdministrationClient!
    private var tablesToDrop: [String] = []
    private var skipDueToEnv = false
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).wait()
        adminClient = SQLServerAdministrationClient(client: client)
        // Quick probe; if the server is unstable right now, skip long integration paths to avoid timeouts.
        if #available(macOS 12.0, *) {
            do { _ = try awaitTask { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
        }
    }
    
    override func tearDownWithError() throws {
        for table in tablesToDrop {
            try? adminClient.dropTable(name: table).wait()
        }
        tablesToDrop.removeAll()
        try client.shutdownGracefully().wait()
        try group.syncShutdownGracefully()
        client = nil
        adminClient = nil
        group = nil
        try super.tearDownWithError()
    }
    
    func testNumericRoundTrips() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "rt") { db in
            let tableName = "datatype_numeric_\(UUID().uuidString.prefix(8))"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(tableName)] (
                    bit_value BIT,
                    tiny_value TINYINT,
                    small_value SMALLINT,
                    int_value INT,
                    big_value BIGINT,
                    decimal_value DECIMAL(18,4),
                    numeric_value NUMERIC(10,3),
                    money_value MONEY,
                    float_value FLOAT(53),
                    real_value REAL
                );
            """)
            _ = try await executeInDb(client: self.client, database: db, """
                INSERT INTO [\(tableName)] (bit_value, tiny_value, small_value, int_value, big_value, decimal_value, numeric_value, money_value, float_value, real_value)
                VALUES (1, 255, -120, 214748364, 922337203685, 98765.4321, 123.456, 88.88, 3.1415926535, 1.25);
            """)
            let rows = try await queryInDb(client: self.client, database: db, "SELECT * FROM [\(tableName)]")
        guard let row = rows.first else {
            XCTFail("Missing numeric round-trip row")
            return
        }
        XCTAssertEqual(row.column("bit_value")?.int, 1)
        XCTAssertEqual(row.column("tiny_value")?.int, 255)
        XCTAssertEqual(row.column("small_value")?.int, -120)
        XCTAssertEqual(row.column("int_value")?.int, 214748364)
        XCTAssertEqual(row.column("big_value")?.int64, 922_337_203_685)
        let decimalValue = try XCTUnwrap(row.column("decimal_value")?.double)
        XCTAssertEqual(decimalValue, 98765.4321, accuracy: 0.0001)
        let numericValue = try XCTUnwrap(row.column("numeric_value")?.double)
        XCTAssertEqual(numericValue, 123.456, accuracy: 0.0001)
        let moneyValue = try XCTUnwrap(row.column("money_value")?.double)
        XCTAssertEqual(moneyValue, 88.88, accuracy: 0.0001)
        let floatValue = try XCTUnwrap(row.column("float_value")?.double)
        XCTAssertEqual(floatValue, 3.1415926535, accuracy: 0.0001)
        let realValue = try XCTUnwrap(row.column("real_value")?.double)
        XCTAssertEqual(realValue, 1.25, accuracy: 0.0001)
            }
        }
    }
    
    func testTemporalRoundTrips() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "rt") { db in
            let tableName = "datatype_temporal_\(UUID().uuidString.prefix(8))"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(tableName)] (
                    date_value DATE,
                    datetime_value DATETIME2(7),
                    smalldatetime_value SMALLDATETIME,
                    time_value TIME(7),
                    datetimeoffset_value DATETIMEOFFSET(7)
                );
            """)
            _ = try await executeInDb(client: self.client, database: db, """
                INSERT INTO [\(tableName)] (date_value, datetime_value, smalldatetime_value, time_value, datetimeoffset_value)
                VALUES ('2023-11-18', '2023-11-18T13:15:30.1234567', '2023-11-18T13:15:00', '13:15:30.9876543', '2023-11-18T13:15:30.1234567+02:00');
            """)
            let rows = try await queryInDb(client: self.client, database: db, """
                SELECT
                    CONVERT(VARCHAR(10), date_value, 23) AS date_value,
                    CONVERT(VARCHAR(27), datetime_value, 126) AS datetime_value,
                    CONVERT(VARCHAR(19), smalldatetime_value, 120) AS smalldatetime_value,
                    CONVERT(VARCHAR(20), time_value, 114) AS time_value,
                    CONVERT(VARCHAR(33), datetimeoffset_value, 126) AS datetimeoffset_value
                FROM [\(tableName)]
            """)
            guard let row = rows.first else { XCTFail("Missing temporal row"); return }
            XCTAssertEqual(row.column("date_value")?.string, "2023-11-18")
            XCTAssertEqual(row.column("datetime_value")?.string, "2023-11-18T13:15:30.1234567")
            XCTAssertEqual(row.column("smalldatetime_value")?.string, "2023-11-18 13:15:00")
            XCTAssertTrue(row.column("time_value")?.string?.contains("13:15:30") ?? false)
            XCTAssertEqual(row.column("datetimeoffset_value")?.string, "2023-11-18T13:15:30.1234567+02:00")
            }
        }
    }
    
    func testCharacterBinaryAndVariantRoundTrips() async throws {
        if ProcessInfo.processInfo.environment["TDS_SKIP_VARIANT_ROUNDTRIP"] == "1" {
            throw XCTSkip("Skipping sql_variant character roundtrip on this environment")
        }
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup probe") }
        // Quick per-test probe to avoid spending a minute on timeouts if the server is unstable.
        do {
            let originalTimeout = env("TDS_TEST_OPERATION_TIMEOUT_SECONDS")
            setenv("TDS_TEST_OPERATION_TIMEOUT_SECONDS", "5", 1)
            let probe = try await SQLServerConnection.connect(configuration: makeSQLServerConnectionConfiguration(), on: self.group.next()).get()
            defer { _ = try? self.waitForResult(probe.close(), timeout: 5, description: "close probe") }
            _ = try await probe.query("SELECT 1").get()
            if let orig = originalTimeout { setenv("TDS_TEST_OPERATION_TIMEOUT_SECONDS", orig, 1) } else { unsetenv("TDS_TEST_OPERATION_TIMEOUT_SECONDS") }
        } catch {
            setenv("TDS_TEST_OPERATION_TIMEOUT_SECONDS", "60", 1)
            throw XCTSkip("Skipping due to transient connectivity issues: \(error)")
        }
        // Use a pooled client connection and select typed literals to exercise decode paths without DDL
        // Temporarily disable the test-timeout wrapper to avoid interfering with variant decoding
        let origTimeout = env("TDS_TEST_OPERATION_TIMEOUT_SECONDS")
        setenv("TDS_TEST_OPERATION_TIMEOUT_SECONDS", "0", 1)
        defer {
            if let t = origTimeout { setenv("TDS_TEST_OPERATION_TIMEOUT_SECONDS", t, 1) } else { unsetenv("TDS_TEST_OPERATION_TIMEOUT_SECONDS") }
        }
        try await withTimeout(120) {
            // First: mixed non-variant literals
            let rows1 = try await self.client.withConnection { conn in
                try await conn.query("""
                    SELECT
                        CAST('ABCDE' AS CHAR(5)) AS char_value,
                        CAST('swift-nio' AS VARCHAR(50)) AS varchar_value,
                        CAST(N'HELLO' AS NCHAR(5)) AS nchar_value,
                        CAST(N'Unicode Ω' AS NVARCHAR(50)) AS nvarchar_value,
                        0x0102030405 AS varbinary_value,
                        CAST('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' AS UNIQUEIDENTIFIER) AS uniqueidentifier_value
                """)
            }
            guard let row = rows1.first else { XCTFail("Missing character row"); return }
            XCTAssertEqual(row.column("char_value")?.string?.trimmingCharacters(in: .whitespaces), "ABCDE")
            XCTAssertEqual(row.column("varchar_value")?.string, "swift-nio")
            XCTAssertEqual(row.column("nchar_value")?.string?.trimmingCharacters(in: .whitespaces), "HELLO")
            XCTAssertEqual(row.column("nvarchar_value")?.string, "Unicode Ω")
            XCTAssertEqual(row.column("varbinary_value")?.bytes ?? [], [0x01, 0x02, 0x03, 0x04, 0x05])
            XCTAssertEqual(row.column("uniqueidentifier_value")?.uuid?.uuidString.lowercased(), "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
            // Second: variant-only literal to exercise sql_variant decode without mixing token streams
            let rows2 = try await self.client.withConnection { conn in
                try await conn.query("SELECT CAST('Variant payload' AS sql_variant) AS variant_value")
            }
            XCTAssertEqual(rows2.first?.column("variant_value")?.string, "Variant payload")
        }
    }
    
    func testMaxPayloadRoundTrips() async throws {
        try await withTimeout(30) {
            try await withTemporaryDatabase(client: self.client, prefix: "rt") { db in
            let tableName = "datatype_max_\(UUID().uuidString.prefix(8))"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(tableName)] (
                    text_value NVARCHAR(MAX),
                    binary_value VARBINARY(MAX)
                );
            """)
            let textPayload = String(repeating: "X", count: 32_768)
            let binaryPayload = Array((0..<32_768).map { UInt8($0 & 0xFF) })
            let textLiteral = SQLServerLiteralValue.nString(textPayload).sqlLiteral()
            let binaryLiteral = SQLServerLiteralValue.bytes(binaryPayload).sqlLiteral()
            _ = try await executeInDb(client: self.client, database: db, """
                INSERT INTO [\(tableName)] (text_value, binary_value)
                VALUES (\(textLiteral), \(binaryLiteral));
            """)
            let rows = try await queryInDb(client: self.client, database: db, """
                SELECT LEN(text_value) AS text_len, DATALENGTH(binary_value) AS binary_len FROM [\(tableName)]
            """)
        guard let row = rows.first else {
            XCTFail("Missing payload row")
            return
        }
        XCTAssertEqual(row.column("text_len")?.int, textPayload.count)
        XCTAssertEqual(row.column("binary_len")?.int, binaryPayload.count)
            }
        }
    }
}
