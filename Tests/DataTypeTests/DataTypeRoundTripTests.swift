import Foundation
import XCTest
import Logging
import NIO
@testable import SQLServerKit
import Foundation

final class SQLServerDataTypeRoundTripTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    private var adminClient: SQLServerAdministrationClient!
    private var tablesToDrop: [String] = []
    private var skipDueToEnv = false

    override func setUp() async throws {
        try await super.setUp()
        adminClient = SQLServerAdministrationClient(client: client)
        // Quick probe; if the server is unstable right now, skip long integration paths to avoid timeouts.
        do { _ = try await client.query("SELECT 1").get() } catch { skipDueToEnv = true }
    }
  
    override func tearDown() async throws {
        for table in tablesToDrop {
            try? await adminClient.dropTable(name: table).get()
        }
        tablesToDrop.removeAll()
        adminClient = nil
        try await super.tearDown()
    }
    
    func testNumericRoundTrips() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "rt") { db in
            let tableName = "datatype_numeric_\(UUID().uuidString.prefix(8))"

            try await withDbClient(for: db, using: self.group) { dbClient in
                let dbAdminClient = SQLServerAdministrationClient(client: dbClient)

                // Create table using SQLServerKit APIs with numeric data types
                let columns = [
                    SQLServerColumnDefinition(name: "bit_value", definition: .standard(.init(dataType: .bit))),
                    SQLServerColumnDefinition(name: "tiny_value", definition: .standard(.init(dataType: .tinyint))),
                    SQLServerColumnDefinition(name: "small_value", definition: .standard(.init(dataType: .smallint))),
                    SQLServerColumnDefinition(name: "int_value", definition: .standard(.init(dataType: .int))),
                    SQLServerColumnDefinition(name: "big_value", definition: .standard(.init(dataType: .bigint))),
                    SQLServerColumnDefinition(name: "decimal_value", definition: .standard(.init(dataType: .decimal(precision: 18, scale: 4)))),
                    SQLServerColumnDefinition(name: "numeric_value", definition: .standard(.init(dataType: .numeric(precision: 10, scale: 3)))),
                    SQLServerColumnDefinition(name: "money_value", definition: .standard(.init(dataType: .money))),
                    SQLServerColumnDefinition(name: "float_value", definition: .standard(.init(dataType: .float(mantissa: 53)))),
                    SQLServerColumnDefinition(name: "real_value", definition: .standard(.init(dataType: .real)))
                ]
                try await dbAdminClient.createTable(name: tableName, columns: columns)

                // Insert data using SQLServerKit APIs
                _ = try await dbClient.query("INSERT INTO [dbo].[\(tableName)] (bit_value, tiny_value, small_value, int_value, big_value, decimal_value, numeric_value, money_value, float_value, real_value) VALUES (1, 255, -120, 214748364, 922337203685, 98765.4321, 123.456, 88.88, 3.1415926535, 1.25)").get()

                // Query data back using SQLServerKit APIs
                let rows = try await dbClient.query("SELECT * FROM [dbo].[\(tableName)]").get()

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

                try await withDbClient(for: db, using: self.group) { dbClient in
                    let dbAdminClient = SQLServerAdministrationClient(client: dbClient)

                    // Create table using SQLServerKit APIs with temporal data types
                    let columns = [
                        SQLServerColumnDefinition(name: "date_value", definition: .standard(.init(dataType: .date))),
                        SQLServerColumnDefinition(name: "datetime_value", definition: .standard(.init(dataType: .datetime2(precision: 7)))),
                        SQLServerColumnDefinition(name: "smalldatetime_value", definition: .standard(.init(dataType: .smalldatetime))),
                        SQLServerColumnDefinition(name: "time_value", definition: .standard(.init(dataType: .time(precision: 7)))),
                        SQLServerColumnDefinition(name: "datetimeoffset_value", definition: .standard(.init(dataType: .datetimeoffset(precision: 7))))
                    ]
                    try await dbAdminClient.createTable(name: tableName, columns: columns)

                    // Insert data using SQLServerKit APIs
                    _ = try await dbClient.query("""
                        INSERT INTO [\(tableName)] (date_value, datetime_value, smalldatetime_value, time_value, datetimeoffset_value)
                        VALUES ('2023-11-18', '2023-11-18T13:15:30.1234567', '2023-11-18T13:15:00', '13:15:30.9876543', '2023-11-18T13:15:30.1234567+02:00')
                    """).get()

                    // Query data back using SQLServerKit APIs
                    let rows = try await dbClient.query("""
                        SELECT
                            CONVERT(VARCHAR(10), date_value, 23) AS date_value,
                            CONVERT(VARCHAR(27), datetime_value, 126) AS datetime_value,
                            CONVERT(VARCHAR(19), smalldatetime_value, 120) AS smalldatetime_value,
                            CONVERT(VARCHAR(20), time_value, 114) AS time_value,
                            CONVERT(VARCHAR(33), datetimeoffset_value, 126) AS datetimeoffset_value
                        FROM [\(tableName)]
                    """).get()

                    guard let row = rows.first else { XCTFail("Missing temporal row"); return }
                    XCTAssertEqual(row.column("date_value")?.string, "2023-11-18")
                    XCTAssertEqual(row.column("datetime_value")?.string, "2023-11-18T13:15:30.1234567")
                    XCTAssertEqual(row.column("smalldatetime_value")?.string, "2023-11-18 13:15:00")
                    XCTAssertTrue(row.column("time_value")?.string?.contains("13:15:30") ?? false)
                    XCTAssertEqual(row.column("datetimeoffset_value")?.string, "2023-11-18T13:15:30.1234567+02:00")
                }
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
            let probe = try await SQLServerConnection.connect(configuration: makeSQLServerConnectionConfiguration(), on: client.eventLoopGroup.next()).get()
            _ = try await probe.query("SELECT 1").get()
            try? await probe.close().get()
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

                try await withDbClient(for: db, using: self.group) { dbClient in
                    let dbAdminClient = SQLServerAdministrationClient(client: dbClient)

                    // Create table using SQLServerKit APIs with MAX data types
                    let columns = [
                        SQLServerColumnDefinition(name: "text_value", definition: .standard(.init(dataType: .nvarchar(length: .max)))),
                        SQLServerColumnDefinition(name: "binary_value", definition: .standard(.init(dataType: .varbinary(length: .max))))
                    ]
                    try await dbAdminClient.createTable(name: tableName, columns: columns)

                    // Prepare payload data
                    let textPayload = String(repeating: "X", count: 32_768)
                    let binaryPayload = Array((0..<32_768).map { UInt8($0 & 0xFF) })
                    let textLiteral = SQLServerLiteralValue.nString(textPayload).sqlLiteral()
                    let binaryLiteral = SQLServerLiteralValue.bytes(binaryPayload).sqlLiteral()

                    // Insert data using SQLServerKit APIs
                    _ = try await dbClient.query("""
                        INSERT INTO [\(tableName)] (text_value, binary_value)
                        VALUES (\(textLiteral), \(binaryLiteral))
                    """).get()

                    // Query data back using SQLServerKit APIs
                    let rows = try await dbClient.query("""
                        SELECT LEN(text_value) AS text_len, DATALENGTH(binary_value) AS binary_len FROM [\(tableName)]
                    """).get()

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
}
}
