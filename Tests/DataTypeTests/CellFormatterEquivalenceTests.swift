import XCTest
import NIOCore
import SQLServerKit
import SQLServerKitTesting

/// Spooled cells must read exactly like live cells. For every SQL Server type,
/// formatting a cell from its stored bytes and its encoded `SQLServerCellType`
/// must equal `SQLServerRow.toStringArray()` for the same cell.
final class CellFormatterEquivalenceTests: StandardTestBase, @unchecked Sendable {
    private static let everyTypeSQL = """
        SELECT
            CAST(1 AS TINYINT) AS c_tinyint, CAST(-2 AS SMALLINT) AS c_smallint, CAST(-3 AS INT) AS c_int,
            CAST(9223372036854775807 AS BIGINT) AS c_bigint, CAST(1 AS BIT) AS c_bit,
            CAST(1.5 AS REAL) AS c_real, CAST(2.25 AS FLOAT) AS c_float,
            CAST(12345.6789 AS DECIMAL(18,4)) AS c_decimal, CAST(-0.5 AS NUMERIC(5,2)) AS c_numeric,
            CAST(922337203685477.5807 AS MONEY) AS c_money, CAST(-214748.3648 AS SMALLMONEY) AS c_smallmoney,
            CAST('2026-09-30' AS DATE) AS c_date, CAST('23:59:59.1234567' AS TIME(7)) AS c_time7,
            CAST('01:02:03' AS TIME(0)) AS c_time0, CAST('2026-09-30T12:34:56.123' AS DATETIME) AS c_datetime,
            CAST('2026-09-30T12:34:00' AS SMALLDATETIME) AS c_smalldatetime,
            CAST('2026-09-30T12:34:56.1234567' AS DATETIME2(7)) AS c_datetime2,
            CAST('2026-09-30T12:34:56.12' AS DATETIME2(2)) AS c_datetime2_2,
            CAST('2026-09-30T12:34:56.1234567+02:00' AS DATETIMEOFFSET(7)) AS c_dto,
            CAST(N'café' COLLATE Latin1_General_CI_AS AS VARCHAR(20)) AS c_varchar_latin1,
            CAST(N'Привет' COLLATE Cyrillic_General_CI_AS AS VARCHAR(20)) AS c_varchar_cyrillic,
            CAST('fixed' AS CHAR(8)) AS c_char, CAST(N'ünïcödé ✓' AS NVARCHAR(40)) AS c_nvarchar,
            CAST(N'n' AS NCHAR(3)) AS c_nchar, CAST(REPLICATE(CAST('x' AS VARCHAR(MAX)), 9000) AS VARCHAR(MAX)) AS c_varchar_max,
            CAST(REPLICATE(CAST(N'ÿ' AS NVARCHAR(MAX)), 5000) AS NVARCHAR(MAX)) AS c_nvarchar_max,
            CAST(0x0102FF AS VARBINARY(10)) AS c_varbinary, CAST(0x0A AS BINARY(4)) AS c_binary,
            CAST(REPLICATE(CAST(0xAB AS VARBINARY(MAX)), 9000) AS VARBINARY(MAX)) AS c_varbinary_max,
            CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS UNIQUEIDENTIFIER) AS c_guid,
            CAST('<a b="1">t</a>' AS XML) AS c_xml,
            CAST('/1/2/' AS HIERARCHYID) AS c_hierarchyid,
            geography::Point(47.65, -122.34, 4326) AS c_geography,
            CAST(CAST(42 AS INT) AS SQL_VARIANT) AS c_variant,
            CAST(NULL AS INT) AS c_null_int, CAST(NULL AS NVARCHAR(10)) AS c_null_nvarchar
        """

    func testEveryTypeFormatsIdenticallyFromStoredBytes() async throws {
        let rows = try await client.query(Self.everyTypeSQL)
        let row = try XCTUnwrap(rows.first)
        let live = row.toStringArray()
        let types = row.cellTypes
        let (buffers, _, _) = row.rawColumnBuffers()
        XCTAssertEqual(types.count, live.count)

        for index in live.indices {
            let type = types[index]
            let decodedType = try XCTUnwrap(SQLServerCellType(encoded: type.encoded), "encoded type must parse: \(type.encoded)")
            XCTAssertEqual(decodedType, type)
            guard let buffer = buffers[index] else {
                XCTAssertNil(live[index], "column \(index) is NULL live but has bytes stored")
                continue
            }
            let stored = Data(buffer.readableBytesView)
            let spooled = SQLServerCellFormatter.string(data: stored, type: decodedType)
            XCTAssertEqual(spooled, live[index], "column \(row.columns[index].name) (\(type.encoded)) differs when spooled")
        }
        // Exact values as SQL Server shows them (CONVERT style 121, SSMS).
        let expected: [String: String?] = [
            "c_money": "922337203685477.5807", "c_smallmoney": "-214748.3648",
            "c_date": "2026-09-30", "c_time7": "23:59:59.1234567", "c_time0": "01:02:03",
            "c_datetime": "2026-09-30 12:34:56.123", "c_smalldatetime": "2026-09-30 12:34:00",
            "c_datetime2": "2026-09-30 12:34:56.1234567", "c_datetime2_2": "2026-09-30 12:34:56.12",
            "c_dto": "2026-09-30 12:34:56.1234567 +02:00",
            "c_varchar_latin1": "café", "c_varchar_cyrillic": "Привет",
            "c_decimal": "12345.6789", "c_geography": "POINT(-122.34 47.65)",
            "c_guid": "6F9619FF-8B86-D011-B42D-00C04FC964FF", "c_null_int": nil,
        ]
        for (name, value) in expected {
            let index = try XCTUnwrap(row.columns.firstIndex { $0.name == name })
            XCTAssertEqual(live[index], value, name)
        }
    }

    func testStreamedColumnDescriptionsCarryTheRowCellTypes() async throws {
        let connection = try await client.connection()
        defer { Task { try? await connection.close() } }
        var described: [SQLServerCellType] = []
        var fromRow: [SQLServerCellType] = []
        for try await event in connection.streamQuery(Self.everyTypeSQL) {
            switch event {
            case .metadata(let columns): described = columns.map(\.cellType)
            case .row(let row): fromRow = row.cellTypes
            default: break
            }
        }
        XCTAssertFalse(described.isEmpty)
        XCTAssertEqual(described, fromRow)
    }

    func testForeignTypeDescriptorsAreRejected() {
        XCTAssertNil(SQLServerCellType(encoded: "INTEGER(23)"), "Postgres descriptors must not parse")
        XCTAssertNil(SQLServerCellType(encoded: "nvarchar"))
        XCTAssertNil(SQLServerCellType(encoded: "mssql1:zz:0:0:0::"))
    }
}
