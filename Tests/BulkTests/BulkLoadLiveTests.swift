import Foundation
import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

/// The TDS bulk load against a live server: values given as text (as imports give them) land in
/// every supported column type exactly as SQL Server would convert them.
final class BulkLoadLiveTests: XCTestCase, @unchecked Sendable {
    private var connection: SQLServerConnection!
    private var table = ""

    override func setUp() async throws {
        _ = isLoggingConfigured
        TestEnvironmentManager.loadEnvironmentVariables()
        var configuration = makeSQLServerConnectionConfiguration()
        configuration.packetSize = 512 // every batch spans many packets
        connection = try await SQLServerConnection.connect(configuration: configuration)
        table = "bulk_load_\(UUID().uuidString.prefix(8))"
    }

    override func tearDown() async throws {
        _ = try? await connection.execute("IF OBJECT_ID(N'dbo.\(table)') IS NOT NULL DROP TABLE dbo.[\(table)]")
        try? await connection.close()
    }

    private func copy(_ rows: [[SQLServerLiteralValue]], columns: [String], _ configure: (inout SQLServerBulkCopyOptions) -> Void = { _ in }) async throws -> SQLServerBulkCopySummary {
        var options = SQLServerBulkCopyOptions(table: table, columns: columns)
        configure(&options)
        return try await connection.bulkCopy(rows: rows.map(SQLServerBulkCopyRow.init(values:)), options: options)
    }

    func testEveryTypeFromText() async throws {
        _ = try await connection.execute("""
        CREATE TABLE dbo.[\(table)] (
            i int NOT NULL, ti tinyint, si smallint, bi bigint, b bit NOT NULL,
            d decimal(18, 4), n numeric(5, 0), m money, sm smallmoney, f float, r real,
            dt date, t time(3), dt2 datetime2(7), dto datetimeoffset(2), dtm datetime, sdt smalldatetime,
            g uniqueidentifier, c char(3), vc varchar(20) COLLATE Latin1_General_CI_AS,
            vu varchar(20) COLLATE Latin1_General_100_CI_AS_SC_UTF8, nv nvarchar(20), nmax nvarchar(max),
            vb varbinary(max), x xml
        )
        """)
        let columns = ["i", "ti", "si", "bi", "b", "d", "n", "m", "sm", "f", "r", "dt", "t", "dt2", "dto", "dtm", "sdt",
                       "g", "c", "vc", "vu", "nv", "nmax", "vb", "x"]
        let big = String(repeating: "Ærø-", count: 20_000)
        let row: [SQLServerLiteralValue] = [
            .string("42"), .string("255"), .string("-32768"), .string("9223372036854775807"), .string("true"),
            .string("-12345.67895"), .string("99999"), .string("922337203685477.5807"), .string("-214748.3648"),
            .string("3.5"), .string("0.25"),
            .string("2024-02-29"), .string("23:59:59.9994"), .string("2024-02-29 13:14:15.1234567"),
            .string("2024-03-01T00:30:00+01:00"), .string("2024-02-29 13:14:15.997"), .string("2024-02-29 13:14:29"),
            .string("6F9619FF-8B86-D011-B42D-00C04FC964FF"), .string("ab"), .string("Café"),
            .string("€ 😀"), .nString("Ærø 😀"), .nString(big),
            .string("0x00FF10"), .string("<a b=\"1\">tekst</a>"),
        ]
        let summary = try await copy([row], columns: columns)
        XCTAssertEqual(summary.method, .bulkLoad)
        XCTAssertEqual(summary.totalRows, 1)

        let result = try await connection.query("""
        SELECT i, ti, si, CONVERT(varchar(30), bi) AS bi, b, CONVERT(varchar(30), d) AS d, CONVERT(varchar(30), n) AS n,
               CONVERT(varchar(30), m, 2) AS m, CONVERT(varchar(30), sm, 2) AS sm, f, CONVERT(varchar(30), r) AS r,
               CONVERT(varchar(30), dt, 23) AS dt, CONVERT(varchar(30), t, 114) AS t, CONVERT(varchar(40), dt2, 121) AS dt2,
               CONVERT(varchar(40), dto) AS dto, CONVERT(varchar(40), dtm, 121) AS dtm, CONVERT(varchar(40), sdt, 120) AS sdt,
               CONVERT(varchar(40), g) AS g, c, vc, vu, nv, LEN(nmax) AS nmax_len, CASE WHEN nmax = N'\(big)' THEN 1 ELSE 0 END AS nmax_same,
               CONVERT(varchar(20), vb, 1) AS vb, CONVERT(nvarchar(100), x) AS x
        FROM dbo.[\(table)]
        """)
        let r = try XCTUnwrap(result.first)
        XCTAssertEqual(r.column("i")?.int, 42)
        XCTAssertEqual(r.column("ti")?.int, 255)
        XCTAssertEqual(r.column("si")?.int, -32768)
        XCTAssertEqual(r.column("bi")?.string, "9223372036854775807")
        XCTAssertEqual(r.column("b")?.bool, true)
        XCTAssertEqual(r.column("d")?.string, "-12345.6790") // rounded half away from zero
        XCTAssertEqual(r.column("n")?.string, "99999")
        XCTAssertEqual(r.column("m")?.string, "922337203685477.5807")
        XCTAssertEqual(r.column("sm")?.string, "-214748.3648")
        XCTAssertEqual(r.column("f")?.double, 3.5)
        XCTAssertEqual(r.column("r")?.string, "0.25")
        XCTAssertEqual(r.column("dt")?.string, "2024-02-29")
        XCTAssertEqual(r.column("t")?.string, "23:59:59.999")
        XCTAssertEqual(r.column("dt2")?.string, "2024-02-29 13:14:15.1234567")
        XCTAssertEqual(r.column("dto")?.string, "2024-03-01 00:30:00.00 +01:00")
        XCTAssertEqual(r.column("dtm")?.string, "2024-02-29 13:14:15.997")
        XCTAssertEqual(r.column("sdt")?.string, "2024-02-29 13:14:00")
        XCTAssertEqual(r.column("g")?.string, "6F9619FF-8B86-D011-B42D-00C04FC964FF")
        XCTAssertEqual(r.column("c")?.string, "ab ")
        XCTAssertEqual(r.column("vc")?.string, "Café")
        XCTAssertEqual(r.column("vu")?.string, "€ 😀")
        XCTAssertEqual(r.column("nv")?.string, "Ærø 😀")
        XCTAssertEqual(r.column("nmax_len")?.int, big.utf16.count)
        XCTAssertEqual(r.column("nmax_same")?.int, 1)
        XCTAssertEqual(r.column("vb")?.string, "0x00FF10")
        XCTAssertEqual(r.column("x")?.string, "<a b=\"1\">tekst</a>")
    }

    func testManyRowsInBatchesAndNulls() async throws {
        _ = try await connection.execute("CREATE TABLE dbo.[\(table)] (id int NOT NULL PRIMARY KEY, name nvarchar(50) NULL, note varchar(10) NULL DEFAULT 'dflt')")
        let rows: [[SQLServerLiteralValue]] = (1...5000).map { i in
            [.int(i), i.isMultiple(of: 7) ? .null : .nString("name \(i)"), .null]
        }
        let summary = try await copy(rows, columns: ["id", "name", "note"]) { $0.batchSize = 2000 }
        XCTAssertEqual(summary.method, .bulkLoad)
        XCTAssertEqual(summary.totalRows, 5000)
        XCTAssertEqual(summary.batchesExecuted, 3)
        let counts = try await connection.query("SELECT COUNT(*) AS n, COUNT(name) AS named, COUNT(note) AS noted, MAX(name) AS last FROM dbo.[\(table)]")
        XCTAssertEqual(counts.first?.column("n")?.int, 5000)
        XCTAssertEqual(counts.first?.column("named")?.int, 5000 - 5000 / 7)
        XCTAssertEqual(counts.first?.column("noted")?.int, 0, "KEEP_NULLS keeps NULL over the default")

        _ = try await connection.execute("TRUNCATE TABLE dbo.[\(table)]")
        _ = try await copy([[.int(1), .null, .null]], columns: ["id", "name", "note"]) { $0.keepNulls = false }
        let defaulted = try await connection.query("SELECT note FROM dbo.[\(table)]")
        XCTAssertEqual(defaulted.first?.column("note")?.string, "dflt")
    }

    func testTextTheClientCannotReadIsConvertedByTheServer() async throws {
        _ = try await connection.execute("CREATE TABLE dbo.[\(table)] (id int NOT NULL, booked date, price decimal(10, 2))")
        // us_english reads 12/31/2023 as month/day/year, as an INSERT would.
        let summary = try await copy(
            [[.string("1"), .string("12/31/2023"), .string("1.50")], [.string("2"), .string("20240229"), .string("2")]],
            columns: ["id", "booked", "price"]
        )
        XCTAssertEqual(summary.method, .bulkLoad)
        let rows = try await connection.query("SELECT CONVERT(varchar(10), booked, 23) AS booked, price FROM dbo.[\(table)] ORDER BY id")
        XCTAssertEqual(rows.map { $0.column("booked")?.string }, ["2023-12-31", "2024-02-29"])

        // Text neither side reads fails with the server's error, as an INSERT did.
        do {
            _ = try await copy([[.string("3"), .string("2024-01-01"), .string("1,50")]], columns: ["id", "booked", "price"])
            XCTFail("expected a conversion error")
        } catch let error as SQLServerError {
            XCTAssertTrue("\(error)".contains("converting"), "\(error)")
        }
        let count = try await connection.queryScalar("SELECT COUNT(*) FROM dbo.[\(table)]", as: Int.self)
        XCTAssertEqual(count, 2)
    }

    func testAValueThatIsNotTextAndDoesNotConvertWritesNothing() async throws {
        _ = try await connection.execute("CREATE TABLE dbo.[\(table)] (id int NOT NULL)")
        do {
            _ = try await copy([[.int(1)], [.bytes([1, 2])]], columns: ["id"]) { $0.batchSize = 1 }
            XCTFail("expected a conversion error")
        } catch let error as SQLServerBulkCopyError {
            XCTAssertEqual(error.localizedDescription, "Row 2, column id: '0x0102' is not a whole number.")
        }
        let count = try await connection.queryScalar("SELECT COUNT(*) FROM dbo.[\(table)]", as: Int.self)
        XCTAssertEqual(count, 0)
    }

    func testServerErrorsSurfaceAndTheConnectionStaysUsable() async throws {
        _ = try await connection.execute("CREATE TABLE dbo.[\(table)] (id int NOT NULL PRIMARY KEY, amount int CHECK (amount >= 0))")
        do {
            _ = try await copy([[.int(1), .int(-1)]], columns: ["id", "amount"])
            XCTFail("expected the CHECK constraint to fail")
        } catch let error as SQLServerError {
            XCTAssertTrue("\(error)".contains("CHECK"), "\(error)")
        }
        // Without CHECK_CONSTRAINTS the row goes in (and the constraint is marked untrusted).
        _ = try await copy([[.int(2), .int(-1)]], columns: ["id", "amount"]) { $0.checkConstraints = false }
        do {
            _ = try await copy([[.int(2), .int(5)]], columns: ["id", "amount"])
            XCTFail("expected a duplicate key")
        } catch let error as SQLServerError {
            XCTAssertTrue("\(error)".contains("PRIMARY KEY") || "\(error)".contains("duplicate"), "\(error)")
        }
        let count = try await connection.queryScalar("SELECT COUNT(*) FROM dbo.[\(table)]", as: Int.self)
        XCTAssertEqual(count, 1)
    }

    func testRollsBackWithTheTransaction() async throws {
        _ = try await connection.execute("CREATE TABLE dbo.[\(table)] (id int NOT NULL)")
        _ = try await connection.execute("BEGIN TRANSACTION")
        _ = try await copy([[.int(1)], [.int(2)]], columns: ["id"]) { $0.tableLock = true }
        let inside = try await connection.queryScalar("SELECT COUNT(*) FROM dbo.[\(table)]", as: Int.self)
        XCTAssertEqual(inside, 2)
        _ = try await connection.execute("ROLLBACK TRANSACTION")
        let after = try await connection.queryScalar("SELECT COUNT(*) FROM dbo.[\(table)]", as: Int.self)
        XCTAssertEqual(after, 0)
    }

    func testIdentityValuesAreKeptOnlyWhenAsked() async throws {
        _ = try await connection.execute("CREATE TABLE dbo.[\(table)] (id int IDENTITY(1, 1) NOT NULL, name nvarchar(10))")
        _ = try await copy([[.int(100), .nString("kept")]], columns: ["id", "name"]) { $0.identityInsert = true }
        _ = try await copy([[.nString("new")]], columns: ["name"])
        // Without identityInsert, a value for the identity column is left out and the server assigns one.
        let summary = try await copy([[.int(500), .nString("assigned")]], columns: ["id", "name"])
        XCTAssertEqual(summary.method, .bulkLoad)
        let ids = try await connection.query("SELECT id FROM dbo.[\(table)] ORDER BY id")
        XCTAssertEqual(ids.map { $0.column("id")?.int }, [100, 101, 102])
    }

    func testTemporaryTable() async throws {
        _ = try await connection.execute("CREATE TABLE #bulk (id int NOT NULL, name varchar(10) COLLATE Latin1_General_CI_AS)")
        let summary = try await connection.bulkCopy(
            rows: [SQLServerBulkCopyRow(values: [.int(1), .string("Café")])],
            options: SQLServerBulkCopyOptions(table: "#bulk", columns: ["id", "name"])
        )
        XCTAssertEqual(summary.method, .bulkLoad)
        let name = try await connection.queryScalar("SELECT name FROM #bulk", as: String.self)
        XCTAssertEqual(name, "Café")
    }

    func testColumnsBulkLoadCannotCarryFallBackToInsertStatements() async throws {
        _ = try await connection.execute("CREATE TABLE dbo.[\(table)] (id int NOT NULL, v sql_variant, shape geometry)")
        let summary = try await copy(
            [[.int(1), .variant(.int(7)), .geometry(wellKnownText: "POINT (1 2)", srid: 0)]],
            columns: ["id", "v", "shape"]
        )
        XCTAssertEqual(summary.method, .insertStatements)
        let text = try await connection.queryScalar("SELECT shape.STAsText() FROM dbo.[\(table)]", as: String.self)
        XCTAssertEqual(text, "POINT (1 2)")
    }
}
