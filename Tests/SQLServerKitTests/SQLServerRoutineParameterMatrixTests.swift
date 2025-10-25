@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerRoutineParameterMatrixTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let cfg = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: cfg, eventLoopGroupProvider: .shared(group)).wait()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    // Enable the deep matrix only when explicitly requested to keep CI fast
    private func deepTestsEnabled() -> Bool {
        return env("TDS_ENABLE_DEEP_SCENARIO_TESTS") == "1"
    }

    // A representative set of SQL types to exercise parameter metadata
    private struct ParamSpec { let sql: String; let expectType: String }
    private let baseTypes: [ParamSpec] = [
        .init(sql: "INT", expectType: "int"),
        .init(sql: "BIGINT", expectType: "bigint"),
        .init(sql: "DECIMAL(10,2)", expectType: "decimal"),
        .init(sql: "NUMERIC(12,3)", expectType: "numeric"),
        .init(sql: "BIT", expectType: "bit"),
        .init(sql: "FLOAT(53)", expectType: "float"),
        .init(sql: "REAL", expectType: "real"),
        .init(sql: "MONEY", expectType: "money"),
        .init(sql: "UNIQUEIDENTIFIER", expectType: "uniqueidentifier"),
        .init(sql: "NVARCHAR(50)", expectType: "nvarchar"),
        .init(sql: "VARBINARY(50)", expectType: "varbinary"),
        .init(sql: "DATE", expectType: "date"),
        .init(sql: "DATETIME2(3)", expectType: "datetime2"),
        .init(sql: "TIME(3)", expectType: "time"),
        .init(sql: "DATETIMEOFFSET(2)", expectType: "datetimeoffset"),
        .init(sql: "XML", expectType: "xml"),
        .init(sql: "SQL_VARIANT", expectType: "sql_variant"),
    ]

    @available(macOS 12.0, *)
    func testProcedureParameterMatrix() async throws {
        guard deepTestsEnabled() else { throw XCTSkip("Enable TDS_ENABLE_DEEP_SCENARIO_TESTS=1 to run deep matrix") }
        try await withTemporaryDatabase(client: self.client, prefix: "pmx") { db in
            // Create a TVP type used by some procs
            _ = try await executeInDb(client: self.client, database: db, "CREATE TYPE dbo.ParamTableType AS TABLE (id INT NOT NULL);")

            // Cover combinations: input, output, defaulted input, TVP readonly
            for (index, spec) in self.baseTypes.enumerated() {
                let procName = "proc_matrix_\(index)"
                let create = """
                CREATE PROCEDURE [dbo].[\(procName)]
                    @In \(spec.sql),
                    @Out \(spec.sql) OUTPUT,
                    @Def \(spec.sql) = \(spec.sql.starts(with: "NVARCHAR") ? "N'def'" : spec.expectType == "int" ? "1" : spec.expectType == "decimal" ? "0" : "DEFAULT")
                AS
                BEGIN
                    SET NOCOUNT ON;
                    SET @Out = @In;
                END
                """
                _ = try await executeInDb(client: self.client, database: db, create)

                let ps = try await withDbConnection(client: self.client, database: db) { conn in
                    try await conn.listParameters(schema: "dbo", object: procName).get()
                }
                // We expect exactly 3 parameters, no return value for procedures
                XCTAssertEqual(ps.filter { !$0.isReturnValue }.count, 3, "Expected 3 parameters for \(procName)")
                XCTAssertTrue(ps.contains(where: { $0.name.caseInsensitiveCompare("@In") == .orderedSame && $0.typeName.lowercased() == spec.expectType }))
                XCTAssertTrue(ps.contains(where: { $0.name.caseInsensitiveCompare("@Out") == .orderedSame && $0.isOutput }))
                XCTAssertTrue(ps.contains(where: { $0.name.caseInsensitiveCompare("@Def") == .orderedSame && $0.hasDefaultValue }))
            }

            // TVP case: READONLY parameter must be surfaced as readOnly
            let tvpProc = "proc_with_tvp"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE PROCEDURE [dbo].[\(tvpProc)] @T dbo.ParamTableType READONLY AS BEGIN SELECT COUNT(*) FROM @T; END
            """)
            let tvpParams = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listParameters(schema: "dbo", object: tvpProc).get()
            }
            guard let tvp = tvpParams.first(where: { $0.name.caseInsensitiveCompare("@T") == .orderedSame }) else {
                XCTFail("Missing TVP param metadata")
                return
            }
            XCTAssertTrue(tvp.isReadOnly)
        }
    }

    @available(macOS 12.0, *)
    func testFunctionParameterMatrix() async throws {
        guard deepTestsEnabled() else { throw XCTSkip("Enable TDS_ENABLE_DEEP_SCENARIO_TESTS=1 to run deep matrix") }
        try await withTemporaryDatabase(client: self.client, prefix: "fmx") { db in
            // Scalar function with default
            let scalar = "f_scalar_\(UUID().uuidString.prefix(6))"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE FUNCTION [dbo].[\(scalar)] (@A INT, @B NVARCHAR(10) = N'def') RETURNS NVARCHAR(100) AS
                BEGIN RETURN CONCAT(@A, '-', @B) END
            """)
            let sParams = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listParameters(schema: "dbo", object: scalar).get()
            }
            // Expect a return value plus two inputs
            XCTAssertTrue(sParams.contains(where: { $0.isReturnValue }))
            XCTAssertTrue(sParams.contains(where: { $0.name.caseInsensitiveCompare("@B") == .orderedSame && $0.hasDefaultValue }))

            // Inline TVF with two inputs
            let tvf = "f_inline_\(UUID().uuidString.prefix(6))"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE FUNCTION [dbo].[\(tvf)](@Start INT, @Finish INT = 2)
                RETURNS TABLE
                AS
                RETURN (SELECT @Start AS S, @Finish AS F);
            """)
            let tParams = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listParameters(schema: "dbo", object: tvf).get()
            }
            XCTAssertEqual(tParams.filter { !$0.isReturnValue }.count, 2)
            XCTAssertTrue(tParams.contains(where: { $0.name.caseInsensitiveCompare("@Finish") == .orderedSame && $0.hasDefaultValue }))
        }
    }
}
