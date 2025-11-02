@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerRoutineParameterMatrixTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
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
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        guard deepTestsEnabled() else { throw XCTSkip("Enable TDS_ENABLE_DEEP_SCENARIO_TESTS=1 to run deep matrix") }
        try await withTemporaryDatabase(client: self.client, prefix: "pmx") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let routineClient = SQLServerRoutineClient(client: dbClient)
                let typeClient = SQLServerTypeClient(client: dbClient)

                // Create a TVP type used by some procs using SQLServerTypeClient
                // Use unique name to avoid conflicts between test runs
                let uniqueId = UUID().uuidString.prefix(8)
                let tableType = UserDefinedTableTypeDefinition(
                    name: "ParamTableType_\(uniqueId)",
                    schema: "dbo",
                    columns: [
                        UserDefinedTableTypeColumn(
                            name: "id",
                            dataType: .int,
                            isNullable: false
                        )
                    ]
                )
                try await typeClient.createUserDefinedTableType(tableType)

                // Cover combinations: input, output, defaulted input, TVP readonly
                for (index, spec) in self.baseTypes.enumerated() {
                    let procName = "proc_matrix_\(uniqueId)_\(index)"

                    // Convert SQL type string to SQLDataType
                    let dataType = self.sqlDataType(from: spec.sql)
                    let defaultValue = spec.sql.starts(with: "NVARCHAR") ? "N'def'" : spec.expectType == "int" ? "1" : spec.expectType == "decimal" ? "0" : "DEFAULT"

                    // Create procedure using SQLServerRoutineClient
                    let parameters = [
                        ProcedureParameter(name: "In", dataType: dataType, direction: .input),
                        ProcedureParameter(name: "Out", dataType: dataType, direction: .output),
                        ProcedureParameter(name: "Def", dataType: dataType, defaultValue: defaultValue)
                    ]

                    let body = """
                    BEGIN
                        SET NOCOUNT ON;
                        SET @Out = @In;
                    END
                    """

                    try await routineClient.createStoredProcedure(
                        name: procName,
                        parameters: parameters,
                        body: body
                    )

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
                let tvpProc = "proc_with_tvp_\(uniqueId)"
                // TODO: This needs a future SQLServerRoutineClient API that supports table-valued parameters
                _ = try await executeInDb(client: self.client, database: db, """
                    CREATE PROCEDURE [dbo].[\(tvpProc)] @T dbo.ParamTableType_\(uniqueId) READONLY AS BEGIN SELECT COUNT(*) FROM @T; END
                """)
                let tvpParams = try await withDbConnection(client: self.client, database: db) { conn in
                    try await conn.listParameters(schema: "dbo", object: tvpProc).get()
                }
                guard let tvp = tvpParams.first(where: { $0.name.caseInsensitiveCompare("@T") == .orderedSame }) else {
                    XCTFail("Missing TVP param metadata")
                    return
                }
                XCTAssertTrue(tvp.isReadOnly)

                // Cleanup the created table type to avoid conflicts
                try? await typeClient.dropUserDefinedTableType(name: "ParamTableType_\(uniqueId)", schema: "dbo")
            }
        }
    }

    // Helper method to convert SQL type strings to SQLDataType
    private func sqlDataType(from sql: String) -> SQLDataType {
        switch sql.uppercased() {
        case "INT": return .int
        case "BIGINT": return .bigint
        case let s where s.starts(with: "DECIMAL"):
            if let regex = try? NSRegularExpression(pattern: #"DECIMAL\((\d+),(\d+)\)"#),
               let match = regex.firstMatch(in: s, range: NSRange(s.startIndex..., in: s)) {
                let precision = Int((s as NSString).substring(with: match.range(at: 1))) ?? 10
                let scale = Int((s as NSString).substring(with: match.range(at: 2))) ?? 0
                return .decimal(precision: UInt8(precision), scale: UInt8(scale))
            }
            return .decimal(precision: 10, scale: 2)
        case let s where s.starts(with: "NUMERIC"):
            if let regex = try? NSRegularExpression(pattern: #"NUMERIC\((\d+),(\d+)\)"#),
               let match = regex.firstMatch(in: s, range: NSRange(s.startIndex..., in: s)) {
                let precision = Int((s as NSString).substring(with: match.range(at: 1))) ?? 12
                let scale = Int((s as NSString).substring(with: match.range(at: 2))) ?? 3
                return .decimal(precision: UInt8(precision), scale: UInt8(scale))
            }
            return .decimal(precision: 12, scale: 3)
        case "BIT": return .bit
        case let s where s.starts(with: "FLOAT"):
            if let regex = try? NSRegularExpression(pattern: #"FLOAT\((\d+)\)"#),
               let match = regex.firstMatch(in: s, range: NSRange(s.startIndex..., in: s)) {
                let mantissa = Int((s as NSString).substring(with: match.range(at: 1))) ?? 53
                return .float(mantissa: UInt8(mantissa))
            }
            return .float(mantissa: 53)
        case "REAL": return .real
        case "MONEY": return .money
        case "UNIQUEIDENTIFIER": return .uniqueidentifier
        case let s where s.starts(with: "NVARCHAR"):
            if s.contains("MAX") { return .nvarchar(length: .max) }
            if let regex = try? NSRegularExpression(pattern: #"NVARCHAR\((\d+)\)"#),
               let match = regex.firstMatch(in: s, range: NSRange(s.startIndex..., in: s)) {
                let length = UInt16((s as NSString).substring(with: match.range(at: 1))) ?? 50
                return .nvarchar(length: .length(length))
            }
            return .nvarchar(length: .length(50))
        case let s where s.starts(with: "VARBINARY"):
            if s.contains("MAX") { return .varbinary(length: .max) }
            if let regex = try? NSRegularExpression(pattern: #"VARBINARY\((\d+)\)"#),
               let match = regex.firstMatch(in: s, range: NSRange(s.startIndex..., in: s)) {
                let length = UInt16((s as NSString).substring(with: match.range(at: 1))) ?? 50
                return .varbinary(length: .length(length))
            }
            return .varbinary(length: .length(50))
        case "DATE": return .date
        case let s where s.starts(with: "DATETIME2"):
            if let regex = try? NSRegularExpression(pattern: #"DATETIME2\((\d+)\)"#),
               let match = regex.firstMatch(in: s, range: NSRange(s.startIndex..., in: s)) {
                let precision = Int((s as NSString).substring(with: match.range(at: 1))) ?? 7
                return .datetime2(precision: UInt8(precision))
            }
            return .datetime2(precision: 7)
        case let s where s.starts(with: "TIME"):
            if let regex = try? NSRegularExpression(pattern: #"TIME\((\d+)\)"#),
               let match = regex.firstMatch(in: s, range: NSRange(s.startIndex..., in: s)) {
                let precision = Int((s as NSString).substring(with: match.range(at: 1))) ?? 7
                return .time(precision: UInt8(precision))
            }
            return .time(precision: 7)
        case let s where s.starts(with: "DATETIMEOFFSET"):
            if let regex = try? NSRegularExpression(pattern: #"DATETIMEOFFSET\((\d+)\)"#),
               let match = regex.firstMatch(in: s, range: NSRange(s.startIndex..., in: s)) {
                let precision = Int((s as NSString).substring(with: match.range(at: 1))) ?? 7
                return .datetimeoffset(precision: UInt8(precision))
            }
            return .datetimeoffset(precision: 7)
        case "XML": return .xml
        case "SQL_VARIANT": return .sql_variant
        default: return .int // fallback
        }
    }

    @available(macOS 12.0, *)
    func testFunctionParameterMatrix() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        guard deepTestsEnabled() else { throw XCTSkip("Enable TDS_ENABLE_DEEP_SCENARIO_TESTS=1 to run deep matrix") }
        try await withTemporaryDatabase(client: self.client, prefix: "fmx") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let routineClient = SQLServerRoutineClient(client: dbClient)

                // Scalar function with default
                let scalar = "f_scalar_\(UUID().uuidString.prefix(6))"

                let scalarParams = [
                    FunctionParameter(name: "A", dataType: .int),
                    FunctionParameter(name: "B", dataType: .nvarchar(length: .length(10)), defaultValue: "N'def'")
                ]

                let scalarBody = "BEGIN RETURN CONCAT(@A, '-', @B) END"

                try await routineClient.createFunction(
                    name: scalar,
                    parameters: scalarParams,
                    returnType: .nvarchar(length: .length(100)),
                    body: scalarBody
                )

                let sParams = try await withDbConnection(client: self.client, database: db) { conn in
                    try await conn.listParameters(schema: "dbo", object: scalar).get()
                }
                // Expect a return value plus two inputs
                XCTAssertTrue(sParams.contains(where: { $0.isReturnValue }))
                XCTAssertTrue(sParams.contains(where: { $0.name.caseInsensitiveCompare("@B") == .orderedSame && $0.hasDefaultValue }))

                // Inline TVF with two inputs
                let tvf = "f_inline_\(UUID().uuidString.prefix(6))"

                let tvfParams = [
                    FunctionParameter(name: "Start", dataType: .int),
                    FunctionParameter(name: "Finish", dataType: .int, defaultValue: "2")
                ]

                let tvfColumns = [
                    TableValuedFunctionColumn(name: "S", dataType: .int),
                    TableValuedFunctionColumn(name: "F", dataType: .int)
                ]

                let tvfBody = "SELECT @Start AS S, @Finish AS F"

                try await routineClient.createTableValuedFunction(
                    name: tvf,
                    parameters: tvfParams,
                    tableDefinition: tvfColumns,
                    body: tvfBody
                )

                let tParams = try await withDbConnection(client: self.client, database: db) { conn in
                    try await conn.listParameters(schema: "dbo", object: tvf).get()
                }
                XCTAssertEqual(tParams.filter { !$0.isReturnValue }.count, 2)
                XCTAssertTrue(tParams.contains(where: { $0.name.caseInsensitiveCompare("@Finish") == .orderedSame && $0.hasDefaultValue }))
            }
        }
    }
}
