@testable import SQLServerKit
@testable import SQLServerTDS
import SQLServerKitTesting
import XCTest

final class SQLServerRPCTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    func testRPCWithOutParamAndReturnCode() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "rpc") { db in
            let procName = "usp_rpc_test_\(UUID().uuidString.prefix(6))"
            try await withDbClient(for: db) { dbClient in

            let routineClient = SQLServerRoutineClient(client: dbClient)
            try await routineClient.createStoredProcedure(
                name: procName,
                parameters: [
                    .init(name: "InVal", dataType: .int),
                    .init(name: "OutVal", dataType: .int, direction: .output)
                ],
                body: """
                BEGIN
                    SET NOCOUNT ON;
                    SET @OutVal = @InVal + 10;
                    RETURN @InVal + 5;
                END
                """
            )

            // Call via RPC
            let pIn = SQLServerConnection.ProcedureParameter(name: "@InVal", value: SQLServerValue(int32: 7), direction: .in)
            let pOut = SQLServerConnection.ProcedureParameter(name: "@OutVal", value: SQLServerValue(int32: 0), direction: .out)
            let result = try await dbClient.withConnection { conn in
                try await conn.call(procedure: "dbo.\(procName)", parameters: [pIn, pOut]).get()
            }

            // Expect at least one return value (the OUT param), and potentially a return status
            XCTAssertTrue(result.returnValues.contains(where: { $0.name.caseInsensitiveCompare("@OutVal") == .orderedSame && $0.int == 17 }))
            }
        }
    }

    func testRPCWithDecimalParam() async throws {
        #if !canImport(Darwin)
        throw XCTSkip("Decimal encoding differs on Linux Foundation — needs investigation")
        #endif
        try await withTemporaryDatabase(client: self.client, prefix: "rpcd") { db in
            let procName = "usp_rpc_dec_\(UUID().uuidString.prefix(6))"
            try await withDbClient(for: db) { dbClient in

            let routineClient = SQLServerRoutineClient(client: dbClient)
            try await routineClient.createStoredProcedure(
                name: procName,
                parameters: [
                    .init(name: "X", dataType: .decimal(precision: 10, scale: 2)),
                    .init(name: "Y", dataType: .decimal(precision: 10, scale: 2), direction: .output)
                ],
                body: """
                BEGIN
                    SET NOCOUNT ON;
                    SET @Y = @X * 2;
                    RETURN 0;
                END
                """
            )

            let x = try TDSData(decimal: Decimal(string: "123.45")!, precision: 10, scale: 2)
            let px = SQLServerConnection.ProcedureParameter(name: "@X", value: SQLServerValue(base: x), direction: .in)
            // Provide TYPE_INFO by sending a zero DECIMAL for the OUT param
            let py = SQLServerConnection.ProcedureParameter(
                name: "@Y",
                value: SQLServerValue(base: try! TDSData(decimal: 0, precision: 10, scale: 2)),
                direction: .out
            )
            let result = try await dbClient.withConnection { conn in
                try await conn.call(procedure: "dbo.\(procName)", parameters: [px, py]).get()
            }
            guard let out = result.returnValues.first(where: { $0.name.caseInsensitiveCompare("@Y") == .orderedSame }) else {
                XCTFail("Missing @Y OUT param"); return
            }
            // Compare as double to avoid decimal rounding differences
            guard let actual = out.double else {
                XCTFail("@Y OUT param did not decode as Double"); return
            }
            XCTAssertEqual(actual, 246.90, accuracy: 0.001)
            }
        }
    }
}
