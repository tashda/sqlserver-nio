@testable import SQLServerKit
import XCTest
import NIO

final class SQLServerRPCTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    func testRPCWithOutParamAndReturnCode() async throws {
        guard env("TDS_ENABLE_RPC_TESTS") == "1" else { throw XCTSkip("Enable TDS_ENABLE_RPC_TESTS=1 to run RPC tests") }
        try await withTemporaryDatabase(client: self.client, prefix: "rpc") { db in
            let procName = "usp_rpc_test_\(UUID().uuidString.prefix(6))"
            try await withDbClient(for: db, using: self.group) { dbClient in

            // Create a simple proc with OUT param and return code
            let create = """
            CREATE PROCEDURE [dbo].[\(procName)]
                @InVal INT,
                @OutVal INT OUTPUT
            AS
            BEGIN
                SET NOCOUNT ON;
                SET @OutVal = @InVal + 10;
                RETURN @InVal + 5;
            END
            """
            _ = try await dbClient.execute(create)

            // Call via RPC
            let pIn = SQLServerConnection.ProcedureParameter(name: "@InVal", value: TDSData(int32: 7), direction: .in)
            let pOut = SQLServerConnection.ProcedureParameter(name: "@OutVal", value: TDSData(int32: 0), direction: .out)
            let result = try await dbClient.withConnection { conn in
                try await conn.call(procedure: "dbo.\(procName)", parameters: [pIn, pOut]).get()
            }

            // Expect at least one return value (the OUT param), and potentially a return status
            XCTAssertTrue(result.returnValues.contains(where: { $0.name.caseInsensitiveCompare("@OutVal") == .orderedSame && $0.int == 17 }))
            }
        }
    }

    func testRPCWithDecimalParam() async throws {
        guard env("TDS_ENABLE_RPC_TESTS") == "1" else { throw XCTSkip("Enable TDS_ENABLE_RPC_TESTS=1 to run RPC tests") }
        try await withTemporaryDatabase(client: self.client, prefix: "rpcd") { db in
            let procName = "usp_rpc_dec_\(UUID().uuidString.prefix(6))"
            try await withDbClient(for: db, using: self.group) { dbClient in

            let create = """
            CREATE PROCEDURE [dbo].[\(procName)]
                @X DECIMAL(10,2),
                @Y DECIMAL(10,2) OUTPUT
            AS
            BEGIN
                SET NOCOUNT ON;
                SET @Y = @X * 2;
                RETURN 0;
            END
            """
            _ = try await dbClient.execute(create)

            let x = try TDSData(decimal: Decimal(string: "123.45")!, precision: 10, scale: 2)
            let px = SQLServerConnection.ProcedureParameter(name: "@X", value: x, direction: .in)
            // Provide TYPE_INFO by sending a zero DECIMAL for the OUT param
            let py = SQLServerConnection.ProcedureParameter(
                name: "@Y",
                value: try! TDSData(decimal: 0, precision: 10, scale: 2),
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
