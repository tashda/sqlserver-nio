import XCTest
import NIO
import Logging
@testable import SQLServerKit
import SQLServerKitTesting

/// Integration tests for stored procedure output parameters (RPC RETURNVALUE token path).
///
/// These tests exercise the full stack:
///   SQL Server → TDS RETURNVALUE token → TDSTokenParser → SQLServerConnection.call() → SQLServerReturnValue
final class OutputParameterTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    // MARK: - Basic output parameter types

    func testIntOutputParameter() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "outp") { db in
                try await withDbClient(for: db, using: self.group) { dbClient in
                    _ = try await dbClient.execute("""
                        CREATE PROCEDURE [dbo].[usp_DoubleInt]
                            @input INT,
                            @result INT OUTPUT
                        AS BEGIN
                            SET NOCOUNT ON;
                            SET @result = @input * 2;
                        END
                    """).get()

                    let result = try await dbClient.call(
                        procedure: "dbo.usp_DoubleInt",
                        parameters: [
                            .init(name: "input",  value: .init(int32: 21)),
                            .init(name: "result", value: .init(int32: 0), direction: .out)
                        ]
                    )

                    let rv = try XCTUnwrap(result.returnValues.first(where: { $0.name.caseInsensitiveCompare("@result") == .orderedSame }))
                    XCTAssertEqual(rv.int, 42)
                }
            }
        }
    }

    func testNVarCharOutputParameter() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "outp") { db in
                try await withDbClient(for: db, using: self.group) { dbClient in
                    _ = try await dbClient.execute("""
                        CREATE PROCEDURE [dbo].[usp_Greet]
                            @name NVARCHAR(50),
                            @greeting NVARCHAR(100) OUTPUT
                        AS BEGIN
                            SET NOCOUNT ON;
                            SET @greeting = N'Hello, ' + @name + N'!';
                        END
                    """).get()

                    let result = try await dbClient.call(
                        procedure: "dbo.usp_Greet",
                        parameters: [
                            .init(name: "name",     value: .init(string: "World")),
                            .init(name: "greeting", value: .init(string: ""), direction: .out)
                        ]
                    )

                    let rv = try XCTUnwrap(result.returnValues.first(where: { $0.name.caseInsensitiveCompare("@greeting") == .orderedSame }))
                    XCTAssertEqual(rv.string, "Hello, World!")
                }
            }
        }
    }

    func testMultipleOutputParameters() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "outp") { db in
                try await withDbClient(for: db, using: self.group) { dbClient in
                    _ = try await dbClient.execute("""
                        CREATE PROCEDURE [dbo].[usp_Divmod]
                            @dividend INT,
                            @divisor  INT,
                            @quotient INT OUTPUT,
                            @remainder INT OUTPUT
                        AS BEGIN
                            SET NOCOUNT ON;
                            SET @quotient  = @dividend / @divisor;
                            SET @remainder = @dividend % @divisor;
                        END
                    """).get()

                    let result = try await dbClient.call(
                        procedure: "dbo.usp_Divmod",
                        parameters: [
                            .init(name: "dividend",  value: .init(int32: 17)),
                            .init(name: "divisor",   value: .init(int32: 5)),
                            .init(name: "quotient",  value: .init(int32: 0), direction: .out),
                            .init(name: "remainder", value: .init(int32: 0), direction: .out)
                        ]
                    )

                    XCTAssertEqual(result.returnValues.count, 2)
                    let q = try XCTUnwrap(result.returnValues.first(where: { $0.name.caseInsensitiveCompare("@quotient") == .orderedSame }))
                    let r = try XCTUnwrap(result.returnValues.first(where: { $0.name.caseInsensitiveCompare("@remainder") == .orderedSame }))
                    XCTAssertEqual(q.int, 3)
                    XCTAssertEqual(r.int, 2)
                }
            }
        }
    }

    func testOutputParameterAlongsideResultSet() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "outp") { db in
                try await withDbClient(for: db, using: self.group) { dbClient in
                    _ = try await dbClient.execute("""
                        CREATE PROCEDURE [dbo].[usp_GetRange]
                            @count    INT,
                            @total    INT OUTPUT
                        AS BEGIN
                            SET NOCOUNT OFF;
                            SELECT n = number
                            FROM master..spt_values
                            WHERE type = 'P' AND number BETWEEN 1 AND @count;
                            SET @total = @count * (@count + 1) / 2;
                        END
                    """).get()

                    let result = try await dbClient.call(
                        procedure: "dbo.usp_GetRange",
                        parameters: [
                            .init(name: "count", value: .init(int32: 5)),
                            .init(name: "total", value: .init(int32: 0), direction: .out)
                        ]
                    )

                    // Should have 5 rows from the SELECT
                    XCTAssertEqual(result.rows.count, 5)
                    // And the output parameter = 1+2+3+4+5 = 15
                    let total = try XCTUnwrap(result.returnValues.first(where: { $0.name.caseInsensitiveCompare("@total") == .orderedSame }))
                    XCTAssertEqual(total.int, 15)
                }
            }
        }
    }

    func testNullOutputParameter() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "outp") { db in
                try await withDbClient(for: db, using: self.group) { dbClient in
                    _ = try await dbClient.execute("""
                        CREATE PROCEDURE [dbo].[usp_NullOut]
                            @out NVARCHAR(50) OUTPUT
                        AS BEGIN
                            SET NOCOUNT ON;
                            SET @out = NULL;
                        END
                    """).get()

                    let result = try await dbClient.call(
                        procedure: "dbo.usp_NullOut",
                        parameters: [
                            .init(name: "out", value: nil, direction: .out)
                        ]
                    )

                    let rv = try XCTUnwrap(result.returnValues.first)
                    XCTAssertNil(rv.value)
                    XCTAssertNil(rv.string)
                }
            }
        }
    }

    // MARK: - Client-level convenience (SQLServerClient.call)

    func testClientLevelCallConvenience() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "outp") { db in
                try await withDbClient(for: db, using: self.group) { dbClient in
                    _ = try await dbClient.execute("""
                        CREATE PROCEDURE [dbo].[usp_Add]
                            @a INT, @b INT, @sum INT OUTPUT
                        AS BEGIN SET NOCOUNT ON; SET @sum = @a + @b; END
                    """).get()

                    let result = try await dbClient.call(
                        procedure: "dbo.usp_Add",
                        parameters: [
                            .init(name: "a",   value: .init(int32: 7)),
                            .init(name: "b",   value: .init(int32: 8)),
                            .init(name: "sum", value: .init(int32: 0), direction: .out)
                        ]
                    )

                    let sum = try XCTUnwrap(result.returnValues.first(where: { $0.name.caseInsensitiveCompare("@sum") == .orderedSame }))
                    XCTAssertEqual(sum.int, 15)
                }
            }
        }
    }
}
