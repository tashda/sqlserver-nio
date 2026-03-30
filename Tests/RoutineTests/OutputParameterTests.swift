import XCTest
import Logging
import SQLServerKit
import SQLServerKitTesting

/// Integration tests for stored procedure output parameters (RPC RETURNVALUE token path).
///
/// These tests exercise the full stack:
///   SQL Server → TDS RETURNVALUE token → TDSTokenOperations → SQLServerConnection.call() → SQLServerReturnValue
final class OutputParameterTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured
        client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    // MARK: - Basic output parameter types

    func testIntOutputParameter() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "outp") { db in
                try await withDbClient(for: db) { dbClient in
                    let routineClient = SQLServerRoutineClient(client: dbClient)
                    try await routineClient.createStoredProcedure(
                        name: "usp_DoubleInt",
                        parameters: [
                            .init(name: "input", dataType: .int),
                            .init(name: "result", dataType: .int, direction: .output)
                        ],
                        body: """
                        BEGIN
                            SET NOCOUNT ON;
                            SET @result = @input * 2;
                        END
                        """
                    )

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
                try await withDbClient(for: db) { dbClient in
                    let routineClient = SQLServerRoutineClient(client: dbClient)
                    try await routineClient.createStoredProcedure(
                        name: "usp_Greet",
                        parameters: [
                            .init(name: "name", dataType: .nvarchar(length: .length(50))),
                            .init(name: "greeting", dataType: .nvarchar(length: .length(100)), direction: .output)
                        ],
                        body: """
                        BEGIN
                            SET NOCOUNT ON;
                            SET @greeting = N'Hello, ' + @name + N'!';
                        END
                        """
                    )

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
                try await withDbClient(for: db) { dbClient in
                    let routineClient = SQLServerRoutineClient(client: dbClient)
                    try await routineClient.createStoredProcedure(
                        name: "usp_Divmod",
                        parameters: [
                            .init(name: "dividend", dataType: .int),
                            .init(name: "divisor", dataType: .int),
                            .init(name: "quotient", dataType: .int, direction: .output),
                            .init(name: "remainder", dataType: .int, direction: .output)
                        ],
                        body: """
                        BEGIN
                            SET NOCOUNT ON;
                            SET @quotient  = @dividend / @divisor;
                            SET @remainder = @dividend % @divisor;
                        END
                        """
                    )

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
                try await withDbClient(for: db) { dbClient in
                    let routineClient = SQLServerRoutineClient(client: dbClient)
                    try await routineClient.createStoredProcedure(
                        name: "usp_GetRange",
                        parameters: [
                            .init(name: "count", dataType: .int),
                            .init(name: "total", dataType: .int, direction: .output)
                        ],
                        body: """
                        BEGIN
                            SET NOCOUNT OFF;
                            SELECT n = number
                            FROM master..spt_values
                            WHERE type = 'P' AND number BETWEEN 1 AND @count;
                            SET @total = @count * (@count + 1) / 2;
                        END
                        """
                    )

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
                try await withDbClient(for: db) { dbClient in
                    let routineClient = SQLServerRoutineClient(client: dbClient)
                    try await routineClient.createStoredProcedure(
                        name: "usp_NullOut",
                        parameters: [
                            .init(name: "out", dataType: .nvarchar(length: .length(50)), direction: .output)
                        ],
                        body: """
                        BEGIN
                            SET NOCOUNT ON;
                            SET @out = NULL;
                        END
                        """
                    )

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
                try await withDbClient(for: db) { dbClient in
                    let routineClient = SQLServerRoutineClient(client: dbClient)
                    try await routineClient.createStoredProcedure(
                        name: "usp_Add",
                        parameters: [
                            .init(name: "a", dataType: .int),
                            .init(name: "b", dataType: .int),
                            .init(name: "sum", dataType: .int, direction: .output)
                        ],
                        body: """
                        BEGIN
                            SET NOCOUNT ON;
                            SET @sum = @a + @b;
                        END
                        """
                    )

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
