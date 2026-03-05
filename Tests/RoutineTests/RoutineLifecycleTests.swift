import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerRoutineTests: XCTestCase {
    private var group: EventLoopGroup!
    private var baseClient: SQLServerClient!
    private var client: SQLServerClient!
    private var routineClient: SQLServerRoutineClient!
    private var testDatabase: String!
    private var skipDueToEnv: Bool = false

    private var eventLoop: EventLoop { self.group.next() }

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.baseClient = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do {
            _ = try await withTimeout(5) { try await self.baseClient.query("SELECT 1").get() }
        } catch {
            skipDueToEnv = true
            return
        }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "rtn")
        self.client = try await makeClient(forDatabase: testDatabase, using: group)
        self.routineClient = SQLServerRoutineClient(client: self.client)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully().get()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try await baseClient?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        testDatabase = nil; group = nil
    }

    // MARK: - Stored Procedure Tests

    func testCreateSimpleStoredProcedure() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_simple_proc_\(UUID().uuidString.prefix(8))"
        let body = """
        BEGIN
            SELECT 'Hello World' AS message
        END
        """
        try await withTimeout(15) { try await self.routineClient.createStoredProcedure(name: procedureName, body: body) }
        let existsRows = try await self.client.query("SELECT COUNT(*) AS cnt FROM sys.objects WHERE type = 'P' AND name = N'\(procedureName)'")
        XCTAssertEqual(existsRows.first?.column("cnt")?.int, 1)
        let result = try await self.client.query("EXEC [dbo].[\(procedureName)]")
        XCTAssertEqual(result.first?.column("message")?.string, "Hello World")
    }

    func testCreateStoredProcedureWithParameters() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_param_proc_\(UUID().uuidString.prefix(8))"

        let parameters = [
            ProcedureParameter(name: "input_value", dataType: .int),
            ProcedureParameter(name: "output_value", dataType: .int, direction: .output)
        ]

        let body = """
        BEGIN
            SET @output_value = @input_value * 2
        END
        """

        try await withTimeout(15) { try await self.routineClient.createStoredProcedure(name: procedureName, parameters: parameters, body: body) }

        let exists = try await self.routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Stored procedure should exist after creation")

        let callSql = """
        DECLARE @result INT
        EXEC [\(procedureName)] @input_value = 5, @output_value = @result OUTPUT
        SELECT @result AS doubled_value
        """
        let result = try await self.client.query(callSql)
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("doubled_value")?.int, 10)
    }

    func testCreateStoredProcedureWithOptions() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_options_proc_\(UUID().uuidString.prefix(8))"

        let options = RoutineOptions(schema: "dbo", withRecompile: true)
        let body = """
        BEGIN
            SELECT GETDATE() AS procedure_time
        END
        """

        try await withTimeout(15) { try await self.routineClient.createStoredProcedure(name: procedureName, body: body, options: options) }

        let exists = try await self.routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Stored procedure should exist after creation")
    }

    func testCreateStoredProcedureWithExecuteAsOption() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_exec_as_proc_\(UUID().uuidString.prefix(8))"

        let options = RoutineOptions(schema: "dbo", executeAs: "dbo")
        let body = """
        BEGIN
            SELECT SYSTEM_USER AS executed_as
        END
        """

        try await withTimeout(15) { try await self.routineClient.createStoredProcedure(name: procedureName, body: body, options: options) }

        let definitionResult = try await self.client.query("""
        SELECT CAST(definition AS NVARCHAR(4000)) AS definition
        FROM sys.sql_modules
        WHERE object_id = OBJECT_ID(N'[dbo].[\(procedureName)]')
        """)
        let definition = definitionResult.first?.column("definition")?.string ?? ""
        XCTAssertTrue(definition.uppercased().contains("EXECUTE AS"), "Stored procedure definition should include EXECUTE AS clause, got: \(definition)")

        let execResult = try await self.client.query("EXEC [\(procedureName)]")
        XCTAssertEqual(execResult.first?.column("executed_as")?.string?.lowercased(), "sa")
    }

    func testAlterStoredProcedure() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_alter_proc_\(UUID().uuidString.prefix(8))"

        let initialBody = """
        BEGIN
            SELECT 'Initial' AS status
        END
        """
        try await withTimeout(15) { try await self.routineClient.createStoredProcedure(name: procedureName, body: initialBody) }

        let alteredBody = """
        BEGIN
            SELECT 'Altered' AS status
        END
        """
        try await withTimeout(15) { try await self.routineClient.alterStoredProcedure(name: procedureName, body: alteredBody) }

        let result = try await self.client.query("EXEC [\(procedureName)]")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("status")?.string, "Altered")
    }

    func testDropStoredProcedure() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_drop_proc_\(UUID().uuidString.prefix(8))"

        let body = """
        BEGIN
            SELECT 'Test' AS message
        END
        """
        try await withTimeout(15) { try await self.routineClient.createStoredProcedure(name: procedureName, body: body) }

        var exists = try await self.routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Stored procedure should exist after creation")

        try await self.routineClient.dropStoredProcedure(name: procedureName)

        exists = try await self.routineClient.procedureExists(name: procedureName)
        XCTAssertFalse(exists, "Stored procedure should not exist after being dropped")
    }

    // MARK: - Scalar Function Tests

    func testCreateSimpleScalarFunction() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let functionName = "test_simple_func_\(UUID().uuidString.prefix(8))"

        let body = """
        BEGIN
            RETURN 42
        END
        """

        try await withTimeout(15) { try await self.routineClient.createFunction(
            name: functionName,
            returnType: .int,
            body: body
        ) }

        let exists = try await self.routineClient.functionExists(name: functionName)
        XCTAssertTrue(exists, "Function should exist after creation")

        let result = try await self.client.query("SELECT dbo.[\(functionName)]() AS result")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("result")?.int, 42)
    }

    func testCreateScalarFunctionWithParameters() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let functionName = "test_param_func_\(UUID().uuidString.prefix(8))"

        let parameters = [
            FunctionParameter(name: "x", dataType: .int),
            FunctionParameter(name: "y", dataType: .int)
        ]

        let body = """
        BEGIN
            RETURN @x + @y
        END
        """

        try await withTimeout(15) { try await self.routineClient.createFunction(
            name: functionName,
            parameters: parameters,
            returnType: .int,
            body: body
        ) }

        let exists = try await self.routineClient.functionExists(name: functionName)
        XCTAssertTrue(exists, "Function should exist after creation")

        let result = try await self.client.query("SELECT dbo.[\(functionName)](10, 20) AS result")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("result")?.int, 30)
    }

    func testCreateScalarFunctionWithDefaultParameter() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let functionName = "test_default_func_\(UUID().uuidString.prefix(8))"

        let parameters = [
            FunctionParameter(name: "multiplier", dataType: .int, defaultValue: "2")
        ]

        let body = """
        BEGIN
            RETURN 10 * @multiplier
        END
        """

        try await withTimeout(15) { try await self.routineClient.createFunction(
            name: functionName,
            parameters: parameters,
            returnType: .int,
            body: body
        ) }

        let result = try await self.client.query("SELECT dbo.[\(functionName)](DEFAULT) AS result")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("result")?.int, 20)
    }

    // MARK: - Table-Valued Function Tests

    func testCreateTableValuedFunction() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let functionName = "test_table_func_\(UUID().uuidString.prefix(8))"

        let parameters = [
            FunctionParameter(name: "max_value", dataType: .int)
        ]

        let tableDefinition = [
            TableValuedFunctionColumn(name: "id", dataType: .int),
            TableValuedFunctionColumn(name: "value", dataType: .nvarchar(length: .length(50)))
        ]

        let body = """
        SELECT 1 AS id, N'First' AS value WHERE 1 <= @max_value
        UNION ALL
        SELECT 2 AS id, N'Second' AS value WHERE 2 <= @max_value
        UNION ALL
        SELECT 3 AS id, N'Third' AS value WHERE 3 <= @max_value
        """

        try await withTimeout(15) { try await self.routineClient.createTableValuedFunction(
            name: functionName,
            parameters: parameters,
            tableDefinition: tableDefinition,
            body: body
        ) }

        let exists = try await self.routineClient.functionExists(name: functionName)
        XCTAssertTrue(exists, "Table-valued function should exist after creation")

        let result = try await self.client.query("SELECT * FROM dbo.[\(functionName)](2) ORDER BY id")
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("id")?.int, 1)
        XCTAssertEqual(result[0].column("value")?.string, "First")
        XCTAssertEqual(result[1].column("id")?.int, 2)
        XCTAssertEqual(result[1].column("value")?.string, "Second")
    }

    func testDropFunction() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let functionName = "test_drop_func_\(UUID().uuidString.prefix(8))"

        let body = """
        BEGIN
            RETURN 123
        END
        """
        try await self.routineClient.createFunction(name: functionName, returnType: .int, body: body)

        var exists = try await self.routineClient.functionExists(name: functionName)
        XCTAssertTrue(exists, "Function should exist after creation")

        try await self.routineClient.dropFunction(name: functionName)

        exists = try await self.routineClient.functionExists(name: functionName)
        XCTAssertFalse(exists, "Function should not exist after being dropped")
    }

    // MARK: - Complex Scenarios

    func testCreateComplexStoredProcedureWithMultipleParameterTypes() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_complex_proc_\(UUID().uuidString.prefix(8))"

        let parameters = [
            ProcedureParameter(name: "input_int", dataType: .int),
            ProcedureParameter(name: "input_string", dataType: .nvarchar(length: .length(100))),
            ProcedureParameter(name: "input_date", dataType: .datetime2(precision: 3)),
            ProcedureParameter(name: "output_result", dataType: .nvarchar(length: .length(200)), direction: .output),
            ProcedureParameter(name: "inout_counter", dataType: .int, direction: .inputOutput)
        ]

        let body = """
        BEGIN
            SET @inout_counter = @inout_counter + 1
            SET @output_result = CONCAT('Processed: ', @input_string, ' at ', FORMAT(@input_date, 'yyyy-MM-dd'), ' with value ', @input_int)
        END
        """

        try await withTimeout(15) { try await self.routineClient.createStoredProcedure(name: procedureName, parameters: parameters, body: body) }

        let exists = try await self.routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Complex stored procedure should exist after creation")

        let callSql = """
        DECLARE @result NVARCHAR(200)
        DECLARE @counter INT = 5
        EXEC [\(procedureName)]
            @input_int = 42,
            @input_string = N'Test Data',
            @input_date = '2023-12-25 10:30:00',
            @output_result = @result OUTPUT,
            @inout_counter = @counter OUTPUT
        SELECT @result AS result, @counter AS counter
        """

        let result = try await self.client.query(callSql)
        XCTAssertEqual(result.count, 1)
        XCTAssertTrue(result.first?.column("result")?.string?.contains("Processed: Test Data") == true)
        XCTAssertEqual(result.first?.column("counter")?.int, 6)
    }

    func testCreateFunctionWithStringReturnType() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let functionName = "test_string_func_\(UUID().uuidString.prefix(8))"

        let parameters = [
            FunctionParameter(name: "first_name", dataType: .nvarchar(length: .length(50))),
            FunctionParameter(name: "last_name", dataType: .nvarchar(length: .length(50)))
        ]

        let body = """
        BEGIN
            RETURN CONCAT(@first_name, ' ', @last_name)
        END
        """

        try await withTimeout(15) { try await self.routineClient.createFunction(
            name: functionName,
            parameters: parameters,
            returnType: .nvarchar(length: .length(100)),
            body: body
        ) }

        let result = try await self.client.query("SELECT dbo.[\(functionName)](N'John', N'Doe') AS full_name")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("full_name")?.string, "John Doe")
    }

    // MARK: - Error Handling Tests

    func testCreateDuplicateStoredProcedure() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_duplicate_proc_\(UUID().uuidString.prefix(8))"

        let body = """
        BEGIN
            SELECT 'Test' AS message
        END
        """

        try await self.routineClient.createStoredProcedure(name: procedureName, body: body)

        do {
            try await self.routineClient.createStoredProcedure(name: procedureName, body: body)
            XCTFail("Creating duplicate procedure should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentStoredProcedure() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "non_existent_proc_\(UUID().uuidString.prefix(8))"

        do {
            try await self.routineClient.dropStoredProcedure(name: procedureName)
            XCTFail("Dropping non-existent procedure should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateStoredProcedureWithInvalidSyntax() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let procedureName = "test_invalid_proc_\(UUID().uuidString.prefix(8))"

        let invalidBody = """
        BEGIN
            INVALID SQL SYNTAX HERE
        END
        """

        do {
            try await self.routineClient.createStoredProcedure(name: procedureName, body: invalidBody)
            XCTFail("Creating procedure with invalid syntax should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
