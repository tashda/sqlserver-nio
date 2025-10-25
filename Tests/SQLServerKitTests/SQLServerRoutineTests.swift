import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerRoutineTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var routineClient: SQLServerRoutineClient!
    private var routinesToDrop: [(name: String, schema: String, type: String)] = []

    private var eventLoop: EventLoop { self.group.next() }

    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
        self.routineClient = SQLServerRoutineClient(client: client)
    }

    override func tearDownWithError() throws {
        // Drop any routines that were created during the test
        for routine in routinesToDrop {
            let dropSql: String
            switch routine.type {
            case "procedure":
                dropSql = "IF OBJECT_ID('[\(routine.schema)].[\(routine.name)]', 'P') IS NOT NULL DROP PROCEDURE [\(routine.schema)].[\(routine.name)]"
            case "function":
                dropSql = "IF OBJECT_ID('[\(routine.schema)].[\(routine.name)]', 'FN') IS NOT NULL DROP FUNCTION [\(routine.schema)].[\(routine.name)]"
            case "table_function":
                dropSql = "IF OBJECT_ID('[\(routine.schema)].[\(routine.name)]', 'TF') IS NOT NULL DROP FUNCTION [\(routine.schema)].[\(routine.name)]"
            default:
                continue
            }
            
            do {
                _ = try client.execute(dropSql).wait()
            } catch {
                // Ignore errors during cleanup
                print("Warning: Failed to drop \(routine.type) \(routine.schema).\(routine.name): \(error)")
            }
        }
        routinesToDrop.removeAll()

        try self.client.shutdownGracefully().wait()
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }

    // MARK: - Stored Procedure Tests

    func testCreateSimpleStoredProcedure() async throws {
        let procedureName = "test_simple_proc_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: procedureName, schema: "dbo", type: "procedure"))

        let body = """
        BEGIN
            SELECT 'Hello World' AS message
        END
        """

        try await routineClient.createStoredProcedure(name: procedureName, body: body)

        // Verify the procedure exists
        let exists = try await routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Stored procedure should exist after creation")

        // Test calling the procedure
        let result = try await client.query("EXEC [\(procedureName)]")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("message")?.string, "Hello World")
    }

    func testCreateStoredProcedureWithParameters() async throws {
        let procedureName = "test_param_proc_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: procedureName, schema: "dbo", type: "procedure"))

        let parameters = [
            ProcedureParameter(name: "input_value", dataType: .int),
            ProcedureParameter(name: "output_value", dataType: .int, direction: .output)
        ]

        let body = """
        BEGIN
            SET @output_value = @input_value * 2
        END
        """

        try await routineClient.createStoredProcedure(name: procedureName, parameters: parameters, body: body)

        // Verify the procedure exists
        let exists = try await routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Stored procedure should exist after creation")

        // Test calling the procedure with parameters
        let callSql = """
        DECLARE @result INT
        EXEC [\(procedureName)] @input_value = 5, @output_value = @result OUTPUT
        SELECT @result AS doubled_value
        """
        let result = try await client.query(callSql)
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("doubled_value")?.int, 10)
    }

    func testCreateStoredProcedureWithOptions() async throws {
        let procedureName = "test_options_proc_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: procedureName, schema: "dbo", type: "procedure"))

        let options = RoutineOptions(schema: "dbo", withRecompile: true)
        let body = """
        BEGIN
            SELECT GETDATE() AS procedure_time
        END
        """

        try await routineClient.createStoredProcedure(name: procedureName, body: body, options: options)

        // Verify the procedure exists
        let exists = try await routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Stored procedure should exist after creation")
    }

    func testAlterStoredProcedure() async throws {
        let procedureName = "test_alter_proc_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: procedureName, schema: "dbo", type: "procedure"))

        // Create initial procedure
        let initialBody = """
        BEGIN
            SELECT 'Initial' AS status
        END
        """
        try await routineClient.createStoredProcedure(name: procedureName, body: initialBody)

        // Alter the procedure
        let alteredBody = """
        BEGIN
            SELECT 'Altered' AS status
        END
        """
        try await routineClient.alterStoredProcedure(name: procedureName, body: alteredBody)

        // Test the altered procedure
        let result = try await client.query("EXEC [\(procedureName)]")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("status")?.string, "Altered")
    }

    func testDropStoredProcedure() async throws {
        let procedureName = "test_drop_proc_\(UUID().uuidString.prefix(8))"

        // Create procedure
        let body = """
        BEGIN
            SELECT 'Test' AS message
        END
        """
        try await routineClient.createStoredProcedure(name: procedureName, body: body)

        // Verify it exists
        var exists = try await routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Stored procedure should exist after creation")

        // Drop the procedure
        try await routineClient.dropStoredProcedure(name: procedureName)

        // Verify it's gone
        exists = try await routineClient.procedureExists(name: procedureName)
        XCTAssertFalse(exists, "Stored procedure should not exist after being dropped")
    }

    // MARK: - Scalar Function Tests

    func testCreateSimpleScalarFunction() async throws {
        let functionName = "test_simple_func_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: functionName, schema: "dbo", type: "function"))

        let body = """
        BEGIN
            RETURN 42
        END
        """

        try await routineClient.createFunction(
            name: functionName,
            returnType: .int,
            body: body
        )

        // Verify the function exists
        let exists = try await routineClient.functionExists(name: functionName)
        XCTAssertTrue(exists, "Function should exist after creation")

        // Test calling the function
        let result = try await client.query("SELECT dbo.[\(functionName)]() AS result")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("result")?.int, 42)
    }

    func testCreateScalarFunctionWithParameters() async throws {
        let functionName = "test_param_func_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: functionName, schema: "dbo", type: "function"))

        let parameters = [
            FunctionParameter(name: "x", dataType: .int),
            FunctionParameter(name: "y", dataType: .int)
        ]

        let body = """
        BEGIN
            RETURN @x + @y
        END
        """

        try await routineClient.createFunction(
            name: functionName,
            parameters: parameters,
            returnType: .int,
            body: body
        )

        // Verify the function exists
        let exists = try await routineClient.functionExists(name: functionName)
        XCTAssertTrue(exists, "Function should exist after creation")

        // Test calling the function
        let result = try await client.query("SELECT dbo.[\(functionName)](10, 20) AS result")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("result")?.int, 30)
    }

    func testCreateScalarFunctionWithDefaultParameter() async throws {
        let functionName = "test_default_func_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: functionName, schema: "dbo", type: "function"))

        let parameters = [
            FunctionParameter(name: "multiplier", dataType: .int, defaultValue: "2")
        ]

        let body = """
        BEGIN
            RETURN 10 * @multiplier
        END
        """

        try await routineClient.createFunction(
            name: functionName,
            parameters: parameters,
            returnType: .int,
            body: body
        )

        // Test calling the function with default parameter
        let result = try await client.query("SELECT dbo.[\(functionName)](DEFAULT) AS result")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("result")?.int, 20)
    }

    // MARK: - Table-Valued Function Tests

    func testCreateTableValuedFunction() async throws {
        let functionName = "test_table_func_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: functionName, schema: "dbo", type: "table_function"))

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

        try await routineClient.createTableValuedFunction(
            name: functionName,
            parameters: parameters,
            tableDefinition: tableDefinition,
            body: body
        )

        // Verify the function exists
        let exists = try await routineClient.functionExists(name: functionName)
        XCTAssertTrue(exists, "Table-valued function should exist after creation")

        // Test calling the function
        let result = try await client.query("SELECT * FROM dbo.[\(functionName)](2) ORDER BY id")
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("id")?.int, 1)
        XCTAssertEqual(result[0].column("value")?.string, "First")
        XCTAssertEqual(result[1].column("id")?.int, 2)
        XCTAssertEqual(result[1].column("value")?.string, "Second")
    }

    func testDropFunction() async throws {
        let functionName = "test_drop_func_\(UUID().uuidString.prefix(8))"

        // Create function
        let body = """
        BEGIN
            RETURN 123
        END
        """
        try await routineClient.createFunction(name: functionName, returnType: .int, body: body)

        // Verify it exists
        var exists = try await routineClient.functionExists(name: functionName)
        XCTAssertTrue(exists, "Function should exist after creation")

        // Drop the function
        try await routineClient.dropFunction(name: functionName)

        // Verify it's gone
        exists = try await routineClient.functionExists(name: functionName)
        XCTAssertFalse(exists, "Function should not exist after being dropped")
    }

    // MARK: - Complex Scenarios

    func testCreateComplexStoredProcedureWithMultipleParameterTypes() async throws {
        let procedureName = "test_complex_proc_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: procedureName, schema: "dbo", type: "procedure"))

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

        try await routineClient.createStoredProcedure(name: procedureName, parameters: parameters, body: body)

        // Verify the procedure exists
        let exists = try await routineClient.procedureExists(name: procedureName)
        XCTAssertTrue(exists, "Complex stored procedure should exist after creation")

        // Test calling the procedure
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
        
        let result = try await client.query(callSql)
        XCTAssertEqual(result.count, 1)
        XCTAssertTrue(result.first?.column("result")?.string?.contains("Processed: Test Data") == true)
        XCTAssertEqual(result.first?.column("counter")?.int, 6)
    }

    func testCreateFunctionWithStringReturnType() async throws {
        let functionName = "test_string_func_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: functionName, schema: "dbo", type: "function"))

        let parameters = [
            FunctionParameter(name: "first_name", dataType: .nvarchar(length: .length(50))),
            FunctionParameter(name: "last_name", dataType: .nvarchar(length: .length(50)))
        ]

        let body = """
        BEGIN
            RETURN CONCAT(@first_name, ' ', @last_name)
        END
        """

        try await routineClient.createFunction(
            name: functionName,
            parameters: parameters,
            returnType: .nvarchar(length: .length(100)),
            body: body
        )

        // Test calling the function
        let result = try await client.query("SELECT dbo.[\(functionName)](N'John', N'Doe') AS full_name")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("full_name")?.string, "John Doe")
    }

    // MARK: - Error Handling Tests

    func testCreateDuplicateStoredProcedure() async throws {
        let procedureName = "test_duplicate_proc_\(UUID().uuidString.prefix(8))"
        routinesToDrop.append((name: procedureName, schema: "dbo", type: "procedure"))

        let body = """
        BEGIN
            SELECT 'Test' AS message
        END
        """

        // Create the first procedure
        try await routineClient.createStoredProcedure(name: procedureName, body: body)

        // Attempt to create duplicate should fail
        do {
            try await routineClient.createStoredProcedure(name: procedureName, body: body)
            XCTFail("Creating duplicate procedure should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentStoredProcedure() async throws {
        let procedureName = "non_existent_proc_\(UUID().uuidString.prefix(8))"

        // Attempt to drop non-existent procedure should fail
        do {
            try await routineClient.dropStoredProcedure(name: procedureName)
            XCTFail("Dropping non-existent procedure should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateStoredProcedureWithInvalidSyntax() async throws {
        let procedureName = "test_invalid_proc_\(UUID().uuidString.prefix(8))"

        let invalidBody = """
        BEGIN
            INVALID SQL SYNTAX HERE
        END
        """

        // Attempt to create procedure with invalid syntax should fail
        do {
            try await routineClient.createStoredProcedure(name: procedureName, body: invalidBody)
            XCTFail("Creating procedure with invalid syntax should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
