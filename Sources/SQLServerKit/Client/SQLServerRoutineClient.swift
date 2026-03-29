import NIO
import Foundation
import SQLServerTDS

// MARK: - Parameter Types

public struct ProcedureParameter: Sendable {
    public let name: String
    public let dataType: SQLDataType
    public let direction: ParameterDirection
    public let defaultValue: String?

    public init(name: String, dataType: SQLDataType, direction: ParameterDirection = .input, defaultValue: String? = nil) {
        self.name = name
        self.dataType = dataType
        self.direction = direction
        self.defaultValue = defaultValue
    }

    public enum ParameterDirection: Sendable {
        case input
        case output
        case inputOutput
    }
}

public struct FunctionParameter: Sendable {
    public let name: String
    public let dataType: SQLDataType
    public let defaultValue: String?

    public init(name: String, dataType: SQLDataType, defaultValue: String? = nil) {
        self.name = name
        self.dataType = dataType
        self.defaultValue = defaultValue
    }
}

public struct TableValuedFunctionColumn: Sendable {
    public let name: String
    public let dataType: SQLDataType

    public init(name: String, dataType: SQLDataType) {
        self.name = name
        self.dataType = dataType
    }
}

// MARK: - Options

public struct RoutineOptions: Sendable {
    @available(*, deprecated, message: "Pass schema as a direct parameter instead")
    public var schemaValue: String { schema }
    internal let schema: String
    public let withEncryption: Bool
    public let withRecompile: Bool
    public let executeAs: String?

    public init(schema: String = "dbo", withEncryption: Bool = false, withRecompile: Bool = false, executeAs: String? = nil) {
        self.schema = schema
        self.withEncryption = withEncryption
        self.withRecompile = withRecompile
        self.executeAs = executeAs
    }
}

// MARK: - SQLServerRoutineClient

public final class SQLServerRoutineClient: @unchecked Sendable {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Stored Procedures

    @discardableResult
    public func createStoredProcedure(
        name: String,
        parameters: [ProcedureParameter] = [],
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createStoredProcedure(name: name, parameters: parameters, body: body, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func createStoredProcedure(
        name: String,
        parameters: [ProcedureParameter] = [],
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        var sql = "CREATE PROCEDURE \(fullName)"

        // Add parameters
        if !parameters.isEmpty {
            let paramStrings = parameters.map { param in
                var paramStr = "@\(param.name) \(param.dataType.toSqlString())"
                if case .userDefinedTable = param.dataType {
                    paramStr += " READONLY"
                } else {
                    if !Self.formatParameterDirection(param.direction).isEmpty {
                        paramStr += " \(Self.formatParameterDirection(param.direction))"
                    }
                    if let defaultValue = param.defaultValue {
                        paramStr += " = \(defaultValue)"
                    }
                }
                return paramStr
            }
            sql += "\n(\n    \(paramStrings.joined(separator: ",\n    "))\n)"
        }

        // Add options
        if let optionClause = Self.buildOptionClause(from: options, allowRecompile: true) {
            sql += "\n\(optionClause)"
        }

        sql += "\nAS\n\(body)"

        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func dropStoredProcedure(name: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropStoredProcedure(name: name, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func dropStoredProcedure(name: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        let sql = "DROP PROCEDURE \(fullName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    public func alterStoredProcedure(
        name: String,
        parameters: [ProcedureParameter] = [],
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.alterStoredProcedure(name: name, parameters: parameters, body: body, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func alterStoredProcedure(
        name: String,
        parameters: [ProcedureParameter] = [],
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        var sql = "ALTER PROCEDURE \(fullName)"

        // Add parameters
        if !parameters.isEmpty {
            let paramStrings = parameters.map { param in
                var paramStr = "@\(param.name) \(param.dataType.toSqlString())"
                if case .userDefinedTable = param.dataType {
                    paramStr += " READONLY"
                } else {
                    if !Self.formatParameterDirection(param.direction).isEmpty {
                        paramStr += " \(Self.formatParameterDirection(param.direction))"
                    }
                    if let defaultValue = param.defaultValue {
                        paramStr += " = \(defaultValue)"
                    }
                }
                return paramStr
            }
            sql += "\n(\n    \(paramStrings.joined(separator: ",\n    "))\n)"
        }

        // Add options
        if let optionClause = Self.buildOptionClause(from: options, allowRecompile: true) {
            sql += "\n\(optionClause)"
        }

        sql += "\nAS\n\(body)"

        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Scalar Functions

    @discardableResult
    public func createFunction(
        name: String,
        parameters: [FunctionParameter] = [],
        returnType: SQLDataType,
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createFunction(name: name, parameters: parameters, returnType: returnType, body: body, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func createFunction(
        name: String,
        parameters: [FunctionParameter] = [],
        returnType: SQLDataType,
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        var sql = "CREATE FUNCTION \(fullName)"

        // Add parameters
        if !parameters.isEmpty {
            let paramStrings = parameters.map { param in
                var paramStr = "@\(param.name) \(param.dataType.toSqlString())"
                if let defaultValue = param.defaultValue {
                    paramStr += " = \(defaultValue)"
                }
                return paramStr
            }
            sql += "\n(\n    \(paramStrings.joined(separator: ",\n    "))\n)"
        } else {
            sql += "()"
        }

        sql += "\nRETURNS \(returnType.toSqlString())"

        // Add options
        if let optionClause = Self.buildOptionClause(from: options, allowRecompile: false) {
            sql += "\n\(optionClause)"
        }

        sql += "\nAS\n\(body)"

        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Table-Valued Functions

    @discardableResult
    public func createTableValuedFunction(
        name: String,
        parameters: [FunctionParameter] = [],
        tableDefinition: [TableValuedFunctionColumn],
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createTableValuedFunction(name: name, parameters: parameters, tableDefinition: tableDefinition, body: body, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func createTableValuedFunction(
        name: String,
        parameters: [FunctionParameter] = [],
        tableDefinition: [TableValuedFunctionColumn],
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        var sql = "CREATE FUNCTION \(fullName)"

        // Add parameters
        if !parameters.isEmpty {
            let paramStrings = parameters.map { param in
                var paramStr = "@\(param.name) \(param.dataType.toSqlString())"
                if let defaultValue = param.defaultValue {
                    paramStr += " = \(defaultValue)"
                }
                return paramStr
            }
            sql += "\n(\n    \(paramStrings.joined(separator: ",\n    "))\n)"
        } else {
            sql += "()"
        }

        let returnTableVariable = "@result_table"

        // Add table definition
        let columnStrings = tableDefinition.map { column in
            "\(SQLServerSQL.escapeIdentifier(column.name)) \(column.dataType.toSqlString())"
        }
        sql += "\nRETURNS \(returnTableVariable) TABLE\n(\n    \(columnStrings.joined(separator: ",\n    "))\n)"

        // Add options
        if let optionClause = Self.buildOptionClause(from: options, allowRecompile: false) {
            sql += "\n\(optionClause)"
        }

        sql += "\nAS\nBEGIN\n"

        let trimmedBody = body.trimmingCharacters(in: .whitespacesAndNewlines)
        let uppercasedBody = trimmedBody.uppercased()
        let shouldAutoInsert = uppercasedBody.hasPrefix("SELECT") || uppercasedBody.hasPrefix("WITH") || trimmedBody.first == "("

        let normalizedBody: String = {
            var withoutLeadingNewline = body
            if withoutLeadingNewline.hasPrefix("\n") {
                withoutLeadingNewline.removeFirst()
            }
            if withoutLeadingNewline.hasSuffix("\n") {
                return withoutLeadingNewline
            } else {
                return withoutLeadingNewline + "\n"
            }
        }()

        if shouldAutoInsert {
            sql += "    INSERT INTO \(returnTableVariable)\n"
        }
        sql += normalizedBody

        if !uppercasedBody.contains("RETURN") {
            sql += "    RETURN\n"
        }
        sql += "END"

        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func dropFunction(name: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropFunction(name: name, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func dropFunction(name: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        let sql = "DROP FUNCTION \(fullName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Alter Scalar Functions

    @discardableResult
    public func alterScalarFunction(
        name: String,
        parameters: [FunctionParameter] = [],
        returnType: SQLDataType,
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.alterScalarFunction(name: name, parameters: parameters, returnType: returnType, body: body, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func alterScalarFunction(
        name: String,
        parameters: [FunctionParameter] = [],
        returnType: SQLDataType,
        body: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        var sql = "ALTER FUNCTION \(fullName)"

        // Add parameters
        if !parameters.isEmpty {
            let paramStrings = parameters.map { param in
                var paramStr = "@\(param.name) \(param.dataType.toSqlString())"
                if let defaultValue = param.defaultValue {
                    paramStr += " = \(defaultValue)"
                }
                return paramStr
            }
            sql += "\n(\n    \(paramStrings.joined(separator: ",\n    "))\n)"
        } else {
            sql += "()"
        }

        sql += "\nRETURNS \(returnType.toSqlString())"

        // Add options
        if let optionClause = Self.buildOptionClause(from: options, allowRecompile: false) {
            sql += "\n\(optionClause)"
        }

        sql += "\nAS\n\(body)"

        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - List Routines

    /// List stored procedures in the given database and schema.
    @available(macOS 12.0, *)
    public func listProcedures(database: String? = nil, schema: String? = nil) async throws -> [RoutineMetadata] {
        try await client.metadata.listProcedures(database: database, schema: schema)
    }

    /// List functions in the given database and schema.
    @available(macOS 12.0, *)
    public func listFunctions(database: String? = nil, schema: String? = nil) async throws -> [RoutineMetadata] {
        try await client.metadata.listFunctions(database: database, schema: schema)
    }

    // MARK: - Utility Methods

    @available(macOS 12.0, *)
    public func procedureExists(name: String, schema: String = "dbo") async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.procedures p
        INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE p.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """

        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }

    @available(macOS 12.0, *)
    public func functionExists(name: String, schema: String = "dbo") async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        AND o.type IN ('FN', 'IF', 'TF')
        """

        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }
    private static func buildOptionClause(from options: RoutineOptions, allowRecompile: Bool) -> String? {
        var parts: [String] = []
        if options.withEncryption {
            parts.append("ENCRYPTION")
        }
        if allowRecompile && options.withRecompile {
            parts.append("RECOMPILE")
        }
        if let executeAs = options.executeAs?.trimmingCharacters(in: .whitespacesAndNewlines), !executeAs.isEmpty {
            parts.append(Self.formatExecuteAsClause(executeAs))
        }
        guard !parts.isEmpty else {
            return nil
        }
        return "WITH \(parts.joined(separator: ", "))"
    }

    private static func formatExecuteAsClause(_ executeAs: String) -> String {
        let normalized = executeAs.uppercased()
        switch normalized {
        case "CALLER", "SELF", "OWNER":
            return "EXECUTE AS \(normalized)"
        default:
            let sanitized = executeAs.replacingOccurrences(of: "'", with: "''")
            return "EXECUTE AS '\(sanitized)'"
        }
    }

    private static func formatParameter(_ param: ProcedureParameter) -> String {
        var paramStr = "@\(param.name) \(param.dataType.toSqlString())"
        switch param.direction {
        case .input:
            break // No additional keyword needed
        case .output, .inputOutput:
            paramStr += " OUTPUT"
        }
        if let defaultValue = param.defaultValue {
            paramStr += " = \(defaultValue)"
        }
        return paramStr
    }

    private static func formatParameterDirection(_ direction: ProcedureParameter.ParameterDirection) -> String {
        switch direction {
        case .input:
            return ""
        case .output, .inputOutput:
            return "OUTPUT"
        }
    }
}
