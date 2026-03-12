import Foundation
import NIO
import NIOCore
import SQLServerTDS

/// Represents a Table-Valued Parameter (TVP) for SQL Server stored procedure calls.
///
/// In this implementation, TVPs are handled by generating a sequence of T-SQL commands:
/// 1. DECLARE a table variable of the target user-defined table type.
/// 2. INSERT the row data into that table variable.
/// 3. Pass the table variable as a parameter to the stored procedure.
public struct SQLServerTableValuedParameter: Sendable {
    public struct Row: Sendable {
        public let values: [SQLServerLiteralValue]

        public init(values: [SQLServerLiteralValue]) {
            self.values = values
        }
    }

    public let typeName: String?
    public let parameterName: String
    public let columns: [ColumnInfo]
    public let rows: [Row]

    public init(typeName: String, parameterName: String, columns: [ColumnInfo], rows: [[SQLServerLiteralValue]]) {
        self.init(
            typeName: typeName,
            parameterName: parameterName,
            columns: columns,
            rows: rows.map(Row.init(values:))
        )
    }

    public init(typeName: String? = nil, parameterName: String, columns: [ColumnInfo], rows: [Row]) {
        self.typeName = typeName
        self.parameterName = parameterName
        self.columns = columns
        self.rows = rows
    }

    public init(name: String, columns: [ColumnInfo], rows: [Row]) {
        self.init(typeName: nil, parameterName: "@\(name)", columns: columns, rows: rows)
    }

    /// Internal helper to build the T-SQL script for this TVP.
    internal func buildDeclarationAndInsert() throws -> String {
        let variableName = "@tvp_" + parameterName.replacingOccurrences(of: "@", with: "")
        let declaration: String
        if let typeName {
            declaration = "DECLARE \(variableName) AS \(typeName);\n"
        } else {
            let definitions = columns.map { "[\($0.name)] \($0.dataType)" }.joined(separator: ", ")
            declaration = "DECLARE \(variableName) TABLE (\(definitions));\n"
        }
        var script = declaration

        for row in rows {
            guard row.values.count == columns.count else {
                throw SQLServerError.invalidArgument("Row column count (\(row.values.count)) does not match schema column count (\(columns.count))")
            }

            let columnNames = columns.map { "[\($0.name)]" }.joined(separator: ", ")
            let values = row.values.map { $0.sqlLiteral() }.joined(separator: ", ")
            script += "INSERT INTO \(variableName) (\(columnNames)) VALUES (\(values));\n"
        }

        return script
    }

    internal var variableName: String {
        "@tvp_" + parameterName.replacingOccurrences(of: "@", with: "")
    }
}

extension SQLServerConnection {
    /// Executes a SQL command or stored procedure with one or more Table-Valued Parameters.
    ///
    /// This is a convenience wrapper that orchestrates the declaration, population, and passing
    /// of TVPs using a single T-SQL batch execution.
    public func executeWithTableParameters(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter]
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        do {
            let script = try Self.buildScript(sql: sql, parameters: tableParameters)
            return self.execute(script)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    /// Async/await version of executeWithTableParameters.
    @available(macOS 12.0, *)
    public func executeWithTableParameters(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter]
    ) async throws -> SQLServerExecutionResult {
        let script = try Self.buildScript(sql: sql, parameters: tableParameters)
        return try await self.execute(script).get()
    }

    private static func buildScript(sql: String, parameters: [SQLServerTableValuedParameter]) throws -> String {
        var script = ""
        for param in parameters {
            script += try param.buildDeclarationAndInsert()
        }

        // Replace parameter references in the original SQL with our generated variable names
        var finalSql = sql
        for param in parameters {
            finalSql = finalSql.replacingOccurrences(of: param.parameterName, with: param.variableName)
        }

        script += finalSql
        return script
    }

    public func execute(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter]
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        executeWithTableParameters(sql, tableParameters: tableParameters)
    }

    @available(macOS 12.0, *)
    public func execute(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter]
    ) async throws -> SQLServerExecutionResult {
        try await executeWithTableParameters(sql, tableParameters: tableParameters)
    }
}

extension SQLServerClient {
    /// Executes a SQL command or stored procedure with one or more Table-Valued Parameters using a connection from the pool.
    public func executeWithTableParameters(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter],
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        do {
            let script = try Self.buildScript(sql: sql, parameters: tableParameters)
            return self.execute(script, on: eventLoop)
        } catch {
            let loop = eventLoop ?? eventLoopGroup.next()
            return loop.makeFailedFuture(error)
        }
    }

    /// Async/await version of executeWithTableParameters for SQLServerClient.
    @available(macOS 12.0, *)
    public func executeWithTableParameters(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter],
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        let script = try Self.buildScript(sql: sql, parameters: tableParameters)
        return try await self.execute(script, on: eventLoop)
    }

    private static func buildScript(sql: String, parameters: [SQLServerTableValuedParameter]) throws -> String {
        var script = ""
        for param in parameters {
            script += try param.buildDeclarationAndInsert()
        }

        var finalSql = sql
        for param in parameters {
            finalSql = finalSql.replacingOccurrences(of: param.parameterName, with: param.variableName)
        }

        script += finalSql
        return script
    }

    public func execute(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter],
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        executeWithTableParameters(sql, tableParameters: tableParameters, on: eventLoop)
    }

    @available(macOS 12.0, *)
    public func execute(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter],
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await executeWithTableParameters(sql, tableParameters: tableParameters, on: eventLoop)
    }
}
