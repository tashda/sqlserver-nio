import Foundation

public struct SQLServerTableValuedParameter: Sendable {
    public struct Column: Sendable {
        public var name: String
        public var dataType: SQLDataType
        
        public init(name: String, dataType: SQLDataType) {
            self.name = name
            self.dataType = dataType
        }
    }
    
    public struct Row: Sendable {
        public var values: [SQLServerLiteralValue]
        
        public init(values: [SQLServerLiteralValue]) {
            self.values = values
        }
    }
    
    public var name: String
    public var columns: [Column]
    public var rows: [Row]
    
    public init(name: String, columns: [Column], rows: [Row] = []) {
        self.name = name
        self.columns = columns
        self.rows = rows
    }
    
    internal func declarationSQL() throws -> String {
        guard !columns.isEmpty else {
            throw SQLServerError.invalidArgument("Table-valued parameter \(name) has no columns.")
        }
        let columnList = columns.map { column in
            "[\(Self.escapeIdentifier(column.name))] \(column.dataType.toSqlString())"
        }.joined(separator: ",\n    ")
        return """
        DECLARE @\(name) TABLE (
            \(columnList)
        );
        """
    }
    
    internal func insertStatements() throws -> String {
        guard !rows.isEmpty else {
            return ""
        }
        let expected = columns.count
        for row in rows where row.values.count != expected {
            throw SQLServerError.invalidArgument("Row for TVP \(name) contains \(row.values.count) values but \(expected) columns were defined.")
        }
        
        let columnList = columns.map { "[\(Self.escapeIdentifier($0.name))]" }.joined(separator: ", ")
        let valueBatches = rows.map { row -> String in
            let literals = row.values.map { $0.sqlLiteral() }.joined(separator: ", ")
            return "(\(literals))"
        }.joined(separator: ",\n")
        
        return """
        INSERT INTO @\(name) (\(columnList))
        VALUES
        \(valueBatches);
        """
    }
    
    private static func escapeIdentifier(_ identifier: String) -> String {
        identifier.replacingOccurrences(of: "]", with: "]]")
    }
}

extension SQLServerClient {
    public func execute(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter],
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        do {
            let script = try Self.buildScript(sql: sql, parameters: tableParameters)
            return execute(script, on: eventLoop)
        } catch {
            let loop = eventLoop ?? eventLoopGroup.next()
            return loop.makeFailedFuture(error)
        }
    }
    
    @available(macOS 12.0, *)
    public func execute(
        _ sql: String,
        tableParameters: [SQLServerTableValuedParameter],
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        let script = try Self.buildScript(sql: sql, parameters: tableParameters)
        return try await execute(script, on: eventLoop)
    }
    
    private static func buildScript(sql: String, parameters: [SQLServerTableValuedParameter]) throws -> String {
        guard !parameters.isEmpty else { return sql }
        var statements: [String] = []
        for parameter in parameters {
            statements.append(try parameter.declarationSQL())
            let inserts = try parameter.insertStatements()
            if !inserts.isEmpty {
                statements.append(inserts)
            }
        }
        statements.append(sql)
        return statements.joined(separator: "\n")
    }
}
