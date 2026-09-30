import Foundation

extension SQLServerRoutineClient {
    // MARK: - Inline Table-Valued Functions

    /// Creates an inline table-valued function: `RETURNS TABLE AS RETURN (query)`.
    @available(macOS 12.0, *)
    @discardableResult
    public func createInlineTableValuedFunction(
        name: String,
        parameters: [FunctionParameter] = [],
        query: String,
        schema: String = "dbo",
        options: RoutineOptions = RoutineOptions()
    ) async throws -> [SQLServerStreamMessage] {
        let sql = Self.inlineTableValuedFunctionSQL(name: name, parameters: parameters, query: query, schema: schema, options: options)
        return try await client.execute(sql).messages
    }

    internal static func inlineTableValuedFunctionSQL(
        name: String,
        parameters: [FunctionParameter],
        query: String,
        schema: String,
        options: RoutineOptions
    ) -> String {
        var sql = "CREATE FUNCTION \(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name))"
        if parameters.isEmpty {
            sql += "()"
        } else {
            let list = parameters.map { parameter in
                var text = "@\(parameter.name) \(parameter.dataType.toSqlString())"
                if let defaultValue = parameter.defaultValue { text += " = \(defaultValue)" }
                return text
            }
            sql += "\n(\n    \(list.joined(separator: ",\n    "))\n)"
        }
        sql += "\nRETURNS TABLE"
        if let optionClause = buildOptionClause(from: options, allowRecompile: false) {
            sql += "\n\(optionClause)"
        }
        sql += "\nAS\nRETURN\n(\n\(query)\n)"
        return sql
    }
}
