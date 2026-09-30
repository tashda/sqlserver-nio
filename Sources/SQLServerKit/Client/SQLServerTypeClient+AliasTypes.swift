import Foundation

extension SQLServerTypeClient {
    // MARK: - Alias Types

    /// Creates an alias data type: `CREATE TYPE [schema].[name] FROM <base type> [NOT] NULL`.
    @available(macOS 12.0, *)
    public func createAliasType(
        name: String,
        schema: String = "dbo",
        baseType: SQLDataType,
        isNullable: Bool = true
    ) async throws {
        _ = try await client.execute(Self.aliasTypeSQL(name: name, schema: schema, baseType: baseType, isNullable: isNullable))
    }

    internal static func aliasTypeSQL(name: String, schema: String, baseType: SQLDataType, isNullable: Bool) -> String {
        "CREATE TYPE \(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name)) FROM \(baseType.toSqlString()) \(isNullable ? "NULL" : "NOT NULL")"
    }
}
