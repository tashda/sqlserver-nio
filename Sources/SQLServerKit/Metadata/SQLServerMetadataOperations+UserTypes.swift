import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {

    // MARK: - User-Defined Types

    /// Lists all user-defined types (alias types, table types, CLR types) in the specified database.
    public func listUserTypes(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[UserTypeMetadata]> {
        let commentSelect = includeComments
            ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment"
            : ""
        let commentJoin = includeComments
            ? "LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS ep WITH (NOLOCK) ON ep.class = 6 AND ep.major_id = t.user_type_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'"
            : ""

        var sql = """
            SELECT
                s.name AS schema_name,
                t.name AS type_name,
                t.is_table_type,
                t.is_assembly_type,
                CASE WHEN t.is_table_type = 1 THEN 'TABLE_TYPE'
                     WHEN t.is_assembly_type = 1 THEN 'CLR'
                     ELSE 'ALIAS' END AS type_kind,
                bt.name AS base_type_name,
                t.max_length,
                t.precision,
                t.scale,
                t.is_nullable\(commentSelect)
            FROM \(qualified(database, object: "sys.types")) t WITH (NOLOCK)
            JOIN \(qualified(database, object: "sys.schemas")) s WITH (NOLOCK) ON t.schema_id = s.schema_id
            LEFT JOIN \(qualified(database, object: "sys.types")) bt WITH (NOLOCK) ON t.system_type_id = bt.user_type_id AND bt.is_user_defined = 0
            \(commentJoin)
            WHERE t.is_user_defined = 1
            """

        if let schema {
            sql += " AND s.name = N'\(SQLServerSQL.escapeLiteral(schema))'"
        }
        sql += " ORDER BY s.name, t.name;"

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schemaName = row.column("schema_name")?.string,
                      let typeName = row.column("type_name")?.string,
                      let kindString = row.column("type_kind")?.string,
                      let kind = UserTypeMetadata.Kind(rawValue: kindString) else { return nil }
                return UserTypeMetadata(
                    name: typeName,
                    schema: schemaName,
                    kind: kind,
                    baseType: row.column("base_type_name")?.string,
                    maxLength: row.column("max_length")?.int,
                    precision: row.column("precision")?.uint8,
                    scale: row.column("scale")?.uint8,
                    isNullable: row.column("is_nullable")?.bool ?? true,
                    isAssemblyType: row.column("is_assembly_type")?.bool ?? false,
                    comment: row.column("comment")?.string
                )
            }
        }
    }

    /// Returns detailed metadata for a specific user-defined type.
    public func userTypeDetails(
        database: String? = nil,
        schema: String,
        name: String
    ) -> EventLoopFuture<UserTypeMetadata?> {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedName = SQLServerSQL.escapeLiteral(name)

        let sql = """
            SELECT
                s.name AS schema_name,
                t.name AS type_name,
                t.is_table_type,
                t.is_assembly_type,
                CASE WHEN t.is_table_type = 1 THEN 'TABLE_TYPE'
                     WHEN t.is_assembly_type = 1 THEN 'CLR'
                     ELSE 'ALIAS' END AS type_kind,
                bt.name AS base_type_name,
                t.max_length,
                t.precision,
                t.scale,
                t.is_nullable,
                ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment
            FROM \(qualified(database, object: "sys.types")) t WITH (NOLOCK)
            JOIN \(qualified(database, object: "sys.schemas")) s WITH (NOLOCK) ON t.schema_id = s.schema_id
            LEFT JOIN \(qualified(database, object: "sys.types")) bt WITH (NOLOCK) ON t.system_type_id = bt.user_type_id AND bt.is_user_defined = 0
            LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS ep WITH (NOLOCK)
                ON ep.class = 6 AND ep.major_id = t.user_type_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'
            WHERE t.is_user_defined = 1
              AND s.name = N'\(escapedSchema)' AND t.name = N'\(escapedName)';
            """

        return queryExecutor(sql).map { rows in
            guard let row = rows.first,
                  let schemaName = row.column("schema_name")?.string,
                  let typeName = row.column("type_name")?.string,
                  let kindString = row.column("type_kind")?.string,
                  let kind = UserTypeMetadata.Kind(rawValue: kindString) else { return nil }
            return UserTypeMetadata(
                name: typeName,
                schema: schemaName,
                kind: kind,
                baseType: row.column("base_type_name")?.string,
                maxLength: row.column("max_length")?.int,
                precision: row.column("precision")?.uint8,
                scale: row.column("scale")?.uint8,
                isNullable: row.column("is_nullable")?.bool ?? true,
                isAssemblyType: row.column("is_assembly_type")?.bool ?? false,
                comment: row.column("comment")?.string
            )
        }
    }
}
