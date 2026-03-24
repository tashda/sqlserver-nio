import Foundation
import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {
    // MARK: - Schemas

    internal func listSchemas(in database: String? = nil) -> EventLoopFuture<[SchemaMetadata]> {
        let qualifiedSchemas = qualified(database, object: "sys.schemas")
        var predicates: [String] = []
        if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }
        let sql = """
        SELECT s.name
        FROM \(qualifiedSchemas) AS s WITH (NOLOCK)
        \(predicates.isEmpty ? "" : "WHERE " + predicates.joined(separator: " AND "))
        ORDER BY s.name;
        """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return SchemaMetadata(name: name)
            }
        }
    }

    // MARK: - Tables

    internal func listTables(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[TableMetadata]> {
        let qualifiedObjects = qualified(database, object: "sys.objects")
        let qualifiedSchemas = qualified(database, object: "sys.schemas")
        let qualifiedTables = qualified(database, object: "sys.tables")
        let qualifiedPeriods = qualified(database, object: "sys.periods")
        let qualifiedColumns = qualified(database, object: "sys.columns")
        let qualifiedExtended = qualified(database, object: "sys.extended_properties")

        var predicates: [String] = [
            "o.type IN ('U', 'S', 'V', 'TT')"
        ]

        if let schema {
            predicates.append("s.name = N'\(Self.escapeLiteral(schema))'")
        }

        if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }

        let whereClause = predicates.isEmpty ? "" : "WHERE " + predicates.joined(separator: " AND ")

        let commentSelect = includeComments ? ", CAST(ep.value AS NVARCHAR(4000)) AS comment" : ""
        let commentJoin = includeComments ? "LEFT JOIN \(qualifiedExtended) AS ep WITH (NOLOCK) ON ep.major_id = o.object_id AND ep.minor_id = 0 AND ep.class = 1 AND ep.name = N'MS_Description'" : ""

        let sql = """
        SELECT
            s.name AS schema_name,
            o.name AS object_name,
            CASE
                WHEN o.type = 'V' THEN 'VIEW'
                WHEN o.type = 'TT' THEN 'TABLE TYPE'
                WHEN o.type = 'U' THEN 'TABLE'
                WHEN o.type = 'S' OR o.is_ms_shipped = 1 THEN 'SYSTEM TABLE'
                ELSE o.type_desc
            END AS table_type,
            o.is_ms_shipped,
            t.temporal_type,
            ht.name AS history_table,
            hs.name AS history_schema,
            pc_start.name AS period_start_column,
            pc_end.name AS period_end_column,
            t.is_memory_optimized,
            t.durability_desc\(commentSelect)
        FROM \(qualifiedObjects) AS o WITH (NOLOCK)
        INNER JOIN \(qualifiedSchemas) AS s WITH (NOLOCK)
            ON s.schema_id = o.schema_id
        LEFT JOIN \(qualifiedTables) AS t WITH (NOLOCK)
            ON t.object_id = o.object_id
        LEFT JOIN \(qualifiedTables) AS ht WITH (NOLOCK)
            ON ht.object_id = t.history_table_id
        LEFT JOIN \(qualifiedSchemas) AS hs WITH (NOLOCK)
            ON hs.schema_id = ht.schema_id
        LEFT JOIN \(qualifiedPeriods) AS p WITH (NOLOCK)
            ON p.object_id = t.object_id
        LEFT JOIN \(qualifiedColumns) AS pc_start WITH (NOLOCK)
            ON pc_start.object_id = t.object_id AND pc_start.column_id = p.start_column_id
        LEFT JOIN \(qualifiedColumns) AS pc_end WITH (NOLOCK)
            ON pc_end.object_id = t.object_id AND pc_end.column_id = p.end_column_id
        \(commentJoin)
        \(whereClause)
        ORDER BY s.name, o.name;
        """

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("object_name")?.string,
                    let tableType = row.column("table_type")?.string
                else {
                    return nil
                }

                if let schema, schemaName.caseInsensitiveCompare(schema) != .orderedSame {
                    return nil
                }

                if !self.configuration.includeSystemSchemas,
                   schemaName.caseInsensitiveCompare("sys") == .orderedSame ||
                   schemaName.caseInsensitiveCompare("INFORMATION_SCHEMA") == .orderedSame {
                    return nil
                }

                let normalizedType = tableType.uppercased()
                let isSystemObject = normalizedType.contains("SYSTEM") || (row.column("is_ms_shipped")?.int ?? 0) != 0

                return TableMetadata(
                    schema: schemaName,
                    name: tableName,
                    type: normalizedType,
                    isSystemObject: isSystemObject,
                    comment: includeComments ? row.column("comment")?.string : nil,
                    temporalType: row.column("temporal_type")?.int ?? 0,
                    historyTableSchema: row.column("history_schema")?.string,
                    historyTableName: row.column("history_table")?.string,
                    periodStartColumn: row.column("period_start_column")?.string,
                    periodEndColumn: row.column("period_end_column")?.string,
                    isMemoryOptimized: (row.column("is_memory_optimized")?.int ?? 0) != 0,
                    durabilityDescription: row.column("durability_desc")?.string
                )
            }
        }
    }
}
