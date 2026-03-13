import Foundation
import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {
    // MARK: - Search

    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default
    ) -> EventLoopFuture<[MetadataSearchHit]> {
        guard scopes.rawValue != 0 else { return eventLoop.makeSucceededFuture([]) }
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%' COLLATE Latin1_General_CI_AI"
        let includeSystem = self.configuration.includeSystemSchemas
        let resolvedDatabase = effectiveDatabase(database)
        let dbPrefix = resolvedDatabase.map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""

        var selects: [String] = []
        let schemaPred = schema.map { "s.name = N'\(SQLServerMetadataOperations.escapeLiteral($0))'" } ?? (includeSystem ? "1=1" : "s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        
        if scopes.contains(.objectNames) {
            selects.append("SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'name' AS match_kind, NULL AS detail FROM \(dbPrefix)sys.objects o JOIN \(dbPrefix)sys.schemas s ON o.schema_id = s.schema_id WHERE \(schemaPred) AND o.name LIKE \(pattern)")
        }
        if scopes.contains(.definitions) {
            selects.append("SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'definition' AS match_kind, CAST(m.definition AS NVARCHAR(4000)) AS detail FROM \(dbPrefix)sys.objects o JOIN \(dbPrefix)sys.schemas s ON o.schema_id = s.schema_id JOIN \(dbPrefix)sys.sql_modules m ON o.object_id = m.object_id WHERE \(schemaPred) AND m.definition COLLATE Latin1_General_CI_AI LIKE \(pattern)")
        }
        if scopes.contains(.columns) {
            selects.append("SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'column' AS match_kind, c.name AS detail FROM \(dbPrefix)sys.objects o JOIN \(dbPrefix)sys.schemas s ON o.schema_id = s.schema_id JOIN \(dbPrefix)sys.columns c ON o.object_id = c.object_id WHERE \(schemaPred) AND c.name COLLATE Latin1_General_CI_AI LIKE \(pattern)")
        }
        
        guard !selects.isEmpty else { return eventLoop.makeSucceededFuture([]) }
        let sql = selects.joined(separator: "\nUNION ALL\n") + "\nORDER BY schema_name, object_name, match_kind;"

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let s = row.column("schema_name")?.string, let o = row.column("object_name")?.string, let td = row.column("type_desc")?.string, let mRaw = row.column("match_kind")?.string, let m = MetadataSearchHit.MatchKind(rawValue: mRaw.lowercased()) else { return nil }
                return MetadataSearchHit(schema: s, name: o, type: ObjectDefinition.ObjectType.from(typeDesc: td), matchKind: m, detail: row.column("detail")?.string)
            }
        }
    }

    // MARK: - Per-Type Search

    public func searchTables(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[TableSearchResult]> {
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%'"
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = """
            SELECT TOP \(limit)
                TABLE_SCHEMA, TABLE_NAME
            FROM \(dbPrefix)INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
              AND TABLE_NAME COLLATE Latin1_General_CI_AI LIKE \(pattern)
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schema = row.column("TABLE_SCHEMA")?.string,
                      let name = row.column("TABLE_NAME")?.string else { return nil }
                return TableSearchResult(schema: schema, name: name)
            }
        }
    }

    public func searchViews(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[ViewSearchResult]> {
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%'"
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = """
            SELECT TOP \(limit)
                TABLE_SCHEMA, TABLE_NAME,
                COALESCE(VIEW_DEFINITION, '') AS VIEW_DEFINITION
            FROM \(dbPrefix)INFORMATION_SCHEMA.VIEWS
            WHERE (
                TABLE_NAME COLLATE Latin1_General_CI_AI LIKE \(pattern)
                OR COALESCE(VIEW_DEFINITION, '') COLLATE Latin1_General_CI_AI LIKE \(pattern)
            )
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schema = row.column("TABLE_SCHEMA")?.string,
                      let name = row.column("TABLE_NAME")?.string else { return nil }
                let def = row.column("VIEW_DEFINITION")?.string
                return ViewSearchResult(schema: schema, name: name, definitionSnippet: def?.isEmpty == true ? nil : def)
            }
        }
    }

    public func searchFunctions(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[RoutineSearchResult]> {
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%'"
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = """
            SELECT TOP \(limit)
                ROUTINE_SCHEMA, ROUTINE_NAME,
                COALESCE(ROUTINE_DEFINITION, '') AS ROUTINE_DEFINITION
            FROM \(dbPrefix)INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'FUNCTION'
              AND (
                ROUTINE_NAME COLLATE Latin1_General_CI_AI LIKE \(pattern)
                OR COALESCE(ROUTINE_DEFINITION, '') COLLATE Latin1_General_CI_AI LIKE \(pattern)
              )
            ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schema = row.column("ROUTINE_SCHEMA")?.string,
                      let name = row.column("ROUTINE_NAME")?.string else { return nil }
                let def = row.column("ROUTINE_DEFINITION")?.string
                return RoutineSearchResult(schema: schema, name: name, definitionSnippet: def?.isEmpty == true ? nil : def)
            }
        }
    }

    public func searchProcedures(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[RoutineSearchResult]> {
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%'"
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = """
            SELECT TOP \(limit)
                s.name AS schema_name,
                p.name AS procedure_name,
                COALESCE(sm.definition, '') AS definition
            FROM \(dbPrefix)sys.procedures AS p
            JOIN \(dbPrefix)sys.schemas AS s ON p.schema_id = s.schema_id
            LEFT JOIN \(dbPrefix)sys.sql_modules AS sm ON p.object_id = sm.object_id
            WHERE p.is_ms_shipped = 0
              AND (
                p.name COLLATE Latin1_General_CI_AI LIKE \(pattern)
                OR COALESCE(sm.definition, '') COLLATE Latin1_General_CI_AI LIKE \(pattern)
              )
            ORDER BY s.name, p.name
            """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schema = row.column("schema_name")?.string,
                      let name = row.column("procedure_name")?.string else { return nil }
                let def = row.column("definition")?.string
                return RoutineSearchResult(schema: schema, name: name, definitionSnippet: def?.isEmpty == true ? nil : def)
            }
        }
    }

    public func searchTriggers(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[TriggerSearchResult]> {
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%'"
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = """
            SELECT TOP \(limit)
                s.name AS schema_name,
                t.name AS table_name,
                tr.name AS trigger_name,
                STUFF((
                    SELECT ', ' + ev.type_desc
                    FROM \(dbPrefix)sys.trigger_events AS ev
                    WHERE ev.object_id = tr.object_id
                    FOR XML PATH(''), TYPE
                ).value('.', 'nvarchar(max)'), 1, 2, '') AS event_list,
                CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END AS timing
            FROM \(dbPrefix)sys.triggers AS tr
            JOIN \(dbPrefix)sys.tables AS t ON tr.parent_id = t.object_id
            JOIN \(dbPrefix)sys.schemas AS s ON t.schema_id = s.schema_id
            WHERE (
                tr.name COLLATE Latin1_General_CI_AI LIKE \(pattern)
                OR t.name COLLATE Latin1_General_CI_AI LIKE \(pattern)
            )
            ORDER BY s.name, tr.name
            """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schema = row.column("schema_name")?.string,
                      let table = row.column("table_name")?.string,
                      let name = row.column("trigger_name")?.string else { return nil }
                let events = row.column("event_list")?.string ?? ""
                let timing = row.column("timing")?.string ?? "AFTER"
                return TriggerSearchResult(schema: schema, table: table, name: name, events: events, timing: timing)
            }
        }
    }

    public func searchColumns(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[ColumnSearchResult]> {
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%'"
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = """
            SELECT TOP \(limit)
                TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM \(dbPrefix)INFORMATION_SCHEMA.COLUMNS
            WHERE COLUMN_NAME COLLATE Latin1_General_CI_AI LIKE \(pattern)
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schema = row.column("TABLE_SCHEMA")?.string,
                      let table = row.column("TABLE_NAME")?.string,
                      let column = row.column("COLUMN_NAME")?.string,
                      let dataType = row.column("DATA_TYPE")?.string else { return nil }
                return ColumnSearchResult(schema: schema, table: table, column: column, dataType: dataType)
            }
        }
    }

    public func searchIndexes(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[IndexSearchResult]> {
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%'"
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = """
            SELECT TOP \(limit)
                s.name AS schema_name,
                t.name AS table_name,
                i.name AS index_name,
                COALESCE(i.filter_definition, '') AS filter_definition
            FROM \(dbPrefix)sys.indexes AS i
            JOIN \(dbPrefix)sys.tables AS t ON i.object_id = t.object_id
            JOIN \(dbPrefix)sys.schemas AS s ON t.schema_id = s.schema_id
            WHERE i.is_primary_key = 0
              AND i.[type] <> 0
              AND (
                i.name COLLATE Latin1_General_CI_AI LIKE \(pattern)
                OR t.name COLLATE Latin1_General_CI_AI LIKE \(pattern)
              )
            ORDER BY s.name, i.name
            """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schema = row.column("schema_name")?.string,
                      let table = row.column("table_name")?.string,
                      let name = row.column("index_name")?.string else { return nil }
                let filterDef = row.column("filter_definition")?.string
                return IndexSearchResult(schema: schema, table: table, name: name, filterDefinition: filterDef?.isEmpty == true ? nil : filterDef)
            }
        }
    }

    public func searchForeignKeys(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[ForeignKeySearchResult]> {
        let pattern = "N'%\(SQLServerMetadataOperations.escapeLiteral(query))%'"
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = """
            SELECT TOP \(limit)
                s.name AS schema_name,
                t.name AS table_name,
                fk.name AS constraint_name,
                OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
                OBJECT_NAME(fk.referenced_object_id) AS referenced_table
            FROM \(dbPrefix)sys.foreign_keys AS fk
            JOIN \(dbPrefix)sys.tables AS t ON fk.parent_object_id = t.object_id
            JOIN \(dbPrefix)sys.schemas AS s ON t.schema_id = s.schema_id
            WHERE fk.name COLLATE Latin1_General_CI_AI LIKE \(pattern)
            ORDER BY s.name, fk.name
            """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schema = row.column("schema_name")?.string,
                      let table = row.column("table_name")?.string,
                      let name = row.column("constraint_name")?.string,
                      let refSchema = row.column("referenced_schema")?.string,
                      let refTable = row.column("referenced_table")?.string else { return nil }
                return ForeignKeySearchResult(schema: schema, table: table, name: name, referencedSchema: refSchema, referencedTable: refTable)
            }
        }
    }
}
