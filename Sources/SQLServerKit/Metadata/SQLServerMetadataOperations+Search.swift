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
}
