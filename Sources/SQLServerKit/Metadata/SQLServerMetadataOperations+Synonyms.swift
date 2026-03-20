import Foundation
import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {
    // MARK: - Synonyms

    internal func listSynonyms(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[SynonymMetadata]> {
        let qualifiedSynonyms = qualified(database, object: "sys.synonyms")
        let qualifiedSchemas = qualified(database, object: "sys.schemas")

        var predicates: [String] = []
        if let schema {
            predicates.append("s.name = N'\(Self.escapeLiteral(schema))'")
        }
        let whereClause = predicates.isEmpty ? "" : "WHERE " + predicates.joined(separator: " AND ")

        let commentSelect = includeComments ? ", CAST(ep.value AS NVARCHAR(4000)) AS comment" : ""
        let commentJoin = includeComments ? "LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS ep WITH (NOLOCK) ON ep.major_id = sy.object_id AND ep.minor_id = 0 AND ep.class = 1 AND ep.name = N'MS_Description'" : ""

        let sql = """
        SELECT s.name AS schema_name, sy.name AS synonym_name, sy.base_object_name\(commentSelect)
        FROM \(qualifiedSynonyms) AS sy WITH (NOLOCK)
        INNER JOIN \(qualifiedSchemas) AS s WITH (NOLOCK) ON s.schema_id = sy.schema_id
        \(commentJoin)
        \(whereClause)
        ORDER BY s.name, sy.name
        """

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schemaName = row.column("schema_name")?.string,
                      let synonymName = row.column("synonym_name")?.string,
                      let baseObject = row.column("base_object_name")?.string
                else { return nil }
                return SynonymMetadata(
                    schema: schemaName,
                    name: synonymName,
                    baseObjectName: baseObject,
                    comment: includeComments ? row.column("comment")?.string : nil
                )
            }
        }
    }
}
