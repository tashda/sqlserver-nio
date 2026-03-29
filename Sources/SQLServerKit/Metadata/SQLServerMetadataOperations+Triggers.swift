import Foundation
import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {
    // MARK: - Triggers

    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[TriggerMetadata]> {
        let definitionSelect = self.configuration.includeTriggerDefinitions && table != nil ? "CAST(m.definition AS NVARCHAR(4000))" : "NULL"
        let moduleJoin = self.configuration.includeTriggerDefinitions && table != nil ? "LEFT JOIN \(qualified(database, object: "sys.sql_modules")) AS m WITH (NOLOCK) ON tr.object_id = m.object_id" : ""
        let commentSelect = includeComments ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment" : ""
        let commentJoin = includeComments ? "LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS ep WITH (NOLOCK) ON ep.class = 1 AND ep.major_id = tr.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'" : ""

        var sql = "SELECT schema_name = s.name, table_name = o.name, trigger_name = tr.name, tr.is_instead_of_trigger, tr.is_disabled, definition = \(definitionSelect)\(commentSelect) FROM \(qualified(database, object: "sys.triggers")) tr WITH (NOLOCK) JOIN \(qualified(database, object: "sys.objects")) o WITH (NOLOCK) ON tr.parent_id = o.object_id AND o.type IN ('U','V') JOIN \(qualified(database, object: "sys.schemas")) s WITH (NOLOCK) ON o.schema_id = s.schema_id \(moduleJoin) \(commentJoin) WHERE tr.parent_class = 1"
        if let schema { sql += " AND s.name = N'\(SQLServerSQL.escapeLiteral(schema))'" }
        if let table { sql += " AND o.name = N'\(SQLServerSQL.escapeLiteral(table))'" }
        sql += " ORDER BY s.name, o.name, tr.name;"

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let s = row.column("schema_name")?.string, let t = row.column("table_name")?.string, let n = row.column("trigger_name")?.string else { return nil }
                return TriggerMetadata(schema: s, table: t, name: n, isInsteadOf: row.column("is_instead_of_trigger")?.bool ?? false, isDisabled: row.column("is_disabled")?.bool ?? false, definition: row.column("definition")?.string, comment: row.column("comment")?.string)
            }
        }
    }

    internal func fetchObjectIndexDetails(database: String?, schema: String, object: String) -> EventLoopFuture<[(name: String, isUnique: Bool, isClustered: Bool, indexType: Int, filterDefinition: String?, optionClause: String?, storageClause: String?, columns: [IndexColumnMetadata])]> {
        let dbPrefix = effectiveDatabase(database).map { "\(SQLServerSQL.escapeIdentifier($0))." } ?? ""
        let sql = """
        SELECT
            i.name AS index_name,
            i.is_unique,
            i.type AS index_type,
            i.is_padded,
            i.fill_factor,
            i.allow_row_locks,
            i.allow_page_locks,
            i.ignore_dup_key,
            CAST(i.filter_definition AS NVARCHAR(4000)) AS filter_definition,
            ds.name AS data_space_name,
            ps.name AS partition_scheme_name,
            st.no_recompute,
            ic.key_ordinal,
            ic.is_descending_key,
            ic.is_included_column,
            c.name AS column_name,
            ic.partition_ordinal,
            pcomp.comp_desc AS compression_desc
        FROM \(dbPrefix)sys.indexes AS i
        JOIN \(dbPrefix)sys.objects AS o ON i.object_id = o.object_id AND o.type IN ('U','V')
        JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
        JOIN \(dbPrefix)sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id
        JOIN \(dbPrefix)sys.stats AS st ON st.object_id = i.object_id AND st.stats_id = i.index_id
        LEFT JOIN \(dbPrefix)sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
        JOIN \(dbPrefix)sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN \(dbPrefix)sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        OUTER APPLY (
            SELECT CASE WHEN MIN(p.data_compression_desc) = MAX(p.data_compression_desc)
                        THEN MIN(p.data_compression_desc) ELSE NULL END AS comp_desc
            FROM \(dbPrefix)sys.partitions AS p
            WHERE p.object_id = i.object_id AND p.index_id = i.index_id
        ) AS pcomp
        WHERE s.name = N'\(SQLServerSQL.escapeLiteral(schema))'
          AND o.name = N'\(SQLServerSQL.escapeLiteral(object))'
          AND i.index_id > 0
          AND i.is_hypothetical = 0
        ORDER BY i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id;
        """

        struct Partial {
            var isUnique: Bool
            var isClustered: Bool
            var indexType: Int
            var filterDefinition: String?
            var columns: [IndexColumnMetadata]
            var partitionColumns: [String]
            var dataSpaceName: String?
            var partitionSchemeName: String?
            var isPadded: Bool
            var fillFactor: Int
            var allowRowLocks: Bool
            var allowPageLocks: Bool
            var ignoreDupKey: Bool
            var compression: String?
            var noRecompute: Bool
        }

        return queryExecutor(sql).map { rows in
            var grouped: [String: Partial] = [:]
            for row in rows {
                guard let indexName = row.column("index_name")?.string else { continue }
                var partial = grouped[indexName] ?? Partial(
                    isUnique: (row.column("is_unique")?.int ?? 0) != 0,
                    isClustered: (row.column("index_type")?.int ?? 0) == 1,
                    indexType: row.column("index_type")?.int ?? 0,
                    filterDefinition: row.column("filter_definition")?.string,
                    columns: [],
                    partitionColumns: [],
                    dataSpaceName: row.column("data_space_name")?.string,
                    partitionSchemeName: row.column("partition_scheme_name")?.string,
                    isPadded: (row.column("is_padded")?.int ?? 0) != 0,
                    fillFactor: row.column("fill_factor")?.int ?? 0,
                    allowRowLocks: (row.column("allow_row_locks")?.int ?? 1) != 0,
                    allowPageLocks: (row.column("allow_page_locks")?.int ?? 1) != 0,
                    ignoreDupKey: (row.column("ignore_dup_key")?.int ?? 0) != 0,
                    compression: row.column("compression_desc")?.string,
                    noRecompute: (row.column("no_recompute")?.int ?? 0) != 0
                )
                if let columnName = row.column("column_name")?.string {
                    partial.columns.append(
                        IndexColumnMetadata(
                            column: columnName,
                            ordinal: row.column("key_ordinal")?.int ?? 0,
                            isDescending: (row.column("is_descending_key")?.int ?? 0) != 0,
                            isIncluded: (row.column("is_included_column")?.int ?? 0) != 0
                        )
                    )
                    if (row.column("partition_ordinal")?.int ?? 0) > 0 {
                        partial.partitionColumns.append(columnName)
                    }
                }
                grouped[indexName] = partial
            }

            func buildOptions(from partial: Partial) -> String? {
                var parts: [String] = []
                parts.append("PAD_INDEX = \(partial.isPadded ? "ON" : "OFF")")
                if partial.fillFactor > 0 { parts.append("FILLFACTOR = \(partial.fillFactor)") }
                parts.append("ALLOW_ROW_LOCKS = \(partial.allowRowLocks ? "ON" : "OFF")")
                parts.append("ALLOW_PAGE_LOCKS = \(partial.allowPageLocks ? "ON" : "OFF")")
                if partial.ignoreDupKey { parts.append("IGNORE_DUP_KEY = ON") }
                if partial.noRecompute { parts.append("STATISTICS_NORECOMPUTE = ON") }
                if let compression = partial.compression, compression != "NONE" {
                    parts.append("DATA_COMPRESSION = \(compression)")
                }
                return parts.isEmpty ? nil : parts.joined(separator: ", ")
            }

            func buildStorage(from partial: Partial) -> String? {
                if let scheme = partial.partitionSchemeName, !scheme.isEmpty {
                    if !partial.partitionColumns.isEmpty {
                        let cols = partial.partitionColumns.map { "\(SQLServerSQL.escapeIdentifier($0))" }.joined(separator: ", ")
                        return "ON \(SQLServerSQL.escapeIdentifier(scheme))(\(cols))"
                    }
                    return "ON \(SQLServerSQL.escapeIdentifier(scheme))"
                }
                if let dataSpace = partial.dataSpaceName, !dataSpace.isEmpty {
                    return "ON \(SQLServerSQL.escapeIdentifier(dataSpace))"
                }
                return nil
            }

            return grouped.map { name, partial in
                (
                    name: name,
                    isUnique: partial.isUnique,
                    isClustered: partial.isClustered,
                    indexType: partial.indexType,
                    filterDefinition: partial.filterDefinition,
                    optionClause: buildOptions(from: partial),
                    storageClause: buildStorage(from: partial),
                    columns: partial.columns
                )
            }.sorted { $0.name < $1.name }
        }
    }
}
