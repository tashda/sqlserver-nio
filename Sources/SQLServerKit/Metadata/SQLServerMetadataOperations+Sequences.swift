import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {

    // MARK: - Sequences

    /// Lists all sequences in the specified database and optional schema.
    public func listSequences(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[SequenceMetadata]> {
        let commentSelect = includeComments
            ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment"
            : ""
        let commentJoin = includeComments
            ? "LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS ep WITH (NOLOCK) ON ep.class = 1 AND ep.major_id = sq.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'"
            : ""

        var sql = """
            SELECT
                s.name AS schema_name,
                sq.name AS sequence_name,
                TYPE_NAME(sq.system_type_id) AS data_type,
                CAST(sq.start_value AS NVARCHAR(50)) AS start_value,
                CAST(sq.increment AS NVARCHAR(50)) AS increment_by,
                CAST(sq.minimum_value AS NVARCHAR(50)) AS min_value,
                CAST(sq.maximum_value AS NVARCHAR(50)) AS max_value,
                sq.is_cycling,
                sq.cache_size,
                CAST(sq.current_value AS NVARCHAR(50)) AS current_value\(commentSelect)
            FROM \(qualified(database, object: "sys.sequences")) sq WITH (NOLOCK)
            JOIN \(qualified(database, object: "sys.schemas")) s WITH (NOLOCK) ON sq.schema_id = s.schema_id
            \(commentJoin)
            WHERE 1=1
            """

        if let schema {
            sql += " AND s.name = N'\(SQLServerSQL.escapeLiteral(schema))'"
        }
        sql += " ORDER BY s.name, sq.name;"

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schemaName = row.column("schema_name")?.string,
                      let seqName = row.column("sequence_name")?.string else { return nil }
                return SequenceMetadata(
                    name: seqName,
                    schema: schemaName,
                    dataType: row.column("data_type")?.string ?? "bigint",
                    startValue: row.column("start_value")?.string ?? "1",
                    incrementBy: row.column("increment_by")?.string ?? "1",
                    minValue: row.column("min_value")?.string ?? "",
                    maxValue: row.column("max_value")?.string ?? "",
                    isCycling: row.column("is_cycling")?.bool ?? false,
                    cacheSize: row.column("cache_size")?.int ?? 0,
                    currentValue: row.column("current_value")?.string,
                    comment: row.column("comment")?.string
                )
            }
        }
    }

    /// Returns detailed metadata for a specific sequence.
    public func sequenceDetails(
        database: String? = nil,
        schema: String,
        name: String
    ) -> EventLoopFuture<SequenceMetadata?> {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedName = SQLServerSQL.escapeLiteral(name)

        let sql = """
            SELECT
                s.name AS schema_name,
                sq.name AS sequence_name,
                TYPE_NAME(sq.system_type_id) AS data_type,
                CAST(sq.start_value AS NVARCHAR(50)) AS start_value,
                CAST(sq.increment AS NVARCHAR(50)) AS increment_by,
                CAST(sq.minimum_value AS NVARCHAR(50)) AS min_value,
                CAST(sq.maximum_value AS NVARCHAR(50)) AS max_value,
                sq.is_cycling,
                sq.cache_size,
                CAST(sq.current_value AS NVARCHAR(50)) AS current_value,
                ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment
            FROM \(qualified(database, object: "sys.sequences")) sq WITH (NOLOCK)
            JOIN \(qualified(database, object: "sys.schemas")) s WITH (NOLOCK) ON sq.schema_id = s.schema_id
            LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS ep WITH (NOLOCK)
                ON ep.class = 1 AND ep.major_id = sq.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'
            WHERE s.name = N'\(escapedSchema)' AND sq.name = N'\(escapedName)';
            """

        return queryExecutor(sql).map { rows in
            guard let row = rows.first,
                  let schemaName = row.column("schema_name")?.string,
                  let seqName = row.column("sequence_name")?.string else { return nil }
            return SequenceMetadata(
                name: seqName,
                schema: schemaName,
                dataType: row.column("data_type")?.string ?? "bigint",
                startValue: row.column("start_value")?.string ?? "1",
                incrementBy: row.column("increment_by")?.string ?? "1",
                minValue: row.column("min_value")?.string ?? "",
                maxValue: row.column("max_value")?.string ?? "",
                isCycling: row.column("is_cycling")?.bool ?? false,
                cacheSize: row.column("cache_size")?.int ?? 0,
                currentValue: row.column("current_value")?.string,
                comment: row.column("comment")?.string
            )
        }
    }
}
