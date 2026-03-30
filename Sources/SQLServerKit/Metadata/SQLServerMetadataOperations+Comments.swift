import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {

    // MARK: - Object Comments (Extended Properties)

    /// Fetch the MS_Description extended property for any schema-scoped object.
    /// Works for tables, views, functions, procedures, sequences, etc.
    public func objectComment(
        database: String? = nil,
        schema: String,
        name: String
    ) -> EventLoopFuture<String?> {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedName = SQLServerSQL.escapeLiteral(name)

        let sql = """
            SELECT CAST(ep.value AS NVARCHAR(MAX)) AS comment
            FROM \(qualified(database, object: "sys.extended_properties")) ep WITH (NOLOCK)
            JOIN \(qualified(database, object: "sys.objects")) o WITH (NOLOCK) ON ep.major_id = o.object_id
            JOIN \(qualified(database, object: "sys.schemas")) s WITH (NOLOCK) ON o.schema_id = s.schema_id
            WHERE s.name = N'\(escapedSchema)'
              AND o.name = N'\(escapedName)'
              AND ep.name = N'MS_Description'
              AND ep.minor_id = 0;
            """

        return queryExecutor(sql).map { rows in
            rows.first?.column("comment")?.string
        }
    }

    // MARK: - Trigger Details

    /// Fetch detailed metadata for a specific trigger including definition, events, and comment.
    public func triggerDetails(
        database: String? = nil,
        schema: String,
        table: String,
        name: String
    ) -> EventLoopFuture<TriggerDetails?> {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedTable = SQLServerSQL.escapeLiteral(table)
        let escapedName = SQLServerSQL.escapeLiteral(name)

        let sql = """
            SELECT
                tr.name AS trigger_name,
                CAST(m.definition AS NVARCHAR(MAX)) AS definition,
                tr.is_instead_of_trigger,
                tr.is_disabled,
                OBJECTPROPERTY(tr.object_id, 'ExecIsInsertTrigger') AS is_insert,
                OBJECTPROPERTY(tr.object_id, 'ExecIsUpdateTrigger') AS is_update,
                OBJECTPROPERTY(tr.object_id, 'ExecIsDeleteTrigger') AS is_delete,
                ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment
            FROM \(qualified(database, object: "sys.triggers")) tr WITH (NOLOCK)
            JOIN \(qualified(database, object: "sys.objects")) o WITH (NOLOCK) ON tr.parent_id = o.object_id
            JOIN \(qualified(database, object: "sys.schemas")) s WITH (NOLOCK) ON o.schema_id = s.schema_id
            LEFT JOIN \(qualified(database, object: "sys.sql_modules")) m WITH (NOLOCK) ON tr.object_id = m.object_id
            LEFT JOIN \(qualified(database, object: "sys.extended_properties")) ep WITH (NOLOCK)
                ON ep.class = 1 AND ep.major_id = tr.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'
            WHERE s.name = N'\(escapedSchema)'
              AND o.name = N'\(escapedTable)'
              AND tr.name = N'\(escapedName)';
            """

        return queryExecutor(sql).map { rows in
            guard let row = rows.first,
                  let triggerName = row.column("trigger_name")?.string else { return nil }
            return TriggerDetails(
                name: triggerName,
                schema: schema,
                table: table,
                definition: row.column("definition")?.string,
                isInsteadOf: row.column("is_instead_of_trigger")?.bool ?? false,
                isDisabled: row.column("is_disabled")?.bool ?? false,
                isInsertTrigger: row.column("is_insert")?.int == 1,
                isUpdateTrigger: row.column("is_update")?.int == 1,
                isDeleteTrigger: row.column("is_delete")?.int == 1,
                comment: row.column("comment")?.string
            )
        }
    }
}

/// Detailed metadata for a SQL Server DML trigger.
public struct TriggerDetails: Sendable {
    public let name: String
    public let schema: String
    public let table: String
    public let definition: String?
    public let isInsteadOf: Bool
    public let isDisabled: Bool
    public let isInsertTrigger: Bool
    public let isUpdateTrigger: Bool
    public let isDeleteTrigger: Bool
    public let comment: String?

    public init(
        name: String, schema: String, table: String, definition: String?,
        isInsteadOf: Bool, isDisabled: Bool,
        isInsertTrigger: Bool, isUpdateTrigger: Bool, isDeleteTrigger: Bool,
        comment: String?
    ) {
        self.name = name; self.schema = schema; self.table = table
        self.definition = definition; self.isInsteadOf = isInsteadOf; self.isDisabled = isDisabled
        self.isInsertTrigger = isInsertTrigger; self.isUpdateTrigger = isUpdateTrigger
        self.isDeleteTrigger = isDeleteTrigger; self.comment = comment
    }
}
