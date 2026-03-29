import NIO
import SQLServerTDS

// MARK: - Trigger Types

public struct TriggerOptions: Sendable {
    @available(*, deprecated, message: "Pass schema as a direct parameter instead")
    public var schemaValue: String { schema }
    internal let schema: String
    public let withEncryption: Bool
    public let executeAs: String?
    public let notForReplication: Bool

    public init(
        schema: String = "dbo",
        withEncryption: Bool = false,
        executeAs: String? = nil,
        notForReplication: Bool = false
    ) {
        self.schema = schema
        self.withEncryption = withEncryption
        self.executeAs = executeAs
        self.notForReplication = notForReplication
    }
}

public enum TriggerTiming: String, Sendable {
    case after = "AFTER"
    case insteadOf = "INSTEAD OF"
}

public enum TriggerEvent: String, Sendable {
    case insert = "INSERT"
    case update = "UPDATE"
    case delete = "DELETE"
}

public struct TriggerInfo: Sendable {
    public let name: String
    public let tableName: String
    public let schemaName: String
    public let timing: String
    public let events: [String]
    public let isDisabled: Bool
    public let definition: String?
}

// MARK: - SQLServerTriggerClient

public final class SQLServerTriggerClient: @unchecked Sendable {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Trigger Creation

    @discardableResult
    public func createTrigger(
        name: String,
        table: String,
        timing: TriggerTiming,
        events: [TriggerEvent],
        body: String,
        schema: String = "dbo",
        options: TriggerOptions = TriggerOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createTrigger(name: name, table: table, timing: timing, events: events, body: body, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func createTrigger(
        name: String,
        table: String,
        timing: TriggerTiming,
        events: [TriggerEvent],
        body: String,
        schema: String = "dbo",
        options: TriggerOptions = TriggerOptions()
    ) async throws -> [SQLServerStreamMessage] {
        guard !events.isEmpty else {
            throw SQLServerError.invalidArgument("At least one trigger event is required")
        }

        let escapedTriggerName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        var sql = "CREATE TRIGGER \(escapedTriggerName)"
        sql += "\nON \(fullTableName)"

        // Add options
        var optionParts: [String] = []
        if options.withEncryption {
            optionParts.append("ENCRYPTION")
        }
        if let executeAs = options.executeAs {
            optionParts.append("EXECUTE AS '\(executeAs)'")
        }
        if options.notForReplication {
            optionParts.append("NOT FOR REPLICATION")
        }

        if !optionParts.isEmpty {
            sql += "\nWITH \(optionParts.joined(separator: ", "))"
        }

        // Add timing and events
        sql += "\n\(timing.rawValue) \(events.map { $0.rawValue }.joined(separator: ", "))"

        sql += "\nAS\n\(body)"

        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func dropTrigger(name: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropTrigger(name: name, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func dropTrigger(name: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedTriggerName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTriggerName = "\(schemaPrefix)\(escapedTriggerName)"

        let sql = "DROP TRIGGER \(fullTriggerName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    public func alterTrigger(
        name: String,
        table: String,
        timing: TriggerTiming,
        events: [TriggerEvent],
        body: String,
        schema: String = "dbo",
        options: TriggerOptions = TriggerOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.alterTrigger(name: name, table: table, timing: timing, events: events, body: body, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func alterTrigger(
        name: String,
        table: String,
        timing: TriggerTiming,
        events: [TriggerEvent],
        body: String,
        schema: String = "dbo",
        options: TriggerOptions = TriggerOptions()
    ) async throws -> [SQLServerStreamMessage] {
        guard !events.isEmpty else {
            throw SQLServerError.invalidArgument("At least one trigger event is required")
        }

        let escapedTriggerName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        var sql = "ALTER TRIGGER \(escapedTriggerName)"
        sql += "\nON \(fullTableName)"

        // Add options
        var optionParts: [String] = []
        if options.withEncryption {
            optionParts.append("ENCRYPTION")
        }
        if let executeAs = options.executeAs {
            optionParts.append("EXECUTE AS '\(executeAs)'")
        }
        if options.notForReplication {
            optionParts.append("NOT FOR REPLICATION")
        }

        if !optionParts.isEmpty {
            sql += "\nWITH \(optionParts.joined(separator: ", "))"
        }

        // Add timing and events
        sql += "\n\(timing.rawValue) \(events.map { $0.rawValue }.joined(separator: ", "))"

        sql += "\nAS\n\(body)"

        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Trigger Management

    @discardableResult
    internal func enableTrigger(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.enableTrigger(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func enableTrigger(name: String, table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedTriggerName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ENABLE TRIGGER \(escapedTriggerName) ON \(fullTableName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func disableTrigger(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.disableTrigger(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func disableTrigger(name: String, table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedTriggerName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "DISABLE TRIGGER \(escapedTriggerName) ON \(fullTableName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func enableAllTriggers(table: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.enableAllTriggers(table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func enableAllTriggers(table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ENABLE TRIGGER ALL ON \(fullTableName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func disableAllTriggers(table: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.disableAllTriggers(table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func disableAllTriggers(table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "DISABLE TRIGGER ALL ON \(fullTableName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Trigger Information

    @available(macOS 12.0, *)
    public func triggerExists(name: String, table: String, schema: String = "dbo") async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.triggers t
        INNER JOIN sys.objects o ON t.parent_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE t.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """

        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }

    @available(macOS 12.0, *)
    public func getTriggerInfo(name: String, table: String, schema: String = "dbo") async throws -> TriggerInfo? {
        let sql = """
        SELECT
            t.name as trigger_name,
            o.name as table_name,
            s.name as schema_name,
            CASE
                WHEN t.is_instead_of_trigger = 1 THEN 'INSTEAD OF'
                ELSE 'AFTER'
            END as timing,
            CASE
                WHEN t.is_disabled = 1 THEN 1
                ELSE 0
            END as is_disabled,
            m.definition
        FROM sys.triggers t
        INNER JOIN sys.objects o ON t.parent_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        LEFT JOIN sys.sql_modules m ON t.object_id = m.object_id
        WHERE t.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else { return nil }

        let triggerName = row.column("trigger_name")?.string ?? ""
        let tableName = row.column("table_name")?.string ?? ""
        let schemaName = row.column("schema_name")?.string ?? ""
        let timing = row.column("timing")?.string ?? ""
        let isDisabled = (row.column("is_disabled")?.int ?? 0) != 0
        let definition = row.column("definition")?.string

        // Get trigger events
        let eventsSql = """
        SELECT
            CASE te.type
                WHEN 1 THEN 'INSERT'
                WHEN 2 THEN 'UPDATE'
                WHEN 3 THEN 'DELETE'
            END as event_type
        FROM sys.trigger_events te
        INNER JOIN sys.triggers t ON te.object_id = t.object_id
        WHERE t.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND t.parent_id = OBJECT_ID('[\(schema)].[\(table)]')
        """

        let eventRows = try await client.query(eventsSql)
        let events = eventRows.compactMap { $0.column("event_type")?.string }

        return TriggerInfo(
            name: triggerName,
            tableName: tableName,
            schemaName: schemaName,
            timing: timing,
            events: events,
            isDisabled: isDisabled,
            definition: definition
        )
    }

    @available(macOS 12.0, *)
    public func listTableTriggers(table: String, schema: String = "dbo") async throws -> [TriggerInfo] {
        let sql = """
        SELECT
            t.name as trigger_name,
            o.name as table_name,
            s.name as schema_name,
            CASE
                WHEN t.is_instead_of_trigger = 1 THEN 'INSTEAD OF'
                ELSE 'AFTER'
            END as timing,
            CASE
                WHEN t.is_disabled = 1 THEN 1
                ELSE 0
            END as is_disabled,
            m.definition
        FROM sys.triggers t
        INNER JOIN sys.objects o ON t.parent_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        LEFT JOIN sys.sql_modules m ON t.object_id = m.object_id
        WHERE o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY t.name
        """

        let rows = try await client.query(sql)
        var triggers: [TriggerInfo] = []

        for row in rows {
            let triggerName = row.column("trigger_name")?.string ?? ""
            let tableName = row.column("table_name")?.string ?? ""
            let schemaName = row.column("schema_name")?.string ?? ""
            let timing = row.column("timing")?.string ?? ""
            let isDisabled = (row.column("is_disabled")?.int ?? 0) != 0
            let definition = row.column("definition")?.string

            // Get trigger events for this trigger
            let eventsSql = """
            SELECT
                CASE te.type
                    WHEN 1 THEN 'INSERT'
                    WHEN 2 THEN 'UPDATE'
                    WHEN 3 THEN 'DELETE'
                END as event_type
            FROM sys.trigger_events te
            INNER JOIN sys.triggers t ON te.object_id = t.object_id
            WHERE t.name = '\(triggerName.replacingOccurrences(of: "'", with: "''"))'
            AND t.parent_id = OBJECT_ID('[\(schema)].[\(table)]')
            """

            let eventRows = try await client.query(eventsSql)
            let events = eventRows.compactMap { $0.column("event_type")?.string }

            let triggerInfo = TriggerInfo(
                name: triggerName,
                tableName: tableName,
                schemaName: schemaName,
                timing: timing,
                events: events,
                isDisabled: isDisabled,
                definition: definition
            )

            triggers.append(triggerInfo)
        }

        return triggers
    }

    @available(macOS 12.0, *)
    public func getTriggerDefinition(name: String, table: String, schema: String = "dbo") async throws -> String? {
        let sql = """
        SELECT m.definition
        FROM sys.triggers t
        INNER JOIN sys.objects o ON t.parent_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.sql_modules m ON t.object_id = m.object_id
        WHERE t.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """

        let result = try await client.queryScalar(sql, as: String.self)
        return result
    }

    // MARK: - Server-Level Triggers

    /// Lists all server-level triggers (DDL and logon triggers).
    @available(macOS 12.0, *)
    public func listServerTriggers() async throws -> [ServerTriggerMetadata] {
        let sql = """
        SELECT
            t.name,
            t.is_disabled,
            t.type_desc,
            CONVERT(varchar(30), t.create_date, 126) AS create_date,
            CONVERT(varchar(30), t.modify_date, 126) AS modify_date,
            m.definition
        FROM sys.server_triggers AS t
        LEFT JOIN sys.server_sql_modules AS m ON m.object_id = t.object_id
        ORDER BY t.name
        """
        let rows = try await client.query(sql)

        var results: [ServerTriggerMetadata] = []
        for row in rows {
            let name = row.column("name")?.string ?? ""
            let eventsSql = """
            SELECT te.type_desc
            FROM sys.server_trigger_events AS te
            WHERE te.object_id = (SELECT object_id FROM sys.server_triggers WHERE name = N'\(SQLServerSQL.escapeLiteral(name))')
            """
            let eventRows = try await client.query(eventsSql)
            let events = eventRows.compactMap { $0.column("type_desc")?.string }

            results.append(ServerTriggerMetadata(
                name: name,
                isDisabled: (row.column("is_disabled")?.int ?? 0) != 0,
                typeDescription: row.column("type_desc")?.string ?? "",
                createDate: row.column("create_date")?.string,
                modifyDate: row.column("modify_date")?.string,
                definition: row.column("definition")?.string,
                events: events
            ))
        }
        return results
    }

    /// Gets the T-SQL definition of a server-level trigger.
    @available(macOS 12.0, *)
    public func getServerTriggerDefinition(name: String) async throws -> String? {
        let sql = """
        SELECT m.definition
        FROM sys.server_triggers AS t
        INNER JOIN sys.server_sql_modules AS m ON m.object_id = t.object_id
        WHERE t.name = N'\(SQLServerSQL.escapeLiteral(name))'
        """
        return try await client.queryScalar(sql, as: String.self)
    }

    /// Enables a server-level trigger.
    @available(macOS 12.0, *)
    @discardableResult
    public func enableServerTrigger(name: String) async throws -> [SQLServerStreamMessage] {
        let result = try await client.execute("ENABLE TRIGGER \(SQLServerSQL.escapeIdentifier(name)) ON ALL SERVER")
        return result.messages
    }

    /// Disables a server-level trigger.
    @available(macOS 12.0, *)
    @discardableResult
    public func disableServerTrigger(name: String) async throws -> [SQLServerStreamMessage] {
        let result = try await client.execute("DISABLE TRIGGER \(SQLServerSQL.escapeIdentifier(name)) ON ALL SERVER")
        return result.messages
    }

    /// Drops a server-level trigger.
    @available(macOS 12.0, *)
    @discardableResult
    public func dropServerTrigger(name: String) async throws -> [SQLServerStreamMessage] {
        let result = try await client.execute("DROP TRIGGER \(SQLServerSQL.escapeIdentifier(name)) ON ALL SERVER")
        return result.messages
    }

    // MARK: - Database-Level DDL Triggers

    /// Lists all database-level DDL triggers in the specified database.
    @available(macOS 12.0, *)
    public func listDatabaseDDLTriggers(database: String) async throws -> [DatabaseDDLTriggerMetadata] {
        let db = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        SELECT
            t.name,
            t.is_disabled,
            CONVERT(varchar(30), t.create_date, 126) AS create_date,
            CONVERT(varchar(30), t.modify_date, 126) AS modify_date,
            m.definition
        FROM \(db).sys.triggers AS t
        LEFT JOIN \(db).sys.sql_modules AS m ON m.object_id = t.object_id
        WHERE t.parent_class = 0
        ORDER BY t.name
        """
        let rows = try await client.query(sql)

        var results: [DatabaseDDLTriggerMetadata] = []
        for row in rows {
            let name = row.column("name")?.string ?? ""
            let eventsSql = """
            SELECT te.type_desc
            FROM \(db).sys.trigger_events AS te
            INNER JOIN \(db).sys.triggers AS t ON t.object_id = te.object_id
            WHERE t.name = N'\(SQLServerSQL.escapeLiteral(name))'
              AND t.parent_class = 0
            """
            let eventRows = try await client.query(eventsSql)
            let events = eventRows.compactMap { $0.column("type_desc")?.string }

            results.append(DatabaseDDLTriggerMetadata(
                name: name,
                isDisabled: (row.column("is_disabled")?.int ?? 0) != 0,
                createDate: row.column("create_date")?.string,
                modifyDate: row.column("modify_date")?.string,
                definition: row.column("definition")?.string,
                events: events
            ))
        }
        return results
    }

    /// Gets the T-SQL definition of a database-level DDL trigger.
    @available(macOS 12.0, *)
    public func getDatabaseDDLTriggerDefinition(name: String, database: String) async throws -> String? {
        let db = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        SELECT m.definition
        FROM \(db).sys.triggers AS t
        INNER JOIN \(db).sys.sql_modules AS m ON m.object_id = t.object_id
        WHERE t.name = N'\(SQLServerSQL.escapeLiteral(name))'
          AND t.parent_class = 0
        """
        return try await client.queryScalar(sql, as: String.self)
    }

    /// Enables a database-level DDL trigger.
    @available(macOS 12.0, *)
    @discardableResult
    public func enableDatabaseDDLTrigger(name: String, database: String) async throws -> [SQLServerStreamMessage] {
        let sql = "ENABLE TRIGGER \(SQLServerSQL.escapeIdentifier(name)) ON DATABASE"
        return try await client.withDatabase(database) { connection in
            let result = try await connection.execute(sql)
            return result.messages
        }
    }

    /// Disables a database-level DDL trigger.
    @available(macOS 12.0, *)
    @discardableResult
    public func disableDatabaseDDLTrigger(name: String, database: String) async throws -> [SQLServerStreamMessage] {
        let sql = "DISABLE TRIGGER \(SQLServerSQL.escapeIdentifier(name)) ON DATABASE"
        return try await client.withDatabase(database) { connection in
            let result = try await connection.execute(sql)
            return result.messages
        }
    }

    /// Drops a database-level DDL trigger.
    @available(macOS 12.0, *)
    @discardableResult
    public func dropDatabaseDDLTrigger(name: String, database: String) async throws -> [SQLServerStreamMessage] {
        let sql = "DROP TRIGGER \(SQLServerSQL.escapeIdentifier(name)) ON DATABASE"
        return try await client.withDatabase(database) { connection in
            let result = try await connection.execute(sql)
            return result.messages
        }
    }

    // MARK: - Server Trigger Creation

    /// Creates a server-level DDL trigger.
    ///
    /// - Parameters:
    ///   - name: Trigger name.
    ///   - events: DDL event types or groups (e.g. `["CREATE_TABLE", "ALTER_TABLE"]` or `["DDL_DATABASE_LEVEL_EVENTS"]`).
    ///   - body: T-SQL body of the trigger.
    ///   - options: Optional encryption, execute-as settings.
    @available(macOS 12.0, *)
    @discardableResult
    public func createServerTrigger(
        name: String,
        events: [String],
        body: String,
        options: TriggerOptions = TriggerOptions()
    ) async throws -> [SQLServerStreamMessage] {
        guard !events.isEmpty else {
            throw SQLServerError.invalidArgument("At least one DDL event is required")
        }
        var sql = "CREATE TRIGGER \(SQLServerSQL.escapeIdentifier(name))\nON ALL SERVER"
        var optionParts: [String] = []
        if options.withEncryption { optionParts.append("ENCRYPTION") }
        if let executeAs = options.executeAs { optionParts.append("EXECUTE AS '\(SQLServerSQL.escapeLiteral(executeAs))'") }
        if !optionParts.isEmpty { sql += "\nWITH \(optionParts.joined(separator: ", "))" }
        sql += "\nAFTER \(events.joined(separator: ", "))"
        sql += "\nAS\n\(body)"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Creates a database-level DDL trigger.
    ///
    /// - Parameters:
    ///   - name: Trigger name.
    ///   - database: Database to create the trigger in.
    ///   - events: DDL event types or groups (e.g. `["CREATE_TABLE", "DROP_TABLE"]`).
    ///   - body: T-SQL body of the trigger.
    ///   - options: Optional encryption, execute-as settings.
    @available(macOS 12.0, *)
    @discardableResult
    public func createDatabaseDDLTrigger(
        name: String,
        database: String,
        events: [String],
        body: String,
        options: TriggerOptions = TriggerOptions()
    ) async throws -> [SQLServerStreamMessage] {
        guard !events.isEmpty else {
            throw SQLServerError.invalidArgument("At least one DDL event is required")
        }

        var sql = "CREATE TRIGGER \(SQLServerSQL.escapeIdentifier(name))\nON DATABASE"
        var optionParts: [String] = []
        if options.withEncryption { optionParts.append("ENCRYPTION") }
        if let executeAs = options.executeAs { optionParts.append("EXECUTE AS '\(SQLServerSQL.escapeLiteral(executeAs))'") }
        if !optionParts.isEmpty { sql += "\nWITH \(optionParts.joined(separator: ", "))" }
        sql += "\nAFTER \(events.joined(separator: ", "))"
        sql += "\nAS\n\(body)"
        let finalSQL = sql
        return try await client.withDatabase(database) { connection in
            let result = try await connection.execute(finalSQL)
            return result.messages
        }
    }

}
