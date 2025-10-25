import NIO
import SQLServerTDS

// MARK: - Trigger Types

public struct TriggerOptions: Sendable {
    public let schema: String
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

public final class SQLServerTriggerClient {
    private let client: SQLServerClient
    
    public init(client: SQLServerClient) {
        self.client = client
    }
    
    // MARK: - Trigger Creation
    
    public func createTrigger(
        name: String,
        table: String,
        timing: TriggerTiming,
        events: [TriggerEvent],
        body: String,
        options: TriggerOptions = TriggerOptions()
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createTrigger(name: name, table: table, timing: timing, events: events, body: body, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func createTrigger(
        name: String,
        table: String,
        timing: TriggerTiming,
        events: [TriggerEvent],
        body: String,
        options: TriggerOptions = TriggerOptions()
    ) async throws {
        guard !events.isEmpty else {
            throw SQLServerError.invalidArgument("At least one trigger event is required")
        }
        
        let escapedTriggerName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = options.schema != "dbo" ? "\(Self.escapeIdentifier(options.schema))." : ""
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
        
        _ = try await client.execute(sql)
    }
    
    public func dropTrigger(name: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
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
    public func dropTrigger(name: String, schema: String = "dbo") async throws {
        let escapedTriggerName = Self.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTriggerName = "\(schemaPrefix)\(escapedTriggerName)"
        
        let sql = "DROP TRIGGER \(fullTriggerName)"
        _ = try await client.execute(sql)
    }
    
    public func alterTrigger(
        name: String,
        table: String,
        timing: TriggerTiming,
        events: [TriggerEvent],
        body: String,
        options: TriggerOptions = TriggerOptions()
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.alterTrigger(name: name, table: table, timing: timing, events: events, body: body, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func alterTrigger(
        name: String,
        table: String,
        timing: TriggerTiming,
        events: [TriggerEvent],
        body: String,
        options: TriggerOptions = TriggerOptions()
    ) async throws {
        guard !events.isEmpty else {
            throw SQLServerError.invalidArgument("At least one trigger event is required")
        }
        
        let escapedTriggerName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = options.schema != "dbo" ? "\(Self.escapeIdentifier(options.schema))." : ""
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
        
        _ = try await client.execute(sql)
    }
    
    // MARK: - Trigger Management
    
    public func enableTrigger(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
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
    public func enableTrigger(name: String, table: String, schema: String = "dbo") async throws {
        let escapedTriggerName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ENABLE TRIGGER \(escapedTriggerName) ON \(fullTableName)"
        _ = try await client.execute(sql)
    }
    
    public func disableTrigger(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
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
    public func disableTrigger(name: String, table: String, schema: String = "dbo") async throws {
        let escapedTriggerName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "DISABLE TRIGGER \(escapedTriggerName) ON \(fullTableName)"
        _ = try await client.execute(sql)
    }
    
    public func enableAllTriggers(table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
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
    public func enableAllTriggers(table: String, schema: String = "dbo") async throws {
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ENABLE TRIGGER ALL ON \(fullTableName)"
        _ = try await client.execute(sql)
    }
    
    public func disableAllTriggers(table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
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
    public func disableAllTriggers(table: String, schema: String = "dbo") async throws {
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "DISABLE TRIGGER ALL ON \(fullTableName)"
        _ = try await client.execute(sql)
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
    
    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}