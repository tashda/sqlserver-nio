import NIO
import SQLServerTDS

extension SQLServerIndexClient {
    // MARK: - Index Management
    
    internal func dropIndex(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropIndex(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func dropIndex(name: String, table: String, schema: String = "dbo") async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "DROP INDEX \(escapedIndexName) ON \(fullTableName)"
        _ = try await client.execute(sql)
    }
    
    internal func rebuildIndex(name: String, table: String, schema: String = "dbo", options: IndexOptions? = nil) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.rebuildIndex(name: name, table: table, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func rebuildIndex(name: String, table: String, schema: String = "dbo", options: IndexOptions? = nil) async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        var sql = "ALTER INDEX \(escapedIndexName) ON \(fullTableName) REBUILD"
        
        // Add rebuild options
        if let options = options {
            var optionParts: [String] = []
            
            if let fillFactor = options.fillFactor {
                optionParts.append("FILLFACTOR = \(fillFactor)")
            }
            if options.padIndex {
                optionParts.append("PAD_INDEX = ON")
            }
            if options.statisticsNoRecompute {
                optionParts.append("STATISTICS_NORECOMPUTE = ON")
            }
            if !options.allowRowLocks {
                optionParts.append("ALLOW_ROW_LOCKS = OFF")
            }
            if !options.allowPageLocks {
                optionParts.append("ALLOW_PAGE_LOCKS = OFF")
            }
            if options.online {
                optionParts.append("ONLINE = ON")
            }
            if let maxDop = options.maxDop {
                optionParts.append("MAXDOP = \(maxDop)")
            }
            if let compression = options.dataCompression {
                optionParts.append("DATA_COMPRESSION = \(compression.rawValue)")
            }
            
            if !optionParts.isEmpty {
                sql += " WITH (\(optionParts.joined(separator: ", ")))"
            }
        }
        
        _ = try await client.execute(sql)
    }
    
    internal func reorganizeIndex(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.reorganizeIndex(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func reorganizeIndex(name: String, table: String, schema: String = "dbo") async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ALTER INDEX \(escapedIndexName) ON \(fullTableName) REORGANIZE"
        _ = try await client.execute(sql)
    }

    @available(macOS 12.0, *)
    public func disableIndex(name: String, table: String, schema: String = "dbo") async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ALTER INDEX \(escapedIndexName) ON \(fullTableName) DISABLE"
        _ = try await client.execute(sql)
    }

    @available(macOS 12.0, *)
    public func rebuildAllIndexes(table: String, schema: String = "dbo") async throws {
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ALTER INDEX ALL ON \(fullTableName) REBUILD"
        _ = try await client.execute(sql)
    }

    @available(macOS 12.0, *)
    public func reorganizeAllIndexes(table: String, schema: String = "dbo") async throws {
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ALTER INDEX ALL ON \(fullTableName) REORGANIZE"
        _ = try await client.execute(sql)
    }
}
