import NIO
import SQLServerTDS

extension SQLServerIndexClient {
    // MARK: - Index Management

    @discardableResult
    internal func dropIndex(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
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
    @discardableResult
    public func dropIndex(name: String, table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedIndexName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "DROP INDEX \(escapedIndexName) ON \(fullTableName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func rebuildIndex(name: String, table: String, schema: String = "dbo", options: IndexOptions? = nil) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
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
    @discardableResult
    public func rebuildIndex(name: String, table: String, schema: String = "dbo", database: String? = nil, options: IndexOptions? = nil) async throws -> [SQLServerStreamMessage] {
        let escapedIndexName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName: String
        if let database {
            fullTableName = "\(SQLServerSQL.escapeIdentifier(database)).\(schemaPrefix)\(escapedTableName)"
        } else {
            fullTableName = "\(schemaPrefix)\(escapedTableName)"
        }

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

        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func reorganizeIndex(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
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
    @discardableResult
    public func reorganizeIndex(name: String, table: String, schema: String = "dbo", database: String? = nil) async throws -> [SQLServerStreamMessage] {
        let escapedIndexName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName: String
        if let database {
            fullTableName = "\(SQLServerSQL.escapeIdentifier(database)).\(schemaPrefix)\(escapedTableName)"
        } else {
            fullTableName = "\(schemaPrefix)\(escapedTableName)"
        }

        let sql = "ALTER INDEX \(escapedIndexName) ON \(fullTableName) REORGANIZE"
        let result = try await client.execute(sql)
        return result.messages
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func disableIndex(name: String, table: String, schema: String = "dbo", database: String? = nil) async throws -> [SQLServerStreamMessage] {
        let escapedIndexName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName: String
        if let database {
            fullTableName = "\(SQLServerSQL.escapeIdentifier(database)).\(schemaPrefix)\(escapedTableName)"
        } else {
            fullTableName = "\(schemaPrefix)\(escapedTableName)"
        }

        let sql = "ALTER INDEX \(escapedIndexName) ON \(fullTableName) DISABLE"
        let result = try await client.execute(sql)
        return result.messages
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func rebuildAllIndexes(table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ALTER INDEX ALL ON \(fullTableName) REBUILD"
        let result = try await client.execute(sql)
        return result.messages
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func reorganizeAllIndexes(table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ALTER INDEX ALL ON \(fullTableName) REORGANIZE"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Re-enables a disabled index by rebuilding it.
    @available(macOS 12.0, *)
    @discardableResult
    public func enableIndex(name: String, table: String, schema: String = "dbo", database: String? = nil) async throws -> [SQLServerStreamMessage] {
        let escapedIndexName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName: String
        if let database {
            fullTableName = "\(SQLServerSQL.escapeIdentifier(database)).\(schemaPrefix)\(escapedTableName)"
        } else {
            fullTableName = "\(schemaPrefix)\(escapedTableName)"
        }

        let sql = "ALTER INDEX \(escapedIndexName) ON \(fullTableName) REBUILD"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Returns physical stats for a specific index (fragmentation, page count, fill factor, etc.).
    @available(macOS 12.0, *)
    public func indexProperties(
        schema: String = "dbo",
        table: String,
        indexName: String,
        database: String? = nil
    ) async throws -> SQLServerIndexProperties {
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedIndex = indexName.replacingOccurrences(of: "'", with: "''")

        let sql = """
        SELECT
            ips.avg_fragmentation_in_percent AS fragmentation_percent,
            ips.page_count,
            ISNULL(i.fill_factor, 0) AS fill_factor,
            ips.avg_page_space_used_in_percent AS avg_page_space_used_percent,
            ips.record_count,
            ips.ghost_record_count,
            ips.index_depth,
            ips.index_type_desc AS index_type
        FROM sys.dm_db_index_physical_stats(
            DB_ID(\(database.map { "N'\($0.replacingOccurrences(of: "'", with: "''"))'" } ?? "NULL")),
            OBJECT_ID(N'\(escapedSchema).\(escapedTable)'),
            NULL, NULL, 'DETAILED'
        ) AS ips
        JOIN sys.indexes AS i
            ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        WHERE i.name = N'\(escapedIndex)';
        """

        let result = try await client.execute(sql)
        let rows = result.rows

        guard let row = rows.first else {
            return SQLServerIndexProperties(
                fragmentationPercent: 0,
                pageCount: 0,
                fillFactor: 0
            )
        }

        return SQLServerIndexProperties(
            fragmentationPercent: row.column("fragmentation_percent")?.double ?? 0,
            pageCount: row.column("page_count")?.int64 ?? 0,
            fillFactor: row.column("fill_factor")?.int ?? 0,
            avgPageSpaceUsedPercent: row.column("avg_page_space_used_percent")?.double,
            recordCount: row.column("record_count")?.int64,
            ghostRecordCount: row.column("ghost_record_count")?.int64,
            indexDepth: row.column("index_depth")?.int,
            indexType: row.column("index_type")?.string
        )
    }
}
