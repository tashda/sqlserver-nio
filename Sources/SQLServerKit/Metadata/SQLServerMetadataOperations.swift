import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

public final class SQLServerMetadataOperations: @unchecked Sendable {
    internal weak var connection: SQLServerConnection?
    internal let eventLoop: EventLoop
    internal let logger: Logger
    internal let queryExecutor: @Sendable (String) -> EventLoopFuture<[TDSRow]>
    internal let cache: MetadataCache<[ColumnMetadata]>?
    internal let configuration: Configuration
    internal let defaultDatabaseLock = NIOLock()
    internal var defaultDatabase: String?
    /// SQL Server product major version captured from LOGINACK. Zero means unknown (assume modern).
    internal let serverMajorVersion: UInt8

    public convenience init(
        connection: SQLServerConnection,
        configuration: Configuration = Configuration()
    ) {
        let eventLoop = connection.eventLoop
        let timeout = configuration.commandTimeout
        let executor: @Sendable (String) -> EventLoopFuture<[TDSRow]> = { [weak connection] sql in
            guard let connection else {
                return eventLoop.makeFailedFuture(SQLServerError.connectionClosed)
            }
            if let timeout {
                return connection.execute(sql, timeout: timeout, invalidateOnTimeout: false).map(\.rawRows)
            }
            return connection.execute(sql).map(\.rawRows)
        }
        self.init(
            connection: connection,
            eventLoop: eventLoop,
            configuration: configuration,
            sharedCache: nil,
            defaultDatabase: connection.currentDatabase,
            serverMajorVersion: connection.base.serverMajorVersion,
            logger: connection.logger,
            queryExecutor: executor
        )
    }

    internal init(
        connection: SQLServerConnection? = nil,
        eventLoop: EventLoop,
        configuration: Configuration,
        sharedCache: MetadataCache<[ColumnMetadata]>?,
        defaultDatabase: String?,
        serverMajorVersion: UInt8 = 0,
        logger: Logger? = nil,
        queryExecutor: (@Sendable (String) -> EventLoopFuture<[TDSRow]>)? = nil
    ) {
        self.connection = connection
        self.eventLoop = eventLoop
        self.serverMajorVersion = serverMajorVersion
        if let providedLogger = logger {
            self.logger = providedLogger
        } else {
            var defaultLogger = Logger(label: "tds.sqlserver.metadata")
            defaultLogger.logLevel = .trace
            self.logger = defaultLogger
        }

        self.configuration = configuration
        self.cache = configuration.enableColumnCache ? (sharedCache ?? MetadataCache<[ColumnMetadata]>()) : nil
        self.defaultDatabase = defaultDatabase

        if let executor = queryExecutor {
            self.queryExecutor = executor
        } else {
            guard let connection else {
                self.queryExecutor = { _ in
                    eventLoop.makeFailedFuture(SQLServerError.connectionClosed)
                }
                return
            }
            let timeout = configuration.commandTimeout
            self.queryExecutor = { [weak connection] sql in
                guard let connection else {
                    return eventLoop.makeFailedFuture(SQLServerError.connectionClosed)
                }
                if let timeout {
                    return connection.execute(sql, timeout: timeout, invalidateOnTimeout: false).map(\.rawRows)
                }
                return connection.execute(sql).map(\.rawRows)
            }
        }
    }

    /// True when the server supports temporal tables (sys.periods, history_table_id, temporal_type).
    /// Introduced in SQL Server 2016 (major version 13).
    internal var supportsTemporalTables: Bool {
        serverMajorVersion == 0 || serverMajorVersion >= 13
    }

    /// True when the server supports In-Memory OLTP columns (is_memory_optimized, durability_desc).
    /// Introduced in SQL Server 2014 (major version 12).
    internal var supportsMemoryOptimized: Bool {
        serverMajorVersion == 0 || serverMajorVersion >= 12
    }

    internal func effectiveDatabase(_ override: String?) -> String? {
        if let override, !override.isEmpty {
            return override
        }
        return defaultDatabaseLock.withLock { defaultDatabase }
    }

    internal func qualified(_ database: String?, object: String) -> String {
        if let resolved = effectiveDatabase(database), !resolved.isEmpty {
            // Skip cross-database prefix when the target matches the connection's
            // current database — avoids unnecessary three-part naming for sys.* views.
            let currentDB = defaultDatabaseLock.withLock { defaultDatabase }
            if let currentDB, resolved.caseInsensitiveCompare(currentDB) == .orderedSame {
                return object
            }
            return "\(SQLServerSQL.escapeIdentifier(resolved)).\(object)"
        } else {
            return object
        }
    }


    internal func timed<T: Sendable>(_ label: String, _ operation: @Sendable () -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        let start = NIODeadline.now()
        return operation().map { value in
            let elapsedMs = (NIODeadline.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000
            if elapsedMs >= 1_000 {
                self.logger.info("[Metadata] \(label) completed elapsedMs=\(elapsedMs)")
            } else {
                self.logger.trace("[Metadata] \(label) completed elapsedMs=\(elapsedMs)")
            }
            return value
        }.flatMapError { error in
            let elapsedMs = (NIODeadline.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000
            self.logger.warning("[Metadata] \(label) failed elapsedMs=\(elapsedMs) error=\(error)")
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    internal func fetchAgentStatus() -> EventLoopFuture<SQLServerAgentStatus> {
        let sql = """
        SELECT
            is_enabled = CAST(ISNULL((
                SELECT CAST(value_in_use AS INT)
                FROM sys.configurations
                WHERE name = 'Agent XPs'
            ), 0) AS INT),
            is_running = CAST(CASE WHEN EXISTS (
                SELECT 1
                FROM sys.dm_server_services
                WHERE servicename LIKE 'SQL Server Agent%'
                  AND status_desc = 'Running'
            ) THEN 1 ELSE 0 END AS INT)
        """
        return queryExecutor(sql).map { rows in
            let enabled = (rows.first?.column("is_enabled")?.int ?? 0) != 0
            let running = (rows.first?.column("is_running")?.int ?? 0) != 0
            return SQLServerAgentStatus(isSqlAgentEnabled: enabled, isSqlAgentRunning: running)
        }
    }

    @available(macOS 12.0, *)
    public func fetchAgentStatus() async throws -> SQLServerAgentStatus {
        try await fetchAgentStatus().get()
    }
}
