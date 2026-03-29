import NIO
import SQLServerTDS
import Foundation

// MARK: - Savepoint Types

public struct SavepointInfo: Sendable {
    public let name: String
    public let transactionId: String?
    public let saveTime: Date?
    public let isActive: Bool

    public init(name: String, transactionId: String? = nil, saveTime: Date? = nil, isActive: Bool = true) {
        self.name = name
        self.transactionId = transactionId
        self.saveTime = saveTime
        self.isActive = isActive
    }
}

// MARK: - SQLServerTransactionClient

public final class SQLServerTransactionClient: @unchecked Sendable {
    private let client: SQLServerClient
    private var activeSavepoints: [String] = []

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Transaction Management

    /// Begins a new transaction
    internal func beginTransaction() -> EventLoopFuture<Void> {
        return client.execute("BEGIN TRANSACTION").map { _ in () }
    }

    /// Begins a new transaction (async version)
    @available(macOS 12.0, *)
    public func beginTransaction() async throws {
        _ = try await client.execute("BEGIN TRANSACTION")
    }

    /// Commits the current transaction
    internal func commitTransaction() -> EventLoopFuture<Void> {
        activeSavepoints.removeAll()
        return client.execute("COMMIT").map { _ in () }
    }

    /// Commits the current transaction (async version)
    @available(macOS 12.0, *)
    public func commitTransaction() async throws {
        activeSavepoints.removeAll()
        _ = try await client.execute("COMMIT")
    }

    /// Rolls back the current transaction
    internal func rollbackTransaction() -> EventLoopFuture<Void> {
        activeSavepoints.removeAll()
        return client.execute("ROLLBACK").map { _ in () }
    }

    /// Rolls back the current transaction (async version)
    @available(macOS 12.0, *)
    public func rollbackTransaction() async throws {
        activeSavepoints.removeAll()
        _ = try await client.execute("ROLLBACK")
    }

    // MARK: - Savepoint Management

    /// Creates a savepoint with the specified name
    internal func createSavepoint(name: String) -> EventLoopFuture<Void> {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        return client.execute("SAVE TRANSACTION \(escapedName)").map { _ in
            self.activeSavepoints.append(name)
        }
    }

    /// Creates a savepoint with the specified name (async version)
    @available(macOS 12.0, *)
    public func createSavepoint(name: String) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        _ = try await client.execute("SAVE TRANSACTION \(escapedName)")
        activeSavepoints.append(name)
    }

    /// Rolls back to the specified savepoint
    internal func rollbackToSavepoint(name: String) -> EventLoopFuture<Void> {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        return client.execute("ROLLBACK TRANSACTION \(escapedName)").map { _ in
            // Remove this savepoint and any savepoints created after it
            if let index = self.activeSavepoints.firstIndex(of: name) {
                self.activeSavepoints.removeSubrange(index...)
            }
        }
    }

    /// Rolls back to the specified savepoint (async version)
    @available(macOS 12.0, *)
    public func rollbackToSavepoint(name: String) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        _ = try await client.execute("ROLLBACK TRANSACTION \(escapedName)")
        if let index = self.activeSavepoints.firstIndex(of: name) {
            self.activeSavepoints.removeSubrange(index...)
        }
    }

    /// Releases the specified savepoint (SQL Server 2008+)
    internal func releaseSavepoint(name: String) -> EventLoopFuture<Void> {
        // SQL Server doesn't have an explicit RELEASE SAVEPOINT command like some other databases
        // We remove it from our tracking, but the savepoint still exists in the transaction
        if let index = activeSavepoints.firstIndex(of: name) {
            activeSavepoints.remove(at: index)
        }
        return client.withConnection { connection in
            connection.eventLoop.makeSucceededFuture(())
        }
    }

    /// Releases the specified savepoint (async version)
    @available(macOS 12.0, *)
    public func releaseSavepoint(name: String) async throws {
        if let index = activeSavepoints.firstIndex(of: name) {
            activeSavepoints.remove(at: index)
        }
    }

    // MARK: - Transaction Information

    /// Gets information about the current transaction
    internal func getTransactionInfo() -> EventLoopFuture<TransactionInfo?> {
        let sql = """
        SELECT
            transaction_id,
            name,
            transaction_type,
            transaction_state,
            transaction_begin_time
        FROM sys.dm_tran_active_transactions
        WHERE transaction_id = CURRENT_TRANSACTION_ID()
        """

        return client.query(sql).flatMap { rows in
            guard let row = rows.first else {
                return self.currentIsolationLevelFuture().map { _ in nil }
            }

            let transactionId = row.column("transaction_id")?.string
            let name = row.column("name")?.string
            let transactionTypeCode = row.column("transaction_type")?.int
            let transactionStateCode = row.column("transaction_state")?.int

            let transactionType: String?
            switch transactionTypeCode {
            case 2: transactionType = "READ"   // read-only
            case 1, 3, 4: transactionType = "WRITE" // read/write, system, distributed
            default: transactionType = nil
            }

            let transactionState: String?
            switch transactionStateCode {
            case 0: transactionState = "Not Initialized"
            case 1: transactionState = "Initialized"
            case 2: transactionState = "Active"
            case 3: transactionState = "Ended"
            case 4: transactionState = "Committing"
            case 5: transactionState = "Prepared"
            case 6: transactionState = "Committed"
            case 7: transactionState = "Rolling Back"
            case 8: transactionState = "Rolled Back"
            default: transactionState = nil
            }

            let beginTime = row.column("transaction_begin_time")?.date
            return self.currentIsolationLevelFuture().map { isolationLevel in
                TransactionInfo(
                    id: transactionId,
                    name: name,
                    type: transactionType,
                    state: transactionState,
                    beginTime: beginTime,
                    isolationLevel: isolationLevel
                )
            }
        }
    }

    /// Gets information about the current transaction (async version)
    @available(macOS 12.0, *)
    public func getTransactionInfo() async throws -> TransactionInfo? {
        let sql = """
        SELECT
            transaction_id,
            name,
            transaction_type,
            transaction_state,
            transaction_begin_time
        FROM sys.dm_tran_active_transactions
        WHERE transaction_id = CURRENT_TRANSACTION_ID()
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else { return nil }

        let transactionId = row.column("transaction_id")?.string
        let name = row.column("name")?.string
        let transactionTypeCode = row.column("transaction_type")?.int
        let transactionStateCode = row.column("transaction_state")?.int

        let transactionType: String?
        switch transactionTypeCode {
        case 2: transactionType = "READ"
        case 1, 3, 4: transactionType = "WRITE"
        default: transactionType = nil
        }

        let transactionState: String?
        switch transactionStateCode {
        case 0: transactionState = "Not Initialized"
        case 1: transactionState = "Initialized"
        case 2: transactionState = "Active"
        case 3: transactionState = "Ended"
        case 4: transactionState = "Committing"
        case 5: transactionState = "Prepared"
        case 6: transactionState = "Committed"
        case 7: transactionState = "Rolling Back"
        case 8: transactionState = "Rolled Back"
        default: transactionState = nil
        }

        return TransactionInfo(
            id: transactionId,
            name: name,
            type: transactionType,
            state: transactionState,
            beginTime: row.column("transaction_begin_time")?.date,
            isolationLevel: try await getCurrentIsolationLevel()
        )
    }

    /// Gets a list of active savepoints
    public func getActiveSavepoints() -> [SavepointInfo] {
        return activeSavepoints.map { name in
            SavepointInfo(name: name, isActive: true)
        }
    }

    /// Checks if a savepoint with the given name is active
    public func isSavepointActive(name: String) -> Bool {
        return activeSavepoints.contains(name)
    }

    /// Gets the current transaction isolation level
    internal func getCurrentIsolationLevel() -> EventLoopFuture<String?> {
        currentIsolationLevelFuture()
    }

    private func currentIsolationLevelFuture() -> EventLoopFuture<String?> {
        let sql = """
        SELECT CASE transaction_isolation_level
            WHEN 0 THEN 'READ UNCOMMITTED'
            WHEN 1 THEN 'READ COMMITTED'
            WHEN 2 THEN 'REPEATABLE READ'
            WHEN 3 THEN 'SERIALIZABLE'
            WHEN 4 THEN 'SNAPSHOT'
            ELSE 'UNKNOWN'
        END as isolation_level
        FROM sys.dm_exec_sessions
        WHERE session_id = @@SPID
        """

        return client.query(sql).map { rows in
            return rows.first?.column("isolation_level")?.string
        }
    }

    /// Gets the current transaction isolation level (async version)
    @available(macOS 12.0, *)
    public func getCurrentIsolationLevel() async throws -> String? {
        try await currentIsolationLevelFuture().get()
    }

    /// Sets the transaction isolation level
    internal func setIsolationLevel(_ level: IsolationLevel) -> EventLoopFuture<Void> {
        let sql = "SET TRANSACTION ISOLATION LEVEL \(level.sqlLiteral)"
        return client.execute(sql).map { _ in () }
    }

    /// Sets the transaction isolation level (async version)
    @available(macOS 12.0, *)
    public func setIsolationLevel(_ level: IsolationLevel) async throws {
        let sql = "SET TRANSACTION ISOLATION LEVEL \(level.sqlLiteral)"
        _ = try await client.execute(sql)
    }

    // MARK: - Advanced Transaction Operations

    /// Executes a closure within a transaction context, automatically handling commit/rollback
    internal func executeInTransaction<T: Sendable>(_ operation: @Sendable @escaping () -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        return beginTransaction()
            .flatMap { _ in
                operation()
                    .flatMap { result in
                        self.commitTransaction()
                            .map { result }
                    }
                    .flatMapError { error in
                        self.rollbackTransaction().flatMapThrowing { _ in
                            throw error
                        }
                    }
            }
    }

    /// Executes a closure within a transaction context, automatically handling commit/rollback (async version).
    /// Pins a single connection for the entire transaction lifetime via task-local scoping.
    @available(macOS 12.0, *)
    public func executeInTransaction<T: Sendable>(_ operation: @Sendable @escaping () async throws -> T) async throws -> T {
        try await client.withConnection { connection in
            try await ClientScopedConnection.$current.withValue(connection) {
                _ = try await self.client.execute("BEGIN TRANSACTION")
                do {
                    let result = try await operation()
                    _ = try await self.client.execute("COMMIT")
                    self.activeSavepoints.removeAll()
                    return result
                } catch {
                    _ = try? await self.client.execute("ROLLBACK")
                    self.activeSavepoints.removeAll()
                    throw error
                }
            }
        }
    }

    /// Executes a closure within a savepoint context, automatically handling rollback on error
    public func executeInSavepoint<T: Sendable>(
        named name: String,
        operation: @Sendable @escaping () -> EventLoopFuture<T>
    ) -> EventLoopFuture<T> {
        return createSavepoint(name: name)
            .flatMap { _ in
                operation()
                    .flatMap { result in
                        self.releaseSavepoint(name: name)
                            .map { result }
                    }
                    .flatMapError { error in
                        self.rollbackToSavepoint(name: name).flatMapThrowing { _ in
                            throw error
                        }
                    }
            }
    }

    /// Executes a closure within a savepoint context, automatically handling rollback on error (async version)
    @available(macOS 12.0, *)
    public func executeInSavepoint<T>(
        named name: String,
        operation: @escaping () async throws -> T
    ) async throws -> T {
        try await createSavepoint(name: name)
        do {
            let result = try await operation()
            try await releaseSavepoint(name: name)
            return result
        } catch {
            try await rollbackToSavepoint(name: name)
            throw error
        }
    }

}

// MARK: - Supporting Types

public struct TransactionInfo: Sendable {
    public let id: String?
    public let name: String?
    public let type: String?
    public let state: String?
    public let beginTime: Date?
    public let isolationLevel: String?

    public init(
        id: String?,
        name: String?,
        type: String?,
        state: String?,
        beginTime: Date?,
        isolationLevel: String?
    ) {
        self.id = id
        self.name = name
        self.type = type
        self.state = state
        self.beginTime = beginTime
        self.isolationLevel = isolationLevel
    }
}

public enum IsolationLevel: String, CaseIterable, Sendable {
    case readUncommitted = "READ UNCOMMITTED"
    case readCommitted = "READ COMMITTED"
    case repeatableRead = "REPEATABLE READ"
    case serializable = "SERIALIZABLE"
    case snapshot = "SNAPSHOT"

    public var sqlLiteral: String {
        return self.rawValue
    }
}
