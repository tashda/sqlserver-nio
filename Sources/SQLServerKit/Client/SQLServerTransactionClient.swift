import NIO
import SQLServerTDS
import Foundation

// MARK: - Savepoint Types

public struct SavepointOptions: Sendable {
    public let name: String

    public init(name: String) {
        self.name = name
    }
}

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

public final class SQLServerTransactionClient {
    private let client: SQLServerClient
    private var activeSavepoints: [String] = []

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Transaction Management

    /// Begins a new transaction
    public func beginTransaction() -> EventLoopFuture<Void> {
        return client.execute("BEGIN TRANSACTION").map { _ in () }
    }

    /// Begins a new transaction (async version)
    @available(macOS 12.0, *)
    public func beginTransaction() async throws {
        try await beginTransaction().get()
    }

    /// Commits the current transaction
    public func commitTransaction() -> EventLoopFuture<Void> {
        activeSavepoints.removeAll()
        return client.execute("COMMIT").map { _ in () }
    }

    /// Commits the current transaction (async version)
    @available(macOS 12.0, *)
    public func commitTransaction() async throws {
        try await commitTransaction().get()
    }

    /// Rolls back the current transaction
    public func rollbackTransaction() -> EventLoopFuture<Void> {
        activeSavepoints.removeAll()
        return client.execute("ROLLBACK").map { _ in () }
    }

    /// Rolls back the current transaction (async version)
    @available(macOS 12.0, *)
    public func rollbackTransaction() async throws {
        try await rollbackTransaction().get()
    }

    // MARK: - Savepoint Management

    /// Creates a savepoint with the specified name
    public func createSavepoint(name: String) -> EventLoopFuture<Void> {
        let escapedName = escapeIdentifier(name)
        return client.execute("SAVE TRANSACTION \(escapedName)").map { _ in
            self.activeSavepoints.append(name)
        }
    }

    /// Creates a savepoint with the specified name (async version)
    @available(macOS 12.0, *)
    public func createSavepoint(name: String) async throws {
        try await createSavepoint(name: name).get()
    }

    /// Creates a savepoint with options
    public func createSavepoint(_ options: SavepointOptions) -> EventLoopFuture<Void> {
        return createSavepoint(name: options.name)
    }

    /// Creates a savepoint with options (async version)
    @available(macOS 12.0, *)
    public func createSavepoint(_ options: SavepointOptions) async throws {
        try await createSavepoint(name: options.name).get()
    }

    /// Rolls back to the specified savepoint
    public func rollbackToSavepoint(name: String) -> EventLoopFuture<Void> {
        let escapedName = escapeIdentifier(name)
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
        try await rollbackToSavepoint(name: name).get()
    }

    /// Rolls back to the specified savepoint options
    public func rollbackToSavepoint(_ options: SavepointOptions) -> EventLoopFuture<Void> {
        return rollbackToSavepoint(name: options.name)
    }

    /// Rolls back to the specified savepoint options (async version)
    @available(macOS 12.0, *)
    public func rollbackToSavepoint(_ options: SavepointOptions) async throws {
        try await rollbackToSavepoint(name: options.name).get()
    }

    /// Releases the specified savepoint (SQL Server 2008+)
    public func releaseSavepoint(name: String) -> EventLoopFuture<Void> {
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
        try await releaseSavepoint(name: name).get()
    }

    /// Releases the specified savepoint options
    public func releaseSavepoint(_ options: SavepointOptions) -> EventLoopFuture<Void> {
        return releaseSavepoint(name: options.name)
    }

    /// Releases the specified savepoint options (async version)
    @available(macOS 12.0, *)
    public func releaseSavepoint(_ options: SavepointOptions) async throws {
        try await releaseSavepoint(name: options.name).get()
    }

    // MARK: - Transaction Information

    /// Gets information about the current transaction
    public func getTransactionInfo() -> EventLoopFuture<TransactionInfo?> {
        let sql = """
        SELECT
            transaction_id,
            name,
            transaction_type,
            transaction_state,
            transaction_begin_time,
            transaction_type_desc,
            transaction_state_desc
        FROM sys.dm_tran_active_transactions
        WHERE transaction_id = CURRENT_TRANSACTION_ID()
        """

        return client.query(sql).map { rows in
            guard let row = rows.first else { return nil }

            let transactionId = row.column("transaction_id")?.string
            let name = row.column("name")?.string
            let transactionType = row.column("transaction_type_desc")?.string
            let transactionState = row.column("transaction_state_desc")?.string
            let beginTime = row.column("transaction_begin_time")?.date

            return TransactionInfo(
                id: transactionId,
                name: name,
                type: transactionType,
                state: transactionState,
                beginTime: beginTime,
                isolationLevel: self.getCurrentIsolationLevel()
            )
        }
    }

    /// Gets information about the current transaction (async version)
    @available(macOS 12.0, *)
    public func getTransactionInfo() async throws -> TransactionInfo? {
        try await getTransactionInfo().get()
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
    public func getCurrentIsolationLevel() -> EventLoopFuture<String?> {
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
        try await getCurrentIsolationLevel().get()
    }

    /// Sets the transaction isolation level
    public func setIsolationLevel(_ level: IsolationLevel) -> EventLoopFuture<Void> {
        let sql = "SET TRANSACTION ISOLATION LEVEL \(level.sqlLiteral)"
        return client.execute(sql).map { _ in () }
    }

    /// Sets the transaction isolation level (async version)
    @available(macOS 12.0, *)
    public func setIsolationLevel(_ level: IsolationLevel) async throws {
        try await setIsolationLevel(level).get()
    }

    // MARK: - Advanced Transaction Operations

    /// Executes a closure within a transaction context, automatically handling commit/rollback
    public func executeInTransaction<T>(_ operation: @escaping () -> EventLoopFuture<T>) -> EventLoopFuture<T> {
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

    /// Executes a closure within a transaction context, automatically handling commit/rollback (async version)
    @available(macOS 12.0, *)
    public func executeInTransaction<T>(_ operation: @escaping () async throws -> T) async throws -> T {
        try await beginTransaction()
        do {
            let result = try await operation()
            try await commitTransaction()
            return result
        } catch {
            try await rollbackTransaction()
            throw error
        }
    }

    /// Executes a closure within a savepoint context, automatically handling rollback on error
    public func executeInSavepoint<T>(
        named name: String,
        operation: @escaping () -> EventLoopFuture<T>
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

    // MARK: - Helper Methods

    private func escapeIdentifier(_ identifier: String) -> String {
        // Basic SQL identifier escaping
        if identifier.contains(" ") || identifier.contains("-") || identifier.contains(".") {
            return "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
        }
        return identifier
    }
}

// MARK: - Supporting Types

public struct TransactionInfo: Sendable {
    public let id: String?
    public let name: String?
    public let type: String?
    public let state: String?
    public let beginTime: Date?
    public let isolationLevel: EventLoopFuture<String?>

    public init(
        id: String?,
        name: String?,
        type: String?,
        state: String?,
        beginTime: Date?,
        isolationLevel: EventLoopFuture<String?>
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