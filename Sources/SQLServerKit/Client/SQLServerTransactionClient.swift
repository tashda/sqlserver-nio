import NIO
import NIOConcurrencyHelpers
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
    private let savepointsByConnection = NIOLockedValueBox<[Swift.ObjectIdentifier: [String]]>([:])

    private func updateSavepoints(_ connection: SQLServerConnection, _ update: (inout [String]) -> Void) {
        savepointsByConnection.withLockedValue { all in
            let id = Swift.ObjectIdentifier(connection)
            var names = all[id] ?? []
            update(&names)
            if names.isEmpty { all.removeValue(forKey: id) }
            else { all[id] = names }
        }
    }

    private func savepoints(on connection: SQLServerConnection?) -> [String] {
        guard let connection else { return [] }
        return savepointsByConnection.withLockedValue { $0[Swift.ObjectIdentifier(connection)] ?? [] }
    }

    private func requireConnection() throws -> SQLServerConnection {
        guard let connection = ClientScopedConnection.current else {
            throw SQLServerError.invalidArgument("Savepoints require a pinned transaction connection")
        }
        return connection
    }

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Transaction Management

    /// Begins a new transaction
    internal func beginTransaction() -> EventLoopFuture<Void> {
        guard let connection = ClientScopedConnection.current else {
            return client.eventLoopGroup.next().makeFailedFuture(SQLServerError.invalidArgument("A transaction requires a pinned connection; use executeInTransaction or SQLServerConnection.withTransaction"))
        }
        return connection.beginTransaction()
    }

    /// Begins a new transaction (async version)
    @available(macOS 12.0, *)
    public func beginTransaction() async throws {
        guard let connection = ClientScopedConnection.current else {
            throw SQLServerError.invalidArgument("A transaction requires a pinned connection; use executeInTransaction or SQLServerConnection.withTransaction")
        }
        try await connection.beginTransaction()
    }

    /// Commits the current transaction
    internal func commitTransaction() -> EventLoopFuture<Void> {
        guard let connection = ClientScopedConnection.current else {
            return client.eventLoopGroup.next().makeFailedFuture(SQLServerError.invalidArgument("No pinned transaction connection"))
        }
        return connection.commit().map {
            self.updateSavepoints(connection) { $0.removeAll() }
        }.flatMapErrorThrowing { error in
            throw SQLServerError.commitOutcomeUnknown(error)
        }
    }

    /// Commits the current transaction (async version)
    @available(macOS 12.0, *)
    public func commitTransaction() async throws {
        guard let connection = ClientScopedConnection.current else {
            throw SQLServerError.invalidArgument("No pinned transaction connection")
        }
        do {
            try await connection.commit()
            updateSavepoints(connection) { $0.removeAll() }
        } catch {
            throw SQLServerError.commitOutcomeUnknown(error)
        }
    }

    /// Rolls back the current transaction
    internal func rollbackTransaction() -> EventLoopFuture<Void> {
        guard let connection = ClientScopedConnection.current else {
            return client.eventLoopGroup.next().makeFailedFuture(SQLServerError.invalidArgument("No pinned transaction connection"))
        }
        return connection.rollback().map {
            self.updateSavepoints(connection) { $0.removeAll() }
        }
    }

    /// Rolls back the current transaction (async version)
    @available(macOS 12.0, *)
    public func rollbackTransaction() async throws {
        guard let connection = ClientScopedConnection.current else {
            throw SQLServerError.invalidArgument("No pinned transaction connection")
        }
        try await connection.rollback()
        updateSavepoints(connection) { $0.removeAll() }
    }

    // MARK: - Savepoint Management

    /// Creates a savepoint with the specified name
    internal func createSavepoint(name: String) -> EventLoopFuture<Void> {
        guard let connection = try? requireConnection() else {
            return client.eventLoopGroup.next().makeFailedFuture(SQLServerError.invalidArgument("Savepoints require a pinned transaction connection"))
        }
        return connection.createSavepoint(name).map {
            self.updateSavepoints(connection) { $0.append(name) }
        }
    }

    /// Creates a savepoint with the specified name (async version)
    @available(macOS 12.0, *)
    public func createSavepoint(name: String) async throws {
        let connection = try requireConnection()
        try await connection.createSavepoint(name)
        updateSavepoints(connection) { $0.append(name) }
    }

    /// Rolls back to the specified savepoint
    internal func rollbackToSavepoint(name: String) -> EventLoopFuture<Void> {
        guard let connection = try? requireConnection() else {
            return client.eventLoopGroup.next().makeFailedFuture(SQLServerError.invalidArgument("Savepoints require a pinned transaction connection"))
        }
        return connection.rollbackToSavepoint(name).map {
            // Remove this savepoint and any savepoints created after it
            self.updateSavepoints(connection) { names in
                if let index = names.firstIndex(of: name) { names.removeSubrange(index...) }
            }
        }
    }

    /// Rolls back to the specified savepoint (async version)
    @available(macOS 12.0, *)
    public func rollbackToSavepoint(name: String) async throws {
        let connection = try requireConnection()
        try await connection.rollbackToSavepoint(name)
        updateSavepoints(connection) { names in
            if let index = names.firstIndex(of: name) { names.removeSubrange(index...) }
        }
    }

    /// Releases the specified savepoint (SQL Server 2008+)
    internal func releaseSavepoint(name: String) -> EventLoopFuture<Void> {
        // SQL Server doesn't have an explicit RELEASE SAVEPOINT command like some other databases
        // We remove it from our tracking, but the savepoint still exists in the transaction
        guard let connection = try? requireConnection() else {
            return client.eventLoopGroup.next().makeFailedFuture(SQLServerError.invalidArgument("Savepoints require a pinned transaction connection"))
        }
        updateSavepoints(connection) { names in
            if let index = names.firstIndex(of: name) { names.remove(at: index) }
        }
        return connection.eventLoop.makeSucceededFuture(())
    }

    /// Releases the specified savepoint (async version)
    @available(macOS 12.0, *)
    public func releaseSavepoint(name: String) async throws {
        let connection = try requireConnection()
        updateSavepoints(connection) { names in
            if let index = names.firstIndex(of: name) { names.remove(at: index) }
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
        return savepoints(on: ClientScopedConnection.current).map { name in
            SavepointInfo(name: name, isActive: true)
        }
    }

    /// Checks if a savepoint with the given name is active
    public func isSavepointActive(name: String) -> Bool {
        return savepoints(on: ClientScopedConnection.current).contains(name)
    }

    /// Gets the current transaction isolation level
    internal func getCurrentIsolationLevel() -> EventLoopFuture<String?> {
        currentIsolationLevelFuture()
    }

    private func currentIsolationLevelFuture() -> EventLoopFuture<String?> {
        let sql = """
        SELECT CASE transaction_isolation_level
            WHEN 0 THEN 'UNSPECIFIED'
            WHEN 1 THEN 'READ UNCOMMITTED'
            WHEN 2 THEN 'READ COMMITTED'
            WHEN 3 THEN 'REPEATABLE READ'
            WHEN 4 THEN 'SERIALIZABLE'
            WHEN 5 THEN 'SNAPSHOT'
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
                    .flatMapError { error in
                        self.rollbackTransaction().flatMapThrowing { _ in
                            throw error
                        }
                    }
                    .flatMap { result in
                        self.commitTransaction().map { result }
                    }
            }
    }

    /// Executes a closure within a transaction context, automatically handling commit/rollback (async version).
    /// Pins a single connection for the entire transaction lifetime via task-local scoping.
    @available(macOS 12.0, *)
    public func executeInTransaction<T: Sendable>(_ operation: @Sendable @escaping () async throws -> T) async throws -> T {
        try await client.withConnection { connection in
            try await ClientScopedConnection.$current.withValue(connection) {
                defer { self.updateSavepoints(connection) { $0.removeAll() } }
                let result = try await connection.withTransaction { _ in
                    try await operation()
                }
                return result
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
