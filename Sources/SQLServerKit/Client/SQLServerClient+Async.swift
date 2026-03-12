import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerClient {
    @available(macOS 12.0, *)
    public func withConnection<Result: Sendable>(
        _ operation: @escaping @Sendable (SQLServerConnection) async throws -> Result
    ) async throws -> Result {
        try await withConnection(on: nil, operation)
    }

    @available(macOS 12.0, *)
    public func withDatabase<Result: Sendable>(
        _ database: String,
        _ operation: @escaping @Sendable (SQLServerConnection) async throws -> Result
    ) async throws -> Result {
        try await withConnection { connection in
            let originalDatabase = connection.currentDatabase
            let needsReset = originalDatabase.caseInsensitiveCompare(database) != .orderedSame
            if needsReset {
                _ = try await connection.changeDatabase(database)
            }
            do {
                let result = try await operation(connection)
                if needsReset {
                    _ = try await connection.changeDatabase(originalDatabase)
                }
                return result
            } catch {
                if needsReset {
                    _ = try? await connection.changeDatabase(originalDatabase)
                }
                throw error
            }
        }
    }

    @available(macOS 12.0, *)
    internal func withConnection<Result: Sendable>(
        on eventLoop: EventLoop? = nil,
        _ operation: @escaping @Sendable (SQLServerConnection) async throws -> Result
    ) async throws -> Result {
        let future: EventLoopFuture<Result> = self.withConnection(on: eventLoop) { connection in
            let promise = connection.eventLoop.makePromise(of: Result.self)
            let completed = NIOLockedValueBox(false)

            let task = Task {
                do {
                    let result = try await withTaskCancellationHandler(operation: {
                        try await operation(connection)
                    }, onCancel: {
                        connection.cancelActiveRequest()
                    })

                    if !completed.withLockedValue({ $0 }) {
                        completed.withLockedValue { $0 = true }
                        promise.succeed(result)
                    }
                } catch {
                    if !completed.withLockedValue({ $0 }) {
                        completed.withLockedValue { $0 = true }
                        let errorToFail: Error
                        if let sqlError = error as? SQLServerError,
                           case .deadlockDetected = sqlError {
                            errorToFail = sqlError
                        } else {
                            errorToFail = error
                        }
                        promise.fail(errorToFail)
                    }
                }
            }

            connection.underlying.closeFuture.whenComplete { _ in
                if !completed.withLockedValue({ $0 }) {
                    completed.withLockedValue { $0 = true }
                    promise.fail(SQLServerError.connectionClosed)
                }
            }

            promise.futureResult.whenFailure { _ in
                task.cancel()
            }

            return promise.futureResult
        }

        do {
            return try await future.get()
        } catch {
            if let channelError = error as? ChannelError,
               case .alreadyClosed = channelError {
                throw SQLServerError.connectionClosed
            }
            throw error
        }
    }

    @available(macOS 12.0, *)
    public func execute(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await withConnection(on: eventLoop) { connection in
            try await connection.execute(sql)
        }
    }

    @available(macOS 12.0, *)
    public func execute(_ sql: String) async throws -> SQLServerExecutionResult {
        try await execute(sql, on: nil)
    }

    @available(macOS 12.0, *)
    public func query(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [SQLServerRow] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.query(sql)
        }
    }

    @available(macOS 12.0, *)
    public func query(_ sql: String) async throws -> [SQLServerRow] {
        try await query(sql, on: nil)
    }

    @available(macOS 12.0, *)
    public func queryPaged(
        _ sql: String,
        limit: Int,
        offset: Int = 0,
        on eventLoop: EventLoop? = nil
    ) async throws -> [SQLServerRow] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.queryPaged(sql, limit: limit, offset: offset)
        }
    }

    @available(macOS 12.0, *)
    public func queryPaged(
        _ sql: String,
        limit: Int,
        offset: Int = 0
    ) async throws -> [SQLServerRow] {
        try await queryPaged(sql, limit: limit, offset: offset, on: nil)
    }

    @available(macOS 12.0, *)
    public func queryScalar<T: SQLServerDataConvertible & Sendable>(
        _ sql: String,
        as type: T.Type = T.self,
        on eventLoop: EventLoop? = nil
    ) async throws -> T? {
        try await withConnection(on: eventLoop) { connection in
            try await connection.queryScalar(sql, as: type)
        }
    }

    @available(macOS 12.0, *)
    public func queryScalar<T: SQLServerDataConvertible & Sendable>(
        _ sql: String,
        as type: T.Type = T.self
    ) async throws -> T? {
        try await queryScalar(sql, as: type, on: nil)
    }

    @available(macOS 12.0, *)
    public func call(
        procedure name: String,
        parameters: [SQLServerConnection.ProcedureParameter] = [],
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await withConnection(on: eventLoop) { connection in
            try await connection.call(procedure: name, parameters: parameters)
        }
    }

    @available(macOS 12.0, *)
    public func call(
        procedure name: String,
        parameters: [SQLServerConnection.ProcedureParameter] = []
    ) async throws -> SQLServerExecutionResult {
        try await call(procedure: name, parameters: parameters, on: nil)
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier],
        on eventLoop: EventLoop? = nil
    ) async throws -> [ObjectDefinition] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.fetchObjectDefinitions(identifiers).get()
        }
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier]
    ) async throws -> [ObjectDefinition] {
        try await fetchObjectDefinitions(identifiers, on: nil)
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind,
        on eventLoop: EventLoop? = nil
    ) async throws -> ObjectDefinition? {
        try await withConnection(on: eventLoop) { connection in
            try await connection.fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind).get()
        }
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind
    ) async throws -> ObjectDefinition? {
        try await fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind, on: nil)
    }

    @available(macOS 12.0, *)
    internal func fetchAgentStatus(on eventLoop: EventLoop? = nil) async throws -> SQLServerAgentStatus {
        try await withConnection(on: eventLoop) { connection in
            try await connection.fetchAgentStatus()
        }
    }

    @available(macOS 12.0, *)
    public func fetchAgentStatus() async throws -> SQLServerAgentStatus {
        try await fetchAgentStatus(on: nil)
    }

    @available(macOS 12.0, *)
    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default,
        on eventLoop: EventLoop? = nil
    ) async throws -> [MetadataSearchHit] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.searchMetadata(query: query, database: database, schema: schema, scopes: scopes).get()
        }
    }

    @available(macOS 12.0, *)
    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default
    ) async throws -> [MetadataSearchHit] {
        try await searchMetadata(query: query, database: database, schema: schema, scopes: scopes, on: nil)
    }

    @available(macOS 12.0, *)
    internal func listDatabases(on eventLoop: EventLoop? = nil) async throws -> [DatabaseMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listDatabases().get()
        }
    }

    @available(macOS 12.0, *)
    public func listDatabases() async throws -> [DatabaseMetadata] {
        try await listDatabases(on: nil)
    }

    @available(macOS 12.0, *)
    public func databaseState(name: String) async throws -> DatabaseMetadata {
        try await withConnection { connection in
            connection.eventLoop.makeFutureWithTask {
                try await connection.databaseState(name: name)
            }
        }.get()
    }

    @available(macOS 12.0, *)
    public func listSchemas(
        in database: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [SchemaMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listSchemas(in: database).get()
        }
    }

    @available(macOS 12.0, *)
    public func listSchemas(in database: String? = nil) async throws -> [SchemaMetadata] {
        try await listSchemas(in: database, on: nil)
    }

    @available(macOS 12.0, *)
    public func listTables(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TableMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listTables(database: database, schema: schema, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func listTables(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [TableMetadata] {
        try await listTables(database: database, schema: schema, includeComments: includeComments, on: nil)
    }

    @available(macOS 12.0, *)
    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        objectTypeHint: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ColumnMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listColumns(
                database: database,
                schema: schema,
                table: table,
                objectTypeHint: objectTypeHint,
                includeComments: includeComments
            ).get()
        }
    }

    @available(macOS 12.0, *)
    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        objectTypeHint: String? = nil,
        includeComments: Bool = false
    ) async throws -> [ColumnMetadata] {
        try await listColumns(database: database, schema: schema, table: table, objectTypeHint: objectTypeHint, includeComments: includeComments, on: nil)
    }

    @available(macOS 12.0, *)
    public func listColumnsForSchema(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ColumnMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listColumnsForSchema(
                database: database,
                schema: schema,
                includeComments: includeComments
            ).get()
        }
    }

    @available(macOS 12.0, *)
    public func listColumnsForDatabase(
        database: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ColumnMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listColumnsForDatabase(
                database: database,
                includeComments: includeComments
            ).get()
        }
    }

    @available(macOS 12.0, *)
    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ParameterMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listParameters(database: database, schema: schema, object: object).get()
        }
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listPrimaryKeys(database: database, schema: schema, table: table).get()
        }
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeysFromCatalog(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listPrimaryKeysFromCatalog(database: database, schema: schema, table: table).get()
        }
    }

    @available(macOS 12.0, *)
    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listUniqueConstraints(database: database, schema: schema, table: table).get()
        }
    }

    @available(macOS 12.0, *)
    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [IndexMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listIndexes(database: database, schema: schema, table: table).get()
        }
    }

    @available(macOS 12.0, *)
    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ForeignKeyMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listForeignKeys(database: database, schema: schema, table: table).get()
        }
    }

    @available(macOS 12.0, *)
    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [DependencyMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listDependencies(database: database, schema: schema, object: object).get()
        }
    }

    @available(macOS 12.0, *)
    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TriggerMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listTriggers(
                database: database,
                schema: schema,
                table: table,
                includeComments: includeComments
            ).get()
        }
    }

    @available(macOS 12.0, *)
    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [RoutineMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listProcedures(database: database, schema: schema, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [RoutineMetadata] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.listFunctions(database: database, schema: schema, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func loadSchemaStructure(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerSchemaStructure {
        try await withConnection(on: eventLoop) { connection in
            try await connection.loadSchemaStructure(database: database, schema: schema, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func loadDatabaseStructure(
        database: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerDatabaseStructure {
        try await withConnection(on: eventLoop) { connection in
            try await connection.loadDatabaseStructure(database: database, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    internal func serverVersion(on eventLoop: EventLoop? = nil) async throws -> String {
        try await withConnection(on: eventLoop) { connection in
            try await connection.serverVersion()
        }
    }

    @available(macOS 12.0, *)
    public func serverVersion() async throws -> String {
        try await serverVersion(on: nil)
    }

    @available(macOS 12.0, *)
    public func executeOnFreshConnection(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        let loop = eventLoop ?? eventLoopGroup.next()
        return try await withCheckedThrowingContinuation { continuation in
            self.executeOnFreshConnection(sql, on: loop).whenComplete { result in
                continuation.resume(with: result)
            }
        }
    }

    @available(macOS 12.0, *)
    public func executeSeparateBatches(_ sqlStatements: [String]) async throws -> [SQLServerExecutionResult] {
        let script = sqlStatements.joined(separator: "\nGO\n")
        return try await executeScript(script)
    }

    @available(macOS 12.0, *)
    public func executeScript(_ sql: String) async throws -> [SQLServerExecutionResult] {
        let splitResults = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        return try await executeWithConnectionLock { connection in
            var results: [SQLServerExecutionResult] = []
            for (_, splitResult) in splitResults.enumerated() {
                if splitResult.text.isEmpty || self.isCommentOnlyBatch(splitResult.text) { continue }
                do {
                    let result = try await connection.execute(splitResult.text).get()
                    if let errorMessage = result.messages.first(where: { $0.kind == .error }) {
                        if errorMessage.number == 1205 {
                            throw SQLServerError.deadlockDetected(message: errorMessage.message)
                        } else {
                            throw SQLServerError.sqlExecutionError(message: errorMessage.message)
                        }
                    }
                    results.append(result)
                } catch {
                    throw error
                }
            }
            return results
        }
    }

    @available(macOS 12.0, *)
    private func executeWithConnectionLock<T: Sendable>(_ operation: @escaping @Sendable (SQLServerConnection) async throws -> T) async throws -> T {
        let future: EventLoopFuture<T> = self.withConnection(on: nil) { connection in
            let promise = connection.eventLoop.makePromise(of: T.self)
            let didComplete = NIOLockedValueBox(false)
            promise.futureResult.whenComplete { _ in
                didComplete.withLockedValue { $0 = true }
            }
            connection.underlying.closeFuture.whenComplete { _ in
                if !didComplete.withLockedValue({ $0 }) {
                    promise.fail(SQLServerError.connectionClosed)
                }
            }
            let _ = Task {
                do {
                    let result = try await operation(connection)
                    if !didComplete.withLockedValue({ $0 }) {
                        promise.succeed(result)
                    }
                } catch {
                    if !didComplete.withLockedValue({ $0 }) {
                        if error.localizedDescription.contains("Already closed") {
                            promise.fail(SQLServerError.connectionClosed)
                        } else {
                            promise.fail(error)
                        }
                    }
                }
            }
            return promise.futureResult
        }
        return try await future.get()
    }

    @available(macOS 12.0, *)
    public func healthCheck() async throws -> Bool {
        do {
            let rows = try await query("SELECT 1 as health_check")
            return rows.count == 1 && rows.first?.column("health_check")?.int == 1
        } catch {
            logger.warning("Health check failed: \(error)")
            return false
        }
    }

    @available(macOS 12.0, *)
    public func validateConnections() async throws {
        _ = try await withConnection { _ in
            ()
        }
    }
}
