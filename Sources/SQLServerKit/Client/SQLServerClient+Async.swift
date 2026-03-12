import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerClient {
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
    public func withConnection<Result: Sendable>(
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
    public func query(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [SQLServerRow] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.query(sql)
        }
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
    public func queryScalar<T: SQLServerDataConvertible & Sendable>(
        _ sql: String,
        as type: T.Type = T.self,
        on eventLoop: EventLoop? = nil
    ) async throws -> T? {
        try await queryScalar(sql, as: type, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func call(
        procedure name: String,
        parameters: [SQLServerConnection.ProcedureParameter] = [],
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await call(procedure: name, parameters: parameters, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier],
        on eventLoop: EventLoop? = nil
    ) async throws -> [ObjectDefinition] {
        try await fetchObjectDefinitions(identifiers, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind,
        on eventLoop: EventLoop? = nil
    ) async throws -> ObjectDefinition? {
        try await fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func fetchAgentStatus(on eventLoop: EventLoop? = nil) async throws -> SQLServerAgentStatus {
        try await fetchAgentStatus(on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default,
        on eventLoop: EventLoop? = nil
    ) async throws -> [MetadataSearchHit] {
        try await searchMetadata(query: query, database: database, schema: schema, scopes: scopes, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listDatabases(on eventLoop: EventLoop? = nil) async throws -> [DatabaseMetadata] {
        try await listDatabases(on: eventLoop).get()
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
        try await listSchemas(in: database, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listTables(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TableMetadata] {
        try await listTables(database: database, schema: schema, includeComments: includeComments, on: eventLoop).get()
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
        try await listColumns(database: database, schema: schema, table: table, objectTypeHint: objectTypeHint, includeComments: includeComments, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listColumnsForSchema(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ColumnMetadata] {
        try await listColumnsForSchema(database: database, schema: schema, includeComments: includeComments, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listColumnsForDatabase(
        database: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ColumnMetadata] {
        try await listColumnsForDatabase(database: database, includeComments: includeComments, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ParameterMetadata] {
        try await listParameters(database: database, schema: schema, object: object, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await listPrimaryKeys(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeysFromCatalog(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await listPrimaryKeysFromCatalog(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await listUniqueConstraints(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [IndexMetadata] {
        try await listIndexes(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ForeignKeyMetadata] {
        try await listForeignKeys(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [DependencyMetadata] {
        try await listDependencies(database: database, schema: schema, object: object, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TriggerMetadata] {
        try await listTriggers(database: database, schema: schema, table: table, includeComments: includeComments, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [RoutineMetadata] {
        try await listProcedures(database: database, schema: schema, includeComments: includeComments, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [RoutineMetadata] {
        try await listFunctions(database: database, schema: schema, includeComments: includeComments, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func loadSchemaStructure(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerSchemaStructure {
        try await loadSchemaStructure(database: database, schema: schema, includeComments: includeComments, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func loadDatabaseStructure(
        database: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerDatabaseStructure {
        try await loadDatabaseStructure(database: database, includeComments: includeComments, on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func serverVersion(on eventLoop: EventLoop? = nil) async throws -> String {
        try await serverVersion(on: eventLoop).get()
    }

    @available(macOS 12.0, *)
    public func executeOnFreshConnection(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await executeOnFreshConnection(sql, on: eventLoop).get()
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
        _ = withConnection { _ in
            return self.eventLoopGroup.next().makeSucceededFuture(())
        }
    }
}
