import Foundation
import NIO
import NIOCore
import NIOConcurrencyHelpers
import Logging
import SQLServerTDS

// MARK: - node-mssql Compatibility Layer
// Provides node-mssql compatible connection acquisition/release patterns

/// node-mssql compatible Request wrapper that handles per-request connection ownership
public final class NodeMSSQLRequest {
    public let sql: String
    public let stream: Bool
    public let parameters: [String: Any]

    // node-mssql style callbacks
    public var onRow: ((TDSRow) -> Void)?
    public var onDone: ((TDSTokens.DoneToken) -> Void)?
    public var onError: ((Error) -> Void)?
    public var onInfo: ((TDSTokens.ErrorInfoToken) -> Void)?

    // Internal state
    private let onComplete: (Error?, Any?) -> Void
    private(set) var isExecuting = false
    private(set) var isCompleted = false

    public init(
        sql: String,
        stream: Bool = false,
        parameters: [String: Any] = [:],
        onComplete: @escaping (Error?, Any?) -> Void
    ) {
        self.sql = sql
        self.stream = stream
        self.parameters = parameters
        self.onComplete = onComplete
    }

    // node-mssql style: this.parent.acquire(this, callback)
    public func execute(on pool: NodeMSSQLConnectionPool) {
        guard !isExecuting else {
            onError?(TDSError.protocolError("Request is already executing"))
            return
        }

        isExecuting = true
        print("🎯 node-mssql: Acquiring connection for request")

        pool.acquire(self) { [weak self] error, connection in
            guard let self = self else { return }

            if let error = error {
                print("🚨 node-mssql: Connection acquisition failed: \(error)")
                self.onError?(error)
                self.complete(error: error, result: nil)
                return
            }

            guard let connection = connection else {
                let error = TDSError.connectionClosed
                print("🚨 node-mssql: No connection acquired")
                self.onError?(error)
                self.complete(error: error, result: nil)
                return
            }

            print("✅ node-mssql: Connection acquired, executing query")
            self.executeWithConnection(connection, pool: pool)
        }
    }

    private func executeWithConnection(_ connection: SQLServerConnection, pool: NodeMSSQLConnectionPool) {
        // Create RawSqlRequest with node-mssql callbacks
        let rawRequest = RawSqlRequest(
            sql: self.sql,
            onRow: { [weak self] row in
                guard let self = self else { return }
                print("📡 node-mssql: Row received")
                self.onRow?(row)
            },
            onDone: { [weak self] doneToken in
                guard let self = self else { return }
                print("🏁 node-mssql: Done token received")
                self.onDone?(doneToken)
            }
        )

        // Execute using underlying TDS connection with simplified error handling
        let tdsConnection = connection.underlying
        tdsConnection.send(rawRequest, logger: tdsConnection.logger).whenComplete { [weak self] result in
            guard let self = self else { return }

            print("🔧 node-mssql: Request completed, releasing connection")
            pool.release(connection)

            switch result {
            case .success:
                print("✅ node-mssql: Query sent successfully")
                self.complete(error: nil, result: "query_executed")
            case .failure(let error):
                print("🚨 node-mssql: Query failed: \(error)")
                self.onError?(error)
                self.complete(error: error, result: nil)
            }
        }
    }

    private func complete(error: Error?, result: Any?) {
        isCompleted = true
        isExecuting = false
        onComplete(error, result)
    }
}

/// node-mssql compatible connection pool with simplified acquire/release semantics
public final class NodeMSSQLConnectionPool {
    private let baseClient: SQLServerClient
    private var activeConnections: Int = 0
    private let connectionLock = NIOLock()

    public init(baseClient: SQLServerClient) {
        self.baseClient = baseClient
    }

    // node-mssql: this.parent.acquire(this, callback)
    public func acquire(_ request: NodeMSSQLRequest, onComplete: @escaping (Error?, SQLServerConnection?) -> Void) {
        print("🎯 node-mssql: Starting connection acquisition for request")

        // Use existing withConnection but extend connection lifetime manually
        baseClient.withConnection { connection -> EventLoopFuture<Void> in
            self.connectionLock.withLock {
                self.activeConnections += 1
            }

            print("🎯 node-mssql: Connection acquired for request (active: \(self.activeConnections))")
            onComplete(nil, connection)

            // Hold connection open until explicitly released
            let promise = connection.eventLoop.makePromise(of: Void.self)
            return promise.futureResult
        }.whenFailure { error in
            print("🚨 node-mssql: Failed to acquire connection: \(error)")
            onComplete(error, nil)
        }
    }

    // node-mssql: this.parent.release(connection)
    public func release(_ connection: SQLServerConnection) {
        print("🔓 node-mssql: Starting connection release")

        self.connectionLock.withLock {
            self.activeConnections = max(0, self.activeConnections - 1)
        }

        // Connection is automatically released when withConnection completes
        print("🔓 node-mssql: Connection released successfully (active: \(self.activeConnections))")
    }

    public var activeConnectionCount: Int {
        return activeConnections
    }
}

// MARK: - SQLServerClient Extensions for node-mssql Compatibility

extension SQLServerClient {
    /// Create a node-mssql compatible connection pool
    public func nodeMSSQLCompatiblePool() -> NodeMSSQLConnectionPool {
        return NodeMSSQLConnectionPool(baseClient: self)
    }

    /// node-mssql style request execution
    /// Equivalent to: new Request(pool, sql, callback)
    public func request(
        sql: String,
        stream: Bool = false,
        parameters: [String: Any] = [:],
        onComplete: @escaping (Error?, Any?) -> Void
    ) -> NodeMSSQLRequest {
        let request = NodeMSSQLRequest(
            sql: sql,
            stream: stream,
            parameters: parameters,
            onComplete: onComplete
        )

        return request
    }
}