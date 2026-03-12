import NIO
import SQLServerTDS

// MARK: - SQLServerSecurityClient

public final class SQLServerSecurityClient: @unchecked Sendable {
    private enum Backing {
        case connection(SQLServerConnection)
        case client(SQLServerClient)
    }
    private let backing: Backing

    public convenience init(client: SQLServerClient) {
        self.init(backing: .client(client))
    }

    public convenience init(connection: SQLServerConnection) {
        self.init(backing: .connection(connection))
    }

    private init(backing: Backing) {
        self.backing = backing
    }
    
    internal var eventLoop: EventLoop {
        switch backing {
        case .client(let c): return c.eventLoopGroup.next()
        case .connection(let conn): return conn.eventLoop
        }
    }

    internal static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }

    // MARK: - Backing execution helpers
    @available(macOS 12.0, *)
    internal func exec(_ sql: String) async throws -> SQLServerExecutionResult {
        switch backing {
        case .client(let c):
            return try await c.execute(sql)
        case .connection(let conn):
            return try await conn.execute(sql).get()
        }
    }
    
    @available(macOS 12.0, *)
    internal func query(_ sql: String) async throws -> [SQLServerRow] {
        switch backing {
        case .client(let c):
            return try await c.query(sql)
        case .connection(let conn):
            return try await conn.query(sql).get()
        }
    }
    
    @available(macOS 12.0, *)
    internal func queryScalar<T: SQLServerDataConvertible & Sendable>(_ sql: String, as: T.Type) async throws -> T? {
        switch backing {
        case .client(let c):
            return try await c.queryScalar(sql, as: T.self)
        case .connection(let conn):
            return try await conn.queryScalar(sql, as: T.self).get()
        }
    }

    // Non-async convenience used by some EventLoopFuture-returning helpers
    internal func run(_ sql: String) -> EventLoopFuture<[SQLServerRow]> {
        switch backing {
        case .client(let c):
            return c.query(sql)
        case .connection(let conn):
            return conn.query(sql)
        }
    }
}

// Back-compat: database-scoped client alias
public typealias SQLServerDatabaseSecurityClient = SQLServerSecurityClient
