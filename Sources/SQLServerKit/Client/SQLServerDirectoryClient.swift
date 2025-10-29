import Foundation
import SQLServerTDS

public protocol DirectoryLookupProvider: Sendable {
    func search(query: String, kinds: [DirectoryKind]) async throws -> [DirectoryPrincipal]
}

public enum DirectoryKind: Sendable {
    case user
    case group
    case service
}

public struct DirectoryPrincipal: Sendable {
    public let name: String
    public let kind: DirectoryKind
}

public final class SQLServerDirectoryClient {
    private enum Backing { case connection(SQLServerConnection), client(SQLServerClient) }
    private let backing: Backing
    public let provider: DirectoryLookupProvider?

    public convenience init(connection: SQLServerConnection, provider: DirectoryLookupProvider? = nil) {
        self.init(backing: .connection(connection), provider: provider)
    }
    public convenience init(client: SQLServerClient, provider: DirectoryLookupProvider? = nil) {
        self.init(backing: .client(client), provider: provider)
    }
    private init(backing: Backing, provider: DirectoryLookupProvider?) {
        self.backing = backing
        self.provider = provider
    }

    // Server-side validation only; best-effort
    public struct ValidationOptions: Sendable { public init() {} }
    public func validatePrincipal(name: String, options: ValidationOptions = .init()) -> EventLoopFuture<PrincipalResolution> {
        let escaped = name.replacingOccurrences(of: "'", with: "''")
        let sql = "SELECT sid = SUSER_SID(N'\(escaped)'), sname = SUSER_SNAME(SUSER_SID(N'\(escaped)'));"
        return run(sql: sql).map { rows in
            if let row = rows.first, let sid = row.column("sid")?.bytes {
                return PrincipalResolution(input: name, exists: true, principalType: nil, sid: Data(sid))
            }
            return PrincipalResolution(input: name, exists: false, principalType: nil, sid: nil)
        }
    }

    private func run(sql: String) -> EventLoopFuture<[TDSRow]> {
        switch backing {
        case .client(let c): return c.query(sql)
        case .connection(let conn): return conn.query(sql)
        }
    }
}

