import NIO
import SQLServerTDS

public struct SQLServerLoginInfo: Sendable {
    public let name: String
    public let type: String
    public let isDisabled: Bool
}

public struct SQLServerRoleInfo: Sendable {
    public let name: String
    public let isFixedRole: Bool
}

public final class SQLServerAdministrationClient {
    private enum Backing {
        case connection(SQLServerConnection)
        case client(SQLServerClient)
    }

    private let backing: Backing

    public convenience init(connection: SQLServerConnection) {
        self.init(backing: .connection(connection))
    }

    public convenience init(client: SQLServerClient) {
        self.init(backing: .client(client))
    }

    private init(backing: Backing) {
        self.backing = backing
    }

    public func listServerLogins(includeDisabled: Bool = true) -> EventLoopFuture<[SQLServerLoginInfo]> {
        let filter = includeDisabled ? "" : "WHERE sp.is_disabled = 0"
        let sql = """
        SELECT sp.name, sp.type_desc, sp.is_disabled
        FROM sys.server_principals AS sp
        WHERE sp.type IN ('S','U','G','E','X')
        \(filter.isEmpty ? "" : filter)
        ORDER BY sp.name;
        """

        return run(sql).map { rows in
            rows.compactMap { row in
                guard
                    let name = row.column("name")?.string,
                    let type = row.column("type_desc")?.string,
                    let disabled = row.column("is_disabled")?.int
                else {
                    return nil
                }
                return SQLServerLoginInfo(name: name, type: type, isDisabled: disabled != 0)
            }
        }
    }

    public func listServerRoles() -> EventLoopFuture<[SQLServerRoleInfo]> {
        let sql = """
        SELECT name, is_fixed_role = CAST(ISNULL(is_fixed_role, 0) AS INT)
        FROM sys.server_principals
        WHERE type = 'R'
        ORDER BY name;
        """

        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                let isFixed = row.column("is_fixed_role")?.int ?? 0
                return SQLServerRoleInfo(name: name, isFixedRole: isFixed != 0)
            }
        }
    }

    @available(macOS 12.0, *)
    public func listServerLogins(includeDisabled: Bool = true) async throws -> [SQLServerLoginInfo] {
        try await listServerLogins(includeDisabled: includeDisabled).get()
    }

    @available(macOS 12.0, *)
    public func listServerRoles() async throws -> [SQLServerRoleInfo] {
        try await listServerRoles().get()
    }

    private func run(_ sql: String) -> EventLoopFuture<[TDSRow]> {
        switch backing {
        case .connection(let connection):
            return connection.query(sql)
        case .client(let client):
            return client.query(sql)
        }
    }
}
