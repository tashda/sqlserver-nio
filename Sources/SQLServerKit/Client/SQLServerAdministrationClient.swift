import NIO
import SQLServerTDS

public final class SQLServerAdministrationClient: @unchecked Sendable {
    internal let client: SQLServerClient
    internal let database: String?

    public init(client: SQLServerClient, database: String? = nil) {
        self.client = client
        self.database = database
    }

    public func scoped(to database: String?) -> SQLServerAdministrationClient {
        SQLServerAdministrationClient(client: client, database: database)
    }

    internal func listServerLogins(includeDisabled: Bool = true) -> EventLoopFuture<[SQLServerLoginInfo]> {
        let filter = includeDisabled ? "" : "WHERE sp.is_disabled = 0"
        let sql = """
        SELECT sp.name, sp.type_desc, sp.is_disabled
        FROM sys.server_principals AS sp
        WHERE sp.type IN ('S','U','G','E','X')
        \(filter.isEmpty ? "" : filter)
        ORDER BY sp.name;
        """

        return client.query(sql).map {
            rows in
            rows.compactMap { row -> SQLServerLoginInfo? in
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

    internal func listServerRoles() -> EventLoopFuture<[SQLServerRoleInfo]> {
        let sql = """
        SELECT name, is_fixed_role = CAST(ISNULL(is_fixed_role, 0) AS INT)
        FROM sys.server_principals
        WHERE type = 'R'
        ORDER BY name;
        """

        return client.query(sql).map {
            rows in
            rows.compactMap { row -> SQLServerRoleInfo? in
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
}
