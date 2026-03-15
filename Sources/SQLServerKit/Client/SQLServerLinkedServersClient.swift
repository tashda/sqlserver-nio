import Foundation
import NIO
import SQLServerTDS

// MARK: - Linked Server Types

/// Information about a linked server registered on the SQL Server instance.
public struct SQLServerLinkedServer: Sendable, Hashable {
    public let name: String
    public let product: String
    public let provider: String
    public let dataSource: String
    public let catalog: String?
    public let providerString: String?
    public let isDataAccessEnabled: Bool
    public let isRemoteLoginEnabled: Bool
    public let modifyDate: Date?

    public init(
        name: String,
        product: String,
        provider: String,
        dataSource: String,
        catalog: String? = nil,
        providerString: String? = nil,
        isDataAccessEnabled: Bool = true,
        isRemoteLoginEnabled: Bool = false,
        modifyDate: Date? = nil
    ) {
        self.name = name
        self.product = product
        self.provider = provider
        self.dataSource = dataSource
        self.catalog = catalog
        self.providerString = providerString
        self.isDataAccessEnabled = isDataAccessEnabled
        self.isRemoteLoginEnabled = isRemoteLoginEnabled
        self.modifyDate = modifyDate
    }
}

/// Login mapping for a linked server.
public struct SQLServerLinkedServerLoginMapping: Sendable, Hashable {
    public let remoteServerName: String
    public let localLogin: String?
    public let usesSelf: Bool
    public let remoteUser: String?

    public init(
        remoteServerName: String,
        localLogin: String? = nil,
        usesSelf: Bool = false,
        remoteUser: String? = nil
    ) {
        self.remoteServerName = remoteServerName
        self.localLogin = localLogin
        self.usesSelf = usesSelf
        self.remoteUser = remoteUser
    }
}

// MARK: - SQLServerLinkedServersClient

public final class SQLServerLinkedServersClient: @unchecked Sendable {
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
    private init(backing: Backing) { self.backing = backing }

    // MARK: - List Linked Servers

    internal func listLinkedServers() -> EventLoopFuture<[SQLServerLinkedServer]> {
        run(sql: """
            SELECT s.name, s.product, s.provider, s.data_source,
                   s.catalog, s.provider_string,
                   s.is_data_access_enabled, s.is_remote_login_enabled,
                   s.modify_date
            FROM sys.servers s
            WHERE s.is_linked = 1
            ORDER BY s.name
        """).map { rows in
            rows.compactMap { row -> SQLServerLinkedServer? in
                guard let name = row.column("name")?.string else { return nil }
                return SQLServerLinkedServer(
                    name: name,
                    product: row.column("product")?.string ?? "",
                    provider: row.column("provider")?.string ?? "",
                    dataSource: row.column("data_source")?.string ?? "",
                    catalog: row.column("catalog")?.string,
                    providerString: row.column("provider_string")?.string,
                    isDataAccessEnabled: (row.column("is_data_access_enabled")?.int ?? 0) == 1,
                    isRemoteLoginEnabled: (row.column("is_remote_login_enabled")?.int ?? 0) == 1,
                    modifyDate: row.column("modify_date")?.date
                )
            }
        }
    }

    // MARK: - Add Linked Server

    internal func addLinkedServer(
        name: String,
        provider: String = "SQLNCLI",
        dataSource: String,
        product: String = "",
        catalog: String? = nil,
        providerString: String? = nil
    ) -> EventLoopFuture<Void> {
        var sql = """
            EXEC sp_addlinkedserver
                @server = N'\(escapeLiteral(name))',
                @srvproduct = N'\(escapeLiteral(product))',
                @provider = N'\(escapeLiteral(provider))',
                @datasrc = N'\(escapeLiteral(dataSource))'
        """
        if let catalog {
            sql += ", @catalog = N'\(escapeLiteral(catalog))'"
        }
        if let providerString {
            sql += ", @provstr = N'\(escapeLiteral(providerString))'"
        }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    // MARK: - Drop Linked Server

    internal func dropLinkedServer(name: String, dropLogins: Bool = true) -> EventLoopFuture<Void> {
        var sql = "EXEC sp_dropserver @server = N'\(escapeLiteral(name))'"
        if dropLogins {
            sql += ", @droplogins = 'droplogins'"
        }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    // MARK: - Add Login Mapping

    internal func addLoginMapping(
        serverName: String,
        usesSelf: Bool = false,
        remoteUser: String? = nil,
        remotePassword: String? = nil
    ) -> EventLoopFuture<Void> {
        var sql = "EXEC sp_addlinkedsrvlogin @rmtsrvname = N'\(escapeLiteral(serverName))'"
        sql += ", @useself = N'\(usesSelf ? "True" : "False")'"
        if let remoteUser {
            sql += ", @rmtuser = N'\(escapeLiteral(remoteUser))'"
        }
        if let remotePassword {
            sql += ", @rmtpassword = N'\(escapeLiteral(remotePassword))'"
        }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    // MARK: - Test Connection

    internal func testConnection(name: String) -> EventLoopFuture<Bool> {
        let sql = """
            BEGIN TRY
                EXEC sp_testlinkedserver N'\(escapeLiteral(name))';
                SELECT CAST(1 AS bit) AS success;
            END TRY
            BEGIN CATCH
                SELECT CAST(0 AS bit) AS success;
            END CATCH
        """
        return run(sql: sql).map { rows in
            guard let first = rows.first else { return false }
            return (first.column("success")?.int ?? 0) == 1
        }
    }

    // MARK: - Async Convenience

    @available(macOS 12.0, *)
    public func list() async throws -> [SQLServerLinkedServer] {
        try await listLinkedServers().get()
    }

    @available(macOS 12.0, *)
    public func add(
        name: String,
        provider: String = "SQLNCLI",
        dataSource: String,
        product: String = "",
        catalog: String? = nil,
        providerString: String? = nil
    ) async throws {
        _ = try await addLinkedServer(
            name: name,
            provider: provider,
            dataSource: dataSource,
            product: product,
            catalog: catalog,
            providerString: providerString
        ).get()
    }

    @available(macOS 12.0, *)
    public func drop(name: String, dropLogins: Bool = true) async throws {
        _ = try await dropLinkedServer(name: name, dropLogins: dropLogins).get()
    }

    @available(macOS 12.0, *)
    public func addLoginMapping(
        serverName: String,
        usesSelf: Bool = false,
        remoteUser: String? = nil,
        remotePassword: String? = nil
    ) async throws {
        _ = try await addLoginMapping(
            serverName: serverName,
            usesSelf: usesSelf,
            remoteUser: remoteUser,
            remotePassword: remotePassword
        ).get()
    }

    @available(macOS 12.0, *)
    public func test(name: String) async throws -> Bool {
        try await testConnection(name: name).get()
    }

    // MARK: - Helpers

    private func run(sql: String) -> EventLoopFuture<[SQLServerRow]> {
        switch backing {
        case .client(let c): return c.query(sql)
        case .connection(let conn): return conn.query(sql)
        }
    }

    private func exec(sql: String) -> EventLoopFuture<SQLServerExecutionResult> {
        switch backing {
        case .client(let c): return c.execute(sql)
        case .connection(let conn): return conn.execute(sql)
        }
    }

    private func escapeLiteral(_ literal: String) -> String {
        literal.replacingOccurrences(of: "'", with: "''")
    }
}
