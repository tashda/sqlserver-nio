import Foundation
import NIO

// MARK: - CMS Types

/// A Central Management Server group.
public struct SQLServerCMSGroup: Sendable, Equatable, Identifiable {
    public var id: Int { groupId }

    public let groupId: Int
    public let name: String
    public let description: String
    public let parentId: Int

    public init(groupId: Int, name: String, description: String, parentId: Int) {
        self.groupId = groupId
        self.name = name
        self.description = description
        self.parentId = parentId
    }

    /// Whether this is a root-level group.
    public var isRoot: Bool { parentId == 0 }
}

/// A server registered in Central Management Server.
public struct SQLServerCMSServer: Sendable, Equatable, Identifiable {
    public var id: Int { serverId }

    public let serverId: Int
    public let name: String
    public let serverName: String
    public let description: String
    public let groupId: Int

    public init(serverId: Int, name: String, serverName: String, description: String, groupId: Int) {
        self.serverId = serverId
        self.name = name
        self.serverName = serverName
        self.description = description
        self.groupId = groupId
    }
}

// MARK: - SQLServerCMSClient

/// Namespace client for SQL Server Central Management Servers.
///
/// Provides access to registered server groups and servers stored in msdb.
///
/// Usage:
/// ```swift
/// let groups = try await client.cms.listGroups()
/// let servers = try await client.cms.listServers()
/// ```
public final class SQLServerCMSClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - List Groups

    /// Lists all CMS server groups.
    @available(macOS 12.0, *)
    public func listGroups() async throws -> [SQLServerCMSGroup] {
        let sql = """
        SELECT server_group_id,
               name,
               ISNULL(description, '') AS description,
               ISNULL(parent_id, 0) AS parent_id
        FROM msdb.dbo.sysmanagement_shared_server_groups
        ORDER BY parent_id, name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerCMSGroup(
                groupId: row.column("server_group_id")?.int ?? 0,
                name: name,
                description: row.column("description")?.string ?? "",
                parentId: row.column("parent_id")?.int ?? 0
            )
        }
    }

    // MARK: - List Servers

    /// Lists all registered CMS servers.
    @available(macOS 12.0, *)
    public func listServers() async throws -> [SQLServerCMSServer] {
        let sql = """
        SELECT s.server_id,
               s.name,
               s.server_name,
               ISNULL(s.description, '') AS description,
               s.server_group_id
        FROM msdb.dbo.sysmanagement_shared_registered_servers s
        ORDER BY s.server_group_id, s.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerCMSServer(
                serverId: row.column("server_id")?.int ?? 0,
                name: name,
                serverName: row.column("server_name")?.string ?? "",
                description: row.column("description")?.string ?? "",
                groupId: row.column("server_group_id")?.int ?? 0
            )
        }
    }

    // MARK: - Add Group

    /// Adds a new CMS server group.
    @available(macOS 12.0, *)
    public func addGroup(name: String, parentId: Int = 0, description: String = "") async throws {
        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        let escapedDesc = description.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC msdb.dbo.sp_sysmanagement_add_shared_server_group
            @name = N'\(escapedName)',
            @description = N'\(escapedDesc)',
            @parent_id = \(parentId)
        """
        _ = try await client.execute(sql)
    }

    // MARK: - Add Server

    /// Registers a server in CMS.
    @available(macOS 12.0, *)
    public func addServer(serverName: String, groupId: Int, description: String = "") async throws {
        let escapedServer = serverName.replacingOccurrences(of: "'", with: "''")
        let escapedDesc = description.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC msdb.dbo.sp_sysmanagement_add_shared_registered_server
            @name = N'\(escapedServer)',
            @server_name = N'\(escapedServer)',
            @description = N'\(escapedDesc)',
            @server_group_id = \(groupId),
            @overwrite = 0
        """
        _ = try await client.execute(sql)
    }

    // MARK: - Remove Server

    /// Unregisters a server from CMS.
    @available(macOS 12.0, *)
    public func removeServer(serverId: Int) async throws {
        let sql = """
        EXEC msdb.dbo.sp_sysmanagement_delete_shared_registered_server
            @server_id = \(serverId)
        """
        _ = try await client.execute(sql)
    }
}
