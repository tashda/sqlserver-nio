import Foundation
import NIO

// MARK: - Availability Groups Types

/// An Always On Availability Group on the server.
public struct SQLServerAvailabilityGroup: Sendable, Equatable, Identifiable {
    public let groupId: String
    public let name: String
    public let automatedBackupPreference: String
    public let failureConditionLevel: Int

    public var id: String { groupId }

    public init(
        groupId: String,
        name: String,
        automatedBackupPreference: String,
        failureConditionLevel: Int
    ) {
        self.groupId = groupId
        self.name = name
        self.automatedBackupPreference = automatedBackupPreference
        self.failureConditionLevel = failureConditionLevel
    }
}

/// A replica within an Availability Group.
public struct SQLServerAGReplica: Sendable, Equatable, Identifiable {
    public let replicaServerName: String
    public let availabilityMode: String
    public let failoverMode: String
    public let role: String
    public let operationalState: String
    public let connectionState: String
    public let synchronizationHealth: String
    public let primaryAllowConnections: String
    public let secondaryAllowConnections: String

    public var id: String { replicaServerName }

    public init(
        replicaServerName: String,
        availabilityMode: String,
        failoverMode: String,
        role: String,
        operationalState: String,
        connectionState: String,
        synchronizationHealth: String,
        primaryAllowConnections: String,
        secondaryAllowConnections: String
    ) {
        self.replicaServerName = replicaServerName
        self.availabilityMode = availabilityMode
        self.failoverMode = failoverMode
        self.role = role
        self.operationalState = operationalState
        self.connectionState = connectionState
        self.synchronizationHealth = synchronizationHealth
        self.primaryAllowConnections = primaryAllowConnections
        self.secondaryAllowConnections = secondaryAllowConnections
    }

    /// Whether this replica is currently the primary.
    public var isPrimary: Bool {
        role.uppercased() == "PRIMARY"
    }

    /// Whether this replica is healthy.
    public var isHealthy: Bool {
        synchronizationHealth.uppercased() == "HEALTHY"
    }
}

/// A database participating in an Availability Group.
public struct SQLServerAGDatabase: Sendable, Equatable, Identifiable {
    public let databaseName: String
    public let synchronizationState: String
    public let synchronizationHealth: String
    public let databaseState: String
    public let isSuspended: Bool
    public let suspendReason: String?
    public let logSendQueueSize: Int64
    public let redoQueueSize: Int64

    public var id: String { databaseName }

    public init(
        databaseName: String,
        synchronizationState: String,
        synchronizationHealth: String,
        databaseState: String,
        isSuspended: Bool,
        suspendReason: String?,
        logSendQueueSize: Int64,
        redoQueueSize: Int64
    ) {
        self.databaseName = databaseName
        self.synchronizationState = synchronizationState
        self.synchronizationHealth = synchronizationHealth
        self.databaseState = databaseState
        self.isSuspended = isSuspended
        self.suspendReason = suspendReason
        self.logSendQueueSize = logSendQueueSize
        self.redoQueueSize = redoQueueSize
    }

    /// Whether this database is in a healthy synchronization state.
    public var isHealthy: Bool {
        synchronizationHealth.uppercased() == "HEALTHY"
    }
}

// MARK: - SQLServerAvailabilityGroupsClient

/// Namespace client for SQL Server Always On Availability Groups operations.
///
/// Provides typed APIs for querying HADR status, listing availability groups,
/// replicas, databases, and performing manual failover.
///
/// Usage:
/// ```swift
/// let isEnabled = try await client.availabilityGroups.isHadrEnabled()
/// let groups = try await client.availabilityGroups.listGroups()
/// ```
public final class SQLServerAvailabilityGroupsClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - HADR Status

    /// Checks whether Always On HADR is enabled on the server.
    @available(macOS 12.0, *)
    public func isHadrEnabled() async throws -> Bool {
        let sql = "SELECT CAST(SERVERPROPERTY('IsHadrEnabled') AS INT) AS IsHadrEnabled"
        let rows = try await client.query(sql)
        guard let row = rows.first else { return false }
        return (row.column("IsHadrEnabled")?.int ?? 0) == 1
    }

    // MARK: - List Groups

    /// Returns all availability groups on the server.
    @available(macOS 12.0, *)
    public func listGroups() async throws -> [SQLServerAvailabilityGroup] {
        let sql = """
        SELECT
            CAST(ag.group_id AS NVARCHAR(36)) AS group_id,
            ag.name,
            ag.automated_backup_preference_desc,
            ag.failure_condition_level
        FROM sys.availability_groups ag
        ORDER BY ag.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let groupId = row.column("group_id")?.string,
                  let name = row.column("name")?.string else { return nil }
            return SQLServerAvailabilityGroup(
                groupId: groupId,
                name: name,
                automatedBackupPreference: row.column("automated_backup_preference_desc")?.string ?? "NONE",
                failureConditionLevel: row.column("failure_condition_level")?.int ?? 0
            )
        }
    }

    // MARK: - List Replicas

    /// Returns replicas for a given availability group.
    @available(macOS 12.0, *)
    public func listReplicas(groupId: String) async throws -> [SQLServerAGReplica] {
        let sql = """
        SELECT
            ar.replica_server_name,
            ar.availability_mode_desc,
            ar.failover_mode_desc,
            ar.primary_role_allow_connections_desc,
            ar.secondary_role_allow_connections_desc,
            ISNULL(ars.role_desc, 'UNKNOWN') AS role_desc,
            ISNULL(ars.operational_state_desc, 'UNKNOWN') AS operational_state_desc,
            ISNULL(ars.connected_state_desc, 'UNKNOWN') AS connected_state_desc,
            ISNULL(ars.synchronization_health_desc, 'UNKNOWN') AS synchronization_health_desc
        FROM sys.availability_replicas ar
        LEFT JOIN sys.dm_hadr_availability_replica_states ars
            ON ar.replica_id = ars.replica_id
        WHERE CAST(ar.group_id AS NVARCHAR(36)) = '\(groupId)'
        ORDER BY ars.role_desc DESC, ar.replica_server_name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let serverName = row.column("replica_server_name")?.string else { return nil }
            return SQLServerAGReplica(
                replicaServerName: serverName,
                availabilityMode: row.column("availability_mode_desc")?.string ?? "UNKNOWN",
                failoverMode: row.column("failover_mode_desc")?.string ?? "UNKNOWN",
                role: row.column("role_desc")?.string ?? "UNKNOWN",
                operationalState: row.column("operational_state_desc")?.string ?? "UNKNOWN",
                connectionState: row.column("connected_state_desc")?.string ?? "UNKNOWN",
                synchronizationHealth: row.column("synchronization_health_desc")?.string ?? "UNKNOWN",
                primaryAllowConnections: row.column("primary_role_allow_connections_desc")?.string ?? "UNKNOWN",
                secondaryAllowConnections: row.column("secondary_role_allow_connections_desc")?.string ?? "UNKNOWN"
            )
        }
    }

    // MARK: - List Databases

    /// Returns databases participating in a given availability group.
    @available(macOS 12.0, *)
    public func listDatabases(groupId: String) async throws -> [SQLServerAGDatabase] {
        let sql = """
        SELECT
            adc.database_name,
            ISNULL(drs.synchronization_state_desc, 'UNKNOWN') AS synchronization_state_desc,
            ISNULL(drs.synchronization_health_desc, 'UNKNOWN') AS synchronization_health_desc,
            ISNULL(drs.database_state_desc, 'UNKNOWN') AS database_state_desc,
            ISNULL(drs.is_suspended, 0) AS is_suspended,
            drs.suspend_reason_desc,
            ISNULL(drs.log_send_queue_size, 0) AS log_send_queue_size,
            ISNULL(drs.redo_queue_size, 0) AS redo_queue_size
        FROM sys.availability_databases_cluster adc
        LEFT JOIN sys.dm_hadr_database_replica_states drs
            ON adc.group_database_id = drs.group_database_id
               AND drs.is_local = 1
        WHERE CAST(adc.group_id AS NVARCHAR(36)) = '\(groupId)'
        ORDER BY adc.database_name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let dbName = row.column("database_name")?.string else { return nil }
            return SQLServerAGDatabase(
                databaseName: dbName,
                synchronizationState: row.column("synchronization_state_desc")?.string ?? "UNKNOWN",
                synchronizationHealth: row.column("synchronization_health_desc")?.string ?? "UNKNOWN",
                databaseState: row.column("database_state_desc")?.string ?? "UNKNOWN",
                isSuspended: row.column("is_suspended")?.bool ?? false,
                suspendReason: row.column("suspend_reason_desc")?.string,
                logSendQueueSize: Int64(row.column("log_send_queue_size")?.int ?? 0),
                redoQueueSize: Int64(row.column("redo_queue_size")?.int ?? 0)
            )
        }
    }

    // MARK: - Failover

    /// Performs a manual failover of the specified availability group.
    /// This must be executed on the target secondary replica that will become the new primary.
    @available(macOS 12.0, *)
    public func failover(groupName: String) async throws {
        let sql = "ALTER AVAILABILITY GROUP [\(groupName)] FAILOVER"
        _ = try await client.execute(sql)
    }

    // MARK: - Database Management

    /// Adds a database to an availability group.
    @available(macOS 12.0, *)
    public func addDatabase(groupName: String, databaseName: String) async throws {
        let g = groupName.replacingOccurrences(of: "]", with: "]]")
        let d = databaseName.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER AVAILABILITY GROUP [\(g)] ADD DATABASE [\(d)];")
    }

    /// Removes a database from an availability group.
    @available(macOS 12.0, *)
    public func removeDatabase(groupName: String, databaseName: String) async throws {
        let g = groupName.replacingOccurrences(of: "]", with: "]]")
        let d = databaseName.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER AVAILABILITY GROUP [\(g)] REMOVE DATABASE [\(d)];")
    }

    // MARK: - Configuration

    /// Sets the automated backup preference for an availability group.
    /// Valid values: PRIMARY, SECONDARY_ONLY, SECONDARY, NONE
    @available(macOS 12.0, *)
    public func setBackupPreference(groupName: String, preference: String) async throws {
        let g = groupName.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER AVAILABILITY GROUP [\(g)] SET (AUTOMATED_BACKUP_PREFERENCE = \(preference));")
    }

    // MARK: - Listeners

    /// An AG listener endpoint.
    public struct SQLServerAGListener: Sendable, Equatable, Identifiable {
        public var id: String { "\(groupId)_\(dnsName)" }
        public let groupId: String
        public let dnsName: String
        public let port: Int
        public let ipAddresses: [String]
        public let state: String
    }

    /// Creates a listener for an availability group.
    ///
    /// - Parameters:
    ///   - groupName: The availability group name.
    ///   - dnsName: The DNS name for the listener.
    ///   - port: The TCP port (typically 1433).
    ///   - ipAddresses: Static IP addresses with subnet masks. Each tuple is (ip, subnetMask).
    ///                   Pass empty to use DHCP.
    @available(macOS 12.0, *)
    public func createListener(
        groupName: String,
        dnsName: String,
        port: Int,
        ipAddresses: [(ip: String, subnetMask: String)] = []
    ) async throws {
        let g = groupName.replacingOccurrences(of: "]", with: "]]")
        let dns = dnsName.replacingOccurrences(of: "'", with: "''")
        var sql = "ALTER AVAILABILITY GROUP [\(g)] ADD LISTENER N'\(dns)' (WITH DHCP"
        if !ipAddresses.isEmpty {
            let ipClauses = ipAddresses.map { ip, mask in
                let escapedIP = ip.replacingOccurrences(of: "'", with: "''")
                let escapedMask = mask.replacingOccurrences(of: "'", with: "''")
                return "(N'\(escapedIP)', N'\(escapedMask)')"
            }
            sql = "ALTER AVAILABILITY GROUP [\(g)] ADD LISTENER N'\(dns)' (WITH IP (\(ipClauses.joined(separator: ", "))), PORT = \(port));"
        } else {
            sql = "ALTER AVAILABILITY GROUP [\(g)] ADD LISTENER N'\(dns)' (WITH DHCP, PORT = \(port));"
        }
        _ = try await client.execute(sql)
    }

    /// Drops a listener from an availability group.
    @available(macOS 12.0, *)
    public func dropListener(groupName: String, dnsName: String) async throws {
        let g = groupName.replacingOccurrences(of: "]", with: "]]")
        let dns = dnsName.replacingOccurrences(of: "'", with: "''")
        _ = try await client.execute("ALTER AVAILABILITY GROUP [\(g)] REMOVE LISTENER N'\(dns)';")
    }

    /// Sets read-only routing URLs for a replica within an availability group.
    ///
    /// - Parameters:
    ///   - groupName: The availability group name.
    ///   - replicaName: The server instance name of the replica.
    ///   - routingUrl: The read-only routing URL (e.g., `TCP://server:1433`).
    ///   - routingList: Ordered list of replica server names for read-only routing.
    @available(macOS 12.0, *)
    public func setReadOnlyRouting(
        groupName: String,
        replicaName: String,
        routingUrl: String? = nil,
        routingList: [String]? = nil
    ) async throws {
        let g = groupName.replacingOccurrences(of: "]", with: "]]")
        let r = replicaName.replacingOccurrences(of: "'", with: "''")

        if let url = routingUrl {
            let escapedUrl = url.replacingOccurrences(of: "'", with: "''")
            _ = try await client.execute(
                "ALTER AVAILABILITY GROUP [\(g)] MODIFY REPLICA ON N'\(r)' WITH (SECONDARY_ROLE (READ_ONLY_ROUTING_URL = N'\(escapedUrl)'));"
            )
        }

        if let list = routingList {
            let listItems = list.map { name in
                let escaped = name.replacingOccurrences(of: "'", with: "''")
                return "N'\(escaped)'"
            }
            _ = try await client.execute(
                "ALTER AVAILABILITY GROUP [\(g)] MODIFY REPLICA ON N'\(r)' WITH (PRIMARY_ROLE (READ_ONLY_ROUTING_LIST = (\(listItems.joined(separator: ", ")))));"
            )
        }
    }

    /// Lists listeners for an availability group.
    @available(macOS 12.0, *)
    public func listListeners(groupId: String) async throws -> [SQLServerAGListener] {
        let escaped = groupId.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            l.group_id,
            l.dns_name,
            l.port,
            l.state_desc,
            STRING_AGG(CAST(ip.ip_address AS NVARCHAR(48)), ', ') AS ip_addresses
        FROM sys.availability_group_listeners l
        LEFT JOIN sys.availability_group_listener_ip_addresses ip
            ON l.listener_id = ip.listener_id
        WHERE CAST(l.group_id AS NVARCHAR(36)) = N'\(escaped)'
        GROUP BY l.group_id, l.dns_name, l.port, l.state_desc
        ORDER BY l.dns_name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let dns = row.column("dns_name")?.string else { return nil }
            let ips = (row.column("ip_addresses")?.string ?? "")
                .components(separatedBy: ", ")
                .filter { !$0.isEmpty }
            return SQLServerAGListener(
                groupId: row.column("group_id")?.string ?? groupId,
                dnsName: dns,
                port: row.column("port")?.int ?? 0,
                ipAddresses: ips,
                state: row.column("state_desc")?.string ?? "UNKNOWN"
            )
        }
    }
}
