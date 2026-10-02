import Foundation

// MARK: - Availability group setup types

/// How an availability group is clustered. `none` (read-scale, no cluster manager) is what
/// containers and Linux without Pacemaker use.
public enum SQLServerAGClusterType: String, Sendable, CaseIterable {
    case none = "NONE"
    case external = "EXTERNAL"
    case wsfc = "WSFC"
}

public enum SQLServerAGAvailabilityMode: String, Sendable, CaseIterable {
    case synchronousCommit = "SYNCHRONOUS_COMMIT"
    case asynchronousCommit = "ASYNCHRONOUS_COMMIT"
    case configurationOnly = "CONFIGURATION_ONLY"
}

public enum SQLServerAGFailoverMode: String, Sendable, CaseIterable {
    case manual = "MANUAL"
    case automatic = "AUTOMATIC"
    case external = "EXTERNAL"
}

public enum SQLServerAGSeedingMode: String, Sendable, CaseIterable {
    case automatic = "AUTOMATIC"
    case manual = "MANUAL"
}

/// Which connections a secondary replica accepts.
public enum SQLServerAGSecondaryConnections: String, Sendable, CaseIterable {
    case no = "NO"
    case readIntentOnly = "READ_ONLY"
    case all = "ALL"
}

/// One replica of a new availability group.
public struct SQLServerAGReplicaSpec: Sendable, Equatable {
    /// The replica's `@@SERVERNAME`.
    public var serverName: String
    /// Its database mirroring endpoint, `TCP://host:5022`.
    public var endpointURL: String
    public var availabilityMode: SQLServerAGAvailabilityMode
    public var failoverMode: SQLServerAGFailoverMode
    public var seedingMode: SQLServerAGSeedingMode
    public var secondaryConnections: SQLServerAGSecondaryConnections

    public init(
        serverName: String,
        endpointURL: String,
        availabilityMode: SQLServerAGAvailabilityMode = .synchronousCommit,
        failoverMode: SQLServerAGFailoverMode = .manual,
        seedingMode: SQLServerAGSeedingMode = .automatic,
        secondaryConnections: SQLServerAGSecondaryConnections = .all
    ) {
        self.serverName = serverName
        self.endpointURL = endpointURL
        self.availabilityMode = availabilityMode
        self.failoverMode = failoverMode
        self.seedingMode = seedingMode
        self.secondaryConnections = secondaryConnections
    }
}

/// Options of a new availability group.
public struct SQLServerAGOptions: Sendable, Equatable {
    public var clusterType: SQLServerAGClusterType
    /// Fail over when a database (not only the instance) becomes unhealthy.
    public var databaseHealthTrigger: Bool
    /// Secondaries that must harden a commit before the primary commits (`REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT`).
    public var requiredSynchronizedSecondariesToCommit: Int?

    public init(clusterType: SQLServerAGClusterType = .none, databaseHealthTrigger: Bool = false,
                requiredSynchronizedSecondariesToCommit: Int? = nil) {
        self.clusterType = clusterType
        self.databaseHealthTrigger = databaseHealthTrigger
        self.requiredSynchronizedSecondariesToCommit = requiredSynchronizedSecondariesToCommit
    }
}

// MARK: - Setup

@available(macOS 12.0, *)
extension SQLServerAvailabilityGroupsClient {
    /// Creates the database mirroring endpoint availability groups talk through, authenticated by a
    /// certificate in master and encrypted with AES.
    public func createEndpoint(name: String = "hadr_endpoint", port: Int = 5022, certificate: String) async throws {
        _ = try await run(Self.createEndpointSQL(name: name, port: port, certificate: certificate))
    }

    /// Creates an availability group on the primary with its replicas (the primary among them).
    public func createGroup(name: String, replicas: [SQLServerAGReplicaSpec], databases: [String] = [],
                            options: SQLServerAGOptions = .init()) async throws {
        _ = try await run(Self.createGroupSQL(name: name, replicas: replicas, databases: databases, options: options))
    }

    /// Joins this secondary to an availability group created on the primary.
    public func join(groupName: String, clusterType: SQLServerAGClusterType = .none) async throws {
        _ = try await run("ALTER AVAILABILITY GROUP \(SQLServerSQL.escapeIdentifier(groupName)) JOIN WITH (CLUSTER_TYPE = \(clusterType.rawValue));")
    }

    /// Lets automatic seeding create the group's databases on this secondary.
    public func grantCreateAnyDatabase(groupName: String) async throws {
        _ = try await run("ALTER AVAILABILITY GROUP \(SQLServerSQL.escapeIdentifier(groupName)) GRANT CREATE ANY DATABASE;")
    }

    /// Makes this primary a secondary (`SET (ROLE = SECONDARY)`): the first step of a manual failover
    /// of a `CLUSTER_TYPE = NONE` or `EXTERNAL` group.
    public func demoteToSecondary(groupName: String) async throws {
        _ = try await run("ALTER AVAILABILITY GROUP \(SQLServerSQL.escapeIdentifier(groupName)) SET (ROLE = SECONDARY);")
    }

    /// Forces this secondary to become primary (`FORCE_FAILOVER_ALLOW_DATA_LOSS`): the failover of
    /// a `CLUSTER_TYPE = NONE` group, run on the target once the old primary is demoted or gone.
    public func forceFailover(groupName: String) async throws {
        _ = try await run("ALTER AVAILABILITY GROUP \(SQLServerSQL.escapeIdentifier(groupName)) FORCE_FAILOVER_ALLOW_DATA_LOSS;")
    }

    private func run(_ sql: String) async throws -> SQLServerExecutionResult {
        try await client.execute(sql)
    }

    internal static func createEndpointSQL(name: String, port: Int, certificate: String) -> String {
        """
        CREATE ENDPOINT \(SQLServerSQL.escapeIdentifier(name)) STATE = STARTED \
        AS TCP (LISTENER_PORT = \(port)) \
        FOR DATABASE_MIRRORING (ROLE = ALL, AUTHENTICATION = CERTIFICATE \(SQLServerSQL.escapeIdentifier(certificate)), \
        ENCRYPTION = REQUIRED ALGORITHM AES);
        """
    }

    internal static func createGroupSQL(name: String, replicas: [SQLServerAGReplicaSpec], databases: [String],
                                        options: SQLServerAGOptions) -> String {
        var with = ["CLUSTER_TYPE = \(options.clusterType.rawValue)", "DB_FAILOVER = \(options.databaseHealthTrigger ? "ON" : "OFF")"]
        if let required = options.requiredSynchronizedSecondariesToCommit {
            with.append("REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = \(required)")
        }
        let replicaClauses = replicas.map { replica in
            "N'\(SQLServerSQL.escapeLiteral(replica.serverName))' WITH (ENDPOINT_URL = N'\(SQLServerSQL.escapeLiteral(replica.endpointURL))', "
                + "AVAILABILITY_MODE = \(replica.availabilityMode.rawValue), FAILOVER_MODE = \(replica.failoverMode.rawValue), "
                + "SEEDING_MODE = \(replica.seedingMode.rawValue), "
                + "SECONDARY_ROLE (ALLOW_CONNECTIONS = \(replica.secondaryConnections.rawValue)))"
        }
        var sql = "CREATE AVAILABILITY GROUP \(SQLServerSQL.escapeIdentifier(name)) WITH (\(with.joined(separator: ", ")))"
        if !databases.isEmpty {
            sql += " FOR DATABASE " + databases.map(SQLServerSQL.escapeIdentifier).joined(separator: ", ")
        }
        return sql + " REPLICA ON " + replicaClauses.joined(separator: ", ") + ";"
    }
}
