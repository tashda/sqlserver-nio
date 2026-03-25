import Foundation
import NIO

// MARK: - Replication Types

/// The type of a SQL Server publication.
public enum SQLServerPublicationType: Int, Sendable, Equatable {
    case transactional = 0
    case snapshot = 1
    case merge = 2

    public var displayName: String {
        switch self {
        case .transactional: "Transactional"
        case .snapshot: "Snapshot"
        case .merge: "Merge"
        }
    }
}

/// A SQL Server replication publication.
public struct SQLServerPublication: Sendable, Equatable, Identifiable {
    public var id: String { name }

    public let name: String
    public let publicationType: SQLServerPublicationType
    public let status: Int
    public let description: String

    public init(
        name: String,
        publicationType: SQLServerPublicationType,
        status: Int,
        description: String
    ) {
        self.name = name
        self.publicationType = publicationType
        self.status = status
        self.description = description
    }

    public var isActive: Bool { status == 1 }
}

/// A SQL Server replication subscription.
public struct SQLServerSubscription: Sendable, Equatable, Identifiable {
    public var id: String { "\(subscriberServer).\(subscriberDB)" }

    public let subscriberServer: String
    public let subscriberDB: String
    public let subscriptionType: Int
    public let status: Int

    public init(
        subscriberServer: String,
        subscriberDB: String,
        subscriptionType: Int,
        status: Int
    ) {
        self.subscriberServer = subscriberServer
        self.subscriberDB = subscriberDB
        self.subscriptionType = subscriptionType
        self.status = status
    }

    /// 0 = Push, 1 = Pull
    public var isPush: Bool { subscriptionType == 0 }

    public var statusDisplayName: String {
        switch status {
        case 0: "Inactive"
        case 1: "Subscribed"
        case 2: "Active"
        default: "Unknown (\(status))"
        }
    }
}

/// An article within a SQL Server replication publication.
public struct SQLServerReplicationArticle: Sendable, Equatable, Identifiable {
    public var id: String { name }

    public let name: String
    public let sourceObject: String
    public let type: Int
    public let status: Int

    public init(name: String, sourceObject: String, type: Int, status: Int) {
        self.name = name
        self.sourceObject = sourceObject
        self.type = type
        self.status = status
    }
}

// MARK: - SQLServerReplicationClient

/// Namespace client for SQL Server Replication monitoring.
///
/// Provides read-only access to replication publications, subscriptions,
/// and articles configured on the server.
///
/// Usage:
/// ```swift
/// let isConfigured = try await client.replication.isDistributorConfigured()
/// let pubs = try await client.replication.listPublications()
/// ```
public final class SQLServerReplicationClient: @unchecked Sendable {
    internal let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Distributor Status

    /// Checks whether distribution is configured on this server.
    @available(macOS 12.0, *)
    public func isDistributorConfigured() async throws -> Bool {
        let sql = """
        EXEC sp_get_distributor
        """
        let rows = try await client.query(sql)
        guard let row = rows.first else { return false }
        // sp_get_distributor returns 'installed' column: 0 or 1
        return (row.column("installed")?.int ?? 0) == 1
    }

    // MARK: - Publications

    /// Lists replication publications in the current database.
    @available(macOS 12.0, *)
    public func listPublications() async throws -> [SQLServerPublication] {
        // Use syspublications which is available in the publisher database
        let sql = """
        SELECT name,
               type AS publication_type,
               status,
               ISNULL(description, '') AS description
        FROM syspublications
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            let typeVal = row.column("publication_type")?.int ?? 0
            return SQLServerPublication(
                name: name,
                publicationType: SQLServerPublicationType(rawValue: typeVal) ?? .transactional,
                status: row.column("status")?.int ?? 0,
                description: row.column("description")?.string ?? ""
            )
        }
    }

    // MARK: - Subscriptions

    /// Lists replication subscriptions in the current database.
    @available(macOS 12.0, *)
    public func listSubscriptions() async throws -> [SQLServerSubscription] {
        let sql = """
        SELECT srvname AS subscriber_server,
               dest_db AS subscriber_db,
               subscription_type,
               status
        FROM syssubscriptions sub
        JOIN sysextendedarticlesview a ON sub.artid = a.artid
        JOIN master.dbo.sysservers srv ON sub.srvid = srv.srvid
        ORDER BY srvname, dest_db
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let server = row.column("subscriber_server")?.string,
                  let db = row.column("subscriber_db")?.string else { return nil }
            return SQLServerSubscription(
                subscriberServer: server,
                subscriberDB: db,
                subscriptionType: row.column("subscription_type")?.int ?? 0,
                status: row.column("status")?.int ?? 0
            )
        }
    }

    // MARK: - Articles

    // MARK: - Agent Status

    /// Replication agent status entry.
    public struct SQLServerReplicationAgentStatus: Sendable, Equatable, Identifiable {
        public var id: String { "\(agentType)_\(name)" }
        public let agentType: String
        public let name: String
        public let status: String
        public let lastAction: String?
        public let lastRunTime: String?
        public let publicationName: String?
    }

    /// Returns status of replication agents (distribution, log reader, snapshot).
    @available(macOS 12.0, *)
    public func agentStatus() async throws -> [SQLServerReplicationAgentStatus] {
        // Query the distribution database agent tables if distributor is configured
        let sql = """
        SELECT
            'Distribution' AS agent_type,
            a.name,
            CASE a.status
                WHEN 1 THEN 'Started'
                WHEN 2 THEN 'Succeeded'
                WHEN 3 THEN 'In progress'
                WHEN 4 THEN 'Idle'
                WHEN 5 THEN 'Retrying'
                WHEN 6 THEN 'Failed'
                ELSE 'Unknown'
            END AS status,
            h.comments AS last_action,
            CONVERT(VARCHAR(30), h.time, 121) AS last_run_time,
            p.publication AS publication_name
        FROM msdb.dbo.MSdistribution_agents a
        LEFT JOIN msdb.dbo.syspublications p ON a.publication = p.name
        OUTER APPLY (
            SELECT TOP 1 comments, time
            FROM msdb.dbo.MSdistribution_history dh
            WHERE dh.agent_id = a.id
            ORDER BY dh.time DESC
        ) h
        """
        let rows: [SQLServerRow]
        do {
            rows = try await client.query(sql)
        } catch {
            // Distribution database may not be configured
            return []
        }
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerReplicationAgentStatus(
                agentType: row.column("agent_type")?.string ?? "Distribution",
                name: name,
                status: row.column("status")?.string ?? "Unknown",
                lastAction: row.column("last_action")?.string,
                lastRunTime: row.column("last_run_time")?.string,
                publicationName: row.column("publication_name")?.string
            )
        }
    }

    // MARK: - Publication Management

    /// Creates a new replication publication using `sp_addpublication`.
    ///
    /// - Parameters:
    ///   - name: The publication name.
    ///   - type: The publication type (transactional, snapshot, or merge).
    ///   - database: The database to create the publication in. If `nil`, uses the current database.
    @available(macOS 12.0, *)
    public func createPublication(
        name: String,
        type: SQLServerPublicationType,
        database: String? = nil
    ) async throws {
        let escaped = name.replacingOccurrences(of: "'", with: "''")
        let typeValue: String = switch type {
        case .transactional: "transactional"
        case .snapshot: "snapshot"
        case .merge: "merge"
        }
        var sql = ""
        if let db = database {
            let escapedDB = db.replacingOccurrences(of: "'", with: "''")
            sql += "USE [\(escapedDB)];\n"
        }
        sql += "EXEC sp_addpublication @publication = N'\(escaped)', @type = N'\(typeValue)'"
        _ = try await client.query(sql)
    }

    /// Drops a replication publication using `sp_droppublication`.
    ///
    /// - Parameter name: The publication name to drop.
    @available(macOS 12.0, *)
    public func dropPublication(name: String) async throws {
        let escaped = name.replacingOccurrences(of: "'", with: "''")
        let sql = "EXEC sp_droppublication @publication = N'\(escaped)'"
        _ = try await client.query(sql)
    }

    // MARK: - Article Management

    /// Adds an article to a publication using `sp_addarticle`.
    ///
    /// - Parameters:
    ///   - publicationName: The publication to add the article to.
    ///   - table: The source table name.
    ///   - schema: The source table schema (defaults to `dbo`).
    @available(macOS 12.0, *)
    public func addArticle(
        publicationName: String,
        table: String,
        schema: String = "dbo"
    ) async throws {
        let escapedPub = publicationName.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_addarticle
            @publication = N'\(escapedPub)',
            @article = N'\(escapedTable)',
            @source_table = N'\(escapedTable)',
            @source_owner = N'\(escapedSchema)',
            @type = N'logbased'
        """
        _ = try await client.query(sql)
    }

    /// Removes an article from a publication using `sp_droparticle`.
    ///
    /// - Parameters:
    ///   - publicationName: The publication to remove the article from.
    ///   - table: The article/table name to remove.
    ///   - schema: The source table schema (defaults to `dbo`).
    @available(macOS 12.0, *)
    public func removeArticle(
        publicationName: String,
        table: String,
        schema: String = "dbo"
    ) async throws {
        let escapedPub = publicationName.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_droparticle
            @publication = N'\(escapedPub)',
            @article = N'\(escapedTable)'
        """
        _ = try await client.query(sql)
    }

    // MARK: - Subscription Management

    /// Creates a push subscription to a publication using `sp_addsubscription`.
    ///
    /// - Parameters:
    ///   - publicationName: The publication to subscribe to.
    ///   - subscriberServer: The subscriber server name.
    ///   - subscriberDB: The subscriber database name.
    @available(macOS 12.0, *)
    public func createSubscription(
        publicationName: String,
        subscriberServer: String,
        subscriberDB: String
    ) async throws {
        let escapedPub = publicationName.replacingOccurrences(of: "'", with: "''")
        let escapedServer = subscriberServer.replacingOccurrences(of: "'", with: "''")
        let escapedDB = subscriberDB.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_addsubscription
            @publication = N'\(escapedPub)',
            @subscriber = N'\(escapedServer)',
            @destination_db = N'\(escapedDB)'
        """
        _ = try await client.query(sql)
    }

    /// Drops a subscription from a publication using `sp_dropsubscription`.
    ///
    /// - Parameters:
    ///   - publicationName: The publication name.
    ///   - subscriberServer: The subscriber server name.
    ///   - subscriberDB: The subscriber database name.
    @available(macOS 12.0, *)
    public func dropSubscription(
        publicationName: String,
        subscriberServer: String,
        subscriberDB: String
    ) async throws {
        let escapedPub = publicationName.replacingOccurrences(of: "'", with: "''")
        let escapedServer = subscriberServer.replacingOccurrences(of: "'", with: "''")
        let escapedDB = subscriberDB.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_dropsubscription
            @publication = N'\(escapedPub)',
            @subscriber = N'\(escapedServer)',
            @destination_db = N'\(escapedDB)'
        """
        _ = try await client.query(sql)
    }

    // MARK: - Articles Query

    /// Lists articles in a specific publication.
    @available(macOS 12.0, *)
    public func listArticles(publicationName: String) async throws -> [SQLServerReplicationArticle] {
        let escaped = publicationName.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT a.name,
               a.objid,
               OBJECT_NAME(a.objid) AS source_object,
               a.type,
               a.status
        FROM sysarticles a
        JOIN syspublications p ON a.pubid = p.pubid
        WHERE p.name = N'\(escaped)'
        ORDER BY a.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerReplicationArticle(
                name: name,
                sourceObject: row.column("source_object")?.string ?? "",
                type: row.column("type")?.int ?? 0,
                status: row.column("status")?.int ?? 0
            )
        }
    }
}
