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
    private let client: SQLServerClient

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
