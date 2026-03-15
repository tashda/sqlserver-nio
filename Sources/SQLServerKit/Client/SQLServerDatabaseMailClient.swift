import Foundation
import NIO

// MARK: - Database Mail Types

/// A Database Mail profile configured on the server.
public struct SQLServerMailProfile: Sendable, Equatable, Identifiable {
    public var id: String { name }

    public let profileID: Int
    public let name: String
    public let description: String?

    public init(profileID: Int, name: String, description: String?) {
        self.profileID = profileID
        self.name = name
        self.description = description
    }
}

/// A Database Mail account configured on the server.
public struct SQLServerMailAccount: Sendable, Equatable, Identifiable {
    public var id: String { name }

    public let accountID: Int
    public let name: String
    public let description: String?
    public let emailAddress: String?
    public let displayName: String?
    public let replyToAddress: String?
    public let serverName: String?
    public let serverPort: Int?

    public init(
        accountID: Int,
        name: String,
        description: String?,
        emailAddress: String?,
        displayName: String?,
        replyToAddress: String?,
        serverName: String?,
        serverPort: Int?
    ) {
        self.accountID = accountID
        self.name = name
        self.description = description
        self.emailAddress = emailAddress
        self.displayName = displayName
        self.replyToAddress = replyToAddress
        self.serverName = serverName
        self.serverPort = serverPort
    }
}

/// Status of the Database Mail system.
public struct SQLServerMailStatus: Sendable, Equatable {
    public let isStarted: Bool
    public let statusDescription: String

    public init(isStarted: Bool, statusDescription: String) {
        self.isStarted = isStarted
        self.statusDescription = statusDescription
    }
}

/// An item from the Database Mail queue.
public struct SQLServerMailQueueItem: Sendable, Equatable, Identifiable {
    public let id: Int
    public let profileID: Int?
    public let recipients: String?
    public let subject: String?
    public let sendRequestDate: Date?
    public let sentDate: Date?
    public let sentStatus: String?

    public init(
        id: Int,
        profileID: Int?,
        recipients: String?,
        subject: String?,
        sendRequestDate: Date?,
        sentDate: Date?,
        sentStatus: String?
    ) {
        self.id = id
        self.profileID = profileID
        self.recipients = recipients
        self.subject = subject
        self.sendRequestDate = sendRequestDate
        self.sentDate = sentDate
        self.sentStatus = sentStatus
    }
}

// MARK: - SQLServerDatabaseMailClient

/// Namespace client for SQL Server Database Mail operations.
///
/// Database Mail lets you send emails from the database engine. This client
/// provides read-only typed APIs for listing profiles, accounts, checking
/// status, and viewing the mail queue.
///
/// Usage:
/// ```swift
/// let profiles = try await client.databaseMail.listProfiles()
/// let status = try await client.databaseMail.status()
/// let queue = try await client.databaseMail.mailQueue(limit: 50)
/// ```
public final class SQLServerDatabaseMailClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - List Profiles

    /// Returns all Database Mail profiles configured on the server.
    @available(macOS 12.0, *)
    public func listProfiles() async throws -> [SQLServerMailProfile] {
        let sql = """
        SELECT profile_id, name, description
        FROM msdb.dbo.sysmail_profile
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerMailProfile(
                profileID: row.column("profile_id")?.int ?? 0,
                name: name,
                description: row.column("description")?.string
            )
        }
    }

    // MARK: - List Accounts

    /// Returns all Database Mail accounts configured on the server.
    @available(macOS 12.0, *)
    public func listAccounts() async throws -> [SQLServerMailAccount] {
        let sql = """
        SELECT
            a.account_id, a.name, a.description,
            a.email_address, a.display_name, a.replyto_address,
            s.servername, s.port
        FROM msdb.dbo.sysmail_account a
        LEFT JOIN msdb.dbo.sysmail_server s ON a.account_id = s.account_id
        ORDER BY a.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerMailAccount(
                accountID: row.column("account_id")?.int ?? 0,
                name: name,
                description: row.column("description")?.string,
                emailAddress: row.column("email_address")?.string,
                displayName: row.column("display_name")?.string,
                replyToAddress: row.column("replyto_address")?.string,
                serverName: row.column("servername")?.string,
                serverPort: row.column("port")?.int
            )
        }
    }

    // MARK: - Status

    /// Returns the current status of Database Mail.
    @available(macOS 12.0, *)
    public func status() async throws -> SQLServerMailStatus {
        let sql = "EXEC msdb.dbo.sysmail_help_status_sp"
        let rows = try await client.query(sql)
        guard let row = rows.first else {
            return SQLServerMailStatus(isStarted: false, statusDescription: "Unknown")
        }
        let statusStr = row.column("Status")?.string ?? row.column("status")?.string ?? "STOPPED"
        return SQLServerMailStatus(
            isStarted: statusStr.uppercased() == "STARTED",
            statusDescription: statusStr
        )
    }

    // MARK: - Mail Queue

    /// Returns recent items from the Database Mail queue.
    @available(macOS 12.0, *)
    public func mailQueue(limit: Int = 100) async throws -> [SQLServerMailQueueItem] {
        let sql = """
        SELECT TOP (\(max(1, min(limit, 1000))))
            mailitem_id, profile_id, recipients, subject,
            send_request_date, sent_date, sent_status
        FROM msdb.dbo.sysmail_allitems
        ORDER BY send_request_date DESC
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let mailID = row.column("mailitem_id")?.int else { return nil }
            return SQLServerMailQueueItem(
                id: mailID,
                profileID: row.column("profile_id")?.int,
                recipients: row.column("recipients")?.string,
                subject: row.column("subject")?.string,
                sendRequestDate: row.column("send_request_date")?.date,
                sentDate: row.column("sent_date")?.date,
                sentStatus: row.column("sent_status")?.string
            )
        }
    }
}
