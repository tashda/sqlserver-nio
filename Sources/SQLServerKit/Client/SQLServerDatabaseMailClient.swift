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
    public let useDefaultCredentials: Bool
    public let enableSSL: Bool

    public init(
        accountID: Int,
        name: String,
        description: String?,
        emailAddress: String?,
        displayName: String?,
        replyToAddress: String?,
        serverName: String?,
        serverPort: Int?,
        useDefaultCredentials: Bool = false,
        enableSSL: Bool = false
    ) {
        self.accountID = accountID
        self.name = name
        self.description = description
        self.emailAddress = emailAddress
        self.displayName = displayName
        self.replyToAddress = replyToAddress
        self.serverName = serverName
        self.serverPort = serverPort
        self.useDefaultCredentials = useDefaultCredentials
        self.enableSSL = enableSSL
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

/// A profile-to-account association with failover sequence number.
public struct SQLServerMailProfileAccount: Sendable, Equatable, Identifiable {
    public var id: String { "\(profileID)-\(accountID)" }

    public let profileID: Int
    public let profileName: String
    public let accountID: Int
    public let accountName: String
    public let sequenceNumber: Int

    public init(
        profileID: Int,
        profileName: String,
        accountID: Int,
        accountName: String,
        sequenceNumber: Int
    ) {
        self.profileID = profileID
        self.profileName = profileName
        self.accountID = accountID
        self.accountName = accountName
        self.sequenceNumber = sequenceNumber
    }
}

/// A principal-to-profile security mapping.
public struct SQLServerMailPrincipalProfile: Sendable, Equatable, Identifiable {
    public var id: String { "\(principalID)-\(profileID)" }

    public let principalID: Int
    public let principalName: String?
    public let profileID: Int
    public let profileName: String
    public let isDefault: Bool

    public init(
        principalID: Int,
        principalName: String?,
        profileID: Int,
        profileName: String,
        isDefault: Bool
    ) {
        self.principalID = principalID
        self.principalName = principalName
        self.profileID = profileID
        self.profileName = profileName
        self.isDefault = isDefault
    }
}

/// A Database Mail system configuration parameter.
public struct SQLServerMailConfigParameter: Sendable, Equatable, Identifiable {
    public var id: String { name }

    public let name: String
    public let value: String
    public let description: String?

    public init(name: String, value: String, description: String?) {
        self.name = name
        self.value = value
        self.description = description
    }
}

/// Input configuration for creating or updating a Database Mail SMTP account.
public struct SQLServerMailAccountConfig: Sendable, Equatable {
    public var accountName: String
    public var emailAddress: String
    public var displayName: String?
    public var replyToAddress: String?
    public var description: String?
    public var serverName: String
    public var port: Int
    public var username: String?
    public var password: String?
    public var useDefaultCredentials: Bool
    public var enableSSL: Bool

    public init(
        accountName: String,
        emailAddress: String,
        displayName: String? = nil,
        replyToAddress: String? = nil,
        description: String? = nil,
        serverName: String,
        port: Int = 25,
        username: String? = nil,
        password: String? = nil,
        useDefaultCredentials: Bool = false,
        enableSSL: Bool = false
    ) {
        self.accountName = accountName
        self.emailAddress = emailAddress
        self.displayName = displayName
        self.replyToAddress = replyToAddress
        self.description = description
        self.serverName = serverName
        self.port = port
        self.username = username
        self.password = password
        self.useDefaultCredentials = useDefaultCredentials
        self.enableSSL = enableSSL
    }
}

// MARK: - SQLServerDatabaseMailClient

/// Namespace client for SQL Server Database Mail operations.
///
/// Database Mail lets you send emails from the database engine. This client
/// provides typed APIs for listing, creating, updating, and deleting
/// profiles, accounts, and configuration.
///
/// Usage:
/// ```swift
/// let profiles = try await client.databaseMail.listProfiles()
/// let status = try await client.databaseMail.status()
/// let id = try await client.databaseMail.createProfile(name: "Alerts", description: nil)
/// ```
public final class SQLServerDatabaseMailClient: @unchecked Sendable {
    let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Internal Helpers

    @available(macOS 12.0, *)
    func run(sql: String) async throws -> [SQLServerRow] {
        try await client.query(sql)
    }

    @available(macOS 12.0, *)
    @discardableResult
    func exec(sql: String) async throws -> SQLServerExecutionResult {
        try await client.execute(sql)
    }

    func escapeLiteral(_ literal: String) -> String {
        literal.replacingOccurrences(of: "'", with: "''")
    }

    // MARK: - Feature Status

    /// Returns whether the Database Mail XPs feature is enabled on the server.
    @available(macOS 12.0, *)
    public func isFeatureEnabled() async throws -> Bool {
        let sql = """
        SELECT CAST(value_in_use AS INT) AS enabled
        FROM sys.configurations
        WHERE name = 'Database Mail XPs'
        """
        let rows = try await run(sql: sql)
        guard let row = rows.first else { return false }
        return (row.column("enabled")?.int ?? 0) == 1
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
        let rows = try await run(sql: sql)
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
            s.servername, s.port,
            s.use_default_credentials, s.enable_ssl
        FROM msdb.dbo.sysmail_account a
        LEFT JOIN msdb.dbo.sysmail_server s ON a.account_id = s.account_id
        ORDER BY a.name
        """
        let rows = try await run(sql: sql)
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
                serverPort: row.column("port")?.int,
                useDefaultCredentials: (row.column("use_default_credentials")?.int ?? 0) == 1,
                enableSSL: (row.column("enable_ssl")?.int ?? 0) == 1
            )
        }
    }

    // MARK: - List Profile-Account Associations

    /// Returns profile-to-account mappings with failover sequence numbers.
    @available(macOS 12.0, *)
    public func listProfileAccounts(profileID: Int? = nil) async throws -> [SQLServerMailProfileAccount] {
        var sql = """
        SELECT
            pa.profile_id, p.name AS profile_name,
            pa.account_id, a.name AS account_name,
            pa.sequence_number
        FROM msdb.dbo.sysmail_profileaccount pa
        JOIN msdb.dbo.sysmail_profile p ON pa.profile_id = p.profile_id
        JOIN msdb.dbo.sysmail_account a ON pa.account_id = a.account_id
        """
        if let profileID {
            sql += " WHERE pa.profile_id = \(profileID)"
        }
        sql += " ORDER BY pa.profile_id, pa.sequence_number"
        let rows = try await run(sql: sql)
        return rows.compactMap { row in
            guard let profileName = row.column("profile_name")?.string,
                  let accountName = row.column("account_name")?.string else { return nil }
            return SQLServerMailProfileAccount(
                profileID: row.column("profile_id")?.int ?? 0,
                profileName: profileName,
                accountID: row.column("account_id")?.int ?? 0,
                accountName: accountName,
                sequenceNumber: row.column("sequence_number")?.int ?? 1
            )
        }
    }

    // MARK: - List Principal-Profile Security

    /// Returns principal-to-profile security mappings.
    @available(macOS 12.0, *)
    public func listPrincipalProfiles(profileID: Int? = nil) async throws -> [SQLServerMailPrincipalProfile] {
        var sql = """
        SELECT
            pp.principal_sid, dp.name AS principal_name,
            pp.profile_id, p.name AS profile_name,
            pp.is_default
        FROM msdb.dbo.sysmail_principalprofile pp
        JOIN msdb.dbo.sysmail_profile p ON pp.profile_id = p.profile_id
        LEFT JOIN msdb.sys.database_principals dp ON pp.principal_sid = dp.sid
        """
        if let profileID {
            sql += " WHERE pp.profile_id = \(profileID)"
        }
        sql += " ORDER BY p.name"
        let rows = try await run(sql: sql)
        return rows.compactMap { row in
            guard let profileName = row.column("profile_name")?.string else { return nil }
            return SQLServerMailPrincipalProfile(
                principalID: row.column("principal_sid")?.int ?? 0,
                principalName: row.column("principal_name")?.string,
                profileID: row.column("profile_id")?.int ?? 0,
                profileName: profileName,
                isDefault: (row.column("is_default")?.int ?? 0) == 1
            )
        }
    }

    // MARK: - System Configuration

    /// Returns all Database Mail system configuration parameters.
    @available(macOS 12.0, *)
    public func configuration() async throws -> [SQLServerMailConfigParameter] {
        let sql = "EXEC msdb.dbo.sysmail_help_configure_sp"
        let rows = try await run(sql: sql)
        return rows.compactMap { row in
            guard let name = row.column("paramname")?.string else { return nil }
            return SQLServerMailConfigParameter(
                name: name,
                value: row.column("paramvalue")?.string ?? "",
                description: row.column("description")?.string
            )
        }
    }

    // MARK: - Status

    /// Returns the current status of Database Mail.
    @available(macOS 12.0, *)
    public func status() async throws -> SQLServerMailStatus {
        let sql = "EXEC msdb.dbo.sysmail_help_status_sp"
        let rows = try await run(sql: sql)
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
        let rows = try await run(sql: sql)
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
