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

/// An item from the Database Mail queue (maps to `msdb.dbo.sysmail_allitems`).
public struct SQLServerMailQueueItem: Sendable, Equatable, Identifiable {
    public let id: Int
    public let profileID: Int?
    public let recipients: String?
    public let copyRecipients: String?
    public let blindCopyRecipients: String?
    public let subject: String?
    public let body: String?
    public let bodyFormat: String?
    public let importance: String?
    public let sensitivity: String?
    public let fileAttachments: String?
    public let sendRequestDate: Date?
    public let sendRequestUser: String?
    public let sentAccountID: Int?
    public let sentDate: Date?
    public let sentStatus: String?
    public let lastModDate: Date?
    public let lastModUser: String?

    public init(
        id: Int,
        profileID: Int?,
        recipients: String?,
        copyRecipients: String? = nil,
        blindCopyRecipients: String? = nil,
        subject: String?,
        body: String? = nil,
        bodyFormat: String? = nil,
        importance: String? = nil,
        sensitivity: String? = nil,
        fileAttachments: String? = nil,
        sendRequestDate: Date?,
        sendRequestUser: String? = nil,
        sentAccountID: Int? = nil,
        sentDate: Date?,
        sentStatus: String?,
        lastModDate: Date? = nil,
        lastModUser: String? = nil
    ) {
        self.id = id
        self.profileID = profileID
        self.recipients = recipients
        self.copyRecipients = copyRecipients
        self.blindCopyRecipients = blindCopyRecipients
        self.subject = subject
        self.body = body
        self.bodyFormat = bodyFormat
        self.importance = importance
        self.sensitivity = sensitivity
        self.fileAttachments = fileAttachments
        self.sendRequestDate = sendRequestDate
        self.sendRequestUser = sendRequestUser
        self.sentAccountID = sentAccountID
        self.sentDate = sentDate
        self.sentStatus = sentStatus
        self.lastModDate = lastModDate
        self.lastModUser = lastModUser
    }
}

/// An entry from the Database Mail event log (`msdb.dbo.sysmail_event_log`).
public struct SQLServerMailEventLogEntry: Sendable, Equatable, Identifiable {
    public let id: Int
    public let eventType: String
    public let logDate: Date?
    public let description: String?
    public let processID: Int?
    public let mailItemID: Int?
    public let accountID: Int?
    public let lastModDate: Date?
    public let lastModUser: String?

    public init(
        id: Int,
        eventType: String,
        logDate: Date?,
        description: String?,
        processID: Int?,
        mailItemID: Int?,
        accountID: Int?,
        lastModDate: Date?,
        lastModUser: String?
    ) {
        self.id = id
        self.eventType = eventType
        self.logDate = logDate
        self.description = description
        self.processID = processID
        self.mailItemID = mailItemID
        self.accountID = accountID
        self.lastModDate = lastModDate
        self.lastModUser = lastModUser
    }
}

/// An attachment associated with a Database Mail item (`msdb.dbo.sysmail_mailattachments`).
public struct SQLServerMailAttachment: Sendable, Equatable, Identifiable {
    public let id: Int
    public let mailItemID: Int
    public let filename: String?
    public let filesize: Int?

    public init(
        id: Int,
        mailItemID: Int,
        filename: String?,
        filesize: Int?
    ) {
        self.id = id
        self.mailItemID = mailItemID
        self.filename = filename
        self.filesize = filesize
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
    ///
    /// Queries `msdb.dbo.sysmail_allitems` with all columns.
    /// Use the `status` parameter to filter by sent status (e.g. "failed", "sent", "unsent", "retrying").
    @available(macOS 12.0, *)
    public func mailQueue(limit: Int = 100, status: String? = nil) async throws -> [SQLServerMailQueueItem] {
        var sql = """
        SELECT TOP (\(max(1, min(limit, 1000))))
            mailitem_id, profile_id, recipients, copy_recipients, blind_copy_recipients,
            subject, body, body_format, importance, sensitivity, file_attachments,
            send_request_date, send_request_user, sent_account_id, sent_date, sent_status,
            last_mod_date, last_mod_user
        FROM msdb.dbo.sysmail_allitems
        """
        if let status {
            sql += " WHERE sent_status = '\(status)'"
        }
        sql += " ORDER BY send_request_date DESC"
        let rows = try await run(sql: sql)
        return rows.compactMap { row in
            guard let mailID = row.column("mailitem_id")?.int else { return nil }
            return SQLServerMailQueueItem(
                id: mailID,
                profileID: row.column("profile_id")?.int,
                recipients: row.column("recipients")?.string,
                copyRecipients: row.column("copy_recipients")?.string,
                blindCopyRecipients: row.column("blind_copy_recipients")?.string,
                subject: row.column("subject")?.string,
                body: row.column("body")?.string,
                bodyFormat: row.column("body_format")?.string,
                importance: row.column("importance")?.string,
                sensitivity: row.column("sensitivity")?.string,
                fileAttachments: row.column("file_attachments")?.string,
                sendRequestDate: row.column("send_request_date")?.date,
                sendRequestUser: row.column("send_request_user")?.string,
                sentAccountID: row.column("sent_account_id")?.int,
                sentDate: row.column("sent_date")?.date,
                sentStatus: row.column("sent_status")?.string,
                lastModDate: row.column("last_mod_date")?.date,
                lastModUser: row.column("last_mod_user")?.string
            )
        }
    }

    // MARK: - Event Log

    /// Returns recent entries from the Database Mail event log.
    ///
    /// Queries `msdb.dbo.sysmail_event_log`. Use `mailItemID` to filter entries for a specific mail item.
    @available(macOS 12.0, *)
    public func eventLog(limit: Int = 100, mailItemID: Int? = nil) async throws -> [SQLServerMailEventLogEntry] {
        var sql = """
        SELECT TOP (\(max(1, min(limit, 1000))))
            log_id, event_type, log_date, description,
            process_id, mailitem_id, account_id,
            last_mod_date, last_mod_user
        FROM msdb.dbo.sysmail_event_log
        """
        if let mailItemID {
            sql += " WHERE mailitem_id = \(mailItemID)"
        }
        sql += " ORDER BY log_date DESC"
        let rows = try await run(sql: sql)
        return rows.compactMap { row in
            guard let logID = row.column("log_id")?.int else { return nil }
            return SQLServerMailEventLogEntry(
                id: logID,
                eventType: row.column("event_type")?.string ?? "unknown",
                logDate: row.column("log_date")?.date,
                description: row.column("description")?.string,
                processID: row.column("process_id")?.int,
                mailItemID: row.column("mailitem_id")?.int,
                accountID: row.column("account_id")?.int,
                lastModDate: row.column("last_mod_date")?.date,
                lastModUser: row.column("last_mod_user")?.string
            )
        }
    }

    // MARK: - Attachments

    /// Returns attachments for a specific mail item.
    ///
    /// Queries `msdb.dbo.sysmail_mailattachments`. Does not include the binary content.
    @available(macOS 12.0, *)
    public func attachments(mailItemID: Int) async throws -> [SQLServerMailAttachment] {
        let sql = """
        SELECT attachment_id, mailitem_id, filename, filesize
        FROM msdb.dbo.sysmail_mailattachments
        WHERE mailitem_id = \(mailItemID)
        ORDER BY attachment_id
        """
        let rows = try await run(sql: sql)
        return rows.compactMap { row in
            guard let attachID = row.column("attachment_id")?.int else { return nil }
            return SQLServerMailAttachment(
                id: attachID,
                mailItemID: row.column("mailitem_id")?.int ?? mailItemID,
                filename: row.column("filename")?.string,
                filesize: row.column("filesize")?.int
            )
        }
    }

    // MARK: - Delete Mail Items

    /// Deletes mail items from the Database Mail queue.
    ///
    /// Calls `msdb.dbo.sysmail_delete_mailitems_sp`. Pass a `sentBefore` date to delete
    /// items older than a specific date, or `status` to delete only items with a given status.
    @available(macOS 12.0, *)
    public func deleteMailItems(sentBefore: Date? = nil, status: String? = nil) async throws {
        var params: [String] = []
        if let sentBefore {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withFullDate, .withFullTime]
            params.append("@sent_before = '\(formatter.string(from: sentBefore))'")
        }
        if let status {
            params.append("@sent_status = '\(status)'")
        }
        let paramStr = params.isEmpty ? "" : " " + params.joined(separator: ", ")
        try await exec(sql: "EXEC msdb.dbo.sysmail_delete_mailitems_sp\(paramStr)")
    }

    // MARK: - Delete Event Log

    /// Deletes entries from the Database Mail event log.
    ///
    /// Calls `msdb.dbo.sysmail_delete_log_sp`. Pass `loggedBefore` to delete entries older
    /// than a specific date, or `eventType` to delete only a specific type ("success", "warning", "error", "information").
    @available(macOS 12.0, *)
    public func deleteEventLog(loggedBefore: Date? = nil, eventType: String? = nil) async throws {
        var params: [String] = []
        if let loggedBefore {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withFullDate, .withFullTime]
            params.append("@logged_before = '\(formatter.string(from: loggedBefore))'")
        }
        if let eventType {
            params.append("@event_type = '\(eventType)'")
        }
        let paramStr = params.isEmpty ? "" : " " + params.joined(separator: ", ")
        try await exec(sql: "EXEC msdb.dbo.sysmail_delete_log_sp\(paramStr)")
    }
}
