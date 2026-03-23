import Foundation

// MARK: - Database Mail Write Operations

@available(macOS 12.0, *)
extension SQLServerDatabaseMailClient {

    // MARK: - Feature Management

    /// Enables the Database Mail XPs feature on the server.
    /// Requires sysadmin role.
    public func enableFeature() async throws {
        let sql = """
        EXEC sp_configure 'show advanced options', 1; RECONFIGURE;
        EXEC sp_configure 'Database Mail XPs', 1; RECONFIGURE;
        """
        try await exec(sql: sql)
    }

    /// Disables the Database Mail XPs feature on the server.
    /// Requires sysadmin role.
    public func disableFeature() async throws {
        let sql = """
        EXEC sp_configure 'Database Mail XPs', 0; RECONFIGURE;
        """
        try await exec(sql: sql)
    }

    /// Starts the Database Mail service (Service Broker queues + external program).
    public func start() async throws {
        try await exec(sql: "EXEC msdb.dbo.sysmail_start_sp")
    }

    /// Stops the Database Mail service.
    public func stop() async throws {
        try await exec(sql: "EXEC msdb.dbo.sysmail_stop_sp")
    }

    // MARK: - Profile CRUD

    /// Creates a new Database Mail profile and returns its ID.
    @discardableResult
    public func createProfile(name: String, description: String? = nil) async throws -> Int {
        var sql = """
        DECLARE @id INT;
        EXEC msdb.dbo.sysmail_add_profile_sp
            @profile_name = N'\(escapeLiteral(name))'
        """
        if let description {
            sql += ", @description = N'\(escapeLiteral(description))'"
        }
        sql += ", @profile_id = @id OUTPUT;"
        sql += " SELECT @id AS profile_id;"
        let rows = try await run(sql: sql)
        return rows.first?.column("profile_id")?.int ?? 0
    }

    /// Updates an existing Database Mail profile.
    public func updateProfile(profileID: Int, name: String, description: String? = nil) async throws {
        var sql = """
        EXEC msdb.dbo.sysmail_update_profile_sp
            @profile_id = \(profileID),
            @profile_name = N'\(escapeLiteral(name))'
        """
        if let description {
            sql += ", @description = N'\(escapeLiteral(description))'"
        }
        sql += ";"
        try await exec(sql: sql)
    }

    /// Deletes a Database Mail profile by ID.
    public func deleteProfile(profileID: Int) async throws {
        let sql = "EXEC msdb.dbo.sysmail_delete_profile_sp @profile_id = \(profileID);"
        try await exec(sql: sql)
    }

    // MARK: - Account CRUD

    /// Creates a new Database Mail SMTP account and returns its ID.
    @discardableResult
    public func createAccount(_ config: SQLServerMailAccountConfig) async throws -> Int {
        var sql = """
        DECLARE @id INT;
        EXEC msdb.dbo.sysmail_add_account_sp
            @account_name = N'\(escapeLiteral(config.accountName))',
            @email_address = N'\(escapeLiteral(config.emailAddress))',
            @mailserver_name = N'\(escapeLiteral(config.serverName))',
            @port = \(config.port),
            @use_default_credentials = \(config.useDefaultCredentials ? 1 : 0),
            @enable_ssl = \(config.enableSSL ? 1 : 0)
        """
        if let displayName = config.displayName {
            sql += ", @display_name = N'\(escapeLiteral(displayName))'"
        }
        if let replyTo = config.replyToAddress {
            sql += ", @replyto_address = N'\(escapeLiteral(replyTo))'"
        }
        if let desc = config.description {
            sql += ", @description = N'\(escapeLiteral(desc))'"
        }
        if let username = config.username {
            sql += ", @username = N'\(escapeLiteral(username))'"
        }
        if let password = config.password {
            sql += ", @password = N'\(escapeLiteral(password))'"
        }
        sql += ", @account_id = @id OUTPUT;"
        sql += " SELECT @id AS account_id;"
        let rows = try await run(sql: sql)
        return rows.first?.column("account_id")?.int ?? 0
    }

    /// Updates an existing Database Mail SMTP account.
    public func updateAccount(accountID: Int, _ config: SQLServerMailAccountConfig) async throws {
        var sql = """
        EXEC msdb.dbo.sysmail_update_account_sp
            @account_id = \(accountID),
            @account_name = N'\(escapeLiteral(config.accountName))',
            @email_address = N'\(escapeLiteral(config.emailAddress))',
            @mailserver_name = N'\(escapeLiteral(config.serverName))',
            @port = \(config.port),
            @use_default_credentials = \(config.useDefaultCredentials ? 1 : 0),
            @enable_ssl = \(config.enableSSL ? 1 : 0)
        """
        if let displayName = config.displayName {
            sql += ", @display_name = N'\(escapeLiteral(displayName))'"
        }
        if let replyTo = config.replyToAddress {
            sql += ", @replyto_address = N'\(escapeLiteral(replyTo))'"
        }
        if let desc = config.description {
            sql += ", @description = N'\(escapeLiteral(desc))'"
        }
        if let username = config.username {
            sql += ", @username = N'\(escapeLiteral(username))'"
        }
        if let password = config.password {
            sql += ", @password = N'\(escapeLiteral(password))'"
        }
        sql += ";"
        try await exec(sql: sql)
    }

    /// Deletes a Database Mail SMTP account by ID.
    public func deleteAccount(accountID: Int) async throws {
        let sql = "EXEC msdb.dbo.sysmail_delete_account_sp @account_id = \(accountID);"
        try await exec(sql: sql)
    }

    // MARK: - Profile-Account Association

    /// Links an account to a profile with a failover sequence number.
    public func addAccountToProfile(
        profileID: Int,
        accountID: Int,
        sequenceNumber: Int
    ) async throws {
        let sql = """
        EXEC msdb.dbo.sysmail_add_profileaccount_sp
            @profile_id = \(profileID),
            @account_id = \(accountID),
            @sequence_number = \(sequenceNumber);
        """
        try await exec(sql: sql)
    }

    /// Removes an account from a profile.
    public func removeAccountFromProfile(profileID: Int, accountID: Int) async throws {
        let sql = """
        EXEC msdb.dbo.sysmail_delete_profileaccount_sp
            @profile_id = \(profileID),
            @account_id = \(accountID);
        """
        try await exec(sql: sql)
    }

    // MARK: - Profile Security

    /// Grants a principal access to a Database Mail profile.
    /// Use `principalName: "public"` to make a profile public.
    public func grantProfileAccess(
        profileID: Int,
        principalName: String,
        isDefault: Bool = false
    ) async throws {
        let sql = """
        EXEC msdb.dbo.sysmail_add_principalprofile_sp
            @profile_id = \(profileID),
            @principal_name = N'\(escapeLiteral(principalName))',
            @is_default = \(isDefault ? 1 : 0);
        """
        try await exec(sql: sql)
    }

    /// Revokes a principal's access to a Database Mail profile.
    public func revokeProfileAccess(profileID: Int, principalName: String) async throws {
        let sql = """
        EXEC msdb.dbo.sysmail_delete_principalprofile_sp
            @profile_id = \(profileID),
            @principal_name = N'\(escapeLiteral(principalName))';
        """
        try await exec(sql: sql)
    }

    // MARK: - System Configuration

    /// Sets a Database Mail system configuration parameter.
    ///
    /// Common parameters:
    /// - `AccountRetryAttempts` (default: 1)
    /// - `AccountRetryDelay` (default: 5000 seconds)
    /// - `MaxFileSize` (default: 1000000 bytes)
    /// - `ProhibitedExtensions` (default: "exe,dll,vbs,js")
    /// - `LoggingLevel` (1=errors, 2=extended, 3=verbose)
    public func setConfiguration(parameter: String, value: String) async throws {
        let sql = """
        EXEC msdb.dbo.sysmail_configure_sp
            @parameter_name = N'\(escapeLiteral(parameter))',
            @parameter_value = N'\(escapeLiteral(value))';
        """
        try await exec(sql: sql)
    }

    // MARK: - Send Test Email

    /// Sends a test email using the specified profile.
    public func sendTestEmail(
        profileName: String,
        recipients: String,
        subject: String? = nil,
        body: String? = nil
    ) async throws {
        var sql = """
        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = N'\(escapeLiteral(profileName))',
            @recipients = N'\(escapeLiteral(recipients))'
        """
        if let subject {
            sql += ", @subject = N'\(escapeLiteral(subject))'"
        }
        if let body {
            sql += ", @body = N'\(escapeLiteral(body))'"
        }
        sql += ";"
        try await exec(sql: sql)
    }
}
