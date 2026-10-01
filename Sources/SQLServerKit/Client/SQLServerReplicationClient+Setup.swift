import Foundation

/// Setting up replication on one instance end to end, the steps Microsoft's Linux tutorial
/// takes: the instance as its own distributor, a database enabled for publishing, a snapshot
/// publication with table articles, and a push subscription (to another database on the same
/// instance or another server). Statements that belong to a database run inside it through
/// `sp_executesql`, so a pooled connection keeps its database.
@available(macOS 12.0, *)
extension SQLServerReplicationClient {
    /// The instance distributes for itself: `sp_adddistributor`, `sp_adddistributiondb` and
    /// `sp_adddistpublisher` with `login`/`password` (SQL authentication) and the snapshot folder.
    public func configureLocalDistributor(login: String, password: String,
                                          snapshotFolder: String = "/var/opt/mssql/data/ReplData") async throws {
        _ = try await client.execute(Self.localDistributorSQL(login: login, password: password, snapshotFolder: snapshotFolder))
    }

    /// `sp_replicationdboption @optname = 'publish'`.
    public func enablePublishing(database: String) async throws {
        _ = try await client.execute(
            "EXEC sp_replicationdboption @dbname = N'\(SQLServerSQL.escapeLiteral(database))', @optname = N'publish', @value = N'true'")
    }

    /// A snapshot publication in `database` with its Snapshot Agent job (run on demand).
    public func createSnapshotPublication(name: String, database: String, description: String = "",
                                          publisherLogin: String, publisherPassword: String) async throws {
        let publication = SQLServerSQL.escapeLiteral(name)
        try await executeInDatabase(database, """
            EXEC sp_addpublication @publication = N'\(publication)', @description = N'\(SQLServerSQL.escapeLiteral(description))',
                @retention = 0, @allow_push = N'true', @repl_freq = N'snapshot', @status = N'active', @independent_agent = N'true';
            EXEC sp_addpublication_snapshot @publication = N'\(publication)', @frequency_type = 1, @frequency_interval = 1,
                @publisher_security_mode = 0, @publisher_login = N'\(SQLServerSQL.escapeLiteral(publisherLogin))',
                @publisher_password = N'\(SQLServerSQL.escapeLiteral(publisherPassword))';
            """)
    }

    /// Publishes a table (`sp_addarticle`, log-based, same name at the subscriber).
    public func addTableArticle(publication: String, database: String, table: String, schema: String = "dbo") async throws {
        let article = SQLServerSQL.escapeLiteral(table)
        try await executeInDatabase(database, """
            EXEC sp_addarticle @publication = N'\(SQLServerSQL.escapeLiteral(publication))', @article = N'\(article)',
                @source_owner = N'\(SQLServerSQL.escapeLiteral(schema))', @source_object = N'\(article)', @type = N'logbased',
                @pre_creation_cmd = N'drop', @identityrangemanagementoption = N'manual',
                @destination_table = N'\(article)', @destination_owner = N'\(SQLServerSQL.escapeLiteral(schema))';
            """)
    }

    /// A push subscription to `subscriberDatabase` on `subscriber` (default: this instance) with
    /// its Distribution Agent job (run on demand).
    public func addPushSubscription(publication: String, database: String, subscriberDatabase: String,
                                    subscriber: String? = nil, login: String, password: String) async throws {
        let publicationName = SQLServerSQL.escapeLiteral(publication)
        let server = subscriber.map { "N'\(SQLServerSQL.escapeLiteral($0))'" } ?? "@@SERVERNAME"
        let destination = SQLServerSQL.escapeLiteral(subscriberDatabase)
        try await executeInDatabase(database, """
            DECLARE @subscriber sysname = \(server);
            EXEC sp_addsubscription @publication = N'\(publicationName)', @subscriber = @subscriber,
                @destination_db = N'\(destination)', @subscription_type = N'Push', @sync_type = N'automatic',
                @article = N'all', @update_mode = N'read only', @subscriber_type = 0;
            EXEC sp_addpushsubscription_agent @publication = N'\(publicationName)', @subscriber = @subscriber,
                @subscriber_db = N'\(destination)', @subscriber_security_mode = 0,
                @subscriber_login = N'\(SQLServerSQL.escapeLiteral(login))', @subscriber_password = N'\(SQLServerSQL.escapeLiteral(password))',
                @frequency_type = 1, @frequency_interval = 0, @active_end_date = 19950101;
            """)
    }

    internal static func localDistributorSQL(login: String, password: String, snapshotFolder: String) -> String {
        let login = SQLServerSQL.escapeLiteral(login), password = SQLServerSQL.escapeLiteral(password)
        let folder = SQLServerSQL.escapeLiteral(snapshotFolder)
        return """
            DECLARE @server sysname = @@SERVERNAME;
            EXEC sp_adddistributor @distributor = @server, @password = N'\(password)';
            EXEC sp_adddistributiondb @database = N'distribution', @security_mode = 0, @login = N'\(login)', @password = N'\(password)';
            EXEC sp_adddistpublisher @publisher = @server, @distribution_db = N'distribution', @security_mode = 0,
                @login = N'\(login)', @password = N'\(password)', @working_directory = N'\(folder)', @trusted = N'false',
                @thirdparty_flag = 0, @publisher_type = N'MSSQLSERVER';
            """
    }

    private func executeInDatabase(_ database: String, _ sql: String) async throws {
        _ = try await client.execute("EXEC \(SQLServerSQL.escapeIdentifier(database)).sys.sp_executesql N'\(SQLServerSQL.escapeLiteral(sql))'")
    }
}
