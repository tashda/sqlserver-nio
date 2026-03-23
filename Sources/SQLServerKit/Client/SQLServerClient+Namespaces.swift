import Foundation

extension SQLServerClient {
    public var metadata: SQLServerMetadataNamespace { SQLServerMetadataNamespace(client: self) }
    public var agent: SQLServerAgentOperations { SQLServerAgentOperations(client: self) }
    public var admin: SQLServerAdministrationClient { SQLServerAdministrationClient(client: self) }
    public var security: SQLServerSecurityClient { SQLServerSecurityClient(client: self) }
    public var serverSecurity: SQLServerServerSecurityClient { SQLServerServerSecurityClient(client: self) }
    public var transactions: SQLServerTransactionClient { SQLServerTransactionClient(client: self) }
    public var constraints: SQLServerConstraintClient { SQLServerConstraintClient(client: self) }
    public var indexes: SQLServerIndexClient { SQLServerIndexClient(client: self) }
    public var routines: SQLServerRoutineClient { SQLServerRoutineClient(client: self) }
    public var views: SQLServerViewClient { SQLServerViewClient(client: self) }
    public var triggers: SQLServerTriggerClient { SQLServerTriggerClient(client: self) }
    public var types: SQLServerTypeClient { SQLServerTypeClient(client: self) }
    public var activity: SQLServerActivityMonitor { SQLServerActivityMonitor(client: self) }
    public var directory: SQLServerDirectoryClient { SQLServerDirectoryClient(client: self) }
    public var bulkCopy: SQLServerBulkCopyClient { SQLServerBulkCopyClient(client: self) }
    public var executionPlan: SQLServerExecutionPlanClient { SQLServerExecutionPlanClient(client: self) }
    public var extendedProperties: SQLServerExtendedPropertiesClient { SQLServerExtendedPropertiesClient(client: self) }
    public var queryStore: SQLServerQueryStoreClient { SQLServerQueryStoreClient(client: self) }
    public var backupRestore: SQLServerBackupRestoreClient { SQLServerBackupRestoreClient(client: self) }
    public var linkedServers: SQLServerLinkedServersClient { SQLServerLinkedServersClient(client: self) }
    public var extendedEvents: SQLServerExtendedEventsClient { SQLServerExtendedEventsClient(client: self) }
    public var availabilityGroups: SQLServerAvailabilityGroupsClient { SQLServerAvailabilityGroupsClient(client: self) }
    public var databaseMail: SQLServerDatabaseMailClient { SQLServerDatabaseMailClient(client: self) }
    public var changeTracking: SQLServerChangeTrackingClient { SQLServerChangeTrackingClient(client: self) }
    public var fullText: SQLServerFullTextClient { SQLServerFullTextClient(client: self) }
    public var maintenance: SQLServerMaintenanceClient { SQLServerMaintenanceClient(client: self) }
    public var replication: SQLServerReplicationClient { SQLServerReplicationClient(client: self) }
    public var cms: SQLServerCMSClient { SQLServerCMSClient(client: self) }
    public var serverConfig: SQLServerServerConfigurationClient { SQLServerServerConfigurationClient(client: self) }
    public var errorLog: SQLServerErrorLogClient { SQLServerErrorLogClient(client: self) }
}
