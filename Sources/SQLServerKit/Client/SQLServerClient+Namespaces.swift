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
    public var audit: SQLServerAuditClient { SQLServerAuditClient(client: self) }
    public var alwaysEncrypted: SQLServerAlwaysEncryptedClient { SQLServerAlwaysEncryptedClient(client: self) }
    public var temporal: SQLServerTemporalClient { SQLServerTemporalClient(client: self) }
    public var serviceBroker: SQLServerServiceBrokerClient { SQLServerServiceBrokerClient(client: self) }
    public var polyBase: SQLServerPolyBaseClient { SQLServerPolyBaseClient(client: self) }
    public var tuning: SQLServerTuningClient { SQLServerTuningClient(client: self) }
    public var profiler: SQLServerProfilerClient { SQLServerProfilerClient(client: self) }
    public var resourceGovernor: SQLServerResourceGovernorClient { SQLServerResourceGovernorClient(client: self) }
    public var policy: SQLServerPolicyClient { SQLServerPolicyClient(client: self) }
    public var dependencies: SQLServerDependencyClient { SQLServerDependencyClient(client: self) }
    public var dac: SQLServerDACClient { SQLServerDACClient(client: self) }
    public var ssis: SQLServerSSISClient { SQLServerSSISClient(client: self) }
    public var ssas: SQLServerSSASClient { SQLServerSSASClient(client: self) }
    public var ssrs: SQLServerSSRSClient { SQLServerSSRSClient(client: self) }
}
