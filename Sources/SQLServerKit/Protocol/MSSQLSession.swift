import Foundation

/// Protocol defining enhanced SQL Server session capabilities
public protocol MSSQLSession {
    func serverVersion() async throws -> String
    var metadata: SQLServerMetadataNamespace { get }
    var agent: SQLServerAgentOperations { get }
    var admin: SQLServerAdministrationClient { get }
    var security: SQLServerSecurityClient { get }
    var serverSecurity: SQLServerServerSecurityClient { get }
    var extendedProperties: SQLServerExtendedPropertiesClient { get }
    var queryStore: SQLServerQueryStoreClient { get }
    var backupRestore: SQLServerBackupRestoreClient { get }
    var linkedServers: SQLServerLinkedServersClient { get }
    var extendedEvents: SQLServerExtendedEventsClient { get }
    var availabilityGroups: SQLServerAvailabilityGroupsClient { get }
    var databaseMail: SQLServerDatabaseMailClient { get }
    var changeTracking: SQLServerChangeTrackingClient { get }
    var fullText: SQLServerFullTextClient { get }
    var maintenance: SQLServerMaintenanceClient { get }
    var replication: SQLServerReplicationClient { get }
    var cms: SQLServerCMSClient { get }
    var errorLog: SQLServerErrorLogClient { get }
    var audit: SQLServerAuditClient { get }
    var alwaysEncrypted: SQLServerAlwaysEncryptedClient { get }
    var triggers: SQLServerTriggerClient { get }
    var temporal: SQLServerTemporalClient { get }
    var serviceBroker: SQLServerServiceBrokerClient { get }
    var polyBase: SQLServerPolyBaseClient { get }
    var tuning: SQLServerTuningClient { get }
    var profiler: SQLServerProfilerClient { get }
    var resourceGovernor: SQLServerResourceGovernorClient { get }
    var policy: SQLServerPolicyClient { get }
    @available(*, deprecated, message: "Use metadata.objectDependencies() instead.")
    var dependencies: SQLServerDependencyClient { get }
    @available(*, deprecated, message: "DAC operations are not yet implemented.")
    var dac: SQLServerDACClient { get }
    var bulkCopy: SQLServerBulkCopyClient { get }
    var ssis: SQLServerSSISClient { get }
    @available(*, deprecated, message: "SSAS requires an XMLA client, not TDS.")
    var ssas: SQLServerSSASClient { get }
    @available(*, deprecated, message: "SSRS requires an HTTP client, not TDS.")
    var ssrs: SQLServerSSRSClient { get }
}

extension MSSQLSession {
    /// Returns the display type for a column based on its TDS metadata
    public static func displayType(for column: SQLServerColumn) -> String {
        column.typeName
    }

    /// Returns the normalized length for a column based on its TDS metadata
    public static func normalizedLength(for column: SQLServerColumn) -> Int? {
        column.normalizedLength
    }
}

@available(macOS 12.0, *)
extension SQLServerClient: MSSQLSession {}
