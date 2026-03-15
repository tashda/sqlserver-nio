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
    @available(*, deprecated, message: "Use client.agent instead.")
    func makeAgentClient() -> SQLServerAgentOperations
    @available(*, deprecated, message: "Use client.admin instead.")
    func makeAdministrationClient() -> SQLServerAdministrationClient
    @available(*, deprecated, message: "Use client.security instead.")
    func makeDatabaseSecurityClient() -> SQLServerDatabaseSecurityClient
    @available(*, deprecated, message: "Use client.serverSecurity instead.")
    func makeServerSecurityClient() -> SQLServerServerSecurityClient
}

extension MSSQLSession {
    @available(*, deprecated, message: "Use client.agent instead.")
    public func makeAgentClient() -> SQLServerAgentOperations { agent }
    @available(*, deprecated, message: "Use client.admin instead.")
    public func makeAdministrationClient() -> SQLServerAdministrationClient { admin }
    @available(*, deprecated, message: "Use client.security instead.")
    public func makeDatabaseSecurityClient() -> SQLServerDatabaseSecurityClient { security }
    @available(*, deprecated, message: "Use client.serverSecurity instead.")
    public func makeServerSecurityClient() -> SQLServerServerSecurityClient { serverSecurity }

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
