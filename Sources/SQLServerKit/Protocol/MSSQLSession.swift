import Foundation
import NIO

/// Protocol defining enhanced SQL Server session capabilities
public protocol MSSQLSession {
    func serverVersion() async throws -> String
    func makeAgentClient() -> SQLServerAgentOperations
    func makeAdministrationClient() -> SQLServerAdministrationClient
    func makeDatabaseSecurityClient() -> SQLServerDatabaseSecurityClient
    func makeServerSecurityClient() -> SQLServerServerSecurityClient
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
