import Foundation
import NIO
import SQLServerTDS

/// Protocol defining enhanced SQL Server session capabilities
public protocol MSSQLSession {
    func serverVersion() async throws -> String
    func makeAgentClient() -> SQLServerAgentClient
    func makeDatabaseSecurityClient() -> SQLServerDatabaseSecurityClient
    func makeServerSecurityClient() -> SQLServerServerSecurityClient
}

extension MSSQLSession {
    /// Returns the display type for a column based on its TDS metadata
    public static func displayType(for column: TDSTokens.ColMetadataToken.ColumnData) -> String {
        return column.displayName
    }

    /// Returns the normalized length for a column based on its TDS metadata
    public static func normalizedLength(for column: TDSTokens.ColMetadataToken.ColumnData) -> Int? {
        return column.normalizedLength
    }
}