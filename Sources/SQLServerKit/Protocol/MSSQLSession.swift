import Foundation
import NIO
import SQLServerTDS

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
    public static func displayType(for column: TDSTokens.ColMetadataToken.ColumnData) -> String {
        return column.displayName
    }

    /// Returns the normalized length for a column based on its TDS metadata
    public static func normalizedLength(for column: TDSTokens.ColMetadataToken.ColumnData) -> Int? {
        return column.normalizedLength
    }
}

extension TDSTokens.ColMetadataToken.ColumnData {
    fileprivate var displayName: String {
        String(describing: dataType)
    }

    fileprivate var normalizedLength: Int? {
        guard length >= 0 else { return nil }
        switch dataType {
        case .nchar, .nvarchar, .nText:
            return Int(length) / 2
        default:
            return Int(length)
        }
    }
}
