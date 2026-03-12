import NIO
import SQLServerTDS

// MARK: - SQLServerConstraintClient

public final class SQLServerConstraintClient: @unchecked Sendable {
    internal let client: SQLServerClient
    
    public init(client: SQLServerClient) {
        self.client = client
    }
    
    internal static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}
