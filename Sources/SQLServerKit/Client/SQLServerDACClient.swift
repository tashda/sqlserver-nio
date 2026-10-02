import Foundation
import NIO

/// Client for Data-Tier Application (DAC) operations (DACPAC/BACPAC).
public final class SQLServerDACClient: @unchecked Sendable {
    private let client: SQLServerClient
    
    internal init(client: SQLServerClient) {
        self.client = client
    }
    
    /// Extracts a database schema into a DACPAC model.
    @available(macOS 12.0, *)
    public func extractDacpac(database: String) async throws -> Data {
        // This usually involves generating a complex XML model of the database.
        // For now, we return a placeholder error as this requires extensive metadata mapping.
        throw SQLServerError.notImplemented("DACPAC extraction is not yet fully implemented. Please use SqlPackage CLI for now.")
    }
    
    /// Deploys a DACPAC model to a database.
    @available(macOS 12.0, *)
    public func deployDacpac(data: Data, targetDatabase: String) async throws {
        throw SQLServerError.notImplemented("DACPAC deployment is not yet fully implemented.")
    }
    
    /// Exports a database (schema + data) into a BACPAC archive.
    @available(macOS 12.0, *)
    public func exportBacpac(database: String) async throws -> URL {
        throw SQLServerError.notImplemented("BACPAC export is not yet fully implemented.")
    }
}
