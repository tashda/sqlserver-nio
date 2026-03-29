import Foundation
import SQLServerTDS

// MARK: - Database Mirroring

@available(macOS 12.0, *)
extension SQLServerAdministrationClient {

    /// Fetches the mirroring status for a database from sys.database_mirroring.
    public func fetchMirroringStatus(database: String) async throws -> SQLServerMirroringStatus {
        let escapedName = database.replacingOccurrences(of: "'", with: "''")
        
        // On some platforms (like SQL Server on Linux), mirroring views might be empty 
        // or throw errors. We attempt to fetch and fall back to unconfigured if not found.
        let sql = """
        SELECT
            dm.database_id,
            dm.mirroring_state_desc,
            dm.mirroring_role_desc,
            dm.mirroring_safety_level_desc,
            COALESCE(dm.mirroring_partner_name, '') AS partner_name,
            COALESCE(dm.mirroring_partner_instance, '') AS partner_instance,
            COALESCE(dm.mirroring_witness_name, '') AS witness_name,
            COALESCE(dm.mirroring_witness_state_desc, '') AS witness_state,
            dm.mirroring_connection_timeout,
            dm.mirroring_redo_queue,
            dm.mirroring_redo_queue_type
        FROM sys.database_mirroring dm
        JOIN sys.databases d ON d.database_id = dm.database_id
        WHERE d.name = N'\(escapedName)'
        """

        let rows: [SQLServerRow]
        do {
            rows = try await client.query(sql)
        } catch {
            // If mirroring is not supported at all, return unconfigured
            return .unconfigured
        }
        
        guard let row = rows.first else {
            return .unconfigured
        }

        let stateDesc = row.column("mirroring_state_desc")?.string
        let isConfigured = stateDesc != nil && !stateDesc!.isEmpty

        if !isConfigured {
            return .unconfigured
        }

        return SQLServerMirroringStatus(
            isConfigured: isConfigured,
            stateDescription: stateDesc,
            roleDescription: row.column("mirroring_role_desc")?.string,
            safetyLevelDescription: row.column("mirroring_safety_level_desc")?.string,
            partnerName: row.column("partner_name")?.string ?? "",
            partnerInstance: row.column("partner_instance")?.string ?? "",
            witnessName: row.column("witness_name")?.string ?? "",
            witnessStateDescription: row.column("witness_state")?.string ?? "",
            connectionTimeout: row.column("mirroring_connection_timeout")?.int,
            redoQueue: row.column("mirroring_redo_queue")?.int,
            redoQueueType: row.column("mirroring_redo_queue_type")?.string
        )
    }

    /// Sets the mirroring partner for a database.
    /// The partner address should be in the format: TCP://hostname:port
    @discardableResult
    public func setMirroringPartner(
        database: String,
        partnerAddress: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let escapedAddr = partnerAddress.replacingOccurrences(of: "'", with: "''")
        let sql = "ALTER DATABASE \(escapedDb) SET PARTNER = N'\(escapedAddr)'"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Sets the mirroring witness for a database.
    /// The witness address should be in the format: TCP://hostname:port
    @discardableResult
    public func setMirroringWitness(
        database: String,
        witnessAddress: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let escapedAddr = witnessAddress.replacingOccurrences(of: "'", with: "''")
        let sql = "ALTER DATABASE \(escapedDb) SET WITNESS = N'\(escapedAddr)'"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Removes the mirroring witness.
    @discardableResult
    public func removeMirroringWitness(
        database: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(escapedDb) SET WITNESS OFF"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Initiates a manual failover from the principal to the mirror.
    /// Must be run on the current principal server.
    @discardableResult
    public func failoverMirroring(
        database: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(escapedDb) SET PARTNER FAILOVER"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Pauses a mirroring session.
    @discardableResult
    public func pauseMirroring(
        database: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(escapedDb) SET PARTNER SUSPEND"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Resumes a paused mirroring session.
    @discardableResult
    public func resumeMirroring(
        database: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(escapedDb) SET PARTNER RESUME"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Removes mirroring from a database entirely.
    @discardableResult
    public func removeMirroring(
        database: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(escapedDb) SET PARTNER OFF"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Sets the mirroring safety level (operating mode).
    @discardableResult
    public func setMirroringSafetyLevel(
        database: String,
        level: SQLServerMirroringSafetyLevel
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(escapedDb) SET PARTNER SAFETY \(level.rawValue)"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Sets the mirroring connection timeout in seconds.
    @discardableResult
    public func setMirroringTimeout(
        database: String,
        seconds: Int
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(escapedDb) SET PARTNER TIMEOUT \(seconds)"
        let result = try await client.execute(sql)
        return result.messages
    }
}
