import Foundation

/// Represents the health and configuration status of a SQL Server database.
public struct SQLServerDatabaseHealth: Sendable, Codable, Equatable {
    /// Name of the database.
    public let name: String
    
    /// Owner of the database.
    public let owner: String
    
    /// Date the database was created.
    public let createDate: Date
    
    /// Total size of the database in megabytes.
    public let sizeMB: Double
    
    /// The recovery model of the database (e.g., FULL, SIMPLE, BULK_LOGGED).
    public let recoveryModel: String
    
    /// Current state of the database (e.g., ONLINE, RESTORING, RECOVERY_PENDING, SUSPECT, EMERGENCY, OFFLINE).
    public let status: String
    
    /// The compatibility level of the database (e.g., 150 for SQL Server 2019).
    public let compatibilityLevel: Int
    
    /// The default collation name for the database.
    public let collationName: String?
    
    public init(
        name: String,
        owner: String,
        createDate: Date,
        sizeMB: Double,
        recoveryModel: String,
        status: String,
        compatibilityLevel: Int,
        collationName: String?
    ) {
        self.name = name
        self.owner = owner
        self.createDate = createDate
        self.sizeMB = sizeMB
        self.recoveryModel = recoveryModel
        self.status = status
        self.compatibilityLevel = compatibilityLevel
        self.collationName = collationName
    }
}
