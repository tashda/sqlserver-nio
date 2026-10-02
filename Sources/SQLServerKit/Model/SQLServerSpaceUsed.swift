/// Result of `sp_spaceused` for a single table.
public struct SQLServerSpaceUsed: Sendable {
    /// Number of rows as returned by sp_spaceused (e.g. "1024").
    public let rows: String
    /// Reserved space string (e.g. "128 KB").
    public let reserved: String
    /// Data space string (e.g. "64 KB").
    public let data: String
    /// Index size string (e.g. "32 KB").
    public let indexSize: String
    /// Unused space string (e.g. "32 KB").
    public let unused: String

    public init(rows: String, reserved: String, data: String, indexSize: String, unused: String) {
        self.rows = rows
        self.reserved = reserved
        self.data = data
        self.indexSize = indexSize
        self.unused = unused
    }
}
