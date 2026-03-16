import Foundation

/// Space usage and metadata for a SQL Server table.
public struct SQLServerTableProperties: Sendable {
    public let rowCount: Int64
    public let reservedKB: Int64
    public let dataKB: Int64
    public let indexKB: Int64
    public let unusedKB: Int64
    public let createDate: Date?
    public let modifyDate: Date?

    public init(
        rowCount: Int64,
        reservedKB: Int64,
        dataKB: Int64,
        indexKB: Int64,
        unusedKB: Int64,
        createDate: Date? = nil,
        modifyDate: Date? = nil
    ) {
        self.rowCount = rowCount
        self.reservedKB = reservedKB
        self.dataKB = dataKB
        self.indexKB = indexKB
        self.unusedKB = unusedKB
        self.createDate = createDate
        self.modifyDate = modifyDate
    }
}
