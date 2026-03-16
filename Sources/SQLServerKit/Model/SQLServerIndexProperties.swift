import Foundation

/// Physical stats and properties of a SQL Server index.
public struct SQLServerIndexProperties: Sendable {
    public let fragmentationPercent: Double
    public let pageCount: Int64
    public let fillFactor: Int
    public let avgPageSpaceUsedPercent: Double?
    public let recordCount: Int64?
    public let ghostRecordCount: Int64?
    public let indexDepth: Int?
    public let indexType: String?

    public init(
        fragmentationPercent: Double,
        pageCount: Int64,
        fillFactor: Int,
        avgPageSpaceUsedPercent: Double? = nil,
        recordCount: Int64? = nil,
        ghostRecordCount: Int64? = nil,
        indexDepth: Int? = nil,
        indexType: String? = nil
    ) {
        self.fragmentationPercent = fragmentationPercent
        self.pageCount = pageCount
        self.fillFactor = fillFactor
        self.avgPageSpaceUsedPercent = avgPageSpaceUsedPercent
        self.recordCount = recordCount
        self.ghostRecordCount = ghostRecordCount
        self.indexDepth = indexDepth
        self.indexType = indexType
    }
}
