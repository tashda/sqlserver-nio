/// Metadata for a SQL Server sequence object.
public struct SequenceMetadata: Sendable {
    public let name: String
    public let schema: String
    public let dataType: String
    public let startValue: String
    public let incrementBy: String
    public let minValue: String
    public let maxValue: String
    public let isCycling: Bool
    public let cacheSize: Int
    public let currentValue: String?
    public let comment: String?

    public init(
        name: String,
        schema: String,
        dataType: String,
        startValue: String,
        incrementBy: String,
        minValue: String,
        maxValue: String,
        isCycling: Bool,
        cacheSize: Int,
        currentValue: String? = nil,
        comment: String? = nil
    ) {
        self.name = name
        self.schema = schema
        self.dataType = dataType
        self.startValue = startValue
        self.incrementBy = incrementBy
        self.minValue = minValue
        self.maxValue = maxValue
        self.isCycling = isCycling
        self.cacheSize = cacheSize
        self.currentValue = currentValue
        self.comment = comment
    }
}
