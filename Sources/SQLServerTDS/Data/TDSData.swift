import NIOCore

public protocol TDSDataConvertible {
    static var tdsMetadata: any Metadata { get }
    init?(tdsData: TDSData)
    var tdsData: TDSData? { get }
}

public struct TDSData: Sendable {
    public let metadata: any Metadata
    public let value: ByteBuffer?

    public init(metadata: any Metadata, value: ByteBuffer?) {
        self.metadata = metadata
        self.value = value
    }
}

extension TDSData: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        TypeMetadata(dataType: .null)
    }

    public init?(tdsData: TDSData) {
        self = tdsData
    }

    public var tdsData: TDSData? {
        self
    }
}
