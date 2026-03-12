import NIOCore

extension TDSData {
    public init(float: Float) {
        self.init(double: Double(float))
    }

    public var float: Float? {
        if self.metadata.dataType == .sqlVariant {
            return self.sqlVariantResolved()?.float
        }
        guard let value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .real:
            return value.getInteger(at: value.readerIndex, endianness: .little, as: UInt32.self).map(Float.init(bitPattern:))
        case .float:
            return value.getInteger(at: value.readerIndex, endianness: .little, as: UInt64.self)
                .map(Double.init(bitPattern:))
                .map(Float.init)
        default:
            return nil
        }
    }
}

extension Float: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .real)
    }

    public init?(tdsData: TDSData) {
        guard let float = tdsData.float else {
            return nil
        }
        self = float
    }

    public var tdsData: TDSData? {
        return .init(float: self)
    }
}
