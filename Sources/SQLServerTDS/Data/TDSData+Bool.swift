import NIOCore

extension TDSData {
    public init(bool: Bool) {
        var buffer = ByteBufferAllocator().buffer(capacity: 1)
        buffer.writeInteger(bool ? 1 : 0, as: UInt8.self)
        self.init(metadata: Bool.tdsMetadata, value: buffer)
    }

    public var bool: Bool? {
        if self.metadata.dataType == .sqlVariant {
            return self.sqlVariantResolved()?.bool
        }

        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .bit, .bitn, .tinyInt:
            guard value.readableBytes == 1,
                  let byte = value.readInteger(as: UInt8.self) else {
                return nil
            }
            return byte != 0
        default:
            return nil
        }
    }
}

extension Bool: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .bit)
    }

    public init?(tdsData: TDSData) {
        guard let bool = tdsData.bool else {
            return nil
        }
        self = bool
    }

    public var tdsData: TDSData? {
        return .init(bool: self)
    }
}
