import Foundation
import NIOCore
import SQLServerTDS

public struct SQLServerValue: Sendable, CustomStringConvertible {
    internal let base: TDSData

    internal init(base: TDSData) {
        self.base = base
    }

    public init(string: String) {
        self.base = TDSData(string: string)
    }

    public init(int: Int) {
        self.base = TDSData(int: int)
    }

    public init(int32: Int32) {
        self.base = TDSData(int32: int32)
    }

    public init(int64: Int64) {
        self.base = TDSData(int64: int64)
    }

    public init(double: Double) {
        self.base = TDSData(double: double)
    }

    public init(bool: Bool) {
        self.base = TDSData(bool: bool)
    }

    public init(date: Date) {
        self.base = TDSData(date: date)
    }

    public init(data: Data) {
        var buffer = ByteBufferAllocator().buffer(capacity: data.count)
        buffer.writeBytes(data)
        self.base = TDSData(
            metadata: TypeMetadata(dataType: .varbinary, length: Int32(data.count)),
            value: buffer
        )
    }

    public init(uuid: UUID) {
        var buffer = ByteBufferAllocator().buffer(capacity: 16)
        _ = withUnsafeBytes(of: uuid.uuid) { raw in
            buffer.writeBytes(raw)
        }
        self.base = TDSData(
            metadata: TypeMetadata(dataType: .guid, length: 16),
            value: buffer
        )
    }

    public var isNull: Bool { base.value == nil }
    public var string: String? { base.string }
    public var int: Int? { base.int }
    public var int8: Int8? { base.int8 }
    public var int16: Int16? { base.int16 }
    public var int32: Int32? { base.int32 }
    public var int64: Int64? { base.int64 }
    public var uint: UInt? { base.uint }
    public var uint8: UInt8? { base.uint8 }
    public var uint16: UInt16? { base.uint16 }
    public var uint32: UInt32? { base.uint32 }
    public var uint64: UInt64? { base.uint64 }
    public var double: Double? { base.double }
    public var bool: Bool? { base.bool }
    public var date: Date? { base.date }
    public var decimal: Decimal? { base.decimal }
    public var uuid: UUID? { base.uuid }
    public var bytes: [UInt8]? { base.bytes }
    public var data: Data? { base.bytes.map { Data($0) } }
    public var type: SQLServerDataType { SQLServerDataType(base: base.metadata.dataType) }
    public var udtTypeName: String? {
        (base.metadata as? TDSTokens.ColMetadataToken.ColumnData)?.udtInfo?.typeName
    }
    
    /// Returns the decoded spatial data if this value is a geometry or geography UDT.
    public var spatial: SQLServerSpatial? {
        guard let name = udtTypeName,
              (name.caseInsensitiveCompare("geometry") == .orderedSame || 
               name.caseInsensitiveCompare("geography") == .orderedSame),
              var buffer = base.value else {
            return nil
        }
        return SQLServerSpatial.decode(from: &buffer)
    }

    public var description: String {
        if isNull { return "NULL" }
        if let name = udtTypeName {
            if name.caseInsensitiveCompare("hierarchyid") == .orderedSame,
               let bytes,
               let hierarchyID = SQLServerHierarchyID.string(from: bytes) {
                return hierarchyID
            }
            if name.caseInsensitiveCompare("geometry") == .orderedSame || 
               name.caseInsensitiveCompare("geography") == .orderedSame,
               let spatial = self.spatial {
                return spatial.wkt
            }
        }
        if let string { return string }
        if let int { return String(int) }
        if let int64 { return String(int64) }
        if let double { return String(double) }
        if let bool { return String(bool) }
        if let date { return ISO8601DateFormatter().string(from: date) }
        if let decimal { return NSDecimalNumber(decimal: decimal).stringValue }
        if let uuid { return uuid.uuidString }
        if let bytes {
            let hex = bytes.map { String(format: "%02X", $0) }.joined()
            return "0x\(hex)"
        }
        return "<\(udtTypeName ?? type.name)>"
    }
}

public protocol SQLServerDataConvertible: Sendable {
    init?(sqlServerValue: SQLServerValue)
    var sqlServerValue: SQLServerValue? { get }
}

extension SQLServerValue: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        self = sqlServerValue
    }

    public var sqlServerValue: SQLServerValue? { self }
}

extension String: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.string else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(string: self) }
}

extension Int: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.int else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(int: self) }
}

extension Int32: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.int32 else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(int32: self) }
}

extension Int64: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.int64 else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(int64: self) }
}

extension Double: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.double else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(double: self) }
}

extension Float: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.double else { return nil }
        self = Float(value)
    }

    public var sqlServerValue: SQLServerValue? { .init(double: Double(self)) }
}

extension Bool: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.bool else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(bool: self) }
}

extension Date: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.date else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(date: self) }
}

extension Decimal: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.decimal else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? {
        guard let data = try? TDSData(decimal: self, precision: 38, scale: 18) else { return nil }
        return .init(base: data)
    }
}

extension Data: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.data else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(data: self) }
}

extension UUID: SQLServerDataConvertible {
    public init?(sqlServerValue: SQLServerValue) {
        guard let value = sqlServerValue.uuid else { return nil }
        self = value
    }

    public var sqlServerValue: SQLServerValue? { .init(uuid: self) }
}
