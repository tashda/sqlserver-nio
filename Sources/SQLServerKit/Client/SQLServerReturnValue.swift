import Foundation
import SQLServerTDS

public struct SQLServerReturnValue: Sendable {
    public let name: String
    public let status: UInt8
    public let value: SQLServerValue?

    public init(name: String, status: UInt8, value: SQLServerValue?) {
        self.name = name
        self.status = status
        self.value = value
    }

    internal init(name: String, status: UInt8, value: TDSData?) {
        self.init(name: name, status: status, value: value.map(SQLServerValue.init(base:)))
    }

    // Typed convenience accessors
    public var string: String? { value?.string }
    public var int: Int? { value?.int }
    public var int64: Int64? { value?.int64 }
    public var double: Double? { value?.double }
    public var bool: Bool? { value?.bool }
    public var bytes: [UInt8]? { value?.bytes }
}
