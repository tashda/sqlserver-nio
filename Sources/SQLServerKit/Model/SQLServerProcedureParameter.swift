import Foundation
import SQLServerTDS

public struct SQLServerProcedureParameter: Sendable {
    public enum Direction: Sendable {
        case `in`
        case out
        case `inout`
    }

    public var name: String
    public var value: TDSData?
    public var direction: Direction

    public init(name: String, value: TDSData?, direction: Direction = .in) {
        self.name = name
        self.value = value
        self.direction = direction
    }
}

// Keep backward compatibility with SQLServerConnection.ProcedureParameter if needed
extension SQLServerConnection {
    public typealias ProcedureParameter = SQLServerProcedureParameter
}
