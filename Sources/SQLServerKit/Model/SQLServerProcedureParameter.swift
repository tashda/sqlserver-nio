import Foundation

public struct SQLServerProcedureParameter: Sendable {
    public enum Direction: Sendable {
        case `in`
        case out
        case `inout`
    }

    public var name: String
    public var value: SQLServerValue?
    public var direction: Direction

    public init(name: String, value: SQLServerValue?, direction: Direction = .in) {
        self.name = name
        self.value = value
        self.direction = direction
    }

    public init<T: SQLServerDataConvertible>(name: String, value: T?, direction: Direction = .in) {
        self.init(name: name, value: value?.sqlServerValue, direction: direction)
    }
}

// Keep backward compatibility with SQLServerConnection.ProcedureParameter if needed
extension SQLServerConnection {
    public typealias ProcedureParameter = SQLServerProcedureParameter
}
