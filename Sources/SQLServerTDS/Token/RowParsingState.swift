
import NIOCore

public struct RowParsingState {
    public var columnIndex: Int = 0
    public var isPartial: Bool = false
    public var savedBufferPosition: Int = 0
}
