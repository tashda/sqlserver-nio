import NIO
import Logging

public final class TDSPacketEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = TDSPacket

    let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }
    
    /// See `MessageToByteEncoder`.
    public func encode(data message: TDSPacket, out: inout ByteBuffer) throws {
        var packet = message
        out.writeBuffer(&packet.buffer)
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}
